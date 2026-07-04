"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025-2026 Ilona Tag

This file is part of elevata.

elevata is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of
the License, or (at your option) any later version.

elevata is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with elevata. If not, see <https://www.gnu.org/licenses/>.

Contact: <https://github.com/elevata-labs/elevata>.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
from typing import Any

from metadata.materialization.plan import MaterializationStep


@dataclass
class MigrationMaterializationResult:
  """
  Translate MigrationPlan intent into executable materialization steps.

  Notes:
  - SQL is rendered by the dialect (dialect-owned SQL).
  - Policy violations are surfaced via blocking_errors (caller decides how to fail).
  """
  steps: list[MaterializationStep] = field(default_factory=list)
  warnings: list[str] = field(default_factory=list)
  blocking_errors: list[str] = field(default_factory=list)
  requires_rebuild: bool = False


def _render_required_schema_sql(
  res: MigrationMaterializationResult,
  dialect,
  method_name: str,
  action_type: str,
  dataset_key: str,
  *args,
) -> str | None:
  """
  Render SQL for a required schema action and fail closed when unsupported.

  MigrationPlan actions are execution intent. Silently dropping a required
  schema action lets the subsequent load run against stale physical objects.
  """
  renderer = getattr(dialect, method_name, None)
  if not callable(renderer):
    res.blocking_errors.append(
      f"UNSUPPORTED_SCHEMA_ACTION: {action_type} for {dataset_key} "
      f"requires dialect.{method_name}()."
    )
    return None

  try:
    sql = renderer(*args)
  except Exception as exc:
    res.blocking_errors.append(
      f"SCHEMA_ACTION_RENDER_FAILED: {action_type} for {dataset_key} "
      f"failed in {method_name}: {type(exc).__name__}: {exc}"
    )
    return None

  if not str(sql or "").strip():
    res.blocking_errors.append(
      f"UNSUPPORTED_SCHEMA_ACTION: {action_type} for {dataset_key} "
      f"was not rendered by dialect.{method_name}()."
    )
    return None

  return str(sql)


def build_materialization_from_migration_plan(
  *,
  td,
  dataset_key: str,
  migration_plan: Any,
  dialect,
  policy,
  introspection_engine,
  exec_engine,
  is_full_refresh: bool,
) -> MigrationMaterializationResult:
  """
  Translate MigrationPlan schema intent into deterministic DDL steps.

  - No heuristic drift healing: steps come only from migration_plan.actions.
  - Dialect remains owner of SQL rendering (no SQL construction here).
  - Preflight/drift detection remains planner-owned (caller keeps planner warnings/errors).
  """
  res = MigrationMaterializationResult()
  if migration_plan is None:
    return res

  actions = list(getattr(migration_plan, "actions", None) or [])
  if not actions:
    return res

  schema_action_types = {
    "RENAME_DATASET",
    "RENAME_COLUMN",
    "ADD_COLUMN",
    "DROP_COLUMN",
    "ALTER_COLUMN",
    "REBUILD_DATASET",
  }

  relevant: list[Any] = []
  for a in actions:
    at = getattr(a, "action_type", None)
    if at not in schema_action_types:
      continue
    dk = getattr(a, "dataset_key", None)
    prev_dk = getattr(a, "previous_dataset_key", None)
    if dk == dataset_key or prev_dk == dataset_key:
      relevant.append(a)

  if not relevant:
    return res

  schema_name = getattr(getattr(td, "target_schema", None), "schema_name", None)
  table_name = getattr(td, "target_dataset_name", None)
  if not schema_name or not table_name:
    res.blocking_errors.append(f"INVALID_TARGET_DATASET: missing schema/table for {dataset_key}")
    return res

  current_actual_cols_cache: dict[str, dict[str, Any]] | None = None

  def _current_actual_cols_by_norm_name() -> dict[str, dict[str, Any]]:
    """
    Return current physical columns for the target relation, best-effort.

    MigrationPlan actions remain the only source of schema-evolution intent.
    This check only makes applying already-approved intent idempotent within
    one execution run or after a partially completed run.
    """
    nonlocal current_actual_cols_cache
    if current_actual_cols_cache is not None:
      return current_actual_cols_cache

    inspect = getattr(dialect, "introspect_table", None)
    if not callable(inspect):
      current_actual_cols_cache = {}
      return current_actual_cols_cache

    try:
      meta = inspect(
        schema_name=schema_name,
        table_name=table_name,
        introspection_engine=introspection_engine,
        exec_engine=exec_engine,
        debug_plan=bool(getattr(policy, "debug_plan", False)),
      )
    except Exception:
      meta = {}

    current_actual_cols_cache = dict((meta or {}).get("actual_cols_by_norm_name") or {})
    return current_actual_cols_cache

  def _norm_column_name(column_name: str) -> str:
    return str(column_name or "").strip().lower()

  def _physical_column_exists(column_name: str) -> bool:
    return _norm_column_name(column_name) in _current_actual_cols_by_norm_name()

  def _rename_column_already_applied(*, old_name: str, new_name: str) -> bool:
    """
    Return True when an approved rename intent has already reached the
    physical table.

    This keeps MigrationPlan application idempotent for companion hist syncs
    and partially completed executions: the intent remains explicit, but we do
    not issue a duplicate RENAME COLUMN when the old physical column is gone
    and the new physical column is already present.
    """
    old_norm = _norm_column_name(old_name)
    new_norm = _norm_column_name(new_name)
    actual_cols = _current_actual_cols_by_norm_name()
    return bool(new_norm and new_norm in actual_cols and old_norm not in actual_cols)

  def _source_name_for_rebuild_column(name: str, rename_cols: dict[str, str]) -> str:
    """
    Return the physical source column to use during deterministic rebuild.

    For a pending rename, the old physical column is the source. If the same
    MigrationPlan is applied again after a companion sync or partial run, the
    old column may already be gone and the new column may already exist. In
    that already-applied case, read from the new physical column instead.
    """
    old_name = rename_cols.get(str(name))
    if not old_name:
      return str(name)

    if _rename_column_already_applied(old_name=old_name, new_name=str(name)):
      res.warnings.append(
        "SCHEMA_ACTION_ALREADY_APPLIED: RENAME_COLUMN requested by MigrationPlan "
        f"for {dataset_key}.{old_name} -> {name}, but the physical column is "
        "already renamed. Reusing the current column during rebuild."
      )
      return str(name)

    return str(old_name)

  # Dataset rename
  rename_ds = next((a for a in relevant if getattr(a, "action_type", None) == "RENAME_DATASET"), None)
  prev_ds_key = getattr(rename_ds, "previous_dataset_key", None) if rename_ds else None
  prev_table = None
  if isinstance(prev_ds_key, str) and "." in prev_ds_key:
    prev_table = prev_ds_key.split(".", 1)[1]
  elif isinstance(prev_ds_key, str) and prev_ds_key:
    prev_table = prev_ds_key

  # Full refresh: only keep dataset rename (so subsequent DROP/CREATE hits the right object)
  if is_full_refresh:
    if rename_ds and prev_table and prev_table != table_name:
      sql = _render_required_schema_sql(
        res,
        dialect,
        "render_rename_table",
        "RENAME_DATASET",
        dataset_key,
        schema_name,
        prev_table,
        table_name,
      )
      if sql:
        res.steps.append(MaterializationStep(
          op="RENAME_DATASET",
          sql=sql,
          reason=f"Dataset rename: {prev_table} -> {table_name}",
          safe=True,
        ))
    return res

  # ------------------ helpers ------------------------------------------------
  def _stable_tmp_name(ds_key: str, base: str) -> str:
    h = hashlib.sha256(ds_key.encode("utf-8")).hexdigest()[:8]
    return f"__elevata_tmp_{base}_{h}"

  def _iter_target_columns(td_obj) -> list[Any]:
    cols_obj = getattr(td_obj, "target_columns", None)
    if cols_obj is None:
      return []
    if hasattr(cols_obj, "all"):
      qs = cols_obj.all()
      if hasattr(qs, "filter"):
        try:
          qs = qs.filter(active=True)
        except Exception:
          pass
      if hasattr(qs, "order_by"):
        try:
          qs = qs.order_by("ordinal_position")
        except Exception:
          pass
      return list(qs)
    try:
      cols = list(cols_obj)
      cols.sort(key=lambda c: getattr(c, "ordinal_position", 0) or 0)
      return cols
    except Exception:
      return []

  def _physical_type_for(col_obj) -> str | None:
    max_length = getattr(col_obj, "max_length", None)
    precision = getattr(col_obj, "precision", None) or getattr(col_obj, "decimal_precision", None)
    scale = getattr(col_obj, "scale", None) or getattr(col_obj, "decimal_scale", None)
    try:
      return dialect.map_logical_type(
        datatype=getattr(col_obj, "datatype", None),
        max_length=max_length,
        precision=precision,
        scale=scale,
        strict=True,
      )
    except Exception:
      return None

  # ------------------ policy checks ------------------------------------------
  is_hist = bool(getattr(td, "is_hist", False)) or str(table_name or "").endswith("_hist")
  allow_auto_drop = bool(getattr(policy, "allow_auto_drop_columns", False))
  allow_hist_drop = bool(getattr(policy, "allow_auto_drop_hist_columns", False))

  drop_column_actions = [
    a for a in relevant
    if getattr(a, "action_type", None) == "DROP_COLUMN"
  ]

  if drop_column_actions:
    if is_hist and not allow_hist_drop:
      cols = [
        str(getattr(a, "column_name", "") or "?")
        for a in drop_column_actions
      ]
      res.warnings.append(
        "POLICY_HIST_DROP_SUPPRESSED: Drop column requested by MigrationPlan "
        f"for historized dataset {dataset_key} ({', '.join(cols)}), but "
        "ELEVATA_ALLOW_AUTO_DROP_HIST_COLUMNS is disabled."
      )
    elif not allow_auto_drop:
      res.blocking_errors.append(
        f"POLICY_AUTO_DROP_DISABLED: Drop column requested by MigrationPlan for {dataset_key} "
        f"(set ELEVATA_ALLOW_AUTO_DROP_COLUMNS=true to allow)."
      )

  # ------------------ determine rebuild necessity ----------------------------
  rebuild_requested = any(getattr(a, "action_type", None) == "REBUILD_DATASET" for a in relevant)

  # If dialect cannot render ALTER for any requested ALTER_COLUMN -> rebuild.
  supported_alter_sql: dict[str, str] = {}
  if not rebuild_requested:
    # Best-effort introspection for old_type
    try:
      meta = dialect.introspect_table(
        schema_name=schema_name,
        table_name=(prev_table or table_name),
        introspection_engine=introspection_engine,
        exec_engine=exec_engine,
        debug_plan=bool(getattr(policy, "debug_plan", False)),
      )
    except Exception:
      meta = {"actual_cols_by_norm_name": {}}
    actual_cols = dict((meta or {}).get("actual_cols_by_norm_name") or {})

    for a in relevant:
      if getattr(a, "action_type", None) != "ALTER_COLUMN":
        continue
      col_name = getattr(a, "column_name", None)
      if not col_name:
        continue

      cols = _iter_target_columns(td)
      col_obj = next(
        (c for c in cols if str(getattr(c, "target_column_name", "")).lower() == str(col_name).lower()),
        None,
      )
      new_type = _physical_type_for(col_obj) if col_obj else None
      if not new_type:
        res.blocking_errors.append(f"MISSING_COLUMN_TYPE: {dataset_key}.{col_name}")
        rebuild_requested = True
        continue

      old_meta = actual_cols.get(str(col_name).lower(), {}) if actual_cols else {}
      old_type = (old_meta or {}).get("type")
      sql = dialect.render_alter_column_type(
        schema=schema_name,
        table=table_name,
        column=str(col_name),
        new_type=str(new_type),
        old_type=old_type,
      )
      if not sql:
        rebuild_requested = True
      else:
        supported_alter_sql[str(col_name)] = sql

  if rebuild_requested:
    res.requires_rebuild = True

  # ------------------ build deterministic steps ------------------------------
  if rebuild_requested:
    src_table = prev_table or table_name
    tmp = _stable_tmp_name(dataset_key, table_name)

    rename_cols = {
      str(getattr(a, "column_name")): str(getattr(a, "previous_column_name"))
      for a in relevant
      if getattr(a, "action_type", None) == "RENAME_COLUMN"
      and getattr(a, "column_name", None)
      and getattr(a, "previous_column_name", None)
    }

    columns_payload: list[dict[str, object]] = []
    for c in _iter_target_columns(td):
      name = getattr(c, "target_column_name", None)
      if not name:
        continue
      ptype = _physical_type_for(c)
      if not ptype:
        res.blocking_errors.append(f"MISSING_COLUMN_TYPE: {dataset_key}.{name}")
        continue

      # New column? -> source_name=None => dialect renders NULL AS col
      is_added = any(
        getattr(a, "action_type", None) == "ADD_COLUMN"
        and str(getattr(a, "column_name", "")).lower() == str(name).lower()
        for a in relevant
      )
      if is_added:
        src_name = None
      else:
        src_name = _source_name_for_rebuild_column(str(name), rename_cols)

      columns_payload.append({
        "name": str(name),
        "type": str(ptype),
        "nullable": bool(getattr(c, "nullable", True)),
        "source_name": src_name,
      })

    sql = dialect.render_drop_table_if_exists(schema=schema_name, table=tmp, cascade=False)
    if sql:
      res.steps.append(MaterializationStep(
        op="DROP_TABLE_IF_EXISTS",
        sql=sql,
        reason=f"rebuild tmp cleanup: {tmp}",
        safe=True,
      ))

    sql = dialect.render_create_table_from_columns(schema=schema_name, table=tmp, columns=columns_payload)
    if sql:
      res.steps.append(MaterializationStep(op="CREATE_TABLE", sql=sql, reason=f"rebuild tmp create: {tmp}", safe=True))

    sql = dialect.render_insert_select_for_rebuild(
      schema=schema_name,
      src_table=src_table,
      dst_table=tmp,
      columns=columns_payload,
      lossy_casts=True,
      truncate_strings=False,
    )
    if sql:
      res.steps.append(MaterializationStep(op="INSERT_SELECT", sql=sql, reason=f"rebuild tmp backfill: {src_table} -> {tmp}", safe=True))

    sql = dialect.render_drop_table(schema=schema_name, table=src_table, cascade=False)
    if sql:
      res.steps.append(MaterializationStep(
        op="DROP_TABLE",
        sql=sql,
        reason=f"rebuild drop source: {src_table}",
        safe=True,
      ))

    sql = dialect.render_rename_table(schema_name, tmp, table_name)
    if sql:
      res.steps.append(MaterializationStep(op="RENAME_TABLE", sql=sql, reason=f"rebuild swap: {tmp} -> {table_name}", safe=True))

    return res

  # Non-rebuild: deterministic order
  if rename_ds and prev_table and prev_table != table_name:
    sql = _render_required_schema_sql(
      res,
      dialect,
      "render_rename_table",
      "RENAME_DATASET",
      dataset_key,
      schema_name,
      prev_table,
      table_name,
    )
    if sql:
      res.steps.append(MaterializationStep(op="RENAME_DATASET", sql=sql, reason=f"Dataset rename: {prev_table} -> {table_name}", safe=True))

  for a in relevant:
    if getattr(a, "action_type", None) != "RENAME_COLUMN":
      continue
    old = getattr(a, "previous_column_name", None)
    new = getattr(a, "column_name", None)
    if not old or not new:
      continue
    if _rename_column_already_applied(old_name=str(old), new_name=str(new)):
      res.warnings.append(
        "SCHEMA_ACTION_ALREADY_APPLIED: RENAME_COLUMN requested by MigrationPlan "
        f"for {dataset_key}.{old} -> {new}, but the physical column is "
        "already renamed. Skipping duplicate DDL."
      )
      continue
    sql = _render_required_schema_sql(
      res,
      dialect,
      "render_rename_column",
      "RENAME_COLUMN",
      dataset_key,
      schema_name,
      table_name,
      str(old),
      str(new),
    )
    if sql:
      res.steps.append(MaterializationStep(op="RENAME_COLUMN", sql=sql, reason=f"Rename column {old} -> {new}", safe=True))

  for a in relevant:
    if getattr(a, "action_type", None) != "ALTER_COLUMN":
      continue
    col_name = getattr(a, "column_name", None)
    if not col_name:
      continue
    sql = supported_alter_sql.get(str(col_name))
    if sql:
      res.steps.append(MaterializationStep(op="ALTER_COLUMN_TYPE", sql=sql, reason=f"alter {col_name} to <dialect_type>", safe=True))

  for a in relevant:
    if getattr(a, "action_type", None) != "ADD_COLUMN":
      continue
    col_name = getattr(a, "column_name", None)
    if not col_name:
      continue
    cols = _iter_target_columns(td)
    col_obj = next(
      (c for c in cols if str(getattr(c, "target_column_name", "")).lower() == str(col_name).lower()),
      None,
    )
    new_type = _physical_type_for(col_obj) if col_obj else None
    if not new_type:
      res.blocking_errors.append(f"MISSING_COLUMN_TYPE: {dataset_key}.{col_name}")
      continue

    if _physical_column_exists(str(col_name)):
      res.warnings.append(
        "SCHEMA_ACTION_ALREADY_APPLIED: ADD_COLUMN requested by MigrationPlan "
        f"for {dataset_key}.{col_name}, but the physical column already exists. "
        "Skipping duplicate DDL."
      )
      continue

    sql = dialect.render_add_column(schema_name, table_name, str(col_name), str(new_type))
    if sql:
      res.steps.append(MaterializationStep(op="ADD_COLUMN", sql=sql, reason=f"Column {col_name} missing", safe=True))

  for a in relevant:
    if getattr(a, "action_type", None) != "DROP_COLUMN":
      continue
    col_name = getattr(a, "column_name", None)
    if not col_name:
      continue
    if is_hist and not allow_hist_drop:
      continue
    if not allow_auto_drop:
      continue
    sql = dialect.render_drop_column(schema_name, table_name, str(col_name))
    if sql:
      res.steps.append(MaterializationStep(
        op="DROP_COLUMN",
        sql=sql,
        reason=f"Drop column {schema_name}.{table_name}.{col_name}",
        # Policy has already approved this destructive step.
        # Mark as safe so the applier will actually execute it.
        safe=True,
      ))

  return res