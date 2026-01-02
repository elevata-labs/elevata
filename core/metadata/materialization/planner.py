"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025 Ilona Tag

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

from metadata.materialization.plan import MaterializationPlan, MaterializationStep
from metadata.materialization.policy import MaterializationPolicy
from typing import Any


def _warn(plan: MaterializationPlan, code: str, msg: str) -> None:
  # Warnings are categorized via string prefixes for UX and testability.
  plan.warnings.append(f"{code}: {msg}")


def _block(plan: MaterializationPlan, code: str, msg: str) -> None:
  # Blocking errors are categorized via string prefixes for UX and testability.
  plan.blocking_errors.append(f"{code}: {msg}")


def _norm_name(name: str) -> str:
  return (name or "").strip().lower()


def _norm_type(t: Any) -> str:
  # SQLAlchemy types, MSSQL string types, etc.
  if t is None:
    return ""
  s = str(t).strip().lower()
  s = " ".join(s.split())
  # Normalize comma whitespace: "decimal(19, 4)" == "decimal(19,4)"
  s = s.replace(", ", ",")
  return s


def _synthetic_former_names_from_dataset(*, desired_col: str, dataset_name: str, dataset_former_names: list[str]) -> list[str]:
  """
  Generate "former column names" for convention-based columns when the dataset/table got renamed.
  Example:
    dataset_name = "rc_aw_product_mod"
    desired_col  = "rc_aw_product_mod_key"
    former ds    = ["rc_aw_product_model"]
    -> candidate = "rc_aw_product_model_key"
  This is especially important for surrogate keys and hist keys that include the dataset name.
  """
  desired_col_n = (desired_col or "").strip()
  dataset_name_n = (dataset_name or "").strip()
  if not desired_col_n or not dataset_name_n:
    return []
  if not desired_col_n.startswith(dataset_name_n):
    return []
  suffix = desired_col_n[len(dataset_name_n):]
  out: list[str] = []
  for old_ds in dataset_former_names or []:
    old_ds = (old_ds or "").strip()
    if not old_ds:
      continue
    out.append(f"{old_ds}{suffix}")
  return out


def build_materialization_plan(*, td, introspection_engine, exec_engine=None, dialect, policy: MaterializationPolicy) -> MaterializationPlan:

  schema_short = getattr(getattr(td, "target_schema", None), "short_name", None)
  schema_name = getattr(getattr(td, "target_schema", None), "schema_name", None)
  table_name = getattr(td, "target_dataset_name", None)
  dataset_key = f"{schema_short or '?'}." + (table_name or "?")

  plan = MaterializationPlan(
    dataset_key=dataset_key,
     steps=[],
     warnings=[],
     blocking_errors=[],
     requires_backfill=False,
   )

  if not schema_short or not schema_name or not table_name:
    _block(
      plan,
      "MISSING_SCHEMA_OR_TABLE",
      "Missing target schema/table metadata (schema_short/schema_name/table_name).",
    )
    return plan

  # raw/stage are rebuild-only by policy; we don't sync them here.
  if schema_short not in policy.sync_schema_shorts:
    _warn(
      plan,
      "MATERIALIZATION_SKIPPED",
      f"Sync skipped for {dataset_key} (schema_short={schema_short}); rebuild-only by default.",
    )
    return plan

  # Ensure schema (optional hook) - MUST happen before DuckDB PRAGMA introspection
  if hasattr(dialect, "render_create_schema_if_not_exists"):
    ddl = dialect.render_create_schema_if_not_exists(schema_name)
    if ddl:
      plan.steps.append(MaterializationStep(
        op="ENSURE_SCHEMA",
        sql=ddl,
        safe=True,
        reason=f"Ensure schema {schema_name} exists",
      ))

  # Desired columns from TargetColumn metadata
  desired_cols = []
  for c in td.target_columns.filter(active=True).order_by("ordinal_position", "id"):
    col_name = c.target_column_name
    col_type = None
    if hasattr(dialect, "map_logical_type"):
      col_type = dialect.map_logical_type(
        datatype=c.datatype,
        max_length=getattr(c, "max_length", None),
        precision=getattr(c, "decimal_precision", None),
        scale=getattr(c, "decimal_scale", None),
        strict=True,
      )
    desired_cols.append((c, col_name, col_type))

  actual_cols_by_name: dict[str, dict[str, Any]] = {}
  table_exists = False
  physical_table_for_introspection = table_name

  # Use dialect identity (not SQLAlchemy engine dialect name) for behavior that should
  # not depend on how introspection_engine is wired.
  dialect_name = getattr(dialect, "DIALECT_NAME", None) or getattr(dialect, "dialect_name", None)
  dialect_name_lc = (dialect_name or "").lower()
  is_duckdb = dialect_name_lc == "duckdb"
  is_bigquery = dialect_name_lc == "bigquery"

  debug_plan = bool(getattr(policy, "debug_plan", False))

  # Introspection (delegated to dialect)
  try:
    res = dialect.introspect_table(
      schema_name=schema_name,
      table_name=physical_table_for_introspection,
      introspection_engine=introspection_engine,
      exec_engine=exec_engine,
      debug_plan=debug_plan,
    )
  except Exception as exc:
    _warn(
      plan,
      "INTROSPECTION_FAILED",
      f"dialect={dialect_name_lc}: {schema_name}.{physical_table_for_introspection}: {exc}",
    )
    res = {"table_exists": False, "actual_cols_by_norm_name": {}}

  table_exists = bool(res.get("table_exists"))
  actual_cols_by_name = dict(res.get("actual_cols_by_norm_name") or {})

  # Ensure table exists (ONLY if missing)
  if not table_exists:

    # ------------------------------------------------------------
    # Dataset rename support (metadata-driven via td.former_names)
    #
    # If the desired table doesn't exist, but a former name exists
    # physically, prefer RENAME over CREATE to avoid duplicate tables.
    # ------------------------------------------------------------
    former_ds = list(getattr(td, "former_names", None) or [])
    former_ds_norm = [_norm_name(n) for n in former_ds if _norm_name(n)]

    # Guardrail: hist tables may only be renamed from hist-like former names.
    # Prevent accidental base -> hist renames if metadata is polluted.
    if isinstance(table_name, str) and table_name.endswith("_hist"):
      former_ds_norm = [n for n in former_ds_norm if isinstance(n, str) and n.endswith("_hist")]

    if former_ds_norm:
      def _old_table_exists(old_table: str) -> bool:
        try:
          r = dialect.introspect_table(
            schema_name=schema_name,
            table_name=old_table,
            introspection_engine=introspection_engine,
            exec_engine=exec_engine,
            debug_plan=debug_plan,
          )
          return bool(r.get("table_exists"))
        except Exception:
          return False

      for fn in former_ds_norm:
        if not fn:
          continue

        # Skip no-op renames (old == new)
        if _norm_name(fn) == _norm_name(table_name):
          continue

        if not _old_table_exists(fn):
          continue

        rename_sql = None
        if hasattr(dialect, "render_rename_table"):
          rename_sql = dialect.render_rename_table(schema_name, fn, table_name)

        if rename_sql:
          plan.steps.append(MaterializationStep(
            op="RENAME_DATASET",
            sql=rename_sql,
            safe=True,
            reason=f"Dataset rename detected via former_names: {fn} -> {table_name}",
          ))

          physical_table_for_introspection = fn
          # We will rename fn -> table_name during execution; but for planning column drift,
          # we must introspect the existing physical table (fn) now.
          try:
            res = dialect.introspect_table(
              schema_name=schema_name,
              table_name=physical_table_for_introspection,
              introspection_engine=introspection_engine,
              exec_engine=exec_engine,
              debug_plan=debug_plan,
            )
          except Exception as exc:
            _warn(
              plan,
              "INTROSPECTION_FAILED",
              f"dialect={dialect_name_lc}: {schema_name}.{physical_table_for_introspection}: {exc}",
            )
            res = {"table_exists": False, "actual_cols_by_norm_name": {}}
          table_exists = bool(res.get("table_exists"))
          actual_cols_by_name = dict(res.get("actual_cols_by_norm_name") or {})
          break

        _block(
          plan,
          "UNSUPPORTED_RENAME_DATASET",
          f"Dataset rename needed ({schema_name}.{fn} -> {schema_name}.{table_name}) but dialect cannot render RENAME TABLE.",
        )

        return plan

    # IMPORTANT:
    # Planner must NOT create missing tables. Table provisioning is handled centrally
    # by elevata_load via ensure_target_table() using the ExecutionEngine.
    if not table_exists:
      _warn(
        plan,
        "MISSING_TABLE",
        f"Table {schema_name}.{table_name} does not exist. "
        f"Skipping CREATE TABLE in planner; expected ensure_target_table() to provision it.",
      )
      return plan

  if table_exists and not actual_cols_by_name:
    _warn(
      plan,
      "NO_COLUMNS_RETURNED",
      f"No columns returned for existing table {schema_name}.{physical_table_for_introspection}; "
      f"planning ADD COLUMN for all desired columns.",
    )

    # Do not return: treat as "no columns known", so the loop below will plan ADD_COLUMN
    # for all desired columns (best-effort).

  # Compare columns: add missing, warn on type mismatch
  for (col_obj, dc_name, dc_type) in desired_cols:
    key = _norm_name(dc_name)
    actual = actual_cols_by_name.get(key)
    if not actual:
      # Try rename based on former_names (metadata-driven)
      former = list(getattr(col_obj, "former_names", None) or [])

      # Also derive synthetic former names when the table was renamed (convention-based key columns).
      ds_former = list(getattr(td, "former_names", None) or [])
      former += _synthetic_former_names_from_dataset(
        desired_col=dc_name,
        dataset_name=table_name,
        dataset_former_names=ds_former,
      )

      former_norm = [_norm_name(n) for n in former if _norm_name(n) and _norm_name(n) != key]
      # De-dupe former_norm (order-preserving) to avoid false "multiple matches"
      seen = set()
      former_norm_dedup = []
      for fn in former_norm:
        if fn not in seen:
          seen.add(fn)
          former_norm_dedup.append(fn)
      former_norm = former_norm_dedup

      rename_from_actual = None
      rename_candidates = []
      for fn in former_norm:
        if fn and fn in actual_cols_by_name:
          rename_candidates.append(actual_cols_by_name[fn])

      if len(rename_candidates) > 1:
        names = [
          (c.get("name") or c.get("column_name") or "?")
          for c in rename_candidates
        ]
        _warn(
          plan,
          "AMBIGUOUS_RENAME",
          f"Multiple former_names match physical columns for {schema_name}.{table_name}.{dc_name}: "
          f"{', '.join(names)}. Manual cleanup required.",
        )
        continue

      if len(rename_candidates) == 1:
        rename_from_actual = rename_candidates[0]

      if rename_from_actual:
        old_physical_name = rename_from_actual.get("name") or rename_from_actual.get("column_name") or ""

        rename_sql = None
        if hasattr(dialect, "render_rename_column"):
          rename_sql = dialect.render_rename_column(schema_name, table_name, old_physical_name, dc_name)

        if rename_sql:
          plan.steps.append(MaterializationStep(
            op="RENAME_COLUMN",
            sql=rename_sql,
            safe=True,
            reason=f"Rename column {old_physical_name} -> {dc_name} (former name match)",
          ))
          plan.requires_backfill = True
          continue

        _block(
          plan,
          "UNSUPPORTED_RENAME_COLUMN",
          f"Column rename needed ({old_physical_name} -> {dc_name}) but dialect cannot render RENAME COLUMN.",
        )

        continue

      if dc_type is None:
        _block(
          plan,
          "UNKNOWN_COLUMN_TYPE",
          f"Cannot determine column type for {schema_name}.{table_name}.{dc_name}; ADD COLUMN not possible.",
        )

        plan.steps.append(MaterializationStep(
          op="BLOCK",
          sql=None,
          safe=False,
          reason=f"Missing column {dc_name} has unknown type",
        ))
        continue
      
      add_sql = None
      if hasattr(dialect, "render_add_column"):
        add_sql = dialect.render_add_column(schema_name, table_name, dc_name, dc_type)
      if add_sql:
        plan.steps.append(MaterializationStep(
          op="ADD_COLUMN",
          sql=add_sql,
          safe=True,
          reason=f"Column {dc_name} missing",
        ))
        plan.requires_backfill = True
      else:
        _block(plan, "UNSUPPORTED_ADD_COLUMN", f"Column {dc_name} missing but dialect cannot render ADD COLUMN.")

        plan.steps.append(MaterializationStep(
          op="BLOCK",
          sql=None,
          safe=False,
          reason=f"Missing column {dc_name} cannot be added automatically",
        ))
      continue

    # Type check (best-effort)
    # If the desired column exists physically, but any former_names ALSO exist physically,
    # do not auto-fix. This indicates historical duplicates and requires manual cleanup.
    former = list(getattr(col_obj, "former_names", None) or [])
    former_norm = [_norm_name(n) for n in former if _norm_name(n) and _norm_name(n) != key]
    dup = None
    for fn in former_norm:
      if fn in actual_cols_by_name:
        dup = actual_cols_by_name[fn]
        break
    if dup:
      old_physical_name = dup.get("name") or dup.get("column_name") or ""
      _warn(
        plan,
        "DUPLICATE_COLUMN",
        f"Duplicate physical columns detected for {schema_name}.{table_name}: "
        f"desired={dc_name} and former={old_physical_name}. Manual cleanup required.",
      )
      continue

    at = _norm_type(actual.get("type"))
    dt = _norm_type(dc_type)

    # Boolean aliases across dialects (e.g. BigQuery returns BOOLEAN)
    bool_aliases = {"bool", "boolean"}
    if dt in bool_aliases and at in bool_aliases:
      continue

    # Postgres: reflection/introspection often returns "timestamp" for
    # columns that are semantically close to timestamptz (or legacy tables).
    # We don't auto-ALTER types in MVP, so treat timestamp <-> timestamptz
    # as equivalent to avoid noisy drift warnings.
    if dialect_name_lc == "postgres":
      if {dt, at} == {"timestamp", "timestamptz"}:
        continue

    # DuckDB does not reliably expose varchar length constraints via information_schema,
    # so treat VARCHAR and VARCHAR(n) as equivalent for drift detection.
    if is_duckdb:
      if (
        (dt.startswith("varchar(") and at == "varchar") or
        (dt.startswith("char(") and at == "char")
      ):
        continue

    # BigQuery: introspection may return INTEGER while desired type is INT64.
    # Treat these as equivalent to avoid noisy drift warnings.
    if is_bigquery:
      bq_int_aliases = {"int64", "integer", "int"}
      if dt in bq_int_aliases and at in bq_int_aliases:
        continue

    if dt and at and dt != at:
      _warn(
        plan,
        "TYPE_DRIFT",
        f"Type mismatch for {schema_name}.{table_name}.{dc_name}: desired={dt}, actual={at}",
      )

  # Drops are intentionally not planned now (policy-gated later).
  return plan
