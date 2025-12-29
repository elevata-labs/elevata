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

import os

from metadata.materialization.plan import MaterializationPlan, MaterializationStep
from metadata.materialization.policy import MaterializationPolicy
from metadata.system.introspection import read_table_metadata
from sqlalchemy import inspect
from typing import Any

from google.cloud import bigquery


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

  table_name = getattr(td, "target_dataset_name", None) or getattr(td, "target_dataset_name", None)
  dataset_key = f"{schema_short}.{table_name}"

  plan = MaterializationPlan(
    dataset_key=dataset_key,
    steps=[],
    warnings=[],
    blocking_errors=[],
  )

  # raw/stage are rebuild-only by policy; we don't sync them here.
  if schema_short not in policy.sync_schema_shorts:
    plan.warnings.append(
      f"Materialization sync skipped for {dataset_key} (schema_short={schema_short}); rebuild-only by default."
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
        c.datatype,
        max_length=getattr(c, "max_length", None),
        precision=getattr(c, "decimal_precision", None),
        scale=getattr(c, "decimal_scale", None),
        strict=True,
      )
    desired_cols.append((c, col_name, col_type))

  actual_cols_by_name: dict[str, dict[str, Any]] = {}
  table_exists = False
  physical_table_for_introspection = table_name

  is_duckdb = getattr(getattr(introspection_engine, "dialect", None), "name", None) == "duckdb"
  is_bigquery = getattr(getattr(introspection_engine, "dialect", None), "name", None) in ("bigquery", "pybigquery")

  debug_plan = bool(getattr(policy, "debug_plan", False))

  def _bq_project_id() -> str | None:
    # Prefer exec_engine properties, then env. Keep best-effort only.
    # Best-effort: support multiple exec_engine implementations
    if exec_engine is None:
      return None
    pid = getattr(exec_engine, "project_id", None)
    if pid:
      return str(pid)
    client = getattr(exec_engine, "client", None)
    pid = getattr(client, "project", None) if client is not None else None
    if pid:
      return str(pid)
    pid = os.getenv("GOOGLE_CLOUD_PROJECT")
    return str(pid) if pid else None

  def _bq_client() -> Any | None:
    # BigQuery API client (google.cloud.bigquery.Client)
    if exec_engine is None:
      return None
    return getattr(exec_engine, "client", None)

  def _bq_table_exists_api(ds: str, tbl: str) -> bool:
    """
    Robust BigQuery existence check via API (avoids INFORMATION_SCHEMA scope issues).
    """
    client = _bq_client()
    if client is None:
      return False

    pid = _bq_project_id() or getattr(client, "project", None)
    if not pid or not ds or not tbl:
      return False

    table_ref = f"{pid}.{ds}.{tbl}"
    try:
      client.get_table(table_ref)
      return True
    except Exception:
      return False

  def _bq_columns_api(ds: str, tbl: str) -> list[tuple[str, str]]:
    """
    Return (column_name, data_type) from BigQuery API.
    data_type is normalized to lower-case canonical-ish strings (e.g. "string", "timestamp").
    """
    client = _bq_client()
    if client is None:
      return []
    pid = _bq_project_id() or getattr(client, "project", None)
    if not pid or not ds or not tbl:
      return []
    table_ref = f"{pid}.{ds}.{tbl}"
    try:
      t = client.get_table(table_ref)
    except Exception:
      return []
    cols: list[tuple[str, str]] = []
    for f in getattr(t, "schema", []) or []:
      # google.cloud.bigquery.SchemaField: name, field_type
      nm = getattr(f, "name", None)
      tp = getattr(f, "field_type", None)
      if nm and tp:
        cols.append((str(nm), str(tp).lower()))
    return cols

  def _introspect_cols(phys_table: str) -> bool:
    nonlocal actual_cols_by_name
    actual_cols_by_name = {}

    if is_duckdb:
      if exec_engine is None:
        plan.blocking_errors.append(
          f"DuckDB materialization requires exec_engine for introspection of {schema_name}.{phys_table}."
        )
        return False
      try:
        rows = exec_engine.fetch_all(f"PRAGMA table_info('{schema_name}.{phys_table}');", [])
        if not rows:
          # Debug hint (best-effort; never fail planning)
          if debug_plan:
            plan.warnings.append(
              f"DuckDB introspection: PRAGMA table_info('{schema_name}.{phys_table}') returned 0 rows "
              f"(table not found or empty result)."
            )
          return False
        for r in rows:
          nm = _norm_name(r[1])
          actual_cols_by_name[nm] = {"name": r[1], "type": _norm_type(r[2])}
        # Debug hint (best-effort; never fail planning)
        if debug_plan:
          plan.warnings.append(
            f"DuckDB introspection: PRAGMA table_info('{schema_name}.{phys_table}') -> "
            f"columns={len(actual_cols_by_name)}"
          )
        return True
      except Exception as exc:
        if debug_plan:
          plan.warnings.append(f"DuckDB introspection failed for {schema_name}.{phys_table}: {exc}")
        return False


    if is_bigquery:
      # BigQuery: SQLAlchemy inspector/reflection can be unreliable depending on driver.
      # Introspect via BigQuery API to avoid INFORMATION_SCHEMA quirks and fetch_all inconsistencies.
      if exec_engine is None:
        plan.blocking_errors.append(
          f"BigQuery materialization requires exec_engine for introspection of {schema_name}.{phys_table}."
        )
        return False
      try:
        ds = (schema_name or "").strip()
        tbl = (phys_table or "").strip()
        if not ds or not tbl:
          return False

        if not _bq_table_exists_api(ds, tbl):
          if debug_plan:
            plan.warnings.append(
              f"BigQuery introspection: table not found via API: {ds}.{tbl}"
            )
          return False

        cols = _bq_columns_api(ds, tbl)
        if not cols:
          if debug_plan:
            plan.warnings.append(
              f"BigQuery introspection: API returned 0 columns for {ds}.{tbl}"
            )
          return True

        for (col_name, col_type) in cols:
          nm = _norm_name(col_name)
          if nm:
            actual_cols_by_name[nm] = {"name": col_name, "type": _norm_type(col_type)}

        if debug_plan:
          plan.warnings.append(
            f"BigQuery introspection: API {ds}.{tbl} -> columns={len(actual_cols_by_name)}"
          )
        return True
      except Exception as exc:
        if debug_plan:
          plan.warnings.append(f"BigQuery introspection failed for {schema_name}.{phys_table}: {exc}")
        return False

    # neither duckdb nor bigquery: SQLAlchemy inspector + reflection
    try:
      insp = inspect(introspection_engine)
      exists = bool(insp.has_table(phys_table, schema=schema_name))
    except Exception:
      exists = False
    if not exists:
      if debug_plan:
        plan.warnings.append(
          f"Introspection: table not found via SQLAlchemy inspector: {schema_name}.{phys_table}"
        )
      return False
    try:
      meta = read_table_metadata(introspection_engine, schema_name, phys_table)
      for ac in meta.get("columns") or []:
        nm = _norm_name(ac.get("name") or ac.get("column_name"))
        if nm:
          actual_cols_by_name[nm] = ac
      if debug_plan:
        plan.warnings.append(
          f"Introspection: reflected {schema_name}.{phys_table} -> columns={len(actual_cols_by_name)}"
        )
    except Exception as exc:
      plan.blocking_errors.append(
        f"Reflection failed for existing table {schema_name}.{phys_table}: {exc}"
      )
      return False
    
    return True

  # First introspection attempt: desired table name
  table_exists = _introspect_cols(physical_table_for_introspection)

  if table_exists and not actual_cols_by_name:
    if debug_plan:
      plan.warnings.append(
        f"No columns returned for existing table {schema_name}.{table_name}; "
        f"planning ADD COLUMN for all desired columns."
      )

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
        if is_duckdb:
          # DuckDB: must use same exec_engine connection
          if exec_engine is None:
            return False
          try:
            rows = exec_engine.fetch_all(f"PRAGMA table_info('{schema_name}.{old_table}');", [])
            return bool(rows)
          except Exception:
            return False

        if is_bigquery:
          if exec_engine is None:
            return False
          try:
            ds = (schema_name or "").strip()
            tbl = (old_table or "").strip()
            if not ds or not tbl:
              return False
            return _bq_table_exists_api(ds, tbl)
          except Exception:
            return False

        # neither duckdb nor bigquery: SQLAlchemy inspector
        try:
          insp = inspect(introspection_engine)
          return bool(insp.has_table(old_table, schema=schema_name))
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
          table_exists = _introspect_cols(physical_table_for_introspection)
          break

        plan.blocking_errors.append(
          f"Dataset rename needed ({schema_name}.{fn} -> {schema_name}.{table_name}) "
          f"but dialect cannot render RENAME TABLE."
        )
        return plan

    # IMPORTANT:
    # Planner must NOT create missing tables. Table provisioning is handled centrally
    # by elevata_load via ensure_target_table() using the ExecutionEngine.
    if not table_exists:
      plan.warnings.append(
        f"Table {schema_name}.{table_name} does not exist. "
        f"Skipping CREATE TABLE in planner; expected ensure_target_table() to provision it."
      )
      return plan

  if table_exists and not actual_cols_by_name:
    plan.warnings.append(
      f"No columns returned for existing table {schema_name}.{physical_table_for_introspection}; "
      f"planning ADD COLUMN for all desired columns."
    )

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
        plan.warnings.append(
          f"Multiple former_names match physical columns for {schema_name}.{table_name}.{dc_name}: "
          f"{', '.join(names)}. Manual cleanup required."
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

        plan.blocking_errors.append(
          f"Column rename needed ({old_physical_name} -> {dc_name}) but dialect cannot render RENAME COLUMN."
        )
        continue

      if dc_type is None:
        plan.blocking_errors.append(
          f"Cannot determine column type for {schema_name}.{table_name}.{dc_name}; "
          f"ADD COLUMN not possible."
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
        plan.blocking_errors.append(
          f"Column {dc_name} missing but dialect cannot render ADD COLUMN."
        )
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
      plan.warnings.append(
        f"Duplicate physical columns detected for {schema_name}.{table_name}: "
        f"desired={dc_name} and former={old_physical_name}. Manual cleanup required."
      )
      continue

    at = _norm_type(actual.get("type"))
    dt = _norm_type(dc_type)

    # Postgres: reflection/introspection often returns "timestamp" for
    # columns that are semantically close to timestamptz (or legacy tables).
    # We don't auto-ALTER types in MVP, so treat timestamp <-> timestamptz
    # as equivalent to avoid noisy drift warnings.
    dialect_name = getattr(dialect, "DIALECT_NAME", None) or getattr(dialect, "dialect_name", None)
    if (dialect_name or "").lower() == "postgres":
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
      msg = f"Type mismatch for {schema_name}.{table_name}.{dc_name}: desired={dt}, actual={at}"
      # warn only (or block if you want strict)
      plan.warnings.append(msg)

  # Drops are intentionally not planned now (policy-gated later).
  return plan
