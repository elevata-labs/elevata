"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2026 Ilona Tag

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

from metadata.materialization.schema import ensure_target_schema
from metadata.system.introspection import read_table_metadata
from metadata.materialization.logging import LOAD_RUN_SNAPSHOT_REGISTRY


LOAD_RUN_SNAPSHOT_COLUMNS = list(LOAD_RUN_SNAPSHOT_REGISTRY.keys())

def build_load_run_snapshot_row(
  *,
  batch_run_id: str,
  created_at,
  root_dataset_key: str,
  is_execute: bool,
  continue_on_error: bool,
  max_retries: int,
  had_error: bool | None,
  step_count: int,
  snapshot_json: str,
) -> dict[str, object]:
  return {
    "batch_run_id": batch_run_id,
    "created_at": created_at,
    "root_dataset_key": root_dataset_key,
    "is_execute": bool(is_execute),
    "continue_on_error": bool(continue_on_error),
    "max_retries": int(max_retries),
    "had_error": had_error if had_error is None else bool(had_error),
    "step_count": int(step_count),
    "snapshot_json": snapshot_json,
  }


def ensure_load_run_snapshot_table(engine, dialect, meta_schema: str, auto_provision: bool) -> None:
  """
  Ensure meta.load_run_snapshot exists and contains all registry columns.
  Best-effort only: never blocks a run. No drops, no alter type.
  """
  if not auto_provision:
    return

  table_name = "load_run_snapshot"

  # Reuse the same canonical type mapping strategy used by load_run_log.
  # (Dialects already know how to map string/bool/int/timestamp via this pathway.)
  type_map = getattr(dialect, "LOAD_RUN_LOG_TYPE_MAP", None) or {}
  mapper = getattr(dialect, "map_load_run_log_type", None)

  # 1) Ensure schema exists
  try:
    ensure_target_schema(
      engine=engine,
      dialect=dialect,
      schema_name=meta_schema,
      auto_provision=True,
    )
  except Exception:
    return

  # 2) Introspect current table (same strategy as load_run_log)
  def _introspect() -> tuple[bool, set[str]]:
    """
    Return (table_exists, existing_columns_norm).
    Prefer dialect.introspect_table(exec_engine=engine). Fallback to SQLAlchemy metadata if available.
    """
    # Preferred: dialect-driven introspection
    try:
      if hasattr(dialect, "introspect_table"):
        res = dialect.introspect_table(
          schema_name=meta_schema,
          table_name=table_name,
          introspection_engine=None,
          exec_engine=engine,
          debug_plan=False,
        )
        table_exists = bool(res.get("table_exists"))
        cols_by_norm = dict(res.get("actual_cols_by_norm_name") or {})
        existing_norm = set(cols_by_norm.keys())
        return table_exists, existing_norm
    except Exception:
      pass

    # Fallback: SQLAlchemy metadata (best-effort)
    try:
      from metadata.system.introspection import read_table_metadata  # local import
      meta = read_table_metadata(engine, meta_schema, table_name)
      cols = []
      for c in (meta.get("columns") or []):
        if isinstance(c, dict) and c.get("name"):
          cols.append(str(c["name"]))
      existing_norm = {str(n).strip().lower() for n in cols if str(n).strip()}
      return (len(existing_norm) > 0), existing_norm
    except Exception:
      return False, set()

  table_exists, existing_columns_norm = _introspect()

  # 3) Create if missing
  if not table_exists:
    try:
      columns = []
      for col_name, spec in LOAD_RUN_SNAPSHOT_REGISTRY.items():
        canonical_type = spec["datatype"]
        if callable(mapper):
          physical_type = mapper(col_name, canonical_type)
        else:
          physical_type = type_map.get(canonical_type)

        if not physical_type:
          return

        columns.append({
          "name": col_name,
          "type": physical_type,
          "nullable": bool(spec.get("nullable", True)),
        })

      ddl = dialect.render_create_table_if_not_exists_from_columns(
        schema=meta_schema,
        table=table_name,
        columns=columns,
      )
      if ddl:
        engine.execute(ddl)
    except Exception:
      pass

    # Do NOT return here: continue best-effort with ADD missing columns.
    table_exists, existing_columns_norm = _introspect()

  # 4) Add missing columns
  for col_name, spec in LOAD_RUN_SNAPSHOT_REGISTRY.items():
    if str(col_name).strip().lower() in existing_columns_norm:
      continue
    try:
      canonical_type = spec["datatype"]
      if callable(mapper):
        physical_type = mapper(col_name, canonical_type)
      else:
        physical_type = type_map.get(canonical_type)

      if not physical_type:
        continue

      if hasattr(dialect, "render_add_column"):
        ddl = dialect.render_add_column(
          schema=meta_schema,
          table=table_name,
          column=col_name,
          column_type=physical_type,
        )
        if ddl:
          engine.execute(ddl)
    except Exception:
      # Never block due to meta schema evolution
      pass

def render_select_load_run_snapshot_json(
  *,
  dialect,
  meta_schema: str,
  batch_run_id: str,
) -> str:
  tbl = dialect.render_table_identifier(meta_schema, "load_run_snapshot")
  bid = dialect.literal(batch_run_id)
  # No LIMIT/TOP needed if batch_run_id is unique in practice.
  return f"SELECT snapshot_json FROM {tbl} WHERE batch_run_id = {bid}"

def fetch_one_value(engine, sql: str):
  """
  Best-effort fetch for unknown engine implementations.
  Returns first column of first row or None.
  """
  # Preferred: engines that support scalar reads (DuckDB already does).
  exec_scalar = getattr(engine, "execute_scalar", None)
  if callable(exec_scalar):
    try:
      return exec_scalar(sql)
    except Exception:
      return None

  # Next best: engines that can return rows
  fetch_all = getattr(engine, "fetch_all", None)
  if callable(fetch_all):
    try:
      rows = fetch_all(sql)
      if not rows:
        return None
      row = rows[0]
      if isinstance(row, dict):
        return row.get("snapshot_json") or next(iter(row.values()), None)
      if isinstance(row, (list, tuple)):
        return row[0] if row else None
      return getattr(row, "snapshot_json", row)
    except Exception:
      return None

  # Fallback: execute() typically returns rowcount, not rows, so this is best-effort only.
  try:
    res = engine.execute(sql)
  except Exception:
    return None

  return None