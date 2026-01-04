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

"""
Canonical schema for meta.load_run_log.

This registry defines the single source of truth for load execution logging
across all target systems and SQL dialects.

Notes:
- Types are canonical (string, bool, int, timestamp)
- Dialects are responsible for mapping canonical types to physical types
- This schema is allowed to change before v1.0 (breaking changes OK)
"""

from metadata.materialization.schema import ensure_target_schema
from metadata.system.introspection import read_table_metadata

LOAD_RUN_LOG_REGISTRY = {
  # ------------------------------------------------------------------
  # Identity & context
  # ------------------------------------------------------------------
  "batch_run_id": {
    "datatype": "string",
    "nullable": False,
    "role": "identifier",
    "description": "Logical batch identifier spanning multiple dataset loads",
  },
  "load_run_id": {
    "datatype": "string",
    "nullable": False,
    "role": "identifier",
    "description": "Unique identifier for this dataset load execution",
  },
  "target_schema": {
    "datatype": "string",
    "nullable": False,
    "description": "Logical target schema short name (e.g. rawcore, stage)",
  },
  "target_dataset": {
    "datatype": "string",
    "nullable": False,
    "description": "Target dataset name within the schema",
  },
  "target_system": {
    "datatype": "string",
    "nullable": False,
    "description": "Target system short name (e.g. dwh)",
  },
  "profile": {
    "datatype": "string",
    "nullable": False,
    "description": "Execution profile/environment name",
  },

  # ------------------------------------------------------------------
  # Load semantics
  # ------------------------------------------------------------------
  "mode": {
    "datatype": "string",
    "nullable": False,
    "description": "Load mode (full / incremental)",
  },
  "handle_deletes": {
    "datatype": "bool",
    "nullable": False,
    "description": "Whether delete handling was enabled for this load",
  },
  "historize": {
    "datatype": "bool",
    "nullable": False,
    "description": "Whether historization (SCD2) was active",
  },

  # ------------------------------------------------------------------
  # Timing & metrics
  # ------------------------------------------------------------------
  "started_at": {
    "datatype": "timestamp",
    "nullable": False,
    "description": "Timestamp when rendering/execution started",
  },
  "finished_at": {
    "datatype": "timestamp",
    "nullable": True,
    "description": "Timestamp when execution finished (null if not executed)",
  },
  "render_ms": {
    "datatype": "int",
    "nullable": True,
    "description": "Time spent rendering SQL (milliseconds)",
  },
  "execution_ms": {
    "datatype": "int",
    "nullable": True,
    "description": "Time spent executing SQL (milliseconds)",
  },
  "sql_length": {
    "datatype": "int",
    "nullable": True,
    "description": "Rendered SQL length in characters",
  },
  "rows_affected": {
    "datatype": "int",
    "nullable": True,
    "description": "Rows affected by execution (if available)",
  },

  # ------------------------------------------------------------------
  # Outcome
  # ------------------------------------------------------------------
  "status": {
    "datatype": "string",
    "nullable": False,
    "description": "Execution status (success / error / skipped / dry_run)",
  },
  "error_message": {
    "datatype": "string",
    "nullable": True,
    "description": "Error message if execution failed",
  },
  "attempt_no": {
    "datatype": "int",
    "nullable": False,
    "description": "Attempt counter for this dataset execution within the batch run (1..N)",
  },
  "status_reason": {
    "datatype": "string",
    "nullable": True,
    "description": "Short machine-readable reason for status (e.g. blocked_by_dependency, retry_exhausted)",
  },
  "blocked_by": {
    "datatype": "string",
    "nullable": True,
    "description": "Upstream dataset_key that blocked this run (if status=skipped)",
  },
}

LOAD_RUN_SNAPSHOT_REGISTRY = {
  "batch_run_id": {
    "datatype": "string",
    "nullable": False,
    "description": "Logical batch identifier of this execution",
  },
  "created_at": {
    "datatype": "timestamp",
    "nullable": False,
    "description": "Snapshot creation timestamp",
  },
  "root_dataset_key": {
    "datatype": "string",
    "nullable": False,
    "description": "Root dataset of this execution (schema.dataset)",
  },
  "is_execute": {
     "datatype": "bool",
     "nullable": False,
    "description": "Whether this load was executed (true) or dry-run (false)",
  },
  "continue_on_error": {
    "datatype": "bool",
    "nullable": False,
    "description": "Execution policy: continue on error",
  },
  "max_retries": {
    "datatype": "int",
    "nullable": False,
    "description": "Maximum retries per dataset",
  },
  "had_error": {
    "datatype": "bool",
    "nullable": False,
    "description": "Whether any dataset errored during execution",
  },
  "step_count": {
    "datatype": "int",
    "nullable": False,
    "description": "Number of execution steps in the plan",
  },
  "snapshot_json": {
    "datatype": "string",
    "nullable": False,
    "description": "Full execution snapshot as JSON",
  },
}

# Stable column order for CREATE/INSERT.
LOAD_RUN_LOG_COLUMNS = list(LOAD_RUN_LOG_REGISTRY.keys())

def build_load_run_log_row(
  *,
  batch_run_id: str,
  load_run_id: str,
  target_schema: str,
  target_dataset: str,
  target_system: str,
  profile: str,
  mode: str,
  handle_deletes: bool,
  historize: bool,
  started_at,
  finished_at,
  render_ms: float | None,
  execution_ms: float | None,
  sql_length: int | None,
  rows_affected: int | None,
  status: str,
  error_message: str | None,
  attempt_no: int = 1,
  status_reason: str | None = None,
  blocked_by: str | None = None,
) -> dict[str, object]:
  # Centralized normalization for meta.load_run_log inserts.
  return {
    "batch_run_id": batch_run_id,
    "load_run_id": load_run_id,
    "target_schema": target_schema,
    "target_dataset": target_dataset,
    "target_system": target_system,
    "profile": profile,
    "mode": mode,
    "handle_deletes": bool(handle_deletes),
    "historize": bool(historize),
    "started_at": started_at,
    "finished_at": finished_at,
    "render_ms": int(render_ms) if render_ms is not None else None,
    "execution_ms": int(execution_ms) if execution_ms is not None else None,
    "sql_length": int(sql_length) if sql_length is not None else None,
    "rows_affected": int(rows_affected) if rows_affected is not None else None,
    "status": status,
    "error_message": error_message,
    "attempt_no": int(attempt_no),
    "status_reason": status_reason,
    "blocked_by": blocked_by,
  }

def _introspect_existing_columns(
  *,
  engine,
  dialect,
  schema_name: str,
  table_name: str,
) -> tuple[bool, set[str]]:
  """
  Return (table_exists, existing_column_names_normalized).

  Prefer dialect.introspect_table(exec_engine=...) because our execution engines
  are not necessarily SQLAlchemy engines.
  Fallback to read_table_metadata only if available and needed.
  """
  # 1) Preferred path: dialect-driven introspection (exec_engine based)
  try:
    if hasattr(dialect, "introspect_table"):
      res = dialect.introspect_table(
        schema_name=schema_name,
        table_name=table_name,
        introspection_engine=None,
        exec_engine=engine,
        debug_plan=False,
      )
      table_exists = bool(res.get("table_exists"))
      cols_by_norm = dict(res.get("actual_cols_by_norm_name") or {})

      # cols_by_norm keys are already normalized in our dialect implementations.
      existing_norm = set(cols_by_norm.keys())
      return table_exists, existing_norm
  except Exception:
    # Best-effort only: fall through to fallback
    pass

  # 2) Fallback path: SQLAlchemy-based metadata (only if the environment supports it)
  try:
    from metadata.system.introspection import read_table_metadata  # local import to avoid hard dependency
    meta = read_table_metadata(engine, schema_name, table_name)
    cols = []
    for c in (meta.get("columns") or []):
      if isinstance(c, dict) and c.get("name"):
        cols.append(str(c["name"]))
    existing_norm = {str(n).strip().lower() for n in cols if str(n).strip()}
    return (len(existing_norm) > 0), existing_norm
  except Exception:
    return False, set()

def ensure_load_run_log_table(engine, dialect, meta_schema: str, auto_provision: bool) -> None:
  """
  Ensure that the warehouse-level meta.load_run_log table exists and
  contains all canonical columns from LOAD_RUN_LOG_REGISTRY.

  Best-effort only: must never block a load.
  No DROP or ALTER TYPE operations are performed.
  """
  if not auto_provision:
    return

  table_name = "load_run_log"
  type_map = getattr(dialect, "LOAD_RUN_LOG_TYPE_MAP", None) or {}
  mapper = getattr(dialect, "map_load_run_log_type", None)

  # ------------------------------------------------------------------
  # 1) Ensure meta schema exists (best-effort)
  # ------------------------------------------------------------------
  try:
    ensure_target_schema(
      engine=engine,
      dialect=dialect,
      schema_name=meta_schema,
      auto_provision=True,
    )
  except Exception:
    return

  # ------------------------------------------------------------------
  # 2) Introspect table metadata (best-effort)
  # ------------------------------------------------------------------
  table_exists, existing_columns_norm = _introspect_existing_columns(
    engine=engine, dialect=dialect, schema_name=meta_schema, table_name=table_name
  )

  # ------------------------------------------------------------------
  # 3) Table does not exist → CREATE
  # ------------------------------------------------------------------
  if not table_exists:
    try:
      columns = []

      for col_name, spec in LOAD_RUN_LOG_REGISTRY.items():
        canonical_type = spec["datatype"]
        if callable(mapper):
          physical_type = mapper(col_name, canonical_type)
        else:
          physical_type = type_map.get(canonical_type)

        if not physical_type:
          # If the dialect cannot map types, skip provisioning entirely.
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
      # Never block a load due to logging DDL
      pass

    # Important: do NOT return here.
    # In non-SQLAlchemy environments, introspection may report table_exists=False
    # even if the table exists (or CREATE IF NOT EXISTS is a no-op).
    # Continue best-effort with "ADD missing columns".
    table_exists, existing_columns_norm = _introspect_existing_columns(
      engine=engine, dialect=dialect, schema_name=meta_schema, table_name=table_name
    )

  # ------------------------------------------------------------------
  # 4) Table exists → ADD missing columns
  # ------------------------------------------------------------------
  for col_name, spec in LOAD_RUN_LOG_REGISTRY.items():
    # Compare normalized names (case/quoting independent)
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

      ddl = dialect.render_add_column(
        schema=meta_schema,
        table=table_name,
        column=col_name,
        column_type=physical_type,
      )
      if ddl:
        engine.execute(ddl)
    except Exception:
      # Best-effort only
      pass
