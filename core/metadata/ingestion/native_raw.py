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

import time
import uuid
import datetime

from sqlalchemy import text

from metadata.rendering.dialects.dialect_factory import get_active_dialect
from metadata.ingestion.connectors import engine_for_source_system
from metadata.rendering.builder import qualify_source_filter
from metadata.rendering.placeholders import (
  resolve_delta_cutoff_for_source_dataset,
  apply_delta_cutoff_placeholder,
)
from metadata.models import TargetSchema, TargetDataset
from metadata.materialization.logging import build_load_run_log_row, ensure_load_run_log_table

META_SCHEMA = "meta"


def _now_utc():
  return datetime.datetime.utcnow()


def ingest_raw_full(
  *,
  source_dataset,
  target_system,
  profile,
  chunk_size: int = 5000,
  batch_run_id: str | None = None,
) -> dict[str, object]:
  """
  Extract data from SourceDataset into the RAW layer (table rebuild).

  Behavior:
  - Always (re)creates RAW schema/table if needed.
  - Always truncates the RAW table before inserting.
  - Extraction may be scoped:
    - Full extraction if no incremental filter applies.
    - Incremental-scoped extraction if SourceDataset.incremental is enabled and an increment_filter is set
      ({{DELTA_CUTOFF}} is resolved via the active increment policy for the current profile).
  - Inserts rows in chunks.
  - Writes a load_run_log entry (success/error).
  """
  batch_run_id = batch_run_id or str(uuid.uuid4())
  load_run_id = str(uuid.uuid4())

  # Resolve RAW target dataset from metadata
  raw_schema = TargetSchema.objects.get(short_name="raw")
  td = (
    TargetDataset.objects
    .filter(target_schema=raw_schema, source_datasets=source_dataset)
    .distinct()
    .first()
  )
  if td is None:
    raise ValueError(
      f"No RAW TargetDataset found for SourceDataset={source_dataset} "
      "Ensure generation ran and RAW landing is enabled."
    )

  target_dialect = get_active_dialect(target_system.type)
  target_engine = target_dialect.get_execution_engine(target_system)

  source_sa_engine = engine_for_source_system(
    system_type=source_dataset.source_system.type,
    short_name=source_dataset.source_system.short_name,
  )
  source_dialect = get_active_dialect(source_sa_engine.dialect.name)

  # Determine columns to extract (integrated source columns, ordered)
  src_cols_qs = source_dataset.source_columns.filter(integrate=True).order_by("ordinal_position")
  src_col_names = [c.source_column_name for c in src_cols_qs]

  if not src_col_names:
    return {
      "status": "skipped",
      "reason": "no_integrated_columns",
      "load_run_id": load_run_id,
      "batch_run_id": batch_run_id,
    }

  # Build SELECT against source
  src_schema = source_dataset.schema_name
  src_table = source_dataset.source_dataset_name

  src_select_cols = ", ".join(f"s.{source_dialect.render_identifier(c)}" for c in src_col_names)
  src_from = source_dialect.render_table_identifier(src_schema, src_table)

  # Use a stable alias for filtering
  src_sql = f"SELECT {src_select_cols} FROM {src_from} AS s"

  static_filter = (getattr(source_dataset, "static_filter", None) or "").strip()
  increment_filter = (getattr(source_dataset, "increment_filter", None) or "").strip()

  where_parts = []

  # Static filter applies to ingestion only (RAW extraction)
  if static_filter:
    where_parts.append(f"({qualify_source_filter(source_dataset, static_filter, source_alias='s')})")

  # Increment filter applies to ingestion only when dataset is incremental
  apply_increment = bool(getattr(source_dataset, "incremental", False) and increment_filter)
  if apply_increment:
    where_parts.append(f"({qualify_source_filter(source_dataset, increment_filter, source_alias='s')})")

  if where_parts:
    src_sql += " WHERE " + " AND ".join(where_parts)

  src_sql += ";"

  # Replace {{DELTA_CUTOFF}} for incremental extraction on the SOURCE side
  if apply_increment and "{{DELTA_CUTOFF" in src_sql:
    cutoff = resolve_delta_cutoff_for_source_dataset(
      source_dataset=source_dataset,
      profile=profile,
      now_ts=_now_utc(),
    )
    if cutoff is None:
      raise ValueError(
        f"increment_filter uses {{DELTA_CUTOFF}} but no active increment policy exists "
        f"for SourceDataset={source_dataset} in environment '{getattr(profile, 'name', None)}'."
      )

    # Important: use SOURCE dialect for rendering the literal
    src_sql = apply_delta_cutoff_placeholder(
      src_sql,
      dialect=source_dialect,
      delta_cutoff=cutoff,
    )

  # Build INSERT into RAW (DuckDB uses ? placeholders)
  # Target columns are derived from generated TargetColumns in RAW dataset.
  tgt_cols_qs = td.target_columns.filter(active=True).order_by("ordinal_position")
  tgt_cols = list(tgt_cols_qs)

  TECH_ROLES = {"load_run_id", "loaded_at"}
  TECH_NAMES = {"load_run_id", "loaded_at"}

  def _is_tech(col) -> bool:
    role = (col.system_role or "").strip()
    return (role in TECH_ROLES) or (col.target_column_name in TECH_NAMES)

  business_cols = [c for c in tgt_cols if not _is_tech(c)]
  tech_cols = [c for c in tgt_cols if _is_tech(c)]

  business_col_names = [c.target_column_name for c in business_cols]
  tech_col_names_found = [c.target_column_name for c in tech_cols]

  if len(src_col_names) != len(business_col_names):
    raise ValueError(
      f"Column mismatch for RAW ingestion: source has {len(src_col_names)} integrated columns "
      f"but target has {len(business_col_names)} business columns "
      f"(plus {len(tech_col_names_found)} technical columns)."
    )

  tech_col_names = [n for n in ("load_run_id", "loaded_at") if n in tech_col_names_found]
  insert_cols = business_col_names + tech_col_names

  ph = target_dialect.param_placeholder()
  placeholders = ", ".join([ph] * len(insert_cols))

  tgt_cols_sql = ", ".join(target_dialect.render_identifier(c) for c in insert_cols)
  tgt_table_sql = target_dialect.render_table_identifier(td.target_schema.schema_name, td.target_dataset_name)
  insert_sql = f"INSERT INTO {tgt_table_sql} ({tgt_cols_sql}) VALUES ({placeholders});"

  started_at = _now_utc()
  loaded_at = started_at
  t0 = time.time()

  rows_affected = 0
  err = None

  try:
    # Ensure meta logging table exists
    ensure_load_run_log_table(
      engine=target_engine,
      dialect=target_dialect,
      meta_schema=META_SCHEMA,
      auto_provision=True,
    )

    # Ensure RAW schema/table exist
    target_engine.execute(target_dialect.render_create_schema_if_not_exists(td.target_schema.schema_name))
    # RAW is a landing area and expected to evolve with the source schema.
    # For full ingests we prefer DROP+CREATE to avoid stale schemas (missing new columns).
    if hasattr(target_dialect, "render_drop_table_if_exists"):
      is_raw = (getattr(getattr(td, "target_schema", None), "short_name", None) or "").lower() == "raw"
      drop_sql = target_dialect.render_drop_table_if_exists(
        schema=td.target_schema.schema_name,
        table=td.target_dataset_name,
        cascade=is_raw,
      )
      if drop_sql:
        target_engine.execute(drop_sql)
    target_engine.execute(target_dialect.render_create_table_if_not_exists(td))

    # Truncate RAW table (RAW is always materialized as table)
    target_engine.execute(
      target_dialect.render_truncate_table(
        schema=td.target_schema.schema_name,
        table=td.target_dataset_name,
      )
    )

    # Stream source rows and insert into RAW in chunks
    with source_sa_engine.connect() as conn:
      result = conn.execute(text(src_sql))
      chunk = []

      for row in result:
        chunk.append(row)
        if len(chunk) >= chunk_size:
          params = []
          for r in chunk:
            values = list(tuple(r))
            for tech_name in tech_col_names:
              if tech_name == "load_run_id":
                values.append(load_run_id)
              elif tech_name == "loaded_at":
                values.append(loaded_at)
              else:
                values.append(None)
            params.append(tuple(values))

          target_engine.execute_many(insert_sql, params)
          rows_affected += len(chunk)
          chunk = []

      if chunk:
        params = []
        for r in chunk:
          values = list(tuple(r))
          for tech_name in tech_col_names:
            if tech_name == "load_run_id":
              values.append(load_run_id)
            elif tech_name == "loaded_at":
              values.append(loaded_at)
            else:
              values.append(None)
          params.append(tuple(values))

        target_engine.execute_many(insert_sql, params)
        rows_affected += len(chunk)

    finished_at = _now_utc()
    exec_ms = (time.time() - t0) * 1000.0

    summary = {
      "schema": td.target_schema.short_name,
      "dataset": td.target_dataset_name,
      "target_dataset_id": td.id,
      "mode": "full",
      "handle_deletes": False,
      "historize": False,
    }

    values = build_load_run_log_row(
      batch_run_id=batch_run_id,
      load_run_id=load_run_id,
      target_schema=td.target_schema.short_name,
      target_dataset=td.target_dataset_name,
      target_system=target_system.short_name,
      profile=profile.name,
      mode=str(summary.get("mode") or "full"),
      handle_deletes=bool(summary.get("handle_deletes") or False),
      historize=bool(summary.get("historize") or False),
      started_at=started_at,
      finished_at=finished_at,
      render_ms=0,
      execution_ms=exec_ms,
      sql_length=0,
      rows_affected=rows_affected,
      status="success",
      error_message=None,
    )
    log_sql = target_dialect.render_insert_load_run_log(meta_schema=META_SCHEMA, values=values)
    if log_sql:
      target_engine.execute(log_sql)

    return {
      "status": "success",
      "rows_affected": rows_affected,
      "load_run_id": load_run_id,
      "batch_run_id": batch_run_id,
      "target_dataset": td.target_dataset_name,
    }

  except Exception as e:
    err = str(e)
    finished_at = _now_utc()
    exec_ms = (time.time() - t0) * 1000.0

    summary = {
      "schema": td.target_schema.short_name,
      "dataset": td.target_dataset_name,
      "target_dataset_id": td.id,
      "mode": "full",
      "handle_deletes": False,
      "historize": False,
    }

    try:
      values = build_load_run_log_row(
        batch_run_id=batch_run_id,
        load_run_id=load_run_id,
        target_schema=td.target_schema.short_name,
        target_dataset=td.target_dataset_name,
        target_system=target_system.short_name,
        profile=profile.name,
        mode=str(summary.get("mode") or "full"),
        handle_deletes=bool(summary.get("handle_deletes") or False),
        historize=bool(summary.get("historize") or False),
        started_at=started_at,
        finished_at=finished_at,
        render_ms=0,
        execution_ms=exec_ms,
        sql_length=0,
        rows_affected=rows_affected,
        status="error",
        error_message=(err or "")[:1000],
      )
      log_sql = target_dialect.render_insert_load_run_log(meta_schema=META_SCHEMA, values=values)
      if log_sql:
        target_engine.execute(log_sql)

    except Exception:
      # Logging must never mask the original failure
      pass

    raise
