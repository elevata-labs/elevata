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
from typing import Iterable

from sqlalchemy import text

from metadata.rendering.dialects.dialect_factory import get_active_dialect
from metadata.ingestion.connectors import engine_for_source_system
from metadata.models import TargetSchema, TargetDataset

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
  Full-load ingestion: SourceDataset -> RAW TargetDataset on the given target_system.

  - Ensures RAW table exists
  - Truncates RAW table
  - Inserts all rows from source in chunks
  - Writes a load_run_log entry (success/error)
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

  src_select_cols = ", ".join(source_dialect.render_identifier(c) for c in src_col_names)
  src_from = source_dialect.render_table_identifier(src_schema, src_table)
  src_sql = f"SELECT {src_select_cols} FROM {src_from};"

  # Build INSERT into RAW (DuckDB uses ? placeholders)
  # Target columns are derived from generated TargetColumns in RAW dataset.
  tgt_cols_qs = td.target_columns.filter(active=True).order_by("ordinal_position")

  tgt_col_names = [c.target_column_name for c in tgt_cols_qs]

  if len(src_col_names) != len(tgt_col_names):
    raise ValueError(
      f"Column mismatch for RAW ingestion: source has {len(src_col_names)} integrated columns "
      f"but target has {len(tgt_col_names)} active columns."
    )

  insert_cols = tgt_col_names

  ph = target_dialect.param_placeholder()
  placeholders = ", ".join([ph] * len(insert_cols))

  tgt_cols_sql = ", ".join(target_dialect.render_identifier(c) for c in insert_cols)
  tgt_table_sql = target_dialect.render_table_identifier(td.target_schema.schema_name, td.target_dataset_name)
  insert_sql = f"INSERT INTO {tgt_table_sql} ({tgt_cols_sql}) VALUES ({placeholders});"

  started_at = _now_utc()
  t0 = time.time()

  rows_affected = 0
  err = None

  try:
    # Ensure meta logging table exists
    target_engine.execute(target_dialect.render_create_load_run_log_if_not_exists(META_SCHEMA))

    # Ensure RAW schema/table exist
    target_engine.execute(target_dialect.render_create_schema_if_not_exists(td.target_schema.schema_name))
    target_engine.execute(target_dialect.render_create_table_if_not_exists(td))

    # Full-load semantics
    target_engine.execute(
      target_dialect.render_truncate_table(schema=td.target_schema.schema_name, table=td.target_dataset_name)
    )

    # Extract + load
    with source_sa_engine.connect() as conn:
      result = conn.execute(text(src_sql))
      while True:
        chunk = result.fetchmany(chunk_size)
        if not chunk:
          break

        params = [tuple(row) for row in chunk]
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

    log_sql = target_dialect.render_insert_load_run_log(
      meta_schema=META_SCHEMA,
      batch_run_id=batch_run_id,
      load_run_id=load_run_id,
      summary=summary,
      profile=profile,
      system=target_system,
      started_at=started_at,
      finished_at=finished_at,
      render_ms=0.0,
      execution_ms=exec_ms,
      sql_length=0,
      rows_affected=rows_affected,
      load_status="success",
      error_message=None,
    )
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
      log_sql = target_dialect.render_insert_load_run_log(
        meta_schema=META_SCHEMA,
        batch_run_id=batch_run_id,
        load_run_id=load_run_id,
        summary=summary,
        profile=profile,
        system=target_system,
        started_at=started_at,
        finished_at=finished_at,
        render_ms=0.0,
        execution_ms=exec_ms,
        sql_length=0,
        rows_affected=rows_affected,
        load_status="error",
        error_message=err[:1000],
      )
      target_engine.execute(log_sql)
    except Exception:
      # Logging must never mask the original failure
      pass

    raise
