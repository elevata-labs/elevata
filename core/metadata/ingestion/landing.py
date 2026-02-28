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

import datetime
import json
from typing import Any

from metadata.ingestion.json_path import extract_json_path
from metadata.ingestion.normalization import normalize_param_value
from metadata.materialization.logging import ensure_load_run_log_table, build_load_run_log_row


def _now_utc():
  return datetime.datetime.now(datetime.timezone.utc)


def render_param_insert_sql(
  *,
  dialect,
  schema_name: str,
  table_name: str,
  target_columns: list[str],
) -> str:
  """
  Render a parameterized INSERT ... VALUES statement via the dialect.

  This helper centralizes the placeholder/VALUES shape and ensures that
  all SQL shape decisions remain in the dialect layer.
  """
  ph = dialect.param_placeholder()
  placeholders = ", ".join([ph] * len(target_columns))
  values_sql = f"({placeholders})"
  return dialect.render_insert_values_statement(
    schema_name,
    table_name,
    target_columns=target_columns,
    values_sql=values_sql,
  )


def land_raw_json_records(
  *,
  target_engine,
  target_dialect,
  td,
  records: list[dict[str, Any]],
  batch_run_id: str,
  load_run_id: str,
  target_system,
  profile,
  meta_schema: str = "meta",
  source_system_short_name: str | None = None,
  source_dataset_name: str | None = None,
  source_object: str | None = None,
  ingest_mode: str | None = None,
  chunk_size: int = 10_000,
  source_dataset=None,
  strict: bool = False,
  rebuild: bool = True,
  write_run_log: bool = True,
) -> dict[str, Any]:
  """
  Land JSON records into a RAW target dataset.

  RAW is treated as a transient landing zone:
    - ensure schema/table exist
    - truncate
    - insert payload rows (+ technical columns)
  """
  started_at = _now_utc()
  loaded_at = started_at

  if source_dataset is None:
    raise ValueError("source_dataset is required for RAW landing.")

  # Determine target columns present on the dataset
  tgt_cols = list(td.target_columns.all()) if hasattr(td, "target_columns") else []
  tgt_names = [getattr(c, "target_column_name", None) for c in tgt_cols]
  col_set = set([n for n in tgt_names if n])

  # RAW landing contract:
  # payload must always be present on RAW targets (preserved raw JSON).
  if "payload" not in col_set:
    raise ValueError(
      f"RAW landing requires a 'payload' TargetColumn on {td.target_schema.short_name}.{td.target_dataset_name}. "
      "Please re-run generation/migrations to include payload."
    )

  # Technical columns are system-managed on RAW (role-first, name fallback)
  TECH_ROLES = {"payload", "load_run_id", "loaded_at"}
  TECH_NAMES = {"payload", "load_run_id", "loaded_at"}
  tech_cols = []
  for c in tgt_cols:
    name = (getattr(c, "target_column_name", None) or "").strip()
    role = (getattr(c, "system_role", None) or "").strip()
    if role in TECH_ROLES or name in TECH_NAMES:
      if name:
        tech_cols.append(name)

  # Business columns are driven by SourceColumns (integrate=True)
  src_cols = list(source_dataset.source_columns.filter(integrate=True).order_by("ordinal_position"))
  src_by_name = {c.source_column_name: c for c in src_cols}

  business_cols: list[str] = []
  missing_paths: list[str] = []

  # Keep stable target-column order
  for c in tgt_cols:
    name = getattr(c, "target_column_name", None)
    if not name:
      continue
    if name in ("payload", "load_run_id", "loaded_at"):
      continue
    sc = src_by_name.get(name)
    if sc is None:
      continue
    if not getattr(sc, "json_path", None):
      missing_paths.append(name)
      continue
    business_cols.append(name)

  if missing_paths:
    msg = "Missing json_path for integrated SourceColumns: " + ", ".join(sorted(missing_paths))
    if strict:
      raise ValueError(msg)

  insert_cols: list[str] = []
  insert_cols.extend(business_cols)
  insert_cols.extend([c for c in tech_cols if c not in insert_cols])

  if rebuild:
    # Ensure log table exists
    ensure_load_run_log_table(
      engine=target_engine,
      dialect=target_dialect,
      meta_schema=meta_schema,
      auto_provision=True,
    )

    # Ensure RAW schema/table exist
    target_engine.execute(
      target_dialect.render_create_schema_if_not_exists(td.target_schema.schema_name)
    )

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

    target_engine.execute(
      target_dialect.render_truncate_table(
        schema=td.target_schema.schema_name,
        table=td.target_dataset_name,
      )
    )

  insert_sql = render_param_insert_sql(
    dialect=target_dialect,
    schema_name=td.target_schema.schema_name,
    table_name=td.target_dataset_name,
    target_columns=insert_cols,
  )

  rows_inserted = 0
  chunk: list[tuple] = []

  for rec in records:
    # If runtime ingestion provided an explicit original payload (e.g. file headers),
    # persist that verbatim in the payload column, but use normalized keys for flattening.
    payload_obj = rec.get("__payload__") if isinstance(rec, dict) else None
    if isinstance(payload_obj, dict):
      payload_json = json.dumps(payload_obj, ensure_ascii=False, default=str)
      extract_rec = {k: v for k, v in rec.items() if k != "__payload__"}
    else:
      payload_json = json.dumps(rec, ensure_ascii=False, default=str)
      extract_rec = rec

    values: list[Any] = []

    # Business columns from SourceColumns.json_path
    for col in business_cols:
      sc = src_by_name.get(col)
      jp = getattr(sc, "json_path", None)
      try:
        v = extract_json_path(extract_rec, str(jp))

      except Exception:
        if strict:
          raise
        v = None
      values.append(normalize_param_value(v))

    # Technical columns
    for tech in tech_cols:
      if tech == "payload":
        values.append(payload_json)
      elif tech == "load_run_id":
        values.append(load_run_id)
      elif tech == "loaded_at":
        values.append(normalize_param_value(loaded_at))
      else:
        values.append(None)
    chunk.append(tuple(values))

    if len(chunk) >= chunk_size:
      target_engine.execute_many(insert_sql, chunk)

      rows_inserted += len(chunk)
      chunk = []

  if chunk:
    target_engine.execute_many(insert_sql, chunk)

    rows_inserted += len(chunk)

  finished_at = _now_utc()

  # Write run log row (best-effort)
  if write_run_log:
    try:
      values = build_load_run_log_row(
        batch_run_id=batch_run_id,
        load_run_id=load_run_id,
        target_schema=td.target_schema.short_name,
        target_dataset=td.target_dataset_name,
        target_system=target_system.short_name,
        profile=profile.name,
        run_kind="ingestion",
        source_system=source_system_short_name,
        source_dataset=source_dataset_name,
        source_object=source_object,
        ingest_mode=ingest_mode,
        delta_cutoff=None,
        rows_extracted=rows_inserted,
        chunk_size=int(chunk_size),
        mode="full",
        handle_deletes=False,
        historize=False,
        started_at=started_at,
        finished_at=finished_at,
        render_ms=0.0,
        execution_ms=0.0,
        sql_length=0,
        rows_affected=rows_inserted,
        status="ok",
        error_message=None,
        attempt_no=1,
      )
      sql = target_dialect.render_insert_load_run_log(meta_schema=meta_schema, values=values)
      if sql:
        target_engine.execute(sql)

    except Exception:
      pass

  return {"rows_inserted": rows_inserted}
