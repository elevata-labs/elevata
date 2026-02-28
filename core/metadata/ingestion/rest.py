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

import os
import json
import urllib.parse
import urllib.request
import urllib.error
import time
from datetime import datetime, timezone
from typing import Any

from metadata.execution.load_run_snapshot_store import (
  build_load_run_snapshot_row,
  ensure_load_run_snapshot_table,
  fetch_one_value,
  render_select_latest_load_run_snapshot_json_by_root_key,
)
from metadata.ingestion.connectors import rest_config_for_source_system
from metadata.ingestion.landing import land_raw_json_records
from metadata.ingestion.validation import (
  parse_rest_validation_config,
  parse_rest_retry_config,
  validate_required_keys,
  validate_schema_drift,
  validate_row_count,
  schema_signature,
)
from metadata.ingestion.normalization import normalize_records_keep_payload


def _utc_now():
  return datetime.now(timezone.utc)


def _json_get_with_retry(url: str, headers: dict[str, str], rcfg) -> Any:
  """
  GET JSON with retry/backoff for transient HTTP errors.
  """
  attempts = 0
  last_exc: Exception | None = None
  retry_on = set(int(x) for x in (rcfg.retry_on_status or []))

  while attempts < int(rcfg.max_attempts or 1):
    attempts += 1
    try:
      req = urllib.request.Request(url, headers=headers or {})
      with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read()
      return json.loads(raw.decode("utf-8"))
    except urllib.error.HTTPError as exc:
      last_exc = exc
      status = int(getattr(exc, "code", 0) or 0)
      if status not in retry_on or attempts >= int(rcfg.max_attempts or 1):
        raise
      time.sleep(float(rcfg.backoff_seconds or 0) * attempts)
    except Exception as exc:
      last_exc = exc
      if attempts >= int(rcfg.max_attempts or 1):
        raise
      time.sleep(float(rcfg.backoff_seconds or 0) * attempts)

  if last_exc:
    raise last_exc
  raise RuntimeError("REST request failed without exception")


def _extract_records(payload: Any, record_path: str | None) -> list[dict[str, Any]]:
  """
  Extract record list from JSON payload.
  record_path uses dotted notation, e.g. 'data.items'.
  If record_path is None: payload must be a list.
  """
  if record_path:
    cur = payload
    for part in record_path.split("."):
      if cur is None:
        break
      if isinstance(cur, dict):
        cur = cur.get(part)
      else:
        cur = None
    payload = cur

  if payload is None:
    return []
  if isinstance(payload, list):
    return [r for r in payload if isinstance(r, dict)]
  if isinstance(payload, dict):
    # Allow single-object responses.
    return [payload]
  return []


def ingest_raw_rest(
  *,
  source_dataset,
  td,
  target_system,
  dialect,
  profile,
  batch_run_id: str,
  load_run_id: str,
  meta_schema: str = "meta",
  max_pages: int = 10_000,
  chunk_size: int = 10_000,
) -> dict[str, Any]:
  """
  REST ingestion (JSON array).

  System secret (type=rest, short_name=<system.short_name>):
    {
      "base_url": "https://api.example.com",
      "headers": {...},
      "query": {...}
    }

  SourceDataset.ingestion_config:
    {
      "path": "/v1/items",
      "query": {"since": "{{CURSOR}}"},
      "record_path": "data.items",
      "cursor": {
        "type": "page_token",
        "request_param": "pageToken",
        "response_field": "nextPageToken"
      }
    }
  """
  sys = getattr(source_dataset, "source_system", None)
  if not sys:
    raise ValueError("source_dataset.source_system is required")

  cfg = getattr(source_dataset, "ingestion_config", None) or {}
  if not isinstance(cfg, dict):
    raise ValueError("source_dataset.ingestion_config must be a JSON object")
  
  vcfg = parse_rest_validation_config(cfg)
  rcfg = parse_rest_retry_config(cfg)

  # RAW landing uses SourceColumns.json_path as the single source of truth.
  # Any mapping configuration is handled via imported SourceColumns, not ingestion_config.  

  path = str(cfg.get("path") or "").strip()
  path = os.path.expandvars(path)
  if not path:
    raise ValueError("REST ingestion requires ingestion_config.path")

  record_path = str(cfg.get("record_path") or "").strip() or None
  query_tpl = dict(cfg.get("query") or {})
  cursor_cfg = dict(cfg.get("cursor") or {})
  cursor_type = str(cursor_cfg.get("type") or "page_token").strip().lower()

  sys_rest = rest_config_for_source_system(system_type=str(sys.type), short_name=str(sys.short_name))
  base_url_val = sys_rest.get("base_url") if isinstance(sys_rest, dict) else sys_rest
  # Support secrets where base_url is nested (e.g. {"base_url": {"base_url": "https://..."}})
  if isinstance(base_url_val, dict):
    base_url_val = base_url_val.get("base_url")
  base_url = os.path.expandvars(str(base_url_val or "")).rstrip("/")
  headers = dict(sys_rest.get("headers") or {})
  # Some public APIs reject requests without a User-Agent.
  headers.setdefault("User-Agent", "elevata/ingestion")
  headers.setdefault("Accept", "application/json")
  fixed_query = dict(sys_rest.get("query") or {})

  if not base_url:
    raise ValueError(f"REST system secret must provide base_url for {sys.short_name}")

  # Cursor snapshot key
  root_key = f"ingestion:{sys.short_name}:{source_dataset.source_dataset_name}"

  # Warehouse engine (target) for snapshot/log writes + RAW landing
  # Use dialect execution engine (works consistently across supported warehouses).
  target_engine = dialect.get_execution_engine(target_system)

  # Ensure snapshot table exists (best-effort)
  ensure_load_run_snapshot_table(engine=target_engine, dialect=dialect, meta_schema=meta_schema, auto_provision=True)

  # Load last cursor (best-effort)
  cursor_state: dict[str, Any] = {}
  try:
    sel = render_select_latest_load_run_snapshot_json_by_root_key(
      dialect=dialect,
      meta_schema=meta_schema,
      root_dataset_key=root_key,
    )
    raw = fetch_one_value(target_engine, sel)
    if raw:
      cursor_state = json.loads(raw) if isinstance(raw, str) else {}
  except Exception:
    cursor_state = {}

  next_cursor = cursor_state.get("cursor")
  pages = 0
  all_rows: list[dict[str, Any]] = []
  empty_pages = 0
  schema_sigs: set[tuple[str, ...]] = set()

  while pages < max_pages:
    pages += 1

    q = {}
    q.update(fixed_query)
    q.update({k: v for k, v in query_tpl.items()})

    # Substitute cursor placeholder in query template
    for k, v in list(q.items()):
      if isinstance(v, str) and "{{CURSOR}}" in v:
        q[k] = v.replace("{{CURSOR}}", "" if next_cursor is None else str(next_cursor))

    # Pagination cursor parameter
    if cursor_type == "page_token":
      req_param = str(cursor_cfg.get("request_param") or "pageToken")
      if next_cursor:
        q[req_param] = str(next_cursor)
    elif cursor_type == "offset":
      req_param = str(cursor_cfg.get("request_param") or "offset")
      q[req_param] = int(next_cursor or 0)

    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
    if q:
      url = f"{url}?{urllib.parse.urlencode(q, doseq=True)}"

    payload = _json_get_with_retry(url, headers, rcfg)

    rows = _extract_records(payload, record_path)
    if len(rows) == 0:
      empty_pages += 1
      if empty_pages > int(vcfg.max_empty_pages or 3):
        msg = f"REST ingestion aborted: too many empty pages (>{vcfg.max_empty_pages})"
        if vcfg.strict:
          raise ValueError(msg)
        break
    else:
      empty_pages = 0

    # Validation on batch level
    validate_required_keys(rows, vcfg.required_keys, strict=vcfg.strict)
    sig = schema_signature(rows)
    validate_schema_drift(signatures_seen=schema_sigs, sig=sig, cfg=vcfg)

    all_rows.extend(rows)

    # Cursor update
    if cursor_type == "page_token":
      resp_field = str(cursor_cfg.get("response_field") or "nextPageToken")
      next_cursor = payload.get(resp_field) if isinstance(payload, dict) else None
      if not next_cursor:
        break
    elif cursor_type == "offset":
      step = int(cursor_cfg.get("step") or len(rows) or 0)
      next_cursor = int(next_cursor or 0) + int(step)
      if len(rows) == 0:
        break
    else:
      break

  # Persist cursor state snapshot (best-effort)
  try:
    cursor_state_out = {"cursor": next_cursor, "updated_at": _utc_now().isoformat()}
    row = build_load_run_snapshot_row(
      batch_run_id=batch_run_id,
      created_at=_utc_now(),
      root_dataset_key=root_key,
      is_execute=True,
      continue_on_error=True,
      max_retries=0,
      had_error=False,
      step_count=1,
      snapshot_json=json.dumps(cursor_state_out),
    )
    sql = dialect.render_insert_load_run_snapshot(meta_schema=meta_schema, values=row)
    if sql:
      target_engine.execute(sql)
  except Exception:
    pass

  # Land into RAW (transient landing zone)
  validate_row_count(len(all_rows), vcfg)

  # Ensure RAW landing can deterministically extract the integrated columns.
  # SourceColumns (integrate=True) and their json_path values are the contract
  # for semi-structured ingestion (REST/files). Target generation relies on this.
  integrated_cols = list(source_dataset.source_columns.filter(integrate=True))
  if not integrated_cols:
    raise ValueError(
      f"No integrated SourceColumns found for REST dataset "
      f"'{sys.short_name}:{source_dataset.source_dataset_name}'. "
      "Run 'Import Metadata', then mark columns as integrated, then run 'Generate Target'."
    )

  missing_paths = [
    c.source_column_name
    for c in integrated_cols
    if not getattr(c, "json_path", None)
  ]
  if missing_paths:
    raise ValueError(
      f"Missing json_path for integrated SourceColumns on REST dataset "
      f"'{sys.short_name}:{source_dataset.source_dataset_name}': "
      f"{', '.join(sorted(missing_paths))}. "
      "Run 'Import Metadata' to refresh column definitions."
    )

  # Normalize keys to match imported SourceColumns.json_path (e.g. userId -> userid),
  # while preserving the original record in __payload__ for the payload system column.
  all_rows = normalize_records_keep_payload(all_rows)

  landing = land_raw_json_records(
    target_engine=target_engine,
    target_dialect=dialect,
    td=td,
    records=all_rows,
    batch_run_id=batch_run_id,
    load_run_id=load_run_id,
    target_system=target_system,
    profile=profile,
    meta_schema=meta_schema,
    source_system_short_name=str(sys.short_name),
    source_dataset_name=str(source_dataset.source_dataset_name),
    source_object=f"{base_url}{path}",
    ingest_mode="rest",
    chunk_size=chunk_size,
    source_dataset=source_dataset,
    strict=vcfg.strict,
  )

  return {
    "rows_extracted": len(all_rows),
    "pages": pages,
    "cursor_after": next_cursor,
    "landing": landing,
  }
