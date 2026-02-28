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
import logging
import urllib.request
from typing import Any, Dict, List

from django.conf import settings
from django.db import transaction

from metadata.models import SourceColumn
from .ref_builder import build_secret_ref
from metadata.secrets.resolver import resolve_profile_secret_value
from metadata.ingestion.infer import (
  infer_column_profile,
  infer_pk_columns,
  get_value_by_json_path,
)
from metadata.ingestion.normalization import normalize_column_name


log = logging.getLogger(__name__)


def _extract_records(payload: Any, record_path: str | None) -> List[Dict[str, Any]]:
  """
  record_path supports dotted dict traversal, e.g. "data.items".
  If record_path is None/empty, accept payload as list[dict] or {"items":[...]} heuristically.
  """
  if payload is None:
    return []

  if record_path:
    cur = payload
    for part in str(record_path).split("."):
      if part == "":
        continue
      if not isinstance(cur, dict):
        return []
      cur = cur.get(part)
    if isinstance(cur, list):
      return [r for r in cur if isinstance(r, dict)]
    return []

  # Heuristics
  if isinstance(payload, list):
    return [r for r in payload if isinstance(r, dict)]
  if isinstance(payload, dict):
    for k in ("items", "data", "results"):
      v = payload.get(k)
      if isinstance(v, list):
        return [r for r in v if isinstance(r, dict)]
  return []


def _flatten_keys(records: List[Dict[str, Any]], *, max_nested: int = 1) -> Dict[str, str]:
  """
  Returns mapping: normalized_column_name -> json_path (JSONPath-like, e.g. $.a.b).
  top-level keys + optionally one nested dict level.
  """
  out: Dict[str, str] = {}
  for r in records:
    for k, v in r.items():
      if k is None:
        continue
      if isinstance(v, dict) and max_nested > 0:
        for k2 in v.keys():
          # Column name is normalized, json_path must keep ORIGINAL key casing.
          col = normalize_column_name(f"{k}.{k2}")
          # Nested normalization is not applied to records yet; keep original path for now.
          out.setdefault(col, f"$.{k}.{k2}")
      else:
        # Column name is normalized, json_path must keep ORIGINAL key casing.
        col = normalize_column_name(str(k))
        # Top-level: json_path must match normalized runtime keys (e.g. userId -> userid).
        out.setdefault(col, f"$.{col}")

  return out


def _resolve_rest_secret(system_short_name: str) -> Dict[str, Any]:
  ref = build_secret_ref(
    profiles_path=settings.ELEVATA_PROFILES_PATH,
    type="rest",
    short_name=system_short_name,
  )
  raw = resolve_profile_secret_value(
    profiles_path=settings.ELEVATA_PROFILES_PATH,
    ref=ref,
  )

  if isinstance(raw, dict):
    return raw
  if isinstance(raw, str):
    try:
      return json.loads(raw)
    except Exception:
      pass
  raise ValueError(f"REST secret for '{system_short_name}' must be a JSON object.")


def _http_get_json(url: str, headers: Dict[str, str] | None = None, timeout: int = 60) -> Any:
  req = urllib.request.Request(url, headers=headers or {})
  with urllib.request.urlopen(req, timeout=timeout) as resp:
    raw = resp.read()
  return json.loads(raw.decode("utf-8"))


def import_rest_metadata_for_dataset(ds, *, autointegrate_pk: bool = True, reset_flags: bool = False) -> Dict[str, Any]:
  """
  REST auto import (Phase 1):
  - fetch sample records
  - infer columns
  - upsert SourceColumn rows (technical fields)
  - set integrate defaults (new columns => True; existing preserved unless reset_flags)
  - infer primary_key_column via heuristic + uniqueness check
  """
  ss = ds.source_system
  secret = _resolve_rest_secret(ss.short_name)

  base_url = os.path.expandvars((secret.get("base_url") or secret.get("url") or "")).rstrip("/")
  if not base_url:
    raise ValueError(f"REST secret for '{ss.short_name}' must contain 'base_url'.")

  headers = dict(secret.get("headers") or {})

  # Dataset-level config:
  # - ds.source_dataset_name is the logical dataset name (e.g. "orders")
  # - path/endpoint lives in ingestion_config (dataset-specific)
  ds_cfg = getattr(ds, "ingestion_config", None) or {}
  if not isinstance(ds_cfg, dict):
    ds_cfg = {}

  path = os.path.expandvars((ds_cfg.get("path") or ds_cfg.get("endpoint") or "").strip())
  if not path:
    raise ValueError(
      f"REST SourceDataset '{ss.short_name}:{ds.source_dataset_name}' is missing ingestion_config.path"
    )

  # Optional system-level base_path prefix (e.g. "/v1")
  sys_cfg = getattr(ss, "external_config", None) or {}
  if not isinstance(sys_cfg, dict):
    sys_cfg = {}
  base_path = (sys_cfg.get("base_path") or "").strip()

  def _join_paths(a: str, b: str) -> str:
    a = (a or "").strip("/")
    b = (b or "").strip("/")
    if a and b:
      return "/" + a + "/" + b
    if a:
      return "/" + a
    return "/" + b if b else ""

  full_path = _join_paths(base_path, path)

  record_path = None
  if isinstance(ds_cfg, dict):
    rp = (ds_cfg.get("record_path") or "").strip()
    if rp:
      record_path = rp

  url = base_url + full_path
  payload = _http_get_json(url, headers=headers)
  records = _extract_records(payload, record_path)

  if not records:
    raise ValueError(
      f"No records found for REST dataset '{ss.short_name}:{ds.source_dataset_name}' "
      f"(path={full_path!r}, record_path={record_path!r})."
    )

  # infer columns (normalized -> source key path)
  col_map = _flatten_keys(records, max_nested=1)
  col_names = list(col_map.keys())

  # For PK inference we only consider top-level paths (no nesting) to avoid surprises.
  top_level_cols = []
  for c in col_names:
    jp = col_map.get(c)
    if jp and jp.startswith("$.") and "." not in jp[2:]:
      top_level_cols.append(c)
  # Build rows with normalized keys for PK inference
  pk_rows = []
  for r in records:
    rr = {}
    for c in top_level_cols:
      jp = col_map.get(c)
      rr[c] = get_value_by_json_path(r, str(jp))
    pk_rows.append(rr)
  pk_final = set(infer_pk_columns(pk_rows, top_level_cols))

  existing: Dict[str, SourceColumn] = {c.source_column_name: c for c in ds.source_columns.all()}
  seen = set()

  created = 0
  updated = 0
  removed = 0

  with transaction.atomic():
    if reset_flags:
      ds.source_columns.update(
        integrate=False,
        pii_level="none",
        description="",
        primary_key_column=False,
      )

    # Prevent UNIQUE(source_dataset_id, ordinal_position) collisions
    if existing:
      base = 10000
      n = 0
      for sc0 in existing.values():
        n += 1
        sc0.ordinal_position = base + n
        sc0.save(update_fields=["ordinal_position"])

    for i, col in enumerate(col_names, start=1):
      sc = existing.get(col)
      is_new = sc is None
      if sc is None:
        sc = SourceColumn(
          source_dataset=ds,
          source_column_name=col,
          integrate=True,   # REST default: take all initially
          pii_level="none",
        )
        created += 1

      # technical fields
      sc.ordinal_position = i
      sc.source_datatype_raw = None
      json_path = col_map.get(col)
      values = [get_value_by_json_path(r, str(json_path)) for r in records]
      dtype, max_len, dec_prec, dec_scale = infer_column_profile(values)
      sc.datatype = dtype
      sc.max_length = max_len
      sc.decimal_precision = dec_prec
      sc.decimal_scale = dec_scale
      sc.nullable = True
      sc.primary_key_column = col in pk_final
      sc.json_path = json_path
      sc.referenced_source_dataset_name = None

      # preserve user fields unless reset_flags
      if reset_flags and is_new is False:
        sc.integrate = False

      if autointegrate_pk and sc.primary_key_column:
        sc.integrate = True

      sc.save()
      if not is_new:
        updated += 1
      seen.add(col)

    # remove disappeared columns (rare for REST, but keep consistent)
    to_remove = [c for name, c in existing.items() if name not in seen]
    if to_remove:
      removed = len(to_remove)
      SourceColumn.objects.filter(pk__in=[c.pk for c in to_remove]).delete()

  return {
    "columns_imported": len(seen),
    "created": created,
    "updated": updated,
    "removed": removed,
    "pk_detected": sorted(pk_final),
  }
