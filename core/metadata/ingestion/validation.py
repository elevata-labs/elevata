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

from dataclasses import dataclass
from typing import Any


@dataclass
class RestValidationConfig:
  """
  Low-level ingestion guardrails.
  """
  strict: bool = False
  min_rows: int | None = None
  max_rows: int | None = None
  required_keys: list[str] | None = None
  max_empty_pages: int = 3
  max_schema_signatures: int = 3


@dataclass
class RestRetryConfig:
  max_attempts: int = 3
  backoff_seconds: float = 2.0
  retry_on_status: list[int] | None = None


def parse_rest_retry_config(cfg: dict[str, Any]) -> RestRetryConfig:
  r = cfg.get("retry") or {}
  if not isinstance(r, dict):
    r = {}
  return RestRetryConfig(
    max_attempts=int(r.get("max_attempts") or 3),
    backoff_seconds=float(r.get("backoff_seconds") or 2.0),
    retry_on_status=list(r.get("retry_on_status") or [429, 500, 502, 503, 504]),
  )


def parse_rest_validation_config(cfg: dict[str, Any]) -> RestValidationConfig:
  v = cfg.get("validation") or {}
  if not isinstance(v, dict):
    v = {}
  return RestValidationConfig(
    strict=bool(v.get("strict") or False),
    min_rows=v.get("min_rows"),
    max_rows=v.get("max_rows"),
    required_keys=list(v.get("required_keys") or []) or None,
    max_empty_pages=int(v.get("max_empty_pages") or 3),
    max_schema_signatures=int(v.get("max_schema_signatures") or 3),
  )


def validate_required_keys(rows: list[dict[str, Any]], required: list[str] | None, *, strict: bool) -> None:
  if not required:
    return
  missing = []
  for k in required:
    if any((k not in r) for r in rows):
      missing.append(k)
  if missing and strict:
    raise ValueError(f"REST ingestion validation failed: required_keys missing in some rows: {missing}")


def schema_signature(rows: list[dict[str, Any]]) -> tuple[str, ...]:
  """
  Basic schema fingerprint: sorted union of keys across the batch.
  """
  keys: set[str] = set()
  for r in rows:
    keys.update(r.keys())
  return tuple(sorted(keys))


def validate_schema_drift(
  *,
  signatures_seen: set[tuple[str, ...]],
  sig: tuple[str, ...],
  cfg: RestValidationConfig,
) -> None:
  signatures_seen.add(sig)
  if len(signatures_seen) > int(cfg.max_schema_signatures or 3):
    msg = f"REST ingestion schema drift exceeded max_schema_signatures={cfg.max_schema_signatures}"
    if cfg.strict:
      raise ValueError(msg)


def validate_row_count(total_rows: int, cfg: RestValidationConfig) -> None:
  if cfg.min_rows is not None and total_rows < int(cfg.min_rows):
    raise ValueError(f"REST ingestion validation failed: total_rows={total_rows} < min_rows={cfg.min_rows}")
  if cfg.max_rows is not None and total_rows > int(cfg.max_rows):
    raise ValueError(f"REST ingestion validation failed: total_rows={total_rows} > max_rows={cfg.max_rows}")
