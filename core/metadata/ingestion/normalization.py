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

import re
import datetime
from typing import Any


def normalize_column_name(name: str) -> str:
  """
  Normalize external column names to a stable, warehouse-friendly form.
  - replaces separators/special chars with underscores
  - collapses multiple underscores
  - lowercases
  """
  s = (name or "").strip()
  s = s.replace(".", "_")
  s = re.sub(r"[^a-zA-Z0-9_]+", "_", s)
  s = re.sub(r"_+", "_", s)
  s = s.strip("_").lower()
  return s[:100] or "col"


def normalize_records_keep_payload(records: list[dict]) -> list[dict]:
  """
  Normalize record keys using normalize_column_name() while preserving the original
  record in __payload__ (used by RAW payload system column).
  """
  out: list[dict] = []
  for rec in records or []:
    if not isinstance(rec, dict):
      continue
    norm: dict[str, Any] = {}
    for k, v in rec.items():
      nk = normalize_column_name(str(k))
      # last write wins (deterministic)
      norm[nk] = v
    norm["__payload__"] = rec
    out.append(norm)
  return out

def normalize_param_value(v: Any) -> Any:
  """
  Normalize Python values before binding them as DB parameters.

  Snowflake's Python connector does not support pandas.Timestamp / numpy datetime64
  directly. We convert those to plain Python datetime (UTC, naive).
  """
  if v is None:
    return None

  # pandas.Timestamp / similar (duck-typed)
  to_pydt = getattr(v, "to_pydatetime", None)
  if callable(to_pydt):
    try:
      v = to_pydt()
    except Exception:
      pass

  # numpy scalar types (e.g. np.int64, np.float64, np.datetime64 scalars)
  item = getattr(v, "item", None)
  if callable(item):
    try:
      v = item()
    except Exception:
      pass

  # Ensure datetime is safe for drivers (prefer UTC naive)
  if isinstance(v, datetime.datetime):
    if v.tzinfo is not None:
      v = v.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return v

  # date is fine
  if isinstance(v, datetime.date):
    return v

  return v
