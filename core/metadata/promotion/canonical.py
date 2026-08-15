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

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
import hashlib
import json
from typing import Any
from uuid import UUID


def canonicalize_metadata_value(value: Any) -> Any:
  """
  Return a JSON-compatible value with deterministic container ordering.
  """
  if isinstance(value, Mapping):
    return {
      str(key): canonicalize_metadata_value(value[key])
      for key in sorted(value, key=lambda item: str(item))
    }
  if isinstance(value, (set, frozenset)):
    normalized = [canonicalize_metadata_value(item) for item in value]
    return sorted(
      normalized,
      key=lambda item: canonical_json(item),
    )
  if isinstance(value, tuple):
    return [canonicalize_metadata_value(item) for item in value]
  if isinstance(value, list):
    return [canonicalize_metadata_value(item) for item in value]
  if isinstance(value, Enum):
    return canonicalize_metadata_value(value.value)
  if isinstance(value, UUID):
    return str(value)
  if isinstance(value, datetime):
    return value.isoformat(timespec="microseconds")
  if isinstance(value, date):
    return value.isoformat()
  if isinstance(value, Decimal):
    return format(value, "f")
  return value


def canonical_json(value: Any) -> str:
  """
  Serialize metadata using the canonical JSON representation.
  """
  return json.dumps(
    canonicalize_metadata_value(value),
    sort_keys=True,
    ensure_ascii=False,
    separators=(",", ":"),
    allow_nan=False,
  )


def canonical_sha256(value: Any) -> str:
  """
  Return a deterministic SHA-256 fingerprint for metadata content.
  """
  payload = canonical_json(value)
  return hashlib.sha256(payload.encode("utf-8")).hexdigest()
