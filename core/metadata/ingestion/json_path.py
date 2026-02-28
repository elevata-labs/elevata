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

from typing import Any


def extract_json_path(obj: Any, path: str) -> Any:
  """
  Minimal JSONPath extractor for ingestion flattening.

  Supported:
    - $.a.b.c
    - $.items[0].id
    - $.a[3]

  Returns None if any segment is missing.
  """
  if obj is None:
    return None
  p = (path or "").strip()
  if not p:
    return None
  if not p.startswith("$."):
    raise ValueError(f"Unsupported json_path (must start with $.): {path!r}")

  cur: Any = obj
  s = p[2:]  # strip '$.'
  i = 0

  while i < len(s):
    # Parse identifier
    j = i
    while j < len(s) and s[j] not in ".[":
      j += 1
    key = s[i:j]
    if key:
      if not isinstance(cur, dict):
        return None
      cur = cur.get(key)
      if cur is None:
        return None
    i = j

    # Parse optional [index]
    if i < len(s) and s[i] == "[":
      k = s.find("]", i)
      if k == -1:
        raise ValueError(f"Unclosed [ in json_path: {path!r}")
      idx_str = s[i + 1:k].strip()
      if not idx_str.isdigit():
        raise ValueError(f"Only numeric indices supported in json_path: {path!r}")
      idx = int(idx_str)
      if not isinstance(cur, list):
        return None
      if idx < 0 or idx >= len(cur):
        return None
      cur = cur[idx]
      i = k + 1

    # Consume optional dot
    if i < len(s) and s[i] == ".":
      i += 1

  return cur
