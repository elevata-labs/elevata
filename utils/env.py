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

import os, json
from typing import List, Optional, Callable

def env_str(key: str, default: Optional[str] = None) -> Optional[str]:
  """Get env var as string with default."""
  val = os.getenv(key)
  return val if val not in (None, "") else default

def env_bool(key: str, default: bool = False) -> bool:
  """Get env var as boolean."""
  val = os.getenv(key)
  return default if val is None else val.strip().lower() in ("1","true","yes","on")

def env_int(key: str, default: int = 0) -> int:
  """Get env var as int."""
  val = os.getenv(key)
  try:
    return int(val) if val is not None else default
  except ValueError:
    return default

def env_list(key: str, default: Optional[List[str]] = None, sep: str = ",") -> List[str]:
  """Get comma-separated list env var."""
  val = os.getenv(key)
  if not val:
    return default or []
  return [x.strip() for x in val.split(sep) if x.strip()]

def env_json(key: str, default, transform: Optional[Callable] = None):
  """Get env var parsed as JSON."""
  val = os.getenv(key)
  if not val:
    return default
  try:
    data = json.loads(val)
    return transform(data) if transform else data
  except json.JSONDecodeError:
    return default
