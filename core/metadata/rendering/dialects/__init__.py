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

import os
from typing import Optional, Type

from django.conf import settings

from metadata.config.profiles import load_profile
from metadata.rendering.dialects.base import SqlDialect
from metadata.rendering.dialects.duckdb import DuckDBDialect

"""
SQL dialect adapters.

Each dialect implements SqlDialect and knows how to render Expr and
LogicalSelect instances into concrete SQL strings.
"""

# Registry of known dialects.
# Extend this in 0.5.x with PostgresDialect, MssqlDialect, ...
_DIALECT_REGISTRY: dict[str, Type[SqlDialect]] = {
  "duckdb": DuckDBDialect,
  # "postgres": PostgresDialect,
  # "mssql": MssqlDialect,
}


def _resolve_dialect_name(explicit: Optional[str] = None) -> str:
  """
  Resolve a dialect name from (in order):

  1. explicit argument
  2. environment variables (ELEVATA_SQL_DIALECT, ELEVATA_DIALECT)
  3. active profile.default_dialect
  4. hard fallback 'duckdb'
  """
  # 1) Explicit argument (e.g. CLI flag)
  if explicit:
    return explicit.lower()

  # 2) Env overrides
  env_name = (
    os.getenv("ELEVATA_SQL_DIALECT")
    or os.getenv("ELEVATA_DIALECT")
  )
  if env_name:
    return env_name.lower()

  # 3) Profile.default_dialect
  try:
    profiles_path = getattr(settings, "ELEVATA_PROFILES_PATH", None)
    profile = load_profile(profiles_path)
    if profile.default_dialect:
      return profile.default_dialect.lower()
  except Exception:
    # If profiles are misconfigured or settings are not ready, fall back
    pass

  # 4) Hard fallback
  return "duckdb"


def get_active_dialect(name: Optional[str] = None) -> SqlDialect:
  """
  Return an instance of the active SqlDialect.

  Resolution order:
    - `name` argument (if provided)
    - ELEVATA_SQL_DIALECT / ELEVATA_DIALECT env vars
    - active profile's `default_dialect`
    - hard fallback 'duckdb'

  Raises:
      ValueError: if the resolved name is not registered.
  """
  dialect_name = _resolve_dialect_name(name)

  try:
    dialect_cls = _DIALECT_REGISTRY[dialect_name]
  except KeyError as exc:
    available = ", ".join(sorted(_DIALECT_REGISTRY))
    raise ValueError(
      f"Unknown SQL dialect: {dialect_name!r}. "
      f"Available dialects: {available}."
    ) from exc

  return dialect_cls()
