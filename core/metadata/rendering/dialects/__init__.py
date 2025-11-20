"""
SQL dialect adapters.

Each dialect implements SqlDialect and knows how to render Expr and
LogicalSelect instances into concrete SQL strings.
"""

import os
from metadata.config.profiles import load_profile
from .duckdb import DuckDBDialect
from django.conf import settings
# from .mssql import MssqlDialect
# from .snowflake import SnowflakeDialect


def get_active_dialect():
  """
  Return the SQL dialect instance for the currently active elevata profile.

  Resolution order:
    1. ELEVATA_SQL_DIALECT or ELEVATA_DIALECT env vars
    2. profile.default_dialect
    3. 'duckdb' fallback
  """
  profile = load_profile(settings.ELEVATA_PROFILES_PATH)

  # Env override first (new name + backwards-compatible old name)
  env_kind = os.environ.get("ELEVATA_SQL_DIALECT")

  kind = (
    env_kind
    or profile.default_dialect
    or "duckdb"
  )

  if kind == "duckdb":
    return DuckDBDialect()

  # To be extended later
  # if kind == "mssql":
  #   return MssqlDialect()
  # ...

  raise ValueError(f"Unsupported SQL dialect: {kind!r}")
