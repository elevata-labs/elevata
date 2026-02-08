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

import pytest

from core.metadata.rendering.dialects.dialect_factory import get_active_dialect


@pytest.mark.parametrize(
  "dialect_name",
  [
    "bigquery",
    "databricks",
    "duckdb",
    "fabric_warehouse",
    "mssql",
    "postgres",
    "snowflake",
  ],
)
def test_supported_dialects_define_load_run_log_type_map(dialect_name):
  d = get_active_dialect(dialect_name)

  assert hasattr(d, "LOAD_RUN_LOG_TYPE_MAP"), f"{dialect_name}: missing LOAD_RUN_LOG_TYPE_MAP"
  m = getattr(d, "LOAD_RUN_LOG_TYPE_MAP")
  assert isinstance(m, dict), f"{dialect_name}: LOAD_RUN_LOG_TYPE_MAP must be a dict"

  required = {"string", "bool", "int", "timestamp"}
  missing = required - set(m.keys())
  assert not missing, f"{dialect_name}: missing keys in LOAD_RUN_LOG_TYPE_MAP: {sorted(missing)}"


@pytest.mark.parametrize(
  "dialect_name",
  [
    "bigquery",
    "databricks",
    "duckdb",
    "fabric_warehouse",
    "mssql",
    "postgres",
    "snowflake",
  ],
)
def test_supported_dialects_map_core_log_types(dialect_name):
  d = get_active_dialect(dialect_name)

  # Either use the optional override, or fall back to the map.
  def map_type(col, canonical):
    fn = getattr(d, "map_load_run_log_type", None)
    if callable(fn):
      t = fn(col, canonical)
      if t:
        return t
    return d.LOAD_RUN_LOG_TYPE_MAP.get(canonical)

  for canonical in ["string", "bool", "int", "timestamp"]:
    phys = map_type("any_col", canonical)
    assert phys, f"{dialect_name}: no physical type mapping for {canonical!r}"
