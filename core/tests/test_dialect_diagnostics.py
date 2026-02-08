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

"""
Dialect diagnostics smoke tests.

These tests validate that the diagnostics module can build a consistent
snapshot for all registered dialects.
"""

import pytest

from metadata.rendering.dialects.dialect_factory import (
  get_available_dialect_names,
  get_active_dialect,
)
from metadata.rendering.dialects.diagnostics import (
  collect_dialect_diagnostics,
  snapshot_all_dialects,
)

from metadata.rendering.dialects import diagnostics as diag_mod
from metadata.rendering.dialects import dialect_factory
from metadata.rendering.dialects.duckdb import DuckDBDialect
from metadata.rendering.dialects.postgres import PostgresDialect
from metadata.rendering.dialects.mssql import MssqlDialect
from metadata.rendering.dialects.databricks import DatabricksDialect
from metadata.rendering.dialects.snowflake import SnowflakeDialect
from metadata.rendering.dialects.fabric_warehouse import FabricWarehouseDialect


def test_collect_dialect_diagnostics_for_each_registered_dialect():
  for name in get_available_dialect_names():
    dialect = get_active_dialect(name)
    diag = collect_dialect_diagnostics(dialect)

    assert diag.name  # non-empty
    assert diag.class_name.endswith("Dialect")

    # Basic capabilities are booleans
    assert isinstance(diag.supports_merge, bool)
    assert isinstance(diag.supports_delete_detection, bool)

    # Literal examples should be non-empty strings
    assert isinstance(diag.literal_true, str) and diag.literal_true
    assert isinstance(diag.literal_false, str) and diag.literal_false
    assert isinstance(diag.literal_null, str) and diag.literal_null
    assert isinstance(diag.literal_sample_date, str) and diag.literal_sample_date

    # Expression samples should look like SQL fragments
    assert isinstance(diag.sample_concat, str) and diag.sample_concat
    assert isinstance(diag.sample_hash256, str) and diag.sample_hash256


def test_snapshot_all_dialects_returns_all_registered_names():
  snapshot = snapshot_all_dialects()
  names = set(get_available_dialect_names())

  assert set(snapshot.keys()) == names

  # Spot-check one entry
  for name, diag in snapshot.items():
    assert diag.name == name or diag.name == diag.class_name.lower()
    # roundtrip to dict should contain at least these keys
    as_dict = diag.to_dict()
    for key in [
      "name",
      "class_name",
      "supports_merge",
      "supports_delete_detection",
      "literal_true",
      "literal_false",
      "literal_null",
      "literal_sample_date",
      "sample_concat",
      "sample_hash256",
    ]:
      assert key in as_dict


def test_snapshot_all_dialects_runs_without_error():
  """All registered dialects should produce a diagnostics snapshot."""
  snapshots = diag_mod.snapshot_all_dialects()

  available = set(dialect_factory.get_available_dialect_names())
  assert set(snapshots.keys()) == available

  for name, diag in snapshots.items():
    # Basic invariants
    assert diag.name
    assert diag.class_name
    # Capabilities must be boolean flags
    assert isinstance(diag.supports_merge, bool)
    assert isinstance(diag.supports_delete_detection, bool)
    assert isinstance(diag.supports_hash_expression, bool)
    # Literal samples should be non-empty strings
    assert isinstance(diag.literal_true, str)
    assert isinstance(diag.literal_false, str)
    assert isinstance(diag.literal_null, str)


def test_duckdb_capabilities_and_hash_support():
  """DuckDB should explicitly opt into merge, delete detection and hashing."""
  dialect = DuckDBDialect()
  diag = diag_mod.collect_dialect_diagnostics(dialect)

  assert diag.name == DuckDBDialect.DIALECT_NAME
  assert diag.supports_merge is True
  assert diag.supports_delete_detection is True
  assert diag.supports_hash_expression is True

  # The sample hash expression should wrap the concat expression
  assert "SHA256" in diag.sample_hash256.upper()
  assert "a" in diag.sample_concat


def test_postgres_capabilities_and_hash_support():
  dialect = PostgresDialect()
  diag = collect_dialect_diagnostics(dialect)

  assert diag.name == PostgresDialect.DIALECT_NAME
  assert diag.supports_merge is True
  assert diag.supports_delete_detection is True
  assert diag.supports_hash_expression is True
  # pgcrypto-style hash
  assert "DIGEST" in diag.sample_hash256.upper()
  assert "ENCODE" in diag.sample_hash256.upper()


def test_mssql_capabilities_and_hash_support():
  dialect = MssqlDialect()
  diag = collect_dialect_diagnostics(dialect)

  assert diag.name == MssqlDialect.DIALECT_NAME
  assert diag.supports_merge is True
  assert diag.supports_delete_detection is True
  assert diag.supports_hash_expression is True
  # HASHBYTES-based hash
  assert "HASHBYTES" in diag.sample_hash256.upper()
  assert "CONVERT" in diag.sample_hash256.upper()


def test_databricks_hash_support():
  dialect = DatabricksDialect()
  diag = collect_dialect_diagnostics(dialect)
  assert diag.name == DatabricksDialect.DIALECT_NAME
  assert diag.supports_hash_expression is True
  assert "SHA2" in diag.sample_hash256.upper()


def test_snowflake_hash_support():
  dialect = SnowflakeDialect()
  diag = collect_dialect_diagnostics(dialect)
  assert diag.name == SnowflakeDialect.DIALECT_NAME
  assert diag.supports_hash_expression is True
  assert "SHA2" in diag.sample_hash256.upper()


def test_fabric_warehouse_hash_support():
  dialect = FabricWarehouseDialect()
  diag = collect_dialect_diagnostics(dialect)
  assert diag.name == FabricWarehouseDialect.DIALECT_NAME
  assert diag.supports_hash_expression is True
  assert "HASHBYTES" in diag.sample_hash256.upper()
  assert "CONVERT" in diag.sample_hash256.upper()