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

from metadata.rendering.dialects.duckdb import DuckDBDialect
from metadata.rendering.dialects.mssql import MssqlDialect


def test_duckdb_quality_not_null_examples_use_limit():
  sql = DuckDBDialect().render_quality_not_null_examples_statement(
    schema_name="rawcore",
    table_name="customer",
    column_name="customer_name",
    context_columns=["customer_key"],
    example_limit=7,
  )

  assert "FROM rawcore.customer AS q" in sql
  assert "WHERE q.customer_name IS NULL" in sql
  assert "ORDER BY q.customer_key" in sql
  assert sql.endswith("LIMIT 7")


def test_duckdb_quality_duplicate_key_examples_use_limit():
  sql = DuckDBDialect().render_quality_duplicate_key_examples_statement(
    schema_name="rawcore",
    table_name="customer",
    key_columns=["customer_id", "source_system"],
    example_limit=3,
  )

  assert 'COUNT(*) AS duplicate_count' in sql
  assert "GROUP BY q.customer_id, q.source_system" in sql
  assert 'HAVING COUNT(*) > 1' in sql
  assert sql.endswith("LIMIT 3")


def test_mssql_quality_checks_use_top_instead_of_limit():
  sql = MssqlDialect().render_quality_duplicate_key_examples_statement(
    schema_name="rawcore",
    table_name="customer",
    key_columns=["customer_id"],
    example_limit=4,
  )

  assert sql.startswith("SELECT TOP (4)")
  assert "LIMIT" not in sql


def test_mssql_quality_row_presence_uses_top_instead_of_limit():
  sql = MssqlDialect().render_quality_row_presence_statement(
    schema_name="rawcore",
    table_name="customer",
  )

  assert sql == "SELECT TOP (1) 1 AS row_exists\nFROM rawcore.customer"
  assert "LIMIT" not in sql


def test_mssql_quality_row_presence_can_probe_real_column():
  sql = MssqlDialect().render_quality_row_presence_statement(
    schema_name="rawcore",
    table_name="customer",
    probe_column="customer_id",
  )

  assert sql == "SELECT TOP (1) customer_id AS row_exists\nFROM rawcore.customer"
  assert "LIMIT" not in sql


def test_duckdb_quality_row_presence_statement_is_dialect_owned():
  sql = DuckDBDialect().render_quality_row_presence_statement(
    schema_name="rawcore",
    table_name="customer",
  )

  assert sql == "SELECT 1 AS row_exists\nFROM rawcore.customer\nLIMIT 1"


def test_duckdb_quality_row_presence_can_probe_real_column():
  sql = DuckDBDialect().render_quality_row_presence_statement(
    schema_name="rawcore",
    table_name="customer",
    probe_column="customer_id",
  )

  assert sql == "SELECT customer_id AS row_exists\nFROM rawcore.customer\nLIMIT 1"


def test_duckdb_quality_blank_string_examples_use_limit():
  sql = DuckDBDialect().render_quality_blank_string_examples_statement(
    schema_name="rawcore",
    table_name="customer",
    column_name="customer_id",
    context_columns=["customer_key"],
    example_limit=5,
  )

  assert "FROM rawcore.customer AS q" in sql
  assert "WHERE q.customer_id IS NOT NULL" in sql
  assert "AND TRIM(q.customer_id) = ''" in sql
  assert "ORDER BY q.customer_key" in sql
  assert sql.endswith("LIMIT 5")


def test_mssql_quality_blank_string_checks_use_top_instead_of_limit():
  sql = MssqlDialect().render_quality_blank_string_examples_statement(
    schema_name="rawcore",
    table_name="customer",
    column_name="customer_id",
    example_limit=6,
  )

  assert sql.startswith("SELECT TOP (6)")
  assert "TRIM" in sql
  assert "LIMIT" not in sql
