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

from metadata.rendering.dialects.dialect_factory import get_active_dialect
from metadata.rendering.dialects.snowflake import SnowflakeDialect, SnowflakeExecutionEngine


def test_snowflake_dialect_is_registered():
  d = get_active_dialect("snowflake")
  assert isinstance(d, SnowflakeDialect)


def test_snowflake_quote_ident_uses_double_quotes():
  d = SnowflakeDialect()
  assert d.quote_ident("foo") == '"foo"'
  assert d.quote_ident('a"b') == '"a""b"'


def test_snowflake_hash_expression_uses_sha2_and_hex():
  d = SnowflakeDialect()
  sql = d.hash_expression("('x')")
  low = sql.lower()
  assert "sha2" in low
  assert "to_varchar" in low


def test_snowflake_merge_renders_merge_into():
  d = SnowflakeDialect()
  sql = d.render_merge_statement(
    target_fqn=d.render_table_identifier("dw", "dim_x"),
    source_select_sql="SELECT 1 AS id, 'a' AS payload",
    key_columns=["id"],
    update_columns=["payload"],
    insert_columns=["id", "payload"],
  )
  assert "merge into" in sql.lower()


def test_snowflake_split_statements():
  assert SnowflakeExecutionEngine._split_statements("SELECT 1; SELECT 2;") == [
    "SELECT 1;",
    "SELECT 2;",
  ]
  assert SnowflakeExecutionEngine._split_statements(";\n  \nSELECT 1;\n") == [
    "SELECT 1;",
  ]
