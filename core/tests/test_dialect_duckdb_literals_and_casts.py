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

import datetime
from decimal import Decimal

from metadata.rendering.dialects.duckdb import DuckDBDialect


def test_render_literal_null():
  dialect = DuckDBDialect()
  sql = dialect.render_literal(None)
  assert sql == "NULL"


def test_render_literal_boolean():
  dialect = DuckDBDialect()
  assert dialect.render_literal(True) == "TRUE"
  assert dialect.render_literal(False) == "FALSE"


def test_render_literal_numeric():
  dialect = DuckDBDialect()
  assert dialect.render_literal(42) == "42"
  assert dialect.render_literal(3.5) == "3.5"
  assert dialect.render_literal(Decimal("10.25")) == "10.25"


def test_render_literal_string_simple():
  dialect = DuckDBDialect()
  sql = dialect.render_literal("hello")
  assert sql == "'hello'"


def test_render_literal_string_with_quote_escaping():
  dialect = DuckDBDialect()
  sql = dialect.render_literal("O'Malley")
  # Single quote must be doubled inside the literal
  assert sql == "'O''Malley'"


def test_render_literal_date():
  dialect = DuckDBDialect()
  d = datetime.date(2024, 5, 17)
  sql = dialect.render_literal(d)
  assert sql == "DATE '2024-05-17'"


def test_render_literal_datetime():
  dialect = DuckDBDialect()
  dt = datetime.datetime(2024, 5, 17, 14, 30, 5)
  sql = dialect.render_literal(dt)
  # Depending on your implementation, the exact formatting may vary.
  # This assertion assumes ISO format with seconds and a space separator.
  assert sql == "TIMESTAMP '2024-05-17 14:30:05'"


def test_cast_expression_basic():
  dialect = DuckDBDialect()
  sql = dialect.cast_expression("customer_id", "BIGINT")
  assert sql == "CAST(customer_id AS BIGINT)"


def test_cast_expression_with_function_call_inside():
  dialect = DuckDBDialect()
  sql = dialect.cast_expression("SUM(amount)", "DECIMAL(10,2)")
  assert sql == "CAST(SUM(amount) AS DECIMAL(10,2))"
