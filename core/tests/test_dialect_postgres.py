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

from metadata.rendering.dialects.postgres import PostgresDialect
from metadata.rendering.expr import (
  ColumnRef,
  row_number_over,
)


def test_pg_row_number_render():
  d = PostgresDialect()

  expr = row_number_over(
    partition_by=[ColumnRef("s", "bk")],
    order_by=[ColumnRef("s", "__src_rank_ord")],
  )

  sql = d.render_expr(expr)
  assert sql == (
    'ROW_NUMBER() OVER (PARTITION BY s."bk" '
    'ORDER BY s."__src_rank_ord")'
  )


def test_pg_hash_expression():
  d = PostgresDialect()
  sql = d.hash_expression("colname")
  assert "digest(colname, 'sha256')" in sql
  assert "encode(" in sql


def test_pg_literal_render():
  d = PostgresDialect()
  assert d.render_literal("abc") == "'abc'"
  assert d.render_literal(42) == "42"
  assert d.render_literal(None) == "NULL"
  assert d.render_literal(True) == "TRUE"
