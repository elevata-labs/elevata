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

from metadata.rendering.logical_plan import LogicalUnion, LogicalSelect, SourceTable


def test_logical_select_structure():
  """Ensure LogicalSelect stores from_/joins/select_list correctly."""
  table = SourceTable(schema="raw", name="customer", alias="c")

  sel = LogicalSelect(from_=table, distinct=True)
  assert sel.from_.alias == "c"
  assert sel.distinct is True
  assert sel.joins == []


class DummySelect:
  """Minimal mock for a SELECT-like node with to_sql()."""

  def __init__(self, sql: str):
    self._sql = sql

  def to_sql(self, dialect) -> str:
    # Ignore the dialect and just return the stored SQL string.
    return self._sql


class DummyDialect:
  """
  Minimal dialect that can be passed to LogicalUnion.to_sql().

  It supports both possible implementations:

    - calling sel.to_sql(dialect)
    - calling dialect.render_select(sel)
  """

  def render_select(self, sel: DummySelect) -> str:
    # Delegate back to the DummySelect, which ignores the dialect.
    return sel.to_sql(self)


def test_logical_union_to_sql_all():
  """Ensure UNION ALL correctly joins multiple SELECTs."""

  sel1 = DummySelect("SELECT * FROM t1")
  sel2 = DummySelect("SELECT * FROM t2")

  union = LogicalUnion([sel1, sel2], union_type="ALL")

  # Use dummy dialect so both implementations work:
  # - sel.to_sql(dialect)
  # - dialect.render_select(sel)
  sql = union.to_sql(dialect=DummyDialect())

  assert "SELECT * FROM t1" in sql
  assert "SELECT * FROM t2" in sql
  assert "UNION ALL" in sql
