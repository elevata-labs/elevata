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

from metadata.rendering.logical_plan import LogicalUnion, LogicalSelect, SourceTable
from tests._dialect_test_mixin import DialectTestMixin


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


class DummyDialect(DialectTestMixin):
  def render_select(self, sel: DummySelect) -> str:
    # Delegate back to the DummySelect, which ignores the dialect.
    return sel.to_sql(self)

  def render_plan(self, plan):
    if isinstance(plan, LogicalSelect):
      return self.render_select(plan)

    if isinstance(plan, LogicalUnion):
      rendered_parts = [self.render_select(sel) for sel in plan.selects]

      ut = (plan.union_type or "").strip().upper()
      if ut == "ALL":
        sep = "UNION ALL"
      else:
        # DISTINCT and "" normalize to UNION
        sep = "UNION"

      return f"\n{sep}\n".join(rendered_parts)

    raise TypeError(f"Unsupported logical plan: {type(plan).__name__}")  


def test_logical_union_render_plan_all():
  """Ensure UNION ALL correctly joins multiple SELECTs via dialect rendering."""

  sel1 = DummySelect("SELECT * FROM t1")
  sel2 = DummySelect("SELECT * FROM t2")

  union = LogicalUnion([sel1, sel2], union_type="ALL")

  sql = DummyDialect().render_plan(union)

  assert "SELECT * FROM t1" in sql
  assert "SELECT * FROM t2" in sql
  assert "UNION ALL" in sql

def test_logical_union_render_plan_distinct_normalizes_to_union():
  """UNION DISTINCT must render as plain UNION."""

  sel1 = DummySelect("SELECT * FROM t1")
  sel2 = DummySelect("SELECT * FROM t2")

  union = LogicalUnion([sel1, sel2], union_type="DISTINCT")

  sql = DummyDialect().render_plan(union)

  assert "UNION DISTINCT" not in sql
  assert "\nUNION\n" in sql
