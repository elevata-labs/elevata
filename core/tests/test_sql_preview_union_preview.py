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

# Template tests for UNION-based SQL preview.

import pytest

from metadata.rendering.logical_plan import LogicalUnion, LogicalSelect, SourceTable, SelectItem


def _build_minimal_select(schema: str, table: str, alias: str, column_alias: str) -> LogicalSelect:
  """Helper to build a very small LogicalSelect."""
  from_table = SourceTable(
    schema=schema,
    name=table,
    alias=alias,
  )

  dummy_expr = object() 

  select_items = [
    SelectItem(expr=dummy_expr, alias=column_alias),
  ]

  return LogicalSelect(
    from_=from_table,
    select_list=select_items,
  )


@pytest.fixture
def union_of_two_selects():
  """Provide a LogicalUnion of two simple selects."""
  sel1 = _build_minimal_select("raw", "customer", "c1", "customer_id")
  sel2 = _build_minimal_select("raw", "customer_backup", "c2", "customer_id")

  return LogicalUnion(
    selects=[sel1, sel2],
    union_type="ALL",
  )


@pytest.mark.skip(reason="Wire this test to your real SQL preview / renderer implementation.")
def test_sql_preview_union_all(union_of_two_selects):
  """
  Once wired, this test should verify that:

  - Both SELECTs appear in the final preview SQL
  - A UNION ALL (or UNION) appears between them
  """
  # Example of how you might connect this later:
  # from metadata.preview import build_union_preview_sql
  # sql = build_union_preview_sql(union_of_two_selects)

  from metadata.rendering.preview import build_union_preview_sql  # type: ignore  # noqa: F401
  sql = build_union_preview_sql(union_of_two_selects)  # type: ignore  # noqa: F821

  lowered = sql.lower()

  assert "select" in lowered
  assert "from raw.customer" in lowered
  assert "from raw.customer_backup" in lowered
  assert "union all" in lowered or "union" in lowered
