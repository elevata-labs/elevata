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

# Template tests for a basic SQL preview pipeline using LogicalSelect.

import pytest

from metadata.rendering.logical_plan import SourceTable, LogicalSelect, SelectItem


@pytest.fixture
def simple_logical_select():
  """
  Build a minimal LogicalSelect for SQL preview tests.

  This fixture does NOT touch the database.
  It only relies on logical_plan structures.
  """
  from_table = SourceTable(
    schema="raw",
    name="customer",
    alias="c",
  )

  dummy_expr = object()

  select_items = [
    SelectItem(expr=dummy_expr, alias="customer_id"),
  ]

  return LogicalSelect(
    from_=from_table,
    select_list=select_items,
    distinct=False,
  )


@pytest.mark.skip(reason="Wire this test to your real SQL preview renderer (e.g. preview.build_preview_sql).")
def test_sql_preview_basic_from_and_alias(simple_logical_select):
  """
  Once wired to your real SQL preview function, this test should verify:

  - FROM clause uses schema + table
  - alias is rendered correctly
  - selected columns/aliases appear in the SQL
  """

  from metadata.rendering.preview import build_preview_sql  # type: ignore  # noqa: F401
  sql = build_preview_sql(simple_logical_select)  # type: ignore  # noqa: F821

  lowered = sql.lower()

  # FROM raw.customer AS c (or similar)
  assert "from raw.customer" in lowered
  assert " c" in lowered  # alias 'c' somewhere after FROM

  # ensure at least the alias from the select list is present
  assert "customer_id" in lowered
