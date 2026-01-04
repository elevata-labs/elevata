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

import re
from metadata.rendering.dialects.duckdb import DuckDBDialect
from metadata.rendering.logical_plan import (
  LogicalSelect,
  LogicalUnion,
  SelectItem,
  SourceTable,
  SubquerySource,
)
from metadata.rendering.expr import (
  ColumnRef,
  RawSql,
  row_number_over,
)

from metadata.rendering.builder import (
  _build_ranked_stage_union,
)


# -----------------------------------------------------------------------------
# Help fakes for TargetDataset / TargetColumn (without Django-ORM)
# -----------------------------------------------------------------------------

class FakeTargetColumn:
  def __init__(
    self,
    target_column_name: str,
    active: bool = True,
    system_role: str = "",
    ordinal_position: int = 1,
    id_: int = 1,
  ) -> None:
    self.target_column_name = target_column_name
    self.active = active
    self.system_role = system_role
    self.ordinal_position = ordinal_position
    self.id = id_


class FakeQuerySet(list):
  """
  Minimal replacement for what the builder expects from target_dataset.target_columns: 
  filter(), order_by(), values_list().
  """

  def filter(self, **kwargs):
    def matches(obj) -> bool:
      for k, v in kwargs.items():
        if getattr(obj, k) != v:
          return False
      return True

    return FakeQuerySet([obj for obj in self if matches(obj)])

  def order_by(self, *fields):
    def key(obj):
      return tuple(getattr(obj, f) for f in fields)

    return FakeQuerySet(sorted(self, key=key))

  def values_list(self, field_name, flat=False):
    values = [getattr(obj, field_name) for obj in self]
    if flat:
      return values
    return [(v,) for v in values]


class FakeTargetDataset:
  def __init__(self, columns: list[FakeTargetColumn]) -> None:
    self.target_columns = FakeQuerySet(columns)


# -----------------------------------------------------------------------------
# 1. Dialect tests: Subquery in FROM + ROW_NUMBER OVER (...)
# -----------------------------------------------------------------------------

def test_duckdb_render_subquery_from():
  """
  Verifies that a LogicalSelect with SubquerySource in FROM will be correctly rendered.
  """
  dialect = DuckDBDialect()

  inner = LogicalSelect(
    from_=SourceTable(schema="raw", name="customer_raw", alias="r"),
    select_list=[
      SelectItem(
        expr=ColumnRef(table_alias="r", column_name="id"),
        alias="id",
      ),
      SelectItem(
        expr=ColumnRef(table_alias="r", column_name="name"),
        alias="name",
      ),
    ],
  )

  outer = LogicalSelect(
    from_=SubquerySource(select=inner, alias="u"),
    select_list=[
      SelectItem(
        expr=ColumnRef(table_alias="u", column_name="id"),
        alias="id",
      ),
    ],
  )

  sql = dialect.render_select(outer)

  # Rough structure check
  assert "FROM" in sql
  assert "customer_raw" in sql
  # Subquery alias has to appear
  assert re.search(r"\)\s+AS\s+u\b", sql, re.IGNORECASE)
  # SELECT part should only project 'id'
  select_part = sql.split("FROM", 1)[0]
  assert "id" in select_part
  assert "name" not in select_part  # inner column is no longer visible


def test_duckdb_render_row_number_over():
  """
  Verifies if row_number_over(...) will be correctly rendered as ROW_NUMBER() OVER (...).
  """
  dialect = DuckDBDialect()

  inner = LogicalSelect(
    from_=SourceTable(schema="stage", name="s_customer", alias="s"),
    select_list=[
      SelectItem(
        expr=ColumnRef(table_alias="s", column_name="id"),
        alias="id",
      ),
      SelectItem(
        expr=ColumnRef(table_alias="s", column_name="load_ts"),
        alias="load_ts",
      ),
    ],
  )

  outer = LogicalSelect(
    from_=SubquerySource(select=inner, alias="u"),
    select_list=[
      SelectItem(
        expr=ColumnRef(table_alias="u", column_name="id"),
        alias="id",
      ),
      SelectItem(
        expr=row_number_over(
          partition_by=[ColumnRef("u", "id")],
          order_by=[ColumnRef("u", "load_ts")],
        ),
        alias="_rn",
      ),
    ],
  )

  sql = dialect.render_select(outer)

  assert "ROW_NUMBER()" in sql
  assert "OVER (" in sql
  assert "PARTITION BY" in sql
  assert "ORDER BY" in sql
  # Column Aliases should be quoted, depending on dialect implementation
  assert "u" in sql


# -----------------------------------------------------------------------------
# 2. Ranking-Plan vs. Identity-Plan (with hidden __src_rank_ord)
# -----------------------------------------------------------------------------

def _build_fake_union_with_hidden_rank() -> LogicalUnion:
  """
  Builds a LogicalUnion with two Branches, each of them containing
  a hidden column __src_rank_ord – according to the way, the builder
  would do per branch after _attach_hidden_rank_ordinal.
  """
  s1 = LogicalSelect(
    from_=SourceTable(schema="raw", name="customer_raw_1", alias="r1"),
    select_list=[
      SelectItem(
        expr=ColumnRef("r1", "customer_id"),
        alias="customer_id",
      ),
      SelectItem(
        expr=ColumnRef("r1", "name"),
        alias="name",
      ),
      SelectItem(
        expr=RawSql("10"),  # simulated source_identity_ordinal
        alias="__src_rank_ord",
      ),
    ],
  )

  s2 = LogicalSelect(
    from_=SourceTable(schema="raw", name="customer_raw_2", alias="r2"),
    select_list=[
      SelectItem(
        expr=ColumnRef("r2", "customer_id"),
        alias="customer_id",
      ),
      SelectItem(
        expr=ColumnRef("r2", "name"),
        alias="name",
      ),
      SelectItem(
        expr=RawSql("20"),
        alias="__src_rank_ord",
      ),
    ],
  )

  return LogicalUnion(selects=[s1, s2], union_type="ALL")


def _build_fake_stage_dataset_for_ranking() -> FakeTargetDataset:
  """
  Simulates a Stage-TargetDataset with:
    - business key: customer_id
    - additional column: name
  """
  cols = [
    FakeTargetColumn(
      target_column_name="customer_id",
      active=True,
      system_role="business_key",
      ordinal_position=1,
      id_=1,
    ),
    FakeTargetColumn(
      target_column_name="name",
      active=True,
      system_role="business_key",
      ordinal_position=2,
      id_=2,
    ),
  ]
  return FakeTargetDataset(columns=cols)


def test_build_ranked_stage_union_uses_hidden_rank_ord():
  """
  Non-Identity-Mode:
    - UNION ALL with hidden __src_rank_ord per Branch
    - Ranking via ROW_NUMBER() OVER (PARTITION BY BK ORDER BY __src_rank_ord)
    - outer WHERE-Klausel filters on _rn = 1
    - __src_rank_ord is not visible in outer SELECT
  """
  dialect = DuckDBDialect()
  target_ds = _build_fake_stage_dataset_for_ranking()
  union_plan = _build_fake_union_with_hidden_rank()

  plan = _build_ranked_stage_union(target_ds, union_plan)
  sql = dialect.render_select(plan)

  # ROW_NUMBER-Ranking active
  assert "ROW_NUMBER()" in sql
  assert "OVER (" in sql
  assert "PARTITION BY" in sql
  assert "__src_rank_ord" in sql  # has to appear in the ORDER BY clause

  # Filter on _rn = 1
  assert "_rn = 1" in sql

  # Hidden column should not appear in outer SELECT
  outer_select_part = sql.split("FROM", 1)[0]
  assert "__src_rank_ord" not in outer_select_part
