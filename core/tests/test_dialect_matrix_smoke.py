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

"""
Cross-dialect smoke tests.

The goal is not to assert exact SQL strings for each dialect, but to
ensure that essential operations work for all registered dialects:
- rendering literals
- rendering a minimal LogicalSelect
- rendering a small HASH256 DSL expression
"""

import datetime

import pytest

from metadata.rendering.dialects.dialect_factory import (
  get_available_dialect_names,
  get_active_dialect,
)
from metadata.rendering.dsl import parse_surrogate_dsl
from metadata.rendering.logical_plan import LogicalSelect, SelectItem, SourceTable
from metadata.rendering.expr import ColumnRef


DSL_HASH = "HASH256(CONCAT_WS('|', COALESCE({expr:a}, '<NULL>'), 'pepper'))"


@pytest.mark.parametrize("dialect_name", get_available_dialect_names())
def test_literal_rendering_smoke(dialect_name: str):
  dialect = get_active_dialect(dialect_name)

  assert dialect.render_literal(True)
  assert dialect.render_literal(False)
  assert dialect.render_literal(None)
  assert dialect.render_literal(42)
  assert dialect.render_literal(3.14)

  dval = datetime.date(2025, 1, 2)
  assert dialect.render_literal(dval)


@pytest.mark.parametrize("dialect_name", get_available_dialect_names())
def test_minimal_logical_select_rendering_smoke(dialect_name: str):
  dialect = get_active_dialect(dialect_name)

  # FROM raw.customer AS c
  source = SourceTable(schema="raw", name="customer", alias="c")
  select = LogicalSelect(
    from_=source,
    select_list=[
      SelectItem(expr=ColumnRef(table_alias="c", column_name="id"), alias="id"),
    ],
  )

  sql = dialect.render_select(select)

  assert isinstance(sql, str)
  assert "from" in sql.lower()
  assert "select" in sql.lower()


@pytest.mark.parametrize("dialect_name", get_available_dialect_names())
def test_hash256_dsl_smoke(dialect_name: str):
  """
  Ensure that the HASH256 surrogate DSL can be rendered for every dialect.
  Detailed dialect-specific expectations are covered in dedicated tests.
  """
  dialect = get_active_dialect(dialect_name)

  expr = parse_surrogate_dsl(DSL_HASH, table_alias="t")
  sql = dialect.render_expr(expr)

  assert isinstance(sql, str)
  assert sql  # non-empty

@pytest.mark.parametrize("dialect_name", get_available_dialect_names())
def test_merge_statement_smoke(dialect_name: str):
  """
  Cross-dialect smoke test for MERGE / UPSERT behavior.

  - Dialects that declare supports_merge=False must raise NotImplementedError.
  - Dialects that support MERGE must return syntactically plausible SQL.
  """

  dialect = get_active_dialect(dialect_name)

  # Minimal synthetic SELECT used as merge source
  select_sql = "SELECT 1 AS id, 'x' AS payload"

  if not getattr(dialect, "supports_merge", False):
    # Dialects may still provide a default implementation in base.
    try:
      sql = dialect.render_merge_statement(
        schema="dw",
        table="dim_dummy",
        select_sql=select_sql,
        unique_key_columns=["id"],
        update_columns=["payload"],
      )
    except NotImplementedError:
      return
  else:
    sql = dialect.render_merge_statement(
      schema="dw",
      table="dim_dummy",
      select_sql=select_sql,
      unique_key_columns=["id"],
      update_columns=["payload"],
    )

  assert isinstance(sql, str)
  lower = sql.lower()

  # Should reference table + select
  assert "dim_dummy" in sql
  assert "dw" in sql
  assert "select" in lower
  assert "id" in lower  # key column should appear

  # Dialects may implement MERGE or INSERT ... ON CONFLICT
  assert (
    "merge" in lower
    or ("insert into" in lower and "on conflict" in lower)
  )

@pytest.mark.parametrize("dialect_name", get_available_dialect_names())
def test_delete_detection_smoke(dialect_name: str):
  """
  Cross-dialect smoke test for delete detection support.

  Dialects that declare supports_delete_detection=False must raise
  NotImplementedError.

  Dialects that support delete detection must return syntactically
  plausible SQL including:
    - DELETE FROM or equivalent
    - the target + stage table names
    - join predicates
  """

  dialect = get_active_dialect(dialect_name)

  # Synthetic table names (existence irrelevant)
  tgt_schema = "dw"
  tgt_table = "dim_dummy"
  stg_schema = "stg"
  stg_table = "dim_dummy_stage"
  join_preds = ["t.id = s.id"]

  if not getattr(dialect, "supports_delete_detection", False):
    # Dialects may still provide a default implementation in base.
    try:
      sql = dialect.render_delete_detection_statement(
        target_schema=tgt_schema,
        target_table=tgt_table,
        stage_schema=stg_schema,
        stage_table=stg_table,
        join_predicates=join_preds,
      )
    except NotImplementedError:
      return
  else:
    sql = dialect.render_delete_detection_statement(
      target_schema=tgt_schema,
      target_table=tgt_table,
      stage_schema=stg_schema,
      stage_table=stg_table,
      join_predicates=join_preds,
    )

  assert isinstance(sql, str)
  lower = sql.lower()

  # table names must appear
  assert tgt_table in lower
  assert stg_table in lower

  # join predicate must appear
  assert "id" in lower

  # dialect may implement DELETE FROM ... USING ...
  # or DELETE ... WHERE NOT EXISTS ...
  assert (
      "delete" in lower
      or "not exists" in lower
      or "using" in lower
  )
