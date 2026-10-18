"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025-2026 Ilona Tag

This file is part of elevata.

Tests for completed DISTINCT, aggregate, and window semantics.
"""

from types import SimpleNamespace

import pytest

from metadata.constants import WINDOW_FUNCTION_CHOICES
from metadata.generation.validators import _window_frame_boundary_position
from metadata.generation.window_fn_registry import get_window_fn_spec
from metadata.rendering.builder import _apply_dataset_distinct
from metadata.rendering.dialects.dialect_factory import get_active_dialect
from metadata.rendering.expr import (
  ColumnRef,
  FuncCall,
  Literal,
  OrderByClause,
  OrderByExpr,
  WindowFrame,
  WindowFrameBoundary,
  WindowFunction,
  WindowSpec,
)
from metadata.rendering.logical_plan import LogicalSelect, LogicalUnion, SelectItem, SourceTable, SubquerySource


DIALECT_NAMES = [
  "bigquery",
  "databricks",
  "duckdb",
  "fabric_warehouse",
  "mssql",
  "postgres",
  "snowflake",
]


def _select(alias: str = "s") -> LogicalSelect:
  return LogicalSelect(
    from_=SourceTable(schema="bizcore", name="bc_sales", alias=alias),
    select_list=[
      SelectItem(expr=ColumnRef(table_alias=alias, column_name="customer_id"), alias="customer_id"),
      SelectItem(expr=ColumnRef(table_alias=alias, column_name="amount"), alias="amount"),
    ],
  )


def test_dataset_distinct_marks_final_select_only():
  plan = _select()
  target_dataset = SimpleNamespace(distinct_select=True)

  result = _apply_dataset_distinct(plan, target_dataset)

  assert result is plan
  assert result.distinct is True


def test_dataset_distinct_wraps_union_without_changing_union_semantics():
  union = LogicalUnion(selects=[_select("a"), _select("b")], union_type="ALL")
  target_dataset = SimpleNamespace(distinct_select=True)

  result = _apply_dataset_distinct(union, target_dataset)

  assert isinstance(result, LogicalSelect)
  assert result.distinct is True
  assert isinstance(result.from_, SubquerySource)
  assert result.from_.select is union
  assert union.union_type == "ALL"
  assert [item.alias for item in result.select_list] == ["customer_id", "amount"]


@pytest.mark.parametrize("dialect_name", DIALECT_NAMES)
@pytest.mark.parametrize("function_name", ["COUNT", "SUM", "AVG", "MIN", "MAX"])
def test_aggregate_distinct_renders_as_portable_sql(dialect_name: str, function_name: str):
  dialect = get_active_dialect(dialect_name)
  expr = FuncCall(
    name=f"{function_name}_DISTINCT",
    args=[ColumnRef(table_alias="s", column_name="amount")],
  )

  sql = dialect.render_expr(expr)

  assert sql.startswith(f"{function_name}(DISTINCT ")
  assert "amount" in sql
  assert sql.endswith(")")


@pytest.mark.parametrize("dialect_name", DIALECT_NAMES)
def test_count_star_window_renders_portably(dialect_name: str):
  dialect = get_active_dialect(dialect_name)
  expr = WindowFunction(
    name="COUNT",
    args=[],
    window=WindowSpec(partition_by=[ColumnRef(table_alias="s", column_name="customer_id")]),
  )

  sql = dialect.render_expr(expr)

  assert sql.startswith("COUNT(*) OVER (")
  assert "PARTITION BY" in sql
  assert "customer_id" in sql


@pytest.mark.parametrize("dialect_name", DIALECT_NAMES)
def test_explicit_window_frame_renders_portably(dialect_name: str):
  dialect = get_active_dialect(dialect_name)
  expr = WindowFunction(
    name="LAST_VALUE",
    args=[ColumnRef(table_alias="s", column_name="amount")],
    window=WindowSpec(
      partition_by=[ColumnRef(table_alias="s", column_name="customer_id")],
      order_by=[OrderByExpr(expr=ColumnRef(table_alias="s", column_name="loaded_at"), direction="ASC")],
      frame=WindowFrame(
        frame_type="ROWS",
        start=WindowFrameBoundary(kind="UNBOUNDED_PRECEDING"),
        end=WindowFrameBoundary(kind="UNBOUNDED_FOLLOWING"),
      ),
    ),
  )

  sql = dialect.render_expr(expr)

  assert "LAST_VALUE(" in sql
  assert "ORDER BY" in sql
  assert "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING" in sql


@pytest.mark.parametrize("dialect_name", DIALECT_NAMES)
def test_ordered_string_agg_has_dialect_owned_rendering(dialect_name: str):
  dialect = get_active_dialect(dialect_name)
  expr = FuncCall(
    name="STRING_AGG",
    args=[
      ColumnRef(table_alias="s", column_name="product_name"),
      Literal(","),
      OrderByClause(
        items=[
          OrderByExpr(
            expr=ColumnRef(table_alias="s", column_name="product_name"),
            direction="ASC",
          )
        ]
      ),
    ],
  )

  sql = dialect.render_expr(expr)

  if dialect_name in {"snowflake", "databricks"}:
    assert sql.startswith("LISTAGG(")
    assert " WITHIN GROUP (ORDER BY " in sql
  elif dialect_name in {"mssql", "fabric_warehouse"}:
    assert sql.startswith("STRING_AGG(")
    assert " WITHIN GROUP (ORDER BY " in sql
  elif dialect_name == "duckdb":
    assert sql.startswith("string_agg(")
    assert " ORDER BY " in sql
  else:
    assert sql.startswith("STRING_AGG(")
    assert " ORDER BY " in sql


def test_portable_window_function_surface_matches_registry_contract():
  names = {value for value, _label in WINDOW_FUNCTION_CHOICES}

  assert "FIRST_VALUE" in names
  assert "LAST_VALUE" in names
  assert "SUM" in names
  assert "AVG" in names
  assert "MIN" in names
  assert "MAX" in names
  assert "COUNT" in names
  assert "NTH_VALUE" not in names
  assert get_window_fn_spec("NTH_VALUE") is None
  assert get_window_fn_spec("FIRST_VALUE").frame_policy == "required"
  assert get_window_fn_spec("LAST_VALUE").frame_policy == "required"
  assert get_window_fn_spec("SUM").frame_policy == "optional"
  assert get_window_fn_spec("ROW_NUMBER").frame_policy == "forbidden"


def test_lag_lead_defaults_are_literal_only_for_portability():
  """Keep LAG/LEAD defaults portable to BigQuery constant-expression semantics."""
  assert get_window_fn_spec("LAG").arg_schema[2] == ["int", "str"]
  assert get_window_fn_spec("LEAD").arg_schema[2] == ["int", "str"]


def test_window_frame_boundary_positions_reject_reversed_ranges():
  start = _window_frame_boundary_position("FOLLOWING", 5)
  end = _window_frame_boundary_position("CURRENT_ROW", None)

  assert start > end
  assert _window_frame_boundary_position("PRECEDING", 10) < _window_frame_boundary_position("PRECEDING", 5)
