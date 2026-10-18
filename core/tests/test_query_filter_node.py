"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025-2026 Ilona Tag

This file is part of elevata.

Tests for portable FILTER query-node planning.
"""

from types import SimpleNamespace

import pytest

from metadata.generation.query_contract import ContractResult
from metadata.rendering.builder import _build_filter_plan_for_node
from metadata.rendering.expr import ColumnRef, Comparison, Literal
from metadata.rendering.logical_plan import LogicalSelect, SelectItem, SourceTable, SubquerySource


def _contract(*columns: str) -> dict:
  return {
    "version": 1,
    "expression": {
      "type": "comparison",
      "operator": ">",
      "left": {"type": "column", "name": columns[0]},
      "right": {"type": "literal", "value": 0},
    },
  }


def _inner_plan() -> LogicalSelect:
  return LogicalSelect(
    from_=SourceTable(schema="bizcore", name="bc_sales", alias="s"),
    select_list=[
      SelectItem(expr=ColumnRef(table_alias="s", column_name="customer_id"), alias="customer_id"),
      SelectItem(expr=ColumnRef(table_alias="s", column_name="amount"), alias="amount"),
    ],
  )


def test_filter_plan_preserves_input_contract_and_uses_portable_predicate(monkeypatch):
  input_node = SimpleNamespace(id=11)
  filter_node = SimpleNamespace(input_node=input_node, predicate=_contract("amount"))
  node = SimpleNamespace(filter=filter_node)

  monkeypatch.setattr(
    "metadata.rendering.builder.infer_query_node_contract",
    lambda _node: ContractResult(
      columns=["customer_id", "amount"],
      issues=[],
      available_input_columns=["customer_id", "amount"],
    ),
  )
  monkeypatch.setattr(
    "metadata.rendering.builder._build_plan_for_query_node",
    lambda _node, required_input_columns=None: _inner_plan(),
  )

  plan = _build_filter_plan_for_node(node)

  assert isinstance(plan, LogicalSelect)
  assert isinstance(plan.from_, SubquerySource)
  assert plan.from_.alias == "f"
  assert [item.alias for item in plan.select_list] == ["customer_id", "amount"]
  assert isinstance(plan.where, Comparison)
  assert plan.where.operator == ">"
  assert isinstance(plan.where.left, ColumnRef)
  assert plan.where.left.table_alias == "f"
  assert plan.where.left.column_name == "amount"
  assert isinstance(plan.where.right, Literal)
  assert plan.where.right.value == 0


def test_filter_plan_rejects_unknown_predicate_column(monkeypatch):
  input_node = SimpleNamespace(id=12)
  filter_node = SimpleNamespace(input_node=input_node, predicate=_contract("missing_amount"))
  node = SimpleNamespace(filter=filter_node)

  monkeypatch.setattr(
    "metadata.rendering.builder.infer_query_node_contract",
    lambda _node: ContractResult(columns=["amount"], issues=[], available_input_columns=["amount"]),
  )
  monkeypatch.setattr(
    "metadata.rendering.builder._build_plan_for_query_node",
    lambda _node, required_input_columns=None: _inner_plan(),
  )

  with pytest.raises(ValueError, match="unknown input column"):
    _build_filter_plan_for_node(node)


def test_filter_plan_rejects_invalid_input_contract(monkeypatch):
  input_node = SimpleNamespace(id=13)
  filter_node = SimpleNamespace(input_node=input_node, predicate=_contract("amount"))
  node = SimpleNamespace(filter=filter_node)

  monkeypatch.setattr(
    "metadata.rendering.builder.infer_query_node_contract",
    lambda _node: ContractResult(
      columns=["amount"],
      issues=["ERROR: Duplicate output column 'amount'."],
      available_input_columns=["amount"],
    ),
  )

  with pytest.raises(ValueError, match="Filter input contract is invalid"):
    _build_filter_plan_for_node(node)


def test_filter_plan_rejects_non_predicate_root(monkeypatch):
  input_node = SimpleNamespace(id=14)
  filter_node = SimpleNamespace(
    input_node=input_node,
    predicate={
      "version": 1,
      "expression": {
        "type": "arithmetic",
        "operator": "+",
        "left": {"type": "column", "name": "amount"},
        "right": {"type": "literal", "value": 1},
      },
    },
  )
  node = SimpleNamespace(filter=filter_node)

  monkeypatch.setattr(
    "metadata.rendering.builder.infer_query_node_contract",
    lambda _node: ContractResult(columns=["amount"], issues=[], available_input_columns=["amount"]),
  )
  monkeypatch.setattr(
    "metadata.rendering.builder._build_plan_for_query_node",
    lambda _node, required_input_columns=None: _inner_plan(),
  )

  with pytest.raises(ValueError, match="boolean predicate root"):
    _build_filter_plan_for_node(node)


def test_filter_plan_pushes_down_hidden_columns_required_by_downstream(monkeypatch):
  """FILTER must preserve non-projected inputs requested by a downstream aggregate."""
  input_node = SimpleNamespace(id=15)
  filter_node = SimpleNamespace(input_node=input_node, predicate=_contract("bc_dim_product_key"))
  node = SimpleNamespace(filter=filter_node)
  captured = {}

  monkeypatch.setattr(
    "metadata.rendering.builder.infer_query_node_contract",
    lambda _node: ContractResult(
      columns=["bc_dim_product_key"],
      issues=[],
      available_input_columns=["bc_dim_product_key", "product_name"],
    ),
  )

  def fake_build(_node, required_input_columns=None):
    captured["required"] = set(required_input_columns or set())
    return LogicalSelect(
      from_=SourceTable(schema="rawcore", name="rc_aw_product", alias="s"),
      select_list=[
        SelectItem(
          expr=ColumnRef(table_alias="s", column_name="bc_dim_product_key"),
          alias="bc_dim_product_key",
        ),
        SelectItem(
          expr=ColumnRef(table_alias="s", column_name="product_name"),
          alias="product_name",
        ),
      ],
    )

  monkeypatch.setattr("metadata.rendering.builder._build_plan_for_query_node", fake_build)

  plan = _build_filter_plan_for_node(node, required_input_columns={"product_name"})

  assert captured["required"] == {"bc_dim_product_key", "product_name"}
  assert [item.alias for item in plan.select_list] == ["bc_dim_product_key", "product_name"]


def test_filter_plan_rejects_hidden_column_not_exposed_by_input_contract(monkeypatch):
  input_node = SimpleNamespace(id=16)
  filter_node = SimpleNamespace(input_node=input_node, predicate=_contract("bc_dim_product_key"))
  node = SimpleNamespace(filter=filter_node)

  monkeypatch.setattr(
    "metadata.rendering.builder.infer_query_node_contract",
    lambda _node: ContractResult(
      columns=["bc_dim_product_key"],
      issues=[],
      available_input_columns=["bc_dim_product_key", "product_name"],
    ),
  )

  with pytest.raises(ValueError, match="unavailable input columns"):
    _build_filter_plan_for_node(node, required_input_columns={"does_not_exist"})


def test_filter_authoring_columns_use_available_input_contract(monkeypatch):
  """FILTER authoring may reference projected expressions and hidden upstream inputs."""
  from metadata import views

  monkeypatch.setattr(
    views,
    "infer_query_node_contract",
    lambda _node: ContractResult(
      columns=["bc_dim_product_key", "display_name"],
      issues=[],
      available_input_columns=[
        "bc_dim_product_key",
        "display_name",
        "product_id",
        "product_name",
      ],
    ),
  )

  assert views._query_filter_column_names(object()) == [
    "bc_dim_product_key",
    "display_name",
    "product_id",
    "product_name",
  ]


def test_filter_predicate_pushes_down_hidden_available_input(monkeypatch):
  """A FILTER may use a selectable upstream column that is not in visible output."""
  input_node = SimpleNamespace(id=17)
  filter_node = SimpleNamespace(input_node=input_node, predicate=_contract("last_name"))
  node = SimpleNamespace(filter=filter_node)
  captured = {}

  monkeypatch.setattr(
    "metadata.rendering.builder.infer_query_node_contract",
    lambda _node: ContractResult(
      columns=["customer"],
      issues=[],
      available_input_columns=["customer", "account_no", "first_name", "last_name"],
    ),
  )

  def fake_build(_node, required_input_columns=None):
    captured["required"] = set(required_input_columns or set())
    return LogicalSelect(
      from_=SourceTable(schema="rawcore", name="rc_aw_customer", alias="s"),
      select_list=[
        SelectItem(expr=ColumnRef(table_alias="s", column_name="customer"), alias="customer"),
        SelectItem(expr=ColumnRef(table_alias="s", column_name="last_name"), alias="last_name"),
      ],
    )

  monkeypatch.setattr("metadata.rendering.builder._build_plan_for_query_node", fake_build)

  plan = _build_filter_plan_for_node(node)

  assert captured["required"] == {"customer", "last_name"}
  assert [item.alias for item in plan.select_list] == ["customer"]
  assert isinstance(plan.where, Comparison)
  assert plan.where.left.column_name == "last_name"
