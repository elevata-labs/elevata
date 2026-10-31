"""Tests for portable TargetColumn and TargetColumnInput expression resolution."""

import pytest

from metadata.rendering.builder import _configured_column_expression
from metadata.rendering.column_expression import column_expression_contract_to_ast
from metadata.rendering.expr import Arithmetic, ColumnRef, FuncCall
from metadata.rendering.portable_expression import PortableExpressionError


class _UpstreamColumn:
  def __init__(self, name):
    self.target_column_name = name


class _Link:
  def __init__(self, name, *, portable_expression=None):
    self.upstream_target_column = _UpstreamColumn(name)
    self.source_column = None
    self.portable_expression = portable_expression


class _TargetColumn:
  def __init__(self, *, portable_expression=None):
    self.portable_expression = portable_expression


def _contract(expression):
  return {"version": 1, "expression": expression}


def _column(name):
  return {"type": "column", "name": name}


def test_input_portable_expression_overrides_column_expression():
  input_contract = _contract({
    "type": "function",
    "name": "UPPER",
    "args": [_column("source_name")],
  })
  column_contract = _contract(_column("source_name"))
  link = _Link("source_name", portable_expression=input_contract)
  tcol = _TargetColumn(portable_expression=column_contract)

  expr = _configured_column_expression(
    tcol,
    selected_input=link,
    available_links=[link],
    alias_resolver=lambda _link: "s",
  )

  assert isinstance(expr, FuncCall)
  assert expr.name == "UPPER"
  assert isinstance(expr.args[0], ColumnRef)
  assert expr.args[0].table_alias == "s"
  assert expr.args[0].column_name == "source_name"


def test_column_portable_expression_is_used_without_input_override():
  link = _Link("amount")
  tcol = _TargetColumn(
    portable_expression=_contract({
      "type": "arithmetic",
      "operator": "*",
      "left": _column("amount"),
      "right": {"type": "literal", "value": 2},
    })
  )

  expr = _configured_column_expression(
    tcol,
    selected_input=link,
    available_links=[link],
    alias_resolver=lambda _link: "s",
  )

  assert isinstance(expr, Arithmetic)
  assert isinstance(expr.left, ColumnRef)
  assert expr.left.table_alias == "s"


def test_portable_expression_rejects_unknown_lineage_column():
  link = _Link("known_column")
  contract = _contract(_column("missing_column"))

  with pytest.raises(PortableExpressionError, match="unknown input column"):
    column_expression_contract_to_ast(
      contract,
      links=[link],
      alias_resolver=lambda _link: "s",
    )


def test_portable_expression_rejects_ambiguous_join_column():
  left = _Link("status")
  right = _Link("status")
  aliases = {id(left): "i1", id(right): "i2"}
  contract = _contract(_column("status"))

  with pytest.raises(PortableExpressionError, match="ambiguous"):
    column_expression_contract_to_ast(
      contract,
      links=[left, right],
      alias_resolver=lambda link: aliases[id(link)],
    )
