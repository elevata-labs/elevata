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

from __future__ import annotations

import pytest

from metadata.rendering.expr import (
  Arithmetic,
  BooleanOp,
  CaseWhen,
  ColumnRef,
  Comparison,
  InList,
  Literal,
  RawSql,
  UnaryArithmetic,
)
from metadata.rendering.portable_expression import (
  PortableExpressionError,
  ast_to_expression_contract,
  canonical_expression_json,
  expression_contract_to_ast,
  expression_fingerprint,
  normalize_expression_contract,
)


def _business_case_contract():
  return {
    "version": 1,
    "expression": {
      "type": "case",
      "branches": [
        {
          "when": {
            "type": "comparison",
            "operator": ">",
            "left": {"type": "column", "name": "quantity"},
            "right": {"type": "literal", "value": 0},
          },
          "then": {
            "type": "arithmetic",
            "operator": "/",
            "left": {"type": "column", "name": "amount"},
            "right": {"type": "column", "name": "quantity"},
          },
        }
      ],
      "else": {"type": "literal", "value": 0},
    },
  }


def test_portable_expression_roundtrip_preserves_business_semantics():
  contract = _business_case_contract()

  ast = expression_contract_to_ast(contract)
  roundtrip = ast_to_expression_contract(ast)

  assert isinstance(ast, CaseWhen)
  assert roundtrip == normalize_expression_contract(contract)


def test_column_resolver_binds_logical_columns_without_persisted_aliases():
  contract = {
    "version": 1,
    "expression": {"type": "column", "name": "amount"},
  }

  ast = expression_contract_to_ast(
    contract,
    column_resolver=lambda name: ColumnRef(table_alias="s", column_name=name),
  )

  assert ast == ColumnRef(table_alias="s", column_name="amount")
  assert ast_to_expression_contract(ast) == contract


def test_boolean_predicate_supports_nested_comparison_and_in_list():
  contract = {
    "version": 1,
    "expression": {
      "type": "boolean",
      "operator": "and",
      "parts": [
        {
          "type": "comparison",
          "operator": ">=",
          "left": {"type": "column", "name": "amount"},
          "right": {"type": "literal", "value": 100},
        },
        {
          "type": "in",
          "expression": {"type": "column", "name": "status"},
          "values": [
            {"type": "literal", "value": "ACTIVE"},
            {"type": "literal", "value": "PENDING"},
          ],
          "negated": False,
        },
      ],
    },
  }

  ast = expression_contract_to_ast(contract)

  assert isinstance(ast, BooleanOp)
  assert ast.operator == "AND"
  assert isinstance(ast.parts[0], Comparison)
  assert isinstance(ast.parts[1], InList)


def test_canonical_json_and_fingerprint_ignore_object_key_order():
  first = _business_case_contract()
  second = {
    "expression": first["expression"],
    "version": 1,
  }

  assert canonical_expression_json(first) == canonical_expression_json(second)
  assert expression_fingerprint(first) == expression_fingerprint(second)


def test_unknown_contract_keys_fail_closed():
  contract = {
    "version": 1,
    "expression": {
      "type": "column",
      "name": "amount",
      "sql": "amount + 1",
    },
  }

  with pytest.raises(PortableExpressionError, match="unknown keys: sql"):
    normalize_expression_contract(contract)


def test_unknown_function_fails_closed():
  contract = {
    "version": 1,
    "expression": {
      "type": "function",
      "name": "SOME_VENDOR_FUNCTION",
      "args": [{"type": "column", "name": "amount"}],
    },
  }

  with pytest.raises(PortableExpressionError, match="Unsupported portable function"):
    normalize_expression_contract(contract)


def test_invalid_operator_fails_closed():
  contract = {
    "version": 1,
    "expression": {
      "type": "arithmetic",
      "operator": "**",
      "left": {"type": "column", "name": "amount"},
      "right": {"type": "literal", "value": 2},
    },
  }

  with pytest.raises(PortableExpressionError, match="Unsupported arithmetic operator"):
    normalize_expression_contract(contract)


def test_raw_sql_ast_cannot_be_serialized_as_portable_expression():
  with pytest.raises(PortableExpressionError, match="not portable: RawSql"):
    ast_to_expression_contract(RawSql("ISNULL(amount, 0)"))


def test_ast_serialization_rejects_single_part_boolean_expression():
  ast = BooleanOp(
    operator="AND",
    parts=[Comparison(ColumnRef(None, "amount"), ">", Literal(0))],
  )

  with pytest.raises(PortableExpressionError, match="at least two parts"):
    ast_to_expression_contract(ast)


def test_arithmetic_ast_serializes_to_portable_contract():
  ast = Arithmetic(
    left=ColumnRef(None, "quantity"),
    operator="*",
    right=ColumnRef(None, "unit_price"),
  )

  assert ast_to_expression_contract(ast) == {
    "version": 1,
    "expression": {
      "type": "arithmetic",
      "operator": "*",
      "left": {"type": "column", "name": "quantity"},
      "right": {"type": "column", "name": "unit_price"},
    },
  }


def test_unary_arithmetic_roundtrip():
  contract = {
    "version": 1,
    "expression": {
      "type": "unary_arithmetic",
      "operator": "-",
      "expression": {"type": "column", "name": "amount"},
    },
  }

  ast = expression_contract_to_ast(contract)

  assert ast == UnaryArithmetic(
    operator="-",
    expr=ColumnRef(table_alias=None, column_name="amount"),
  )
  assert ast_to_expression_contract(ast) == contract


def test_function_arity_fails_closed():
  contract = {
    "version": 1,
    "expression": {
      "type": "function",
      "name": "NULLIF",
      "args": [{"type": "column", "name": "amount"}],
    },
  }

  with pytest.raises(PortableExpressionError, match="NULLIF requires 2 argument"):
    normalize_expression_contract(contract)


def test_vendor_specific_cast_type_fails_closed():
  contract = {
    "version": 1,
    "expression": {
      "type": "cast",
      "expression": {"type": "column", "name": "amount"},
      "target_type": "NVARCHAR(50)",
    },
  }

  with pytest.raises(PortableExpressionError, match="Unsupported portable CAST type"):
    normalize_expression_contract(contract)


def test_non_finite_numeric_literal_fails_closed():
  contract = {
    "version": 1,
    "expression": {"type": "literal", "value": float("nan")},
  }

  with pytest.raises(PortableExpressionError, match="Literal value"):
    normalize_expression_contract(contract)
