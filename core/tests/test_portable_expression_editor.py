"""Tests for portable-expression authoring forms and predicate semantics."""

import json

import pytest
from django.core.exceptions import ValidationError

from metadata.forms import (
  PortableExpressionFormField,
  TargetColumnForm,
  TargetColumnInputForm,
)
from metadata.rendering.portable_expression import (
  PortableExpressionError,
  normalize_expression_contract,
)
from metadata.widgets.portable_expression_widget import PortableExpressionWidget


def _contract(expression):
  return {"version": 1, "expression": expression}


def _column(name):
  return {"type": "column", "name": name}


def test_editor_form_field_normalizes_valid_contract():
  field = PortableExpressionFormField()
  raw = json.dumps(_contract({
    "type": "function",
    "name": "lower",
    "args": [_column("customer_name")],
  }))

  cleaned = field.clean(raw)

  assert cleaned["version"] == 1
  assert cleaned["expression"]["name"] == "LOWER"


def test_editor_form_field_allows_clearing_expression():
  field = PortableExpressionFormField()

  assert field.clean("") is None


def test_editor_form_field_rejects_invalid_contract():
  field = PortableExpressionFormField()

  with pytest.raises(ValidationError, match="Unsupported expression node type"):
    field.clean(json.dumps(_contract({"type": "vendor_sql", "sql": "NVL(x, 0)"})))


def test_widget_exposes_column_names_without_embedding_contract_structure_in_ui_controls():
  widget = PortableExpressionWidget()
  widget.column_names = ["amount", "customer_name"]

  context = widget.get_context("portable_expression_editor", "", {})

  assert json.loads(context["widget"]["column_names_json"]) == ["amount", "customer_name"]


def test_target_column_forms_expose_structured_editor_field():
  assert "portable_expression_editor" in TargetColumnForm.base_fields
  assert "portable_expression_editor" in TargetColumnInputForm.base_fields


def test_case_when_requires_predicate_semantics():
  contract = _contract({
    "type": "case",
    "branches": [
      {
        "when": _column("status"),
        "then": {"type": "literal", "value": "active"},
      }
    ],
    "else": {"type": "literal", "value": "other"},
  })

  with pytest.raises(PortableExpressionError, match="CASE WHEN requires a predicate expression"):
    normalize_expression_contract(contract)


def test_boolean_parts_require_predicate_semantics():
  contract = _contract({
    "type": "boolean",
    "operator": "AND",
    "parts": [
      {
        "type": "comparison",
        "operator": ">",
        "left": _column("amount"),
        "right": {"type": "literal", "value": 0},
      },
      {
        "type": "arithmetic",
        "operator": "+",
        "left": _column("amount"),
        "right": {"type": "literal", "value": 1},
      },
    ],
  })

  with pytest.raises(PortableExpressionError, match="boolean expression requires a predicate expression"):
    normalize_expression_contract(contract)
