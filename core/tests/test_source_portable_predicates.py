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

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from metadata.rendering.builder import (
  build_source_dataset_select_sql,
  build_source_dataset_where_expr,
  source_increment_filter_uses_delta_cutoff,
)
from metadata.rendering.dialects.dialect_factory import get_active_dialect
from metadata.rendering.expr import BooleanOp, ColumnRef, Comparison, Literal
from metadata.rendering.placeholders import resolve_delta_cutoff_for_source_dataset
from metadata.rendering.portable_expression import (
  PortableExpressionError,
  format_expression_contract,
  normalize_predicate_contract,
  predicate_contract_to_ast,
)


class _SourceColumns:
  def __init__(self, *names):
    self._columns = [SimpleNamespace(source_column_name=name) for name in names]

  def all(self):
    return list(self._columns)


def _source_dataset(**overrides):
  values = {
    "source_columns": _SourceColumns("created_at", "updated_at", "year_no"),
    "static_filter": "",
    "portable_static_filter": None,
    "incremental": True,
    "increment_filter": "",
    "portable_increment_filter": None,
  }
  values.update(overrides)
  return SimpleNamespace(**values)


def _increment_contract():
  return {
    "version": 1,
    "expression": {
      "type": "boolean",
      "operator": "OR",
      "parts": [
        {
          "type": "comparison",
          "operator": ">",
          "left": {"type": "column", "name": "created_at"},
          "right": {"type": "runtime_value", "name": "delta_cutoff"},
        },
        {
          "type": "comparison",
          "operator": ">",
          "left": {"type": "column", "name": "updated_at"},
          "right": {"type": "runtime_value", "name": "delta_cutoff"},
        },
      ],
    },
  }


def test_runtime_value_is_rejected_outside_an_explicit_context():
  with pytest.raises(PortableExpressionError, match="not allowed"):
    normalize_predicate_contract(_increment_contract())


def test_delta_cutoff_can_be_resolved_to_a_runtime_literal():
  expr = predicate_contract_to_ast(
    _increment_contract(),
    column_resolver=lambda name: ColumnRef(table_alias="s", column_name=name),
    allowed_runtime_values=("delta_cutoff",),
    runtime_value_resolver=lambda name: Literal("2026-10-15T00:00:00"),
  )

  assert isinstance(expr, BooleanOp)
  assert expr.operator == "OR"
  assert len(expr.parts) == 2
  assert all(isinstance(part, Comparison) for part in expr.parts)
  assert all(isinstance(part.right, Literal) for part in expr.parts)


def test_source_increment_filter_prefers_portable_metadata_over_sql_fallback():
  dataset = _source_dataset(
    increment_filter="created_at > {{DELTA_CUTOFF}}",
    portable_increment_filter=_increment_contract(),
  )

  expr = build_source_dataset_where_expr(
    dataset,
    delta_cutoff="2026-10-15T00:00:00",
  )

  assert isinstance(expr, BooleanOp)
  assert expr.operator == "OR"
  assert source_increment_filter_uses_delta_cutoff(dataset) is True


def test_source_filter_fails_closed_for_unknown_columns():
  dataset = _source_dataset(
    portable_static_filter={
      "version": 1,
      "expression": {
        "type": "comparison",
        "operator": "=",
        "left": {"type": "column", "name": "missing_column"},
        "right": {"type": "literal", "value": 1},
      },
    },
  )

  with pytest.raises(ValueError, match="unknown SourceDataset column"):
    build_source_dataset_where_expr(dataset)


def test_source_static_and_increment_filters_are_combined_with_and():
  dataset = _source_dataset(
    portable_static_filter={
      "version": 1,
      "expression": {
        "type": "comparison",
        "operator": "=",
        "left": {"type": "column", "name": "year_no"},
        "right": {"type": "literal", "value": 2026},
      },
    },
    portable_increment_filter=_increment_contract(),
  )

  expr = build_source_dataset_where_expr(
    dataset,
    delta_cutoff="2026-10-15T00:00:00",
  )

  assert isinstance(expr, BooleanOp)
  assert expr.operator == "AND"
  assert len(expr.parts) == 2
  assert isinstance(expr.parts[0], Comparison)
  assert isinstance(expr.parts[1], BooleanOp)


def test_source_static_filter_prefers_portable_metadata_over_sql_fallback():
  dataset = _source_dataset(
    static_filter="year_no = 1999",
    portable_static_filter={
      "version": 1,
      "expression": {
        "type": "comparison",
        "operator": "=",
        "left": {"type": "column", "name": "year_no"},
        "right": {"type": "literal", "value": 2026},
      },
    },
    incremental=False,
  )

  expr = build_source_dataset_where_expr(dataset)

  assert isinstance(expr, Comparison)
  assert isinstance(expr.left, ColumnRef)
  assert expr.left.column_name == "year_no"
  assert isinstance(expr.right, Literal)
  assert expr.right.value == 2026

def test_portable_filter_display_uses_user_facing_delta_cutoff_label():
  display = format_expression_contract(
    _increment_contract(),
    allowed_runtime_values=("delta_cutoff",),
  )

  assert "Delta cutoff" in display
  assert "runtime_value" not in display
  assert "created_at" in display
  assert "updated_at" in display


def test_source_select_preview_reuses_runtime_filter_builder_and_keeps_cutoff_symbolic():
  dataset = _source_dataset(portable_increment_filter=_increment_contract())
  dataset.schema_name = "Sales"
  dataset.source_dataset_name = "SalesOrderHeader"
  dialect = get_active_dialect("mssql")

  sql = build_source_dataset_select_sql(
    dataset,
    dialect=dialect,
    source_column_names=["created_at", "updated_at"],
  )

  assert sql.startswith("SELECT ")
  assert "SalesOrderHeader" in sql
  assert "{{DELTA_CUTOFF}}" in sql
  assert "created_at" in sql
  assert "updated_at" in sql

class _PolicyQuery:
  def __init__(self, policy):
    self._policy = policy
    self.filter_kwargs = None

  def filter(self, **kwargs):
    self.filter_kwargs = kwargs
    return self

  def order_by(self, *args):
    return self

  def first(self):
    return self._policy


def test_delta_cutoff_resolver_accepts_portable_increment_filter_without_legacy_sql():
  policy = SimpleNamespace(
    increment_interval_length=4,
    increment_interval_unit="year",
  )
  policies = _PolicyQuery(policy)
  dataset = _source_dataset(
    increment_filter="",
    portable_increment_filter=_increment_contract(),
    increment_policies=policies,
  )
  profile = SimpleNamespace(name="dev")
  now_ts = datetime(2026, 10, 18, 6, 30, tzinfo=timezone.utc)

  cutoff = resolve_delta_cutoff_for_source_dataset(
    source_dataset=dataset,
    profile=profile,
    now_ts=now_ts,
  )

  assert cutoff == datetime(2022, 10, 18, 6, 30, tzinfo=timezone.utc)
  assert policies.filter_kwargs == {"active": True, "environment": "dev"}


def test_delta_cutoff_resolver_keeps_legacy_increment_filter_fallback():
  policy = SimpleNamespace(
    increment_interval_length=2,
    increment_interval_unit="month",
  )
  policies = _PolicyQuery(policy)
  dataset = _source_dataset(
    increment_filter="created_at > {{DELTA_CUTOFF}}",
    portable_increment_filter=None,
    increment_policies=policies,
  )
  profile = SimpleNamespace(name="dev")
  now_ts = datetime(2026, 10, 18, 6, 30, tzinfo=timezone.utc)

  cutoff = resolve_delta_cutoff_for_source_dataset(
    source_dataset=dataset,
    profile=profile,
    now_ts=now_ts,
  )

  assert cutoff == datetime(2026, 8, 18, 6, 30, tzinfo=timezone.utc)

