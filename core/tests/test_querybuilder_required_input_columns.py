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


class _QS:
  """
  Tiny stand-in for a Django-ish related manager / queryset chain:
    .all().select_related(...).filter(...).order_by(...) -> list
  """
  def __init__(self, items):
    self._items = list(items or [])

  def all(self):
    return self

  def select_related(self, *args, **kwargs):
    return self

  def filter(self, *args, **kwargs):
    return self

  def order_by(self, *args, **kwargs):
    return list(self._items)

  def __iter__(self):
    return iter(self._items)


class _AggMeasure:
  def __init__(self, input_column_name):
    self.input_column_name = input_column_name
    self.output_name = "turnover_amt"
    self.function = "SUM"
    self.delimiter = None
    self.order_by_column_name = None
    self.distinct = False
    self.ordinal_position = 1


class _AggGroupKey:
  def __init__(self, input_column_name):
    self.input_column_name = input_column_name
    self.output_name = None
    self.ordinal_position = 1


class _UnionOutputCol:
  def __init__(self, output_name):
    self.id = 1
    self.output_name = output_name
    self.ordinal_position = 1
    self.active = True


def test_aggregate_passes_required_input_columns_to_select(monkeypatch):
  """
  Regression test:
  Aggregate measures may reference an input column that is not part of the aggregate output
  (e.g. subtotal_amt). The builder must pass required_input_columns down to the SELECT
  compilation of the input node.
  """
  import metadata.rendering.builder as builder
  from metadata.rendering.logical_plan import LogicalSelect, SourceTable

  captured = {}

  def fake_build_plan_from_dataset_definition(td, required_input_columns=None):
    captured["required_input_columns"] = set(required_input_columns or set())
    # Return a real LogicalSelect so the aggregate builder accepts it.
    return LogicalSelect(from_=SourceTable(schema="rawcore", name="rc_test", alias="s"))

  monkeypatch.setattr(
    builder,
    "_build_plan_from_dataset_definition",
    fake_build_plan_from_dataset_definition,
    raising=True,
  )

  class FakeTD:
    pass

  class FakeSelectNode:
    node_type = "select"
    target_dataset = FakeTD()

  class FakeAgg:
    input_node = FakeSelectNode()
    group_keys = _QS([_AggGroupKey("rc_aw_customer_key")])
    measures = _QS([_AggMeasure("subtotal_amt")])

  class FakeAggNode:
    node_type = "aggregate"
    aggregate = FakeAgg()

  builder._build_plan_for_query_node(FakeAggNode(), required_input_columns=None)

  assert "required_input_columns" in captured
  assert "subtotal_amt" in captured["required_input_columns"]
  assert "rc_aw_customer_key" in captured["required_input_columns"]


def test_union_builder_accepts_required_input_columns_parameter(monkeypatch):
  """
  Regression test:
  Union plan builder must accept required_input_columns to keep the call graph consistent.
  The union node also requires output_columns (contract) and branch mappings, so we provide them.
  """
  import metadata.rendering.builder as builder
  from metadata.rendering.logical_plan import LogicalSelect, SourceTable

  def fake_build_plan_from_dataset_definition(td, required_input_columns=None):
    return LogicalSelect(from_=SourceTable(schema="raw", name="t", alias="s"))

  monkeypatch.setattr(
    builder,
    "_build_plan_from_dataset_definition",
    fake_build_plan_from_dataset_definition,
    raising=True,
  )

  class FakeSelectNode:
    node_type = "select"
    target_dataset = object()

  class FakeBranchMapping:
    def __init__(self, output_name, input_name):
      # Name-based fields (often used for rendering)
      self.output_column_name = output_name
      self.input_column_name = input_name

      # FK-style fields (builder may use *_id for lookup)
      self.output_column_id = 1
      self.input_column_id = 1

      # Common metadata fields
      self.active = True
      self.ordinal_position = 1
      self.id = 1

  class FakeBranch:
    input_node = FakeSelectNode()
    ordinal_position = 1
    id = 1
    mappings = _QS([FakeBranchMapping("x", "x")])

  class FakeUnion:
    branches = _QS([FakeBranch()])
    output_columns = _QS([_UnionOutputCol("x")])

  class FakeUnionNode:
    node_type = "union"
    union = FakeUnion()

  builder._build_plan_for_query_node(FakeUnionNode(), required_input_columns={"x"})
