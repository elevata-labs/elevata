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


from metadata.services.query_contract_column_sync import QueryContractColumnSyncService

class _QS:
  def __init__(self, items):
    self._items = list(items or [])

  def all(self):
    return self

  def order_by(self, *args, **kwargs):
    return self._items


class _WinCol:
  def __init__(self, output_name, function):
    self.output_name = output_name
    self.function = function
    self.ordinal_position = 1
    self.id = 1


class _AggMeasure:
  def __init__(self, output_name, function, input_column_name=""):
    self.output_name = output_name
    self.function = function
    self.input_column_name = input_column_name
    self.ordinal_position = 1
    self.id = 1


class _AggGroupKey:
  def __init__(self, input_column_name, output_name=""):
    self.input_column_name = input_column_name
    self.output_name = output_name
    self.ordinal_position = 1
    self.id = 1


def _make_td_with_window(cols):
  class _Window:
    def __init__(self, cols):
      self.columns = _QS(cols)

  class _Head:
    node_type = "window"
    def __init__(self, cols):
      self.window = _Window(cols)

  class _TD:
    def __init__(self, cols):
      self.query_head = _Head(cols)
      self.query_root = None

  return _TD(cols)


def _make_td_with_aggregate(measures, group_keys=None):
  class _Agg:
    def __init__(self, measures, group_keys):
      self.measures = _QS(measures)
      self.group_keys = _QS(group_keys)
      self.input_node = type("_In", (), {"target_dataset": object()})()

  class _Head:
    node_type = "aggregate"
    def __init__(self, measures, group_keys):
      self.aggregate = _Agg(measures, group_keys)

  class _TD:
    def __init__(self, measures, group_keys):
      self.query_head = _Head(measures, group_keys)
      self.query_root = None

  return _TD(measures, group_keys or [])


def test_query_derived_row_number_is_int():
  svc = QueryContractColumnSyncService()

  td = _make_td_with_window([_WinCol("row_number", "ROW_NUMBER")])
  out = svc._infer_query_output_types(td)

  assert out["row_number"]["datatype"] in ("INTEGER", "INT")


def test_query_derived_count_is_int():
  svc = QueryContractColumnSyncService()

  td = _make_td_with_aggregate([_AggMeasure("cnt", "COUNT", "any_col")])
  out = svc._infer_query_output_types(td)

  assert out["cnt"]["datatype"] in ("INTEGER", "INT")


def test_query_derived_sum_decimal_widens_integral_headroom(monkeypatch):
  svc = QueryContractColumnSyncService()

  monkeypatch.setattr(
    svc,
    "_infer_type_from_target_dataset",
    lambda td, name: {
      "datatype": "DECIMAL",
      "decimal_precision": 12,
      "decimal_scale": 2,
      "max_length": None,
    } if name == "subtotal_amt" else None,
    raising=True,
  )

  td = _make_td_with_aggregate([_AggMeasure("turnover_amt", "SUM", "subtotal_amt")])
  out = svc._infer_query_output_types(td)

  assert out["turnover_amt"]["datatype"] == "DECIMAL"
  assert out["turnover_amt"]["decimal_precision"] == 22
  assert out["turnover_amt"]["decimal_scale"] == 2


def test_query_derived_sum_integer_widens_to_bigint_when_available(monkeypatch):
  svc = QueryContractColumnSyncService()
  monkeypatch.setattr(
    svc,
    "_allowed_datatypes",
    lambda: {"STRING", "INTEGER", "BIGINT", "DECIMAL", "FLOAT"},
    raising=True,
  )
  monkeypatch.setattr(
    svc,
    "_infer_type_from_target_dataset",
    lambda td, name: {
      "datatype": "INTEGER",
      "decimal_precision": None,
      "decimal_scale": None,
      "max_length": None,
    } if name == "quantity" else None,
    raising=True,
  )

  td = _make_td_with_aggregate([_AggMeasure("quantity_total", "SUM", "quantity")])
  out = svc._infer_query_output_types(td)

  assert out["quantity_total"] == {
    "datatype": "BIGINT",
    "max_length": None,
    "decimal_precision": None,
    "decimal_scale": None,
  }


def test_query_derived_avg_integer_returns_fraction_capable_decimal(monkeypatch):
  svc = QueryContractColumnSyncService()
  monkeypatch.setattr(
    svc,
    "_allowed_datatypes",
    lambda: {"STRING", "INTEGER", "BIGINT", "DECIMAL", "FLOAT"},
    raising=True,
  )
  monkeypatch.setattr(
    svc,
    "_infer_type_from_target_dataset",
    lambda td, name: {
      "datatype": "INTEGER",
      "decimal_precision": None,
      "decimal_scale": None,
      "max_length": None,
    } if name == "quantity" else None,
    raising=True,
  )

  td = _make_td_with_aggregate([_AggMeasure("quantity_avg", "AVG", "quantity")])
  out = svc._infer_query_output_types(td)

  assert out["quantity_avg"] == {
    "datatype": "DECIMAL",
    "max_length": None,
    "decimal_precision": 38,
    "decimal_scale": 10,
  }


def test_query_derived_avg_decimal_expands_fractional_capacity(monkeypatch):
  svc = QueryContractColumnSyncService()
  monkeypatch.setattr(
    svc,
    "_infer_type_from_target_dataset",
    lambda td, name: {
      "datatype": "DECIMAL",
      "decimal_precision": 12,
      "decimal_scale": 2,
      "max_length": None,
    } if name == "subtotal_amt" else None,
    raising=True,
  )

  td = _make_td_with_aggregate([_AggMeasure("turnover_avg_amt", "AVG", "subtotal_amt")])
  out = svc._infer_query_output_types(td)

  assert out["turnover_avg_amt"] == {
    "datatype": "DECIMAL",
    "max_length": None,
    "decimal_precision": 38,
    "decimal_scale": 10,
  }


def test_query_derived_min_preserves_complete_input_type(monkeypatch):
  svc = QueryContractColumnSyncService()
  inferred = {
    "datatype": "DECIMAL",
    "decimal_precision": 12,
    "decimal_scale": 2,
    "max_length": None,
  }
  monkeypatch.setattr(
    svc,
    "_infer_type_from_target_dataset",
    lambda td, name: inferred if name == "subtotal_amt" else None,
    raising=True,
  )

  td = _make_td_with_aggregate([_AggMeasure("minimum_amt", "MIN", "subtotal_amt")])
  out = svc._infer_query_output_types(td)

  assert out["minimum_amt"] == inferred


def test_query_derived_group_key_uses_input_column_type(monkeypatch):
  svc = QueryContractColumnSyncService()

  monkeypatch.setattr(
    svc,
    "_infer_type_from_target_dataset",
    lambda td, name: {
      "datatype": "INTEGER",
      "decimal_precision": None,
      "decimal_scale": None,
      "max_length": None,
    } if name == "product_model_id" else None,
    raising=True,
  )

  td = _make_td_with_aggregate(
    [],
    group_keys=[_AggGroupKey("product_model_id")],
  )
  out = svc._infer_query_output_types(td)

  assert out["product_model_id"]["datatype"] == "INTEGER"


def test_query_derived_group_key_alias_keeps_input_column_type(monkeypatch):
  svc = QueryContractColumnSyncService()

  monkeypatch.setattr(
    svc,
    "_infer_type_from_target_dataset",
    lambda td, name: {
      "datatype": "STRING",
      "decimal_precision": None,
      "decimal_scale": None,
      "max_length": 25,
    } if name == "product_no" else None,
    raising=True,
  )

  td = _make_td_with_aggregate(
    [],
    group_keys=[_AggGroupKey("product_no", "grouped_product_no")],
  )
  out = svc._infer_query_output_types(td)

  assert out["grouped_product_no"]["datatype"] == "STRING"
  assert out["grouped_product_no"]["max_length"] == 25


def test_empty_query_contract_defers_destructive_column_sync():
  """A transient empty query head must never be treated as an empty dataset schema."""
  svc = QueryContractColumnSyncService()
  query_root = object()
  query_head = object()
  td = type("_TD", (), {"query_root": query_root, "query_head": query_head})()

  assert svc._should_defer_empty_contract_sync(td, []) is True


def test_nonempty_query_contract_is_reconciled_normally():
  """Once an output contract exists, normal synchronization must resume."""
  svc = QueryContractColumnSyncService()
  td = type("_TD", (), {"query_root": object(), "query_head": object()})()
  desired = [type("_Col", (), {"name": "customer_id"})()]

  assert svc._should_defer_empty_contract_sync(td, desired) is False


def test_standard_dataset_does_not_defer_empty_contract_sync():
  """Datasets without a query tree keep the existing non-query synchronization semantics."""
  svc = QueryContractColumnSyncService()
  td = type("_TD", (), {"query_root": None, "query_head": None})()

  assert svc._should_defer_empty_contract_sync(td, []) is False


def test_query_derived_creation_ordinal_starts_above_all_existing_columns():
  """New query-derived columns must not collide with existing query-derived ordinals."""
  svc = QueryContractColumnSyncService()
  columns = [
    type("_Col", (), {"ordinal_position": 1})(),
    type("_Col", (), {"ordinal_position": 7})(),
    type("_Col", (), {"ordinal_position": 20})(),
  ]

  assert svc._query_derived_creation_ordinal_base(columns) == 20


def test_query_derived_creation_ordinal_defaults_to_zero_without_columns():
  svc = QueryContractColumnSyncService()

  assert svc._query_derived_creation_ordinal_base([]) == 0


def test_query_sync_manages_only_query_derived_columns():
  """Original dataset columns must never become deletion candidates during query authoring."""
  svc = QueryContractColumnSyncService()
  direct = type("_Col", (), {"lineage_origin": "direct", "system_column_role": ""})()
  query = type("_Col", (), {"lineage_origin": "query_derived", "system_column_role": ""})()
  technical = type("_Col", (), {"lineage_origin": "query_derived", "system_column_role": "loaded_at"})()

  assert svc._managed_query_columns([direct, query, technical]) == [query]


def test_head_contract_retains_intermediate_query_outputs(monkeypatch):
  """A downstream head must not orphan outputs still defined by an active query node."""
  svc = QueryContractColumnSyncService()
  td = type("_TD", (), {"pk": 7})()

  window_column = type("_WinCol", (), {"output_name": "prior_product_id"})()

  class _WindowColumns:
    def filter(self, **kwargs):
      assert kwargs == {"active": True}
      return [window_column]

  window = type("_Window", (), {"columns": _WindowColumns()})()
  node = type("_Node", (), {"node_type": "window", "window": window})()

  class _Nodes:
    def filter(self, **kwargs):
      assert kwargs == {"target_dataset_id": 7, "active": True}
      return [node]

  import metadata.models as models
  monkeypatch.setattr(models.QueryNode, "objects", _Nodes())

  assert svc._query_defined_output_names(td) == {"prior_product_id"}


def test_join_aware_upstream_type_map_preserves_right_input_string_length():
  svc = QueryContractColumnSyncService()

  class _Col:
    def __init__(self, name, datatype, max_length=None):
      self.target_column_name = name
      self.datatype = datatype
      self.max_length = max_length
      self.decimal_precision = None
      self.decimal_scale = None
      self.ordinal_position = 1
      self.id = 1

  class _Manager:
    def __init__(self, items):
      self._items = list(items)

    def all(self):
      return self

    def filter(self, **kwargs):
      return self

    def order_by(self, *args):
      return self

    def __iter__(self):
      return iter(self._items)

  left_ds = type(
    "_DS",
    (),
    {"target_columns": _Manager([_Col("person_id", "INTEGER")])},
  )()
  right_ds = type(
    "_DS",
    (),
    {"target_columns": _Manager([_Col("person_type_code", "STRING", max_length=2)])},
  )()

  left_input = type(
    "_Input",
    (),
    {"id": 1, "upstream_target_dataset": left_ds, "source_dataset": None},
  )()
  right_input = type(
    "_Input",
    (),
    {"id": 2, "upstream_target_dataset": right_ds, "source_dataset": None},
  )()
  join = type(
    "_Join",
    (),
    {
      "id": 1,
      "join_order": 1,
      "left_input_id": 1,
      "right_input_id": 2,
      "left_input": left_input,
      "right_input": right_input,
    },
  )()
  td = type(
    "_TD",
    (),
    {
      "input_links": _Manager([left_input, right_input]),
      "joins": _Manager([join]),
    },
  )()

  inferred = svc._select_upstream_type_map(td)

  assert inferred["person_type_code"]["datatype"] == "STRING"
  assert inferred["person_type_code"]["max_length"] == 2


def test_join_aware_upstream_type_map_keeps_left_precedence_for_duplicate_names():
  svc = QueryContractColumnSyncService()

  class _Col:
    def __init__(self, name, datatype):
      self.target_column_name = name
      self.datatype = datatype
      self.max_length = None
      self.decimal_precision = None
      self.decimal_scale = None
      self.ordinal_position = 1
      self.id = 1

  class _Manager:
    def __init__(self, items):
      self._items = list(items)

    def all(self):
      return self

    def filter(self, **kwargs):
      return self

    def order_by(self, *args):
      return self

    def __iter__(self):
      return iter(self._items)

  left_ds = type("_DS", (), {"target_columns": _Manager([_Col("shared_code", "INTEGER")])})()
  right_ds = type("_DS", (), {"target_columns": _Manager([_Col("shared_code", "STRING")])})()
  left_input = type(
    "_Input",
    (),
    {"id": 10, "upstream_target_dataset": left_ds, "source_dataset": None},
  )()
  right_input = type(
    "_Input",
    (),
    {"id": 20, "upstream_target_dataset": right_ds, "source_dataset": None},
  )()
  join = type(
    "_Join",
    (),
    {
      "id": 1,
      "join_order": 1,
      "left_input_id": 10,
      "right_input_id": 20,
      "left_input": left_input,
      "right_input": right_input,
    },
  )()
  td = type(
    "_TD",
    (),
    {
      "input_links": _Manager([left_input, right_input]),
      "joins": _Manager([join]),
    },
  )()

  inferred = svc._select_upstream_type_map(td)

  assert inferred["shared_code"]["datatype"] == "INTEGER"


def test_group_key_type_flows_through_filter_from_right_join_input():
  """A FILTER between SELECT and AGGREGATE must preserve right-join type metadata."""
  svc = QueryContractColumnSyncService()

  class _Col:
    def __init__(self, name, datatype, max_length=None):
      self.target_column_name = name
      self.datatype = datatype
      self.max_length = max_length
      self.decimal_precision = None
      self.decimal_scale = None
      self.ordinal_position = 1
      self.id = 1
      self.lineage_origin = ""

  class _Manager:
    def __init__(self, items):
      self._items = list(items)

    def all(self):
      return self

    def filter(self, **kwargs):
      return self

    def order_by(self, *args):
      return self

    def __iter__(self):
      return iter(self._items)

  left_ds = type(
    "_DS",
    (),
    {"target_columns": _Manager([_Col("person_id", "INTEGER")])},
  )()
  right_ds = type(
    "_DS",
    (),
    {"target_columns": _Manager([_Col("person_type_code", "STRING", max_length=2)])},
  )()

  left_input = type(
    "_Input",
    (),
    {"id": 1, "upstream_target_dataset": left_ds, "source_dataset": None},
  )()
  right_input = type(
    "_Input",
    (),
    {"id": 2, "upstream_target_dataset": right_ds, "source_dataset": None},
  )()
  join = type(
    "_Join",
    (),
    {
      "id": 1,
      "join_order": 1,
      "left_input_id": 1,
      "right_input_id": 2,
      "left_input": left_input,
      "right_input": right_input,
    },
  )()
  select_td = type(
    "_TD",
    (),
    {
      "input_links": _Manager([left_input, right_input]),
      "joins": _Manager([join]),
      "target_columns": _Manager([]),
    },
  )()
  select_node = type(
    "_SelectNode",
    (),
    {"node_type": "select", "target_dataset": select_td},
  )()
  filter_details = type("_Filter", (), {"input_node": select_node})()
  filter_node = type(
    "_FilterNode",
    (),
    {"node_type": "filter", "filter": filter_details},
  )()

  inferred = svc._infer_types_from_node(filter_node)

  assert inferred["person_type_code"]["datatype"] == "STRING"
  assert inferred["person_type_code"]["max_length"] == 2
