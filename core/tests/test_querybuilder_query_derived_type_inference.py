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


def _make_td_with_aggregate(measures):
  class _Agg:
    def __init__(self, measures):
      self.measures = _QS(measures)
      self.input_node = type("_In", (), {"target_dataset": object()})()

  class _Head:
    node_type = "aggregate"
    def __init__(self, measures):
      self.aggregate = _Agg(measures)

  class _TD:
    def __init__(self, measures):
      self.query_head = _Head(measures)
      self.query_root = None

  return _TD(measures)


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


def test_query_derived_sum_uses_input_column_type(monkeypatch):
  svc = QueryContractColumnSyncService()

  # Patch the resolver that looks up input column types.
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
  assert out["turnover_amt"]["decimal_precision"] == 12
  assert out["turnover_amt"]["decimal_scale"] == 2
