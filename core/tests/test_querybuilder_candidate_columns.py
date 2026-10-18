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


def test_candidate_column_names_prefers_contract(monkeypatch):
  # Import the helper from where it currently lives
  from metadata.views_scoped import _candidate_column_names_for_targetdataset

  class FakeContract:
    output_columns = ["a", "b", "c"]

  class FakeTD:
    query_root = object()

  # Force the contract inference path
  def fake_infer_query_node_contract(root):
    assert root is FakeTD.query_root
    return FakeContract()

  monkeypatch.setattr(
    "metadata.generation.query_contract.infer_query_node_contract",
    fake_infer_query_node_contract,
    raising=False,
  )

  cols = _candidate_column_names_for_targetdataset(FakeTD())
  assert cols == ["a", "b", "c"]


def test_candidate_column_names_falls_back_to_dataset_columns(monkeypatch):
  from metadata.views_scoped import _candidate_column_names_for_targetdataset

  class FakeCol:
    def __init__(self, name):
      self.target_column_name = name

  class FakeColsManager:
    def all(self):
      return [FakeCol("x"), FakeCol("y")]

  class FakeTD:
    query_root = None
    columns = FakeColsManager()

  # Make contract inference fail
  monkeypatch.setattr(
    "metadata.generation.query_contract.infer_query_node_contract",
    lambda _: (_ for _ in ()).throw(Exception("nope")),
    raising=False,
  )

  cols = _candidate_column_names_for_targetdataset(FakeTD())
  assert cols == ["x", "y"]


def test_candidate_column_names_prefers_available_input_columns(monkeypatch):
  """Aggregate authoring may use selectable inputs beyond the visible node output."""
  from metadata.views_scoped import _candidate_column_names_for_targetdataset

  input_node = object()

  class FakeContract:
    columns = ["bc_dim_product_key"]
    available_input_columns = ["bc_dim_product_key", "product_name"]

  class FakeTD:
    query_root = object()

  def fake_infer_query_node_contract(node):
    assert node is input_node
    return FakeContract()

  monkeypatch.setattr(
    "metadata.generation.query_contract.infer_query_node_contract",
    fake_infer_query_node_contract,
    raising=False,
  )

  cols = _candidate_column_names_for_targetdataset(FakeTD(), query_node=input_node)
  assert cols == ["bc_dim_product_key", "product_name"]


def test_select_contract_exposes_projected_and_upstream_columns(monkeypatch):
  """SELECT authoring keeps projected expressions and non-projected upstream inputs selectable."""
  from metadata.generation import query_contract

  class FakeTargetColumn:
    def __init__(self, name, lineage_origin="direct"):
      self.target_column_name = name
      self.lineage_origin = lineage_origin

  class FakeTargetColumns:
    def all(self):
      return self

    def filter(self, **_kwargs):
      return self

    def exclude(self, **_kwargs):
      return self

    def order_by(self, *_fields):
      return [FakeTargetColumn("bc_dim_product_key"), FakeTargetColumn("display_name")]

  td = type("FakeTD", (), {"target_columns": FakeTargetColumns()})()
  node = type(
    "FakeNode",
    (),
    {"id": 1, "node_type": "select", "target_dataset": td},
  )()

  monkeypatch.setattr(
    query_contract,
    "_select_upstream_column_names",
    lambda _td: ["product_id", "product_name"],
  )

  contract = query_contract.infer_query_node_contract(node)

  assert contract.columns == ["bc_dim_product_key", "display_name"]
  assert contract.available_input_columns == [
    "bc_dim_product_key",
    "display_name",
    "product_id",
    "product_name",
  ]


def test_select_upstream_columns_order_left_then_right_and_left_wins_duplicates():
  """Join authoring follows left-to-right order and duplicate names resolve left."""
  from metadata.generation.query_contract import _select_upstream_column_names

  class FakeColumn:
    def __init__(self, name, ordinal):
      self.target_column_name = name
      self.ordinal_position = ordinal
      self.id = ordinal

  class FakeColumns:
    def __init__(self, names):
      self.items = [FakeColumn(name, idx) for idx, name in enumerate(names, start=1)]

    def filter(self, **_kwargs):
      return self

    def order_by(self, *_fields):
      return self.items

  class FakeUpstream:
    def __init__(self, names):
      self.target_columns = FakeColumns(names)

  class FakeInput:
    def __init__(self, upstream, pk):
      self.upstream_target_dataset = upstream
      self.source_dataset = None
      self.id = pk

  class FakeInputs:
    def __init__(self, items):
      self.items = items

    def all(self):
      return self.items

  class FakeJoin:
    def __init__(self, left_input, right_input):
      self.id = 1
      self.join_order = 1
      self.left_input = left_input
      self.left_input_id = left_input.id
      self.right_input = right_input
      self.right_input_id = right_input.id

  class FakeJoins:
    def __init__(self, items):
      self.items = items

    def all(self):
      return self.items

  customer = FakeUpstream(["account_no", "person_id", "customer_id"])
  person = FakeUpstream(["person_id", "first_name", "last_name"])
  left = FakeInput(customer, 10)
  right = FakeInput(person, 20)
  td = type(
    "FakeTD",
    (),
    {"input_links": FakeInputs([left, right]), "joins": FakeJoins([FakeJoin(left, right)])},
  )()

  assert _select_upstream_column_names(td) == [
    "account_no",
    "person_id",
    "customer_id",
    "first_name",
    "last_name",
  ]


def test_select_upstream_columns_do_not_require_input_ordinal_position():
  """Real TargetDatasetInput rows expose ids, not an ordinal_position field."""
  from metadata.generation.query_contract import _select_upstream_column_names

  class FakeColumn:
    def __init__(self, name):
      self.target_column_name = name

  class FakeColumns:
    def filter(self, **_kwargs):
      return self

    def order_by(self, *_fields):
      return [FakeColumn("product_id"), FakeColumn("product_name")]

  class FakeUpstream:
    target_columns = FakeColumns()

  class FakeInput:
    def __init__(self, pk):
      self.id = pk
      self.upstream_target_dataset = FakeUpstream()
      self.source_dataset = None

  class FakeInputs:
    def all(self):
      return [FakeInput(20), FakeInput(10)]

  td = type("FakeTD", (), {"input_links": FakeInputs()})()

  assert _select_upstream_column_names(td) == ["product_id", "product_name"]


def test_select_contract_keeps_inactive_authored_columns_as_node_output(monkeypatch):
  """A later aggregate may retire physical columns without erasing SELECT output."""
  from metadata.generation import query_contract

  class FakeTargetColumn:
    def __init__(self, name, *, active, lineage_origin="direct"):
      self.target_column_name = name
      self.active = active
      self.lineage_origin = lineage_origin
      self.ordinal_position = 1
      self.id = 1

  class FakeTargetColumns:
    def all(self):
      return self

    def order_by(self, *_fields):
      return [
        FakeTargetColumn("customer", active=False),
        FakeTargetColumn("group_count", active=True, lineage_origin="query_derived"),
      ]

  td = type("FakeTD", (), {"target_columns": FakeTargetColumns()})()
  node = type("FakeNode", (), {"id": 44, "node_type": "select", "target_dataset": td})()

  monkeypatch.setattr(query_contract, "_select_upstream_column_names", lambda _td: ["last_name"])

  contract = query_contract.infer_query_node_contract(node)

  assert contract.columns == ["customer"]
  assert contract.available_input_columns == ["customer", "last_name"]
