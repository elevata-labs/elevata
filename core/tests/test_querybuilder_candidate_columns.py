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
