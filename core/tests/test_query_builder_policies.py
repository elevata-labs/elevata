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

import types

import pytest


class DummySchema:
  def __init__(self, short_name: str):
    self.short_name = short_name


class DummyTD:
  def __init__(self, schema_short: str):
    self.target_schema = DummySchema(schema_short)


def test_schema_short_for_dataset_reads_target_schema_short_name():
  from metadata.generation.policies import schema_short_for_dataset
  td = DummyTD("bizcore")
  assert schema_short_for_dataset(td) == "bizcore"


def test_query_tree_allowed_for_dataset_true_for_bizcore_and_serving():
  from metadata.generation.policies import query_tree_allowed_for_dataset
  assert query_tree_allowed_for_dataset(DummyTD("bizcore")) is True
  assert query_tree_allowed_for_dataset(DummyTD("serving")) is True


def test_query_tree_allowed_for_dataset_false_for_other_layers():
  from metadata.generation.policies import query_tree_allowed_for_dataset
  for schema in ["raw", "rawcore", "stage", "other", ""]:
    assert query_tree_allowed_for_dataset(DummyTD(schema)) is False


def test_allowed_query_node_types_for_dataset():
  from metadata.generation.policies import allowed_query_node_types_for_dataset
  assert allowed_query_node_types_for_dataset(DummyTD("bizcore")) == {"select", "aggregate", "union", "window"}
  assert allowed_query_node_types_for_dataset(DummyTD("serving")) == {"select", "aggregate", "union", "window"}
  assert allowed_query_node_types_for_dataset(DummyTD("raw")) == set()


def test_allowed_function_kinds_for_dataset():
  from metadata.generation.policies import allowed_function_kinds_for_dataset
  assert allowed_function_kinds_for_dataset(DummyTD("bizcore")) == {"scalar", "aggregate", "window"}
  assert allowed_function_kinds_for_dataset(DummyTD("serving")) == {"scalar", "aggregate", "window"}
  assert allowed_function_kinds_for_dataset(DummyTD("raw")) == set()


def test_downstream_dependents_exist_for_dataset_uses_qs_exists(monkeypatch):
  from metadata.generation import policies

  td = DummyTD("bizcore")

  class DummyQS:
    def exists(self):
      return True

  monkeypatch.setattr(policies, "downstream_dependents_qs_for_dataset", lambda _td: DummyQS())
  assert policies.downstream_dependents_exist_for_dataset(td) is True


def test_downstream_dependents_exist_for_dataset_falls_back_to_bool(monkeypatch):
  from metadata.generation import policies

  td = DummyTD("bizcore")

  class DummyQS:
    def exists(self):
      raise RuntimeError("no exists")

    def __bool__(self):
      return True

  monkeypatch.setattr(policies, "downstream_dependents_qs_for_dataset", lambda _td: DummyQS())
  assert policies.downstream_dependents_exist_for_dataset(td) is True


def test_downstream_dependents_count_for_dataset_uses_qs_count(monkeypatch):
  from metadata.generation import policies

  td = DummyTD("bizcore")

  class DummyQS:
    def count(self):
      return 7

  monkeypatch.setattr(policies, "downstream_dependents_qs_for_dataset", lambda _td: DummyQS())
  assert policies.downstream_dependents_count_for_dataset(td) == 7


def test_downstream_dependents_count_for_dataset_falls_back_to_len(monkeypatch):
  from metadata.generation import policies

  td = DummyTD("bizcore")

  class DummyQS(list):
    def count(self):
      raise RuntimeError("no count")

  monkeypatch.setattr(policies, "downstream_dependents_qs_for_dataset", lambda _td: DummyQS([1, 2, 3]))
  assert policies.downstream_dependents_count_for_dataset(td) == 3


def test_query_tree_mutations_allowed_for_dataset_false_when_schema_disallowed(monkeypatch):
  from metadata.generation import policies
  td = DummyTD("raw")
  # downstream shouldn't matter if schema disallowed
  monkeypatch.setattr(policies, "downstream_dependents_exist_for_dataset", lambda _td: False)
  assert policies.query_tree_mutations_allowed_for_dataset(td) is False


def test_query_tree_mutations_allowed_for_dataset_false_when_downstream_exists(monkeypatch):
  from metadata.generation import policies
  td = DummyTD("bizcore")
  monkeypatch.setattr(policies, "downstream_dependents_exist_for_dataset", lambda _td: True)
  assert policies.query_tree_mutations_allowed_for_dataset(td) is False


def test_query_tree_mutations_allowed_for_dataset_true_when_no_downstream(monkeypatch):
  from metadata.generation import policies
  td = DummyTD("serving")
  monkeypatch.setattr(policies, "downstream_dependents_exist_for_dataset", lambda _td: False)
  assert policies.query_tree_mutations_allowed_for_dataset(td) is True


def test_query_tree_mutation_block_reason_schema(monkeypatch):
  from metadata.generation import policies
  td = DummyTD("raw")
  monkeypatch.setattr(policies, "downstream_dependents_exist_for_dataset", lambda _td: False)
  assert "bizcore/serving" in policies.query_tree_mutation_block_reason(td)


def test_query_tree_mutation_block_reason_downstream(monkeypatch):
  from metadata.generation import policies
  td = DummyTD("bizcore")
  monkeypatch.setattr(policies, "downstream_dependents_exist_for_dataset", lambda _td: True)
  msg = policies.query_tree_mutation_block_reason(td)
  assert "downstream" in msg.lower()
  assert "lineage" in msg.lower()
