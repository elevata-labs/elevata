"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2026 Ilona Tag

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
from dataclasses import dataclass

import metadata.execution.load_graph as lg


@dataclass(frozen=True)
class DummySchema:
  short_name: str


@dataclass(frozen=True)
class DummyTD:
  target_schema: DummySchema
  target_dataset_name: str


def test_resolve_execution_order_all_includes_upstreams_and_is_deterministic(monkeypatch):
  # Graph:
  #   stage.s1 depends on raw.r1
  #   rawcore.c1 depends on stage.s1
  raw = DummySchema("raw")
  stage = DummySchema("stage")
  rawcore = DummySchema("rawcore")

  r1 = DummyTD(raw, "r1")
  s1 = DummyTD(stage, "s1")
  c1 = DummyTD(rawcore, "c1")

  deps = {
    s1: {r1},
    c1: {s1},
    r1: set(),
  }

  def fake_resolve_upstream_datasets(td):
    return deps.get(td, set())

  monkeypatch.setattr(lg, "resolve_upstream_datasets", fake_resolve_upstream_datasets)

  order = lg.resolve_execution_order_all([c1])
  assert order == [r1, s1, c1]

  # Multiple roots: ensure deterministic order and no duplicates
  order2 = lg.resolve_execution_order_all([c1, s1])
  assert order2 == [r1, s1, c1]


def test_resolve_execution_order_all_empty_roots_returns_empty():
  assert lg.resolve_execution_order_all([]) == []
