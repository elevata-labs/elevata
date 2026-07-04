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
from dataclasses import dataclass, field
from typing import Any

import metadata.execution.load_graph as lg


class _Manager(list):
  """Tiny Django-ish queryset stand-in for load graph tests."""

  def select_related(self, *args, **kwargs):
    return self


@dataclass(frozen=True)
class DummySchema:
  short_name: str


@dataclass(frozen=True)
class DummyTD:
  target_schema: DummySchema
  target_dataset_name: str
  id: int | None = None
  input_links: Any = field(default_factory=_Manager, compare=False, hash=False)


@dataclass(frozen=True)
class DummyLink:
  upstream_target_dataset: DummyTD | None = None
  source_dataset: Any = None


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

  def fake_resolve_execution_upstream_datasets(td):
    return deps.get(td, set())

  monkeypatch.setattr(
    lg,
    "resolve_execution_upstream_datasets",
    fake_resolve_execution_upstream_datasets,
  )

  order = lg.resolve_execution_order_all([c1])
  assert order == [r1, s1, c1]

  # Multiple roots: ensure deterministic order and no duplicates
  order2 = lg.resolve_execution_order_all([c1, s1])
  assert order2 == [r1, s1, c1]


def test_resolve_execution_order_all_empty_roots_returns_empty():
  assert lg.resolve_execution_order_all([]) == []


def test_resolve_execution_dependencies_returns_structured_reasons():
  """
  Verify TargetDataset input dependencies keep explicit execution reasons.
  """
  raw = DummySchema("raw")
  stage = DummySchema("stage")

  upstream = DummyTD(raw, "raw_customer", id=1)
  child = DummyTD(
    stage,
    "stg_customer",
    id=2,
    input_links=_Manager([
      DummyLink(upstream_target_dataset=upstream),
    ]),
  )

  deps = lg.resolve_execution_dependencies(child)

  assert deps == (
    lg.ExecutionDependency(
      upstream=upstream,
      reason=lg.EXECUTION_DEPENDENCY_LINEAGE_INPUT,
    ),
  )
  assert lg.resolve_execution_upstream_datasets(child) == {upstream}


def test_resolve_execution_dependencies_skips_self_raw_source_dependency(monkeypatch):
  """
  Verify SourceDataset -> raw readiness never creates a self-dependency.
  """
  raw = DummySchema("raw")
  source = object()
  raw_target = DummyTD(
    raw,
    "raw_customer",
    id=1,
    input_links=_Manager([
      DummyLink(source_dataset=source),
    ]),
  )

  monkeypatch.setattr(
    lg,
    "resolve_raw_dataset_for_source",
    lambda _source: raw_target,
  )

  assert lg.resolve_execution_dependencies(raw_target) == ()
  assert lg.resolve_execution_upstream_datasets(raw_target) == set()
