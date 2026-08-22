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
  historize: bool = False
  is_hist: bool = False
  active: bool = True
  lineage_key: str | None = None
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


def test_build_load_graph_expands_history_companion_and_keeps_siblings_parallelizable(monkeypatch):
  """
  A historized rawcore base pulls its history companion into the execution scope.
  Ordinary downstream datasets and the history companion remain sibling tasks once
  the base dataset has completed.
  """
  rawcore = DummySchema("rawcore")
  bizcore = DummySchema("bizcore")

  base = DummyTD(rawcore, "rc_customer")
  hist = DummyTD(rawcore, "rc_customer_hist")
  downstream = DummyTD(bizcore, "bc_customer")

  deps = {
    base: set(),
    hist: {base},
    downstream: {base},
  }

  monkeypatch.setattr(
    lg,
    "resolve_execution_upstream_datasets",
    lambda td: deps.get(td, set()),
  )
  monkeypatch.setattr(
    lg,
    "resolve_hist_companion_for_base",
    lambda td: hist if td == base else None,
  )

  graph = lg.build_load_graph(downstream)

  assert set(graph) == {base, hist, downstream}
  assert graph[base] == set()
  assert graph[hist] == {base}
  assert graph[downstream] == {base}

  levels = lg.topological_levels(graph)
  assert levels[0] == [base]
  assert set(levels[1]) == {hist, downstream}


def test_resolve_execution_dependencies_adds_history_base_reason(monkeypatch):
  rawcore = DummySchema("rawcore")
  base = DummyTD(rawcore, "rc_customer", id=1)
  hist = DummyTD(rawcore, "rc_customer_hist", id=2, is_hist=True)

  monkeypatch.setattr(
    lg,
    "resolve_hist_base_for_companion",
    lambda _td: base,
  )

  deps = lg.resolve_execution_dependencies(hist)

  assert deps == (
    lg.ExecutionDependency(
      upstream=base,
      reason=lg.EXECUTION_DEPENDENCY_HIST_BASE_READY,
    ),
  )


def test_resolve_execution_order_all_propagates_history_contract_errors(monkeypatch):
  rawcore = DummySchema("rawcore")
  base = DummyTD(rawcore, "rc_customer")

  def fail(_root):
    raise lg.ExecutionGraphError("missing history companion")

  monkeypatch.setattr(lg, "build_load_graph", fail)

  with pytest.raises(lg.ExecutionGraphError, match="missing history companion"):
    lg.resolve_execution_order_all([base])


@pytest.mark.django_db
def test_resolve_execution_order_includes_generated_history_companion():
  from metadata.models import TargetDataset, TargetSchema

  rawcore, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={"schema_name": "rawcore"},
  )
  base = TargetDataset.objects.create(
    target_schema=rawcore,
    target_dataset_name="rc_customer_scope_test",
    incremental_strategy="full",
    historize=True,
    lineage_key="generated:rawcore:customer-scope-test",
  )
  hist = TargetDataset.objects.create(
    target_schema=rawcore,
    target_dataset_name="rc_customer_scope_test_hist",
    incremental_strategy="historize",
    historize=False,
    is_system_managed=True,
    lineage_key=base.lineage_key,
  )

  order = lg.resolve_execution_order(base)

  assert order == [base, hist]


@pytest.mark.django_db
def test_downstream_scope_includes_history_companion_as_parallel_sibling():
  from metadata.models import TargetDataset, TargetDatasetInput, TargetSchema

  rawcore, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={"schema_name": "rawcore"},
  )
  bizcore, _ = TargetSchema.objects.get_or_create(
    short_name="bizcore",
    defaults={"schema_name": "bizcore"},
  )
  base = TargetDataset.objects.create(
    target_schema=rawcore,
    target_dataset_name="rc_customer_parallel_test",
    incremental_strategy="full",
    historize=True,
    lineage_key="generated:rawcore:customer-parallel-test",
  )
  hist = TargetDataset.objects.create(
    target_schema=rawcore,
    target_dataset_name="rc_customer_parallel_test_hist",
    incremental_strategy="historize",
    historize=False,
    is_system_managed=True,
    lineage_key=base.lineage_key,
  )
  downstream = TargetDataset.objects.create(
    target_schema=bizcore,
    target_dataset_name="bc_customer_parallel_test",
    incremental_strategy="full",
    historize=False,
  )
  TargetDatasetInput.objects.create(
    target_dataset=downstream,
    upstream_target_dataset=base,
    source_dataset=None,
    role="primary",
    active=True,
  )

  graph = lg.build_load_graph(downstream)
  levels = lg.topological_levels(graph)

  assert set(graph) == {base, hist, downstream}
  assert graph[hist] == {base}
  assert graph[downstream] == {base}
  assert levels[0] == [base]
  assert set(levels[1]) == {hist, downstream}


@pytest.mark.django_db
def test_historized_base_without_history_companion_fails_closed():
  from metadata.models import TargetDataset, TargetSchema

  rawcore, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={"schema_name": "rawcore"},
  )
  base = TargetDataset.objects.create(
    target_schema=rawcore,
    target_dataset_name="rc_customer_missing_hist_test",
    incremental_strategy="full",
    historize=True,
    lineage_key="generated:rawcore:customer-missing-hist-test",
  )

  with pytest.raises(
    lg.ExecutionGraphError,
    match="requires a history companion",
  ):
    lg.resolve_execution_order(base)


@pytest.mark.django_db
def test_non_historized_rawcore_does_not_require_history_companion():
  from metadata.models import TargetDataset, TargetSchema

  rawcore, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={"schema_name": "rawcore"},
  )
  if rawcore.default_historize:
    rawcore.default_historize = False
    rawcore.save(update_fields=["default_historize"])

  base = TargetDataset.objects.create(
    target_schema=rawcore,
    target_dataset_name="rc_customer_no_hist_scope_test",
    incremental_strategy="full",
    historize=False,
  )

  assert lg.resolve_execution_order(base) == [base]


def test_execution_plan_propagates_history_contract_errors(monkeypatch):
  from metadata.execution.executor import build_execution_plan

  rawcore = DummySchema("rawcore")
  base = DummyTD(rawcore, "rc_customer", id=1, historize=True)

  def fail(_td):
    raise lg.ExecutionGraphError("history contract invalid")

  monkeypatch.setattr(lg, "resolve_execution_upstream_datasets", fail)

  with pytest.raises(lg.ExecutionGraphError, match="history contract invalid"):
    build_execution_plan(
      batch_run_id="batch",
      execution_order=[base],
    )


def test_execution_plan_keeps_history_base_upstream_key(monkeypatch):
  from metadata.execution.executor import build_execution_plan

  rawcore = DummySchema("rawcore")
  base = DummyTD(rawcore, "rc_customer", id=1, historize=True)
  hist = DummyTD(rawcore, "rc_customer_hist", id=2, is_hist=True)

  monkeypatch.setattr(
    lg,
    "resolve_execution_upstream_datasets",
    lambda td: {base} if td == hist else set(),
  )

  plan = build_execution_plan(
    batch_run_id="batch",
    execution_order=[base, hist],
  )

  assert plan.steps[0].dataset_key == "rawcore.rc_customer"
  assert plan.steps[0].upstream_keys == ()
  assert plan.steps[1].dataset_key == "rawcore.rc_customer_hist"
  assert plan.steps[1].upstream_keys == ("rawcore.rc_customer",)
