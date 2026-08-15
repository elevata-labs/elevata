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

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from metadata.architecture.execution_impact import ExecutionImpactPlan
from metadata.architecture.execution_impact_service import (
  ExecutionImpactPlanError,
  build_execution_impact_plan,
)
from metadata.architecture.state import ArchitectureState, ColumnState, DatasetState


@dataclass(frozen=True)
class DummySchema:
  short_name: str
  default_materialization_type: str = "table"


@dataclass(frozen=True)
class DummyTargetDataset:
  target_schema: DummySchema
  target_dataset_name: str
  incremental_strategy: str = "merge"
  materialization_type: str | None = None

  @property
  def effective_materialization_type(self) -> str:
    return (
      self.materialization_type
      or self.target_schema.default_materialization_type
    )


@dataclass(frozen=True)
class DummyDependency:
  upstream: DummyTargetDataset
  reason: str
  reference_id: int | None = None


class FakeReport:
  def __init__(
    self,
    *,
    current_state: ArchitectureState,
    previous_state: ArchitectureState | None,
    dataset_keys: tuple[str, ...],
    dataset_changes: tuple[dict[str, Any], ...] = (),
    migration_actions: tuple[dict[str, Any], ...] = (),
    policy_decisions: tuple[dict[str, Any], ...] = (),
    report_fingerprint: str = "report-fingerprint",
    is_blocked: bool = False,
  ):
    self.report_fingerprint = report_fingerprint
    self.has_changes = bool(dataset_changes or migration_actions)
    self.is_blocked = is_blocked
    self._payload = {
      "report_fingerprint": report_fingerprint,
      "scope": {
        "mode": "scoped",
        "dataset_keys": list(dataset_keys),
      },
      "state": {
        "previous_fingerprint": (
          previous_state.fingerprint
          if previous_state is not None
          else None
        ),
        "current_fingerprint": current_state.fingerprint,
        "has_changes": self.has_changes,
      },
      "summary": {
        "dataset_change_count": len(dataset_changes),
        "column_change_count": 0,
        "migration_action_count": len(migration_actions),
        "policy_decision_count": len(policy_decisions),
      },
      "dataset_changes": list(dataset_changes),
      "column_changes": [],
      "migration_actions": list(migration_actions),
      "policy_decisions": list(policy_decisions),
      "is_blocked": is_blocked,
    }

  def to_dict(self) -> dict[str, Any]:
    return self._payload


def _column(
  name: str,
  *,
  lineage_key: str | None = None,
) -> ColumnState:
  return ColumnState(
    column_name=name,
    datatype="string",
    nullable=True,
    active=True,
    lineage_key=lineage_key or f"lk_{name}",
  )


def _dataset(
  dataset_key: str,
  *,
  incremental_strategy: str | None = "merge",
  materialization_type: str | None = "table",
  columns: tuple[ColumnState, ...] | None = None,
) -> DatasetState:
  schema_short, dataset_name = dataset_key.split(".", 1)
  return DatasetState(
    dataset_key=dataset_key,
    schema_short_name=schema_short,
    dataset_name=dataset_name,
    materialization_type=materialization_type,
    incremental_strategy=incremental_strategy,
    historize=False,
    is_hist=False,
    active=True,
    column_states=columns or (_column("id"),),
  )


def _state(*datasets: DatasetState) -> ArchitectureState:
  return ArchitectureState(datasets=tuple(datasets))


def _target(
  dataset_key: str,
  *,
  incremental_strategy: str = "merge",
  materialization_type: str | None = None,
  schema_materialization_type: str = "table",
) -> DummyTargetDataset:
  schema_short, target_name = dataset_key.split(".", 1)
  return DummyTargetDataset(
    target_schema=DummySchema(
      schema_short,
      default_materialization_type=schema_materialization_type,
    ),
    target_dataset_name=target_name,
    incremental_strategy=incremental_strategy,
    materialization_type=materialization_type,
  )


def _baseline(
  previous_state: ArchitectureState | None,
  *,
  source: str | None = None,
  can_execute: bool | None = None,
) -> SimpleNamespace:
  resolved_can_execute = (
    previous_state is not None
    if can_execute is None
    else can_execute
  )
  return SimpleNamespace(
    previous_state=previous_state,
    source=(
      source
      or (
        "recorded_state"
        if previous_state is not None
        else "missing_or_unsupported"
      )
    ),
    can_execute=resolved_can_execute,
    message="baseline resolution message",
    state_file=Path(".elevata/state/dev/dwh/architecture_state.json"),
  )


def _review_status(
  *,
  scope_key: str = "all",
  status: str = "no_changes",
  report_fingerprint: str = "report-fingerprint",
  approval_id: str | None = None,
  artifact_fingerprint: str | None = None,
) -> SimpleNamespace:
  return SimpleNamespace(
    status=status,
    message=f"review status: {status}",
    dataset_key=scope_key,
    report_fingerprint=report_fingerprint,
    approval_id=approval_id,
    artifact_fingerprint=artifact_fingerprint,
    review_decision="approved" if status == "approved" else None,
    has_changes=status != "no_changes",
    is_blocked=status == "blocked",
  )


def _resolver(dependencies: dict[str, tuple[DummyDependency, ...]]):
  def resolve(target_dataset: DummyTargetDataset):
    key = (
      f"{target_dataset.target_schema.short_name}."
      f"{target_dataset.target_dataset_name}"
    )
    return dependencies.get(key, ())

  return resolve


def _items_by_key(plan: ExecutionImpactPlan):
  return {
    item.dataset_key: item
    for item in plan.items
  }


def test_service_assembles_evidence_decisions_and_transitive_propagation() -> None:
  previous = _state(
    _dataset("raw.customer"),
    _dataset("stage.customer"),
    _dataset("rawcore.customer"),
  )
  current = _state(
    _dataset("raw.customer", columns=(_column("id"), _column("name"))),
    _dataset("stage.customer"),
    _dataset("rawcore.customer"),
  )
  report = FakeReport(
    current_state=current,
    previous_state=previous,
    dataset_keys=(
      "raw.customer",
      "stage.customer",
      "rawcore.customer",
    ),
    dataset_changes=(
      {
        "dataset_key": "raw.customer",
        "change_type": "DATASET_CHANGED",
        "details": {"changed_fields": ["dataset_name"]},
      },
    ),
    migration_actions=(
      {
        "dataset_key": "raw.customer",
        "action_type": "REBUILD_DATASET",
      },
    ),
    policy_decisions=(
      {
        "dataset_key": "raw.customer",
        "status": "ALLOW",
        "code": "REBUILD_ALLOWED",
        "action_type": "REBUILD_DATASET",
      },
    ),
  )
  raw = _target("raw.customer")
  stage = _target("stage.customer", incremental_strategy="full")
  rawcore = _target("rawcore.customer", incremental_strategy="full")

  plan = build_execution_impact_plan(
    scope_key="all",
    current_state=current,
    baseline_resolution=_baseline(previous),
    report=report,
    review_status=_review_status(
      status="approved",
      approval_id="apr_123",
      artifact_fingerprint="approval-fingerprint",
    ),
    target_datasets=(rawcore, raw, stage),
    dependency_resolver=_resolver({
      "stage.customer": (DummyDependency(raw, "lineage_input"),),
      "rawcore.customer": (DummyDependency(stage, "lineage_input"),),
    }),
  )
  items = _items_by_key(plan)

  assert plan.assessed_count == 3
  assert plan.decision_counts == {
    "REUSE": 0,
    "REVALIDATE": 0,
    "INCREMENTAL_EXECUTE": 0,
    "FULL_REBUILD": 3,
    "BLOCKED": 0,
  }
  assert items["raw.customer"].reason_codes == (
    "DATASET_DEFINITION_CHANGED",
  )
  assert items["stage.customer"].reason_codes == (
    "FULL_REFRESH_STRATEGY",
    "UPSTREAM_REBUILD_REQUIRED",
  )
  assert items["rawcore.customer"].reason_codes == (
    "FULL_REFRESH_STRATEGY",
    "UPSTREAM_REBUILD_REQUIRED",
  )
  assert plan.evidence[-1].evidence_type == "execution_dependency"
  assert plan.evidence[-1].evidence_key == "scope:all"


def test_service_allows_verified_initial_deployment_without_approval() -> None:
  """
  Verify an empty-target bootstrap produces executable full rebuild decisions.
  """
  previous = _state()
  current = _state(
    _dataset(
      "raw.customer",
      incremental_strategy="full",
    )
  )
  report = FakeReport(
    current_state=current,
    previous_state=previous,
    dataset_keys=("raw.customer",),
    dataset_changes=(
      {
        "dataset_key": "raw.customer",
        "change_type": "DATASET_ADDED",
        "details": {},
      },
    ),
    migration_actions=(
      {
        "dataset_key": "raw.customer",
        "action_type": "CREATE_DATASET",
      },
    ),
    policy_decisions=(
      {
        "dataset_key": "raw.customer",
        "status": "ALLOW",
        "code": "CREATE_DATASET_ALLOWED",
        "action_type": "CREATE_DATASET",
      },
    ),
  )

  plan = build_execution_impact_plan(
    scope_key="all",
    current_state=current,
    baseline_resolution=_baseline(
      previous,
      source="verified_empty_target",
      can_execute=True,
    ),
    report=report,
    review_status=_review_status(
      status="initial_deployment",
    ),
    target_datasets=(
      _target(
        "raw.customer",
        incremental_strategy="full",
      ),
    ),
    dependency_resolver=_resolver({}),
  )

  item = plan.items[0]
  approval_evidence = next(
    evidence
    for evidence in item.evidence
    if evidence.evidence_type == "architecture_approval"
  )

  assert item.decision == "FULL_REBUILD"
  assert item.reason_codes == ("DATASET_ADDED",)
  assert plan.decision_counts["BLOCKED"] == 0
  assert approval_evidence.status == "not_applicable"
  assert approval_evidence.required is False


def test_service_keeps_historized_target_incremental_after_upstream_full_rebuild() -> None:
  previous = _state(
    _dataset("rawcore.customer", incremental_strategy="full"),
    _dataset("rawcore.customer_hist", incremental_strategy="historize"),
  )
  current = _state(
    _dataset(
      "rawcore.customer",
      incremental_strategy="full",
      columns=(_column("id"), _column("name")),
    ),
    _dataset("rawcore.customer_hist", incremental_strategy="historize"),
  )
  report = FakeReport(
    current_state=current,
    previous_state=previous,
    dataset_keys=(
      "rawcore.customer",
      "rawcore.customer_hist",
    ),
    dataset_changes=(
      {
        "dataset_key": "rawcore.customer",
        "change_type": "DATASET_CHANGED",
        "details": {"changed_fields": ["dataset_name"]},
      },
    ),
    migration_actions=(
      {
        "dataset_key": "rawcore.customer",
        "action_type": "REBUILD_DATASET",
      },
    ),
    policy_decisions=(
      {
        "dataset_key": "rawcore.customer",
        "status": "ALLOW",
        "code": "REBUILD_ALLOWED",
        "action_type": "REBUILD_DATASET",
      },
    ),
  )
  upstream = _target(
    "rawcore.customer",
    incremental_strategy="full",
  )
  hist = _target(
    "rawcore.customer_hist",
    incremental_strategy="historize",
  )

  plan = build_execution_impact_plan(
    scope_key="all",
    current_state=current,
    baseline_resolution=_baseline(previous),
    report=report,
    review_status=_review_status(
      status="approved",
      approval_id="apr_hist",
      artifact_fingerprint="approval-hist",
    ),
    target_datasets=(hist, upstream),
    dependency_resolver=_resolver({
      "rawcore.customer_hist": (
        DummyDependency(upstream, "lineage_input"),
      ),
    }),
  )
  item = _items_by_key(plan)["rawcore.customer_hist"]

  assert item.decision == "INCREMENTAL_EXECUTE"
  assert item.reason_codes == (
    "INCREMENTAL_LOAD_STRATEGY",
    "UPSTREAM_EXECUTION_REQUIRED",
  )


def test_service_consumes_target_dataset_iterable_once() -> None:
  dataset = _dataset("raw.customer")
  state = _state(dataset)
  target = _target("raw.customer")
  iterations = 0

  def targets():
    nonlocal iterations
    iterations += 1
    yield target

  plan = build_execution_impact_plan(
    scope_key="all",
    current_state=state,
    baseline_resolution=_baseline(state),
    report=FakeReport(
      current_state=state,
      previous_state=state,
      dataset_keys=("raw.customer",),
    ),
    review_status=_review_status(),
    target_datasets=targets(),
    dependency_resolver=_resolver({}),
  )

  assert iterations == 1
  assert plan.items[0].decision == "INCREMENTAL_EXECUTE"
  assert plan.items[0].reason_codes == ("INCREMENTAL_LOAD_STRATEGY",)


def test_service_resolves_inherited_table_materialization_from_runtime_target() -> None:
  dataset = _dataset(
    "raw.customer",
    materialization_type=None,
    incremental_strategy="full",
  )
  state = _state(dataset)

  plan = build_execution_impact_plan(
    scope_key="all",
    current_state=state,
    baseline_resolution=_baseline(state),
    report=FakeReport(
      current_state=state,
      previous_state=state,
      dataset_keys=("raw.customer",),
    ),
    review_status=_review_status(),
    target_datasets=(
      _target(
        "raw.customer",
        incremental_strategy="full",
        materialization_type=None,
        schema_materialization_type="table",
      ),
    ),
    dependency_resolver=_resolver({}),
  )

  assert plan.items[0].decision == "FULL_REBUILD"
  assert plan.items[0].reason_codes == ("FULL_REFRESH_STRATEGY",)


def test_service_resolves_inherited_view_materialization_from_runtime_target() -> None:
  dataset = _dataset(
    "serving.customer",
    materialization_type=None,
    incremental_strategy="full",
  )
  state = _state(dataset)

  plan = build_execution_impact_plan(
    scope_key="all",
    current_state=state,
    baseline_resolution=_baseline(state),
    report=FakeReport(
      current_state=state,
      previous_state=state,
      dataset_keys=("serving.customer",),
    ),
    review_status=_review_status(),
    target_datasets=(
      _target(
        "serving.customer",
        incremental_strategy="full",
        materialization_type=None,
        schema_materialization_type="view",
      ),
    ),
    dependency_resolver=_resolver({}),
  )

  assert plan.items[0].decision == "REUSE"
  assert plan.items[0].reason_codes == (
    "NO_RELEVANT_CHANGE",
    "VIRTUAL_MATERIALIZATION_REUSED",
  )


def test_service_rebuilds_view_after_upstream_full_refresh() -> None:
  """
  Verify service assembly recreates a managed view after an upstream rebuild.
  """
  previous = _state(
    _dataset(
      "raw.customer",
      incremental_strategy="full",
      materialization_type="table",
    ),
    _dataset(
      "serving.customer",
      incremental_strategy="full",
      materialization_type=None,
    ),
  )
  current = previous

  raw = _target(
    "raw.customer",
    incremental_strategy="full",
    materialization_type=None,
    schema_materialization_type="table",
  )
  serving = _target(
    "serving.customer",
    incremental_strategy="full",
    materialization_type=None,
    schema_materialization_type="view",
  )

  plan = build_execution_impact_plan(
    scope_key="all",
    current_state=current,
    baseline_resolution=_baseline(previous),
    report=FakeReport(
      current_state=current,
      previous_state=previous,
      dataset_keys=(
        "raw.customer",
        "serving.customer",
      ),
    ),
    review_status=_review_status(),
    target_datasets=(serving, raw),
    dependency_resolver=_resolver({
      "serving.customer": (
        DummyDependency(raw, "lineage_input"),
      ),
    }),
  )
  items = _items_by_key(plan)

  assert plan.decision_counts == {
    "REUSE": 0,
    "REVALIDATE": 0,
    "INCREMENTAL_EXECUTE": 0,
    "FULL_REBUILD": 2,
    "BLOCKED": 0,
  }
  assert items["raw.customer"].decision == "FULL_REBUILD"
  assert items["serving.customer"].decision == "FULL_REBUILD"
  assert items["serving.customer"].reason_codes == (
    "UPSTREAM_REBUILD_REQUIRED",
  )
  assert items["serving.customer"].propagations[0].reason_code == (
    "UPSTREAM_REBUILD_REQUIRED"
  )


def test_service_plan_is_deterministic_across_target_order() -> None:
  state = _state(
    _dataset("raw.customer"),
    _dataset("stage.customer"),
  )
  raw = _target("raw.customer")
  stage = _target("stage.customer")
  report = FakeReport(
    current_state=state,
    previous_state=state,
    dataset_keys=("stage.customer", "raw.customer"),
  )
  kwargs = {
    "scope_key": "all",
    "current_state": state,
    "baseline_resolution": _baseline(state),
    "report": report,
    "review_status": _review_status(),
    "dependency_resolver": _resolver({
      "stage.customer": (DummyDependency(raw, "lineage_input"),),
    }),
  }

  left = build_execution_impact_plan(
    **kwargs,
    target_datasets=(raw, stage),
  )
  right = build_execution_impact_plan(
    **kwargs,
    target_datasets=(stage, raw),
  )

  assert left.to_dict() == right.to_dict()
  assert left.plan_fingerprint == right.plan_fingerprint


def test_service_rejects_report_current_state_mismatch_before_resolution() -> None:
  report_state = _state(_dataset("raw.customer"))
  supplied_state = _state(
    _dataset("raw.customer", incremental_strategy="full")
  )
  report = FakeReport(
    current_state=report_state,
    previous_state=report_state,
    dataset_keys=("raw.customer",),
  )

  with pytest.raises(
    ExecutionImpactPlanError,
    match="current fingerprint does not match",
  ):
    build_execution_impact_plan(
      scope_key="all",
      current_state=supplied_state,
      baseline_resolution=_baseline(report_state),
      report=report,
      review_status=_review_status(),
      target_datasets=(_target("raw.customer"),),
      dependency_resolver=_resolver({}),
    )


def test_service_rejects_report_baseline_mismatch() -> None:
  current = _state(_dataset("raw.customer"))
  report_baseline = _state(_dataset("raw.customer", incremental_strategy="full"))
  supplied_baseline = _state(_dataset("raw.customer", incremental_strategy="append"))
  report = FakeReport(
    current_state=current,
    previous_state=report_baseline,
    dataset_keys=("raw.customer",),
  )

  with pytest.raises(
    ExecutionImpactPlanError,
    match="previous fingerprint does not match",
  ):
    build_execution_impact_plan(
      scope_key="all",
      current_state=current,
      baseline_resolution=_baseline(supplied_baseline),
      report=report,
      review_status=_review_status(),
      target_datasets=(_target("raw.customer"),),
      dependency_resolver=_resolver({}),
    )


def test_service_rejects_review_status_from_another_report_or_scope() -> None:
  state = _state(_dataset("raw.customer"))
  report = FakeReport(
    current_state=state,
    previous_state=state,
    dataset_keys=("raw.customer",),
  )

  with pytest.raises(
    ExecutionImpactPlanError,
    match="does not belong",
  ):
    build_execution_impact_plan(
      scope_key="all",
      current_state=state,
      baseline_resolution=_baseline(state),
      report=report,
      review_status=_review_status(report_fingerprint="other-report"),
      target_datasets=(_target("raw.customer"),),
      dependency_resolver=_resolver({}),
    )

  with pytest.raises(
    ExecutionImpactPlanError,
    match="scope does not match",
  ):
    build_execution_impact_plan(
      scope_key="all",
      current_state=state,
      baseline_resolution=_baseline(state),
      report=report,
      review_status=_review_status(scope_key="raw.customer"),
      target_datasets=(_target("raw.customer"),),
      dependency_resolver=_resolver({}),
    )


def test_service_wraps_dependency_cycle_with_stage_context() -> None:
  state = _state(
    _dataset("raw.a"),
    _dataset("raw.b"),
  )
  a = _target("raw.a")
  b = _target("raw.b")

  with pytest.raises(
    ExecutionImpactPlanError,
    match="dependency propagation failed: Cycle detected",
  ):
    build_execution_impact_plan(
      scope_key="all",
      current_state=state,
      baseline_resolution=_baseline(state),
      report=FakeReport(
        current_state=state,
        previous_state=state,
        dataset_keys=("raw.a", "raw.b"),
      ),
      review_status=_review_status(),
      target_datasets=(a, b),
      dependency_resolver=_resolver({
        "raw.a": (DummyDependency(b, "lineage_input"),),
        "raw.b": (DummyDependency(a, "lineage_input"),),
      }),
    )


def test_service_target_only_uses_exact_execution_scope() -> None:
  previous = _state(
    _dataset("stage.customer", incremental_strategy="full"),
    _dataset("rawcore.customer", incremental_strategy="merge"),
    _dataset("rawcore.customer_hist", incremental_strategy="historize"),
  )
  current = previous
  report = FakeReport(
    current_state=current,
    previous_state=previous,
    dataset_keys=(
      "rawcore.customer",
      "rawcore.customer_hist",
    ),
  )
  stage = _target("stage.customer", incremental_strategy="full")
  rawcore = _target("rawcore.customer", incremental_strategy="merge")
  hist = _target(
    "rawcore.customer_hist",
    incremental_strategy="historize",
  )

  plan = build_execution_impact_plan(
    scope_key="rawcore.customer",
    current_state=current,
    baseline_resolution=_baseline(previous),
    report=report,
    review_status=_review_status(scope_key="rawcore.customer"),
    target_datasets=(hist, rawcore, stage),
    dependency_resolver=_resolver({
      "rawcore.customer": (
        DummyDependency(stage, "lineage_input"),
      ),
      "rawcore.customer_hist": (
        DummyDependency(rawcore, "lineage_input"),
      ),
    }),
    execution_dataset_keys=("rawcore.customer",),
    dependency_mode="target_only",
  )

  assert tuple(item.dataset_key for item in plan.items) == (
    "rawcore.customer",
  )
  assert plan.items[0].decision == "INCREMENTAL_EXECUTE"
  assert plan.items[0].reason_codes == ("INCREMENTAL_LOAD_STRATEGY",)
  assert plan.items[0].has_unavailable_required_evidence is False


def test_service_with_dependencies_can_assess_upstreams_outside_review_scope() -> None:
  previous = _state(
    _dataset("stage.customer", incremental_strategy="full"),
    _dataset("rawcore.customer", incremental_strategy="merge"),
    _dataset("rawcore.customer_hist", incremental_strategy="historize"),
  )
  current = previous
  report = FakeReport(
    current_state=current,
    previous_state=previous,
    dataset_keys=(
      "rawcore.customer",
      "rawcore.customer_hist",
    ),
  )
  stage = _target("stage.customer", incremental_strategy="full")
  rawcore = _target("rawcore.customer", incremental_strategy="merge")
  hist = _target(
    "rawcore.customer_hist",
    incremental_strategy="historize",
  )

  plan = build_execution_impact_plan(
    scope_key="rawcore.customer",
    current_state=current,
    baseline_resolution=_baseline(previous),
    report=report,
    review_status=_review_status(scope_key="rawcore.customer"),
    target_datasets=(hist, rawcore, stage),
    dependency_resolver=_resolver({
      "rawcore.customer": (
        DummyDependency(stage, "lineage_input"),
      ),
    }),
    execution_dataset_keys=(
      "stage.customer",
      "rawcore.customer",
    ),
    dependency_mode="with_dependencies",
  )

  assert {
    item.dataset_key
    for item in plan.items
  } == {
    "stage.customer",
    "rawcore.customer",
  }
  assert "rawcore.customer_hist" not in {
    item.dataset_key
    for item in plan.items
  }
  assert all(item.decision != "BLOCKED" for item in plan.items)


def test_service_reuses_view_for_lineage_only_identity_migration() -> None:
  previous = _state(_dataset(
    "serving.customer",
    materialization_type="view",
    incremental_strategy="full",
    columns=(
      _column("customer_id", lineage_key="legacy:1:6"),
    ),
  ))
  current = _state(_dataset(
    "serving.customer",
    materialization_type="view",
    incremental_strategy="full",
    columns=(
      _column(
        "customer_id",
        lineage_key="generated:serving:" + ("a" * 64),
      ),
    ),
  ))

  assert previous.fingerprint == current.fingerprint

  plan = build_execution_impact_plan(
    scope_key="all",
    current_state=current,
    baseline_resolution=_baseline(previous),
    report=FakeReport(
      current_state=current,
      previous_state=previous,
      dataset_keys=("serving.customer",),
    ),
    review_status=_review_status(),
    target_datasets=(
      _target(
        "serving.customer",
        incremental_strategy="full",
        materialization_type="view",
      ),
    ),
    dependency_resolver=_resolver({}),
  )

  assert plan.items[0].decision == "REUSE"
  assert plan.items[0].reason_codes == (
    "NO_RELEVANT_CHANGE",
    "VIRTUAL_MATERIALIZATION_REUSED",
  )
