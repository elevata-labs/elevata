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

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from metadata.architecture.execution_impact_evidence import (
  resolve_execution_impact_evidence,
)
from metadata.architecture.state import ArchitectureState, ColumnState, DatasetState


def _column(name: str, *, datatype: str = "string") -> ColumnState:
  return ColumnState(
    column_name=name,
    datatype=datatype,
    nullable=True,
    active=True,
    lineage_key=f"lk_{name}",
  )


def _dataset(
  dataset_key: str,
  *,
  materialization_type: str = "table",
  incremental_strategy: str = "full",
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


class FakeReport:
  def __init__(
    self,
    *,
    dataset_keys: tuple[str, ...],
    dataset_changes: tuple[dict[str, Any], ...] = (),
    column_changes: tuple[dict[str, Any], ...] = (),
    migration_actions: tuple[dict[str, Any], ...] = (),
    policy_decisions: tuple[dict[str, Any], ...] = (),
    report_fingerprint: str = "report-fingerprint",
    is_blocked: bool = False,
  ):
    self.report_fingerprint = report_fingerprint
    self.has_changes = bool(
      dataset_changes or column_changes or migration_actions
    )
    self.is_blocked = is_blocked
    self._payload = {
      "report_fingerprint": report_fingerprint,
      "scope": {
        "mode": "scoped",
        "dataset_keys": list(reversed(dataset_keys)),
      },
      "state": {
        "has_changes": self.has_changes,
      },
      "summary": {
        "dataset_change_count": len(dataset_changes),
        "column_change_count": len(column_changes),
        "migration_action_count": len(migration_actions),
        "policy_decision_count": len(policy_decisions),
      },
      "dataset_changes": list(reversed(dataset_changes)),
      "column_changes": list(reversed(column_changes)),
      "migration_actions": list(reversed(migration_actions)),
      "policy_decisions": list(reversed(policy_decisions)),
      "is_blocked": is_blocked,
    }

  def to_dict(self) -> dict[str, Any]:
    return self._payload


def _review_status(
  *,
  status: str = "no_changes",
  has_changes: bool = False,
  is_blocked: bool = False,
  approval_id: str | None = None,
  artifact_fingerprint: str | None = None,
  review_decision: str | None = None,
) -> SimpleNamespace:
  return SimpleNamespace(
    status=status,
    message=f"review status: {status}",
    report_fingerprint="report-fingerprint",
    approval_id=approval_id,
    artifact_fingerprint=artifact_fingerprint,
    review_decision=review_decision,
    has_changes=has_changes,
    is_blocked=is_blocked,
  )


def _baseline(
  previous_state: ArchitectureState | None,
  *,
  source: str = "recorded_state",
  can_execute: bool = True,
) -> SimpleNamespace:
  return SimpleNamespace(
    previous_state=previous_state,
    source=source,
    can_execute=can_execute,
    message="baseline resolution message",
    state_file=Path(".elevata/state/dev/dwh/architecture_state.json"),
  )


class FakeExecutionStore:
  def __init__(self, summaries: tuple[SimpleNamespace, ...]):
    self.summaries = summaries
    self.list_calls = 0

  def list_records(self, *, limit=None):
    assert limit is None
    self.list_calls += 1
    return self.summaries


class FailingExecutionStore:
  def list_records(self, *, limit=None):
    raise ValueError("execution store unreadable")


def _execution_summary(
  *,
  execution_id: str,
  scope_key: str,
  started_at: str,
  status: str = "success",
) -> SimpleNamespace:
  return SimpleNamespace(
    execution_id=execution_id,
    scope_key=scope_key,
    status=status,
    started_at=started_at,
    finished_at=started_at,
    record_fingerprint=f"record-{execution_id}",
    path=f"/runtime/executions/{execution_id}.execution.json",
  )


def test_resolve_execution_impact_evidence_normalizes_dataset_signals() -> None:
  previous = _state(_dataset("rawcore.customer"))
  current = _state(
    _dataset(
      "rawcore.customer",
      incremental_strategy="merge",
      columns=(_column("id"), _column("customer_name")),
    )
  )
  report = FakeReport(
    dataset_keys=("rawcore.customer",),
    dataset_changes=(
      {
        "dataset_key": "rawcore.customer",
        "change_type": "DATASET_CHANGED",
        "details": {
          "changed_fields": ["incremental_strategy", "materialization_type"],
        },
      },
    ),
    column_changes=(
      {
        "dataset_key": "rawcore.customer",
        "change_type": "COLUMN_ADDED",
        "column_name": "customer_name",
      },
    ),
    migration_actions=(
      {
        "dataset_key": "rawcore.customer",
        "action_type": "ADD_COLUMN",
        "column_name": "customer_name",
      },
    ),
    policy_decisions=(
      {
        "dataset_key": "rawcore.customer",
        "status": "ALLOW",
        "code": "ADD_COLUMN_ALLOWED",
        "action_type": "ADD_COLUMN",
      },
    ),
  )

  resolution = resolve_execution_impact_evidence(
    scope_key="rawcore.customer",
    current_state=current,
    baseline_resolution=_baseline(previous),
    report=report,
    review_status=_review_status(
      status="approved",
      has_changes=True,
      approval_id="apr_123",
      artifact_fingerprint="approval-fingerprint",
      review_decision="approved",
    ),
  )

  item = resolution.datasets[0]
  assert item.materialization_type == "table"
  assert item.incremental_strategy == "merge"
  assert item.dataset_change_types == ("DATASET_CHANGED",)
  assert item.dataset_changed_fields == (
    "incremental_strategy",
    "materialization_type",
  )
  assert item.column_change_types == ("COLUMN_ADDED",)
  assert item.migration_action_types == ("ADD_COLUMN",)
  assert item.policy_statuses == ("ALLOW",)
  assert item.policy_codes == ("ADD_COLUMN_ALLOWED",)
  assert item.has_architecture_changes is True
  assert item.has_blocking_policy_decision is False

  approval = next(
    evidence
    for evidence in item.evidence
    if evidence.evidence_type == "architecture_approval"
  )
  assert approval.status == "available"
  assert approval.required is True
  assert approval.fingerprint == "approval-fingerprint"


def test_new_dataset_baseline_is_not_applicable_not_unavailable() -> None:
  previous = _state()
  current = _state(_dataset("rawcore.customer"))
  report = FakeReport(
    dataset_keys=("rawcore.customer",),
    dataset_changes=(
      {
        "dataset_key": "rawcore.customer",
        "change_type": "DATASET_ADDED",
      },
    ),
    migration_actions=(
      {
        "dataset_key": "rawcore.customer",
        "action_type": "CREATE_DATASET",
      },
    ),
  )

  resolution = resolve_execution_impact_evidence(
    scope_key="rawcore.customer",
    current_state=current,
    baseline_resolution=_baseline(previous),
    report=report,
    review_status=_review_status(status="pending", has_changes=True),
  )

  baseline_evidence = next(
    evidence
    for evidence in resolution.datasets[0].evidence
    if evidence.evidence_type == "architecture_state"
    and evidence.evidence_key == "baseline:rawcore.customer"
  )
  assert baseline_evidence.status == "not_applicable"

  approval_evidence = next(
    evidence
    for evidence in resolution.datasets[0].evidence
    if evidence.evidence_type == "architecture_approval"
  )
  assert approval_evidence.status == "unavailable"
  assert approval_evidence.required is True


def test_missing_baseline_remains_explicit() -> None:
  current = _state(_dataset("rawcore.customer"))
  report = FakeReport(dataset_keys=("rawcore.customer",))

  resolution = resolve_execution_impact_evidence(
    scope_key="rawcore.customer",
    current_state=current,
    baseline_resolution=_baseline(
      None,
      source="missing_or_unsupported",
      can_execute=False,
    ),
    report=report,
    review_status=_review_status(),
  )

  item = resolution.datasets[0]
  baseline = next(
    evidence
    for evidence in item.evidence
    if evidence.evidence_type == "architecture_state"
    and evidence.evidence_key == "baseline:rawcore.customer"
  )
  assert baseline.status == "unavailable"
  assert resolution.baseline_can_execute is False


def test_exact_scope_execution_record_is_resolved_without_output_claim() -> None:
  state = _state(_dataset("rawcore.customer"))
  report = FakeReport(dataset_keys=("rawcore.customer",))
  store = FakeExecutionStore((
    _execution_summary(
      execution_id="all_newer",
      scope_key="all",
      started_at="2026-07-19T12:00:00+00:00",
    ),
    _execution_summary(
      execution_id="dataset_old",
      scope_key="rawcore.customer",
      started_at="2026-07-18T12:00:00+00:00",
    ),
    _execution_summary(
      execution_id="dataset_new",
      scope_key="rawcore.customer",
      started_at="2026-07-19T10:00:00+00:00",
    ),
  ))

  resolution = resolve_execution_impact_evidence(
    scope_key="rawcore.customer",
    current_state=state,
    baseline_resolution=_baseline(state),
    report=report,
    review_status=_review_status(),
    execution_record_store=store,
  )

  item = resolution.datasets[0]
  assert item.latest_successful_execution_id == "dataset_new"
  assert (
    item.latest_successful_execution_record_fingerprint
    == "record-dataset_new"
  )

  execution_evidence = next(
    evidence
    for evidence in item.evidence
    if evidence.evidence_type == "architecture_execution_record"
  )
  assert execution_evidence.status == "available"
  assert execution_evidence.required is False
  assert "not an output or materialization fingerprint" in (
    execution_evidence.message or ""
  )
  assert execution_evidence.artifact_reference == (
    "dataset_new.execution.json"
  )


def test_schema_or_all_execution_records_are_not_promoted_to_dataset_evidence() -> None:
  state = _state(_dataset("rawcore.customer"))
  report = FakeReport(dataset_keys=("rawcore.customer",))
  store = FakeExecutionStore((
    _execution_summary(
      execution_id="schema_run",
      scope_key="schema:rawcore",
      started_at="2026-07-19T12:00:00+00:00",
    ),
    _execution_summary(
      execution_id="all_run",
      scope_key="all",
      started_at="2026-07-19T13:00:00+00:00",
    ),
  ))

  resolution = resolve_execution_impact_evidence(
    scope_key="rawcore.customer",
    current_state=state,
    baseline_resolution=_baseline(state),
    report=report,
    review_status=_review_status(),
    execution_record_store=store,
  )

  item = resolution.datasets[0]
  assert item.latest_successful_execution_id is None
  execution_evidence = next(
    evidence
    for evidence in item.evidence
    if evidence.evidence_type == "architecture_execution_record"
  )
  assert execution_evidence.status == "unavailable"


def test_execution_record_store_is_read_once_for_complete_scope() -> None:
  state = _state(
    _dataset("rawcore.customer"),
    _dataset("rawcore.order"),
  )
  report = FakeReport(
    dataset_keys=("rawcore.customer", "rawcore.order"),
  )
  store = FakeExecutionStore((
    _execution_summary(
      execution_id="customer_run",
      scope_key="rawcore.customer",
      started_at="2026-07-19T10:00:00+00:00",
    ),
    _execution_summary(
      execution_id="order_run",
      scope_key="rawcore.order",
      started_at="2026-07-19T11:00:00+00:00",
    ),
  ))

  resolution = resolve_execution_impact_evidence(
    scope_key="all",
    current_state=state,
    baseline_resolution=_baseline(state),
    report=report,
    review_status=_review_status(),
    execution_record_store=store,
  )

  assert store.list_calls == 1
  assert {
    item.dataset_key: item.latest_successful_execution_id
    for item in resolution.datasets
  } == {
    "rawcore.customer": "customer_run",
    "rawcore.order": "order_run",
  }


def test_execution_record_store_failure_is_explicit_evidence() -> None:
  state = _state(_dataset("rawcore.customer"))
  report = FakeReport(dataset_keys=("rawcore.customer",))

  resolution = resolve_execution_impact_evidence(
    scope_key="rawcore.customer",
    current_state=state,
    baseline_resolution=_baseline(state),
    report=report,
    review_status=_review_status(),
    execution_record_store=FailingExecutionStore(),
  )

  execution_evidence = next(
    evidence
    for evidence in resolution.datasets[0].evidence
    if evidence.evidence_type == "architecture_execution_record"
  )
  assert execution_evidence.status == "unavailable"
  assert "execution store unreadable" in (execution_evidence.message or "")


def test_physical_discovery_remains_explicit_as_baseline_source() -> None:
  physical_state = _state(_dataset("rawcore.customer"))
  current = _state(_dataset("rawcore.customer"))
  report = FakeReport(dataset_keys=("rawcore.customer",))

  resolution = resolve_execution_impact_evidence(
    scope_key="rawcore.customer",
    current_state=current,
    baseline_resolution=_baseline(
      physical_state,
      source="discovered_physical_state",
    ),
    report=report,
    review_status=_review_status(),
  )

  baseline = next(
    evidence
    for evidence in resolution.datasets[0].evidence
    if evidence.evidence_type == "architecture_state"
    and evidence.evidence_key == "baseline:rawcore.customer"
  )
  assert resolution.baseline_source == "discovered_physical_state"
  assert baseline.status == "available"
  assert baseline.fingerprint == physical_state.datasets[0].fingerprint


def test_evidence_resolution_fingerprint_is_independent_of_report_list_order() -> None:
  customer = _dataset("rawcore.customer")
  order = _dataset("rawcore.order")
  state = _state(customer, order)
  changes = (
    {
      "dataset_key": "rawcore.customer",
      "change_type": "DATASET_CHANGED",
      "details": {"changed_fields": ["incremental_strategy"]},
    },
    {
      "dataset_key": "rawcore.order",
      "change_type": "DATASET_CHANGED",
      "details": {"changed_fields": ["materialization_type"]},
    },
  )

  first = resolve_execution_impact_evidence(
    scope_key="all",
    current_state=state,
    baseline_resolution=_baseline(state),
    report=FakeReport(
      dataset_keys=("rawcore.customer", "rawcore.order"),
      dataset_changes=changes,
    ),
    review_status=_review_status(status="approved", has_changes=True),
  )
  second = resolve_execution_impact_evidence(
    scope_key="all",
    current_state=state,
    baseline_resolution=_baseline(state),
    report=FakeReport(
      dataset_keys=("rawcore.order", "rawcore.customer"),
      dataset_changes=tuple(reversed(changes)),
    ),
    review_status=_review_status(status="approved", has_changes=True),
  )

  assert first.to_dict() == second.to_dict()
  assert first.resolution_fingerprint == second.resolution_fingerprint


def test_report_scope_must_reference_current_datasets() -> None:
  state = _state(_dataset("rawcore.customer"))

  with pytest.raises(ValueError, match="missing from the current Architecture State"):
    resolve_execution_impact_evidence(
      scope_key="rawcore.missing",
      current_state=state,
      baseline_resolution=_baseline(state),
      report=FakeReport(dataset_keys=("rawcore.missing",)),
      review_status=_review_status(),
    )
