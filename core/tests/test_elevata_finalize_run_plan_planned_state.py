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

from metadata.architecture.execution_run_outcome import (
  write_execution_run_plan_step_outcome,
)
from metadata.architecture.execution_run_plan import (
  ExecutionRunPlan,
  ExecutionRunPlanStore,
)
from metadata.architecture.execution_run_plan_state import (
  save_execution_run_plan_bundle,
)
from metadata.architecture.state import (
  ArchitectureState,
  ColumnState,
  DatasetState,
)
from metadata.architecture.store import ArchitectureStateStore
from metadata.management.commands import elevata_finalize_run_plan


def _state(nullable: bool = False) -> ArchitectureState:
  """
  Return one architecture state used by finalization tests.
  """
  return ArchitectureState(datasets=(
    DatasetState(
      dataset_key="rawcore.customer",
      schema_short_name="rawcore",
      dataset_name="customer",
      materialization_type=None,
      incremental_strategy="merge",
      historize=True,
      is_hist=False,
      active=True,
      column_states=(
        ColumnState(
          column_name="customer_id",
          datatype="INT",
          nullable=nullable,
          active=True,
        ),
      ),
    ),
  ))


def _plan(state: ArchitectureState) -> ExecutionRunPlan:
  """
  Return one finalizable run plan.
  """
  return ExecutionRunPlan(
    run_plan_id="run-plan-001",
    batch_run_id="batch-001",
    created_at="2026-07-27T04:00:00Z",
    profile_name="dev",
    target_system_short="msdwh",
    scope_mode="all",
    scope_key="all",
    scope_label="All datasets",
    dependency_mode="with_dependencies",
    review_status="initial_deployment",
    approval_id=None,
    architecture_fingerprint=state.fingerprint,
    report_fingerprint="b" * 64,
    impact_plan_fingerprint="c" * 64,
    preview_fingerprint="d" * 64,
    execution_plan_fingerprint="e" * 64,
    root_dataset_keys=("rawcore.customer",),
    dataset_decisions=(("rawcore.customer", "FULL_REBUILD"),),
  )


def test_finalizer_persists_planned_state_and_reports_later_drift(
  monkeypatch,
  tmp_path,
) -> None:
  """
  Verify metadata drift no longer invalidates an otherwise successful run.
  """
  planned = _state()
  current = _state(nullable=True)
  plan = _plan(planned)
  run_plan_path, _planned_state_path = save_execution_run_plan_bundle(
    store=ExecutionRunPlanStore(base_path=tmp_path),
    plan=plan,
    planned_state=planned,
  )
  write_execution_run_plan_step_outcome(
    run_plan_path=run_plan_path,
    plan=plan,
    dataset_key="rawcore.customer",
    result={
      "dataset": "rawcore.customer",
      "status": "success",
      "kind": "sql",
      "load_run_id": "load-001",
      "attempt_no": 1,
    },
    had_error=False,
    recorded_at="2026-07-27T04:05:00Z",
  )

  state_dir = tmp_path / "applied-state"

  class TestArchitectureStateStore(ArchitectureStateStore):
    """
    Store applied state in the isolated test directory.
    """

    def __init__(self, *args, **kwargs):
      super().__init__(base_path=state_dir)

  monkeypatch.setattr(
    elevata_finalize_run_plan,
    "ArchitectureStateStore",
    TestArchitectureStateStore,
  )
  monkeypatch.setattr(
    elevata_finalize_run_plan.ArchitectureStateService,
    "build_current_state",
    lambda self: current,
  )

  (
    finalized_plan,
    outcomes,
    state_path,
    finalization_path,
    finalization,
    drift,
  ) = elevata_finalize_run_plan.finalize_execution_run_plan(
    run_plan_path=run_plan_path,
  )

  assert finalized_plan == plan
  assert len(outcomes) == 1
  assert finalization_path.exists()
  assert finalization["architecture_fingerprint"] == planned.fingerprint
  assert ArchitectureStateStore.load_file(state_path) == planned
  assert drift.status == "changed"
  assert drift.current_fingerprint == current.fingerprint
  assert drift.column_changes == (
    "COLUMN_CHANGED rawcore.customer.customer_id fields=nullable",
  )

  ArchitectureStateStore.save_file(
    state_path,
    current,
  )
  (
    _second_plan,
    _second_outcomes,
    second_state_path,
    _second_finalization_path,
    _second_finalization,
    _second_drift,
  ) = elevata_finalize_run_plan.finalize_execution_run_plan(
    run_plan_path=run_plan_path,
  )

  assert second_state_path == state_path
  assert ArchitectureStateStore.load_file(state_path) == current
