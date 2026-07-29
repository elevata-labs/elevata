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

from io import StringIO

import pytest
from django.core.management.base import CommandError

from metadata.architecture.execution_run_outcome import (
  write_execution_run_plan_step_outcome,
)
from metadata.architecture.execution_run_plan import (
  ExecutionRunPlan,
  ExecutionRunPlanStore,
)
from metadata.architecture.state import (
  ArchitectureState,
  ColumnState,
  DatasetState,
)
from metadata.architecture.store import ArchitectureStateStore
from metadata.management.commands import elevata_finalize_run_plan


def _state() -> ArchitectureState:
  """
  Return one current and physically validated Architecture State.
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
          nullable=True,
          active=True,
        ),
      ),
    ),
  ))


def _plan() -> ExecutionRunPlan:
  """
  Return one legacy initial-deployment Run Plan.
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
    architecture_fingerprint="a" * 64,
    report_fingerprint="b" * 64,
    impact_plan_fingerprint="c" * 64,
    preview_fingerprint="d" * 64,
    execution_plan_fingerprint="e" * 64,
    root_dataset_keys=("rawcore.customer",),
    dataset_decisions=(("rawcore.customer", "FULL_REBUILD"),),
  )


def _prepare_run(tmp_path) -> tuple[ExecutionRunPlan, object]:
  """
  Persist one legacy Run Plan and its successful scheduler outcome.
  """
  plan = _plan()
  run_plan_path = ExecutionRunPlanStore(
    base_path=tmp_path,
  ).save(plan)
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
  return plan, run_plan_path


def test_recovery_persists_physically_validated_current_state(
  monkeypatch,
  tmp_path,
) -> None:
  """
  Verify an interrupted initial deployment is recovered without target reset.
  """
  plan, run_plan_path = _prepare_run(tmp_path)
  state = _state()
  state_dir = tmp_path / "state"

  class TestArchitectureStateStore(ArchitectureStateStore):
    """
    Store recovered state in the isolated test directory.
    """

    def __init__(self, *args, **kwargs):
      super().__init__(base_path=state_dir)

  monkeypatch.setattr(
    elevata_finalize_run_plan,
    "ArchitectureStateStore",
    TestArchitectureStateStore,
  )
  monkeypatch.setattr(
    elevata_finalize_run_plan,
    "_resolve_recovery_architecture",
    lambda **kwargs: (
      state,
      state,
      ("fallback introspection used",),
    ),
  )

  (
    recovered_plan,
    outcomes,
    state_path,
    recovery_path,
    recovery,
  ) = elevata_finalize_run_plan.recover_interrupted_initial_deployment(
    run_plan_path=run_plan_path,
  )

  assert recovered_plan == plan
  assert len(outcomes) == 1
  assert ArchitectureStateStore.load_file(state_path) == state
  assert recovery_path.exists()
  assert recovery["metadata_changed_after_plan"] is True
  assert recovery["warnings"] == ["fallback introspection used"]

  (
    _second_plan,
    _second_outcomes,
    second_state_path,
    second_recovery_path,
    second_recovery,
  ) = elevata_finalize_run_plan.recover_interrupted_initial_deployment(
    run_plan_path=run_plan_path,
  )

  assert second_state_path == state_path
  assert second_recovery_path == recovery_path
  assert second_recovery == recovery


def test_recovery_command_prints_operator_summary(
  monkeypatch,
  tmp_path,
) -> None:
  """
  Verify the explicit recovery option presents actionable operator output.
  """
  plan = _plan()
  state_path = tmp_path / "architecture_state.json"
  recovery_path = tmp_path / "run-plan.recovered.json"
  recovery = {
    "physical_validation": "matched_current_metadata",
    "recovered_architecture_fingerprint": "f" * 64,
    "metadata_changed_after_plan": True,
    "recovery_fingerprint": "e" * 64,
    "warnings": [],
  }

  monkeypatch.setattr(
    elevata_finalize_run_plan,
    "recover_interrupted_initial_deployment",
    lambda **kwargs: (
      plan,
      ({"dataset_key": "rawcore.customer"},),
      state_path,
      recovery_path,
      recovery,
    ),
  )

  stdout = StringIO()
  command = elevata_finalize_run_plan.Command()
  command.stdout = stdout
  command.handle(
    run_plan_path=tmp_path / "run-plan.json",
    recover_interrupted_initial_deployment=True,
    print_json=False,
  )

  output = stdout.getvalue()
  assert "Interrupted initial deployment recovered" in output
  assert "Physical validation: matched_current_metadata" in output
  assert "Metadata changed after original plan: True" in output


def test_recovery_rejects_unrelated_recorded_state(
  monkeypatch,
  tmp_path,
) -> None:
  """
  Verify recovery cannot replace an independently established baseline.
  """
  _plan_value, run_plan_path = _prepare_run(tmp_path)
  state_dir = tmp_path / "state"
  ArchitectureStateStore.save_file(
    state_dir / "architecture_state.json",
    _state(),
  )

  class TestArchitectureStateStore(ArchitectureStateStore):
    """
    Resolve the existing state from the isolated test directory.
    """

    def __init__(self, *args, **kwargs):
      super().__init__(base_path=state_dir)

  monkeypatch.setattr(
    elevata_finalize_run_plan,
    "ArchitectureStateStore",
    TestArchitectureStateStore,
  )

  with pytest.raises(
    CommandError,
    match="A baseline already exists",
  ):
    elevata_finalize_run_plan.recover_interrupted_initial_deployment(
      run_plan_path=run_plan_path,
    )
