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

from __future__ import annotations

from dataclasses import replace

import pytest

from metadata.architecture.execution_run_plan import (
  ExecutionRunPlan,
  ExecutionRunPlanStore,
)
from metadata.architecture.execution_run_plan_state import (
  build_execution_run_plan_architecture_drift,
  execution_run_plan_planned_state_path,
  load_execution_run_plan_planned_state,
  save_execution_run_plan_bundle,
  validate_execution_run_plan_dataset_state,
  write_execution_run_plan_planned_state,
)
from metadata.architecture.state import (
  ArchitectureState,
  ColumnState,
  DatasetState,
)


def _dataset_state(
  *,
  nullable: bool = False,
) -> DatasetState:
  """
  Return one deterministic dataset state for planned-state tests.
  """
  return DatasetState(
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
  )


def _state(
  *,
  nullable: bool = False,
) -> ArchitectureState:
  """
  Return one deterministic architecture state.
  """
  return ArchitectureState(
    datasets=(
      _dataset_state(nullable=nullable),
    )
  )


def _plan(state: ArchitectureState) -> ExecutionRunPlan:
  """
  Return one run plan bound to the supplied architecture state.
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


def test_run_plan_bundle_persists_bound_planned_state(
  tmp_path,
) -> None:
  """
  Verify one immutable plan and its planned state are stored together.
  """
  state = _state()
  plan = _plan(state)
  store = ExecutionRunPlanStore(base_path=tmp_path)

  plan_path, state_path = save_execution_run_plan_bundle(
    store=store,
    plan=plan,
    planned_state=state,
  )

  assert plan_path == tmp_path / "run-plan-001.run_plan.json"
  assert state_path == execution_run_plan_planned_state_path(
    plan_path
  )
  assert store.load_path(plan_path) == plan
  assert (
    load_execution_run_plan_planned_state(
      run_plan_path=plan_path,
      plan=plan,
    )
    == state
  )


def test_planned_state_rejects_another_architecture(
  tmp_path,
) -> None:
  """
  Verify a planned-state artifact cannot be rebound to another plan state.
  """
  state = _state()
  plan = _plan(state)
  run_plan_path = tmp_path / "run-plan.json"

  with pytest.raises(
    ValueError,
    match="fingerprint does not match",
  ):
    write_execution_run_plan_planned_state(
      run_plan_path=run_plan_path,
      plan=plan,
      planned_state=_state(nullable=True),
    )


def test_dataset_guard_reports_concrete_metadata_change() -> None:
  """
  Verify scheduler steps fail with an actionable dataset-level change detail.
  """
  state = _state()
  plan = _plan(state)

  with pytest.raises(
    ValueError,
    match=(
      "superseded by metadata changes.*"
      "COLUMN_CHANGED rawcore.customer.customer_id.*nullable"
    ),
  ):
    validate_execution_run_plan_dataset_state(
      plan=plan,
      planned_state=state,
      dataset_key="rawcore.customer",
      current_dataset_state=_dataset_state(nullable=True),
    )


def test_post_plan_drift_preserves_planned_state_and_explains_change() -> None:
  """
  Verify finalization drift is reported without redefining the planned state.
  """
  planned = _state()
  current = _state(nullable=True)

  drift = build_execution_run_plan_architecture_drift(
    planned_state=planned,
    current_state=current,
  )

  assert drift.status == "changed"
  assert drift.planned_fingerprint == planned.fingerprint
  assert drift.current_fingerprint == current.fingerprint
  assert drift.dataset_changes == ()
  assert drift.column_changes == (
    "COLUMN_CHANGED rawcore.customer.customer_id fields=nullable",
  )


def test_existing_bundle_rejects_plan_identifier_reuse(
  tmp_path,
) -> None:
  """
  Verify another immutable plan cannot replace an existing bundle.
  """
  state = _state()
  plan = _plan(state)
  store = ExecutionRunPlanStore(base_path=tmp_path)
  save_execution_run_plan_bundle(
    store=store,
    plan=plan,
    planned_state=state,
  )

  with pytest.raises(
    ValueError,
    match="another immutable plan",
  ):
    save_execution_run_plan_bundle(
      store=store,
      plan=replace(
        plan,
        batch_run_id="batch-002",
      ),
      planned_state=state,
    )
