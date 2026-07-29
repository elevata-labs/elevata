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

from metadata.architecture.execution_run_plan import ExecutionRunPlan
from metadata.architecture.execution_run_recovery import (
  build_execution_run_plan_recovery,
  execution_run_plan_recovery_path,
  load_execution_run_plan_recovery,
  validate_execution_run_plan_recovery,
  validate_recovery_architecture,
  write_execution_run_plan_recovery,
)
from metadata.architecture.state import (
  ArchitectureState,
  ColumnState,
  DatasetState,
)


def _state(
  *,
  nullable: bool = False,
) -> ArchitectureState:
  """
  Return one deterministic recovered Architecture State.
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


def _plan() -> ExecutionRunPlan:
  """
  Return one legacy initial-deployment Run Plan without a state snapshot.
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


def _finalization() -> dict[str, object]:
  """
  Return validated successful scheduler evidence.
  """
  return {
    "finalization_fingerprint": "f" * 64,
    "finalized_at": "2026-07-27T04:05:00Z",
    "decision_counts": {"FULL_REBUILD": 1},
    "status_counts": {"success": 1},
  }


def test_recovery_binds_current_and_physical_architecture(
  tmp_path,
) -> None:
  """
  Verify one recovery artifact records a physically matched current state.
  """
  plan = _plan()
  state = _state()
  path = tmp_path / "run-plan.json"

  recovery_path = write_execution_run_plan_recovery(
    run_plan_path=path,
    plan=plan,
    finalization=_finalization(),
    recovered_state=state,
    physical_state=state,
    warnings=("fallback introspection used",),
  )
  recovery = load_execution_run_plan_recovery(
    recovery_path,
    plan=plan,
  )

  assert recovery_path == execution_run_plan_recovery_path(path)
  assert recovery["recovered_architecture_fingerprint"] == state.fingerprint
  assert recovery["physical_architecture_fingerprint"] == state.fingerprint
  assert recovery["metadata_changed_after_plan"] is True
  assert recovery["physical_validation"] == "matched_current_metadata"
  assert recovery["warnings"] == ["fallback introspection used"]


def test_recovery_rejects_physical_architecture_drift() -> None:
  """
  Verify recovery cannot establish a baseline that differs from the target.
  """
  with pytest.raises(
    ValueError,
    match="rejected physical architecture drift.*nullable",
  ):
    validate_recovery_architecture(
      plan=_plan(),
      current_state=_state(nullable=True),
      physical_state=_state(nullable=False),
    )


def test_recovery_rejects_non_initial_or_reuse_plan() -> None:
  """
  Verify recovery remains restricted to complete initial deployments.
  """
  plan = replace(
    _plan(),
    review_status="ready",
    dataset_decisions=(("rawcore.customer", "REUSE"),),
  )

  with pytest.raises(
    ValueError,
    match="initial-deployment Run Plan without REUSE",
  ):
    build_execution_run_plan_recovery(
      plan=plan,
      finalization=_finalization(),
      recovered_state=_state(),
      physical_state=_state(),
    )


def test_recovery_rejects_unexpected_artifact_fields() -> None:
  """
  Verify the versioned recovery artifact contract remains closed.
  """
  payload = build_execution_run_plan_recovery(
    plan=_plan(),
    finalization=_finalization(),
    recovered_state=_state(),
    physical_state=_state(),
  )
  payload["unexpected"] = True

  with pytest.raises(
    ValueError,
    match="fields do not match the versioned contract",
  ):
    validate_execution_run_plan_recovery(payload)
