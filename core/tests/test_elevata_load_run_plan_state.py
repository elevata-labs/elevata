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

from types import SimpleNamespace

import pytest
from django.core.management.base import CommandError

from metadata.architecture.execution_run_plan import ExecutionRunPlan
from metadata.architecture.state import (
  ArchitectureState,
  ColumnState,
  DatasetState,
)
from metadata.management.commands import elevata_load


def _dataset_state(nullable: bool = False) -> DatasetState:
  """
  Return one current or planned dataset state.
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


def _plan(state: ArchitectureState) -> ExecutionRunPlan:
  """
  Return one scheduler plan bound to the supplied state.
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
    review_status="ready",
    approval_id=None,
    architecture_fingerprint=state.fingerprint,
    report_fingerprint="b" * 64,
    impact_plan_fingerprint="c" * 64,
    preview_fingerprint="d" * 64,
    execution_plan_fingerprint="e" * 64,
    root_dataset_keys=("rawcore.customer",),
    dataset_decisions=(("rawcore.customer", "INCREMENTAL_EXECUTE"),),
  )


def _target_dataset():
  """
  Return one TargetDataset-shaped scheduler-step value.
  """
  return SimpleNamespace(
    target_schema=SimpleNamespace(short_name="rawcore"),
    target_dataset_name="customer",
  )


def test_scheduler_step_accepts_matching_planned_dataset(
  monkeypatch,
) -> None:
  """
  Verify an unchanged dataset retains its planned incremental decision.
  """
  state = ArchitectureState(datasets=(_dataset_state(),))
  plan = _plan(state)
  monkeypatch.setattr(
    elevata_load.ArchitectureStateService,
    "build_dataset_state",
    lambda self, target_dataset: _dataset_state(),
  )

  decisions = elevata_load._resolve_execution_run_plan_step_decisions(
    plan=plan,
    planned_architecture_state=state,
    profile_name="dev",
    target_system_short="msdwh",
    execution_order=[_target_dataset()],
  )

  assert decisions == {
    "rawcore.customer": "INCREMENTAL_EXECUTE",
  }


def test_scheduler_step_rejects_superseded_dataset_metadata(
  monkeypatch,
) -> None:
  """
  Verify a changed dataset requires a new DAG run instead of plan reuse.
  """
  state = ArchitectureState(datasets=(_dataset_state(),))
  plan = _plan(state)
  monkeypatch.setattr(
    elevata_load.ArchitectureStateService,
    "build_dataset_state",
    lambda self, target_dataset: _dataset_state(nullable=True),
  )

  with pytest.raises(
    CommandError,
    match="Start a new DAG run",
  ):
    elevata_load._resolve_execution_run_plan_step_decisions(
      plan=plan,
      planned_architecture_state=state,
      profile_name="dev",
      target_system_short="msdwh",
      execution_order=[_target_dataset()],
    )
