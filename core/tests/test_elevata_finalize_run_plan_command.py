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
from types import SimpleNamespace

import pytest
from django.core.management.base import CommandError

from metadata.architecture.execution_run_plan import (
  ExecutionRunPlan,
)
from metadata.architecture.execution_run_plan_state import (
  ExecutionRunPlanArchitectureDrift,
)
from metadata.management.commands import (
  elevata_finalize_run_plan as command_module,
)


def _plan() -> ExecutionRunPlan:
  return ExecutionRunPlan(
    run_plan_id="run-plan-001",
    batch_run_id="batch-001",
    created_at="2026-07-25T04:00:00Z",
    profile_name="dev",
    target_system_short="msdwh",
    scope_mode="all",
    scope_key="all",
    scope_label="All datasets",
    dependency_mode="with_dependencies",
    review_status="ready",
    approval_id=None,
    architecture_fingerprint="a" * 64,
    report_fingerprint="b" * 64,
    impact_plan_fingerprint="c" * 64,
    preview_fingerprint="d" * 64,
    execution_plan_fingerprint="e" * 64,
    root_dataset_keys=(
      "raw.customer",
    ),
    dataset_decisions=(
      (
        "raw.customer",
        "FULL_REBUILD",
      ),
      (
        "stage.customer",
        "REUSE",
      ),
    ),
  )


def _finalization() -> dict[str, object]:
  return {
    "decision_counts": {
      "FULL_REBUILD": 1,
      "REUSE": 1,
    },
    "status_counts": {
      "skipped": 1,
      "success": 1,
    },
    "finalization_fingerprint": "f" * 64,
  }


def _unchanged_drift(
  plan: ExecutionRunPlan,
) -> ExecutionRunPlanArchitectureDrift:
  """
  Return a no-drift result bound to the supplied Run Plan.
  """
  return ExecutionRunPlanArchitectureDrift(
    status="unchanged",
    planned_fingerprint=plan.architecture_fingerprint,
    current_fingerprint=plan.architecture_fingerprint,
    message="Current metadata still matches the planned architecture.",
  )


def test_finalize_adapter_writes_artifact(
  monkeypatch,
  tmp_path,
) -> None:
  """
  Verify the adapter binds planned architecture and outcomes.
  """
  plan = _plan()
  planned_state = SimpleNamespace(
    fingerprint=plan.architecture_fingerprint
  )
  drift = _unchanged_drift(plan)
  run_plan_path = (
    tmp_path / "run-plan.json"
  )
  finalization_path = (
    tmp_path
    / "run-plan.finalized.json"
  )
  state_path = (
    tmp_path
    / "architecture_state.json"
  )
  outcomes = (
    {
      "dataset_key": "raw.customer",
    },
    {
      "dataset_key": "stage.customer",
    },
  )
  calls: dict[str, object] = {}

  monkeypatch.setattr(
    command_module,
    "_load_execution_run_plan",
    lambda path: plan,
  )
  monkeypatch.setattr(
    command_module,
    "load_execution_run_plan_planned_state",
    lambda **kwargs: planned_state,
  )

  def fake_load_outcomes(**kwargs):
    calls["load_outcomes"] = kwargs
    return outcomes

  def fake_build_finalization(**kwargs):
    calls["build_finalization"] = kwargs
    return _finalization()

  def fake_persist_state(**kwargs):
    calls["persist_state"] = kwargs
    return state_path

  def fake_write_finalization(**kwargs):
    calls["write_finalization"] = kwargs
    return finalization_path

  def fake_load_finalization(
    path,
    **kwargs,
  ):
    calls["load_finalization"] = {
      "path": path,
      **kwargs,
    }
    return _finalization()

  monkeypatch.setattr(
    command_module,
    "load_execution_run_plan_outcomes",
    fake_load_outcomes,
  )
  monkeypatch.setattr(
    command_module,
    "build_execution_run_plan_finalization",
    fake_build_finalization,
  )
  monkeypatch.setattr(
    command_module,
    "_persist_planned_architecture_state",
    fake_persist_state,
  )
  monkeypatch.setattr(
    command_module,
    "write_execution_run_plan_finalization",
    fake_write_finalization,
  )
  monkeypatch.setattr(
    command_module,
    "load_execution_run_plan_finalization",
    fake_load_finalization,
  )
  monkeypatch.setattr(
    command_module,
    "_resolve_post_plan_architecture_drift",
    lambda **kwargs: drift,
  )

  result = (
    command_module
    .finalize_execution_run_plan(
      run_plan_path=run_plan_path,
    )
  )

  assert result == (
    plan,
    outcomes,
    state_path,
    finalization_path,
    _finalization(),
    drift,
  )

  assert calls["load_outcomes"] == {
    "run_plan_path": run_plan_path,
    "plan": plan,
  }
  assert calls["build_finalization"] == {
    "plan": plan,
    "outcomes": outcomes,
  }
  assert calls["persist_state"] == {
    "plan": plan,
    "planned_state": planned_state,
  }
  assert calls["write_finalization"] == {
    "run_plan_path": run_plan_path,
    "plan": plan,
    "outcomes": outcomes,
  }
  assert calls["load_finalization"] == {
    "path": finalization_path,
    "plan": plan,
  }


def test_finalize_adapter_surfaces_incomplete_outcomes(
  monkeypatch,
  tmp_path,
) -> None:
  """
  Verify an incomplete scheduler run cannot be finalized.
  """
  plan = _plan()

  monkeypatch.setattr(
    command_module,
    "_load_execution_run_plan",
    lambda path: plan,
  )
  monkeypatch.setattr(
    command_module,
    "load_execution_run_plan_planned_state",
    lambda **kwargs: SimpleNamespace(
      fingerprint=plan.architecture_fingerprint
    ),
  )

  def fail_load_outcomes(**kwargs):
    raise ValueError(
      "Missing scheduler-step outcomes for: "
      "stage.customer."
    )

  monkeypatch.setattr(
    command_module,
    "load_execution_run_plan_outcomes",
    fail_load_outcomes,
  )

  with pytest.raises(
    CommandError,
    match="Missing scheduler-step outcomes",
  ):
    (
      command_module
      .finalize_execution_run_plan(
        run_plan_path=(
          tmp_path / "run-plan.json"
        ),
      )
    )


def test_command_prints_finalization_summary(
  monkeypatch,
  tmp_path,
) -> None:
  """
  Verify stable operator-facing finalization output.
  """
  plan = _plan()
  state_path = (
    tmp_path
    / "architecture_state.json"
  )
  finalization_path = (
    tmp_path
    / "run-plan.finalized.json"
  )
  drift = _unchanged_drift(plan)

  monkeypatch.setattr(
    command_module,
    "finalize_execution_run_plan",
    lambda **kwargs: (
      plan,
      (
        {
          "dataset_key": "raw.customer",
        },
        {
          "dataset_key": "stage.customer",
        },
      ),
      state_path,
      finalization_path,
      _finalization(),
      drift,
    ),
  )

  stdout = StringIO()
  command = command_module.Command()
  command.stdout = stdout

  command.handle(
    run_plan_path=(
      tmp_path / "run-plan.json"
    ),
    print_json=False,
  )

  output = stdout.getvalue()

  assert (
    "Execution Run Plan finalized"
    in output
  )
  assert (
    "Run plan id: run-plan-001"
    in output
  )
  assert (
    "Batch run id: batch-001"
    in output
  )
  assert "Datasets: 2" in output
  assert "Architecture state:" in output
  assert (
    "Finalization fingerprint"
    in output
  )
  assert "Post-plan metadata drift: none" in output
