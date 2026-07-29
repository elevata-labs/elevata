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

from datetime import datetime, timezone

import pytest
from django.core.management.base import CommandError

from metadata.architecture.execution_run_outcome import (
  build_execution_run_plan_step_outcome,
  execution_run_plan_step_outcome_path,
  load_execution_run_plan_step_outcome,
  validate_execution_run_plan_step_outcome,
  write_execution_run_plan_step_outcome,
)
from metadata.architecture.execution_run_plan import (
  ExecutionRunPlan,
)
from metadata.management.commands import (
  elevata_load as elevata_load_command,
)


def _plan() -> ExecutionRunPlan:
  return ExecutionRunPlan(
    run_plan_id="run-plan-001",
    batch_run_id="batch-001",
    created_at="2026-07-24T20:00:00Z",
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
      "stage.customer",
    ),
    dataset_decisions=(
      (
        "stage.customer",
        "FULL_REBUILD",
      ),
    ),
  )


def _result(
  *,
  status: str = "success",
) -> dict[str, object]:
  return {
    "dataset": "stage.customer",
    "status": status,
    "kind": "sql",
    "load_run_id": "load-001",
    "message": None,
    "rows_affected": 12,
    "attempt_no": 1,
    "status_reason": None,
    "blocked_by": None,
    "started_at": datetime(
      2026,
      7,
      24,
      20,
      1,
      tzinfo=timezone.utc,
    ),
    "finished_at": datetime(
      2026,
      7,
      24,
      20,
      2,
      tzinfo=timezone.utc,
    ),
    "render_ms": 2.5,
    "execution_ms": 15.0,
    "sql_length": 120,
  }


def test_build_step_outcome_binds_result_to_run_plan() -> None:
  plan = _plan()

  payload = build_execution_run_plan_step_outcome(
    plan=plan,
    dataset_key="stage.customer",
    result=_result(),
    had_error=False,
    recorded_at="2026-07-24T20:02:01Z",
  )

  assert payload["run_plan_id"] == "run-plan-001"
  assert payload["batch_run_id"] == "batch-001"
  assert payload["dataset_key"] == "stage.customer"
  assert payload["decision"] == "FULL_REBUILD"
  assert payload["had_error"] is False
  assert (
    payload["outcome"]["started_at"]
    == "2026-07-24T20:01:00+00:00"
  )
  assert len(
    payload["outcome_fingerprint"]
  ) == 64

  validated = (
    validate_execution_run_plan_step_outcome(
      payload,
      plan=plan,
      expected_dataset_key="stage.customer",
    )
  )

  assert (
    validated["outcome"]["status"]
    == "success"
  )


def test_step_outcome_path_is_deterministic(
  tmp_path,
) -> None:
  run_plan_path = (
    tmp_path
    / "airflow-run.json"
  )

  first = execution_run_plan_step_outcome_path(
    run_plan_path=run_plan_path,
    dataset_key="stage.customer",
  )
  second = execution_run_plan_step_outcome_path(
    run_plan_path=run_plan_path,
    dataset_key="stage.customer",
  )

  assert first == second
  assert (
    first.parent
    == tmp_path / "airflow-run.outcomes"
  )
  assert first.suffix == ".json"


def test_write_step_outcome_replaces_retry(
  tmp_path,
) -> None:
  plan = _plan()
  run_plan_path = (
    tmp_path
    / "airflow-run.json"
  )

  first_path = (
    write_execution_run_plan_step_outcome(
      run_plan_path=run_plan_path,
      plan=plan,
      dataset_key="stage.customer",
      result=_result(status="error"),
      had_error=True,
      recorded_at="2026-07-24T20:02:01Z",
    )
  )

  second_path = (
    write_execution_run_plan_step_outcome(
      run_plan_path=run_plan_path,
      plan=plan,
      dataset_key="stage.customer",
      result=_result(status="success"),
      had_error=False,
      recorded_at="2026-07-24T20:03:01Z",
    )
  )

  assert first_path == second_path

  payload = (
    load_execution_run_plan_step_outcome(
      second_path,
      plan=plan,
      expected_dataset_key="stage.customer",
    )
  )

  assert payload["had_error"] is False
  assert (
    payload["outcome"]["status"]
    == "success"
  )


def test_step_outcome_rejects_tampering() -> None:
  plan = _plan()

  payload = build_execution_run_plan_step_outcome(
    plan=plan,
    dataset_key="stage.customer",
    result=_result(),
    had_error=False,
    recorded_at="2026-07-24T20:02:01Z",
  )
  payload["outcome"]["status"] = "error"

  with pytest.raises(
    ValueError,
    match="fingerprint mismatch",
  ):
    validate_execution_run_plan_step_outcome(
      payload,
      plan=plan,
    )


def test_step_outcome_rejects_another_dataset() -> None:
  result = _result()
  result["dataset"] = "stage.other"

  with pytest.raises(
    ValueError,
    match="does not match the scheduler step",
  ):
    build_execution_run_plan_step_outcome(
      plan=_plan(),
      dataset_key="stage.customer",
      result=result,
      had_error=False,
      recorded_at="2026-07-24T20:02:01Z",
    )


def test_load_adapter_persists_bound_step_outcome(
  monkeypatch,
  tmp_path,
) -> None:
  """
  Verify elevata_load passes the complete scheduler-step binding.
  """
  plan = _plan()
  result = _result()
  run_plan_path = tmp_path / "run-plan.json"
  expected_path = tmp_path / "outcome.json"
  calls: dict[str, object] = {}

  def fake_write_execution_run_plan_step_outcome(
    **kwargs,
  ):
    calls.update(kwargs)
    return expected_path

  monkeypatch.setattr(
    elevata_load_command,
    "write_execution_run_plan_step_outcome",
    fake_write_execution_run_plan_step_outcome,
  )

  actual_path = (
    elevata_load_command
    ._persist_execution_run_plan_step_outcome(
      run_plan_path=run_plan_path,
      plan=plan,
      dataset_key="stage.customer",
      result=result,
      had_error=False,
      recorded_at="2026-07-24T20:02:01Z",
    )
  )

  assert actual_path == expected_path
  assert calls == {
    "run_plan_path": run_plan_path,
    "plan": plan,
    "dataset_key": "stage.customer",
    "result": result,
    "had_error": False,
    "recorded_at": "2026-07-24T20:02:01Z",
  }


def test_load_adapter_surfaces_outcome_write_failure(
  monkeypatch,
  tmp_path,
) -> None:
  """
  Verify a scheduler task cannot succeed without its outcome artifact.
  """
  def fail_write(**kwargs):
    raise OSError("outcome directory is not writable")

  monkeypatch.setattr(
    elevata_load_command,
    "write_execution_run_plan_step_outcome",
    fail_write,
  )

  with pytest.raises(
    CommandError,
    match="outcome directory is not writable",
  ):
    (
      elevata_load_command
      ._persist_execution_run_plan_step_outcome(
        run_plan_path=tmp_path / "run-plan.json",
        plan=_plan(),
        dataset_key="stage.customer",
        result=_result(),
        had_error=False,
        recorded_at="2026-07-24T20:02:01Z",
      )
    )
