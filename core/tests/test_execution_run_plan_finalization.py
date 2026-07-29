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

import copy

import pytest

from metadata.architecture.execution_run_finalization import (
  build_execution_run_plan_finalization,
  execution_run_plan_finalization_path,
  load_execution_run_plan_finalization,
  load_execution_run_plan_outcomes,
  validate_execution_run_plan_finalization,
  write_execution_run_plan_finalization,
)
from metadata.architecture.execution_run_outcome import (
  build_execution_run_plan_step_outcome,
  write_execution_run_plan_step_outcome,
)
from metadata.architecture.execution_run_plan import (
  ExecutionRunPlan,
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
    root_dataset_keys=("raw.customer",),
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


def _success_outcomes(
  plan: ExecutionRunPlan,
  *,
  raw_recorded_at: str = (
    "2026-07-25T04:01:00Z"
  ),
  stage_recorded_at: str = (
    "2026-07-25T04:02:00Z"
  ),
) -> tuple[dict[str, object], ...]:
  raw = build_execution_run_plan_step_outcome(
    plan=plan,
    dataset_key="raw.customer",
    result={
      "dataset": "raw.customer",
      "status": "success",
      "kind": "ingestion",
      "load_run_id": "load-raw",
      "message": None,
    },
    had_error=False,
    recorded_at=raw_recorded_at,
  )

  stage = build_execution_run_plan_step_outcome(
    plan=plan,
    dataset_key="stage.customer",
    result={
      "dataset": "stage.customer",
      "status": "skipped",
      "kind": "impact_reuse",
      "load_run_id": "load-stage",
      "message": (
        "reused_by_execution_impact_plan"
      ),
      "status_reason": (
        "execution_impact_reuse"
      ),
    },
    had_error=False,
    recorded_at=stage_recorded_at,
  )

  return raw, stage


def test_build_finalization_binds_complete_success() -> None:
  plan = _plan()

  payload = build_execution_run_plan_finalization(
    plan=plan,
    outcomes=_success_outcomes(plan),
  )

  assert payload["batch_run_id"] == "batch-001"
  assert payload["dataset_count"] == 2
  assert payload["had_error"] is False
  assert payload["finalized_at"] == (
    "2026-07-25T04:02:00Z"
  )
  assert payload["decision_counts"] == {
    "FULL_REBUILD": 1,
    "REUSE": 1,
  }
  assert payload["status_counts"] == {
    "skipped": 1,
    "success": 1,
  }
  assert len(
    payload["outcome_bindings"]
  ) == 2

  validated = (
    validate_execution_run_plan_finalization(
      payload,
      plan=plan,
    )
  )
  assert (
    validated["finalization_fingerprint"]
    == payload["finalization_fingerprint"]
  )


def test_finalization_rejects_invalid_reuse() -> None:
  plan = _plan()
  raw, _stage = _success_outcomes(plan)
  stage = build_execution_run_plan_step_outcome(
    plan=plan,
    dataset_key="stage.customer",
    result={
      "dataset": "stage.customer",
      "status": "success",
      "kind": "sql",
      "load_run_id": "load-stage",
      "message": None,
    },
    had_error=False,
    recorded_at="2026-07-25T04:02:00Z",
  )

  with pytest.raises(
    ValueError,
    match="impact-reuse outcome",
  ):
    build_execution_run_plan_finalization(
      plan=plan,
      outcomes=(raw, stage),
    )


def test_finalization_accepts_external_ingestion_handoff() -> None:
  """
  Verify a physically guarded external RAW handoff is a successful no-op.
  """
  plan = _plan()
  _raw, stage = _success_outcomes(plan)
  raw = build_execution_run_plan_step_outcome(
    plan=plan,
    dataset_key="raw.customer",
    result={
      "dataset": "raw.customer",
      "status": "skipped",
      "kind": "ingestion",
      "load_run_id": "load-raw",
      # Legacy scheduler outcomes stored the reason in message only.
      "message": "external_ingest",
    },
    had_error=False,
    recorded_at="2026-07-25T04:01:00Z",
  )

  payload = build_execution_run_plan_finalization(
    plan=plan,
    outcomes=(raw, stage),
  )

  assert payload["had_error"] is False
  assert payload["status_counts"] == {
    "skipped": 2,
  }
  assert payload["kind_counts"] == {
    "impact_reuse": 1,
    "ingestion": 1,
  }


def test_finalization_rejects_unconfigured_ingestion_skip() -> None:
  """
  Verify defensive include-ingest-none skips cannot finalize execution.
  """
  plan = _plan()
  _raw, stage = _success_outcomes(plan)
  raw = build_execution_run_plan_step_outcome(
    plan=plan,
    dataset_key="raw.customer",
    result={
      "dataset": "raw.customer",
      "status": "skipped",
      "kind": "ingestion",
      "load_run_id": "load-raw",
      "message": "include_ingest_none",
      "status_reason": "include_ingest_none",
    },
    had_error=False,
    recorded_at="2026-07-25T04:01:00Z",
  )

  with pytest.raises(
    ValueError,
    match="status_reason=include_ingest_none",
  ):
    build_execution_run_plan_finalization(
      plan=plan,
      outcomes=(raw, stage),
    )


def test_finalization_rejects_failed_execution() -> None:
  plan = _plan()
  _raw, stage = _success_outcomes(plan)
  raw = build_execution_run_plan_step_outcome(
    plan=plan,
    dataset_key="raw.customer",
    result={
      "dataset": "raw.customer",
      "status": "error",
      "kind": "ingestion",
      "load_run_id": "load-raw",
      "message": "source unavailable",
    },
    had_error=True,
    recorded_at="2026-07-25T04:01:00Z",
  )

  with pytest.raises(
    ValueError,
    match="reports an error",
  ):
    build_execution_run_plan_finalization(
      plan=plan,
      outcomes=(raw, stage),
    )


def test_load_outcomes_requires_complete_scope(
  tmp_path,
) -> None:
  plan = _plan()
  run_plan_path = tmp_path / "run-plan.json"
  raw, _stage = _success_outcomes(plan)

  write_execution_run_plan_step_outcome(
    run_plan_path=run_plan_path,
    plan=plan,
    dataset_key="raw.customer",
    result=raw["outcome"],
    had_error=False,
    recorded_at=raw["recorded_at"],
  )

  with pytest.raises(
    ValueError,
    match="Missing scheduler-step outcomes",
  ):
    load_execution_run_plan_outcomes(
      run_plan_path=run_plan_path,
      plan=plan,
    )


def test_write_finalization_is_idempotent(
  tmp_path,
) -> None:
  plan = _plan()
  run_plan_path = tmp_path / "run-plan.json"
  outcomes = _success_outcomes(plan)

  first_path = (
    write_execution_run_plan_finalization(
      run_plan_path=run_plan_path,
      plan=plan,
      outcomes=outcomes,
    )
  )
  second_path = (
    write_execution_run_plan_finalization(
      run_plan_path=run_plan_path,
      plan=plan,
      outcomes=outcomes,
    )
  )

  assert first_path == second_path
  assert first_path == (
    execution_run_plan_finalization_path(
      run_plan_path
    )
  )

  loaded = load_execution_run_plan_finalization(
    first_path,
    plan=plan,
  )
  assert loaded["had_error"] is False


def test_finalization_rejects_tampering() -> None:
  plan = _plan()
  payload = build_execution_run_plan_finalization(
    plan=plan,
    outcomes=_success_outcomes(plan),
  )
  tampered = copy.deepcopy(payload)
  tampered["dataset_count"] = 99

  with pytest.raises(
    ValueError,
    match="fingerprint mismatch",
  ):
    validate_execution_run_plan_finalization(
      tampered,
      plan=plan,
    )
    

def test_finalization_rejects_tampered_step_evidence() -> None:
  """
  Verify finalization validates every bound outcome fingerprint.
  """
  plan = _plan()
  raw, stage = _success_outcomes(plan)

  raw["outcome"]["rows_affected"] = 999

  with pytest.raises(
    ValueError,
    match="fingerprint mismatch",
  ):
    build_execution_run_plan_finalization(
      plan=plan,
      outcomes=(raw, stage),
    )


def test_finalization_uses_latest_actual_timestamp() -> None:
  """
  Verify timestamp ordering does not depend on ISO string length.
  """
  plan = _plan()
  outcomes = _success_outcomes(
    plan,
    raw_recorded_at=(
      "2026-07-25T04:02:00.900000Z"
    ),
    stage_recorded_at=(
      "2026-07-25T04:02:00Z"
    ),
  )

  payload = build_execution_run_plan_finalization(
    plan=plan,
    outcomes=outcomes,
  )

  assert payload["finalized_at"] == (
    "2026-07-25T04:02:00.900000Z"
  )
