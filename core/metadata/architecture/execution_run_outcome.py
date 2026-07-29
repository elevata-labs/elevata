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

from collections.abc import Mapping
from pathlib import Path
import hashlib
import json
import os
import uuid

from metadata.architecture.execution_run_plan import ExecutionRunPlan


ARTIFACT_TYPE = "execution_run_plan_step_outcome"
ARTIFACT_VERSION = 1

_OUTCOME_FIELDS = (
  "dataset",
  "status",
  "kind",
  "load_run_id",
  "message",
  "rows_affected",
  "attempt_no",
  "status_reason",
  "blocked_by",
  "started_at",
  "finished_at",
  "render_ms",
  "execution_ms",
  "sql_length",
)


def _required_text(
  value: object,
  *,
  label: str,
) -> str:
  """
  Return one normalized required text value.
  """
  text = str(value or "").strip()
  if not text:
    raise ValueError(f"{label} is required.")
  return text


def _json_value(value: object) -> object:
  """
  Return a deterministic JSON-compatible scalar value.
  """
  if value is None or isinstance(
    value,
    (str, int, float, bool),
  ):
    return value

  isoformat = getattr(value, "isoformat", None)
  if callable(isoformat):
    return isoformat()

  return str(value)


def _stable_json_hash(
  payload: Mapping[str, object],
) -> str:
  """
  Return the canonical SHA-256 fingerprint of one JSON payload.
  """
  canonical = json.dumps(
    payload,
    sort_keys=True,
    separators=(",", ":"),
    ensure_ascii=False,
    allow_nan=False,
  )
  return hashlib.sha256(
    canonical.encode("utf-8")
  ).hexdigest()


def execution_run_plan_step_outcome_dir(
  run_plan_path: str | Path,
) -> Path:
  """
  Return the deterministic outcome directory beside one Run Plan.
  """
  plan_path = Path(run_plan_path)
  return (
    plan_path.parent
    / f"{plan_path.stem}.outcomes"
  )


def execution_run_plan_step_outcome_path(
  *,
  run_plan_path: str | Path,
  dataset_key: str,
) -> Path:
  """
  Return the deterministic artifact path for one dataset step.
  """
  normalized_dataset_key = _required_text(
    dataset_key,
    label="Execution Run Plan step dataset key",
  )
  dataset_token = hashlib.sha256(
    normalized_dataset_key.encode("utf-8")
  ).hexdigest()

  return (
    execution_run_plan_step_outcome_dir(
      run_plan_path
    )
    / f"{dataset_token}.json"
  )


def build_execution_run_plan_step_outcome(
  *,
  plan: ExecutionRunPlan,
  dataset_key: str,
  result: Mapping[str, object],
  had_error: bool,
  recorded_at: object,
) -> dict[str, object]:
  """
  Build one scheduler-step outcome bound to an immutable Run Plan.
  """
  if not isinstance(plan, ExecutionRunPlan):
    raise ValueError(
      "Invalid Execution Run Plan step outcome contract."
    )

  if not isinstance(result, Mapping):
    raise ValueError(
      "Execution Run Plan step result must be a mapping."
    )

  normalized_dataset_key = _required_text(
    dataset_key,
    label="Execution Run Plan step dataset key",
  )
  result_dataset_key = _required_text(
    result.get("dataset"),
    label="Execution Run Plan step result dataset key",
  )

  if result_dataset_key != normalized_dataset_key:
    raise ValueError(
      "Execution Run Plan step result dataset does not match "
      "the scheduler step: "
      f"{result_dataset_key!r} != "
      f"{normalized_dataset_key!r}."
    )

  decision = plan.decision_for_dataset(
    normalized_dataset_key
  )

  outcome = {
    field: _json_value(result.get(field))
    for field in _OUTCOME_FIELDS
  }

  _required_text(
    outcome.get("status"),
    label="Execution Run Plan step status",
  )
  _required_text(
    outcome.get("kind"),
    label="Execution Run Plan step kind",
  )

  payload: dict[str, object] = {
    "artifact_type": ARTIFACT_TYPE,
    "artifact_version": ARTIFACT_VERSION,
    "run_plan_id": plan.run_plan_id,
    "run_plan_fingerprint": (
      plan.run_plan_fingerprint
    ),
    "batch_run_id": plan.batch_run_id,
    "profile_name": plan.profile_name,
    "target_system_short": (
      plan.target_system_short
    ),
    "dataset_key": normalized_dataset_key,
    "decision": decision,
    "recorded_at": _json_value(recorded_at),
    "had_error": bool(had_error),
    "outcome": outcome,
  }

  _required_text(
    payload["recorded_at"],
    label="Execution Run Plan step recorded_at",
  )

  payload["outcome_fingerprint"] = (
    _stable_json_hash(payload)
  )

  return payload


def validate_execution_run_plan_step_outcome(
  payload: Mapping[str, object],
  *,
  plan: ExecutionRunPlan | None = None,
  expected_dataset_key: str | None = None,
) -> dict[str, object]:
  """
  Validate one scheduler-step outcome and its Run Plan binding.
  """
  if not isinstance(payload, Mapping):
    raise ValueError(
      "Execution Run Plan step outcome must be a JSON object."
    )

  normalized = dict(payload)

  if normalized.get("artifact_type") != ARTIFACT_TYPE:
    raise ValueError(
      "Unsupported Execution Run Plan step outcome "
      "artifact type."
    )

  if (
    normalized.get("artifact_version")
    != ARTIFACT_VERSION
  ):
    raise ValueError(
      "Unsupported Execution Run Plan step outcome "
      "artifact version."
    )

  actual_fingerprint = _required_text(
    normalized.get("outcome_fingerprint"),
    label=(
      "Execution Run Plan step outcome "
      "fingerprint"
    ),
  )
  fingerprint_payload = {
    key: value
    for key, value in normalized.items()
    if key != "outcome_fingerprint"
  }
  expected_fingerprint = _stable_json_hash(
    fingerprint_payload
  )

  if actual_fingerprint != expected_fingerprint:
    raise ValueError(
      "Execution Run Plan step outcome "
      "fingerprint mismatch."
    )

  dataset_key = _required_text(
    normalized.get("dataset_key"),
    label="Execution Run Plan step dataset key",
  )
  _required_text(
    normalized.get("recorded_at"),
    label="Execution Run Plan step recorded_at",
  )

  outcome = normalized.get("outcome")
  if not isinstance(outcome, Mapping):
    raise ValueError(
      "Execution Run Plan step outcome details "
      "must be a JSON object."
    )

  outcome_dict = dict(outcome)
  result_dataset_key = _required_text(
    outcome_dict.get("dataset"),
    label="Execution Run Plan step result dataset key",
  )

  if result_dataset_key != dataset_key:
    raise ValueError(
      "Execution Run Plan step outcome dataset "
      "binding is inconsistent."
    )

  _required_text(
    outcome_dict.get("status"),
    label="Execution Run Plan step status",
  )
  _required_text(
    outcome_dict.get("kind"),
    label="Execution Run Plan step kind",
  )

  if not isinstance(
    normalized.get("had_error"),
    bool,
  ):
    raise ValueError(
      "Execution Run Plan step had_error "
      "must be boolean."
    )

  if expected_dataset_key is not None:
    expected_key = _required_text(
      expected_dataset_key,
      label=(
        "Expected Execution Run Plan step "
        "dataset key"
      ),
    )

    if dataset_key != expected_key:
      raise ValueError(
        "Execution Run Plan step outcome belongs "
        "to another dataset."
      )

  if plan is not None:
    if not isinstance(plan, ExecutionRunPlan):
      raise ValueError(
        "Invalid Execution Run Plan step "
        "validation contract."
      )

    expected_values = {
      "run_plan_id": plan.run_plan_id,
      "run_plan_fingerprint": (
        plan.run_plan_fingerprint
      ),
      "batch_run_id": plan.batch_run_id,
      "profile_name": plan.profile_name,
      "target_system_short": (
        plan.target_system_short
      ),
      "decision": plan.decision_for_dataset(
        dataset_key
      ),
    }

    mismatches = [
      field
      for field, expected in expected_values.items()
      if normalized.get(field) != expected
    ]

    if mismatches:
      raise ValueError(
        "Execution Run Plan step outcome does not "
        "match the bound Run Plan fields: "
        + ", ".join(mismatches)
        + "."
      )

  normalized["outcome"] = outcome_dict
  return normalized


def write_execution_run_plan_step_outcome(
  *,
  run_plan_path: str | Path,
  plan: ExecutionRunPlan,
  dataset_key: str,
  result: Mapping[str, object],
  had_error: bool,
  recorded_at: object,
) -> Path:
  """
  Atomically write or replace one scheduler-step outcome.

  A retry for the same dataset and Run Plan intentionally replaces
  the previous outcome at the same deterministic artifact path.
  """
  payload = build_execution_run_plan_step_outcome(
    plan=plan,
    dataset_key=dataset_key,
    result=result,
    had_error=had_error,
    recorded_at=recorded_at,
  )

  validate_execution_run_plan_step_outcome(
    payload,
    plan=plan,
    expected_dataset_key=dataset_key,
  )

  path = execution_run_plan_step_outcome_path(
    run_plan_path=run_plan_path,
    dataset_key=dataset_key,
  )
  path.parent.mkdir(
    parents=True,
    exist_ok=True,
  )

  temp_path = path.with_name(
    f".{path.name}.{uuid.uuid4().hex}.tmp"
  )

  try:
    temp_path.write_text(
      json.dumps(
        payload,
        indent=2,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
      ) + "\n",
      encoding="utf-8",
    )
    os.replace(
      temp_path,
      path,
    )
  finally:
    try:
      temp_path.unlink()
    except OSError:
      pass

  return path


def load_execution_run_plan_step_outcome(
  path: str | Path,
  *,
  plan: ExecutionRunPlan | None = None,
  expected_dataset_key: str | None = None,
) -> dict[str, object]:
  """
  Load and validate one scheduler-step outcome artifact.
  """
  outcome_path = Path(path)

  try:
    payload = json.loads(
      outcome_path.read_text(
        encoding="utf-8"
      )
    )
  except OSError as exc:
    raise ValueError(
      "Execution Run Plan step outcome could not "
      f"be read: {outcome_path}"
    ) from exc
  except json.JSONDecodeError as exc:
    raise ValueError(
      "Execution Run Plan step outcome contains "
      f"invalid JSON: {outcome_path}"
    ) from exc

  return validate_execution_run_plan_step_outcome(
    payload,
    plan=plan,
    expected_dataset_key=expected_dataset_key,
  )
