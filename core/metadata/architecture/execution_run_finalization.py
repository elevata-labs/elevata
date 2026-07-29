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

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import os
import uuid

from metadata.architecture.execution_run_outcome import (
  execution_run_plan_step_outcome_path,
  load_execution_run_plan_step_outcome,
  validate_execution_run_plan_step_outcome,
)
from metadata.architecture.execution_run_plan import (
  ExecutionRunPlan,
)


ARTIFACT_TYPE = "execution_run_plan_finalization"
ARTIFACT_VERSION = 1


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


def _normalize_timestamp(
  value: object,
  *,
  label: str,
) -> str:
  """
  Return one canonical UTC timestamp.
  """
  text = _required_text(
    value,
    label=label,
  )

  try:
    parsed = datetime.fromisoformat(
      text.replace("Z", "+00:00")
    )
  except ValueError as exc:
    raise ValueError(
      f"{label} must be an ISO-8601 timestamp."
    ) from exc

  if parsed.tzinfo is None:
    parsed = parsed.replace(
      tzinfo=timezone.utc
    )

  return (
    parsed
    .astimezone(timezone.utc)
    .isoformat()
    .replace("+00:00", "Z")
  )


def execution_run_plan_finalization_path(
  run_plan_path: str | Path,
) -> Path:
  """
  Return the deterministic finalization artifact path.
  """
  plan_path = Path(run_plan_path)
  return plan_path.with_name(
    f"{plan_path.stem}.finalized.json"
  )


def load_execution_run_plan_outcomes(
  *,
  run_plan_path: str | Path,
  plan: ExecutionRunPlan,
) -> tuple[dict[str, object], ...]:
  """
  Load every expected dataset outcome in exact Run Plan order.
  """
  if not isinstance(plan, ExecutionRunPlan):
    raise ValueError(
      "Invalid Execution Run Plan finalization contract."
    )

  outcomes: list[dict[str, object]] = []
  missing_dataset_keys: list[str] = []

  for dataset_key in plan.dataset_keys:
    outcome_path = (
      execution_run_plan_step_outcome_path(
        run_plan_path=run_plan_path,
        dataset_key=dataset_key,
      )
    )

    if not outcome_path.exists():
      missing_dataset_keys.append(
        dataset_key
      )
      continue

    outcome = (
      load_execution_run_plan_step_outcome(
        outcome_path,
        plan=plan,
        expected_dataset_key=dataset_key,
      )
    )
    outcomes.append(outcome)

  if missing_dataset_keys:
    raise ValueError(
      "Execution Run Plan finalization is incomplete. "
      "Missing scheduler-step outcomes for: "
      + ", ".join(missing_dataset_keys)
      + "."
    )

  return tuple(outcomes)


def _validate_successful_outcome(
  *,
  dataset_key: str,
  decision: str,
  payload: Mapping[str, object],
) -> None:
  """
  Validate final success semantics for one dataset decision.
  """
  if payload.get("had_error") is not False:
    raise ValueError(
      "Execution Run Plan dataset outcome reports an error: "
      f"{dataset_key}."
    )

  outcome = payload.get("outcome")
  if not isinstance(outcome, Mapping):
    raise ValueError(
      "Execution Run Plan dataset outcome details are invalid: "
      f"{dataset_key}."
    )

  status = _required_text(
    outcome.get("status"),
    label=(
      "Execution Run Plan dataset "
      f"status for {dataset_key}"
    ),
  )
  kind = _required_text(
    outcome.get("kind"),
    label=(
      "Execution Run Plan dataset "
      f"kind for {dataset_key}"
    ),
  )

  if decision == "REUSE":
    if (
      status != "skipped"
      or kind != "impact_reuse"
      or outcome.get("status_reason")
      != "execution_impact_reuse"
    ):
      raise ValueError(
        "Execution Run Plan REUSE dataset does not "
        "contain the required impact-reuse outcome: "
        f"{dataset_key}."
      )
    return

  if decision in {
    "INCREMENTAL_EXECUTE",
    "FULL_REBUILD",
  }:
    if status == "success":
      return

    skip_reason = str(
      outcome.get("status_reason")
      or outcome.get("message")
      or ""
    ).strip()
    if (
      status == "skipped"
      and kind == "ingestion"
      and skip_reason == "external_ingest"
    ):
      # External RAW ingestion is an intentional handoff. The load runner
      # verifies or non-destructively provisions the landing structure before
      # returning this outcome; data population remains externally owned.
      return

    raise ValueError(
      "Execution Run Plan executable dataset did "
      "not complete successfully: "
      f"{dataset_key} "
      f"(decision={decision}, "
      f"status={status}, kind={kind}, "
      f"status_reason={skip_reason or '<missing>'})."
    )

  raise ValueError(
    "Execution Run Plan contains an unsupported "
    f"finalization decision for {dataset_key}: "
    f"{decision}."
  )


def build_execution_run_plan_finalization(
  *,
  plan: ExecutionRunPlan,
  outcomes: Sequence[Mapping[str, object]],
) -> dict[str, object]:
  """
  Build one deterministic successful batch finalization artifact.
  """
  if not isinstance(plan, ExecutionRunPlan):
    raise ValueError(
      "Invalid Execution Run Plan finalization contract."
    )

  outcome_by_dataset: dict[
    str,
    Mapping[str, object],
  ] = {}

  for payload in outcomes:
    if not isinstance(payload, Mapping):
      raise ValueError(
        "Execution Run Plan finalization outcomes "
        "must be JSON objects."
      )

    dataset_key = _required_text(
      payload.get("dataset_key"),
      label=(
        "Execution Run Plan finalization "
        "dataset key"
      ),
    )

    if dataset_key in outcome_by_dataset:
      raise ValueError(
        "Execution Run Plan finalization contains "
        f"duplicate outcomes for {dataset_key}."
      )

    outcome_by_dataset[dataset_key] = (
      validate_execution_run_plan_step_outcome(
        payload,
        plan=plan,
        expected_dataset_key=dataset_key,
      )
    )

  actual_dataset_keys = set(
    outcome_by_dataset
  )
  expected_dataset_keys = set(
    plan.dataset_keys
  )

  if actual_dataset_keys != expected_dataset_keys:
    missing = sorted(
      expected_dataset_keys
      - actual_dataset_keys
    )
    unexpected = sorted(
      actual_dataset_keys
      - expected_dataset_keys
    )

    details: list[str] = []
    if missing:
      details.append(
        "missing: " + ", ".join(missing)
      )
    if unexpected:
      details.append(
        "unexpected: "
        + ", ".join(unexpected)
      )

    raise ValueError(
      "Execution Run Plan finalization outcome "
      "scope mismatch: "
      + "; ".join(details)
      + "."
    )

  outcome_bindings: list[
    dict[str, str]
  ] = []
  status_counts: Counter[str] = Counter()
  kind_counts: Counter[str] = Counter()
  recorded_timestamps: list[
    tuple[datetime, str]
  ] = []

  for dataset_key, decision in (
    plan.dataset_decisions
  ):
    payload = outcome_by_dataset[
      dataset_key
    ]

    if payload.get("decision") != decision:
      raise ValueError(
        "Execution Run Plan finalization decision "
        f"mismatch for {dataset_key}."
      )

    _validate_successful_outcome(
      dataset_key=dataset_key,
      decision=decision,
      payload=payload,
    )

    outcome = payload["outcome"]
    status_counts[
      str(outcome["status"])
    ] += 1
    kind_counts[
      str(outcome["kind"])
    ] += 1

    outcome_fingerprint = _required_text(
      payload.get("outcome_fingerprint"),
      label=(
        "Execution Run Plan step outcome "
        f"fingerprint for {dataset_key}"
      ),
    )
    recorded_at = _normalize_timestamp(
      payload.get("recorded_at"),
      label=(
        "Execution Run Plan step recorded_at "
        f"for {dataset_key}"
      ),
    )
    recorded_timestamps.append((
      datetime.fromisoformat(
        recorded_at.replace(
          "Z",
          "+00:00",
        )
      ),
      recorded_at,
    ))

    outcome_bindings.append({
      "dataset_key": dataset_key,
      "decision": decision,
      "outcome_fingerprint": (
        outcome_fingerprint
      ),
    })

  finalized_at = max(
    recorded_timestamps,
    key=lambda item: item[0],
  )[1]

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
    "architecture_fingerprint": (
      plan.architecture_fingerprint
    ),
    "impact_plan_fingerprint": (
      plan.impact_plan_fingerprint
    ),
    "execution_plan_fingerprint": (
      plan.execution_plan_fingerprint
    ),
    "finalized_at": finalized_at,
    "dataset_count": plan.dataset_count,
    "decision_counts": dict(
      sorted(
        Counter(
          decision
          for _dataset_key, decision
          in plan.dataset_decisions
        ).items()
      )
    ),
    "status_counts": dict(
      sorted(status_counts.items())
    ),
    "kind_counts": dict(
      sorted(kind_counts.items())
    ),
    "outcome_bindings": (
      outcome_bindings
    ),
    "had_error": False,
  }

  payload["finalization_fingerprint"] = (
    _stable_json_hash(payload)
  )

  return payload


def validate_execution_run_plan_finalization(
  payload: Mapping[str, object],
  *,
  plan: ExecutionRunPlan | None = None,
) -> dict[str, object]:
  """
  Validate one persisted batch finalization artifact.
  """
  if not isinstance(payload, Mapping):
    raise ValueError(
      "Execution Run Plan finalization must be "
      "a JSON object."
    )

  normalized = dict(payload)

  if normalized.get("artifact_type") != ARTIFACT_TYPE:
    raise ValueError(
      "Unsupported Execution Run Plan "
      "finalization artifact type."
    )

  if (
    normalized.get("artifact_version")
    != ARTIFACT_VERSION
  ):
    raise ValueError(
      "Unsupported Execution Run Plan "
      "finalization artifact version."
    )

  actual_fingerprint = _required_text(
    normalized.get(
      "finalization_fingerprint"
    ),
    label=(
      "Execution Run Plan finalization "
      "fingerprint"
    ),
  )

  fingerprint_payload = {
    key: value
    for key, value in normalized.items()
    if key != "finalization_fingerprint"
  }
  expected_fingerprint = (
    _stable_json_hash(
      fingerprint_payload
    )
  )

  if actual_fingerprint != expected_fingerprint:
    raise ValueError(
      "Execution Run Plan finalization "
      "fingerprint mismatch."
    )

  if normalized.get("had_error") is not False:
    raise ValueError(
      "Successful Execution Run Plan finalization "
      "must declare had_error=false."
    )

  if plan is not None:
    if not isinstance(plan, ExecutionRunPlan):
      raise ValueError(
        "Invalid Execution Run Plan finalization "
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
      "architecture_fingerprint": (
        plan.architecture_fingerprint
      ),
      "impact_plan_fingerprint": (
        plan.impact_plan_fingerprint
      ),
      "execution_plan_fingerprint": (
        plan.execution_plan_fingerprint
      ),
      "dataset_count": plan.dataset_count,
    }

    mismatches = [
      field
      for field, expected
      in expected_values.items()
      if normalized.get(field) != expected
    ]

    if mismatches:
      raise ValueError(
        "Execution Run Plan finalization does "
        "not match the bound Run Plan fields: "
        + ", ".join(mismatches)
        + "."
      )

  return normalized


def write_execution_run_plan_finalization(
  *,
  run_plan_path: str | Path,
  plan: ExecutionRunPlan,
  outcomes: Sequence[Mapping[str, object]],
) -> Path:
  """
  Atomically persist an idempotent successful finalization artifact.
  """
  payload = (
    build_execution_run_plan_finalization(
      plan=plan,
      outcomes=outcomes,
    )
  )
  validate_execution_run_plan_finalization(
    payload,
    plan=plan,
  )

  path = execution_run_plan_finalization_path(
    run_plan_path
  )

  if path.exists():
    existing = (
      load_execution_run_plan_finalization(
        path,
        plan=plan,
      )
    )

    if (
      existing["finalization_fingerprint"]
      != payload["finalization_fingerprint"]
    ):
      raise ValueError(
        "Execution Run Plan finalization already "
        "exists with different evidence."
      )

    return path

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


def load_execution_run_plan_finalization(
  path: str | Path,
  *,
  plan: ExecutionRunPlan | None = None,
) -> dict[str, object]:
  """
  Load and validate one finalization artifact.
  """
  finalization_path = Path(path)

  try:
    payload = json.loads(
      finalization_path.read_text(
        encoding="utf-8"
      )
    )
  except OSError as exc:
    raise ValueError(
      "Execution Run Plan finalization could "
      f"not be read: {finalization_path}"
    ) from exc
  except json.JSONDecodeError as exc:
    raise ValueError(
      "Execution Run Plan finalization contains "
      f"invalid JSON: {finalization_path}"
    ) from exc

  return (
    validate_execution_run_plan_finalization(
      payload,
      plan=plan,
    )
  )
