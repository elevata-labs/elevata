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

from collections.abc import Mapping, Sequence
from pathlib import Path
import hashlib
import json
import os
import re
import uuid

from metadata.architecture.execution_run_plan import ExecutionRunPlan
from metadata.architecture.execution_run_plan_state import (
  build_execution_run_plan_architecture_drift,
)
from metadata.architecture.state import ArchitectureState


ARTIFACT_TYPE = "execution_run_plan_recovery"
ARTIFACT_VERSION = 1
RECOVERY_MODE = "interrupted_initial_deployment"
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_PAYLOAD_KEYS = frozenset({
  "artifact_type",
  "artifact_version",
  "recovery_mode",
  "run_plan_id",
  "run_plan_fingerprint",
  "batch_run_id",
  "profile_name",
  "target_system_short",
  "run_plan_architecture_fingerprint",
  "recovered_architecture_fingerprint",
  "physical_architecture_fingerprint",
  "metadata_changed_after_plan",
  "finalization_fingerprint",
  "recovered_at",
  "dataset_count",
  "decision_counts",
  "status_counts",
  "physical_validation",
  "warnings",
  "recovery_fingerprint",
})


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



def _fingerprint(
  value: object,
  *,
  label: str,
) -> str:
  """
  Return one validated SHA-256 fingerprint.
  """
  text = _required_text(
    value,
    label=label,
  )
  if not _SHA256_RE.fullmatch(text):
    raise ValueError(
      f"{label} must be a SHA-256 fingerprint."
    )
  return text.lower()


def _count_mapping(
  value: object,
  *,
  label: str,
) -> dict[str, int]:
  """
  Return one normalized non-negative count mapping.
  """
  if not isinstance(value, Mapping):
    raise ValueError(
      f"{label} must be a JSON object."
    )

  normalized: dict[str, int] = {}
  for raw_key, raw_count in value.items():
    key = _required_text(
      raw_key,
      label=f"{label} key",
    )
    if (
      isinstance(raw_count, bool)
      or not isinstance(raw_count, int)
      or raw_count < 0
    ):
      raise ValueError(
        f"{label} values must be non-negative integers."
      )
    normalized[key] = raw_count
  return dict(sorted(normalized.items()))


def _warnings(
  value: object,
) -> list[str]:
  """
  Return one canonical warning list.
  """
  if not isinstance(value, list):
    raise ValueError(
      "Execution Run Plan recovery warnings must be a JSON array."
    )
  normalized = sorted({
    _required_text(
      warning,
      label="Execution Run Plan recovery warning",
    )
    for warning in value
  })
  if normalized != value:
    raise ValueError(
      "Execution Run Plan recovery warnings are not canonical."
    )
  return normalized


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


def execution_run_plan_recovery_path(
  run_plan_path: str | Path,
) -> Path:
  """
  Return the deterministic interrupted-deployment recovery artifact path.
  """
  path = Path(run_plan_path).expanduser()
  return path.with_name(
    f"{path.stem}.recovered.json"
  )


def validate_interrupted_initial_deployment_scope(
  plan: ExecutionRunPlan,
) -> None:
  """
  Verify that one Run Plan is eligible for initial-deployment recovery.
  """
  if not isinstance(plan, ExecutionRunPlan):
    raise ValueError(
      "Invalid interrupted initial-deployment recovery Run Plan."
    )

  mismatches: list[str] = []
  if plan.scope_mode != "all":
    mismatches.append(
      f"scope_mode={plan.scope_mode!r}"
    )
  if plan.scope_key != "all":
    mismatches.append(
      f"scope_key={plan.scope_key!r}"
    )
  if plan.dependency_mode != "with_dependencies":
    mismatches.append(
      f"dependency_mode={plan.dependency_mode!r}"
    )
  if plan.review_status != "initial_deployment":
    mismatches.append(
      f"review_status={plan.review_status!r}"
    )
  if plan.decision_counts.get("REUSE", 0):
    mismatches.append(
      "REUSE decisions are present"
    )

  if mismatches:
    raise ValueError(
      "Interrupted initial-deployment recovery requires a full all-datasets "
      "initial-deployment Run Plan without REUSE decisions: "
      + "; ".join(mismatches)
      + "."
    )


def validate_recovery_architecture(
  *,
  plan: ExecutionRunPlan,
  current_state: ArchitectureState,
  physical_state: ArchitectureState,
) -> None:
  """
  Verify that current metadata scope and physical target state match exactly.
  """
  validate_interrupted_initial_deployment_scope(plan)

  if not isinstance(current_state, ArchitectureState):
    raise ValueError(
      "Interrupted initial-deployment recovery requires a current "
      "Architecture State."
    )
  if not isinstance(physical_state, ArchitectureState):
    raise ValueError(
      "Interrupted initial-deployment recovery requires a discovered "
      "physical Architecture State."
    )

  plan_keys = set(plan.dataset_keys)
  current_keys = set(current_state.datasets_by_key)
  if plan_keys != current_keys:
    missing = sorted(plan_keys - current_keys)
    added = sorted(current_keys - plan_keys)
    details: list[str] = []
    if missing:
      details.append(
        "missing from current metadata: " + ", ".join(missing)
      )
    if added:
      details.append(
        "added after plan creation: " + ", ".join(added)
      )
    raise ValueError(
      "Interrupted initial-deployment recovery cannot change the Run Plan "
      "dataset scope: "
      + "; ".join(details)
      + ". Start a new DAG run after establishing a safe baseline."
    )

  drift = build_execution_run_plan_architecture_drift(
    planned_state=physical_state,
    current_state=current_state,
  )
  if drift.has_changes:
    details = drift.dataset_changes + drift.column_changes
    detail_text = (
      "; ".join(details)
      if details
      else "physical architecture fingerprint changed"
    )
    raise ValueError(
      "Interrupted initial-deployment recovery rejected physical architecture "
      "drift. The target platform does not match current metadata: "
      f"{detail_text}."
    )

  if physical_state.fingerprint != current_state.fingerprint:
    raise ValueError(
      "Interrupted initial-deployment recovery rejected an unexplained "
      "physical Architecture State fingerprint mismatch."
    )


def build_execution_run_plan_recovery(
  *,
  plan: ExecutionRunPlan,
  finalization: Mapping[str, object],
  recovered_state: ArchitectureState,
  physical_state: ArchitectureState,
  warnings: Sequence[str] = (),
) -> dict[str, object]:
  """
  Build one deterministic interrupted initial-deployment recovery artifact.
  """
  validate_recovery_architecture(
    plan=plan,
    current_state=recovered_state,
    physical_state=physical_state,
  )

  if not isinstance(finalization, Mapping):
    raise ValueError(
      "Interrupted initial-deployment recovery requires validated scheduler "
      "finalization evidence."
    )

  finalization_fingerprint = _required_text(
    finalization.get("finalization_fingerprint"),
    label="Execution Run Plan finalization fingerprint",
  )
  recovered_at = _required_text(
    finalization.get("finalized_at"),
    label="Execution Run Plan finalization timestamp",
  )

  payload: dict[str, object] = {
    "artifact_type": ARTIFACT_TYPE,
    "artifact_version": ARTIFACT_VERSION,
    "recovery_mode": RECOVERY_MODE,
    "run_plan_id": plan.run_plan_id,
    "run_plan_fingerprint": plan.run_plan_fingerprint,
    "batch_run_id": plan.batch_run_id,
    "profile_name": plan.profile_name,
    "target_system_short": plan.target_system_short,
    "run_plan_architecture_fingerprint": (
      plan.architecture_fingerprint
    ),
    "recovered_architecture_fingerprint": (
      recovered_state.fingerprint
    ),
    "physical_architecture_fingerprint": (
      physical_state.fingerprint
    ),
    "metadata_changed_after_plan": (
      recovered_state.fingerprint
      != plan.architecture_fingerprint
    ),
    "finalization_fingerprint": finalization_fingerprint,
    "recovered_at": recovered_at,
    "dataset_count": plan.dataset_count,
    "decision_counts": dict(
      finalization.get("decision_counts") or {}
    ),
    "status_counts": dict(
      finalization.get("status_counts") or {}
    ),
    "physical_validation": "matched_current_metadata",
    "warnings": sorted({
      str(warning).strip()
      for warning in warnings
      if str(warning).strip()
    }),
  }
  payload["recovery_fingerprint"] = _stable_json_hash(
    payload
  )
  return payload


def validate_execution_run_plan_recovery(
  payload: Mapping[str, object],
  *,
  plan: ExecutionRunPlan | None = None,
) -> dict[str, object]:
  """
  Validate one persisted interrupted initial-deployment recovery artifact.
  """
  if not isinstance(payload, Mapping):
    raise ValueError(
      "Execution Run Plan recovery artifact must be a JSON object."
    )

  normalized = dict(payload)
  actual_keys = frozenset(normalized)
  if actual_keys != _PAYLOAD_KEYS:
    missing = sorted(_PAYLOAD_KEYS - actual_keys)
    unexpected = sorted(actual_keys - _PAYLOAD_KEYS)
    details: list[str] = []
    if missing:
      details.append("missing: " + ", ".join(missing))
    if unexpected:
      details.append("unexpected: " + ", ".join(unexpected))
    raise ValueError(
      "Execution Run Plan recovery artifact fields do not match the "
      "versioned contract: "
      + "; ".join(details)
      + "."
    )

  if normalized.get("artifact_type") != ARTIFACT_TYPE:
    raise ValueError(
      "Unsupported Execution Run Plan recovery artifact type."
    )
  if normalized.get("artifact_version") != ARTIFACT_VERSION:
    raise ValueError(
      "Unsupported Execution Run Plan recovery artifact version."
    )
  if normalized.get("recovery_mode") != RECOVERY_MODE:
    raise ValueError(
      "Unsupported Execution Run Plan recovery mode."
    )
  if normalized.get("physical_validation") != "matched_current_metadata":
    raise ValueError(
      "Execution Run Plan recovery physical validation is invalid."
    )

  actual_fingerprint = _fingerprint(
    normalized.get("recovery_fingerprint"),
    label="Execution Run Plan recovery fingerprint",
  )
  expected_fingerprint = _stable_json_hash({
    key: value
    for key, value in normalized.items()
    if key != "recovery_fingerprint"
  })
  if actual_fingerprint != expected_fingerprint:
    raise ValueError(
      "Execution Run Plan recovery fingerprint mismatch."
    )

  run_plan_fingerprint = _fingerprint(
    normalized.get("run_plan_fingerprint"),
    label="Execution Run Plan fingerprint",
  )
  run_plan_architecture_fingerprint = _fingerprint(
    normalized.get("run_plan_architecture_fingerprint"),
    label="Run Plan Architecture State fingerprint",
  )
  recovered_fingerprint = _fingerprint(
    normalized.get("recovered_architecture_fingerprint"),
    label="Recovered Architecture State fingerprint",
  )
  physical_fingerprint = _fingerprint(
    normalized.get("physical_architecture_fingerprint"),
    label="Physical Architecture State fingerprint",
  )
  _fingerprint(
    normalized.get("finalization_fingerprint"),
    label="Execution Run Plan finalization fingerprint",
  )
  _required_text(
    normalized.get("recovered_at"),
    label="Execution Run Plan recovery timestamp",
  )

  if recovered_fingerprint != physical_fingerprint:
    raise ValueError(
      "Execution Run Plan recovery physical and recovered Architecture State "
      "fingerprints do not match."
    )

  metadata_changed = normalized.get("metadata_changed_after_plan")
  if not isinstance(metadata_changed, bool):
    raise ValueError(
      "Execution Run Plan recovery metadata_changed_after_plan must be a "
      "boolean."
    )
  if metadata_changed != (
    recovered_fingerprint != run_plan_architecture_fingerprint
  ):
    raise ValueError(
      "Execution Run Plan recovery metadata-change marker does not match "
      "its Architecture State fingerprints."
    )

  dataset_count = normalized.get("dataset_count")
  if (
    isinstance(dataset_count, bool)
    or not isinstance(dataset_count, int)
    or dataset_count < 1
  ):
    raise ValueError(
      "Execution Run Plan recovery dataset count must be a positive integer."
    )

  decision_counts = _count_mapping(
    normalized.get("decision_counts"),
    label="Execution Run Plan recovery decision counts",
  )
  status_counts = _count_mapping(
    normalized.get("status_counts"),
    label="Execution Run Plan recovery status counts",
  )
  warnings = _warnings(
    normalized.get("warnings")
  )

  normalized.update({
    "run_plan_fingerprint": run_plan_fingerprint,
    "run_plan_architecture_fingerprint": (
      run_plan_architecture_fingerprint
    ),
    "recovered_architecture_fingerprint": recovered_fingerprint,
    "physical_architecture_fingerprint": physical_fingerprint,
    "decision_counts": decision_counts,
    "status_counts": status_counts,
    "warnings": warnings,
    "recovery_fingerprint": actual_fingerprint,
  })

  if plan is not None:
    if not isinstance(plan, ExecutionRunPlan):
      raise ValueError(
        "Invalid Execution Run Plan recovery validation contract."
      )
    expected_values = {
      "run_plan_id": plan.run_plan_id,
      "run_plan_fingerprint": plan.run_plan_fingerprint,
      "batch_run_id": plan.batch_run_id,
      "profile_name": plan.profile_name,
      "target_system_short": plan.target_system_short,
      "run_plan_architecture_fingerprint": (
        plan.architecture_fingerprint
      ),
      "dataset_count": plan.dataset_count,
      "decision_counts": {
        decision: count
        for decision, count in plan.decision_counts.items()
        if count
      },
    }
    mismatches = [
      key
      for key, expected in expected_values.items()
      if normalized.get(key) != expected
    ]
    if mismatches:
      raise ValueError(
        "Execution Run Plan recovery artifact does not match the bound Run "
        "Plan fields: "
        + ", ".join(mismatches)
        + "."
      )

  return normalized


def write_execution_run_plan_recovery(
  *,
  run_plan_path: str | Path,
  plan: ExecutionRunPlan,
  finalization: Mapping[str, object],
  recovered_state: ArchitectureState,
  physical_state: ArchitectureState,
  warnings: Sequence[str] = (),
) -> Path:
  """
  Atomically persist one idempotent recovery artifact.
  """
  payload = build_execution_run_plan_recovery(
    plan=plan,
    finalization=finalization,
    recovered_state=recovered_state,
    physical_state=physical_state,
    warnings=warnings,
  )
  validate_execution_run_plan_recovery(
    payload,
    plan=plan,
  )

  path = execution_run_plan_recovery_path(
    run_plan_path
  )
  if path.exists():
    existing = load_execution_run_plan_recovery(
      path,
      plan=plan,
    )
    if (
      existing["recovery_fingerprint"]
      != payload["recovery_fingerprint"]
    ):
      raise ValueError(
        "Execution Run Plan recovery already exists with different evidence."
      )
    return path

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


def load_execution_run_plan_recovery(
  path: str | Path,
  *,
  plan: ExecutionRunPlan | None = None,
) -> dict[str, object]:
  """
  Load and validate one recovery artifact.
  """
  recovery_path = Path(path).expanduser()
  try:
    payload = json.loads(
      recovery_path.read_text(encoding="utf-8")
    )
  except OSError as exc:
    raise ValueError(
      f"Execution Run Plan recovery artifact cannot be read: {recovery_path}"
    ) from exc
  except json.JSONDecodeError as exc:
    raise ValueError(
      f"Execution Run Plan recovery artifact contains invalid JSON: {recovery_path}"
    ) from exc

  return validate_execution_run_plan_recovery(
    payload,
    plan=plan,
  )
