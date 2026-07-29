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

from collections.abc import Iterable
from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
from typing import Any

from .execution_impact import ExecutionImpactEvidenceReference
from .state import ArchitectureState, DatasetState


@dataclass(frozen=True)
class ExecutionImpactDatasetEvidence:
  """
  Resolved read-only evidence and report signals for one current TargetDataset.

  The structure deliberately distinguishes controlled-execution audit evidence
  from a future output or materialization fingerprint. An Architecture
  Execution Record proves that a controlled command completed successfully; it
  does not by itself prove that a materialization can be reused safely.
  """
  dataset_key: str
  architecture_fingerprint: str
  baseline_fingerprint: str | None
  report_evidence_fingerprint: str
  review_status: str
  materialization_type: str
  incremental_strategy: str
  approval_id: str | None = None
  approval_fingerprint: str | None = None
  latest_successful_execution_id: str | None = None
  latest_successful_execution_started_at: str | None = None
  latest_successful_execution_record_fingerprint: str | None = None
  dataset_change_types: tuple[str, ...] = field(default_factory=tuple)
  dataset_changed_fields: tuple[str, ...] = field(default_factory=tuple)
  column_change_types: tuple[str, ...] = field(default_factory=tuple)
  migration_action_types: tuple[str, ...] = field(default_factory=tuple)
  policy_statuses: tuple[str, ...] = field(default_factory=tuple)
  policy_codes: tuple[str, ...] = field(default_factory=tuple)
  evidence: tuple[ExecutionImpactEvidenceReference, ...] = field(
    default_factory=tuple,
  )

  def __post_init__(self) -> None:
    if not str(self.dataset_key).strip():
      raise ValueError("Execution impact dataset evidence key must not be empty.")

    if not str(self.architecture_fingerprint).strip():
      raise ValueError(
        "Execution impact dataset evidence requires an architecture fingerprint."
      )

    if not str(self.report_evidence_fingerprint).strip():
      raise ValueError(
        "Execution impact dataset evidence requires a report evidence fingerprint."
      )

    if not str(self.review_status).strip():
      raise ValueError("Execution impact review status must not be empty.")

    materialization_type = str(self.materialization_type or "").strip().lower()
    if not materialization_type:
      raise ValueError(
        "Execution impact materialization type must not be empty."
      )
    object.__setattr__(
      self,
      "materialization_type",
      materialization_type,
    )

    incremental_strategy = str(self.incremental_strategy or "").strip().lower()
    if not incremental_strategy:
      raise ValueError(
        "Execution impact incremental strategy must not be empty."
      )
    object.__setattr__(
      self,
      "incremental_strategy",
      incremental_strategy,
    )

    if (
      self.latest_successful_execution_id
      and not self.latest_successful_execution_record_fingerprint
    ):
      raise ValueError(
        "A resolved execution identifier requires an execution record fingerprint."
      )

    if (
      self.latest_successful_execution_record_fingerprint
      and not self.latest_successful_execution_id
    ):
      raise ValueError(
        "An execution record fingerprint requires a resolved execution identifier."
      )

    if not self.evidence:
      raise ValueError(
        "Execution impact dataset evidence requires evidence references."
      )

    for field_name in (
      "dataset_change_types",
      "dataset_changed_fields",
      "column_change_types",
      "migration_action_types",
      "policy_statuses",
      "policy_codes",
    ):
      values = getattr(self, field_name)
      object.__setattr__(self, field_name, tuple(sorted(set(values))))

    object.__setattr__(
      self,
      "evidence",
      tuple(sorted(self.evidence, key=_evidence_sort_key)),
    )

  @property
  def has_architecture_changes(self) -> bool:
    """
    Return True when the report contains dataset, column or migration signals.
    """
    return bool(
      self.dataset_change_types
      or self.column_change_types
      or self.migration_action_types
    )

  @property
  def has_blocking_policy_decision(self) -> bool:
    """
    Return True when at least one dataset policy decision blocks execution.
    """
    return "BLOCKED_BY_POLICY" in self.policy_statuses

  @property
  def evidence_fingerprint(self) -> str:
    """
    Return a deterministic fingerprint of the resolved dataset evidence.
    """
    return _stable_json_hash(self.to_dict(include_fingerprint=False))

  def to_dict(self, *, include_fingerprint: bool = True) -> dict[str, Any]:
    """
    Return the canonical dataset evidence payload.
    """
    payload = {
      "dataset_key": self.dataset_key,
      "architecture_fingerprint": self.architecture_fingerprint,
      "baseline_fingerprint": self.baseline_fingerprint,
      "report_evidence_fingerprint": self.report_evidence_fingerprint,
      "review_status": self.review_status,
      "materialization_type": self.materialization_type,
      "incremental_strategy": self.incremental_strategy,
      "approval_id": self.approval_id,
      "approval_fingerprint": self.approval_fingerprint,
      "latest_successful_execution_id": (
        self.latest_successful_execution_id
      ),
      "latest_successful_execution_started_at": (
        self.latest_successful_execution_started_at
      ),
      "latest_successful_execution_record_fingerprint": (
        self.latest_successful_execution_record_fingerprint
      ),
      "dataset_change_types": list(self.dataset_change_types),
      "dataset_changed_fields": list(self.dataset_changed_fields),
      "column_change_types": list(self.column_change_types),
      "migration_action_types": list(self.migration_action_types),
      "policy_statuses": list(self.policy_statuses),
      "policy_codes": list(self.policy_codes),
      "has_architecture_changes": self.has_architecture_changes,
      "has_blocking_policy_decision": self.has_blocking_policy_decision,
      "evidence": [item.to_dict() for item in self.evidence],
    }

    if include_fingerprint:
      payload["evidence_fingerprint"] = self.evidence_fingerprint

    return payload


@dataclass(frozen=True)
class ExecutionImpactEvidenceResolution:
  """
  Deterministic read-only evidence resolution for one Architecture Control scope.
  """
  scope_key: str
  architecture_fingerprint: str
  baseline_fingerprint: str | None
  baseline_source: str
  baseline_can_execute: bool
  report_fingerprint: str
  report_has_changes: bool
  report_is_blocked: bool
  review_status: str
  datasets: tuple[ExecutionImpactDatasetEvidence, ...] = field(
    default_factory=tuple,
  )

  def __post_init__(self) -> None:
    if not str(self.scope_key).strip():
      raise ValueError("Execution impact evidence scope key must not be empty.")

    if not str(self.architecture_fingerprint).strip():
      raise ValueError(
        "Execution impact evidence requires an architecture fingerprint."
      )

    if not str(self.baseline_source).strip():
      raise ValueError("Execution impact baseline source must not be empty.")

    if not str(self.report_fingerprint).strip():
      raise ValueError("Execution impact report fingerprint must not be empty.")

    if not str(self.review_status).strip():
      raise ValueError("Execution impact review status must not be empty.")

    ordered_datasets = tuple(
      sorted(self.datasets, key=lambda item: item.dataset_key)
    )
    dataset_keys = [item.dataset_key for item in ordered_datasets]
    if len(dataset_keys) != len(set(dataset_keys)):
      raise ValueError(
        "Execution impact evidence contains duplicate dataset keys."
      )

    object.__setattr__(self, "datasets", ordered_datasets)

  @property
  def dataset_count(self) -> int:
    """
    Return the number of current TargetDatasets covered by the resolution.
    """
    return len(self.datasets)

  @property
  def resolution_fingerprint(self) -> str:
    """
    Return the deterministic fingerprint of the complete evidence resolution.
    """
    return _stable_json_hash(self.to_dict(include_fingerprint=False))

  def to_dict(self, *, include_fingerprint: bool = True) -> dict[str, Any]:
    """
    Return the canonical scope-level evidence payload.
    """
    payload = {
      "scope_key": self.scope_key,
      "architecture_fingerprint": self.architecture_fingerprint,
      "baseline_fingerprint": self.baseline_fingerprint,
      "baseline_source": self.baseline_source,
      "baseline_can_execute": self.baseline_can_execute,
      "report_fingerprint": self.report_fingerprint,
      "report_has_changes": self.report_has_changes,
      "report_is_blocked": self.report_is_blocked,
      "review_status": self.review_status,
      "dataset_count": self.dataset_count,
      "datasets": [item.to_dict() for item in self.datasets],
    }

    if include_fingerprint:
      payload["resolution_fingerprint"] = self.resolution_fingerprint

    return payload


def resolve_execution_impact_evidence(
  *,
  scope_key: str,
  current_state: ArchitectureState,
  baseline_resolution: Any,
  report: Any,
  review_status: Any,
  execution_record_store: Any | None = None,
  dataset_keys: Iterable[str] | None = None,
) -> ExecutionImpactEvidenceResolution:
  """
  Resolve existing architecture and execution artifacts into evidence inputs.

  The resolver is read-only. It does not decide whether a dataset should be
  reused, revalidated, executed or rebuilt, and it does not persist artifacts.
  """
  report_payload = _require_report_payload(report)
  report_fingerprint = str(
    getattr(report, "report_fingerprint", None)
    or report_payload.get("report_fingerprint")
    or ""
  ).strip()
  if not report_fingerprint:
    raise ValueError("Architecture Change Report fingerprint is missing.")

  payload_fingerprint = str(
    report_payload.get("report_fingerprint") or report_fingerprint
  ).strip()
  if payload_fingerprint != report_fingerprint:
    raise ValueError(
      "Architecture Change Report fingerprint does not match its payload."
    )

  current_by_key = current_state.datasets_by_key
  resolved_dataset_keys = _resolve_current_scope_dataset_keys(
    report_payload=report_payload,
    current_state=current_state,
    requested_dataset_keys=dataset_keys,
  )

  previous_state = getattr(baseline_resolution, "previous_state", None)
  previous_by_key = (
    previous_state.datasets_by_key
    if isinstance(previous_state, ArchitectureState)
    else {}
  )
  baseline_fingerprint = (
    previous_state.fingerprint
    if isinstance(previous_state, ArchitectureState)
    else None
  )
  baseline_source = str(
    getattr(baseline_resolution, "source", None)
    or "missing_or_unsupported"
  )
  baseline_can_execute = bool(
    getattr(baseline_resolution, "can_execute", False)
  )

  report_has_changes = bool(
    getattr(report, "has_changes", None)
    if hasattr(report, "has_changes")
    else (report_payload.get("state") or {}).get("has_changes", False)
  )
  report_is_blocked = bool(
    getattr(report, "is_blocked", None)
    if hasattr(report, "is_blocked")
    else report_payload.get("is_blocked", False)
  )
  review_status_key = str(getattr(review_status, "status", "") or "").strip()
  if not review_status_key:
    raise ValueError("Architecture review status is missing.")

  execution_summaries, execution_store_error = (
    _latest_successful_exact_scope_executions(
      dataset_keys=resolved_dataset_keys,
      execution_record_store=execution_record_store,
    )
  )

  dataset_evidence = tuple(
    _build_dataset_evidence(
      dataset_key=dataset_key,
      current_dataset=current_by_key[dataset_key],
      previous_dataset=previous_by_key.get(dataset_key),
      baseline_resolution=baseline_resolution,
      report_payload=report_payload,
      report_fingerprint=report_fingerprint,
      review_status=review_status,
      execution_summary=execution_summaries.get(dataset_key),
      execution_store_available=execution_record_store is not None,
      execution_store_error=execution_store_error,
    )
    for dataset_key in resolved_dataset_keys
  )

  return ExecutionImpactEvidenceResolution(
    scope_key=scope_key,
    architecture_fingerprint=current_state.fingerprint,
    baseline_fingerprint=baseline_fingerprint,
    baseline_source=baseline_source,
    baseline_can_execute=baseline_can_execute,
    report_fingerprint=report_fingerprint,
    report_has_changes=report_has_changes,
    report_is_blocked=report_is_blocked,
    review_status=review_status_key,
    datasets=dataset_evidence,
  )


def _build_dataset_evidence(
  *,
  dataset_key: str,
  current_dataset: DatasetState,
  previous_dataset: DatasetState | None,
  baseline_resolution: Any,
  report_payload: dict[str, Any],
  report_fingerprint: str,
  review_status: Any,
  execution_summary: Any | None,
  execution_store_available: bool,
  execution_store_error: str | None,
) -> ExecutionImpactDatasetEvidence:
  """
  Build normalized evidence for one current TargetDataset.
  """
  dataset_changes = _dataset_payloads(
    report_payload.get("dataset_changes"),
    dataset_key,
  )
  column_changes = _dataset_payloads(
    report_payload.get("column_changes"),
    dataset_key,
  )
  migration_actions = _dataset_payloads(
    report_payload.get("migration_actions"),
    dataset_key,
  )
  policy_decisions = _dataset_payloads(
    report_payload.get("policy_decisions"),
    dataset_key,
  )

  dataset_change_types = tuple(
    str(item.get("change_type") or "")
    for item in dataset_changes
    if item.get("change_type")
  )
  dataset_changed_fields = tuple(
    str(field_name)
    for item in dataset_changes
    for field_name in ((item.get("details") or {}).get("changed_fields") or [])
    if str(field_name).strip()
  )
  column_change_types = tuple(
    str(item.get("change_type") or "")
    for item in column_changes
    if item.get("change_type")
  )
  migration_action_types = tuple(
    str(item.get("action_type") or "")
    for item in migration_actions
    if item.get("action_type")
  )
  policy_statuses = tuple(
    str(item.get("status") or "")
    for item in policy_decisions
    if item.get("status")
  )
  policy_codes = tuple(
    str(item.get("code") or "")
    for item in policy_decisions
    if item.get("code")
  )

  report_evidence_payload = {
    "dataset_key": dataset_key,
    "report_fingerprint": report_fingerprint,
    "dataset_changes": dataset_changes,
    "column_changes": column_changes,
    "migration_actions": migration_actions,
    "policy_decisions": policy_decisions,
  }
  report_evidence_fingerprint = _stable_json_hash(report_evidence_payload)
  has_dataset_changes = bool(
    dataset_changes or column_changes or migration_actions
  )
  review_status_key = str(
    getattr(review_status, "status", "") or ""
  )
  approval_required = (
    has_dataset_changes
    and review_status_key != "initial_deployment"
  )

  evidence = (
    ExecutionImpactEvidenceReference(
      evidence_type="architecture_state",
      evidence_key=f"current:{dataset_key}",
      status="available",
      required=True,
      fingerprint=current_dataset.fingerprint,
      message="Current metadata-defined TargetDataset state.",
    ),
    _baseline_dataset_evidence(
      dataset_key=dataset_key,
      previous_dataset=previous_dataset,
      baseline_resolution=baseline_resolution,
    ),
    ExecutionImpactEvidenceReference(
      evidence_type="architecture_change_report",
      evidence_key=f"dataset:{dataset_key}",
      status="available",
      required=True,
      fingerprint=report_evidence_fingerprint,
      artifact_reference=report_fingerprint,
      message="Dataset-specific Architecture Change Report evidence.",
    ),
    _review_status_evidence(review_status, dataset_key=dataset_key),
    _approval_evidence(
      review_status=review_status,
      evidence_key=f"dataset:{dataset_key}",
      required=approval_required,
    ),
    ExecutionImpactEvidenceReference(
      evidence_type="policy_decision",
      evidence_key=f"dataset:{dataset_key}",
      status="available",
      required=True,
      fingerprint=_stable_json_hash(policy_decisions),
      message=(
        f"{len(policy_decisions)} policy decision(s) resolved for this dataset."
      ),
    ),
    _execution_record_evidence(
      dataset_key=dataset_key,
      execution_summary=execution_summary,
      store_available=execution_store_available,
      store_error=execution_store_error,
    ),
  )

  return ExecutionImpactDatasetEvidence(
    dataset_key=dataset_key,
    architecture_fingerprint=current_dataset.fingerprint,
    baseline_fingerprint=(
      previous_dataset.fingerprint if previous_dataset is not None else None
    ),
    report_evidence_fingerprint=report_evidence_fingerprint,
    review_status=review_status_key,
    materialization_type=str(
      current_dataset.materialization_type or "unknown"
    ),
    incremental_strategy=str(
      current_dataset.incremental_strategy or "unknown"
    ),
    approval_id=getattr(review_status, "approval_id", None),
    approval_fingerprint=getattr(
      review_status,
      "artifact_fingerprint",
      None,
    ),
    latest_successful_execution_id=(
      str(getattr(execution_summary, "execution_id", "") or "") or None
    ),
    latest_successful_execution_started_at=(
      str(getattr(execution_summary, "started_at", "") or "") or None
    ),
    latest_successful_execution_record_fingerprint=(
      str(getattr(execution_summary, "record_fingerprint", "") or "") or None
    ),
    dataset_change_types=dataset_change_types,
    dataset_changed_fields=dataset_changed_fields,
    column_change_types=column_change_types,
    migration_action_types=migration_action_types,
    policy_statuses=policy_statuses,
    policy_codes=policy_codes,
    evidence=evidence,
  )


def _baseline_dataset_evidence(
  *,
  dataset_key: str,
  previous_dataset: DatasetState | None,
  baseline_resolution: Any,
) -> ExecutionImpactEvidenceReference:
  """
  Build baseline Architecture State evidence for one current dataset.
  """
  previous_state = getattr(baseline_resolution, "previous_state", None)
  if not isinstance(previous_state, ArchitectureState):
    return ExecutionImpactEvidenceReference(
      evidence_type="architecture_state",
      evidence_key=f"baseline:{dataset_key}",
      status="unavailable",
      required=False,
      message=str(
        getattr(baseline_resolution, "message", None)
        or "Architecture comparison baseline is unavailable."
      ),
    )

  if previous_dataset is None:
    return ExecutionImpactEvidenceReference(
      evidence_type="architecture_state",
      evidence_key=f"baseline:{dataset_key}",
      status="not_applicable",
      required=False,
      message="The current dataset is not present in the resolved baseline.",
    )

  return ExecutionImpactEvidenceReference(
    evidence_type="architecture_state",
    evidence_key=f"baseline:{dataset_key}",
    status="available",
    required=False,
    fingerprint=previous_dataset.fingerprint,
    artifact_reference=_stable_artifact_reference(
      getattr(baseline_resolution, "state_file", None)
    ),
    message="Dataset state from the resolved architecture baseline.",
  )


def _review_status_evidence(
  review_status: Any,
  *,
  dataset_key: str | None,
) -> ExecutionImpactEvidenceReference:
  """
  Build a stable review-status evidence reference.
  """
  payload = {
    "status": getattr(review_status, "status", None),
    "report_fingerprint": getattr(review_status, "report_fingerprint", None),
    "approval_id": getattr(review_status, "approval_id", None),
    "artifact_fingerprint": getattr(
      review_status,
      "artifact_fingerprint",
      None,
    ),
    "review_decision": getattr(review_status, "review_decision", None),
    "has_changes": bool(getattr(review_status, "has_changes", False)),
    "is_blocked": bool(getattr(review_status, "is_blocked", False)),
  }
  key = f"dataset:{dataset_key}" if dataset_key else "scope:review"
  return ExecutionImpactEvidenceReference(
    evidence_type="architecture_review_status",
    evidence_key=key,
    status="available",
    required=True,
    fingerprint=_stable_json_hash(payload),
    artifact_reference=str(
      getattr(review_status, "report_fingerprint", None) or ""
    ) or None,
    message=str(getattr(review_status, "message", None) or "Review status resolved."),
  )


def _approval_evidence(
  *,
  review_status: Any,
  evidence_key: str,
  required: bool,
) -> ExecutionImpactEvidenceReference:
  """
  Build report-bound approval evidence without overriding policy status.
  """
  approval_id = str(getattr(review_status, "approval_id", None) or "") or None
  artifact_fingerprint = str(
    getattr(review_status, "artifact_fingerprint", None) or ""
  ) or None
  review_decision = str(
    getattr(review_status, "review_decision", None) or ""
  )

  if not required:
    if str(getattr(review_status, "status", "") or "") == "initial_deployment":
      message = (
        "Approval is not required because the complete managed target scope "
        "was physically verified as an initial deployment."
      )
    else:
      message = "No dataset-specific architecture change requires approval."

    return ExecutionImpactEvidenceReference(
      evidence_type="architecture_approval",
      evidence_key=evidence_key,
      status="not_applicable",
      required=False,
      artifact_reference=approval_id,
      message=message,
    )

  if artifact_fingerprint and review_decision == "approved":
    return ExecutionImpactEvidenceReference(
      evidence_type="architecture_approval",
      evidence_key=evidence_key,
      status="available",
      required=True,
      fingerprint=artifact_fingerprint,
      artifact_reference=approval_id,
      message=(
        "A report-bound approved Architecture Approval Artifact is available."
      ),
    )

  return ExecutionImpactEvidenceReference(
    evidence_type="architecture_approval",
    evidence_key=evidence_key,
    status="unavailable",
    required=True,
    fingerprint=artifact_fingerprint,
    artifact_reference=approval_id,
    message=str(
      getattr(review_status, "message", None)
      or "A matching Architecture Approval Artifact is unavailable."
    ),
  )


def _latest_successful_exact_scope_executions(
  *,
  dataset_keys: tuple[str, ...],
  execution_record_store: Any | None,
) -> tuple[dict[str, Any], str | None]:
  """
  Return latest successful exact-dataset-scope execution summaries and errors.

  The store is read once for the complete scope. All- and schema-scope records
  are intentionally excluded because the current Architecture Execution Record
  contract does not enumerate dataset outcomes.
  """
  if execution_record_store is None or not dataset_keys:
    return {}, None

  try:
    summaries = execution_record_store.list_records(limit=None)
  except (OSError, TypeError, ValueError) as exc:
    return {}, str(exc) or exc.__class__.__name__

  dataset_key_set = set(dataset_keys)
  matches = [
    summary
    for summary in summaries
    if str(getattr(summary, "status", "") or "") == "success"
    and str(getattr(summary, "scope_key", "") or "") in dataset_key_set
    and str(getattr(summary, "record_fingerprint", "") or "")
  ]
  matches.sort(
    key=lambda summary: (
      str(getattr(summary, "started_at", "") or ""),
      str(getattr(summary, "finished_at", "") or ""),
      str(getattr(summary, "execution_id", "") or ""),
    ),
    reverse=True,
  )

  latest_by_dataset: dict[str, Any] = {}
  for summary in matches:
    dataset_key = str(getattr(summary, "scope_key", "") or "")
    latest_by_dataset.setdefault(dataset_key, summary)

  return latest_by_dataset, None


def _execution_record_evidence(
  *,
  dataset_key: str,
  execution_summary: Any | None,
  store_available: bool,
  store_error: str | None,
) -> ExecutionImpactEvidenceReference:
  """
  Build exact-scope controlled-execution audit evidence.
  """
  if execution_summary is None:
    if store_error:
      message = (
        "Architecture Execution Record evidence could not be resolved: "
        f"{store_error}"
      )
    elif store_available:
      message = (
        "No successful exact-dataset-scope Architecture Execution Record is "
        "available."
      )
    else:
      message = "Architecture Execution Record evidence was not supplied."
    return ExecutionImpactEvidenceReference(
      evidence_type="architecture_execution_record",
      evidence_key=f"dataset:{dataset_key}",
      status="unavailable",
      required=False,
      message=message,
    )

  execution_id = str(getattr(execution_summary, "execution_id", "") or "")
  record_fingerprint = str(
    getattr(execution_summary, "record_fingerprint", "") or ""
  )
  return ExecutionImpactEvidenceReference(
    evidence_type="architecture_execution_record",
    evidence_key=f"dataset:{dataset_key}",
    status="available",
    required=False,
    fingerprint=record_fingerprint,
    artifact_reference=(
      _stable_artifact_reference(getattr(execution_summary, "path", None))
      or execution_id
      or None
    ),
    message=(
      "Latest successful controlled execution for the exact dataset scope. "
      "This is audit evidence, not an output or materialization fingerprint."
    ),
  )


def _require_report_payload(report: Any) -> dict[str, Any]:
  """
  Return and validate a JSON-compatible Architecture Change Report payload.
  """
  try:
    payload = report.to_dict()
  except AttributeError as exc:
    raise ValueError(
      "Architecture Change Report must provide a to_dict() contract."
    ) from exc

  if not isinstance(payload, dict):
    raise ValueError("Architecture Change Report payload must be a dictionary.")

  return payload


def _resolve_current_scope_dataset_keys(
  *,
  report_payload: dict[str, Any],
  current_state: ArchitectureState,
  requested_dataset_keys: Iterable[str] | None = None,
) -> tuple[str, ...]:
  """
  Resolve current TargetDataset keys covered by the execution scope.

  When explicit keys are supplied, they are authoritative for the Execution
  Impact Plan. The Architecture Change Report remains the source of
  dataset-specific change, policy and approval evidence.
  """
  if requested_dataset_keys is not None:
    if isinstance(requested_dataset_keys, (str, bytes)):
      raise ValueError(
        "Execution Impact Plan dataset keys must be an iterable of keys."
      )

    dataset_keys = tuple(sorted({
      str(item).strip()
      for item in requested_dataset_keys
      if str(item).strip()
    }))
    if not dataset_keys:
      raise ValueError(
        "Execution Impact Plan execution scope must contain at least one dataset."
      )
  else:
    scope = report_payload.get("scope") or {}
    raw_dataset_keys = scope.get("dataset_keys") or []
    if not isinstance(raw_dataset_keys, (list, tuple)):
      raise ValueError(
        "Architecture Change Report scope dataset_keys must be a list or tuple."
      )

    dataset_keys = tuple(
      sorted({str(item) for item in raw_dataset_keys if str(item).strip()})
    )
    if not dataset_keys:
      dataset_keys = tuple(sorted(current_state.datasets_by_key))

  missing_keys = [
    dataset_key
    for dataset_key in dataset_keys
    if dataset_key not in current_state.datasets_by_key
  ]
  if missing_keys:
    raise ValueError(
      "Execution Impact Plan scope references datasets missing from the "
      "current Architecture State: "
      + ", ".join(missing_keys)
    )

  return dataset_keys


def _dataset_payloads(value: Any, dataset_key: str) -> tuple[dict[str, Any], ...]:
  """
  Return normalized report payload items affecting one dataset key.
  """
  return tuple(
    item
    for item in _normalized_mapping_tuple(value)
    if str(item.get("dataset_key") or "") == dataset_key
    or str(item.get("previous_dataset_key") or "") == dataset_key
  )


def _normalized_mapping_tuple(value: Any) -> tuple[dict[str, Any], ...]:
  """
  Normalize a report payload list to deterministic canonical dictionaries.
  """
  if value is None:
    return ()

  if not isinstance(value, (list, tuple)):
    raise ValueError("Architecture Change Report detail lists must be sequences.")

  normalized: list[dict[str, Any]] = []
  for item in value:
    if not isinstance(item, dict):
      raise ValueError(
        "Architecture Change Report detail entries must be dictionaries."
      )
    normalized.append(_canonicalize(item))

  return tuple(sorted(normalized, key=_stable_json_text))


def _evidence_sort_key(
  item: ExecutionImpactEvidenceReference,
) -> tuple[str, str, str, str]:
  """
  Return a deterministic sort key for evidence references.
  """
  return (
    item.evidence_type,
    item.evidence_key,
    item.status,
    item.fingerprint or "",
  )


def _stable_artifact_reference(value: Any) -> str | None:
  """
  Return a stable file-name reference instead of an environment-specific path.
  """
  if value is None:
    return None

  raw_value = str(value).strip()
  if not raw_value:
    return None

  return Path(raw_value).name


def _canonicalize(value: Any) -> Any:
  """
  Return a deterministic JSON-compatible representation.
  """
  if isinstance(value, dict):
    return {
      str(key): _canonicalize(item)
      for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
    }
  if isinstance(value, (list, tuple)):
    return [_canonicalize(item) for item in value]
  return value


def _stable_json_text(value: Any) -> str:
  """
  Return canonical compact JSON for deterministic ordering.
  """
  return json.dumps(
    value,
    sort_keys=True,
    ensure_ascii=False,
    separators=(",", ":"),
    default=str,
  )


def _stable_json_hash(value: Any) -> str:
  """
  Return a deterministic SHA-256 hash for a JSON-serializable value.
  """
  return hashlib.sha256(_stable_json_text(value).encode("utf-8")).hexdigest()
