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

from dataclasses import replace

from .execution_impact import (
  ExecutionImpactDecision,
  ExecutionImpactEvidenceReference,
  ExecutionImpactItem,
  ExecutionImpactPlan,
  ExecutionImpactReasonCode,
)
from .execution_impact_evidence import (
  ExecutionImpactDatasetEvidence,
  ExecutionImpactEvidenceResolution,
)


_METADATA_ONLY_ACTION_TYPES = frozenset({
  "RETIRE_COLUMN",
  "UNRETIRE_COLUMN",
})

_REBUILD_ACTION_TYPES = frozenset({
  "RENAME_DATASET",
  "RENAME_COLUMN",
  "CREATE_DATASET",
  "DROP_DATASET",
  "ADD_COLUMN",
  "DROP_COLUMN",
  "ALTER_COLUMN",
  "REBUILD_DATASET",
})

_COLUMN_ACTION_TYPES = frozenset({
  "RENAME_COLUMN",
  "ADD_COLUMN",
  "DROP_COLUMN",
  "ALTER_COLUMN",
  "RETIRE_COLUMN",
  "UNRETIRE_COLUMN",
})

_PHYSICAL_MATERIALIZATION_TYPES = frozenset({
  "table",
  "incremental",
})

_VIRTUAL_MATERIALIZATION_TYPES = frozenset({
  "view",
})

_INCREMENTAL_LOAD_STRATEGIES = frozenset({
  "append",
  "merge",
  "snapshot",
  "historize",
})

_APPROVAL_REVIEW_STATUSES = frozenset({
  "pending",
  "drift",
})

_ALLOWED_POLICY_STATUSES = frozenset({
  "ALLOW",
  "BLOCKED_BY_POLICY",
  "METADATA_ONLY",
  "REQUIRES_PREFLIGHT",
})

_ALLOWED_CHANGED_FIELDS = frozenset({
  "active",
  "dataset_key",
  "dataset_name",
  "former_names",
  "historize",
  "incremental_strategy",
  "is_hist",
  "materialization_type",
  "schema_short_name",
})


def build_local_execution_impact_plan(
  *,
  evidence_resolution: ExecutionImpactEvidenceResolution,
) -> ExecutionImpactPlan:
  """
  Build deterministic dataset-local impact decisions from resolved evidence.

  Local decisions consider only evidence attached directly to the dataset. They
  deliberately do not inspect execution dependencies, source changes or
  upstream outcomes. A later propagation stage may therefore strengthen a
  local REUSE or REVALIDATE decision.
  """
  items = tuple(
    build_local_execution_impact_item(
      dataset_evidence=dataset_evidence,
      baseline_source=evidence_resolution.baseline_source,
      baseline_can_execute=evidence_resolution.baseline_can_execute,
    )
    for dataset_evidence in evidence_resolution.datasets
  )

  return ExecutionImpactPlan(
    scope_key=evidence_resolution.scope_key,
    architecture_fingerprint=(
      evidence_resolution.architecture_fingerprint
    ),
    baseline_fingerprint=evidence_resolution.baseline_fingerprint,
    report_fingerprint=evidence_resolution.report_fingerprint,
    items=items,
    evidence=_scope_evidence(evidence_resolution),
  )


def build_local_execution_impact_item(
  *,
  dataset_evidence: ExecutionImpactDatasetEvidence,
  baseline_source: str,
  baseline_can_execute: bool,
) -> ExecutionImpactItem:
  """
  Build one deterministic dataset-local execution impact decision.
  """
  evidence = _decision_evidence(
    dataset_evidence=dataset_evidence,
  )
  decision, reason_codes = _resolve_local_decision(
    dataset_evidence=dataset_evidence,
    evidence=evidence,
    baseline_source=baseline_source,
    baseline_can_execute=baseline_can_execute,
  )

  return ExecutionImpactItem(
    dataset_key=dataset_evidence.dataset_key,
    decision=decision,
    reason_codes=reason_codes,
    architecture_fingerprint=dataset_evidence.architecture_fingerprint,
    last_successful_execution_fingerprint=(
      dataset_evidence.latest_successful_execution_record_fingerprint
    ),
    evidence=evidence,
  )


def _resolve_local_decision(
  *,
  dataset_evidence: ExecutionImpactDatasetEvidence,
  evidence: tuple[ExecutionImpactEvidenceReference, ...],
  baseline_source: str,
  baseline_can_execute: bool,
) -> tuple[ExecutionImpactDecision, tuple[ExecutionImpactReasonCode, ...]]:
  """
  Resolve the strongest safe local decision and its reason codes.
  """
  blocking_reasons = set(
    _blocking_reason_codes(
      dataset_evidence=dataset_evidence,
      evidence=evidence,
      baseline_can_execute=baseline_can_execute,
    )
  )
  if blocking_reasons:
    return "BLOCKED", tuple(sorted(blocking_reasons))

  if not dataset_evidence.has_architecture_changes:
    return _resolve_unchanged_dataset_decision(
      dataset_evidence=dataset_evidence,
      baseline_source=baseline_source,
    )

  return _resolve_changed_dataset_decision(dataset_evidence)


def _blocking_reason_codes(
  *,
  dataset_evidence: ExecutionImpactDatasetEvidence,
  evidence: tuple[ExecutionImpactEvidenceReference, ...],
  baseline_can_execute: bool,
) -> tuple[ExecutionImpactReasonCode, ...]:
  """
  Return local blockers that take precedence over execution recommendations.
  """
  reasons: set[ExecutionImpactReasonCode] = set()

  if not baseline_can_execute:
    reasons.add("BASELINE_UNAVAILABLE")

  if dataset_evidence.has_blocking_policy_decision:
    reasons.add("POLICY_BLOCKED")

  if dataset_evidence.has_architecture_changes:
    if dataset_evidence.review_status in _APPROVAL_REVIEW_STATUSES:
      reasons.add("APPROVAL_REQUIRED")
    elif dataset_evidence.review_status == "blocked":
      reasons.add("POLICY_BLOCKED")
    elif dataset_evidence.review_status == "invalid":
      reasons.add("MANUAL_REVIEW_REQUIRED")
    elif dataset_evidence.review_status not in {
      "approved",
      "initial_deployment",
    }:
      reasons.add("MANUAL_REVIEW_REQUIRED")

  for item in evidence:
    if item.is_unavailable_required:
      reasons.add(_reason_for_unavailable_evidence(
        item,
        review_status=dataset_evidence.review_status,
      ))

  return tuple(sorted(reasons))


def _resolve_unchanged_dataset_decision(
  *,
  dataset_evidence: ExecutionImpactDatasetEvidence,
  baseline_source: str,
) -> tuple[ExecutionImpactDecision, tuple[ExecutionImpactReasonCode, ...]]:
  """
  Resolve a local decision for a dataset without report-level changes.
  """
  if dataset_evidence.baseline_fingerprint is None:
    return "BLOCKED", ("BASELINE_UNAVAILABLE",)

  if (
    dataset_evidence.baseline_fingerprint
    != dataset_evidence.architecture_fingerprint
  ):
    return "BLOCKED", ("MANUAL_REVIEW_REQUIRED",)

  if baseline_source not in {
    "recorded_state",
    "discovered_physical_state",
  }:
    return "BLOCKED", ("BASELINE_UNAVAILABLE",)

  materialization_type = dataset_evidence.materialization_type
  incremental_strategy = dataset_evidence.incremental_strategy

  if materialization_type in _VIRTUAL_MATERIALIZATION_TYPES:
    return "REUSE", tuple(sorted({
      "NO_RELEVANT_CHANGE",
      "VIRTUAL_MATERIALIZATION_REUSED",
    }))

  if materialization_type not in _PHYSICAL_MATERIALIZATION_TYPES:
    return "BLOCKED", ("MANUAL_REVIEW_REQUIRED",)

  if incremental_strategy == "full":
    return "FULL_REBUILD", ("FULL_REFRESH_STRATEGY",)

  if incremental_strategy in _INCREMENTAL_LOAD_STRATEGIES:
    return "INCREMENTAL_EXECUTE", ("INCREMENTAL_LOAD_STRATEGY",)

  return "BLOCKED", ("MANUAL_REVIEW_REQUIRED",)


def _resolve_changed_dataset_decision(
  dataset_evidence: ExecutionImpactDatasetEvidence,
) -> tuple[ExecutionImpactDecision, tuple[ExecutionImpactReasonCode, ...]]:
  """
  Resolve a local decision for a dataset with architecture changes.
  """
  action_types = set(dataset_evidence.migration_action_types)
  policy_statuses = set(dataset_evidence.policy_statuses)

  if not action_types or not policy_statuses:
    return "BLOCKED", ("MANUAL_REVIEW_REQUIRED",)

  unknown_actions = action_types - (
    _METADATA_ONLY_ACTION_TYPES | _REBUILD_ACTION_TYPES
  )
  if unknown_actions:
    return "BLOCKED", ("MANUAL_REVIEW_REQUIRED",)

  unknown_policy_statuses = policy_statuses - _ALLOWED_POLICY_STATUSES
  if unknown_policy_statuses:
    return "BLOCKED", ("MANUAL_REVIEW_REQUIRED",)

  unknown_changed_fields = set(dataset_evidence.dataset_changed_fields) - (
    _ALLOWED_CHANGED_FIELDS
  )
  if unknown_changed_fields:
    return "BLOCKED", ("MANUAL_REVIEW_REQUIRED",)

  if action_types <= _METADATA_ONLY_ACTION_TYPES:
    if policy_statuses != {"METADATA_ONLY"}:
      return "BLOCKED", ("MANUAL_REVIEW_REQUIRED",)

    return "REVALIDATE", tuple(sorted({
      "COLUMN_CONTRACT_CHANGED",
      "REVALIDATION_REQUIRED",
    }))

  if not action_types & _REBUILD_ACTION_TYPES:
    return "BLOCKED", ("MANUAL_REVIEW_REQUIRED",)

  return "FULL_REBUILD", _full_rebuild_reason_codes(dataset_evidence)


def _full_rebuild_reason_codes(
  dataset_evidence: ExecutionImpactDatasetEvidence,
) -> tuple[ExecutionImpactReasonCode, ...]:
  """
  Return specific reasons for a conservative local full rebuild decision.
  """
  reasons: set[ExecutionImpactReasonCode] = set()
  action_types = set(dataset_evidence.migration_action_types)
  dataset_change_types = set(dataset_evidence.dataset_change_types)
  column_change_types = set(dataset_evidence.column_change_types)
  changed_fields = set(dataset_evidence.dataset_changed_fields)

  is_added = bool(
    "DATASET_ADDED" in dataset_change_types
    or "CREATE_DATASET" in action_types
  )
  if is_added:
    reasons.add("DATASET_ADDED")

  if "materialization_type" in changed_fields:
    reasons.add("MATERIALIZATION_CHANGED")

  if "incremental_strategy" in changed_fields:
    reasons.add("INCREMENTAL_STRATEGY_CHANGED")

  if not is_added and (
    dataset_change_types
    or action_types & {"RENAME_DATASET", "REBUILD_DATASET", "DROP_DATASET"}
    or changed_fields - {"materialization_type", "incremental_strategy"}
  ):
    reasons.add("DATASET_DEFINITION_CHANGED")

  if not is_added and (
    column_change_types
    or action_types & _COLUMN_ACTION_TYPES
  ):
    reasons.add("COLUMN_CONTRACT_CHANGED")

  if not reasons:
    reasons.add("DATASET_DEFINITION_CHANGED")

  return tuple(sorted(reasons))


def _decision_evidence(
  *,
  dataset_evidence: ExecutionImpactDatasetEvidence,
) -> tuple[ExecutionImpactEvidenceReference, ...]:
  """
  Mark baseline evidence as required when the current dataset already existed.
  """
  normalized: list[ExecutionImpactEvidenceReference] = []

  for item in dataset_evidence.evidence:
    is_baseline_state = (
      item.evidence_type == "architecture_state"
      and item.evidence_key == f"baseline:{dataset_evidence.dataset_key}"
    )
    if is_baseline_state and dataset_evidence.baseline_fingerprint is not None:
      normalized.append(replace(item, required=True))
      continue

    if is_baseline_state and not dataset_evidence.has_architecture_changes:
      normalized.append(replace(item, required=True))
      continue

    normalized.append(item)

  return tuple(normalized)


def _reason_for_unavailable_evidence(
  item: ExecutionImpactEvidenceReference,
  *,
  review_status: str,
) -> ExecutionImpactReasonCode:
  """
  Map unavailable required evidence to a stable blocking reason code.
  """
  if item.evidence_type == "architecture_approval":
    if review_status == "blocked":
      return "POLICY_BLOCKED"
    if review_status == "invalid":
      return "MANUAL_REVIEW_REQUIRED"
    return "APPROVAL_REQUIRED"
  if item.evidence_type == "architecture_state":
    return "BASELINE_UNAVAILABLE"
  if item.evidence_type == "architecture_execution_record":
    return "EXECUTION_EVIDENCE_UNAVAILABLE"
  if item.evidence_type == "physical_state":
    return "PHYSICAL_STATE_UNAVAILABLE"
  if item.evidence_type == "source_evidence":
    return "SOURCE_EVIDENCE_UNAVAILABLE"
  if item.evidence_type == "policy_decision":
    return "MANUAL_REVIEW_REQUIRED"
  return "MANUAL_REVIEW_REQUIRED"


def _scope_evidence(
  resolution: ExecutionImpactEvidenceResolution,
) -> tuple[ExecutionImpactEvidenceReference, ...]:
  """
  Build stable scope-level evidence for the local impact plan.
  """
  baseline_evidence: ExecutionImpactEvidenceReference
  if resolution.baseline_fingerprint:
    baseline_evidence = ExecutionImpactEvidenceReference(
      evidence_type="architecture_state",
      evidence_key=f"baseline:{resolution.scope_key}",
      status="available",
      required=True,
      fingerprint=resolution.baseline_fingerprint,
      message=(
        "Architecture comparison baseline used for local impact decisions. "
        f"Source: {resolution.baseline_source}."
      ),
    )
  else:
    baseline_evidence = ExecutionImpactEvidenceReference(
      evidence_type="architecture_state",
      evidence_key=f"baseline:{resolution.scope_key}",
      status="unavailable",
      required=True,
      message=(
        "Architecture comparison baseline is unavailable for local impact "
        "decisions."
      ),
    )

  return (
    ExecutionImpactEvidenceReference(
      evidence_type="architecture_state",
      evidence_key=f"current:{resolution.scope_key}",
      status="available",
      required=True,
      fingerprint=resolution.architecture_fingerprint,
      message="Current metadata-defined architecture state.",
    ),
    baseline_evidence,
    ExecutionImpactEvidenceReference(
      evidence_type="architecture_change_report",
      evidence_key=f"scope:{resolution.scope_key}",
      status="available",
      required=True,
      fingerprint=resolution.report_fingerprint,
      message="Architecture Change Report used for local impact decisions.",
    ),
  )
