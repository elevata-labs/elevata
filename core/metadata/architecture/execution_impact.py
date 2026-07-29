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

from dataclasses import dataclass, field
import hashlib
import json
from typing import Any, Literal


EXECUTION_IMPACT_PLAN_ARTIFACT_TYPE = "execution_impact_plan"
EXECUTION_IMPACT_PLAN_ARTIFACT_VERSION = 1

ExecutionImpactDecision = Literal[
  "REUSE",
  "REVALIDATE",
  "INCREMENTAL_EXECUTE",
  "FULL_REBUILD",
  "BLOCKED",
]

ExecutionImpactExecutableDecision = Literal[
  "REUSE",
  "INCREMENTAL_EXECUTE",
  "FULL_REBUILD",
]

ExecutionImpactRecommendedAction = Literal[
  "reuse_materialization",
  "run_revalidation",
  "execute_incremental",
  "execute_full_rebuild",
  "resolve_blocker",
]

ExecutionImpactEvidenceStatus = Literal[
  "available",
  "unavailable",
  "not_applicable",
]

ExecutionImpactEvidenceType = Literal[
  "architecture_state",
  "architecture_change_report",
  "architecture_approval",
  "architecture_execution_record",
  "architecture_review_status",
  "policy_decision",
  "physical_state",
  "source_evidence",
  "execution_dependency",
]

ExecutionImpactReasonCode = Literal[
  "NO_RELEVANT_CHANGE",
  "REVALIDATION_REQUIRED",
  "DATASET_ADDED",
  "DATASET_DEFINITION_CHANGED",
  "COLUMN_CONTRACT_CHANGED",
  "MATERIALIZATION_CHANGED",
  "INCREMENTAL_STRATEGY_CHANGED",
  "FULL_REFRESH_STRATEGY",
  "INCREMENTAL_LOAD_STRATEGY",
  "VIRTUAL_MATERIALIZATION_REUSED",
  "LOGICAL_PLAN_CHANGED",
  "UPSTREAM_EXECUTION_REQUIRED",
  "UPSTREAM_REBUILD_REQUIRED",
  "UPSTREAM_BLOCKED",
  "APPROVAL_REQUIRED",
  "POLICY_BLOCKED",
  "BASELINE_UNAVAILABLE",
  "EXECUTION_EVIDENCE_UNAVAILABLE",
  "PHYSICAL_STATE_DRIFT",
  "PHYSICAL_STATE_UNAVAILABLE",
  "SOURCE_CHANGE_DETECTED",
  "SOURCE_EVIDENCE_UNAVAILABLE",
  "MANUAL_REVIEW_REQUIRED",
]

_DECISION_ORDER: tuple[ExecutionImpactDecision, ...] = (
  "REUSE",
  "REVALIDATE",
  "INCREMENTAL_EXECUTE",
  "FULL_REBUILD",
  "BLOCKED",
)

_DECISION_PRIORITY: dict[ExecutionImpactDecision, int] = {
  decision: priority
  for priority, decision in enumerate(_DECISION_ORDER)
}

_DECISION_LABELS: dict[ExecutionImpactDecision, str] = {
  "REUSE": "Reuse",
  "REVALIDATE": "Revalidate",
  "INCREMENTAL_EXECUTE": "Incremental execute",
  "FULL_REBUILD": "Full rebuild",
  "BLOCKED": "Blocked",
}

_RECOMMENDED_ACTIONS: dict[
  ExecutionImpactDecision,
  ExecutionImpactRecommendedAction,
] = {
  "REUSE": "reuse_materialization",
  "REVALIDATE": "run_revalidation",
  "INCREMENTAL_EXECUTE": "execute_incremental",
  "FULL_REBUILD": "execute_full_rebuild",
  "BLOCKED": "resolve_blocker",
}

_ALLOWED_DECISIONS = frozenset(_DECISION_ORDER)
_ALLOWED_EXECUTABLE_DECISIONS = frozenset({
  "REUSE",
  "INCREMENTAL_EXECUTE",
  "FULL_REBUILD",
})
_ALLOWED_EVIDENCE_STATUSES = frozenset({
  "available",
  "unavailable",
  "not_applicable",
})
_ALLOWED_EVIDENCE_TYPES = frozenset({
  "architecture_state",
  "architecture_change_report",
  "architecture_approval",
  "architecture_execution_record",
  "architecture_review_status",
  "policy_decision",
  "physical_state",
  "source_evidence",
  "execution_dependency",
})
_ALLOWED_REASON_CODES = frozenset({
  "NO_RELEVANT_CHANGE",
  "REVALIDATION_REQUIRED",
  "DATASET_ADDED",
  "DATASET_DEFINITION_CHANGED",
  "COLUMN_CONTRACT_CHANGED",
  "MATERIALIZATION_CHANGED",
  "INCREMENTAL_STRATEGY_CHANGED",
  "FULL_REFRESH_STRATEGY",
  "INCREMENTAL_LOAD_STRATEGY",
  "VIRTUAL_MATERIALIZATION_REUSED",
  "LOGICAL_PLAN_CHANGED",
  "UPSTREAM_EXECUTION_REQUIRED",
  "UPSTREAM_REBUILD_REQUIRED",
  "UPSTREAM_BLOCKED",
  "APPROVAL_REQUIRED",
  "POLICY_BLOCKED",
  "BASELINE_UNAVAILABLE",
  "EXECUTION_EVIDENCE_UNAVAILABLE",
  "PHYSICAL_STATE_DRIFT",
  "PHYSICAL_STATE_UNAVAILABLE",
  "SOURCE_CHANGE_DETECTED",
  "SOURCE_EVIDENCE_UNAVAILABLE",
  "MANUAL_REVIEW_REQUIRED",
})


@dataclass(frozen=True)
class ExecutionImpactSelection:
  """
  Immutable operational selection derived from one Execution Impact Plan.

  Dataset decisions retain the exact controlled-execution order. Only
  decisions with a defined runtime behavior are accepted.
  """
  plan_fingerprint: str
  dataset_decisions: tuple[
    tuple[str, ExecutionImpactExecutableDecision],
    ...,
  ]

  def __post_init__(self) -> None:
    plan_fingerprint = str(self.plan_fingerprint or "").strip()
    if not plan_fingerprint:
      raise ValueError(
        "Execution Impact Selection requires a plan fingerprint."
      )

    normalized: list[
      tuple[str, ExecutionImpactExecutableDecision]
    ] = []
    seen_keys: set[str] = set()

    for raw_dataset_key, raw_decision in tuple(
      self.dataset_decisions or ()
    ):
      dataset_key = str(raw_dataset_key or "").strip()
      decision = str(raw_decision or "").strip()

      if not dataset_key:
        raise ValueError(
          "Execution Impact Selection dataset keys must not be empty."
        )
      if dataset_key in seen_keys:
        raise ValueError(
          "Execution Impact Selection contains duplicate dataset key: "
          f"{dataset_key}"
        )

      _validate_decision(decision)
      if decision not in _ALLOWED_EXECUTABLE_DECISIONS:
        raise ValueError(
          "Execution Impact Selection cannot execute decision "
          f"{decision} for {dataset_key}."
        )

      seen_keys.add(dataset_key)
      normalized.append((dataset_key, decision))

    if not normalized:
      raise ValueError(
        "Execution Impact Selection requires at least one dataset."
      )

    object.__setattr__(self, "plan_fingerprint", plan_fingerprint)
    object.__setattr__(self, "dataset_decisions", tuple(normalized))

  @property
  def dataset_keys(self) -> tuple[str, ...]:
    """
    Return selected dataset keys in controlled-execution order.
    """
    return tuple(
      dataset_key
      for dataset_key, _decision in self.dataset_decisions
    )

  @property
  def decisions_by_key(
    self,
  ) -> dict[str, ExecutionImpactExecutableDecision]:
    """
    Return operational decisions indexed by dataset key.
    """
    return dict(self.dataset_decisions)

  def to_dict(self) -> dict[str, Any]:
    """
    Return the canonical operational selection payload.
    """
    return {
      "plan_fingerprint": self.plan_fingerprint,
      "dataset_decisions": [
        {
          "dataset_key": dataset_key,
          "decision": decision,
        }
        for dataset_key, decision in self.dataset_decisions
      ],
    }


@dataclass(frozen=True)
class ExecutionImpactEvidenceReference:
  """
  Stable reference to one evidence input used by an impact decision.

  Evidence references identify and fingerprint evidence without embedding the
  complete source artifact. Required unavailable evidence prevents a safe
  non-blocking decision.
  """
  evidence_type: ExecutionImpactEvidenceType
  evidence_key: str
  status: ExecutionImpactEvidenceStatus
  required: bool = True
  fingerprint: str | None = None
  artifact_reference: str | None = None
  message: str | None = None

  def __post_init__(self) -> None:
    if not str(self.evidence_type).strip():
      raise ValueError("Execution impact evidence type must not be empty.")

    if self.evidence_type not in _ALLOWED_EVIDENCE_TYPES:
      raise ValueError(
        f"Unsupported execution impact evidence type: {self.evidence_type}"
      )

    if not str(self.evidence_key).strip():
      raise ValueError("Execution impact evidence key must not be empty.")

    if self.status not in _ALLOWED_EVIDENCE_STATUSES:
      raise ValueError(
        f"Unsupported execution impact evidence status: {self.status}"
      )

    if self.status == "available" and not str(self.fingerprint or "").strip():
      raise ValueError(
        "Available execution impact evidence requires a fingerprint."
      )

  @property
  def is_unavailable_required(self) -> bool:
    """
    Return True when evidence required for a safe decision is unavailable.
    """
    return self.required and self.status == "unavailable"

  def to_dict(self) -> dict[str, Any]:
    """
    Return a deterministic dictionary representation.
    """
    return {
      "evidence_type": self.evidence_type,
      "evidence_key": self.evidence_key,
      "status": self.status,
      "required": self.required,
      "fingerprint": self.fingerprint,
      "artifact_reference": self.artifact_reference,
      "message": self.message,
    }


@dataclass(frozen=True)
class ExecutionImpactPropagation:
  """
  Explain one upstream contribution to a downstream impact decision.

  Dependency reason is retained because scheduling dependencies and data-change
  propagation dependencies are not necessarily equivalent.
  """
  upstream_dataset_key: str
  upstream_decision: ExecutionImpactDecision
  dependency_reason: str
  reason_code: ExecutionImpactReasonCode

  def __post_init__(self) -> None:
    if not str(self.upstream_dataset_key).strip():
      raise ValueError("Execution impact upstream dataset key must not be empty.")

    _validate_decision(self.upstream_decision)

    if not str(self.dependency_reason).strip():
      raise ValueError("Execution impact dependency reason must not be empty.")

    _validate_reason_code(self.reason_code)

  def to_dict(self) -> dict[str, Any]:
    """
    Return a deterministic dictionary representation.
    """
    return {
      "upstream_dataset_key": self.upstream_dataset_key,
      "upstream_decision": self.upstream_decision,
      "dependency_reason": self.dependency_reason,
      "reason_code": self.reason_code,
    }


@dataclass(frozen=True)
class ExecutionImpactItem:
  """
  Explain the read-only execution impact decision for one TargetDataset.
  """
  dataset_key: str
  decision: ExecutionImpactDecision
  reason_codes: tuple[ExecutionImpactReasonCode, ...]
  architecture_fingerprint: str
  last_successful_execution_fingerprint: str | None = None
  evidence: tuple[ExecutionImpactEvidenceReference, ...] = field(default_factory=tuple)
  upstream_dataset_keys: tuple[str, ...] = field(default_factory=tuple)
  downstream_dataset_keys: tuple[str, ...] = field(default_factory=tuple)
  propagations: tuple[ExecutionImpactPropagation, ...] = field(default_factory=tuple)

  def __post_init__(self) -> None:
    if not str(self.dataset_key).strip():
      raise ValueError("Execution impact dataset key must not be empty.")

    _validate_decision(self.decision)

    if not self.reason_codes:
      raise ValueError("Execution impact decisions require at least one reason code.")

    for reason_code in self.reason_codes:
      _validate_reason_code(reason_code)

    if not str(self.architecture_fingerprint).strip():
      raise ValueError(
        "Execution impact decisions require an architecture fingerprint."
      )

    if not self.evidence:
      raise ValueError("Execution impact decisions require evidence references.")

    object.__setattr__(
      self,
      "reason_codes",
      tuple(sorted(set(self.reason_codes))),
    )
    object.__setattr__(
      self,
      "evidence",
      tuple(sorted(self.evidence, key=_evidence_sort_key)),
    )
    object.__setattr__(
      self,
      "upstream_dataset_keys",
      tuple(sorted(set(self.upstream_dataset_keys))),
    )
    object.__setattr__(
      self,
      "downstream_dataset_keys",
      tuple(sorted(set(self.downstream_dataset_keys))),
    )
    object.__setattr__(
      self,
      "propagations",
      tuple(sorted(self.propagations, key=_propagation_sort_key)),
    )

    if self.has_unavailable_required_evidence and self.decision != "BLOCKED":
      raise ValueError(
        "Unavailable required evidence requires a BLOCKED impact decision."
      )

  @property
  def decision_label(self) -> str:
    """
    Return the human-readable decision label.
    """
    return execution_impact_decision_label(self.decision)

  @property
  def recommended_action(self) -> ExecutionImpactRecommendedAction:
    """
    Return the deterministic recommended action for this decision.
    """
    return _RECOMMENDED_ACTIONS[self.decision]

  @property
  def has_unavailable_required_evidence(self) -> bool:
    """
    Return True when at least one required evidence input is unavailable.
    """
    return any(item.is_unavailable_required for item in self.evidence)

  def to_dict(self) -> dict[str, Any]:
    """
    Return a deterministic dictionary representation.
    """
    return {
      "dataset_key": self.dataset_key,
      "decision": self.decision,
      "decision_label": self.decision_label,
      "recommended_action": self.recommended_action,
      "reason_codes": list(self.reason_codes),
      "architecture_fingerprint": self.architecture_fingerprint,
      "last_successful_execution_fingerprint": (
        self.last_successful_execution_fingerprint
      ),
      "evidence": [item.to_dict() for item in self.evidence],
      "upstream_dataset_keys": list(self.upstream_dataset_keys),
      "downstream_dataset_keys": list(self.downstream_dataset_keys),
      "propagations": [item.to_dict() for item in self.propagations],
    }


@dataclass(frozen=True)
class ExecutionImpactPlan:
  """
  Deterministic read-only Execution Impact Plan for one architecture scope.
  """
  scope_key: str
  architecture_fingerprint: str
  report_fingerprint: str
  baseline_fingerprint: str | None = None
  items: tuple[ExecutionImpactItem, ...] = field(default_factory=tuple)
  evidence: tuple[ExecutionImpactEvidenceReference, ...] = field(default_factory=tuple)

  def __post_init__(self) -> None:
    if not str(self.scope_key).strip():
      raise ValueError("Execution impact scope key must not be empty.")

    if not str(self.architecture_fingerprint).strip():
      raise ValueError("Execution impact architecture fingerprint must not be empty.")

    if not str(self.report_fingerprint).strip():
      raise ValueError("Execution impact report fingerprint must not be empty.")

    ordered_items = tuple(sorted(self.items, key=lambda item: item.dataset_key))
    dataset_keys = [item.dataset_key for item in ordered_items]
    if len(dataset_keys) != len(set(dataset_keys)):
      raise ValueError("Execution Impact Plan contains duplicate dataset keys.")

    object.__setattr__(self, "items", ordered_items)
    object.__setattr__(
      self,
      "evidence",
      tuple(sorted(self.evidence, key=_evidence_sort_key)),
    )

  @property
  def assessed_count(self) -> int:
    """
    Return the number of assessed TargetDatasets.
    """
    return len(self.items)

  @property
  def decision_counts(self) -> dict[ExecutionImpactDecision, int]:
    """
    Return stable counts for every decision type, including zero values.
    """
    return {
      decision: sum(1 for item in self.items if item.decision == decision)
      for decision in _DECISION_ORDER
    }

  @property
  def plan_fingerprint(self) -> str:
    """
    Return the deterministic fingerprint of the complete impact plan.
    """
    return _stable_json_hash(self.to_dict(include_fingerprint=False))

  def to_dict(self, *, include_fingerprint: bool = True) -> dict[str, Any]:
    """
    Return the canonical Execution Impact Plan payload.
    """
    payload = {
      "artifact_type": EXECUTION_IMPACT_PLAN_ARTIFACT_TYPE,
      "artifact_version": EXECUTION_IMPACT_PLAN_ARTIFACT_VERSION,
      "scope_key": self.scope_key,
      "architecture_fingerprint": self.architecture_fingerprint,
      "baseline_fingerprint": self.baseline_fingerprint,
      "report_fingerprint": self.report_fingerprint,
      "assessed_count": self.assessed_count,
      "decision_counts": self.decision_counts,
      "evidence": [item.to_dict() for item in self.evidence],
      "items": [item.to_dict() for item in self.items],
    }

    if include_fingerprint:
      payload["plan_fingerprint"] = self.plan_fingerprint

    return payload


def build_execution_impact_selection(
  plan: ExecutionImpactPlan,
  *,
  execution_dataset_keys: tuple[str, ...],
) -> ExecutionImpactSelection:
  """
  Bind executable impact decisions to one exact execution sequence.
  """
  normalized_keys = tuple(
    str(dataset_key or "").strip()
    for dataset_key in execution_dataset_keys
  )

  if not normalized_keys or any(
    not dataset_key
    for dataset_key in normalized_keys
  ):
    raise ValueError(
      "Execution Impact Selection requires non-empty execution dataset keys."
    )

  if len(normalized_keys) != len(set(normalized_keys)):
    raise ValueError(
      "Execution Impact Selection contains duplicate execution dataset keys."
    )

  item_by_key = {
    item.dataset_key: item
    for item in plan.items
  }
  plan_keys = set(item_by_key)
  execution_keys = set(normalized_keys)

  if (
    plan_keys != execution_keys
    or len(plan.items) != len(normalized_keys)
  ):
    missing_keys = tuple(sorted(plan_keys - execution_keys))
    unexpected_keys = tuple(sorted(execution_keys - plan_keys))
    details: list[str] = []

    if missing_keys:
      details.append(
        "missing from execution scope: " + ", ".join(missing_keys)
      )
    if unexpected_keys:
      details.append(
        "not present in impact plan: " + ", ".join(unexpected_keys)
      )

    raise ValueError(
      "Execution Impact Plan does not match the controlled execution scope"
      + (f"; {'; '.join(details)}" if details else "")
      + "."
    )

  non_executable = tuple(
    (
      dataset_key,
      item_by_key[dataset_key].decision,
    )
    for dataset_key in normalized_keys
    if (
      item_by_key[dataset_key].decision
      not in _ALLOWED_EXECUTABLE_DECISIONS
    )
  )
  if non_executable:
    detail = ", ".join(
      f"{dataset_key}={decision}"
      for dataset_key, decision in non_executable
    )
    raise ValueError(
      "Execution Impact Plan contains non-executable decisions: "
      f"{detail}."
    )

  return ExecutionImpactSelection(
    plan_fingerprint=plan.plan_fingerprint,
    dataset_decisions=tuple(
      (
        dataset_key,
        item_by_key[dataset_key].decision,
      )
      for dataset_key in normalized_keys
    ),
  )


def execution_impact_decision_label(
  decision: ExecutionImpactDecision,
) -> str:
  """
  Return the stable human-readable label for one impact decision.
  """
  _validate_decision(decision)
  return _DECISION_LABELS[decision]


def strongest_execution_impact_decision(
  *decisions: ExecutionImpactDecision,
) -> ExecutionImpactDecision:
  """
  Return the highest-priority decision from one or more impact decisions.

  Priority is conservative and deterministic:
  BLOCKED > FULL_REBUILD > INCREMENTAL_EXECUTE > REVALIDATE > REUSE.
  """
  if not decisions:
    raise ValueError("At least one execution impact decision is required.")

  for decision in decisions:
    _validate_decision(decision)

  return max(decisions, key=lambda decision: _DECISION_PRIORITY[decision])


def render_execution_impact_plan_json(plan: ExecutionImpactPlan) -> str:
  """
  Render an Execution Impact Plan as canonical JSON.
  """
  return json.dumps(
    plan.to_dict(),
    sort_keys=True,
    ensure_ascii=False,
    indent=2,
    default=str,
  ) + "\n"


def _validate_decision(decision: str) -> None:
  """
  Validate one execution impact decision key.
  """
  if decision not in _ALLOWED_DECISIONS:
    raise ValueError(f"Unsupported execution impact decision: {decision}")


def _validate_reason_code(reason_code: str) -> None:
  """
  Validate one execution impact reason code.
  """
  if reason_code not in _ALLOWED_REASON_CODES:
    raise ValueError(f"Unsupported execution impact reason code: {reason_code}")


def _evidence_sort_key(
  item: ExecutionImpactEvidenceReference,
) -> tuple[str, str, str, str]:
  """
  Return the deterministic sort key for evidence references.
  """
  return (
    item.evidence_type,
    item.evidence_key,
    item.status,
    item.fingerprint or "",
  )


def _propagation_sort_key(
  item: ExecutionImpactPropagation,
) -> tuple[str, str, str, str]:
  """
  Return the deterministic sort key for dependency propagation evidence.
  """
  return (
    item.upstream_dataset_key,
    item.dependency_reason,
    item.upstream_decision,
    item.reason_code,
  )


def _stable_json_hash(value: Any) -> str:
  """
  Return a deterministic SHA-256 hash for a JSON-serializable value.
  """
  payload = json.dumps(
    value,
    sort_keys=True,
    ensure_ascii=False,
    separators=(",", ":"),
    default=str,
  )
  return hashlib.sha256(payload.encode("utf-8")).hexdigest()
