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

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from json import JSONDecodeError
from pathlib import Path
import re
from typing import Any, Literal, Mapping, Sequence

from metadata.generation.target_generation_plan import (
  TargetGenerationPlan,
  parse_target_generation_plan_json,
)


TARGET_GENERATION_REVIEW_ARTIFACT_TYPE = "target_generation_review"
TARGET_GENERATION_REVIEW_ARTIFACT_VERSION = 1
TARGET_GENERATION_APPROVAL_ARTIFACT_TYPE = "target_generation_approval"
TARGET_GENERATION_APPROVAL_ARTIFACT_VERSION = 1
TARGET_GENERATION_APPROVAL_REFERENCE_TYPE = "target_generation_review"

TargetGenerationApprovalDecision = Literal["approved", "rejected"]

_CLASSIFICATION_ORDER = ("BREAKING", "ADDITIVE", "NEUTRAL")
_EFFECT_ORIGIN_ORDER = (
  "DIRECT",
  "HISTORY_COMPANION",
  "GENERATED_LIFECYCLE",
  "MODEL_SIDE_EFFECT",
)
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")

_REVIEW_KEYS = frozenset({
  "artifact_type",
  "artifact_version",
  "plan_fingerprint",
  "scope",
  "source_metadata_fingerprint",
  "target_metadata_fingerprint",
  "summary",
  "dataset_impacts",
  "source_impacts",
  "review_fingerprint",
})
_DATASET_IMPACT_KEYS = frozenset({
  "dataset_key",
  "source_keys",
  "action_count",
  "action_counts",
  "effect_origins",
  "change_classification",
})
_SOURCE_IMPACT_KEYS = frozenset({
  "source_key",
  "target_dataset_keys",
  "action_count",
  "change_classification",
})
_APPROVAL_KEYS = frozenset({
  "artifact_type",
  "artifact_version",
  "approval_id",
  "review",
  "decision",
  "artifact_fingerprint",
})
_DECISION_KEYS = frozenset({
  "decision",
  "decided_by",
  "decided_at",
  "note",
})


class TargetGenerationControlError(ValueError):
  """Raised when a Target Generation review or approval is invalid."""


@dataclass(frozen=True)
class TargetGenerationDatasetImpact:
  """Summarize all planned actions for one target dataset."""

  dataset_key: str
  source_keys: tuple[str, ...]
  action_counts: tuple[tuple[str, int], ...]
  effect_origins: tuple[str, ...]
  change_classification: str

  def __post_init__(self) -> None:
    dataset_key = _required_text(self.dataset_key, label="dataset key")
    source_keys = _normalized_text_tuple(self.source_keys, label="source key")
    action_counts = _normalized_counts(
      self.action_counts,
      label="dataset action counts",
    )
    effect_origins = _normalized_choice_tuple(
      self.effect_origins,
      allowed=_EFFECT_ORIGIN_ORDER,
      label="effect origin",
    )
    classification = _classification(self.change_classification)

    if not action_counts:
      raise TargetGenerationControlError(
        "Target Generation dataset impacts require at least one action."
      )

    object.__setattr__(self, "dataset_key", dataset_key)
    object.__setattr__(self, "source_keys", source_keys)
    object.__setattr__(self, "action_counts", action_counts)
    object.__setattr__(self, "effect_origins", effect_origins)
    object.__setattr__(self, "change_classification", classification)

  @property
  def action_count(self) -> int:
    """Return the number of planned actions for this target dataset."""
    return sum(count for _action_type, count in self.action_counts)

  def to_dict(self) -> dict[str, Any]:
    """Return the canonical public dataset-impact payload."""
    return {
      "dataset_key": self.dataset_key,
      "source_keys": list(self.source_keys),
      "action_count": self.action_count,
      "action_counts": dict(self.action_counts),
      "effect_origins": list(self.effect_origins),
      "change_classification": self.change_classification,
    }


@dataclass(frozen=True)
class TargetGenerationSourceImpact:
  """Summarize all planned target impacts originating from one source."""

  source_key: str
  target_dataset_keys: tuple[str, ...]
  action_count: int
  change_classification: str

  def __post_init__(self) -> None:
    source_key = _required_text(self.source_key, label="source key")
    target_dataset_keys = _normalized_text_tuple(
      self.target_dataset_keys,
      label="target dataset key",
    )
    if not target_dataset_keys:
      raise TargetGenerationControlError(
        "Target Generation source impacts require at least one target dataset."
      )
    action_count = _positive_int(self.action_count, label="source action count")
    classification = _classification(self.change_classification)

    object.__setattr__(self, "source_key", source_key)
    object.__setattr__(self, "target_dataset_keys", target_dataset_keys)
    object.__setattr__(self, "action_count", action_count)
    object.__setattr__(self, "change_classification", classification)

  def to_dict(self) -> dict[str, Any]:
    """Return the canonical public source-impact payload."""
    return {
      "source_key": self.source_key,
      "target_dataset_keys": list(self.target_dataset_keys),
      "action_count": self.action_count,
      "change_classification": self.change_classification,
    }


@dataclass(frozen=True)
class TargetGenerationReview:
  """Deterministic Source-to-Target review derived from one exact plan."""

  plan_fingerprint: str
  scope_mode: str
  target_schema_short_names: tuple[str, ...]
  source_dataset_keys: tuple[str, ...]
  reconcile_lifecycle: bool
  source_metadata_fingerprint: str
  target_metadata_fingerprint: str
  action_counts: tuple[tuple[str, int], ...]
  classification_counts: tuple[tuple[str, int], ...]
  effect_origin_counts: tuple[tuple[str, int], ...]
  dataset_impacts: tuple[TargetGenerationDatasetImpact, ...]
  source_impacts: tuple[TargetGenerationSourceImpact, ...]

  def __post_init__(self) -> None:
    plan_fingerprint = _fingerprint(self.plan_fingerprint, label="plan")
    scope_mode = _required_text(self.scope_mode, label="scope mode")
    if scope_mode not in {"schema", "all"}:
      raise TargetGenerationControlError(
        f"Unsupported Target Generation review scope mode: {scope_mode}"
      )
    target_schema_short_names = _normalized_text_tuple(
      self.target_schema_short_names,
      label="target schema short name",
    )
    if not target_schema_short_names:
      raise TargetGenerationControlError(
        "Target Generation reviews require at least one target schema."
      )
    source_dataset_keys = _normalized_text_tuple(
      self.source_dataset_keys,
      label="source dataset key",
    )
    if not isinstance(self.reconcile_lifecycle, bool):
      raise TargetGenerationControlError(
        "Target Generation review reconcile_lifecycle must be a boolean."
      )
    source_metadata_fingerprint = _fingerprint(
      self.source_metadata_fingerprint,
      label="source metadata",
    )
    target_metadata_fingerprint = _fingerprint(
      self.target_metadata_fingerprint,
      label="target metadata",
    )
    action_counts = _normalized_counts(self.action_counts, label="action counts")
    classification_counts = _normalized_named_counts(
      self.classification_counts,
      names=_CLASSIFICATION_ORDER,
      label="classification counts",
    )
    effect_origin_counts = _normalized_named_counts(
      self.effect_origin_counts,
      names=_EFFECT_ORIGIN_ORDER,
      label="effect origin counts",
    )
    dataset_impacts = tuple(sorted(
      tuple(self.dataset_impacts or ()),
      key=lambda impact: impact.dataset_key,
    ))
    source_impacts = tuple(sorted(
      tuple(self.source_impacts or ()),
      key=lambda impact: impact.source_key,
    ))
    if any(not isinstance(item, TargetGenerationDatasetImpact) for item in dataset_impacts):
      raise TargetGenerationControlError(
        "Target Generation reviews require validated dataset impacts."
      )
    if any(not isinstance(item, TargetGenerationSourceImpact) for item in source_impacts):
      raise TargetGenerationControlError(
        "Target Generation reviews require validated source impacts."
      )

    action_count = sum(count for _name, count in action_counts)
    if action_count != sum(count for _name, count in classification_counts):
      raise TargetGenerationControlError(
        "Target Generation review classification counts do not match actions."
      )
    if action_count != sum(count for _name, count in effect_origin_counts):
      raise TargetGenerationControlError(
        "Target Generation review effect-origin counts do not match actions."
      )
    if action_count != sum(item.action_count for item in dataset_impacts):
      raise TargetGenerationControlError(
        "Target Generation review dataset impacts do not match actions."
      )

    object.__setattr__(self, "plan_fingerprint", plan_fingerprint)
    object.__setattr__(self, "scope_mode", scope_mode)
    object.__setattr__(self, "target_schema_short_names", target_schema_short_names)
    object.__setattr__(self, "source_dataset_keys", source_dataset_keys)
    object.__setattr__(self, "source_metadata_fingerprint", source_metadata_fingerprint)
    object.__setattr__(self, "target_metadata_fingerprint", target_metadata_fingerprint)
    object.__setattr__(self, "action_counts", action_counts)
    object.__setattr__(self, "classification_counts", classification_counts)
    object.__setattr__(self, "effect_origin_counts", effect_origin_counts)
    object.__setattr__(self, "dataset_impacts", dataset_impacts)
    object.__setattr__(self, "source_impacts", source_impacts)

  @property
  def action_count(self) -> int:
    """Return the number of reviewed plan actions."""
    return sum(count for _name, count in self.action_counts)

  @property
  def has_breaking_changes(self) -> bool:
    """Return whether the plan contains at least one breaking change."""
    return dict(self.classification_counts)["BREAKING"] > 0

  @property
  def approval_recommended(self) -> bool:
    """Return whether review policy recommends a generation approval."""
    return self.has_breaking_changes

  @property
  def review_fingerprint(self) -> str:
    """Return the deterministic fingerprint of this review."""
    return _stable_json_hash(self.to_dict(include_fingerprint=False))

  def to_dict(self, *, include_fingerprint: bool = True) -> dict[str, Any]:
    """Return the canonical public review payload."""
    payload = {
      "artifact_type": TARGET_GENERATION_REVIEW_ARTIFACT_TYPE,
      "artifact_version": TARGET_GENERATION_REVIEW_ARTIFACT_VERSION,
      "plan_fingerprint": self.plan_fingerprint,
      "scope": {
        "mode": self.scope_mode,
        "target_schema_short_names": list(self.target_schema_short_names),
        "source_dataset_keys": list(self.source_dataset_keys),
        "reconcile_lifecycle": self.reconcile_lifecycle,
      },
      "source_metadata_fingerprint": self.source_metadata_fingerprint,
      "target_metadata_fingerprint": self.target_metadata_fingerprint,
      "summary": {
        "action_count": self.action_count,
        "action_counts": dict(self.action_counts),
        "classification_counts": dict(self.classification_counts),
        "effect_origin_counts": dict(self.effect_origin_counts),
        "target_dataset_count": len(self.dataset_impacts),
        "source_count": len(self.source_impacts),
        "has_breaking_changes": self.has_breaking_changes,
        "approval_recommended": self.approval_recommended,
      },
      "dataset_impacts": [item.to_dict() for item in self.dataset_impacts],
      "source_impacts": [item.to_dict() for item in self.source_impacts],
    }
    if include_fingerprint:
      payload["review_fingerprint"] = self.review_fingerprint
    return payload


@dataclass(frozen=True)
class TargetGenerationApprovalDecisionRecord:
  """Review decision that authorizes one exact generation review."""

  decision: TargetGenerationApprovalDecision
  decided_by: str
  decided_at: str
  note: str = ""

  def __post_init__(self) -> None:
    decision = _required_text(self.decision, label="approval decision")
    if decision not in {"approved", "rejected"}:
      raise TargetGenerationControlError(
        f"Unsupported Target Generation approval decision: {decision}"
      )
    object.__setattr__(self, "decision", decision)
    object.__setattr__(self, "decided_by", _required_text(self.decided_by, label="reviewer"))
    object.__setattr__(self, "decided_at", _normalize_decided_at(self.decided_at))
    object.__setattr__(self, "note", str(self.note or ""))

  def to_dict(self) -> dict[str, Any]:
    """Return the canonical decision payload."""
    return {
      "decision": self.decision,
      "decided_by": self.decided_by,
      "decided_at": self.decided_at,
      "note": self.note,
    }


@dataclass(frozen=True)
class TargetGenerationApprovalArtifact:
  """Approval artifact authorizing metadata mutation of one exact plan."""

  review: Mapping[str, Any]
  decision: TargetGenerationApprovalDecisionRecord

  def __post_init__(self) -> None:
    review = _canonical_object(self.review, label="generation review reference")
    _validate_review_reference(review)
    if not isinstance(self.decision, TargetGenerationApprovalDecisionRecord):
      raise TargetGenerationControlError(
        "Target Generation approvals require a validated decision record."
      )
    object.__setattr__(self, "review", review)

  @property
  def artifact_fingerprint(self) -> str:
    """Return the deterministic artifact fingerprint."""
    return _stable_json_hash(self.to_dict(include_fingerprint=False))

  @property
  def approval_id(self) -> str:
    """Return the generation-specific stable approval identifier."""
    return f"gpa_{self.artifact_fingerprint[:16]}"

  def to_dict(self, *, include_fingerprint: bool = True) -> dict[str, Any]:
    """Return the canonical public approval payload."""
    payload = {
      "artifact_type": TARGET_GENERATION_APPROVAL_ARTIFACT_TYPE,
      "artifact_version": TARGET_GENERATION_APPROVAL_ARTIFACT_VERSION,
      "review": _canonicalize(self.review),
      "decision": self.decision.to_dict(),
    }
    if include_fingerprint:
      payload["approval_id"] = self.approval_id
      payload["artifact_fingerprint"] = self.artifact_fingerprint
    return payload


@dataclass(frozen=True)
class TargetGenerationApprovalCheckResult:
  """Result of validating a Generation Approval against a review."""

  is_valid: bool
  status: str
  message: str
  plan_fingerprint: str | None = None
  review_fingerprint: str | None = None
  approval_id: str | None = None
  artifact_fingerprint: str | None = None


class TargetGenerationApprovalStore:
  """File-backed Generation Approval store in Architecture Control storage."""

  def __init__(self, base_path: str | Path | None = None, *, context=None):
    if base_path is not None:
      self.base_path = Path(base_path)
    else:
      from metadata.architecture.paths import resolve_architecture_approval_dir
      self.base_path = resolve_architecture_approval_dir(context=context) / "generation"

  def approval_file(self, review_fingerprint: str) -> Path:
    """Return the artifact path for one generation review fingerprint."""
    fingerprint = _fingerprint(review_fingerprint, label="review")
    return self.base_path / f"{fingerprint}.generation.approval.json"

  def save(self, artifact: TargetGenerationApprovalArtifact) -> Path:
    """Store one Generation Approval by its review fingerprint."""
    review_fingerprint = _fingerprint(
      artifact.review.get("review_fingerprint"),
      label="review",
    )
    path = self.approval_file(review_fingerprint)
    self.save_file(path, artifact)
    return path

  def load_for_review_fingerprint(
    self,
    review_fingerprint: str,
  ) -> TargetGenerationApprovalArtifact | None:
    """Load the matching Generation Approval when present."""
    path = self.approval_file(review_fingerprint)
    if not path.exists():
      return None
    return self.load_file(path)

  @classmethod
  def load_file(cls, path: str | Path) -> TargetGenerationApprovalArtifact:
    """Load and validate one Generation Approval artifact file."""
    artifact_path = Path(path)
    try:
      payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except OSError as exc:
      raise TargetGenerationControlError(
        f"Target Generation approval artifact could not be read: {artifact_path}"
      ) from exc
    except JSONDecodeError as exc:
      raise TargetGenerationControlError(
        f"Target Generation approval artifact JSON is invalid: {artifact_path}"
      ) from exc
    return target_generation_approval_from_dict(payload)

  @classmethod
  def save_file(
    cls,
    path: str | Path,
    artifact: TargetGenerationApprovalArtifact,
  ) -> None:
    """Write one canonical Generation Approval artifact."""
    artifact_path = Path(path)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(
      render_target_generation_approval_json(artifact),
      encoding="utf-8",
    )


def build_target_generation_review(plan: TargetGenerationPlan) -> TargetGenerationReview:
  """Build one deterministic Source-to-Target review from a plan."""
  if not isinstance(plan, TargetGenerationPlan):
    raise TargetGenerationControlError(
      "Target Generation review requires a validated plan."
    )

  dataset_actions: dict[str, list[Any]] = {}
  source_actions: dict[str, list[Any]] = {}
  for action in plan.actions:
    dataset_actions.setdefault(action.dataset_key, []).append(action)
    for source_key in action.source_keys:
      source_actions.setdefault(source_key, []).append(action)

  dataset_impacts = tuple(
    TargetGenerationDatasetImpact(
      dataset_key=dataset_key,
      source_keys=tuple(
        source_key
        for action in actions
        for source_key in action.source_keys
      ),
      action_counts=tuple(
        (action_type, sum(1 for action in actions if action.action_type == action_type))
        for action_type in plan.action_counts
        if any(action.action_type == action_type for action in actions)
      ),
      effect_origins=tuple(action.effect_origin for action in actions),
      change_classification=_highest_classification(
        action.change_classification for action in actions
      ),
    )
    for dataset_key, actions in sorted(dataset_actions.items())
  )

  source_impacts = tuple(
    TargetGenerationSourceImpact(
      source_key=source_key,
      target_dataset_keys=tuple(action.dataset_key for action in actions),
      action_count=len(actions),
      change_classification=_highest_classification(
        action.change_classification for action in actions
      ),
    )
    for source_key, actions in sorted(source_actions.items())
  )

  return TargetGenerationReview(
    plan_fingerprint=plan.plan_fingerprint,
    scope_mode=plan.scope_mode,
    target_schema_short_names=plan.target_schema_short_names,
    source_dataset_keys=plan.source_dataset_keys,
    reconcile_lifecycle=plan.reconcile_lifecycle,
    source_metadata_fingerprint=plan.source_metadata_fingerprint,
    target_metadata_fingerprint=plan.target_metadata_fingerprint,
    action_counts=tuple(plan.action_counts.items()),
    classification_counts=tuple(
      (
        classification,
        sum(1 for action in plan.actions if action.change_classification == classification),
      )
      for classification in _CLASSIFICATION_ORDER
    ),
    effect_origin_counts=tuple(
      (
        origin,
        sum(1 for action in plan.actions if action.effect_origin == origin),
      )
      for origin in _EFFECT_ORIGIN_ORDER
    ),
    dataset_impacts=dataset_impacts,
    source_impacts=source_impacts,
  )


def build_target_generation_approval(
  *,
  review: TargetGenerationReview,
  decided_by: str,
  note: str = "",
  decided_at: str | None = None,
) -> TargetGenerationApprovalArtifact:
  """Build a Generation Approval for one exact reviewed plan."""
  if not isinstance(review, TargetGenerationReview):
    raise TargetGenerationControlError(
      "Target Generation approval requires a validated review."
    )
  if review.action_count == 0:
    raise TargetGenerationControlError(
      "Target Generation approval is not required for a plan without actions."
    )
  return TargetGenerationApprovalArtifact(
    review=_build_review_reference(review),
    decision=TargetGenerationApprovalDecisionRecord(
      decision="approved",
      decided_by=decided_by,
      decided_at=_normalize_decided_at(decided_at),
      note=note,
    ),
  )


def check_target_generation_approval(
  *,
  review: TargetGenerationReview,
  approval: TargetGenerationApprovalArtifact,
) -> TargetGenerationApprovalCheckResult:
  """Validate one Generation Approval against the exact current review."""
  plan_fingerprint = getattr(review, "plan_fingerprint", None)
  review_fingerprint = getattr(review, "review_fingerprint", None)
  approval_id = getattr(approval, "approval_id", None)
  artifact_fingerprint = getattr(approval, "artifact_fingerprint", None)

  try:
    if not isinstance(review, TargetGenerationReview):
      raise TargetGenerationControlError(
        "Generation approval check requires a validated review."
      )
    if not isinstance(approval, TargetGenerationApprovalArtifact):
      raise TargetGenerationControlError(
        "Generation approval check requires a validated approval artifact."
      )
    if approval.review != _build_review_reference(review):
      return TargetGenerationApprovalCheckResult(
        is_valid=False,
        status="drift",
        message=(
          "Target Generation approval is bound to a different review or plan."
        ),
        plan_fingerprint=plan_fingerprint,
        review_fingerprint=review_fingerprint,
        approval_id=approval_id,
        artifact_fingerprint=artifact_fingerprint,
      )
    if approval.decision.decision != "approved":
      return TargetGenerationApprovalCheckResult(
        is_valid=False,
        status=approval.decision.decision,
        message="Target Generation review is not approved.",
        plan_fingerprint=plan_fingerprint,
        review_fingerprint=review_fingerprint,
        approval_id=approval_id,
        artifact_fingerprint=artifact_fingerprint,
      )
    return TargetGenerationApprovalCheckResult(
      is_valid=True,
      status="approved",
      message="Generation Approval matches the exact Target Generation review.",
      plan_fingerprint=plan_fingerprint,
      review_fingerprint=review_fingerprint,
      approval_id=approval_id,
      artifact_fingerprint=artifact_fingerprint,
    )
  except TargetGenerationControlError as exc:
    return TargetGenerationApprovalCheckResult(
      is_valid=False,
      status="invalid",
      message=str(exc),
      plan_fingerprint=plan_fingerprint,
      review_fingerprint=review_fingerprint,
      approval_id=approval_id,
      artifact_fingerprint=artifact_fingerprint,
    )


def target_generation_review_from_dict(value: Mapping[str, Any]) -> TargetGenerationReview:
  """Parse and validate one Target Generation Review payload."""
  payload = _mapping(value, label="Target Generation Review")
  _exact_keys(payload, _REVIEW_KEYS, label="Target Generation Review")
  if payload.get("artifact_type") != TARGET_GENERATION_REVIEW_ARTIFACT_TYPE:
    raise TargetGenerationControlError("Target Generation Review artifact type is invalid.")
  if payload.get("artifact_version") != TARGET_GENERATION_REVIEW_ARTIFACT_VERSION:
    raise TargetGenerationControlError("Unsupported Target Generation Review artifact version.")

  scope = _mapping(payload.get("scope"), label="review scope")
  _exact_keys(
    scope,
    {
      "mode",
      "target_schema_short_names",
      "source_dataset_keys",
      "reconcile_lifecycle",
    },
    label="review scope",
  )
  summary = _mapping(payload.get("summary"), label="review summary")
  _exact_keys(
    summary,
    {
      "action_count",
      "action_counts",
      "classification_counts",
      "effect_origin_counts",
      "target_dataset_count",
      "source_count",
      "has_breaking_changes",
      "approval_recommended",
    },
    label="review summary",
  )
  dataset_impacts = tuple(
    _dataset_impact_from_dict(item)
    for item in _sequence(payload.get("dataset_impacts"), label="dataset impacts")
  )
  source_impacts = tuple(
    _source_impact_from_dict(item)
    for item in _sequence(payload.get("source_impacts"), label="source impacts")
  )

  review = TargetGenerationReview(
    plan_fingerprint=payload.get("plan_fingerprint"),
    scope_mode=scope.get("mode"),
    target_schema_short_names=tuple(_sequence(
      scope.get("target_schema_short_names"),
      label="target schema short names",
    )),
    source_dataset_keys=tuple(_sequence(
      scope.get("source_dataset_keys"),
      label="source dataset keys",
    )),
    reconcile_lifecycle=scope.get("reconcile_lifecycle"),
    source_metadata_fingerprint=payload.get("source_metadata_fingerprint"),
    target_metadata_fingerprint=payload.get("target_metadata_fingerprint"),
    action_counts=tuple(_mapping(summary.get("action_counts"), label="action counts").items()),
    classification_counts=tuple(
      _mapping(summary.get("classification_counts"), label="classification counts").items()
    ),
    effect_origin_counts=tuple(
      _mapping(summary.get("effect_origin_counts"), label="effect origin counts").items()
    ),
    dataset_impacts=dataset_impacts,
    source_impacts=source_impacts,
  )
  expected = _fingerprint(payload.get("review_fingerprint"), label="review")
  if expected != review.review_fingerprint:
    raise TargetGenerationControlError(
      "Target Generation Review fingerprint does not match its payload."
    )
  expected_summary = review.to_dict()["summary"]
  if _canonicalize(summary) != expected_summary:
    raise TargetGenerationControlError(
      "Target Generation Review summary does not match its impacts."
    )
  return review


def target_generation_approval_from_dict(
  value: Mapping[str, Any],
) -> TargetGenerationApprovalArtifact:
  """Parse and validate one Target Generation Approval payload."""
  payload = _mapping(value, label="Target Generation Approval")
  _exact_keys(payload, _APPROVAL_KEYS, label="Target Generation Approval")
  if payload.get("artifact_type") != TARGET_GENERATION_APPROVAL_ARTIFACT_TYPE:
    raise TargetGenerationControlError(
      "Target Generation Approval artifact type is invalid."
    )
  if payload.get("artifact_version") != TARGET_GENERATION_APPROVAL_ARTIFACT_VERSION:
    raise TargetGenerationControlError(
      "Unsupported Target Generation Approval artifact version."
    )
  decision_payload = _mapping(payload.get("decision"), label="approval decision")
  _exact_keys(decision_payload, _DECISION_KEYS, label="approval decision")
  artifact = TargetGenerationApprovalArtifact(
    review=_mapping(payload.get("review"), label="generation review reference"),
    decision=TargetGenerationApprovalDecisionRecord(
      decision=decision_payload.get("decision"),
      decided_by=decision_payload.get("decided_by"),
      decided_at=decision_payload.get("decided_at"),
      note=decision_payload.get("note", ""),
    ),
  )
  expected_fingerprint = _fingerprint(
    payload.get("artifact_fingerprint"),
    label="approval artifact",
  )
  if expected_fingerprint != artifact.artifact_fingerprint:
    raise TargetGenerationControlError(
      "Target Generation approval fingerprint does not match its payload."
    )
  if payload.get("approval_id") != artifact.approval_id:
    raise TargetGenerationControlError(
      "Target Generation approval identifier does not match its fingerprint."
    )
  return artifact


def parse_target_generation_review_json(value: str) -> TargetGenerationReview:
  """Parse one Target Generation Review JSON document."""
  try:
    payload = json.loads(str(value))
  except (TypeError, ValueError) as exc:
    raise TargetGenerationControlError(
      "Target Generation Review JSON is invalid."
    ) from exc
  return target_generation_review_from_dict(payload)


def parse_target_generation_approval_json(
  value: str,
) -> TargetGenerationApprovalArtifact:
  """Parse one Target Generation Approval JSON document."""
  try:
    payload = json.loads(str(value))
  except (TypeError, ValueError) as exc:
    raise TargetGenerationControlError(
      "Target Generation Approval JSON is invalid."
    ) from exc
  return target_generation_approval_from_dict(payload)


def render_target_generation_review_json(review: TargetGenerationReview) -> str:
  """Render one Generation Review as canonical readable JSON."""
  if not isinstance(review, TargetGenerationReview):
    raise TargetGenerationControlError(
      "Target Generation Review rendering requires a validated review."
    )
  return json.dumps(
    review.to_dict(),
    sort_keys=True,
    ensure_ascii=False,
    allow_nan=False,
    indent=2,
  ) + "\n"


def render_target_generation_review_text(review: TargetGenerationReview) -> str:
  """Render one concise Source-to-Target impact preview."""
  lines = [
    "Target Generation Review",
    f"Plan: {review.plan_fingerprint}",
    f"Review: {review.review_fingerprint}",
    (
      "Summary: "
      f"{review.action_count} actions; "
      f"{dict(review.classification_counts)['BREAKING']} breaking; "
      f"{dict(review.classification_counts)['ADDITIVE']} additive; "
      f"{dict(review.classification_counts)['NEUTRAL']} neutral."
    ),
    (
      "Generation approval: "
      + ("recommended" if review.approval_recommended else "optional")
    ),
  ]
  for impact in review.dataset_impacts:
    sources = ", ".join(impact.source_keys) or "lifecycle/current target state"
    lines.append(
      f"- {sources} -> {impact.dataset_key}: {impact.action_count} actions "
      f"[{impact.change_classification}]"
    )
  return "\n".join(lines) + "\n"


def render_target_generation_approval_json(
  approval: TargetGenerationApprovalArtifact,
) -> str:
  """Render one Generation Approval as canonical readable JSON."""
  if not isinstance(approval, TargetGenerationApprovalArtifact):
    raise TargetGenerationControlError(
      "Target Generation Approval rendering requires a validated artifact."
    )
  return json.dumps(
    approval.to_dict(),
    sort_keys=True,
    ensure_ascii=False,
    allow_nan=False,
    indent=2,
  ) + "\n"


def load_plan_and_review(path: str | Path) -> tuple[TargetGenerationPlan, TargetGenerationReview]:
  """Load one canonical plan file and derive its exact review."""
  try:
    text = Path(path).read_text(encoding="utf-8")
  except OSError as exc:
    raise TargetGenerationControlError(
      f"Target Generation Plan could not be read: {path}"
    ) from exc
  plan = parse_target_generation_plan_json(text)
  return plan, build_target_generation_review(plan)


def _build_review_reference(review: TargetGenerationReview) -> dict[str, Any]:
  return {
    "type": TARGET_GENERATION_APPROVAL_REFERENCE_TYPE,
    "review_fingerprint": review.review_fingerprint,
    "plan_fingerprint": review.plan_fingerprint,
    "scope": review.to_dict()["scope"],
    "summary": review.to_dict()["summary"],
  }


def _validate_review_reference(reference: Mapping[str, Any]) -> None:
  expected_keys = {
    "type",
    "review_fingerprint",
    "plan_fingerprint",
    "scope",
    "summary",
  }
  _exact_keys(reference, expected_keys, label="generation review reference")
  if reference.get("type") != TARGET_GENERATION_APPROVAL_REFERENCE_TYPE:
    raise TargetGenerationControlError(
      "Target Generation approval review reference type is invalid."
    )
  _fingerprint(reference.get("review_fingerprint"), label="review")
  _fingerprint(reference.get("plan_fingerprint"), label="plan")
  _mapping(reference.get("scope"), label="review scope")
  _mapping(reference.get("summary"), label="review summary")


def _dataset_impact_from_dict(value: Any) -> TargetGenerationDatasetImpact:
  payload = _mapping(value, label="dataset impact")
  _exact_keys(payload, _DATASET_IMPACT_KEYS, label="dataset impact")
  action_counts = _mapping(payload.get("action_counts"), label="dataset action counts")
  impact = TargetGenerationDatasetImpact(
    dataset_key=payload.get("dataset_key"),
    source_keys=tuple(_sequence(payload.get("source_keys"), label="source keys")),
    action_counts=tuple(action_counts.items()),
    effect_origins=tuple(_sequence(payload.get("effect_origins"), label="effect origins")),
    change_classification=payload.get("change_classification"),
  )
  if payload.get("action_count") != impact.action_count:
    raise TargetGenerationControlError(
      "Target Generation dataset impact action count is invalid."
    )
  return impact


def _source_impact_from_dict(value: Any) -> TargetGenerationSourceImpact:
  payload = _mapping(value, label="source impact")
  _exact_keys(payload, _SOURCE_IMPACT_KEYS, label="source impact")
  return TargetGenerationSourceImpact(
    source_key=payload.get("source_key"),
    target_dataset_keys=tuple(_sequence(
      payload.get("target_dataset_keys"),
      label="target dataset keys",
    )),
    action_count=payload.get("action_count"),
    change_classification=payload.get("change_classification"),
  )


def _highest_classification(values: Sequence[str] | Any) -> str:
  present = set(values)
  for classification in _CLASSIFICATION_ORDER:
    if classification in present:
      return classification
  raise TargetGenerationControlError(
    "Target Generation impact requires at least one classification."
  )


def _classification(value: Any) -> str:
  classification = _required_text(value, label="change classification").upper()
  if classification not in _CLASSIFICATION_ORDER:
    raise TargetGenerationControlError(
      f"Unsupported Target Generation change classification: {classification}"
    )
  return classification


def _normalized_named_counts(value, *, names: Sequence[str], label: str):
  counts = dict(_normalized_counts(value, label=label))
  if set(counts) != set(names):
    raise TargetGenerationControlError(
      f"Target Generation {label} must contain the complete stable key set."
    )
  return tuple((name, counts[name]) for name in names)


def _normalized_counts(value, *, label: str) -> tuple[tuple[str, int], ...]:
  try:
    items = tuple(value or ())
  except TypeError as exc:
    raise TargetGenerationControlError(f"Target Generation {label} are invalid.") from exc
  normalized: list[tuple[str, int]] = []
  seen: set[str] = set()
  for raw_name, raw_count in items:
    name = _required_text(raw_name, label=f"{label} key")
    if name in seen:
      raise TargetGenerationControlError(f"Target Generation {label} contain duplicates.")
    seen.add(name)
    normalized.append((name, _non_negative_int(raw_count, label=f"{name} count")))
  return tuple(sorted(normalized, key=lambda item: item[0]))


def _normalized_choice_tuple(value, *, allowed: Sequence[str], label: str) -> tuple[str, ...]:
  values = set(_normalized_text_tuple(value, label=label))
  unknown = values - set(allowed)
  if unknown:
    raise TargetGenerationControlError(
      f"Unsupported Target Generation {label}: {', '.join(sorted(unknown))}"
    )
  return tuple(item for item in allowed if item in values)


def _normalized_text_tuple(value, *, label: str) -> tuple[str, ...]:
  if isinstance(value, str):
    raw_values = (value,)
  else:
    try:
      raw_values = tuple(value or ())
    except TypeError as exc:
      raise TargetGenerationControlError(f"Target Generation {label} list is invalid.") from exc
  normalized = tuple(sorted({
    _required_text(item, label=label)
    for item in raw_values
  }))
  return normalized


def _required_text(value: Any, *, label: str) -> str:
  text = str(value or "").strip()
  if not text:
    raise TargetGenerationControlError(f"Target Generation {label} is required.")
  return text


def _fingerprint(value: Any, *, label: str) -> str:
  fingerprint = _required_text(value, label=f"{label} fingerprint").lower()
  if not _SHA256_RE.fullmatch(fingerprint):
    raise TargetGenerationControlError(
      f"Target Generation {label} fingerprint must be a SHA-256 value."
    )
  return fingerprint


def _positive_int(value: Any, *, label: str) -> int:
  if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
    raise TargetGenerationControlError(f"Target Generation {label} must be positive.")
  return value


def _non_negative_int(value: Any, *, label: str) -> int:
  if isinstance(value, bool) or not isinstance(value, int) or value < 0:
    raise TargetGenerationControlError(
      f"Target Generation {label} must be a non-negative integer."
    )
  return value


def _mapping(value: Any, *, label: str) -> dict[str, Any]:
  if not isinstance(value, Mapping):
    raise TargetGenerationControlError(f"{label} must be a JSON object.")
  return dict(value)


def _sequence(value: Any, *, label: str) -> tuple[Any, ...]:
  if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
    raise TargetGenerationControlError(f"{label} must be a JSON array.")
  return tuple(value)


def _exact_keys(value: Mapping[str, Any], expected, *, label: str) -> None:
  actual = set(value)
  expected_set = set(expected)
  if actual != expected_set:
    missing = sorted(expected_set - actual)
    extra = sorted(actual - expected_set)
    details = []
    if missing:
      details.append("missing: " + ", ".join(missing))
    if extra:
      details.append("unsupported: " + ", ".join(extra))
    raise TargetGenerationControlError(
      f"{label} fields are invalid ({'; '.join(details)})."
    )


def _canonical_object(value: Mapping[str, Any], *, label: str) -> dict[str, Any]:
  canonical = _canonicalize(value)
  if not isinstance(canonical, dict):
    raise TargetGenerationControlError(f"{label} must be a JSON object.")
  return canonical


def _canonicalize(value: Any) -> Any:
  try:
    return json.loads(json.dumps(
      value,
      sort_keys=True,
      ensure_ascii=False,
      allow_nan=False,
      separators=(",", ":"),
    ))
  except (TypeError, ValueError) as exc:
    raise TargetGenerationControlError(
      "Target Generation control payload is not JSON-compatible."
    ) from exc


def _stable_json_hash(value: Any) -> str:
  payload = json.dumps(
    value,
    sort_keys=True,
    ensure_ascii=False,
    allow_nan=False,
    separators=(",", ":"),
  )
  return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _normalize_decided_at(value: str | None) -> str:
  if value is None:
    dt = datetime.now(timezone.utc)
  else:
    text = str(value).strip()
    if text.endswith("Z"):
      text = text[:-1] + "+00:00"
    try:
      dt = datetime.fromisoformat(text)
    except ValueError as exc:
      raise TargetGenerationControlError(
        "Target Generation approval timestamp is invalid."
      ) from exc
    if dt.tzinfo is None:
      dt = dt.replace(tzinfo=timezone.utc)
    else:
      dt = dt.astimezone(timezone.utc)
  return dt.isoformat().replace("+00:00", "Z")
