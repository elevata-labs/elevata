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
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from json import JSONDecodeError
import json
from typing import Any

from metadata.promotion.canonical import (
  canonical_json,
  canonical_sha256,
  canonicalize_metadata_value,
)
from metadata.promotion.plan import (
  EnvironmentPromotionPlan,
  EnvironmentPromotionReadinessStatus,
)


ENVIRONMENT_PROMOTION_APPROVAL_ARTIFACT_TYPE = (
  "environment_promotion_approval"
)
ENVIRONMENT_PROMOTION_APPROVAL_ARTIFACT_VERSION = 1


class EnvironmentPromotionApprovalError(ValueError):
  """
  Raised when an Environment Promotion Approval Artifact is invalid.
  """


class EnvironmentPromotionApprovalDecision(str, Enum):
  APPROVED = "approved"
  REJECTED = "rejected"


@dataclass(frozen=True)
class EnvironmentPromotionPlanReference:
  """
  Exact immutable plan identity reviewed by an approver.
  """
  plan_id: str
  plan_fingerprint: str
  release_id: str
  bundle_fingerprint: str
  source_environment_label: str
  target_environment_label: str
  target_snapshot_fingerprint: str
  target_metadata_fingerprint: str
  mutating_action_ids: tuple[str, ...]

  def __post_init__(self) -> None:
    for field_name in (
      "plan_id",
      "plan_fingerprint",
      "release_id",
      "bundle_fingerprint",
      "source_environment_label",
      "target_environment_label",
      "target_snapshot_fingerprint",
      "target_metadata_fingerprint",
    ):
      object.__setattr__(
        self,
        field_name,
        _require_text(getattr(self, field_name), field_name),
      )
    object.__setattr__(
      self,
      "mutating_action_ids",
      tuple(
        _require_text(item, "mutating action ID")
        for item in self.mutating_action_ids
      ),
    )
    if len(set(self.mutating_action_ids)) != len(self.mutating_action_ids):
      raise EnvironmentPromotionApprovalError(
        "Promotion approval contains duplicate mutating action IDs."
      )

  def to_dict(self) -> dict[str, Any]:
    return {
      "plan_id": self.plan_id,
      "plan_fingerprint": self.plan_fingerprint,
      "release_id": self.release_id,
      "bundle_fingerprint": self.bundle_fingerprint,
      "source_environment_label": self.source_environment_label,
      "target_environment_label": self.target_environment_label,
      "target_snapshot_fingerprint": self.target_snapshot_fingerprint,
      "target_metadata_fingerprint": self.target_metadata_fingerprint,
      "mutating_action_ids": list(self.mutating_action_ids),
    }

  @classmethod
  def from_plan(
    cls,
    plan: EnvironmentPromotionPlan,
  ) -> EnvironmentPromotionPlanReference:
    return cls(
      plan_id=plan.plan_id,
      plan_fingerprint=plan.plan_fingerprint,
      release_id=plan.release_id,
      bundle_fingerprint=plan.bundle_fingerprint,
      source_environment_label=plan.source_environment_label,
      target_environment_label=plan.target_environment_label,
      target_snapshot_fingerprint=plan.target_snapshot_fingerprint,
      target_metadata_fingerprint=plan.target_metadata_fingerprint,
      mutating_action_ids=tuple(
        item.action_id
        for item in plan.mutating_actions
      ),
    )

  @classmethod
  def from_dict(
    cls,
    data: Mapping[str, Any],
  ) -> EnvironmentPromotionPlanReference:
    _require_exact_keys(
      data,
      expected={
        "plan_id",
        "plan_fingerprint",
        "release_id",
        "bundle_fingerprint",
        "source_environment_label",
        "target_environment_label",
        "target_snapshot_fingerprint",
        "target_metadata_fingerprint",
        "mutating_action_ids",
      },
      label="promotion plan reference",
    )
    return cls(
      plan_id=_require_text(data.get("plan_id"), "plan_id"),
      plan_fingerprint=_require_text(
        data.get("plan_fingerprint"),
        "plan_fingerprint",
      ),
      release_id=_require_text(data.get("release_id"), "release_id"),
      bundle_fingerprint=_require_text(
        data.get("bundle_fingerprint"),
        "bundle_fingerprint",
      ),
      source_environment_label=_require_text(
        data.get("source_environment_label"),
        "source_environment_label",
      ),
      target_environment_label=_require_text(
        data.get("target_environment_label"),
        "target_environment_label",
      ),
      target_snapshot_fingerprint=_require_text(
        data.get("target_snapshot_fingerprint"),
        "target_snapshot_fingerprint",
      ),
      target_metadata_fingerprint=_require_text(
        data.get("target_metadata_fingerprint"),
        "target_metadata_fingerprint",
      ),
      mutating_action_ids=tuple(
        _require_text(item, "mutating action ID")
        for item in _require_sequence(
          data.get("mutating_action_ids"),
          "mutating_action_ids",
        )
      ),
    )


@dataclass(frozen=True)
class EnvironmentPromotionApprovalReview:
  """
  Human decision and provenance for one exact promotion plan.
  """
  decision: EnvironmentPromotionApprovalDecision
  decided_by: str
  decided_at: datetime
  note: str = ""

  def __post_init__(self) -> None:
    try:
      decision = EnvironmentPromotionApprovalDecision(self.decision)
    except ValueError as exc:
      raise EnvironmentPromotionApprovalError(
        f"Unsupported promotion approval decision: {self.decision}."
      ) from exc
    object.__setattr__(self, "decision", decision)
    object.__setattr__(
      self,
      "decided_by",
      _require_text(self.decided_by, "decided_by"),
    )
    if self.decided_at.tzinfo is None:
      raise EnvironmentPromotionApprovalError(
        "Promotion approval decided_at must be timezone-aware."
      )
    object.__setattr__(
      self,
      "note",
      _require_text(self.note, "note", allow_empty=True),
    )

  def to_dict(self) -> dict[str, Any]:
    return {
      "decision": self.decision.value,
      "decided_by": self.decided_by,
      "decided_at": _format_datetime(self.decided_at),
      "note": self.note,
    }

  @classmethod
  def from_dict(
    cls,
    data: Mapping[str, Any],
  ) -> EnvironmentPromotionApprovalReview:
    _require_exact_keys(
      data,
      expected={"decision", "decided_by", "decided_at", "note"},
      label="promotion approval review",
    )
    try:
      decision = EnvironmentPromotionApprovalDecision(
        _require_text(data.get("decision"), "decision")
      )
    except ValueError as exc:
      raise EnvironmentPromotionApprovalError(
        f"Unsupported promotion approval decision: {data.get('decision')}."
      ) from exc
    return cls(
      decision=decision,
      decided_by=_require_text(data.get("decided_by"), "decided_by"),
      decided_at=_parse_datetime(data.get("decided_at")),
      note=_require_text(data.get("note"), "note", allow_empty=True),
    )


@dataclass(frozen=True)
class EnvironmentPromotionApprovalArtifact:
  """
  Immutable approval or rejection bound to one exact promotion plan.
  """
  plan: EnvironmentPromotionPlanReference
  review: EnvironmentPromotionApprovalReview
  artifact_type: str = ENVIRONMENT_PROMOTION_APPROVAL_ARTIFACT_TYPE
  artifact_version: int = ENVIRONMENT_PROMOTION_APPROVAL_ARTIFACT_VERSION

  def __post_init__(self) -> None:
    if self.artifact_type != ENVIRONMENT_PROMOTION_APPROVAL_ARTIFACT_TYPE:
      raise EnvironmentPromotionApprovalError(
        f"Unsupported promotion approval artifact type: {self.artifact_type}."
      )
    if self.artifact_version != ENVIRONMENT_PROMOTION_APPROVAL_ARTIFACT_VERSION:
      raise EnvironmentPromotionApprovalError(
        "Unsupported promotion approval artifact version: "
        f"{self.artifact_version}."
      )

  @property
  def artifact_fingerprint(self) -> str:
    return canonical_sha256(self.to_dict(include_identifiers=False))

  @property
  def approval_id(self) -> str:
    return f"papr-{self.artifact_fingerprint[:16]}"

  def to_dict(self, *, include_identifiers: bool = True) -> dict[str, Any]:
    data = {
      "artifact_type": self.artifact_type,
      "artifact_version": self.artifact_version,
      "plan": self.plan.to_dict(),
      "review": self.review.to_dict(),
    }
    if include_identifiers:
      data["approval_id"] = self.approval_id
      data["artifact_fingerprint"] = self.artifact_fingerprint
    return data

  @classmethod
  def from_dict(
    cls,
    data: Mapping[str, Any],
  ) -> EnvironmentPromotionApprovalArtifact:
    _require_exact_keys(
      data,
      expected={
        "artifact_type",
        "artifact_version",
        "approval_id",
        "artifact_fingerprint",
        "plan",
        "review",
      },
      label="Environment Promotion Approval Artifact",
    )
    artifact_version = data.get("artifact_version")
    if not isinstance(artifact_version, int):
      raise EnvironmentPromotionApprovalError(
        "Promotion approval artifact_version must be an integer."
      )
    artifact = cls(
      artifact_type=_require_text(data.get("artifact_type"), "artifact_type"),
      artifact_version=artifact_version,
      plan=EnvironmentPromotionPlanReference.from_dict(
        _require_mapping(data.get("plan"), "plan")
      ),
      review=EnvironmentPromotionApprovalReview.from_dict(
        _require_mapping(data.get("review"), "review")
      ),
    )
    _require_match(
      actual=_require_text(
        data.get("artifact_fingerprint"),
        "artifact_fingerprint",
      ),
      expected=artifact.artifact_fingerprint,
      label="promotion approval artifact fingerprint",
    )
    _require_match(
      actual=_require_text(data.get("approval_id"), "approval_id"),
      expected=artifact.approval_id,
      label="promotion approval ID",
    )
    return artifact


@dataclass(frozen=True)
class EnvironmentPromotionApprovalCheckResult:
  """
  Result of binding one approval artifact to one current promotion plan.
  """
  is_valid: bool
  status: str
  message: str
  plan_id: str
  plan_fingerprint: str
  approval_id: str | None = None
  artifact_fingerprint: str | None = None

  def __post_init__(self) -> None:
    object.__setattr__(self, "status", _require_text(self.status, "status"))
    object.__setattr__(
      self,
      "message",
      _require_text(self.message, "message"),
    )
    object.__setattr__(self, "plan_id", _require_text(self.plan_id, "plan_id"))
    object.__setattr__(
      self,
      "plan_fingerprint",
      _require_text(self.plan_fingerprint, "plan_fingerprint"),
    )

  def to_dict(self) -> dict[str, Any]:
    return {
      "is_valid": self.is_valid,
      "status": self.status,
      "message": self.message,
      "plan_id": self.plan_id,
      "plan_fingerprint": self.plan_fingerprint,
      "approval_id": self.approval_id,
      "artifact_fingerprint": self.artifact_fingerprint,
    }


def build_environment_promotion_approval(
  *,
  plan: EnvironmentPromotionPlan,
  decided_by: str,
  note: str = "",
  decided_at: datetime | None = None,
  decision: EnvironmentPromotionApprovalDecision = (
    EnvironmentPromotionApprovalDecision.APPROVED
  ),
) -> EnvironmentPromotionApprovalArtifact:
  """
  Build a review artifact for one exact immutable promotion plan.
  """
  if not isinstance(plan, EnvironmentPromotionPlan):
    raise EnvironmentPromotionApprovalError(
      "plan must be an EnvironmentPromotionPlan."
    )
  if (
    decision == EnvironmentPromotionApprovalDecision.APPROVED
    and plan.readiness.status != EnvironmentPromotionReadinessStatus.READY
  ):
    raise EnvironmentPromotionApprovalError(
      "Only a ready Environment Promotion Plan can be approved."
    )
  return EnvironmentPromotionApprovalArtifact(
    plan=EnvironmentPromotionPlanReference.from_plan(plan),
    review=EnvironmentPromotionApprovalReview(
      decision=decision,
      decided_by=decided_by,
      decided_at=decided_at or datetime.now(timezone.utc),
      note=note,
    ),
  )


def check_environment_promotion_approval(
  *,
  plan: EnvironmentPromotionPlan,
  approval: EnvironmentPromotionApprovalArtifact | None,
) -> EnvironmentPromotionApprovalCheckResult:
  """
  Verify decision, plan identity and exact mutating action coverage.
  """
  if approval is None:
    return EnvironmentPromotionApprovalCheckResult(
      is_valid=False,
      status="missing",
      message="No Environment Promotion Approval Artifact is available.",
      plan_id=plan.plan_id,
      plan_fingerprint=plan.plan_fingerprint,
    )

  if approval.review.decision != EnvironmentPromotionApprovalDecision.APPROVED:
    return EnvironmentPromotionApprovalCheckResult(
      is_valid=False,
      status="rejected",
      message="The Environment Promotion Plan was rejected.",
      plan_id=plan.plan_id,
      plan_fingerprint=plan.plan_fingerprint,
      approval_id=approval.approval_id,
      artifact_fingerprint=approval.artifact_fingerprint,
    )

  expected = EnvironmentPromotionPlanReference.from_plan(plan)
  if approval.plan != expected:
    return EnvironmentPromotionApprovalCheckResult(
      is_valid=False,
      status="plan_mismatch",
      message=(
        "The Environment Promotion Approval Artifact does not match the exact "
        "current plan."
      ),
      plan_id=plan.plan_id,
      plan_fingerprint=plan.plan_fingerprint,
      approval_id=approval.approval_id,
      artifact_fingerprint=approval.artifact_fingerprint,
    )

  return EnvironmentPromotionApprovalCheckResult(
    is_valid=True,
    status="approved",
    message="The exact Environment Promotion Plan is approved.",
    plan_id=plan.plan_id,
    plan_fingerprint=plan.plan_fingerprint,
    approval_id=approval.approval_id,
    artifact_fingerprint=approval.artifact_fingerprint,
  )


def require_environment_promotion_approval(
  *,
  plan: EnvironmentPromotionPlan,
  approval: EnvironmentPromotionApprovalArtifact | None,
) -> EnvironmentPromotionApprovalArtifact:
  """
  Return the approval or raise a precise guarded-apply error.
  """
  result = check_environment_promotion_approval(
    plan=plan,
    approval=approval,
  )
  if not result.is_valid or approval is None:
    raise EnvironmentPromotionApprovalError(result.message)
  return approval


def serialize_environment_promotion_approval(
  approval: EnvironmentPromotionApprovalArtifact,
  *,
  pretty: bool = True,
) -> str:
  if not pretty:
    return canonical_json(approval.to_dict())
  return json.dumps(
    canonicalize_metadata_value(approval.to_dict()),
    sort_keys=True,
    ensure_ascii=False,
    indent=2,
    allow_nan=False,
  ) + "\n"


def deserialize_environment_promotion_approval(
  payload: str,
) -> EnvironmentPromotionApprovalArtifact:
  try:
    data = json.loads(payload)
  except JSONDecodeError as exc:
    raise EnvironmentPromotionApprovalError(
      f"Environment Promotion Approval Artifact is not valid JSON: {exc}."
    ) from exc
  return EnvironmentPromotionApprovalArtifact.from_dict(
    _require_mapping(data, "Environment Promotion Approval Artifact")
  )


def _format_datetime(value: datetime) -> str:
  return (
    value.astimezone(timezone.utc)
    .isoformat(timespec="microseconds")
    .replace("+00:00", "Z")
  )


def _parse_datetime(value: Any) -> datetime:
  text = _require_text(value, "datetime")
  try:
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
  except ValueError as exc:
    raise EnvironmentPromotionApprovalError(
      f"Invalid promotion approval datetime: {value}."
    ) from exc
  if parsed.tzinfo is None:
    raise EnvironmentPromotionApprovalError(
      "Promotion approval datetime must include a timezone."
    )
  return parsed


def _require_text(
  value: Any,
  label: str,
  *,
  allow_empty: bool = False,
) -> str:
  if not isinstance(value, str):
    raise EnvironmentPromotionApprovalError(f"{label} must be a string.")
  normalized = value.strip()
  if not allow_empty and not normalized:
    raise EnvironmentPromotionApprovalError(f"{label} must not be empty.")
  return normalized if not allow_empty else value


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
  if not isinstance(value, Mapping):
    raise EnvironmentPromotionApprovalError(f"{label} must be an object.")
  return value


def _require_sequence(value: Any, label: str) -> Sequence[Any]:
  if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
    raise EnvironmentPromotionApprovalError(f"{label} must be an array.")
  return value


def _require_exact_keys(
  data: Mapping[str, Any],
  *,
  expected: set[str],
  label: str,
) -> None:
  actual = {str(key) for key in data}
  if actual == expected:
    return
  missing = sorted(expected - actual)
  unexpected = sorted(actual - expected)
  details = []
  if missing:
    details.append("missing: " + ", ".join(missing))
  if unexpected:
    details.append("unexpected: " + ", ".join(unexpected))
  raise EnvironmentPromotionApprovalError(
    f"{label} fields are invalid: {'; '.join(details)}."
  )


def _require_match(*, actual: Any, expected: Any, label: str) -> None:
  if actual != expected:
    raise EnvironmentPromotionApprovalError(f"{label} mismatch.")
