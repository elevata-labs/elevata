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
from types import MappingProxyType
from typing import Any

from metadata.promotion.canonical import (
  canonical_json,
  canonical_sha256,
  canonicalize_metadata_value,
)
from metadata.promotion.snapshot import (
  EnvironmentMetadataObject,
  EnvironmentMetadataRelationship,
  EnvironmentMetadataSnapshotError,
)


ENVIRONMENT_PROMOTION_PLAN_ARTIFACT_TYPE = "environment_promotion_plan"
ENVIRONMENT_PROMOTION_PLAN_ARTIFACT_VERSION = 1


class EnvironmentPromotionPlanError(ValueError):
  """
  Raised when an Environment Promotion Plan is structurally invalid.
  """


class EnvironmentPromotionActionType(str, Enum):
  CREATE = "create"
  UPDATE = "update"
  DELETE = "delete"
  DEACTIVATE = "deactivate"
  REACTIVATE = "reactivate"
  RETIRE = "retire"
  VERIFY = "verify"
  ADD_RELATIONSHIP = "add_relationship"
  REMOVE_RELATIONSHIP = "remove_relationship"


class EnvironmentPromotionChangeClass(str, Enum):
  ADDITIVE = "additive"
  MUTATING = "mutating"
  LIFECYCLE = "lifecycle"
  DESTRUCTIVE = "destructive"
  VERIFICATION = "verification"


class EnvironmentPromotionSubjectType(str, Enum):
  OBJECT = "object"
  RELATIONSHIP = "relationship"


class EnvironmentPromotionIssueSeverity(str, Enum):
  ERROR = "error"
  WARNING = "warning"


class EnvironmentPromotionReadinessStatus(str, Enum):
  READY = "ready"
  NO_CHANGES = "no_changes"
  BLOCKED = "blocked"


_ACTION_CHANGE_CLASS = {
  EnvironmentPromotionActionType.CREATE:
    EnvironmentPromotionChangeClass.ADDITIVE,
  EnvironmentPromotionActionType.UPDATE:
    EnvironmentPromotionChangeClass.MUTATING,
  EnvironmentPromotionActionType.DELETE:
    EnvironmentPromotionChangeClass.DESTRUCTIVE,
  EnvironmentPromotionActionType.DEACTIVATE:
    EnvironmentPromotionChangeClass.LIFECYCLE,
  EnvironmentPromotionActionType.REACTIVATE:
    EnvironmentPromotionChangeClass.LIFECYCLE,
  EnvironmentPromotionActionType.RETIRE:
    EnvironmentPromotionChangeClass.LIFECYCLE,
  EnvironmentPromotionActionType.VERIFY:
    EnvironmentPromotionChangeClass.VERIFICATION,
  EnvironmentPromotionActionType.ADD_RELATIONSHIP:
    EnvironmentPromotionChangeClass.ADDITIVE,
  EnvironmentPromotionActionType.REMOVE_RELATIONSHIP:
    EnvironmentPromotionChangeClass.DESTRUCTIVE,
}


@dataclass(frozen=True)
class EnvironmentPromotionIssue:
  """
  One deterministic readiness finding for a promotion plan.
  """
  severity: EnvironmentPromotionIssueSeverity
  code: str
  message: str
  subject_name: str | None = None
  current_key: str | None = None
  desired_key: str | None = None

  def __post_init__(self) -> None:
    object.__setattr__(self, "code", _require_text(self.code, "code"))
    object.__setattr__(
      self,
      "message",
      _require_text(self.message, "message"),
    )
    for field_name in ("subject_name", "current_key", "desired_key"):
      value = getattr(self, field_name)
      if value is not None:
        object.__setattr__(
          self,
          field_name,
          _require_text(value, field_name),
        )

  def to_dict(self) -> dict[str, Any]:
    return {
      "severity": self.severity.value,
      "code": self.code,
      "message": self.message,
      "subject_name": self.subject_name,
      "current_key": self.current_key,
      "desired_key": self.desired_key,
    }

  @classmethod
  def from_dict(cls, data: Mapping[str, Any]) -> EnvironmentPromotionIssue:
    _require_exact_keys(
      data,
      expected={
        "severity",
        "code",
        "message",
        "subject_name",
        "current_key",
        "desired_key",
      },
      label="promotion readiness issue",
    )
    try:
      severity = EnvironmentPromotionIssueSeverity(
        _require_text(data.get("severity"), "severity")
      )
    except ValueError as exc:
      raise EnvironmentPromotionPlanError(
        f"Unsupported promotion issue severity: {data.get('severity')}."
      ) from exc
    return cls(
      severity=severity,
      code=_require_text(data.get("code"), "code"),
      message=_require_text(data.get("message"), "message"),
      subject_name=_optional_text(data.get("subject_name"), "subject_name"),
      current_key=_optional_text(data.get("current_key"), "current_key"),
      desired_key=_optional_text(data.get("desired_key"), "desired_key"),
    )


@dataclass(frozen=True)
class EnvironmentPromotionAction:
  """
  One exact object or relationship operation in a promotion plan.
  """
  action_type: EnvironmentPromotionActionType
  subject_type: EnvironmentPromotionSubjectType
  subject_name: str
  dependency_phase: int
  current: Mapping[str, Any] | None = None
  desired: Mapping[str, Any] | None = None
  changed_fields: tuple[str, ...] = ()
  blocked: bool = False
  message: str = ""

  def __post_init__(self) -> None:
    subject_name = _require_text(self.subject_name, "subject_name")
    object.__setattr__(self, "subject_name", subject_name)
    if not isinstance(self.dependency_phase, int):
      raise EnvironmentPromotionPlanError(
        "Promotion action dependency_phase must be an integer."
      )

    current = _normalize_subject_payload(
      self.current,
      subject_type=self.subject_type,
      subject_name=subject_name,
      label="current",
    )
    desired = _normalize_subject_payload(
      self.desired,
      subject_type=self.subject_type,
      subject_name=subject_name,
      label="desired",
    )
    object.__setattr__(self, "current", current)
    object.__setattr__(self, "desired", desired)
    object.__setattr__(
      self,
      "changed_fields",
      tuple(sorted({
        _require_text(item, "changed field")
        for item in self.changed_fields
      })),
    )
    object.__setattr__(
      self,
      "message",
      _require_text(self.message, "message", allow_empty=True),
    )

    if current is None and desired is None:
      raise EnvironmentPromotionPlanError(
        "Promotion action requires current or desired subject content."
      )
    if self.subject_type == EnvironmentPromotionSubjectType.RELATIONSHIP:
      if self.changed_fields:
        raise EnvironmentPromotionPlanError(
          "Relationship actions must not declare changed fields."
        )
      if self.action_type not in {
        EnvironmentPromotionActionType.ADD_RELATIONSHIP,
        EnvironmentPromotionActionType.REMOVE_RELATIONSHIP,
      }:
        raise EnvironmentPromotionPlanError(
          "Relationship promotion actions must add or remove a relationship."
        )
    elif self.action_type in {
      EnvironmentPromotionActionType.ADD_RELATIONSHIP,
      EnvironmentPromotionActionType.REMOVE_RELATIONSHIP,
    }:
      raise EnvironmentPromotionPlanError(
        "Object promotion actions cannot use relationship action types."
      )

    if self.action_type == EnvironmentPromotionActionType.CREATE:
      _require_absent(current, "CREATE current subject")
      _require_present(desired, "CREATE desired subject")
    elif self.action_type == EnvironmentPromotionActionType.DELETE:
      _require_present(current, "DELETE current subject")
      _require_absent(desired, "DELETE desired subject")
    elif self.action_type == EnvironmentPromotionActionType.ADD_RELATIONSHIP:
      _require_absent(current, "ADD_RELATIONSHIP current subject")
      _require_present(desired, "ADD_RELATIONSHIP desired subject")
    elif self.action_type == EnvironmentPromotionActionType.REMOVE_RELATIONSHIP:
      _require_present(current, "REMOVE_RELATIONSHIP current subject")
      _require_absent(desired, "REMOVE_RELATIONSHIP desired subject")
    elif self.action_type in {
      EnvironmentPromotionActionType.UPDATE,
      EnvironmentPromotionActionType.REACTIVATE,
    }:
      _require_present(current, f"{self.action_type.value} current subject")
      _require_present(desired, f"{self.action_type.value} desired subject")
    elif self.action_type in {
      EnvironmentPromotionActionType.DEACTIVATE,
      EnvironmentPromotionActionType.RETIRE,
    }:
      _require_present(current, f"{self.action_type.value} current subject")

    if self.blocked and self.action_type != EnvironmentPromotionActionType.VERIFY:
      raise EnvironmentPromotionPlanError(
        "Only VERIFY actions may be marked as blocked."
      )

  @property
  def change_class(self) -> EnvironmentPromotionChangeClass:
    return _ACTION_CHANGE_CLASS[self.action_type]

  @property
  def is_mutating(self) -> bool:
    return self.action_type != EnvironmentPromotionActionType.VERIFY

  @property
  def current_key(self) -> str | None:
    return _subject_key(self.current, self.subject_type)

  @property
  def desired_key(self) -> str | None:
    return _subject_key(self.desired, self.subject_type)

  @property
  def action_fingerprint(self) -> str:
    return canonical_sha256(self.to_dict(include_identifier=False))

  @property
  def action_id(self) -> str:
    return f"act-{self.action_fingerprint[:16]}"

  def to_dict(self, *, include_identifier: bool = True) -> dict[str, Any]:
    data = {
      "action_type": self.action_type.value,
      "change_class": self.change_class.value,
      "subject_type": self.subject_type.value,
      "subject_name": self.subject_name,
      "dependency_phase": self.dependency_phase,
      "current_key": self.current_key,
      "desired_key": self.desired_key,
      "changed_fields": list(self.changed_fields),
      "blocked": self.blocked,
      "message": self.message,
      "current": canonicalize_metadata_value(self.current),
      "desired": canonicalize_metadata_value(self.desired),
    }
    if include_identifier:
      data["action_id"] = self.action_id
      data["action_fingerprint"] = self.action_fingerprint
    return data

  @classmethod
  def from_dict(cls, data: Mapping[str, Any]) -> EnvironmentPromotionAction:
    _require_exact_keys(
      data,
      expected={
        "action_id",
        "action_fingerprint",
        "action_type",
        "change_class",
        "subject_type",
        "subject_name",
        "dependency_phase",
        "current_key",
        "desired_key",
        "changed_fields",
        "blocked",
        "message",
        "current",
        "desired",
      },
      label="promotion action",
    )
    try:
      action_type = EnvironmentPromotionActionType(
        _require_text(data.get("action_type"), "action_type")
      )
      subject_type = EnvironmentPromotionSubjectType(
        _require_text(data.get("subject_type"), "subject_type")
      )
    except ValueError as exc:
      raise EnvironmentPromotionPlanError(
        "Unsupported promotion action or subject type."
      ) from exc

    dependency_phase = data.get("dependency_phase")
    if not isinstance(dependency_phase, int):
      raise EnvironmentPromotionPlanError(
        "Promotion action dependency_phase must be an integer."
      )
    blocked = data.get("blocked")
    if not isinstance(blocked, bool):
      raise EnvironmentPromotionPlanError(
        "Promotion action blocked must be a boolean."
      )
    changed_fields = _require_sequence(
      data.get("changed_fields"),
      "changed_fields",
    )
    action = cls(
      action_type=action_type,
      subject_type=subject_type,
      subject_name=_require_text(data.get("subject_name"), "subject_name"),
      dependency_phase=dependency_phase,
      current=_optional_mapping(data.get("current"), "current"),
      desired=_optional_mapping(data.get("desired"), "desired"),
      changed_fields=tuple(
        _require_text(value, "changed field")
        for value in changed_fields
      ),
      blocked=blocked,
      message=_require_text(data.get("message"), "message", allow_empty=True),
    )

    _require_match(
      actual=_require_text(data.get("change_class"), "change_class"),
      expected=action.change_class.value,
      label="promotion action change class",
    )
    _require_match(
      actual=_optional_text(data.get("current_key"), "current_key"),
      expected=action.current_key,
      label="promotion action current key",
    )
    _require_match(
      actual=_optional_text(data.get("desired_key"), "desired_key"),
      expected=action.desired_key,
      label="promotion action desired key",
    )
    _require_match(
      actual=_require_text(
        data.get("action_fingerprint"),
        "action_fingerprint",
      ),
      expected=action.action_fingerprint,
      label="promotion action fingerprint",
    )
    _require_match(
      actual=_require_text(data.get("action_id"), "action_id"),
      expected=action.action_id,
      label="promotion action ID",
    )
    return action


@dataclass(frozen=True)
class EnvironmentPromotionReadiness:
  """
  Environment-level gate derived from plan actions and readiness issues.
  """
  status: EnvironmentPromotionReadinessStatus
  issues: tuple[EnvironmentPromotionIssue, ...] = ()

  def __post_init__(self) -> None:
    object.__setattr__(
      self,
      "issues",
      tuple(sorted(
        self.issues,
        key=lambda item: (
          item.severity.value,
          item.code,
          item.subject_name or "",
          item.current_key or "",
          item.desired_key or "",
          item.message,
        ),
      )),
    )

  @property
  def can_apply(self) -> bool:
    return self.status == EnvironmentPromotionReadinessStatus.READY

  @property
  def error_count(self) -> int:
    return sum(
      1
      for item in self.issues
      if item.severity == EnvironmentPromotionIssueSeverity.ERROR
    )

  @property
  def warning_count(self) -> int:
    return sum(
      1
      for item in self.issues
      if item.severity == EnvironmentPromotionIssueSeverity.WARNING
    )

  def to_dict(self) -> dict[str, Any]:
    return {
      "status": self.status.value,
      "can_apply": self.can_apply,
      "error_count": self.error_count,
      "warning_count": self.warning_count,
      "issues": [item.to_dict() for item in self.issues],
    }

  @classmethod
  def from_dict(cls, data: Mapping[str, Any]) -> EnvironmentPromotionReadiness:
    _require_exact_keys(
      data,
      expected={
        "status",
        "can_apply",
        "error_count",
        "warning_count",
        "issues",
      },
      label="promotion readiness",
    )
    try:
      status = EnvironmentPromotionReadinessStatus(
        _require_text(data.get("status"), "status")
      )
    except ValueError as exc:
      raise EnvironmentPromotionPlanError(
        f"Unsupported promotion readiness status: {data.get('status')}."
      ) from exc
    issues = tuple(
      EnvironmentPromotionIssue.from_dict(
        _require_mapping(item, "promotion readiness issue")
      )
      for item in _require_sequence(data.get("issues"), "issues")
    )
    readiness = cls(status=status, issues=issues)
    _require_match(
      actual=data.get("can_apply"),
      expected=readiness.can_apply,
      label="promotion readiness can_apply",
    )
    _require_match(
      actual=data.get("error_count"),
      expected=readiness.error_count,
      label="promotion readiness error_count",
    )
    _require_match(
      actual=data.get("warning_count"),
      expected=readiness.warning_count,
      label="promotion readiness warning_count",
    )
    return readiness


@dataclass(frozen=True)
class EnvironmentPromotionPlan:
  """
  Immutable plan from one release bundle to one target metadata snapshot.
  """
  release_id: str
  bundle_fingerprint: str
  source_environment_label: str
  source_snapshot_fingerprint: str
  source_metadata_fingerprint: str
  target_environment_label: str
  target_snapshot_fingerprint: str
  target_metadata_fingerprint: str
  created_at: datetime
  actions: tuple[EnvironmentPromotionAction, ...]
  readiness: EnvironmentPromotionReadiness
  artifact_type: str = ENVIRONMENT_PROMOTION_PLAN_ARTIFACT_TYPE
  artifact_version: int = ENVIRONMENT_PROMOTION_PLAN_ARTIFACT_VERSION

  def __post_init__(self) -> None:
    for field_name in (
      "release_id",
      "bundle_fingerprint",
      "source_environment_label",
      "source_snapshot_fingerprint",
      "source_metadata_fingerprint",
      "target_environment_label",
      "target_snapshot_fingerprint",
      "target_metadata_fingerprint",
    ):
      object.__setattr__(
        self,
        field_name,
        _require_text(getattr(self, field_name), field_name),
      )
    if self.artifact_type != ENVIRONMENT_PROMOTION_PLAN_ARTIFACT_TYPE:
      raise EnvironmentPromotionPlanError(
        f"Unsupported promotion plan artifact type: {self.artifact_type}."
      )
    if self.artifact_version != ENVIRONMENT_PROMOTION_PLAN_ARTIFACT_VERSION:
      raise EnvironmentPromotionPlanError(
        "Unsupported promotion plan artifact version: "
        f"{self.artifact_version}."
      )
    if self.created_at.tzinfo is None:
      raise EnvironmentPromotionPlanError(
        "Promotion plan created_at must be timezone-aware."
      )

    ordered_actions = tuple(sorted(
      self.actions,
      key=_action_sort_key,
    ))
    action_ids = [item.action_id for item in ordered_actions]
    duplicates = _duplicates(action_ids)
    if duplicates:
      raise EnvironmentPromotionPlanError(
        "Duplicate promotion actions: " + ", ".join(duplicates)
      )
    object.__setattr__(self, "actions", ordered_actions)

    expected_status = _derive_readiness_status(
      actions=ordered_actions,
      issues=self.readiness.issues,
    )
    if self.readiness.status != expected_status:
      raise EnvironmentPromotionPlanError(
        "Promotion readiness status does not match actions and issues."
      )

  @property
  def mutating_actions(self) -> tuple[EnvironmentPromotionAction, ...]:
    return tuple(item for item in self.actions if item.is_mutating)

  @property
  def plan_fingerprint(self) -> str:
    """
    Return the stable identity of exact inputs, actions and readiness.

    Creation provenance is excluded so rebuilding from unchanged inputs yields
    the same plan fingerprint.
    """
    return canonical_sha256(self.to_dict(
      include_identifiers=False,
      include_provenance=False,
    ))

  @property
  def plan_id(self) -> str:
    return f"plan-{self.plan_fingerprint[:16]}"

  @property
  def summary(self) -> Mapping[str, Any]:
    by_action_type = {
      action_type.value: sum(
        1 for item in self.actions if item.action_type == action_type
      )
      for action_type in EnvironmentPromotionActionType
      if any(item.action_type == action_type for item in self.actions)
    }
    by_change_class = {
      change_class.value: sum(
        1 for item in self.actions if item.change_class == change_class
      )
      for change_class in EnvironmentPromotionChangeClass
      if any(item.change_class == change_class for item in self.actions)
    }
    return MappingProxyType({
      "action_count": len(self.actions),
      "mutating_action_count": len(self.mutating_actions),
      "blocked_action_count": sum(1 for item in self.actions if item.blocked),
      "by_action_type": by_action_type,
      "by_change_class": by_change_class,
    })

  def to_dict(
    self,
    *,
    include_identifiers: bool = True,
    include_provenance: bool = True,
  ) -> dict[str, Any]:
    data = {
      "artifact_type": self.artifact_type,
      "artifact_version": self.artifact_version,
      "release_id": self.release_id,
      "bundle_fingerprint": self.bundle_fingerprint,
      "source_environment_label": self.source_environment_label,
      "source_snapshot_fingerprint": self.source_snapshot_fingerprint,
      "source_metadata_fingerprint": self.source_metadata_fingerprint,
      "target_environment_label": self.target_environment_label,
      "target_snapshot_fingerprint": self.target_snapshot_fingerprint,
      "target_metadata_fingerprint": self.target_metadata_fingerprint,
      "actions": [item.to_dict() for item in self.actions],
      "readiness": self.readiness.to_dict(),
      "summary": canonicalize_metadata_value(self.summary),
    }
    if include_provenance:
      data["created_at"] = _format_datetime(self.created_at)
    if include_identifiers:
      data["plan_id"] = self.plan_id
      data["plan_fingerprint"] = self.plan_fingerprint
    return data

  @classmethod
  def from_dict(cls, data: Mapping[str, Any]) -> EnvironmentPromotionPlan:
    _require_exact_keys(
      data,
      expected={
        "artifact_type",
        "artifact_version",
        "plan_id",
        "plan_fingerprint",
        "release_id",
        "bundle_fingerprint",
        "source_environment_label",
        "source_snapshot_fingerprint",
        "source_metadata_fingerprint",
        "target_environment_label",
        "target_snapshot_fingerprint",
        "target_metadata_fingerprint",
        "created_at",
        "actions",
        "readiness",
        "summary",
      },
      label="Environment Promotion Plan",
    )
    artifact_version = data.get("artifact_version")
    if not isinstance(artifact_version, int):
      raise EnvironmentPromotionPlanError(
        "Promotion plan artifact_version must be an integer."
      )
    plan = cls(
      artifact_type=_require_text(data.get("artifact_type"), "artifact_type"),
      artifact_version=artifact_version,
      release_id=_require_text(data.get("release_id"), "release_id"),
      bundle_fingerprint=_require_text(
        data.get("bundle_fingerprint"),
        "bundle_fingerprint",
      ),
      source_environment_label=_require_text(
        data.get("source_environment_label"),
        "source_environment_label",
      ),
      source_snapshot_fingerprint=_require_text(
        data.get("source_snapshot_fingerprint"),
        "source_snapshot_fingerprint",
      ),
      source_metadata_fingerprint=_require_text(
        data.get("source_metadata_fingerprint"),
        "source_metadata_fingerprint",
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
      created_at=_parse_datetime(data.get("created_at")),
      actions=tuple(
        EnvironmentPromotionAction.from_dict(
          _require_mapping(item, "promotion action")
        )
        for item in _require_sequence(data.get("actions"), "actions")
      ),
      readiness=EnvironmentPromotionReadiness.from_dict(
        _require_mapping(data.get("readiness"), "readiness")
      ),
    )
    _require_match(
      actual=canonicalize_metadata_value(
        _require_mapping(data.get("summary"), "summary")
      ),
      expected=canonicalize_metadata_value(plan.summary),
      label="promotion plan summary",
    )
    _require_match(
      actual=_require_text(
        data.get("plan_fingerprint"),
        "plan_fingerprint",
      ),
      expected=plan.plan_fingerprint,
      label="promotion plan fingerprint",
    )
    _require_match(
      actual=_require_text(data.get("plan_id"), "plan_id"),
      expected=plan.plan_id,
      label="promotion plan ID",
    )
    return plan


def serialize_environment_promotion_plan(
  plan: EnvironmentPromotionPlan,
  *,
  pretty: bool = True,
) -> str:
  """
  Serialize an immutable Environment Promotion Plan.
  """
  if not pretty:
    return canonical_json(plan.to_dict())
  return json.dumps(
    canonicalize_metadata_value(plan.to_dict()),
    sort_keys=True,
    ensure_ascii=False,
    indent=2,
    allow_nan=False,
  ) + "\n"


def deserialize_environment_promotion_plan(
  payload: str,
) -> EnvironmentPromotionPlan:
  """
  Deserialize and validate an Environment Promotion Plan JSON document.
  """
  try:
    data = json.loads(payload)
  except JSONDecodeError as exc:
    raise EnvironmentPromotionPlanError(
      f"Environment Promotion Plan is not valid JSON: {exc}."
    ) from exc
  return EnvironmentPromotionPlan.from_dict(
    _require_mapping(data, "Environment Promotion Plan")
  )


def render_environment_promotion_plan_text(
  plan: EnvironmentPromotionPlan,
) -> str:
  """
  Render a compact deterministic human review representation.
  """
  lines = [
    f"Environment Promotion Plan {plan.plan_id}",
    f"Release: {plan.release_id}",
    (
      "Route: "
      f"{plan.source_environment_label} -> {plan.target_environment_label}"
    ),
    f"Readiness: {plan.readiness.status.value}",
    (
      "Actions: "
      f"{plan.summary['action_count']} total, "
      f"{plan.summary['mutating_action_count']} mutating, "
      f"{plan.summary['blocked_action_count']} blocked"
    ),
  ]
  if plan.readiness.issues:
    lines.append("Readiness issues:")
    for issue in plan.readiness.issues:
      subject = f" [{issue.subject_name}]" if issue.subject_name else ""
      lines.append(
        f"  - {issue.severity.value.upper()} {issue.code}{subject}: "
        f"{issue.message}"
      )
  if plan.actions:
    lines.append("Actions:")
    for action in plan.actions:
      key = action.desired_key or action.current_key or "?"
      blocked = " BLOCKED" if action.blocked else ""
      changed = (
        " fields=" + ",".join(action.changed_fields)
        if action.changed_fields
        else ""
      )
      lines.append(
        f"  - {action.action_type.value.upper()}{blocked} "
        f"{action.subject_name} {key}{changed}"
      )
  return "\n".join(lines) + "\n"


def derive_environment_promotion_readiness(
  *,
  actions: Sequence[EnvironmentPromotionAction],
  issues: Sequence[EnvironmentPromotionIssue],
) -> EnvironmentPromotionReadiness:
  """
  Build the environment readiness gate from deterministic planner output.
  """
  return EnvironmentPromotionReadiness(
    status=_derive_readiness_status(actions=actions, issues=issues),
    issues=tuple(issues),
  )


def _derive_readiness_status(
  *,
  actions: Sequence[EnvironmentPromotionAction],
  issues: Sequence[EnvironmentPromotionIssue],
) -> EnvironmentPromotionReadinessStatus:
  if any(
    item.severity == EnvironmentPromotionIssueSeverity.ERROR
    for item in issues
  ):
    return EnvironmentPromotionReadinessStatus.BLOCKED
  if any(item.is_mutating for item in actions):
    return EnvironmentPromotionReadinessStatus.READY
  return EnvironmentPromotionReadinessStatus.NO_CHANGES


def _action_sort_key(action: EnvironmentPromotionAction) -> tuple[Any, ...]:
  negative = action.action_type in {
    EnvironmentPromotionActionType.DELETE,
    EnvironmentPromotionActionType.DEACTIVATE,
    EnvironmentPromotionActionType.RETIRE,
    EnvironmentPromotionActionType.REMOVE_RELATIONSHIP,
  }
  group = (
    0
    if action.action_type == EnvironmentPromotionActionType.VERIFY
    else (2 if negative else 1)
  )
  phase = -action.dependency_phase if negative else action.dependency_phase
  rank = {
    EnvironmentPromotionActionType.VERIFY: 0,
    EnvironmentPromotionActionType.CREATE: 10,
    EnvironmentPromotionActionType.UPDATE: 20,
    EnvironmentPromotionActionType.REACTIVATE: 30,
    EnvironmentPromotionActionType.ADD_RELATIONSHIP: 40,
    EnvironmentPromotionActionType.REMOVE_RELATIONSHIP: 10,
    EnvironmentPromotionActionType.DEACTIVATE: 20,
    EnvironmentPromotionActionType.RETIRE: 30,
    EnvironmentPromotionActionType.DELETE: 40,
  }[action.action_type]
  return (
    group,
    phase,
    rank,
    action.subject_name,
    action.desired_key or action.current_key or "",
    action.action_id,
  )


def _normalize_subject_payload(
  value: Mapping[str, Any] | None,
  *,
  subject_type: EnvironmentPromotionSubjectType,
  subject_name: str,
  label: str,
) -> Mapping[str, Any] | None:
  if value is None:
    return None
  if not isinstance(value, Mapping):
    raise EnvironmentPromotionPlanError(
      f"Promotion action {label} subject must be an object or null."
    )
  try:
    if subject_type == EnvironmentPromotionSubjectType.OBJECT:
      subject = EnvironmentMetadataObject.from_dict(value)
      if subject.model_name != subject_name:
        raise EnvironmentPromotionPlanError(
          f"Promotion action {label} model does not match subject_name."
        )
    else:
      subject = EnvironmentMetadataRelationship.from_dict(value)
      if subject.relationship_name != subject_name:
        raise EnvironmentPromotionPlanError(
          f"Promotion action {label} relationship does not match subject_name."
        )
  except EnvironmentMetadataSnapshotError as exc:
    raise EnvironmentPromotionPlanError(str(exc)) from exc
  return _freeze_mapping(subject.to_dict())


def _subject_key(
  value: Mapping[str, Any] | None,
  subject_type: EnvironmentPromotionSubjectType,
) -> str | None:
  if value is None:
    return None
  key_name = (
    "object_key"
    if subject_type == EnvironmentPromotionSubjectType.OBJECT
    else "relationship_key"
  )
  return str(value[key_name])


def _freeze_mapping(value: Mapping[str, Any]) -> Mapping[str, Any]:
  return MappingProxyType({
    str(key): _freeze_value(item)
    for key, item in canonicalize_metadata_value(value).items()
  })


def _freeze_value(value: Any) -> Any:
  if isinstance(value, Mapping):
    return MappingProxyType({
      str(key): _freeze_value(item)
      for key, item in value.items()
    })
  if isinstance(value, list):
    return tuple(_freeze_value(item) for item in value)
  return value


def _format_datetime(value: datetime) -> str:
  return (
    value.astimezone(timezone.utc)
    .isoformat(timespec="microseconds")
    .replace("+00:00", "Z")
  )


def _parse_datetime(value: Any) -> datetime:
  text = _require_text(value, "created_at")
  if text.endswith("Z"):
    text = text[:-1] + "+00:00"
  try:
    parsed = datetime.fromisoformat(text)
  except ValueError as exc:
    raise EnvironmentPromotionPlanError(
      f"created_at is not a valid ISO timestamp: {value}."
    ) from exc
  if parsed.tzinfo is None:
    raise EnvironmentPromotionPlanError(
      "Promotion plan created_at must contain a timezone."
    )
  return parsed


def _require_text(
  value: Any,
  label: str,
  *,
  allow_empty: bool = False,
) -> str:
  if not isinstance(value, str):
    raise EnvironmentPromotionPlanError(f"{label} must be text.")
  normalized = value.strip()
  if not normalized and not allow_empty:
    raise EnvironmentPromotionPlanError(f"{label} must not be empty.")
  return normalized


def _optional_text(value: Any, label: str) -> str | None:
  if value is None:
    return None
  return _require_text(value, label)


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
  if not isinstance(value, Mapping):
    raise EnvironmentPromotionPlanError(f"{label} must be an object.")
  return value


def _optional_mapping(
  value: Any,
  label: str,
) -> Mapping[str, Any] | None:
  if value is None:
    return None
  return _require_mapping(value, label)


def _require_sequence(value: Any, label: str) -> Sequence[Any]:
  if (
    not isinstance(value, Sequence)
    or isinstance(value, (str, bytes, bytearray))
  ):
    raise EnvironmentPromotionPlanError(f"{label} must be an array.")
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
  raise EnvironmentPromotionPlanError(
    f"{label} keys are invalid: " + "; ".join(details)
  )


def _require_match(*, actual: Any, expected: Any, label: str) -> None:
  if actual != expected:
    raise EnvironmentPromotionPlanError(f"{label} mismatch.")


def _require_present(value: Any, label: str) -> None:
  if value is None:
    raise EnvironmentPromotionPlanError(f"{label} is required.")


def _require_absent(value: Any, label: str) -> None:
  if value is not None:
    raise EnvironmentPromotionPlanError(f"{label} must be null.")


def _duplicates(values: Sequence[str]) -> list[str]:
  counts: dict[str, int] = {}
  for value in values:
    counts[value] = counts.get(value, 0) + 1
  return sorted(value for value, count in counts.items() if count > 1)
