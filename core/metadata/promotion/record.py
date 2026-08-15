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
  EnvironmentPromotionAction,
  EnvironmentPromotionActionType,
  EnvironmentPromotionSubjectType,
)


ENVIRONMENT_PROMOTION_RECORD_ARTIFACT_TYPE = "environment_promotion_record"
ENVIRONMENT_PROMOTION_RECORD_ARTIFACT_VERSION = 1


class EnvironmentPromotionRecordError(ValueError):
  """
  Raised when an Environment Promotion Record is structurally invalid.
  """


class EnvironmentPromotionActionResultType(str, Enum):
  APPLIED = "applied"
  VERIFIED = "verified"


@dataclass(frozen=True)
class EnvironmentPromotionActionResult:
  """
  Successful exact consumption evidence for one planned action.
  """
  action_id: str
  action_fingerprint: str
  action_type: EnvironmentPromotionActionType
  subject_type: EnvironmentPromotionSubjectType
  subject_name: str
  subject_key: str
  result: EnvironmentPromotionActionResultType

  def __post_init__(self) -> None:
    for field_name in (
      "action_id",
      "action_fingerprint",
      "subject_name",
      "subject_key",
    ):
      object.__setattr__(
        self,
        field_name,
        _require_text(getattr(self, field_name), field_name),
      )
    expected_result = (
      EnvironmentPromotionActionResultType.VERIFIED
      if self.action_type == EnvironmentPromotionActionType.VERIFY
      else EnvironmentPromotionActionResultType.APPLIED
    )
    if self.result != expected_result:
      raise EnvironmentPromotionRecordError(
        "Promotion action result does not match its action type."
      )

  @classmethod
  def from_action(
    cls,
    action: EnvironmentPromotionAction,
  ) -> EnvironmentPromotionActionResult:
    return cls(
      action_id=action.action_id,
      action_fingerprint=action.action_fingerprint,
      action_type=action.action_type,
      subject_type=action.subject_type,
      subject_name=action.subject_name,
      subject_key=action.desired_key or action.current_key or "unknown",
      result=(
        EnvironmentPromotionActionResultType.VERIFIED
        if action.action_type == EnvironmentPromotionActionType.VERIFY
        else EnvironmentPromotionActionResultType.APPLIED
      ),
    )

  def to_dict(self) -> dict[str, Any]:
    return {
      "action_id": self.action_id,
      "action_fingerprint": self.action_fingerprint,
      "action_type": self.action_type.value,
      "subject_type": self.subject_type.value,
      "subject_name": self.subject_name,
      "subject_key": self.subject_key,
      "result": self.result.value,
    }

  @classmethod
  def from_dict(
    cls,
    data: Mapping[str, Any],
  ) -> EnvironmentPromotionActionResult:
    _require_exact_keys(
      data,
      expected={
        "action_id",
        "action_fingerprint",
        "action_type",
        "subject_type",
        "subject_name",
        "subject_key",
        "result",
      },
      label="promotion action result",
    )
    try:
      action_type = EnvironmentPromotionActionType(
        _require_text(data.get("action_type"), "action_type")
      )
      subject_type = EnvironmentPromotionSubjectType(
        _require_text(data.get("subject_type"), "subject_type")
      )
      result = EnvironmentPromotionActionResultType(
        _require_text(data.get("result"), "result")
      )
    except ValueError as exc:
      raise EnvironmentPromotionRecordError(
        "Unsupported promotion action result value."
      ) from exc
    return cls(
      action_id=_require_text(data.get("action_id"), "action_id"),
      action_fingerprint=_require_text(
        data.get("action_fingerprint"),
        "action_fingerprint",
      ),
      action_type=action_type,
      subject_type=subject_type,
      subject_name=_require_text(data.get("subject_name"), "subject_name"),
      subject_key=_require_text(data.get("subject_key"), "subject_key"),
      result=result,
    )


@dataclass(frozen=True)
class EnvironmentPromotionRecord:
  """
  Immutable local evidence of one successful guarded metadata apply.
  """
  plan_id: str
  plan_fingerprint: str
  approval_id: str
  approval_fingerprint: str
  release_id: str
  bundle_fingerprint: str
  source_environment_label: str
  target_environment_label: str
  pre_target_snapshot_fingerprint: str
  pre_target_metadata_fingerprint: str
  post_target_snapshot_fingerprint: str
  post_target_metadata_fingerprint: str
  post_validation_plan_id: str
  post_validation_plan_fingerprint: str
  applied_at: datetime
  applied_by: str
  action_results: tuple[EnvironmentPromotionActionResult, ...]
  artifact_type: str = ENVIRONMENT_PROMOTION_RECORD_ARTIFACT_TYPE
  artifact_version: int = ENVIRONMENT_PROMOTION_RECORD_ARTIFACT_VERSION

  def __post_init__(self) -> None:
    for field_name in (
      "plan_id",
      "plan_fingerprint",
      "approval_id",
      "approval_fingerprint",
      "release_id",
      "bundle_fingerprint",
      "source_environment_label",
      "target_environment_label",
      "pre_target_snapshot_fingerprint",
      "pre_target_metadata_fingerprint",
      "post_target_snapshot_fingerprint",
      "post_target_metadata_fingerprint",
      "post_validation_plan_id",
      "post_validation_plan_fingerprint",
      "applied_by",
    ):
      object.__setattr__(
        self,
        field_name,
        _require_text(getattr(self, field_name), field_name),
      )
    if self.artifact_type != ENVIRONMENT_PROMOTION_RECORD_ARTIFACT_TYPE:
      raise EnvironmentPromotionRecordError(
        f"Unsupported promotion record artifact type: {self.artifact_type}."
      )
    if self.artifact_version != ENVIRONMENT_PROMOTION_RECORD_ARTIFACT_VERSION:
      raise EnvironmentPromotionRecordError(
        "Unsupported promotion record artifact version: "
        f"{self.artifact_version}."
      )
    if self.applied_at.tzinfo is None:
      raise EnvironmentPromotionRecordError(
        "Promotion record applied_at must be timezone-aware."
      )
    ordered_results = tuple(sorted(
      self.action_results,
      key=lambda item: item.action_id,
    ))
    action_ids = [item.action_id for item in ordered_results]
    if len(set(action_ids)) != len(action_ids):
      raise EnvironmentPromotionRecordError(
        "Promotion record contains duplicate action results."
      )
    object.__setattr__(self, "action_results", ordered_results)

  @property
  def record_fingerprint(self) -> str:
    return canonical_sha256(self.to_dict(include_identifiers=False))

  @property
  def record_id(self) -> str:
    return f"prom-{self.record_fingerprint[:16]}"

  @property
  def summary(self) -> dict[str, Any]:
    return {
      "action_count": len(self.action_results),
      "applied_action_count": sum(
        1
        for item in self.action_results
        if item.result == EnvironmentPromotionActionResultType.APPLIED
      ),
      "verified_action_count": sum(
        1
        for item in self.action_results
        if item.result == EnvironmentPromotionActionResultType.VERIFIED
      ),
    }

  def to_dict(self, *, include_identifiers: bool = True) -> dict[str, Any]:
    data = {
      "artifact_type": self.artifact_type,
      "artifact_version": self.artifact_version,
      "plan_id": self.plan_id,
      "plan_fingerprint": self.plan_fingerprint,
      "approval_id": self.approval_id,
      "approval_fingerprint": self.approval_fingerprint,
      "release_id": self.release_id,
      "bundle_fingerprint": self.bundle_fingerprint,
      "source_environment_label": self.source_environment_label,
      "target_environment_label": self.target_environment_label,
      "pre_target_snapshot_fingerprint": self.pre_target_snapshot_fingerprint,
      "pre_target_metadata_fingerprint": self.pre_target_metadata_fingerprint,
      "post_target_snapshot_fingerprint": self.post_target_snapshot_fingerprint,
      "post_target_metadata_fingerprint": self.post_target_metadata_fingerprint,
      "post_validation_plan_id": self.post_validation_plan_id,
      "post_validation_plan_fingerprint": (
        self.post_validation_plan_fingerprint
      ),
      "applied_at": _format_datetime(self.applied_at),
      "applied_by": self.applied_by,
      "action_results": [item.to_dict() for item in self.action_results],
      "summary": self.summary,
    }
    if include_identifiers:
      data["record_id"] = self.record_id
      data["record_fingerprint"] = self.record_fingerprint
    return data

  @classmethod
  def from_dict(
    cls,
    data: Mapping[str, Any],
  ) -> EnvironmentPromotionRecord:
    _require_exact_keys(
      data,
      expected={
        "artifact_type",
        "artifact_version",
        "record_id",
        "record_fingerprint",
        "plan_id",
        "plan_fingerprint",
        "approval_id",
        "approval_fingerprint",
        "release_id",
        "bundle_fingerprint",
        "source_environment_label",
        "target_environment_label",
        "pre_target_snapshot_fingerprint",
        "pre_target_metadata_fingerprint",
        "post_target_snapshot_fingerprint",
        "post_target_metadata_fingerprint",
        "post_validation_plan_id",
        "post_validation_plan_fingerprint",
        "applied_at",
        "applied_by",
        "action_results",
        "summary",
      },
      label="Environment Promotion Record",
    )
    artifact_version = data.get("artifact_version")
    if not isinstance(artifact_version, int):
      raise EnvironmentPromotionRecordError(
        "Promotion record artifact_version must be an integer."
      )
    record = cls(
      artifact_type=_require_text(data.get("artifact_type"), "artifact_type"),
      artifact_version=artifact_version,
      plan_id=_require_text(data.get("plan_id"), "plan_id"),
      plan_fingerprint=_require_text(
        data.get("plan_fingerprint"),
        "plan_fingerprint",
      ),
      approval_id=_require_text(data.get("approval_id"), "approval_id"),
      approval_fingerprint=_require_text(
        data.get("approval_fingerprint"),
        "approval_fingerprint",
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
      pre_target_snapshot_fingerprint=_require_text(
        data.get("pre_target_snapshot_fingerprint"),
        "pre_target_snapshot_fingerprint",
      ),
      pre_target_metadata_fingerprint=_require_text(
        data.get("pre_target_metadata_fingerprint"),
        "pre_target_metadata_fingerprint",
      ),
      post_target_snapshot_fingerprint=_require_text(
        data.get("post_target_snapshot_fingerprint"),
        "post_target_snapshot_fingerprint",
      ),
      post_target_metadata_fingerprint=_require_text(
        data.get("post_target_metadata_fingerprint"),
        "post_target_metadata_fingerprint",
      ),
      post_validation_plan_id=_require_text(
        data.get("post_validation_plan_id"),
        "post_validation_plan_id",
      ),
      post_validation_plan_fingerprint=_require_text(
        data.get("post_validation_plan_fingerprint"),
        "post_validation_plan_fingerprint",
      ),
      applied_at=_parse_datetime(data.get("applied_at")),
      applied_by=_require_text(data.get("applied_by"), "applied_by"),
      action_results=tuple(
        EnvironmentPromotionActionResult.from_dict(
          _require_mapping(item, "promotion action result")
        )
        for item in _require_sequence(
          data.get("action_results"),
          "action_results",
        )
      ),
    )
    if canonicalize_metadata_value(data.get("summary")) != record.summary:
      raise EnvironmentPromotionRecordError(
        "Promotion record summary mismatch."
      )
    _require_match(
      actual=_require_text(
        data.get("record_fingerprint"),
        "record_fingerprint",
      ),
      expected=record.record_fingerprint,
      label="promotion record fingerprint",
    )
    _require_match(
      actual=_require_text(data.get("record_id"), "record_id"),
      expected=record.record_id,
      label="promotion record ID",
    )
    return record


def serialize_environment_promotion_record(
  record: EnvironmentPromotionRecord,
  *,
  pretty: bool = True,
) -> str:
  if not pretty:
    return canonical_json(record.to_dict())
  return json.dumps(
    canonicalize_metadata_value(record.to_dict()),
    sort_keys=True,
    ensure_ascii=False,
    indent=2,
    allow_nan=False,
  ) + "\n"


def deserialize_environment_promotion_record(
  payload: str,
) -> EnvironmentPromotionRecord:
  try:
    data = json.loads(payload)
  except JSONDecodeError as exc:
    raise EnvironmentPromotionRecordError(
      f"Environment Promotion Record is not valid JSON: {exc}."
    ) from exc
  return EnvironmentPromotionRecord.from_dict(
    _require_mapping(data, "Environment Promotion Record")
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
    raise EnvironmentPromotionRecordError(
      f"Invalid promotion record datetime: {value}."
    ) from exc
  if parsed.tzinfo is None:
    raise EnvironmentPromotionRecordError(
      "Promotion record datetime must include a timezone."
    )
  return parsed


def _require_text(value: Any, label: str) -> str:
  if not isinstance(value, str):
    raise EnvironmentPromotionRecordError(f"{label} must be a string.")
  normalized = value.strip()
  if not normalized:
    raise EnvironmentPromotionRecordError(f"{label} must not be empty.")
  return normalized


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
  if not isinstance(value, Mapping):
    raise EnvironmentPromotionRecordError(f"{label} must be an object.")
  return value


def _require_sequence(value: Any, label: str) -> Sequence[Any]:
  if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
    raise EnvironmentPromotionRecordError(f"{label} must be an array.")
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
  raise EnvironmentPromotionRecordError(
    f"{label} fields are invalid: {'; '.join(details)}."
  )


def _require_match(*, actual: Any, expected: Any, label: str) -> None:
  if actual != expected:
    raise EnvironmentPromotionRecordError(f"{label} mismatch.")
