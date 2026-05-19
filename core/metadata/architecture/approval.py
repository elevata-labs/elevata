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
import os
from pathlib import Path
from typing import Any, Literal


ARCHITECTURE_APPROVAL_ARTIFACT_TYPE = "architecture_approval"
ARCHITECTURE_APPROVAL_ARTIFACT_VERSION = 1
ARCHITECTURE_APPROVAL_REPORT_TYPE = "architecture_change_report"
DEFAULT_ARCHITECTURE_APPROVAL_DIR = ".elevata/approvals"
ARCHITECTURE_APPROVAL_DIR_ENV = "ELEVATA_ARCH_APPROVAL_DIR"

ArchitectureApprovalDecision = Literal["approved", "rejected"]

_ARTIFACT_KEYS = {
  "artifact_type",
  "artifact_version",
  "approval_id",
  "report",
  "review",
  "artifact_fingerprint",
}

_REVIEW_KEYS = {
  "decision",
  "decided_by",
  "decided_at",
  "note",
}


class ArchitectureApprovalError(ValueError):
  """
  Raised when an architecture approval artifact is invalid.
  """


@dataclass(frozen=True)
class ArchitectureApprovalReview:
  """
  Review decision bound to an architecture approval artifact.
  """
  decision: ArchitectureApprovalDecision
  decided_by: str
  decided_at: str
  note: str = ""

  def to_dict(self) -> dict[str, Any]:
    """
    Return a deterministic dictionary representation.
    """
    return {
      "decision": self.decision,
      "decided_by": self.decided_by,
      "decided_at": self.decided_at,
      "note": self.note,
    }

  @classmethod
  def from_dict(cls, data: dict[str, Any]) -> ArchitectureApprovalReview:
    """
    Build a review decision from a JSON-compatible dictionary.
    """
    _validate_allowed_keys(data, _REVIEW_KEYS, "Architecture approval review")

    decision = _require_str(data, "decision")
    if decision not in {"approved", "rejected"}:
      raise ArchitectureApprovalError(
        f"Unsupported architecture approval decision: {decision}"
      )

    decided_by = _require_str(data, "decided_by")
    decided_at = _normalize_decided_at(_require_str(data, "decided_at"))
    note = str(data.get("note") or "")

    return cls(
      decision=decision,
      decided_by=decided_by,
      decided_at=decided_at,
      note=note,
    )


@dataclass(frozen=True)
class ArchitectureApprovalArtifact:
  """
  Deterministic approval artifact for an Architecture Change Report.
  """
  report: dict[str, Any]
  review: ArchitectureApprovalReview

  @property
  def artifact_fingerprint(self) -> str:
    """
    Return the deterministic fingerprint of this approval artifact.
    """
    return _stable_json_hash(self.to_dict(include_fingerprint=False))

  @property
  def approval_id(self) -> str:
    """
    Return the stable approval identifier derived from the artifact fingerprint.
    """
    return _approval_id_for_fingerprint(self.artifact_fingerprint)

  def to_dict(self, *, include_fingerprint: bool = True) -> dict[str, Any]:
    """
    Return a deterministic dictionary representation.
    """
    data = {
      "artifact_type": ARCHITECTURE_APPROVAL_ARTIFACT_TYPE,
      "artifact_version": ARCHITECTURE_APPROVAL_ARTIFACT_VERSION,
      "report": _canonicalize(self.report),
      "review": self.review.to_dict(),
    }

    if include_fingerprint:
      data["approval_id"] = self.approval_id
      data["artifact_fingerprint"] = self.artifact_fingerprint

    return data

  @classmethod
  def from_dict(cls, data: dict[str, Any]) -> ArchitectureApprovalArtifact:
    """
    Build an approval artifact from a JSON-compatible dictionary.
    """
    _validate_allowed_keys(data, _ARTIFACT_KEYS, "Architecture approval artifact")

    artifact_type = _require_str(data, "artifact_type")
    if artifact_type != ARCHITECTURE_APPROVAL_ARTIFACT_TYPE:
      raise ArchitectureApprovalError(
        f"Unsupported architecture approval artifact type: {artifact_type}"
      )

    artifact_version = _require_int(data, "artifact_version")
    if artifact_version != ARCHITECTURE_APPROVAL_ARTIFACT_VERSION:
      raise ArchitectureApprovalError(
        f"Unsupported architecture approval artifact version: {artifact_version}"
      )

    return cls(
      report=_canonical_dict(_require_dict(data, "report"), "report"),
      review=ArchitectureApprovalReview.from_dict(_require_dict(data, "review")),
    )


@dataclass(frozen=True)
class ArchitectureApprovalCheckResult:
  """
  Result of checking an approval artifact against an Architecture Change Report.
  """
  is_valid: bool
  status: str
  message: str
  report_fingerprint: str | None = None
  approval_id: str | None = None
  artifact_fingerprint: str | None = None

  def to_dict(self) -> dict[str, Any]:
    """
    Return a deterministic dictionary representation.
    """
    return {
      "is_valid": self.is_valid,
      "status": self.status,
      "message": self.message,
      "report_fingerprint": self.report_fingerprint,
      "approval_id": self.approval_id,
      "artifact_fingerprint": self.artifact_fingerprint,
    }


def resolve_architecture_approval_dir(
  default: str | Path = DEFAULT_ARCHITECTURE_APPROVAL_DIR,
) -> Path:
  """
  Resolve the architecture approval artifact directory.
  """
  value = os.getenv(ARCHITECTURE_APPROVAL_DIR_ENV)
  if value and value.strip():
    return Path(value.strip())

  return Path(default)


class ArchitectureApprovalStore:
  """
  File-based store for architecture approval artifacts.
  """

  def __init__(self, base_path: str | Path | None = None):
    self.base_path = (
      Path(base_path)
      if base_path is not None
      else resolve_architecture_approval_dir()
    )

  def approval_file(self, report_fingerprint: str) -> Path:
    """
    Return the approval artifact path for a report fingerprint.
    """
    fingerprint = (report_fingerprint or "").strip()
    if not fingerprint:
      raise ArchitectureApprovalError("Architecture report fingerprint is required.")

    return self.base_path / f"{fingerprint}.approval.json"

  def save(self, artifact: ArchitectureApprovalArtifact) -> Path:
    """
    Store an approval artifact using its report fingerprint.
    """
    report_fingerprint = _require_str(artifact.report, "report_fingerprint")
    path = self.approval_file(report_fingerprint)
    self.save_file(path, artifact)
    return path

  def load_for_report_fingerprint(
    self,
    report_fingerprint: str,
  ) -> ArchitectureApprovalArtifact | None:
    """
    Load the approval artifact for a report fingerprint.
    """
    path = self.approval_file(report_fingerprint)
    if not path.exists():
      return None

    return self.load_file(path)

  def load_all(self) -> tuple[ArchitectureApprovalArtifact, ...]:
    """
    Load all valid approval artifacts from the store.
    """
    if not self.base_path.exists():
      return ()

    artifacts: list[ArchitectureApprovalArtifact] = []
    for path in sorted(self.base_path.glob("*.json")):
      try:
        artifacts.append(self.load_file(path))
      except ArchitectureApprovalError:
        continue

    return tuple(artifacts)

  @classmethod
  def load_file(cls, path: str | Path) -> ArchitectureApprovalArtifact:
    """
    Load an approval artifact from a JSON file.
    """
    artifact_path = Path(path)

    try:
      payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except OSError as exc:
      raise ArchitectureApprovalError(
        f"Architecture approval artifact could not be read: {artifact_path}"
      ) from exc
    except JSONDecodeError as exc:
      raise ArchitectureApprovalError(
        f"Architecture approval artifact JSON is invalid: {artifact_path}"
      ) from exc

    if not isinstance(payload, dict):
      raise ArchitectureApprovalError(
        f"Architecture approval artifact must contain an object: {artifact_path}"
      )

    artifact = ArchitectureApprovalArtifact.from_dict(payload)
    actual_fingerprint = _require_str(payload, "artifact_fingerprint")
    if actual_fingerprint != artifact.artifact_fingerprint:
      raise ArchitectureApprovalError(
        "Approval artifact fingerprint does not match its payload."
      )

    actual_approval_id = _require_str(payload, "approval_id")
    if actual_approval_id != artifact.approval_id:
      raise ArchitectureApprovalError(
        "Approval identifier does not match the artifact fingerprint."
      )

    return artifact

  @classmethod
  def save_file(
    cls,
    path: str | Path,
    artifact: ArchitectureApprovalArtifact,
  ) -> None:
    """
    Write an approval artifact to a JSON file.
    """
    artifact_path = Path(path)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(
      json.dumps(
        artifact.to_dict(),
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
      ) + "\n",
      encoding="utf-8",
    )


def build_architecture_approval_artifact(
  *,
  report_payload: dict[str, Any],
  decided_by: str,
  note: str = "",
  decided_at: str | None = None,
) -> ArchitectureApprovalArtifact:
  """
  Build a deterministic approval artifact for an Architecture Change Report.
  """
  reviewer = (decided_by or "").strip()
  if not reviewer:
    raise ArchitectureApprovalError("Architecture approval reviewer is required.")

  review = ArchitectureApprovalReview(
    decision="approved",
    decided_by=reviewer,
    decided_at=_normalize_decided_at(decided_at),
    note=str(note or ""),
  )

  return ArchitectureApprovalArtifact(
    report=build_architecture_report_reference(report_payload),
    review=review,
  )


def build_architecture_report_reference(report_payload: dict[str, Any]) -> dict[str, Any]:
  """
  Build the approval-bound reference for an Architecture Change Report.
  """
  if "promotion_fingerprint" in report_payload:
    raise ArchitectureApprovalError(
      "Architecture approvals accept Architecture Change Report JSON."
    )

  report_fingerprint = _require_str(report_payload, "report_fingerprint")

  return {
    "type": ARCHITECTURE_APPROVAL_REPORT_TYPE,
    "report_fingerprint": report_fingerprint,
    "state": _canonical_dict(_require_dict(report_payload, "state"), "state"),
    "scope": _canonical_dict(_require_dict(report_payload, "scope"), "scope"),
    "summary": _canonical_dict(_require_dict(report_payload, "summary"), "summary"),
    "is_blocked": _require_bool(report_payload, "is_blocked"),
  }


def check_architecture_approval(
  *,
  report_payload: dict[str, Any],
  approval_payload: dict[str, Any],
) -> ArchitectureApprovalCheckResult:
  """
  Check whether an approval artifact matches an Architecture Change Report.
  """
  report_fingerprint = _safe_str(report_payload, "report_fingerprint")
  approval_id = _safe_str(approval_payload, "approval_id")
  artifact_fingerprint = _safe_str(approval_payload, "artifact_fingerprint")

  try:
    expected_report = build_architecture_report_reference(report_payload)
    artifact = ArchitectureApprovalArtifact.from_dict(approval_payload)

    expected_artifact_fingerprint = artifact.artifact_fingerprint
    actual_artifact_fingerprint = _require_str(
      approval_payload,
      "artifact_fingerprint",
    )
    if actual_artifact_fingerprint != expected_artifact_fingerprint:
      return ArchitectureApprovalCheckResult(
        is_valid=False,
        status="invalid",
        message="Approval artifact fingerprint does not match its payload.",
        report_fingerprint=report_fingerprint,
        approval_id=approval_id,
        artifact_fingerprint=artifact_fingerprint,
      )

    expected_approval_id = _approval_id_for_fingerprint(expected_artifact_fingerprint)
    actual_approval_id = _require_str(approval_payload, "approval_id")
    if actual_approval_id != expected_approval_id:
      return ArchitectureApprovalCheckResult(
        is_valid=False,
        status="invalid",
        message="Approval identifier does not match the artifact fingerprint.",
        report_fingerprint=report_fingerprint,
        approval_id=approval_id,
        artifact_fingerprint=artifact_fingerprint,
      )

    if artifact.report != expected_report:
      return ArchitectureApprovalCheckResult(
        is_valid=False,
        status="invalid",
        message="Approval artifact is bound to a different architecture report.",
        report_fingerprint=report_fingerprint,
        approval_id=approval_id,
        artifact_fingerprint=artifact_fingerprint,
      )

    if artifact.review.decision != "approved":
      return ArchitectureApprovalCheckResult(
        is_valid=False,
        status=artifact.review.decision,
        message="Architecture report is not approved.",
        report_fingerprint=report_fingerprint,
        approval_id=approval_id,
        artifact_fingerprint=artifact_fingerprint,
      )

    return ArchitectureApprovalCheckResult(
      is_valid=True,
      status="approved",
      message="Approval artifact matches the architecture change report.",
      report_fingerprint=report_fingerprint,
      approval_id=approval_id,
      artifact_fingerprint=artifact_fingerprint,
    )

  except ArchitectureApprovalError as exc:
    return ArchitectureApprovalCheckResult(
      is_valid=False,
      status="invalid",
      message=str(exc),
      report_fingerprint=report_fingerprint,
      approval_id=approval_id,
      artifact_fingerprint=artifact_fingerprint,
    )


def _normalize_decided_at(value: str | None) -> str:
  """
  Normalize a decision timestamp to UTC ISO-8601 form.
  """
  if value is None:
    dt = datetime.now(timezone.utc)
  else:
    text = value.strip()
    if not text:
      raise ArchitectureApprovalError("Architecture approval timestamp is required.")
    try:
      dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
      raise ArchitectureApprovalError(
        f"Invalid architecture approval timestamp: {value}"
      ) from exc
    if dt.tzinfo is None:
      dt = dt.replace(tzinfo=timezone.utc)
    else:
      dt = dt.astimezone(timezone.utc)

  return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _approval_id_for_fingerprint(fingerprint: str) -> str:
  """
  Return a stable approval identifier for an artifact fingerprint.
  """
  return f"apr_{fingerprint[:16]}"


def _stable_json_hash(value: Any) -> str:
  """
  Return a deterministic SHA-256 hash for a JSON-serializable value.
  """
  payload = json.dumps(
    value,
    sort_keys=True,
    ensure_ascii=False,
    separators=(",", ":"),
  )
  return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _canonicalize(value: Any) -> Any:
  """
  Return a canonical JSON-compatible value.
  """
  return json.loads(
    json.dumps(
      value,
      sort_keys=True,
      ensure_ascii=False,
      separators=(",", ":"),
    )
  )


def _canonical_dict(value: dict[str, Any], field_name: str) -> dict[str, Any]:
  """
  Return a canonical dictionary for a JSON object field.
  """
  canonical = _canonicalize(value)
  if not isinstance(canonical, dict):
    raise ArchitectureApprovalError(
      f"Architecture approval field must be an object: {field_name}"
    )
  return canonical


def _validate_allowed_keys(
  data: dict[str, Any],
  allowed_keys: set[str],
  label: str,
) -> None:
  """
  Validate that a dictionary contains only supported fields.
  """
  unexpected_keys = sorted(set(data) - allowed_keys)
  if unexpected_keys:
    fields = ", ".join(unexpected_keys)
    raise ArchitectureApprovalError(f"{label} contains unsupported fields: {fields}")


def _require_dict(data: dict[str, Any], field_name: str) -> dict[str, Any]:
  """
  Return a required dictionary field.
  """
  value = data.get(field_name)
  if not isinstance(value, dict):
    raise ArchitectureApprovalError(
      f"Architecture approval field must be an object: {field_name}"
    )
  return value


def _require_str(data: dict[str, Any], field_name: str) -> str:
  """
  Return a required non-empty string field.
  """
  value = data.get(field_name)
  if not isinstance(value, str) or not value.strip():
    raise ArchitectureApprovalError(
      f"Architecture approval field must be a non-empty string: {field_name}"
    )
  return value.strip()


def _require_int(data: dict[str, Any], field_name: str) -> int:
  """
  Return a required integer field.
  """
  value = data.get(field_name)
  if isinstance(value, bool) or not isinstance(value, int):
    raise ArchitectureApprovalError(
      f"Architecture approval field must be an integer: {field_name}"
    )
  return value


def _require_bool(data: dict[str, Any], field_name: str) -> bool:
  """
  Return a required boolean field.
  """
  value = data.get(field_name)
  if not isinstance(value, bool):
    raise ArchitectureApprovalError(
      f"Architecture approval field must be a boolean: {field_name}"
    )
  return value


def _safe_str(data: dict[str, Any], field_name: str) -> str | None:
  """
  Return a string field when available.
  """
  if not isinstance(data, dict):
    return None
  value = data.get(field_name)
  return value if isinstance(value, str) else None