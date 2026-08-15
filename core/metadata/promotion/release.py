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

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from json import JSONDecodeError
import json
from typing import Any

from metadata.promotion.canonical import (
  canonical_json,
  canonical_sha256,
  canonicalize_metadata_value,
)
from metadata.promotion.snapshot import (
  EnvironmentMetadataSnapshot,
  EnvironmentMetadataSnapshotError,
)


ARCHITECTURE_RELEASE_BUNDLE_ARTIFACT_TYPE = "architecture_release_bundle"
ARCHITECTURE_RELEASE_BUNDLE_ARTIFACT_VERSION = 1


class ArchitectureReleaseBundleError(ValueError):
  """
  Raised when an Architecture Release Bundle is structurally invalid.
  """


@dataclass(frozen=True)
class ArchitectureReleaseBundle:
  """
  Immutable release artifact containing one complete metadata snapshot.
  """
  release_name: str
  release_version: str
  created_at: datetime
  created_by: str
  snapshot: EnvironmentMetadataSnapshot
  description: str = ""
  artifact_type: str = ARCHITECTURE_RELEASE_BUNDLE_ARTIFACT_TYPE
  artifact_version: int = ARCHITECTURE_RELEASE_BUNDLE_ARTIFACT_VERSION

  def __post_init__(self) -> None:
    object.__setattr__(
      self,
      "release_name",
      _normalize_text(self.release_name, "release_name", max_length=128),
    )
    object.__setattr__(
      self,
      "release_version",
      _normalize_text(self.release_version, "release_version", max_length=64),
    )
    object.__setattr__(
      self,
      "created_by",
      _normalize_text(self.created_by, "created_by", max_length=254),
    )
    object.__setattr__(
      self,
      "description",
      _normalize_optional_text(
        self.description,
        "description",
        max_length=2000,
      ),
    )

    if self.artifact_type != ARCHITECTURE_RELEASE_BUNDLE_ARTIFACT_TYPE:
      raise ArchitectureReleaseBundleError(
        f"Unsupported release artifact type: {self.artifact_type}."
      )
    if self.artifact_version != ARCHITECTURE_RELEASE_BUNDLE_ARTIFACT_VERSION:
      raise ArchitectureReleaseBundleError(
        f"Unsupported release artifact version: {self.artifact_version}."
      )
    if self.created_at.tzinfo is None:
      raise ArchitectureReleaseBundleError(
        "Release created_at must be timezone-aware."
      )
    if not isinstance(self.snapshot, EnvironmentMetadataSnapshot):
      raise ArchitectureReleaseBundleError(
        "Release snapshot must be an EnvironmentMetadataSnapshot."
      )

  @property
  def source_environment_label(self) -> str:
    """
    Return the environment label captured by the source snapshot.
    """
    return self.snapshot.environment_label

  @property
  def metadata_fingerprint(self) -> str:
    """
    Return the fingerprint of the portable metadata definition.
    """
    return self.snapshot.metadata_fingerprint

  @property
  def source_snapshot_fingerprint(self) -> str:
    """
    Return the stable source snapshot fingerprint.
    """
    return self.snapshot.snapshot_fingerprint

  @property
  def bundle_fingerprint(self) -> str:
    """
    Return the fingerprint of the complete immutable release content.
    """
    return canonical_sha256(self.to_dict(include_identifiers=False))

  @property
  def release_id(self) -> str:
    """
    Return the opaque release identifier derived from the bundle fingerprint.
    """
    return f"rel-{self.bundle_fingerprint[:16]}"

  @property
  def coordinate(self) -> tuple[str, str]:
    """
    Return the immutable human release coordinate.
    """
    return (self.release_name, self.release_version)

  def to_dict(
    self,
    *,
    include_identifiers: bool = True,
  ) -> dict[str, Any]:
    """
    Return the deterministic public release representation.
    """
    data = {
      "artifact_type": self.artifact_type,
      "artifact_version": self.artifact_version,
      "release_name": self.release_name,
      "release_version": self.release_version,
      "description": self.description,
      "created_at": _format_datetime(self.created_at),
      "created_by": self.created_by,
      "source_environment_label": self.source_environment_label,
      "source_snapshot_fingerprint": self.source_snapshot_fingerprint,
      "metadata_fingerprint": self.metadata_fingerprint,
      "snapshot": self.snapshot.to_dict(),
    }
    if include_identifiers:
      data["release_id"] = self.release_id
      data["bundle_fingerprint"] = self.bundle_fingerprint
    return data

  @classmethod
  def from_dict(
    cls,
    data: Mapping[str, Any],
  ) -> ArchitectureReleaseBundle:
    """
    Deserialize and validate one Architecture Release Bundle.
    """
    _require_exact_keys(
      data,
      expected={
        "artifact_type",
        "artifact_version",
        "release_id",
        "release_name",
        "release_version",
        "description",
        "created_at",
        "created_by",
        "source_environment_label",
        "source_snapshot_fingerprint",
        "metadata_fingerprint",
        "snapshot",
        "bundle_fingerprint",
      },
      label="Architecture Release Bundle",
    )

    artifact_version = data.get("artifact_version")
    if not isinstance(artifact_version, int):
      raise ArchitectureReleaseBundleError(
        "artifact_version must be an integer."
      )

    try:
      snapshot = EnvironmentMetadataSnapshot.from_dict(
        _require_mapping(data.get("snapshot"), "snapshot")
      )
    except EnvironmentMetadataSnapshotError as exc:
      raise ArchitectureReleaseBundleError(str(exc)) from exc

    bundle = cls(
      artifact_type=_require_text(data.get("artifact_type"), "artifact_type"),
      artifact_version=artifact_version,
      release_name=_require_text(data.get("release_name"), "release_name"),
      release_version=_require_text(
        data.get("release_version"),
        "release_version",
      ),
      description=_require_text(
        data.get("description"),
        "description",
        allow_empty=True,
      ),
      created_at=_parse_datetime(data.get("created_at")),
      created_by=_require_text(data.get("created_by"), "created_by"),
      snapshot=snapshot,
    )

    _require_match(
      actual=_require_text(
        data.get("source_environment_label"),
        "source_environment_label",
      ),
      expected=bundle.source_environment_label,
      label="source environment label",
    )
    _require_match(
      actual=_require_text(
        data.get("source_snapshot_fingerprint"),
        "source_snapshot_fingerprint",
      ),
      expected=bundle.source_snapshot_fingerprint,
      label="source snapshot fingerprint",
    )
    _require_match(
      actual=_require_text(
        data.get("metadata_fingerprint"),
        "metadata_fingerprint",
      ),
      expected=bundle.metadata_fingerprint,
      label="metadata fingerprint",
    )
    _require_match(
      actual=_require_text(
        data.get("bundle_fingerprint"),
        "bundle_fingerprint",
      ),
      expected=bundle.bundle_fingerprint,
      label="bundle fingerprint",
    )
    _require_match(
      actual=_require_text(data.get("release_id"), "release_id"),
      expected=bundle.release_id,
      label="release ID",
    )
    return bundle


def serialize_architecture_release_bundle(
  bundle: ArchitectureReleaseBundle,
  *,
  pretty: bool = True,
) -> str:
  """
  Serialize a release bundle using deterministic JSON ordering.
  """
  if not pretty:
    return canonical_json(bundle.to_dict())
  return json.dumps(
    canonicalize_metadata_value(bundle.to_dict()),
    sort_keys=True,
    ensure_ascii=False,
    indent=2,
    allow_nan=False,
  ) + "\n"


def deserialize_architecture_release_bundle(
  payload: str,
) -> ArchitectureReleaseBundle:
  """
  Deserialize and validate one release bundle JSON document.
  """
  try:
    data = json.loads(payload)
  except JSONDecodeError as exc:
    raise ArchitectureReleaseBundleError(
      f"Architecture Release Bundle is not valid JSON: {exc}."
    ) from exc
  return ArchitectureReleaseBundle.from_dict(
    _require_mapping(data, "Architecture Release Bundle")
  )


def _format_datetime(value: datetime) -> str:
  return (
    value.astimezone(timezone.utc)
    .isoformat(timespec="microseconds")
    .replace("+00:00", "Z")
  )


def _parse_datetime(value: Any) -> datetime:
  if not isinstance(value, str) or not value.strip():
    raise ArchitectureReleaseBundleError(
      "created_at must be a non-empty ISO timestamp."
    )
  text = value.strip()
  if text.endswith("Z"):
    text = text[:-1] + "+00:00"
  try:
    parsed = datetime.fromisoformat(text)
  except ValueError as exc:
    raise ArchitectureReleaseBundleError(
      f"created_at is not a valid ISO timestamp: {value}."
    ) from exc
  if parsed.tzinfo is None:
    raise ArchitectureReleaseBundleError(
      "created_at must contain a timezone."
    )
  return parsed


def _normalize_text(value: Any, label: str, *, max_length: int) -> str:
  text = _require_text(value, label)
  if len(text) > max_length:
    raise ArchitectureReleaseBundleError(
      f"{label} must not exceed {max_length} characters."
    )
  if any(ord(character) < 32 for character in text):
    raise ArchitectureReleaseBundleError(
      f"{label} must not contain control characters."
    )
  return text


def _normalize_optional_text(
  value: Any,
  label: str,
  *,
  max_length: int,
) -> str:
  text = _require_text(value, label, allow_empty=True)
  if len(text) > max_length:
    raise ArchitectureReleaseBundleError(
      f"{label} must not exceed {max_length} characters."
    )
  if any(ord(character) < 32 and character not in "\n\r\t" for character in text):
    raise ArchitectureReleaseBundleError(
      f"{label} contains unsupported control characters."
    )
  return text


def _require_text(value: Any, label: str, *, allow_empty: bool = False) -> str:
  if not isinstance(value, str):
    raise ArchitectureReleaseBundleError(f"{label} must be a string.")
  text = value.strip()
  if not text and not allow_empty:
    raise ArchitectureReleaseBundleError(f"{label} must not be empty.")
  return text


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
  if not isinstance(value, Mapping):
    raise ArchitectureReleaseBundleError(f"{label} must be a JSON object.")
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
  raise ArchitectureReleaseBundleError(
    f"{label} keys do not match the contract: " + "; ".join(details)
  )


def _require_match(*, actual: str, expected: str, label: str) -> None:
  if actual != expected:
    raise ArchitectureReleaseBundleError(
      f"Architecture Release Bundle {label} mismatch."
    )
