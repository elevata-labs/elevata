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
from json import JSONDecodeError
import json
from types import MappingProxyType
from typing import Any

from metadata.promotion.canonical import (
  canonical_json,
  canonical_sha256,
  canonicalize_metadata_value,
)
from metadata.promotion.identities import (
  MetadataObjectIdentity,
  build_metadata_object_identity,
)
from metadata.promotion.model_contracts import (
  METADATA_TRANSPORT_REGISTRY,
)


ENVIRONMENT_METADATA_SNAPSHOT_ARTIFACT_TYPE = (
  "environment_metadata_snapshot"
)
ENVIRONMENT_METADATA_SNAPSHOT_ARTIFACT_VERSION = 1


class EnvironmentMetadataSnapshotError(ValueError):
  """
  Raised when an environment metadata snapshot is structurally invalid.
  """


@dataclass(frozen=True)
class EnvironmentMetadataObject:
  """
  One portable metadata object with an ID-independent logical identity.
  """
  model_name: str
  identity: MetadataObjectIdentity
  fields: Mapping[str, Any]

  def __post_init__(self) -> None:
    if self.identity.model_name != self.model_name:
      raise EnvironmentMetadataSnapshotError(
        "Metadata object model and identity model do not match."
      )

    contract = METADATA_TRANSPORT_REGISTRY.get_model(self.model_name)
    field_names = frozenset(str(name) for name in self.fields)
    if field_names != contract.transport_fields:
      missing = sorted(contract.transport_fields - field_names)
      unexpected = sorted(field_names - contract.transport_fields)
      details = []
      if missing:
        details.append("missing: " + ", ".join(missing))
      if unexpected:
        details.append("unexpected: " + ", ".join(unexpected))
      raise EnvironmentMetadataSnapshotError(
        f"Portable fields do not match the {self.model_name} contract"
        + (": " + "; ".join(details) if details else ".")
      )

    normalized_fields = _freeze_value(
      canonicalize_metadata_value(dict(self.fields))
    )
    object.__setattr__(self, "fields", normalized_fields)

  @property
  def object_key(self) -> str:
    """
    Return the opaque portable identity key.
    """
    return self.identity.object_key

  def to_dict(self) -> dict[str, Any]:
    """
    Return the deterministic public object representation.
    """
    return {
      "model": self.model_name,
      "object_key": self.object_key,
      "identity": canonicalize_metadata_value({
        name: value
        for name, value in self.identity.components
      }),
      "fields": canonicalize_metadata_value(self.fields),
    }

  @classmethod
  def from_dict(
    cls,
    data: Mapping[str, Any],
  ) -> EnvironmentMetadataObject:
    """
    Build and validate one portable metadata object.
    """
    _require_exact_keys(
      data,
      expected={"model", "object_key", "identity", "fields"},
      label="metadata object",
    )
    model_name = _require_non_empty_string(data.get("model"), "model")
    object_key = _require_non_empty_string(
      data.get("object_key"),
      "object_key",
    )
    identity_values = _require_mapping(data.get("identity"), "identity")
    fields = _require_mapping(data.get("fields"), "fields")

    contract = METADATA_TRANSPORT_REGISTRY.get_model(model_name)
    identity = build_metadata_object_identity(
      model_name=model_name,
      contract=contract.identity,
      values=identity_values,
    )
    expected_identity_fields = {
      name
      for name, _ in identity.components
    }
    actual_identity_fields = {str(name) for name in identity_values}
    if actual_identity_fields != expected_identity_fields:
      raise EnvironmentMetadataSnapshotError(
        f"Portable identity fields do not match the {model_name} contract."
      )
    if identity.object_key != object_key:
      raise EnvironmentMetadataSnapshotError(
        f"Object key mismatch for {model_name}: expected "
        f"{identity.object_key}, received {object_key}."
      )

    return cls(
      model_name=model_name,
      identity=identity,
      fields=fields,
    )


@dataclass(frozen=True)
class EnvironmentMetadataRelationship:
  """
  One portable implicit many-to-many relationship.
  """
  relationship_name: str
  source_model: str
  source_key: str
  target_model: str
  target_key: str

  def __post_init__(self) -> None:
    relationship_contracts = {
      item.name: item
      for item in METADATA_TRANSPORT_REGISTRY.relationships
    }
    try:
      contract = relationship_contracts[self.relationship_name]
    except KeyError as exc:
      raise EnvironmentMetadataSnapshotError(
        "Unknown portable relationship: "
        f"{self.relationship_name}."
      ) from exc

    if (
      self.source_model != contract.source_model
      or self.target_model != contract.target_model
    ):
      raise EnvironmentMetadataSnapshotError(
        f"Relationship model mismatch for {self.relationship_name}."
      )

    for label, value, model_name in (
      ("source_key", self.source_key, self.source_model),
      ("target_key", self.target_key, self.target_model),
    ):
      _require_non_empty_string(value, label)
      if not value.startswith(f"{model_name}:"):
        raise EnvironmentMetadataSnapshotError(
          f"{label} does not reference a {model_name} object."
        )

  @property
  def relationship_key(self) -> str:
    """
    Return the deterministic relationship identity.
    """
    return canonical_sha256({
      "relationship": self.relationship_name,
      "source_key": self.source_key,
      "target_key": self.target_key,
    })

  def to_dict(self) -> dict[str, Any]:
    """
    Return the deterministic public relationship representation.
    """
    return {
      "relationship": self.relationship_name,
      "relationship_key": self.relationship_key,
      "source_model": self.source_model,
      "source_key": self.source_key,
      "target_model": self.target_model,
      "target_key": self.target_key,
    }

  @classmethod
  def from_dict(
    cls,
    data: Mapping[str, Any],
  ) -> EnvironmentMetadataRelationship:
    """
    Build and validate one portable implicit relationship.
    """
    _require_exact_keys(
      data,
      expected={
        "relationship",
        "relationship_key",
        "source_model",
        "source_key",
        "target_model",
        "target_key",
      },
      label="metadata relationship",
    )
    relationship = cls(
      relationship_name=_require_non_empty_string(
        data.get("relationship"),
        "relationship",
      ),
      source_model=_require_non_empty_string(
        data.get("source_model"),
        "source_model",
      ),
      source_key=_require_non_empty_string(
        data.get("source_key"),
        "source_key",
      ),
      target_model=_require_non_empty_string(
        data.get("target_model"),
        "target_model",
      ),
      target_key=_require_non_empty_string(
        data.get("target_key"),
        "target_key",
      ),
    )
    relationship_key = _require_non_empty_string(
      data.get("relationship_key"),
      "relationship_key",
    )
    if relationship.relationship_key != relationship_key:
      raise EnvironmentMetadataSnapshotError(
        "Metadata relationship fingerprint mismatch for "
        f"{relationship.relationship_name}."
      )
    return relationship


@dataclass(frozen=True)
class EnvironmentMetadataPayload:
  """
  Canonical portable metadata definition of one environment.
  """
  objects: tuple[EnvironmentMetadataObject, ...] = ()
  relationships: tuple[EnvironmentMetadataRelationship, ...] = ()

  def __post_init__(self) -> None:
    ordered_objects = tuple(sorted(
      self.objects,
      key=lambda item: (item.model_name, item.object_key),
    ))
    ordered_relationships = tuple(sorted(
      self.relationships,
      key=lambda item: (
        item.relationship_name,
        item.source_key,
        item.target_key,
      ),
    ))

    object_keys = [item.object_key for item in ordered_objects]
    duplicate_object_keys = _duplicates(object_keys)
    if duplicate_object_keys:
      raise EnvironmentMetadataSnapshotError(
        "Duplicate portable metadata object keys: "
        + ", ".join(duplicate_object_keys)
      )

    relationship_keys = [
      item.relationship_key
      for item in ordered_relationships
    ]
    duplicate_relationship_keys = _duplicates(relationship_keys)
    if duplicate_relationship_keys:
      raise EnvironmentMetadataSnapshotError(
        "Duplicate portable metadata relationships: "
        + ", ".join(duplicate_relationship_keys)
      )

    known_object_keys = set(object_keys)
    dangling = sorted({
      key
      for relationship in ordered_relationships
      for key in (relationship.source_key, relationship.target_key)
      if key not in known_object_keys
    })
    if dangling:
      raise EnvironmentMetadataSnapshotError(
        "Portable relationships reference missing objects: "
        + ", ".join(dangling)
      )

    object.__setattr__(self, "objects", ordered_objects)
    object.__setattr__(self, "relationships", ordered_relationships)

  @property
  def metadata_fingerprint(self) -> str:
    """
    Return the stable fingerprint of the complete portable metadata payload.
    """
    return canonical_sha256(self.to_dict())

  def to_dict(self) -> dict[str, Any]:
    """
    Return the canonical public metadata payload.
    """
    return {
      "objects": [item.to_dict() for item in self.objects],
      "relationships": [
        item.to_dict()
        for item in self.relationships
      ],
    }

  @classmethod
  def from_dict(
    cls,
    data: Mapping[str, Any],
  ) -> EnvironmentMetadataPayload:
    """
    Deserialize and validate one complete portable metadata payload.
    """
    _require_exact_keys(
      data,
      expected={"objects", "relationships"},
      label="metadata payload",
    )
    object_values = _require_sequence(data.get("objects"), "objects")
    relationship_values = _require_sequence(
      data.get("relationships"),
      "relationships",
    )
    return cls(
      objects=tuple(
        EnvironmentMetadataObject.from_dict(
          _require_mapping(value, "metadata object")
        )
        for value in object_values
      ),
      relationships=tuple(
        EnvironmentMetadataRelationship.from_dict(
          _require_mapping(value, "metadata relationship")
        )
        for value in relationship_values
      ),
    )


@dataclass(frozen=True)
class EnvironmentMetadataSnapshot:
  """
  Immutable read-only metadata export of one environment.
  """
  environment_label: str
  created_at: datetime
  metadata: EnvironmentMetadataPayload
  created_by: str | None = None
  artifact_type: str = ENVIRONMENT_METADATA_SNAPSHOT_ARTIFACT_TYPE
  artifact_version: int = ENVIRONMENT_METADATA_SNAPSHOT_ARTIFACT_VERSION

  def __post_init__(self) -> None:
    environment_label = _require_non_empty_string(
      self.environment_label,
      "environment_label",
    )
    object.__setattr__(self, "environment_label", environment_label)

    if self.artifact_type != ENVIRONMENT_METADATA_SNAPSHOT_ARTIFACT_TYPE:
      raise EnvironmentMetadataSnapshotError(
        f"Unsupported artifact type: {self.artifact_type}."
      )
    if self.artifact_version != ENVIRONMENT_METADATA_SNAPSHOT_ARTIFACT_VERSION:
      raise EnvironmentMetadataSnapshotError(
        f"Unsupported snapshot artifact version: {self.artifact_version}."
      )
    if self.created_at.tzinfo is None:
      raise EnvironmentMetadataSnapshotError(
        "Snapshot created_at must be timezone-aware."
      )

    created_by = (
      str(self.created_by).strip()
      if self.created_by is not None
      else None
    )
    object.__setattr__(self, "created_by", created_by or None)

  @property
  def metadata_fingerprint(self) -> str:
    """
    Return the fingerprint of the portable metadata definition only.
    """
    return self.metadata.metadata_fingerprint

  @property
  def snapshot_fingerprint(self) -> str:
    """
    Return the stable drift identity of this snapshot.

    Creation provenance is intentionally excluded so an unchanged environment
    produces the same snapshot fingerprint when exported again.
    """
    return canonical_sha256({
      "artifact_type": self.artifact_type,
      "artifact_version": self.artifact_version,
      "environment_label": self.environment_label,
      "metadata_fingerprint": self.metadata_fingerprint,
    })

  def to_dict(self) -> dict[str, Any]:
    """
    Return the complete public snapshot representation.
    """
    return {
      "artifact_type": self.artifact_type,
      "artifact_version": self.artifact_version,
      "environment_label": self.environment_label,
      "created_at": _format_datetime(self.created_at),
      "created_by": self.created_by,
      "metadata_fingerprint": self.metadata_fingerprint,
      "snapshot_fingerprint": self.snapshot_fingerprint,
      "metadata": self.metadata.to_dict(),
    }

  @classmethod
  def from_dict(
    cls,
    data: Mapping[str, Any],
  ) -> EnvironmentMetadataSnapshot:
    """
    Deserialize and validate one immutable metadata snapshot.
    """
    _require_exact_keys(
      data,
      expected={
        "artifact_type",
        "artifact_version",
        "environment_label",
        "created_at",
        "created_by",
        "metadata_fingerprint",
        "snapshot_fingerprint",
        "metadata",
      },
      label="environment metadata snapshot",
    )

    artifact_type = _require_non_empty_string(
      data.get("artifact_type"),
      "artifact_type",
    )
    artifact_version = data.get("artifact_version")
    if not isinstance(artifact_version, int):
      raise EnvironmentMetadataSnapshotError(
        "artifact_version must be an integer."
      )

    metadata = EnvironmentMetadataPayload.from_dict(
      _require_mapping(data.get("metadata"), "metadata")
    )
    snapshot = cls(
      artifact_type=artifact_type,
      artifact_version=artifact_version,
      environment_label=_require_non_empty_string(
        data.get("environment_label"),
        "environment_label",
      ),
      created_at=_parse_datetime(data.get("created_at")),
      created_by=(
        None
        if data.get("created_by") is None
        else str(data.get("created_by"))
      ),
      metadata=metadata,
    )

    metadata_fingerprint = _require_non_empty_string(
      data.get("metadata_fingerprint"),
      "metadata_fingerprint",
    )
    if snapshot.metadata_fingerprint != metadata_fingerprint:
      raise EnvironmentMetadataSnapshotError(
        "Environment metadata fingerprint mismatch."
      )

    snapshot_fingerprint = _require_non_empty_string(
      data.get("snapshot_fingerprint"),
      "snapshot_fingerprint",
    )
    if snapshot.snapshot_fingerprint != snapshot_fingerprint:
      raise EnvironmentMetadataSnapshotError(
        "Environment snapshot fingerprint mismatch."
      )

    return snapshot


def serialize_environment_metadata_snapshot(
  snapshot: EnvironmentMetadataSnapshot,
  *,
  pretty: bool = True,
) -> str:
  """
  Serialize a snapshot using stable JSON key and collection ordering.
  """
  if not pretty:
    return canonical_json(snapshot.to_dict())

  return json.dumps(
    canonicalize_metadata_value(snapshot.to_dict()),
    sort_keys=True,
    ensure_ascii=False,
    indent=2,
    allow_nan=False,
  ) + "\n"


def deserialize_environment_metadata_snapshot(
  payload: str,
) -> EnvironmentMetadataSnapshot:
  """
  Deserialize and validate one snapshot JSON document.
  """
  try:
    data = json.loads(payload)
  except JSONDecodeError as exc:
    raise EnvironmentMetadataSnapshotError(
      f"Environment metadata snapshot is not valid JSON: {exc}."
    ) from exc

  return EnvironmentMetadataSnapshot.from_dict(
    _require_mapping(data, "environment metadata snapshot")
  )


def _format_datetime(value: datetime) -> str:
  """
  Render a timezone-aware timestamp in normalized UTC form.
  """
  return (
    value.astimezone(timezone.utc)
    .isoformat(timespec="microseconds")
    .replace("+00:00", "Z")
  )


def _parse_datetime(value: Any) -> datetime:
  """
  Parse one normalized ISO timestamp.
  """
  if not isinstance(value, str) or not value.strip():
    raise EnvironmentMetadataSnapshotError(
      "created_at must be a non-empty ISO timestamp."
    )
  text = value.strip()
  if text.endswith("Z"):
    text = text[:-1] + "+00:00"
  try:
    parsed = datetime.fromisoformat(text)
  except ValueError as exc:
    raise EnvironmentMetadataSnapshotError(
      f"created_at is not a valid ISO timestamp: {value}."
    ) from exc
  if parsed.tzinfo is None:
    raise EnvironmentMetadataSnapshotError(
      "created_at must contain a timezone."
    )
  return parsed


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
  if not isinstance(value, Mapping):
    raise EnvironmentMetadataSnapshotError(
      f"{label} must be a JSON object."
    )
  return value


def _require_sequence(value: Any, label: str) -> Sequence[Any]:
  if (
    not isinstance(value, Sequence)
    or isinstance(value, (str, bytes, bytearray))
  ):
    raise EnvironmentMetadataSnapshotError(
      f"{label} must be a JSON array."
    )
  return value


def _require_non_empty_string(value: Any, label: str) -> str:
  if not isinstance(value, str) or not value.strip():
    raise EnvironmentMetadataSnapshotError(
      f"{label} must be a non-empty string."
    )
  return value.strip()


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
  raise EnvironmentMetadataSnapshotError(
    f"Invalid {label} fields: " + "; ".join(details)
  )



def _freeze_value(value: Any) -> Any:
  """
  Recursively freeze canonical metadata containers.
  """
  if isinstance(value, Mapping):
    return MappingProxyType({
      str(key): _freeze_value(item)
      for key, item in value.items()
    })
  if isinstance(value, list):
    return tuple(_freeze_value(item) for item in value)
  if isinstance(value, tuple):
    return tuple(_freeze_value(item) for item in value)
  return value

def _duplicates(values: Sequence[str]) -> list[str]:
  seen: set[str] = set()
  duplicates: set[str] = set()
  for value in values:
    if value in seen:
      duplicates.add(value)
    seen.add(value)
  return sorted(duplicates)
