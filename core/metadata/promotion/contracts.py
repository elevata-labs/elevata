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
from enum import Enum, IntEnum
from types import MappingProxyType
from typing import Any, Mapping


class MetadataLifecycleStrategy(str, Enum):
  """
  Describe how a missing object is represented during full-scope promotion.
  """
  DELETE = "delete"
  DEACTIVATE = "deactivate"
  RETIRE = "retire"


class MetadataManagedApplyMode(str, Enum):
  """
  Describe how system-managed metadata is handled by promotion apply.
  """
  NORMAL = "normal"
  VERIFY_ONLY = "verify_only"


class MetadataDependencyPhase(IntEnum):
  """
  Stable creation order for metadata objects.
  """
  FOUNDATION = 10
  SOURCE = 20
  SOURCE_CHILD = 25
  SOURCE_RELATION = 30
  TARGET_SCHEMA = 40
  TARGET_DATASET = 50
  QUERY_NODE = 60
  QUERY_DEFINITION = 65
  QUERY_CHILD = 70
  QUERY_BRANCH = 75
  QUERY_MAPPING = 80
  TARGET_INPUT = 90
  TARGET_JOIN = 95
  TARGET_JOIN_PREDICATE = 100
  TARGET_COLUMN = 110
  TARGET_COLUMN_RELATION = 120
  TARGET_REFERENCE = 130
  TARGET_REFERENCE_COMPONENT = 135
  GOVERNANCE_ASSIGNMENT = 140


@dataclass(frozen=True)
class MetadataIdentityContract:
  """
  Declarative logical identity for one metadata model.

  Components contain logical values, not database fields. Exporters resolve
  relationship components to the stable identity of the referenced object.
  """
  components: tuple[str, ...]
  fallback_components: tuple[tuple[str, ...], ...] = ()
  normalize_empty_components: frozenset[str] = field(default_factory=frozenset)

  def __post_init__(self) -> None:
    if not self.components:
      raise ValueError("Metadata identity components must not be empty.")
    all_components = self.components + tuple(
      component
      for fallback in self.fallback_components
      for component in fallback
    )
    forbidden = {
      component
      for component in all_components
      if component in {
        "id",
        "pk",
        "database_id",
        "database_pk",
        "local_id",
        "local_pk",
      }
    }
    if forbidden:
      raise ValueError(
        "Metadata identities must not depend on local database IDs: "
        + ", ".join(sorted(forbidden))
      )


@dataclass(frozen=True)
class MetadataRelationshipContract:
  """
  Portable relationship represented by an implicit Django M2M table.
  """
  name: str
  source_model: str
  source_field: str
  target_model: str
  source_identity_component: str
  target_identity_component: str
  dependency_phase: MetadataDependencyPhase


@dataclass(frozen=True)
class MetadataModelContract:
  """
  Portable field, identity, dependency and lifecycle contract for one model.
  """
  model_name: str
  identity: MetadataIdentityContract
  transport_fields: frozenset[str]
  relationship_fields: frozenset[str]
  excluded_fields: frozenset[str]
  dependency_phase: MetadataDependencyPhase
  lifecycle: MetadataLifecycleStrategy
  transport_filter: tuple[tuple[str, Any], ...] = ()
  deferred_fields: frozenset[str] = field(default_factory=frozenset)
  managed_field: str | None = None
  system_managed_apply_mode: MetadataManagedApplyMode = MetadataManagedApplyMode.NORMAL
  system_managed_update_fields: frozenset[str] = field(default_factory=frozenset)

  def __post_init__(self) -> None:
    field_groups = (
      self.transport_fields,
      self.relationship_fields,
      self.excluded_fields,
    )
    if any(
      left & right
      for index, left in enumerate(field_groups)
      for right in field_groups[index + 1:]
    ):
      raise ValueError(
        f"Metadata field classifications overlap for {self.model_name}."
      )
    unknown_deferred_fields = self.deferred_fields - self.transport_fields
    if unknown_deferred_fields:
      raise ValueError(
        f"Deferred fields are not transported for {self.model_name}: "
        + ", ".join(sorted(unknown_deferred_fields))
      )
    unknown_filter_fields = {
      field_name
      for field_name, _ in self.transport_filter
      if field_name not in self.transport_fields
    }
    if unknown_filter_fields:
      raise ValueError(
        f"Transport filter fields are not transported for {self.model_name}: "
        + ", ".join(sorted(unknown_filter_fields))
      )
    if self.managed_field and self.managed_field not in self.transport_fields:
      raise ValueError(
        f"Managed field {self.managed_field!r} is not transported for "
        f"{self.model_name}."
      )
    if (
      self.system_managed_apply_mode != MetadataManagedApplyMode.NORMAL
      and not self.managed_field
    ):
      raise ValueError(
        f"System-managed apply mode requires a managed field for "
        f"{self.model_name}."
      )
    unknown_system_managed_update_fields = (
      self.system_managed_update_fields - self.transport_fields
    )
    if unknown_system_managed_update_fields:
      raise ValueError(
        f"System-managed update fields are not transported for "
        f"{self.model_name}: "
        + ", ".join(sorted(unknown_system_managed_update_fields))
      )
    if (
      self.system_managed_update_fields
      and self.system_managed_apply_mode != MetadataManagedApplyMode.VERIFY_ONLY
    ):
      raise ValueError(
        f"System-managed update fields require verify-only apply mode for "
        f"{self.model_name}."
      )
    if self.managed_field in self.system_managed_update_fields:
      raise ValueError(
        f"Managed field {self.managed_field!r} cannot be mutable for "
        f"system-managed {self.model_name}."
      )

  @property
  def classified_fields(self) -> frozenset[str]:
    """
    Return every explicitly classified local model field.
    """
    return (
      self.transport_fields
      | self.relationship_fields
      | self.excluded_fields
    )


@dataclass(frozen=True)
class MetadataTransportRegistry:
  """
  Immutable registry of portable models and implicit relationships.
  """
  models: Mapping[str, MetadataModelContract]
  relationships: tuple[MetadataRelationshipContract, ...] = ()

  def __post_init__(self) -> None:
    model_map = dict(self.models)
    mismatches = [
      key
      for key, contract in model_map.items()
      if key != contract.model_name
    ]
    if mismatches:
      raise ValueError(
        "Metadata registry keys must match contract model names: "
        + ", ".join(sorted(mismatches))
      )

    unknown_relationship_models = sorted({
      model_name
      for relationship in self.relationships
      for model_name in (relationship.source_model, relationship.target_model)
      if model_name not in model_map
    })
    if unknown_relationship_models:
      raise ValueError(
        "Metadata relationships reference unregistered models: "
        + ", ".join(unknown_relationship_models)
      )

    relationship_names = [
      relationship.name
      for relationship in self.relationships
    ]
    duplicate_relationship_names = sorted({
      name
      for name in relationship_names
      if relationship_names.count(name) > 1
    })
    if duplicate_relationship_names:
      raise ValueError(
        "Duplicate metadata relationship contracts: "
        + ", ".join(duplicate_relationship_names)
      )

    invalid_source_fields = sorted(
      f"{relationship.name}:{relationship.source_field}"
      for relationship in self.relationships
      if relationship.source_field not in (
        model_map[relationship.source_model].relationship_fields
      )
    )
    if invalid_source_fields:
      raise ValueError(
        "Metadata relationship source fields are not classified as portable "
        "relationships: " + ", ".join(invalid_source_fields)
      )

    object.__setattr__(
      self,
      "models",
      MappingProxyType(model_map),
    )

  def get_model(self, model_name: str) -> MetadataModelContract:
    """
    Return one model contract or fail with a precise error.
    """
    try:
      return self.models[model_name]
    except KeyError as exc:
      raise KeyError(
        f"No metadata transport contract registered for {model_name}."
      ) from exc

  @property
  def creation_order(self) -> tuple[MetadataModelContract, ...]:
    """
    Return contracts in deterministic dependency order.
    """
    return tuple(sorted(
      self.models.values(),
      key=lambda item: (
        int(item.dependency_phase),
        item.model_name,
      ),
    ))
