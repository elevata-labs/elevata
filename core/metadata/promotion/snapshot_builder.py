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

from collections.abc import Iterable
from datetime import datetime
from typing import Any

from django.apps import apps
from django.core.exceptions import FieldDoesNotExist
from django.db import transaction
from django.db.models import Model
from django.utils import timezone

from metadata.promotion.canonical import canonicalize_metadata_value
from metadata.promotion.contracts import (
  MetadataModelContract,
  MetadataTransportRegistry,
)
from metadata.promotion.identities import (
  MetadataObjectIdentity,
  build_metadata_object_identity,
)
from metadata.promotion.model_contracts import (
  METADATA_TRANSPORT_REGISTRY,
)
from metadata.promotion.snapshot import (
  EnvironmentMetadataObject,
  EnvironmentMetadataPayload,
  EnvironmentMetadataRelationship,
  EnvironmentMetadataSnapshot,
)


class EnvironmentMetadataSnapshotBuildError(RuntimeError):
  """
  Raised when the local metadata graph cannot form a portable snapshot.
  """


class MetadataIdentityResolver:
  """
  Resolve stable object identities from Django model instances.
  """

  def __init__(
    self,
    registry: MetadataTransportRegistry = METADATA_TRANSPORT_REGISTRY,
  ) -> None:
    self.registry = registry
    self._cache: dict[tuple[str, Any], MetadataObjectIdentity] = {}
    self._resolving: set[tuple[str, Any]] = set()

  def identity_for(self, instance: Model) -> MetadataObjectIdentity:
    """
    Return the stable logical identity of one persisted metadata object.
    """
    model_name = instance.__class__.__name__
    contract = self.registry.get_model(model_name)
    cache_key = self._cache_key(instance)

    cached = self._cache.get(cache_key)
    if cached is not None:
      return cached
    if cache_key in self._resolving:
      raise EnvironmentMetadataSnapshotBuildError(
        f"Cyclic metadata identity dependency detected for {model_name}."
      )

    self._resolving.add(cache_key)
    try:
      component_names = _identity_component_names(contract)
      values = {
        component: self._resolve_component(instance, component)
        for component in component_names
      }
      identity = build_metadata_object_identity(
        model_name=model_name,
        contract=contract.identity,
        values=values,
      )
      self._cache[cache_key] = identity
      return identity
    except (AttributeError, FieldDoesNotExist, ValueError) as exc:
      raise EnvironmentMetadataSnapshotBuildError(
        f"Cannot resolve portable identity for {model_name}(pk={instance.pk}): "
        f"{exc}"
      ) from exc
    finally:
      self._resolving.discard(cache_key)

  def object_key_for(self, instance: Model | None) -> str | None:
    """
    Return a portable object key for a related model instance.
    """
    if instance is None:
      return None
    return self.identity_for(instance).object_key

  def _resolve_component(self, instance: Model, component: str) -> Any:
    if component == "dataset_variant":
      if instance.__class__.__name__ != "TargetDataset":
        raise AttributeError(
          "dataset_variant is only available for TargetDataset"
        )
      return "hist" if bool(getattr(instance, "is_hist", False)) else "base"

    if component in {"upstream_kind", "upstream_key"}:
      upstream_kind, upstream_object = _resolve_upstream(instance)
      if component == "upstream_kind":
        return upstream_kind
      return self.object_key_for(upstream_object)

    field = _get_model_field(instance, component)
    if field is not None:
      return _identity_field_value(
        instance=instance,
        field_name=component,
        resolver=self,
      )

    if component.endswith("_key"):
      relation_name = component[:-4]
      relation_field = _get_model_field(instance, relation_name)
      if relation_field is None or not relation_field.is_relation:
        raise AttributeError(
          f"identity relation {relation_name!r} is unavailable"
        )
      return self.object_key_for(getattr(instance, relation_name))

    for suffix, target_attribute in (
      ("_email", "email"),
      ("_name", "name"),
    ):
      if component.endswith(suffix):
        relation_name = component[:-len(suffix)]
        relation_field = _get_model_field(instance, relation_name)
        if relation_field is None or not relation_field.is_relation:
          raise AttributeError(
            f"identity relation {relation_name!r} is unavailable"
          )
        related = getattr(instance, relation_name)
        return getattr(related, target_attribute)

    raise AttributeError(
      f"identity component {component!r} is unavailable"
    )

  @staticmethod
  def _cache_key(instance: Model) -> tuple[str, Any]:
    if instance.pk is None:
      raise EnvironmentMetadataSnapshotBuildError(
        f"Cannot snapshot unsaved {instance.__class__.__name__} instance."
      )
    return (instance.__class__.__name__, instance.pk)


class EnvironmentMetadataSnapshotBuilder:
  """
  Build a complete canonical snapshot from the local metadata database.
  """

  def __init__(
    self,
    registry: MetadataTransportRegistry = METADATA_TRANSPORT_REGISTRY,
  ) -> None:
    self.registry = registry

  def build(
    self,
    *,
    environment_label: str,
    created_by: str | None = None,
    created_at: datetime | None = None,
  ) -> EnvironmentMetadataSnapshot:
    """
    Export the complete portable metadata definition in one read transaction.
    """
    resolver = MetadataIdentityResolver(self.registry)
    objects: list[EnvironmentMetadataObject] = []
    instances_by_model: dict[str, list[Model]] = {}

    with transaction.atomic():
      for contract in self.registry.creation_order:
        model = apps.get_model("metadata", contract.model_name)
        queryset = model._default_manager.all()
        for field_name, expected_value in contract.transport_filter:
          queryset = queryset.filter(**{field_name: expected_value})

        model_instances = list(queryset)
        instances_by_model[contract.model_name] = model_instances
        objects.extend(
          self._serialize_object(
            instance=instance,
            contract=contract,
            resolver=resolver,
          )
          for instance in model_instances
        )

      object_keys = {item.object_key for item in objects}
      relationships = tuple(self._serialize_relationships(
        instances_by_model=instances_by_model,
        object_keys=object_keys,
        resolver=resolver,
      ))

    return EnvironmentMetadataSnapshot(
      environment_label=environment_label,
      created_at=created_at or timezone.now(),
      created_by=created_by,
      metadata=EnvironmentMetadataPayload(
        objects=tuple(objects),
        relationships=relationships,
      ),
    )

  def _serialize_object(
    self,
    *,
    instance: Model,
    contract: MetadataModelContract,
    resolver: MetadataIdentityResolver,
  ) -> EnvironmentMetadataObject:
    identity = resolver.identity_for(instance)
    fields = {
      field_name: _serialize_transport_field(
        instance=instance,
        field_name=field_name,
        contract=contract,
        resolver=resolver,
      )
      for field_name in sorted(contract.transport_fields)
    }
    return EnvironmentMetadataObject(
      model_name=contract.model_name,
      identity=identity,
      fields=fields,
    )

  def _serialize_relationships(
    self,
    *,
    instances_by_model: dict[str, list[Model]],
    object_keys: set[str],
    resolver: MetadataIdentityResolver,
  ) -> Iterable[EnvironmentMetadataRelationship]:
    for contract in self.registry.relationships:
      for source in instances_by_model.get(contract.source_model, []):
        source_key = resolver.object_key_for(source)
        if source_key is None:
          continue
        manager = getattr(source, contract.source_field)
        for target in manager.all():
          target_key = resolver.object_key_for(target)
          if target_key is None:
            continue
          if source_key not in object_keys or target_key not in object_keys:
            raise EnvironmentMetadataSnapshotBuildError(
              f"Portable relationship {contract.name} references an "
              "object excluded from the snapshot."
            )
          yield EnvironmentMetadataRelationship(
            relationship_name=contract.name,
            source_model=contract.source_model,
            source_key=source_key,
            target_model=contract.target_model,
            target_key=target_key,
          )


def build_environment_metadata_snapshot(
  *,
  environment_label: str,
  created_by: str | None = None,
  created_at: datetime | None = None,
) -> EnvironmentMetadataSnapshot:
  """
  Build the current environment metadata snapshot.
  """
  return EnvironmentMetadataSnapshotBuilder().build(
    environment_label=environment_label,
    created_by=created_by,
    created_at=created_at,
  )


def _serialize_transport_field(
  *,
  instance: Model,
  field_name: str,
  contract: MetadataModelContract,
  resolver: MetadataIdentityResolver,
) -> Any:
  field = instance._meta.get_field(field_name)
  if field.is_relation:
    value = resolver.object_key_for(getattr(instance, field_name))
  else:
    value = getattr(instance, field_name)

  if (
    field_name in contract.identity.normalize_empty_components
    and value is None
  ):
    value = ""
  if field_name == "former_names":
    value = sorted({
      str(item)
      for item in (value or [])
      if str(item)
    })

  return canonicalize_metadata_value(value)


def _identity_field_value(
  *,
  instance: Model,
  field_name: str,
  resolver: MetadataIdentityResolver,
) -> Any:
  field = instance._meta.get_field(field_name)
  if field.is_relation:
    return resolver.object_key_for(getattr(instance, field_name))
  return getattr(instance, field_name)


def _get_model_field(instance: Model, field_name: str):
  try:
    return instance._meta.get_field(field_name)
  except FieldDoesNotExist:
    return None


def _identity_component_names(
  contract: MetadataModelContract,
) -> tuple[str, ...]:
  ordered: list[str] = []
  for candidate in (
    (contract.identity.components,)
    + contract.identity.fallback_components
  ):
    for component in candidate:
      if component not in ordered:
        ordered.append(component)
  return tuple(ordered)


def _resolve_upstream(instance: Model) -> tuple[str, Model]:
  candidates = []
  for field_name, kind in (
    ("source_dataset", "source_dataset"),
    ("upstream_target_dataset", "target_dataset"),
    ("source_column", "source_column"),
    ("upstream_target_column", "target_column"),
  ):
    if _get_model_field(instance, field_name) is None:
      continue
    related_id = getattr(instance, f"{field_name}_id", None)
    if related_id is not None:
      candidates.append((kind, getattr(instance, field_name)))

  if len(candidates) != 1:
    raise EnvironmentMetadataSnapshotBuildError(
      f"{instance.__class__.__name__} requires exactly one portable upstream; "
      f"found {len(candidates)}."
    )
  return candidates[0]
