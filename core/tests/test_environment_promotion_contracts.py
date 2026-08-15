"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2026 Ilona Tag

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

from django.apps import apps
from django.db.models import Q

from metadata.promotion.contracts import (
  MetadataLifecycleStrategy,
  MetadataManagedApplyMode,
)
from metadata.promotion.model_contracts import (
  METADATA_TRANSPORT_REGISTRY,
  REQUIRED_SYSTEM_MANAGED_TARGET_SCHEMA_KEYS,
)
from metadata.models import (
  QueryNode,
  SourceDatasetGroup,
  SourceDatasetIncrementPolicy,
  TargetSchema,
)


def _portable_metadata_models():
  return {
    model.__name__: model
    for model in apps.get_app_config("metadata").get_models()
    if not model._meta.abstract
  }


def _local_field_names(model) -> set[str]:
  return {
    field.name
    for field in (
      tuple(model._meta.local_fields)
      + tuple(model._meta.local_many_to_many)
    )
  }


def test_model_foundation_matches_transport_contract():
  target_schema_fields = {
    field.name
    for field in TargetSchema._meta.local_fields
  }
  assert "database_name" not in target_schema_fields

  logical_key = QueryNode._meta.get_field("logical_key")
  assert logical_key.unique is True
  assert logical_key.editable is False

  group_constraints = {
    constraint.name: constraint
    for constraint in SourceDatasetGroup._meta.constraints
  }
  assert "unique_source_dataset_group" in group_constraints

  policy_constraints = {
    constraint.name: constraint
    for constraint in SourceDatasetIncrementPolicy._meta.constraints
  }
  active_policy_constraint = policy_constraints[
    "unique_active_increment_policy_per_env"
  ]
  assert active_policy_constraint.fields == ("source_dataset", "environment")
  assert active_policy_constraint.condition == Q(active=True)


def test_every_metadata_model_has_a_transport_contract():
  models = _portable_metadata_models()

  assert set(METADATA_TRANSPORT_REGISTRY.models) == set(models)


def test_every_local_model_field_is_explicitly_classified():
  models = _portable_metadata_models()

  for model_name, model in models.items():
    contract = METADATA_TRANSPORT_REGISTRY.get_model(model_name)
    assert contract.classified_fields == _local_field_names(model), model_name


def test_local_ids_and_audit_fields_are_excluded_from_every_contract():
  forbidden = {
    "id",
    "created_at",
    "updated_at",
    "created_by",
    "updated_by",
  }

  for contract in METADATA_TRANSPORT_REGISTRY.models.values():
    assert forbidden <= contract.excluded_fields
    assert not forbidden & contract.transport_fields
    assert not forbidden & contract.relationship_fields


def test_increment_policy_transport_contains_only_active_definitions():
  contract = METADATA_TRANSPORT_REGISTRY.get_model(
    "SourceDatasetIncrementPolicy"
  )

  assert contract.transport_filter == (("active", True),)


def test_target_schema_is_portable_but_system_managed_rows_are_verify_only():
  contract = METADATA_TRANSPORT_REGISTRY.get_model("TargetSchema")

  assert contract.identity.components == ("short_name",)
  assert contract.managed_field == "is_system_managed"
  assert contract.system_managed_apply_mode == MetadataManagedApplyMode.VERIFY_ONLY
  assert "database_name" not in contract.classified_fields
  assert REQUIRED_SYSTEM_MANAGED_TARGET_SCHEMA_KEYS == (
    "raw",
    "stage",
    "rawcore",
    "bizcore",
    "serving",
  )


def test_creation_order_respects_nested_dependencies():
  ordered_names = [
    contract.model_name
    for contract in METADATA_TRANSPORT_REGISTRY.creation_order
  ]
  position = {
    model_name: index
    for index, model_name in enumerate(ordered_names)
  }

  assert position["SourceDataset"] < position["SourceColumn"]
  assert position["SourceDataset"] < position["SourceDatasetIncrementPolicy"]
  assert position["TargetDataset"] < position["QueryNode"]
  assert position["QueryNode"] < position["QueryUnionNode"]
  assert position["QueryUnionOutputColumn"] < position["QueryUnionBranchMapping"]
  assert position["QueryUnionBranch"] < position["QueryUnionBranchMapping"]
  assert position["QueryWindowColumn"] < position["QueryWindowColumnArg"]
  assert position["TargetDatasetInput"] < position["TargetDatasetJoin"]
  assert position["TargetDatasetJoin"] < position["TargetDatasetJoinPredicate"]
  assert position["TargetColumn"] < position["TargetDatasetReference"]
  assert position["TargetDatasetReference"] < position[
    "TargetDatasetReferenceComponent"
  ]


def test_target_dataset_query_links_are_deferred():
  contract = METADATA_TRANSPORT_REGISTRY.get_model("TargetDataset")

  assert contract.deferred_fields == frozenset({"query_root", "query_head"})


def test_lifecycle_contract_matches_model_capabilities():
  assert (
    METADATA_TRANSPORT_REGISTRY.get_model("TargetDataset").lifecycle
    == MetadataLifecycleStrategy.RETIRE
  )
  assert (
    METADATA_TRANSPORT_REGISTRY.get_model("TargetDatasetInput").lifecycle
    == MetadataLifecycleStrategy.DEACTIVATE
  )
  assert (
    METADATA_TRANSPORT_REGISTRY.get_model("TargetDatasetJoinPredicate").lifecycle
    == MetadataLifecycleStrategy.DELETE
  )


def test_implicit_relationships_are_explicit_and_stable():
  relationships = {
    relationship.name: relationship
    for relationship in METADATA_TRANSPORT_REGISTRY.relationships
  }

  assert set(relationships) == {
    "PersonTeamMembership",
    "SourceDatasetGroupOwner",
    "TargetDatasetPartialLoadAssignment",
  }
  assert relationships["TargetDatasetPartialLoadAssignment"].source_model == (
    "TargetDataset"
  )
  assert relationships["PersonTeamMembership"].source_field == "team"
  assert relationships["SourceDatasetGroupOwner"].source_field == "owner"
  assert relationships["TargetDatasetPartialLoadAssignment"].source_field == (
    "partial_load"
  )
