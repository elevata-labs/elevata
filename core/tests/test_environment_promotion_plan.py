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

from __future__ import annotations

from datetime import datetime, timezone
import json

import pytest

from metadata.promotion.identities import build_metadata_object_identity
from metadata.promotion.model_contracts import (
  METADATA_TRANSPORT_REGISTRY,
  REQUIRED_SYSTEM_MANAGED_TARGET_SCHEMA_KEYS,
)
from metadata.promotion.plan import (
  EnvironmentPromotionActionType,
  EnvironmentPromotionPlanError,
  EnvironmentPromotionReadinessStatus,
  deserialize_environment_promotion_plan,
  render_environment_promotion_plan_text,
  serialize_environment_promotion_plan,
)
from metadata.promotion.planner import build_environment_promotion_plan
from metadata.promotion.release import ArchitectureReleaseBundle
from metadata.promotion.snapshot import (
  EnvironmentMetadataObject,
  EnvironmentMetadataPayload,
  EnvironmentMetadataRelationship,
  EnvironmentMetadataSnapshot,
)


UTC = timezone.utc


def test_environment_promotion_plan_classifies_exact_changes():
  schemas = _required_schema_objects()
  desired_new = _object(
    "System",
    {"short_name": "new"},
    short_name="new",
    name="New",
    active=True,
  )
  desired_updated = _object(
    "System",
    {"short_name": "updated"},
    short_name="updated",
    name="Updated name",
    active=True,
  )
  desired_reactivated = _object(
    "System",
    {"short_name": "reactivated"},
    short_name="reactivated",
    name="Reactivated",
    active=True,
  )
  current_updated = _object(
    "System",
    {"short_name": "updated"},
    short_name="updated",
    name="Old name",
    active=True,
  )
  current_reactivated = _object(
    "System",
    {"short_name": "reactivated"},
    short_name="reactivated",
    name="Reactivated",
    active=False,
  )
  current_retired = _object(
    "System",
    {"short_name": "retired"},
    short_name="retired",
    name="Retired",
    active=True,
  )
  current_deleted = _object(
    "Team",
    {"name": "obsolete"},
    name="obsolete",
    description="Obsolete",
  )

  person_add = _object(
    "Person",
    {"email": "add@example.com"},
    email="add@example.com",
    name="Add",
  )
  team_add = _object(
    "Team",
    {"name": "add-team"},
    name="add-team",
    description="Add",
  )
  person_remove = _object(
    "Person",
    {"email": "remove@example.com"},
    email="remove@example.com",
    name="Remove",
  )
  team_remove = _object(
    "Team",
    {"name": "remove-team"},
    name="remove-team",
    description="Remove",
  )
  add_relationship = _relationship(
    "PersonTeamMembership",
    person_add,
    team_add,
  )
  remove_relationship = _relationship(
    "PersonTeamMembership",
    person_remove,
    team_remove,
  )

  desired = _snapshot(
    "dev",
    objects=(
      *schemas,
      desired_new,
      desired_updated,
      desired_reactivated,
      person_add,
      team_add,
      person_remove,
      team_remove,
    ),
    relationships=(add_relationship,),
  )
  current = _snapshot(
    "prod",
    objects=(
      *schemas,
      current_updated,
      current_reactivated,
      current_retired,
      current_deleted,
      person_add,
      team_add,
      person_remove,
      team_remove,
    ),
    relationships=(remove_relationship,),
  )
  plan = build_environment_promotion_plan(
    bundle=_bundle(desired),
    target_snapshot=current,
    created_at=datetime(2026, 8, 4, 4, 0, tzinfo=UTC),
  )

  action_types = [item.action_type for item in plan.actions]
  assert EnvironmentPromotionActionType.CREATE in action_types
  assert EnvironmentPromotionActionType.UPDATE in action_types
  assert EnvironmentPromotionActionType.REACTIVATE in action_types
  assert EnvironmentPromotionActionType.RETIRE in action_types
  assert EnvironmentPromotionActionType.DELETE in action_types
  assert EnvironmentPromotionActionType.ADD_RELATIONSHIP in action_types
  assert EnvironmentPromotionActionType.REMOVE_RELATIONSHIP in action_types
  assert action_types.count(EnvironmentPromotionActionType.VERIFY) == 5
  assert plan.readiness.status == EnvironmentPromotionReadinessStatus.READY
  assert plan.readiness.can_apply is True

  update = next(
    item
    for item in plan.actions
    if (
      item.action_type == EnvironmentPromotionActionType.UPDATE
      and item.desired_key == desired_updated.object_key
    )
  )
  assert update.changed_fields == ("name",)
  assert plan.summary["mutating_action_count"] == 7


def test_environment_promotion_plan_blocks_verify_only_schema_drift():
  desired_schemas = _required_schema_objects()
  current_schemas = list(_required_schema_objects())
  raw = next(
    item
    for item in current_schemas
    if item.fields["short_name"] == "raw"
  )
  current_schemas[current_schemas.index(raw)] = _object(
    "TargetSchema",
    {"short_name": "raw"},
    short_name="raw",
    schema_name="wrong_raw",
    is_system_managed=True,
  )

  plan = build_environment_promotion_plan(
    bundle=_bundle(_snapshot("dev", objects=desired_schemas)),
    target_snapshot=_snapshot("prod", objects=tuple(current_schemas)),
  )

  assert plan.readiness.status == EnvironmentPromotionReadinessStatus.BLOCKED
  assert plan.readiness.can_apply is False
  assert any(
    issue.code == "verify_only_object_mismatch"
    for issue in plan.readiness.issues
  )
  verify = next(
    item
    for item in plan.actions
    if item.desired_key == raw.object_key
  )
  assert verify.action_type == EnvironmentPromotionActionType.VERIFY
  assert verify.blocked is True
  assert verify.changed_fields == ("schema_name",)


def test_environment_promotion_plan_updates_editable_system_schema_fields():
  desired_schemas = list(_required_schema_objects())
  current_schemas = list(_required_schema_objects())
  desired_stage = next(
    item
    for item in desired_schemas
    if item.fields["short_name"] == "stage"
  )
  current_stage = next(
    item
    for item in current_schemas
    if item.fields["short_name"] == "stage"
  )
  desired_stage = _object(
    "TargetSchema",
    {"short_name": "stage"},
    short_name="stage",
    schema_name="stage",
    display_name="Staging",
    description="Lightly standardized source data",
    sensitivity_default="confidential",
    access_intent_default="analytics",
    is_system_managed=True,
  )
  desired_schemas[1] = desired_stage
  current_schemas[current_schemas.index(current_stage)] = _object(
    "TargetSchema",
    {"short_name": "stage"},
    short_name="stage",
    schema_name="stage",
    display_name="Stage",
    description="Old description",
    sensitivity_default="public",
    access_intent_default="",
    is_system_managed=True,
  )

  plan = build_environment_promotion_plan(
    bundle=_bundle(_snapshot("dev", objects=tuple(desired_schemas))),
    target_snapshot=_snapshot("prod", objects=tuple(current_schemas)),
  )

  stage_actions = [
    item
    for item in plan.actions
    if item.desired_key == desired_stage.object_key
  ]
  verify = next(
    item
    for item in stage_actions
    if item.action_type == EnvironmentPromotionActionType.VERIFY
  )
  update = next(
    item
    for item in stage_actions
    if item.action_type == EnvironmentPromotionActionType.UPDATE
  )
  assert verify.blocked is False
  assert verify.changed_fields == ()
  assert update.changed_fields == (
    "access_intent_default",
    "description",
    "display_name",
    "sensitivity_default",
  )
  assert plan.readiness.status == EnvironmentPromotionReadinessStatus.READY
  assert plan.readiness.can_apply is True


def test_environment_promotion_plan_uses_former_names_and_normalizes_children():
  schemas = _required_schema_objects()
  serving = next(
    item for item in schemas if item.fields["short_name"] == "serving"
  )
  current_dataset = _target_dataset(
    schema=serving,
    name="customer_old",
    former_names=(),
  )
  desired_dataset = _target_dataset(
    schema=serving,
    name="customer",
    former_names=("customer_old",),
  )
  current_column = _target_column(
    dataset=current_dataset,
    name="customer_no",
    lineage_key="manual:customer:no",
  )
  desired_column = _target_column(
    dataset=desired_dataset,
    name="customer_no",
    lineage_key="manual:customer:no",
  )

  plan = build_environment_promotion_plan(
    bundle=_bundle(_snapshot(
      "dev",
      objects=(*schemas, desired_dataset, desired_column),
    )),
    target_snapshot=_snapshot(
      "prod",
      objects=(*schemas, current_dataset, current_column),
    ),
  )

  dataset_action = next(
    item
    for item in plan.actions
    if item.subject_name == "TargetDataset"
  )
  assert dataset_action.action_type == EnvironmentPromotionActionType.UPDATE
  assert dataset_action.current_key == current_dataset.object_key
  assert dataset_action.desired_key == desired_dataset.object_key
  assert "target_dataset_name" in dataset_action.changed_fields
  assert not any(
    item.subject_name == "TargetColumn" and item.is_mutating
    for item in plan.actions
  )
  assert plan.readiness.status == EnvironmentPromotionReadinessStatus.READY


def test_environment_promotion_plan_blocks_ambiguous_former_names():
  schemas = _required_schema_objects()
  serving = next(
    item for item in schemas if item.fields["short_name"] == "serving"
  )
  desired_dataset = _target_dataset(
    schema=serving,
    name="customer",
    former_names=("customer_a", "customer_b"),
  )
  current_a = _target_dataset(schema=serving, name="customer_a")
  current_b = _target_dataset(schema=serving, name="customer_b")

  plan = build_environment_promotion_plan(
    bundle=_bundle(_snapshot(
      "dev",
      objects=(*schemas, desired_dataset),
    )),
    target_snapshot=_snapshot(
      "prod",
      objects=(*schemas, current_a, current_b),
    ),
  )

  assert plan.readiness.status == EnvironmentPromotionReadinessStatus.BLOCKED
  assert any(
    issue.code == "ambiguous_former_name_match"
    for issue in plan.readiness.issues
  )


def test_environment_promotion_plan_roundtrip_is_stable_and_detects_tampering():
  schemas = _required_schema_objects()
  desired = _snapshot("dev", objects=schemas)
  current = _snapshot("prod", objects=schemas)
  bundle = _bundle(desired)

  first = build_environment_promotion_plan(
    bundle=bundle,
    target_snapshot=current,
    created_at=datetime(2026, 8, 4, 4, 0, tzinfo=UTC),
  )
  second = build_environment_promotion_plan(
    bundle=bundle,
    target_snapshot=current,
    created_at=datetime(2026, 8, 4, 5, 0, tzinfo=UTC),
  )

  assert first.plan_fingerprint == second.plan_fingerprint
  assert first.readiness.status == EnvironmentPromotionReadinessStatus.NO_CHANGES
  rendered = serialize_environment_promotion_plan(first)
  restored = deserialize_environment_promotion_plan(rendered)
  assert serialize_environment_promotion_plan(restored) == rendered
  assert "Readiness: no_changes" in render_environment_promotion_plan_text(first)

  tampered = json.loads(rendered)
  tampered["target_environment_label"] = "test"
  with pytest.raises(
    EnvironmentPromotionPlanError,
    match="promotion plan fingerprint mismatch",
  ):
    deserialize_environment_promotion_plan(json.dumps(tampered))


def _required_schema_objects() -> tuple[EnvironmentMetadataObject, ...]:
  return tuple(
    _object(
      "TargetSchema",
      {"short_name": short_name},
      short_name=short_name,
      schema_name=short_name,
      is_system_managed=True,
    )
    for short_name in REQUIRED_SYSTEM_MANAGED_TARGET_SCHEMA_KEYS
  )


def _target_dataset(
  *,
  schema: EnvironmentMetadataObject,
  name: str,
  former_names=(),
) -> EnvironmentMetadataObject:
  return _object(
    "TargetDataset",
    {
      "target_schema_key": schema.object_key,
      "target_dataset_name": name,
    },
    target_schema=schema.object_key,
    target_dataset_name=name,
    lineage_key="",
    former_names=former_names,
    active=True,
    is_system_managed=False,
  )


def _target_column(
  *,
  dataset: EnvironmentMetadataObject,
  name: str,
  lineage_key: str,
) -> EnvironmentMetadataObject:
  return _object(
    "TargetColumn",
    {
      "target_dataset_key": dataset.object_key,
      "lineage_key": lineage_key,
    },
    target_dataset=dataset.object_key,
    target_column_name=name,
    lineage_key=lineage_key,
    former_names=(),
    active=True,
    is_system_managed=False,
  )


def _object(
  model_name: str,
  identity_values: dict,
  **field_values,
) -> EnvironmentMetadataObject:
  contract = METADATA_TRANSPORT_REGISTRY.get_model(model_name)
  fields = {
    field_name: None
    for field_name in contract.transport_fields
  }
  fields.update(field_values)
  identity = build_metadata_object_identity(
    model_name=model_name,
    contract=contract.identity,
    values=identity_values,
  )
  return EnvironmentMetadataObject(
    model_name=model_name,
    identity=identity,
    fields=fields,
  )


def _relationship(
  relationship_name: str,
  source: EnvironmentMetadataObject,
  target: EnvironmentMetadataObject,
) -> EnvironmentMetadataRelationship:
  contract = next(
    item
    for item in METADATA_TRANSPORT_REGISTRY.relationships
    if item.name == relationship_name
  )
  return EnvironmentMetadataRelationship(
    relationship_name=relationship_name,
    source_model=contract.source_model,
    source_key=source.object_key,
    target_model=contract.target_model,
    target_key=target.object_key,
  )


def _snapshot(
  environment_label: str,
  *,
  objects=(),
  relationships=(),
) -> EnvironmentMetadataSnapshot:
  return EnvironmentMetadataSnapshot(
    environment_label=environment_label,
    created_at=datetime(2026, 8, 4, 3, 0, tzinfo=UTC),
    created_by="test@example.com",
    metadata=EnvironmentMetadataPayload(
      objects=tuple(objects),
      relationships=tuple(relationships),
    ),
  )


def _bundle(snapshot: EnvironmentMetadataSnapshot) -> ArchitectureReleaseBundle:
  return ArchitectureReleaseBundle(
    release_name="customer-platform",
    release_version="1.0.0",
    created_at=datetime(2026, 8, 4, 3, 30, tzinfo=UTC),
    created_by="test@example.com",
    snapshot=snapshot,
  )
