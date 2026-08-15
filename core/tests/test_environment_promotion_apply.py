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
from uuid import uuid4

import pytest

from metadata.models import (
  Person,
  QueryNode,
  SourceDataset,
  System,
  TargetColumn,
  TargetDataset,
  TargetSchema,
  Team,
)
from metadata.promotion.approval import build_environment_promotion_approval
from metadata.promotion.apply import (
  EnvironmentPromotionActionApplyError,
  EnvironmentPromotionApplyError,
  EnvironmentPromotionDriftError,
  EnvironmentPromotionExecutor,
  apply_environment_promotion_plan,
)
from metadata.promotion.identities import build_metadata_object_identity
from metadata.promotion.model_contracts import METADATA_TRANSPORT_REGISTRY
from metadata.promotion.planner import build_environment_promotion_plan
from metadata.promotion.record import (
  deserialize_environment_promotion_record,
  serialize_environment_promotion_record,
)
from metadata.promotion.record_store import EnvironmentPromotionRecordStore
from metadata.promotion.release import ArchitectureReleaseBundle
from metadata.promotion.snapshot import (
  EnvironmentMetadataObject,
  EnvironmentMetadataPayload,
  EnvironmentMetadataRelationship,
  EnvironmentMetadataSnapshot,
)
from metadata.promotion.snapshot_builder import (
  build_environment_metadata_snapshot,
)


UTC = timezone.utc


@pytest.mark.django_db
def test_guarded_environment_promotion_apply_converges_and_records_history(
  tmp_path,
):
  fixture = _build_apply_fixture()
  store = EnvironmentPromotionRecordStore(tmp_path)

  record = apply_environment_promotion_plan(
    plan=fixture["plan"],
    bundle=fixture["bundle"],
    approval=fixture["approval"],
    applied_by="deployer@example.com",
    runtime_environment_label="prod",
    applied_at=datetime(2026, 8, 5, 4, 0, tzinfo=UTC),
    record_store=store,
  )

  fixture["existing_team"].refresh_from_db()
  fixture["retired_system"].refresh_from_db()
  fixture["stage_schema"].refresh_from_db()
  new_team = Team.objects.get(name=fixture["new_team_name"])
  fixture["person"].refresh_from_db()

  assert fixture["existing_team"].description == "Promoted description"
  assert fixture["retired_system"].active is False
  assert fixture["retired_system"].retired_at is not None
  assert fixture["stage_schema"].description == (
    fixture["promoted_stage_description"]
  )
  assert fixture["person"].team.filter(pk=new_team.pk).exists()

  assert {
    item.action_id
    for item in record.action_results
  } == {
    item.action_id
    for item in fixture["plan"].actions
  }
  assert record.summary["action_count"] == len(fixture["plan"].actions)
  assert record.post_target_snapshot_fingerprint != (
    record.pre_target_snapshot_fingerprint
  )

  stored = store.load(
    target_environment_label="prod",
    record_id=record.record_id,
  )
  assert stored == record
  rendered = serialize_environment_promotion_record(record)
  assert deserialize_environment_promotion_record(rendered) == record


@pytest.mark.django_db
def test_environment_promotion_apply_preserves_children_across_dataset_rename(
  tmp_path,
):
  suffix = uuid4().hex[:6]
  schema = TargetSchema.objects.get(short_name="serving")
  dataset = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=f"old_{suffix}",
  )
  column = TargetColumn.objects.create(
    target_dataset=dataset,
    target_column_name="customer_no",
    ordinal_position=1,
    datatype="STRING",
    lineage_key=f"manual:rename:{suffix}:customer_no",
  )
  target_snapshot = build_environment_metadata_snapshot(
    environment_label="prod",
    created_at=datetime(2026, 8, 5, 2, 0, tzinfo=UTC),
  )
  current_dataset = next(
    item
    for item in target_snapshot.metadata.objects
    if (
      item.model_name == "TargetDataset"
      and item.fields["target_dataset_name"] == dataset.target_dataset_name
    )
  )
  current_column = next(
    item
    for item in target_snapshot.metadata.objects
    if (
      item.model_name == "TargetColumn"
      and item.fields["target_column_name"] == column.target_column_name
      and item.fields["target_dataset"] == current_dataset.object_key
    )
  )
  new_name = f"renamed_{suffix}"
  desired_dataset = _renamed_target_dataset_object(
    current=current_dataset,
    new_name=new_name,
  )
  desired_column = _reparented_target_column_object(
    current=current_column,
    target_dataset=desired_dataset,
  )
  desired_objects = tuple(
    desired_dataset
    if item.object_key == current_dataset.object_key
    else (
      desired_column
      if item.object_key == current_column.object_key
      else item
    )
    for item in target_snapshot.metadata.objects
  )
  source_snapshot = EnvironmentMetadataSnapshot(
    environment_label="dev",
    created_at=datetime(2026, 8, 5, 2, 30, tzinfo=UTC),
    metadata=EnvironmentMetadataPayload(
      objects=desired_objects,
      relationships=target_snapshot.metadata.relationships,
    ),
  )
  bundle = ArchitectureReleaseBundle(
    release_name=f"rename-{suffix}",
    release_version="1.0.0",
    created_at=datetime(2026, 8, 5, 2, 45, tzinfo=UTC),
    created_by="release@example.com",
    snapshot=source_snapshot,
  )
  plan = build_environment_promotion_plan(
    bundle=bundle,
    target_snapshot=target_snapshot,
  )
  assert not any(
    item.subject_name == "TargetColumn" and item.is_mutating
    for item in plan.actions
  )
  approval = build_environment_promotion_approval(
    plan=plan,
    decided_by="reviewer@example.com",
  )

  apply_environment_promotion_plan(
    plan=plan,
    bundle=bundle,
    approval=approval,
    applied_by="deployer@example.com",
    runtime_environment_label="prod",
    record_store=EnvironmentPromotionRecordStore(tmp_path),
  )

  dataset.refresh_from_db()
  column.refresh_from_db()
  assert dataset.target_dataset_name == new_name
  assert column.target_dataset_id == dataset.pk
  assert column.target_column_name == "customer_no"


@pytest.mark.django_db
def test_environment_promotion_apply_resolves_deferred_query_links(tmp_path):
  suffix = uuid4().hex[:6]
  schema = TargetSchema.objects.get(short_name="serving")
  dataset = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=f"deferred_{suffix}",
    lineage_key=f"manual:deferred:{suffix}",
  )
  target_snapshot = build_environment_metadata_snapshot(
    environment_label="prod",
    created_at=datetime(2026, 8, 5, 2, 0, tzinfo=UTC),
  )
  current_dataset = next(
    item
    for item in target_snapshot.metadata.objects
    if (
      item.model_name == "TargetDataset"
      and item.fields["target_dataset_name"] == dataset.target_dataset_name
    )
  )
  logical_key = str(uuid4())
  query_node = _query_node_object(
    target_dataset=current_dataset,
    logical_key=logical_key,
  )
  dataset_fields = dict(current_dataset.fields)
  dataset_fields["query_root"] = query_node.object_key
  dataset_fields["query_head"] = query_node.object_key
  desired_dataset = EnvironmentMetadataObject(
    model_name=current_dataset.model_name,
    identity=current_dataset.identity,
    fields=dataset_fields,
  )
  desired_objects = tuple(
    desired_dataset if item.object_key == current_dataset.object_key else item
    for item in target_snapshot.metadata.objects
  ) + (query_node,)
  source_snapshot = EnvironmentMetadataSnapshot(
    environment_label="dev",
    created_at=datetime(2026, 8, 5, 2, 30, tzinfo=UTC),
    metadata=EnvironmentMetadataPayload(
      objects=desired_objects,
      relationships=target_snapshot.metadata.relationships,
    ),
  )
  bundle = ArchitectureReleaseBundle(
    release_name=f"deferred-{suffix}",
    release_version="1.0.0",
    created_at=datetime(2026, 8, 5, 2, 45, tzinfo=UTC),
    created_by="release@example.com",
    snapshot=source_snapshot,
  )
  plan = build_environment_promotion_plan(
    bundle=bundle,
    target_snapshot=target_snapshot,
  )
  approval = build_environment_promotion_approval(
    plan=plan,
    decided_by="reviewer@example.com",
  )

  apply_environment_promotion_plan(
    plan=plan,
    bundle=bundle,
    approval=approval,
    applied_by="deployer@example.com",
    runtime_environment_label="prod",
    record_store=EnvironmentPromotionRecordStore(tmp_path),
  )

  dataset.refresh_from_db()
  node = QueryNode.objects.get(logical_key=logical_key)
  assert dataset.query_root_id == node.pk
  assert dataset.query_head_id == node.pk


@pytest.mark.django_db
def test_guarded_environment_promotion_apply_requires_matching_runtime_environment(
  tmp_path,
):
  fixture = _build_apply_fixture()

  with pytest.raises(
    EnvironmentPromotionApplyError,
    match="target does not match the local runtime environment",
  ):
    apply_environment_promotion_plan(
      plan=fixture["plan"],
      bundle=fixture["bundle"],
      approval=fixture["approval"],
      applied_by="deployer@example.com",
      runtime_environment_label="test",
      record_store=EnvironmentPromotionRecordStore(tmp_path),
    )

  fixture["existing_team"].refresh_from_db()
  assert fixture["existing_team"].description == "Original description"


@pytest.mark.django_db
def test_environment_promotion_apply_thaws_nested_ingestion_config(tmp_path):
  suffix = uuid4().hex[:6]
  system = System.objects.create(
    short_name=f"f{suffix}",
    name="File source",
    type="csv",
    target_short_name=f"f{suffix}",
  )
  source_dataset = SourceDataset.objects.create(
    source_system=system,
    source_dataset_name=f"orders_{suffix}",
    ingestion_config={
      "uri": "${ORDERS_SOURCE_URI}",
      "options": {"delimiter": ",", "headers": ["order_id"]},
    },
  )
  target_snapshot = build_environment_metadata_snapshot(
    environment_label="prod",
    created_at=datetime(2026, 8, 5, 2, 0, tzinfo=UTC),
  )
  desired_objects = []
  for item in target_snapshot.metadata.objects:
    if (
      item.model_name == "SourceDataset"
      and item.fields["source_dataset_name"] == source_dataset.source_dataset_name
    ):
      fields = dict(item.fields)
      fields["ingestion_config"] = {
        "uri": "${ORDERS_SOURCE_URI}",
        "options": {
          "delimiter": ";",
          "headers": ["order_id", "customer_id"],
        },
      }
      item = EnvironmentMetadataObject(
        model_name=item.model_name,
        identity=item.identity,
        fields=fields,
      )
    desired_objects.append(item)
  source_snapshot = EnvironmentMetadataSnapshot(
    environment_label="dev",
    created_at=datetime(2026, 8, 5, 2, 30, tzinfo=UTC),
    metadata=EnvironmentMetadataPayload(
      objects=tuple(desired_objects),
      relationships=target_snapshot.metadata.relationships,
    ),
  )
  bundle = ArchitectureReleaseBundle(
    release_name=f"ingestion-{suffix}",
    release_version="1.0.0",
    created_at=datetime(2026, 8, 5, 2, 45, tzinfo=UTC),
    created_by="release@example.com",
    snapshot=source_snapshot,
  )
  plan = build_environment_promotion_plan(
    bundle=bundle,
    target_snapshot=target_snapshot,
  )
  approval = build_environment_promotion_approval(
    plan=plan,
    decided_by="reviewer@example.com",
  )

  apply_environment_promotion_plan(
    plan=plan,
    bundle=bundle,
    approval=approval,
    applied_by="deployer@example.com",
    runtime_environment_label="prod",
    record_store=EnvironmentPromotionRecordStore(tmp_path),
  )

  source_dataset.refresh_from_db()
  assert source_dataset.ingestion_config == {
    "uri": "${ORDERS_SOURCE_URI}",
    "options": {
      "delimiter": ";",
      "headers": ["order_id", "customer_id"],
    },
  }


@pytest.mark.django_db
def test_guarded_environment_promotion_apply_blocks_target_drift(tmp_path):
  fixture = _build_apply_fixture()
  fixture["existing_team"].description = "Changed after planning"
  fixture["existing_team"].save()
  store = EnvironmentPromotionRecordStore(tmp_path)

  with pytest.raises(
    EnvironmentPromotionDriftError,
    match="Target metadata drift detected",
  ):
    apply_environment_promotion_plan(
      plan=fixture["plan"],
      bundle=fixture["bundle"],
      approval=fixture["approval"],
      applied_by="deployer@example.com",
      runtime_environment_label="prod",
      record_store=store,
    )

  fixture["existing_team"].refresh_from_db()
  fixture["retired_system"].refresh_from_db()
  assert fixture["existing_team"].description == "Changed after planning"
  assert fixture["retired_system"].active is True
  assert not Team.objects.filter(name=fixture["new_team_name"]).exists()
  assert store.load_all() == ()


@pytest.mark.django_db
def test_environment_promotion_apply_rolls_back_all_metadata_on_action_failure(
  tmp_path,
  monkeypatch,
):
  fixture = _build_apply_fixture()
  store = EnvironmentPromotionRecordStore(tmp_path)

  def fail_relationship(self, action):
    raise EnvironmentPromotionActionApplyError("Forced relationship failure.")

  monkeypatch.setattr(
    EnvironmentPromotionExecutor,
    "_consume_add_relationship",
    fail_relationship,
  )

  with pytest.raises(
    EnvironmentPromotionActionApplyError,
    match="Forced relationship failure",
  ):
    apply_environment_promotion_plan(
      plan=fixture["plan"],
      bundle=fixture["bundle"],
      approval=fixture["approval"],
      applied_by="deployer@example.com",
      runtime_environment_label="prod",
      record_store=store,
    )

  fixture["existing_team"].refresh_from_db()
  fixture["retired_system"].refresh_from_db()
  assert fixture["existing_team"].description == "Original description"
  assert fixture["retired_system"].active is True
  assert not Team.objects.filter(name=fixture["new_team_name"]).exists()
  assert store.load_all() == ()


def _build_apply_fixture() -> dict:
  suffix = uuid4().hex[:6]
  existing_team = Team.objects.create(
    name=f"existing-{suffix}",
    description="Original description",
  )
  person = Person.objects.create(
    email=f"owner-{suffix}@example.com",
    name="Promotion Owner",
  )
  stage_schema = TargetSchema.objects.get(short_name="stage")
  promoted_stage_description = f"Promoted stage description {suffix}"
  retired_system = System.objects.create(
    short_name=f"r{suffix}",
    name="Retire after promotion",
    type="rest",
    target_short_name=f"r{suffix}",
    active=True,
  )

  target_snapshot = build_environment_metadata_snapshot(
    environment_label="prod",
    created_by="planner@example.com",
    created_at=datetime(2026, 8, 5, 2, 0, tzinfo=UTC),
  )
  new_team_name = f"new-{suffix}"
  desired_objects = []
  for item in target_snapshot.metadata.objects:
    if (
      item.model_name == "System"
      and item.fields["short_name"] == retired_system.short_name
    ):
      continue
    if (
      item.model_name == "Team"
      and item.fields["name"] == existing_team.name
    ):
      fields = dict(item.fields)
      fields["description"] = "Promoted description"
      item = EnvironmentMetadataObject(
        model_name=item.model_name,
        identity=item.identity,
        fields=fields,
      )
    if (
      item.model_name == "TargetSchema"
      and item.fields["short_name"] == "stage"
    ):
      fields = dict(item.fields)
      fields["description"] = promoted_stage_description
      item = EnvironmentMetadataObject(
        model_name=item.model_name,
        identity=item.identity,
        fields=fields,
      )
    desired_objects.append(item)

  new_team = _team_object(
    name=new_team_name,
    description="Created by promotion",
  )
  desired_objects.append(new_team)
  person_object = next(
    item
    for item in desired_objects
    if (
      item.model_name == "Person"
      and item.fields["email"] == person.email
    )
  )
  desired_relationships = tuple(target_snapshot.metadata.relationships) + (
    _relationship(
      relationship_name="PersonTeamMembership",
      source=person_object,
      target=new_team,
    ),
  )
  source_snapshot = EnvironmentMetadataSnapshot(
    environment_label="dev",
    created_at=datetime(2026, 8, 5, 2, 30, tzinfo=UTC),
    created_by="release@example.com",
    metadata=EnvironmentMetadataPayload(
      objects=tuple(desired_objects),
      relationships=desired_relationships,
    ),
  )
  bundle = ArchitectureReleaseBundle(
    release_name=f"promotion-{suffix}",
    release_version="1.0.0",
    created_at=datetime(2026, 8, 5, 2, 45, tzinfo=UTC),
    created_by="release@example.com",
    snapshot=source_snapshot,
  )
  plan = build_environment_promotion_plan(
    bundle=bundle,
    target_snapshot=target_snapshot,
    created_at=datetime(2026, 8, 5, 3, 0, tzinfo=UTC),
  )
  approval = build_environment_promotion_approval(
    plan=plan,
    decided_by="reviewer@example.com",
    decided_at=datetime(2026, 8, 5, 3, 30, tzinfo=UTC),
  )
  return {
    "existing_team": existing_team,
    "person": person,
    "retired_system": retired_system,
    "stage_schema": stage_schema,
    "promoted_stage_description": promoted_stage_description,
    "new_team_name": new_team_name,
    "bundle": bundle,
    "plan": plan,
    "approval": approval,
  }


def _team_object(*, name: str, description: str) -> EnvironmentMetadataObject:
  contract = METADATA_TRANSPORT_REGISTRY.get_model("Team")
  identity = build_metadata_object_identity(
    model_name="Team",
    contract=contract.identity,
    values={"name": name},
  )
  return EnvironmentMetadataObject(
    model_name="Team",
    identity=identity,
    fields={"name": name, "description": description},
  )


def _renamed_target_dataset_object(
  *,
  current: EnvironmentMetadataObject,
  new_name: str,
) -> EnvironmentMetadataObject:
  contract = METADATA_TRANSPORT_REGISTRY.get_model("TargetDataset")
  identity = build_metadata_object_identity(
    model_name="TargetDataset",
    contract=contract.identity,
    values={
      "target_schema_key": current.fields["target_schema"],
      "target_dataset_name": new_name,
    },
  )
  fields = dict(current.fields)
  fields["target_dataset_name"] = new_name
  fields["former_names"] = (current.fields["target_dataset_name"],)
  return EnvironmentMetadataObject(
    model_name="TargetDataset",
    identity=identity,
    fields=fields,
  )


def _reparented_target_column_object(
  *,
  current: EnvironmentMetadataObject,
  target_dataset: EnvironmentMetadataObject,
) -> EnvironmentMetadataObject:
  contract = METADATA_TRANSPORT_REGISTRY.get_model("TargetColumn")
  identity = build_metadata_object_identity(
    model_name="TargetColumn",
    contract=contract.identity,
    values={
      "target_dataset_key": target_dataset.object_key,
      "lineage_key": current.fields["lineage_key"],
    },
  )
  fields = dict(current.fields)
  fields["target_dataset"] = target_dataset.object_key
  return EnvironmentMetadataObject(
    model_name="TargetColumn",
    identity=identity,
    fields=fields,
  )


def _query_node_object(
  *,
  target_dataset: EnvironmentMetadataObject,
  logical_key: str,
) -> EnvironmentMetadataObject:
  contract = METADATA_TRANSPORT_REGISTRY.get_model("QueryNode")
  identity = build_metadata_object_identity(
    model_name="QueryNode",
    contract=contract.identity,
    values={"logical_key": logical_key},
  )
  return EnvironmentMetadataObject(
    model_name="QueryNode",
    identity=identity,
    fields={
      "target_dataset": target_dataset.object_key,
      "logical_key": logical_key,
      "node_type": "select",
      "name": "Promoted root",
      "active": True,
    },
  )


def _relationship(
  *,
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
