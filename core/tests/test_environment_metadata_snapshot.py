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
from io import StringIO
import json
from uuid import uuid4

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from metadata.models import (
  PartialLoad,
  Person,
  QueryNode,
  SourceDataset,
  SourceDatasetGroup,
  SourceDatasetIncrementPolicy,
  System,
  TargetColumn,
  TargetDataset,
  TargetSchema,
  Team,
)
from metadata.promotion.snapshot import (
  EnvironmentMetadataPayload,
  EnvironmentMetadataSnapshotError,
  deserialize_environment_metadata_snapshot,
  serialize_environment_metadata_snapshot,
)
from metadata.promotion.snapshot_builder import (
  build_environment_metadata_snapshot,
)


@pytest.mark.django_db
def test_environment_metadata_snapshot_is_portable_complete_and_stable():
  suffix = uuid4().hex[:6]
  team = Team.objects.create(
    name=f"Snapshot {suffix}",
    description="Snapshot test team",
  )
  person = Person.objects.create(
    email=f"snapshot-{suffix}@example.com",
    name="Snapshot Owner",
  )
  person.team.add(team)
  group = SourceDatasetGroup.objects.create(
    target_short_name=f"g{suffix}",
    unified_source_dataset_name=f"customer_{suffix}",
    description="Snapshot source group",
  )
  group.owner.add(person)

  system = System.objects.create(
    short_name=f"s{suffix}",
    name="Snapshot CRM",
    type="rest",
    target_short_name=f"s{suffix}",
  )
  source_dataset = SourceDataset.objects.create(
    source_system=system,
    schema_name=None,
    source_dataset_name=f"customer_{suffix}",
    ingestion_config={
      "endpoint": "${SNAPSHOT_API_ENDPOINT}",
      "pagination": {"mode": "page", "size": 100},
    },
    incremental=True,
    increment_filter="updated_at >= {{DELTA_CUTOFF}}",
  )
  active_policy = SourceDatasetIncrementPolicy.objects.create(
    source_dataset=source_dataset,
    environment="prod",
    increment_interval_length=7,
    increment_interval_unit="day",
    active=True,
  )
  inactive_policy = SourceDatasetIncrementPolicy.objects.create(
    source_dataset=source_dataset,
    environment="prod",
    increment_interval_length=30,
    increment_interval_unit="day",
    active=False,
  )

  schema = TargetSchema.objects.create(
    short_name=f"u{suffix}",
    display_name="Use-case Serving",
    schema_name=f"u{suffix}",
    generate_layer=False,
    is_system_managed=False,
  )
  partial_load = PartialLoad.objects.create(
    name=f"p{suffix}",
    description="Snapshot partial load",
  )
  target_dataset = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=f"use_case_{suffix}",
    lineage_key=f"manual:serving:{suffix}",
    former_names=[f"old_b_{suffix}", f"old_a_{suffix}"],
  )
  target_dataset.partial_load.add(partial_load)
  query_node = QueryNode.objects.create(
    target_dataset=target_dataset,
    node_type="select",
    name="Snapshot root",
  )
  target_dataset.query_root = query_node
  target_dataset.query_head = query_node
  target_dataset.save(update_fields=["query_root", "query_head"])

  first = build_environment_metadata_snapshot(
    environment_label="prod",
    created_by="first@example.com",
    created_at=datetime(2026, 8, 2, 10, 0, tzinfo=timezone.utc),
  )
  second = build_environment_metadata_snapshot(
    environment_label="prod",
    created_by="second@example.com",
    created_at=datetime(2026, 8, 2, 11, 0, tzinfo=timezone.utc),
  )

  assert first.metadata_fingerprint == second.metadata_fingerprint
  assert first.snapshot_fingerprint == second.snapshot_fingerprint
  assert first.created_at != second.created_at
  assert first.created_by != second.created_by

  reordered_payload = EnvironmentMetadataPayload(
    objects=tuple(reversed(first.metadata.objects)),
    relationships=tuple(reversed(first.metadata.relationships)),
  )
  assert reordered_payload.metadata_fingerprint == first.metadata_fingerprint

  objects = {
    item.object_key: item
    for item in first.metadata.objects
  }
  by_model = {}
  for item in first.metadata.objects:
    by_model.setdefault(item.model_name, []).append(item)

  system_object = next(
    item
    for item in by_model["System"]
    if item.fields["short_name"] == system.short_name
  )
  source_object = next(
    item
    for item in by_model["SourceDataset"]
    if item.fields["source_dataset_name"] == source_dataset.source_dataset_name
  )
  target_object = next(
    item
    for item in by_model["TargetDataset"]
    if item.fields["lineage_key"] == target_dataset.lineage_key
  )

  assert source_object.fields["source_system"] == system_object.object_key
  assert source_object.fields["schema_name"] == ""
  assert target_object.fields["former_names"] == (
    f"old_a_{suffix}",
    f"old_b_{suffix}",
  )
  query_object = next(
    item
    for item in by_model["QueryNode"]
    if item.fields["name"] == "Snapshot root"
  )
  assert target_object.fields["query_root"] == query_object.object_key
  assert target_object.fields["query_head"] == query_object.object_key

  policy_objects = [
    item
    for item in by_model["SourceDatasetIncrementPolicy"]
    if item.fields["source_dataset"] == source_object.object_key
  ]
  assert len(policy_objects) == 1
  assert policy_objects[0].fields["increment_interval_length"] == 7
  assert active_policy.pk is not None
  assert inactive_policy.pk is not None

  relationship_names = {
    item.relationship_name
    for item in first.metadata.relationships
  }
  assert {
    "PersonTeamMembership",
    "SourceDatasetGroupOwner",
    "TargetDatasetPartialLoadAssignment",
  } <= relationship_names
  for relationship in first.metadata.relationships:
    assert relationship.source_key in objects
    assert relationship.target_key in objects

  forbidden_fields = {
    "id",
    "created_at",
    "updated_at",
    "created_by",
    "updated_by",
    "retired_at",
    "database_name",
  }
  for item in first.metadata.objects:
    assert not forbidden_fields & set(item.fields)

  rendered = serialize_environment_metadata_snapshot(first)
  restored = deserialize_environment_metadata_snapshot(rendered)
  assert serialize_environment_metadata_snapshot(restored) == rendered
  assert restored.metadata_fingerprint == first.metadata_fingerprint
  assert restored.snapshot_fingerprint == first.snapshot_fingerprint


@pytest.mark.django_db
def test_environment_metadata_snapshot_fingerprint_detects_tampering():
  snapshot = build_environment_metadata_snapshot(
    environment_label="test",
    created_at=datetime(2026, 8, 2, 12, 0, tzinfo=timezone.utc),
  )
  data = json.loads(serialize_environment_metadata_snapshot(snapshot))
  data["environment_label"] = "prod"

  with pytest.raises(
    EnvironmentMetadataSnapshotError,
    match="snapshot fingerprint mismatch",
  ):
    deserialize_environment_metadata_snapshot(json.dumps(data))

  metadata_data = json.loads(
    serialize_environment_metadata_snapshot(snapshot)
  )
  target_schema = next(
    item
    for item in metadata_data["metadata"]["objects"]
    if item["model"] == "TargetSchema"
  )
  target_schema["fields"]["description"] = "tampered"

  with pytest.raises(
    EnvironmentMetadataSnapshotError,
    match="metadata fingerprint mismatch",
  ):
    deserialize_environment_metadata_snapshot(json.dumps(metadata_data))


@pytest.mark.django_db
def test_elevata_metadata_snapshot_command_exports_immutable_file(tmp_path):
  output_path = tmp_path / "prod-metadata-snapshot.json"
  stdout = StringIO()

  call_command(
    "elevata_metadata_snapshot",
    environment_label="prod",
    created_by="deployment@example.com",
    output_path=str(output_path),
    stdout=stdout,
  )

  snapshot = deserialize_environment_metadata_snapshot(
    output_path.read_text(encoding="utf-8")
  )
  assert snapshot.environment_label == "prod"
  assert snapshot.created_by == "deployment@example.com"
  assert snapshot.snapshot_fingerprint in stdout.getvalue()

  with pytest.raises(CommandError, match="will not be overwritten"):
    call_command(
      "elevata_metadata_snapshot",
      environment_label="prod",
      output_path=str(output_path),
    )


@pytest.mark.django_db
def test_snapshot_distinguishes_base_history_and_dataset_scoped_column_lineage():
  suffix = uuid4().hex[:6]
  schema = TargetSchema.objects.get(short_name="rawcore")
  lineage_key = f"generated:rawcore:snapshot:{suffix}"

  base = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=f"rc_snapshot_{suffix}",
    lineage_key=lineage_key,
    incremental_strategy="full",
    historize=True,
    is_system_managed=True,
  )
  history = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=f"rc_snapshot_{suffix}_hist",
    lineage_key=lineage_key,
    incremental_strategy="historize",
    historize=False,
    is_system_managed=True,
  )
  column_lineage_key = f"source:snapshot:{suffix}:customer_id"
  base_column = TargetColumn.objects.create(
    target_dataset=base,
    target_column_name="customer_id",
    ordinal_position=1,
    datatype="STRING",
    lineage_key=column_lineage_key,
    is_system_managed=True,
  )
  history_column = TargetColumn.objects.create(
    target_dataset=history,
    target_column_name="customer_id",
    ordinal_position=1,
    datatype="STRING",
    lineage_key=column_lineage_key,
    is_system_managed=True,
  )

  snapshot = build_environment_metadata_snapshot(environment_label="dev")
  dataset_objects = [
    item
    for item in snapshot.metadata.objects
    if item.model_name == "TargetDataset"
    and item.fields["lineage_key"] == lineage_key
  ]
  column_objects = [
    item
    for item in snapshot.metadata.objects
    if item.model_name == "TargetColumn"
    and item.fields["lineage_key"] == column_lineage_key
  ]

  assert len(dataset_objects) == 2
  assert len({item.object_key for item in dataset_objects}) == 2
  assert len(column_objects) == 2
  assert len({item.object_key for item in column_objects}) == 2
  assert {
    item.fields["target_dataset"]
    for item in column_objects
  } == {
    item.object_key
    for item in dataset_objects
  }
  assert base_column.pk is not None
  assert history_column.pk is not None
