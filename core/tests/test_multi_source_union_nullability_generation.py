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

import pytest

from metadata.generation import naming
from metadata.generation.target_generation_service import (
  TargetGenerationService,
)
from metadata.models import (
  SourceColumn,
  SourceDataset,
  SourceDatasetGroup,
  SourceDatasetGroupMembership,
  System,
  TargetColumn,
  TargetDataset,
  TargetSchema,
)


def _target_schema(
  short_name: str,
  physical_prefix: str,
) -> TargetSchema:
  """
  Return one system-managed generation schema.
  """
  schema, _ = TargetSchema.objects.get_or_create(
    short_name=short_name,
    defaults={
      "display_name": short_name.title(),
      "schema_name": short_name,
    },
  )
  schema.display_name = short_name.title()
  schema.schema_name = short_name
  schema.physical_prefix = physical_prefix
  schema.generate_layer = True
  schema.is_system_managed = True
  schema.surrogate_keys_enabled = False
  schema.default_historize = False
  schema.save()
  return schema


def _source_system(
  short_name: str,
) -> System:
  """
  Return one direct-source system for STAGE generation.
  """
  return System.objects.create(
    short_name=short_name,
    name=f"{short_name.upper()} Source",
    type="db",
    is_source=True,
    is_target=False,
    target_short_name=short_name,
    generate_raw_tables=False,
  )


def _source_dataset(
  source_system: System,
) -> SourceDataset:
  """
  Return one active, integrated Person source dataset.
  """
  return SourceDataset.objects.create(
    source_system=source_system,
    schema_name="Person",
    source_dataset_name="Person",
    integrate=True,
    active=True,
    generate_raw_table=False,
  )


def _source_column(
  source_dataset: SourceDataset,
  *,
  name: str,
  ordinal: int,
  nullable: bool,
  primary_key: bool = False,
) -> SourceColumn:
  """
  Add one integrated source column.
  """
  return SourceColumn.objects.create(
    source_dataset=source_dataset,
    source_column_name=name,
    ordinal_position=ordinal,
    datatype=(
      "INTEGER"
      if name == "person_id"
      else "STRING"
    ),
    max_length=(
      None
      if name == "person_id"
      else 50
    ),
    nullable=nullable,
    primary_key_column=primary_key,
    integrate=True,
  )


def _generate(
  service: TargetGenerationService,
  target_schema: TargetSchema,
) -> None:
  """
  Run complete generation for one target schema.
  """
  service.apply_all(
    service.get_eligible_source_datasets_for_schema(
      target_schema
    ),
    target_schema,
  )


def _target_column(
  target_dataset: TargetDataset,
  column_name: str,
) -> TargetColumn:
  """
  Return one generated target column by name.
  """
  return TargetColumn.objects.get(
    target_dataset=target_dataset,
    target_column_name=column_name,
  )


@pytest.mark.django_db
def test_multi_source_union_nullability_propagates_to_rawcore() -> None:
  """
  Verify the effective UNION contract is persisted through RAWCORE.
  """
  stage_schema = _target_schema(
    "stage",
    "stg",
  )
  rawcore_schema = _target_schema(
    "rawcore",
    "rc",
  )
  source_one = _source_dataset(
    _source_system("aw1")
  )
  source_two = _source_dataset(
    _source_system("aw2")
  )

  _source_column(
    source_one,
    name="person_id",
    ordinal=1,
    nullable=False,
    primary_key=True,
  )
  _source_column(
    source_one,
    name="person_type_code",
    ordinal=2,
    nullable=False,
  )
  _source_column(
    source_one,
    name="title_name",
    ordinal=3,
    nullable=False,
  )
  _source_column(
    source_one,
    name="first_name",
    ordinal=4,
    nullable=False,
  )

  _source_column(
    source_two,
    name="person_id",
    ordinal=1,
    nullable=False,
    primary_key=True,
  )
  _source_column(
    source_two,
    name="title_name",
    ordinal=2,
    nullable=True,
  )
  _source_column(
    source_two,
    name="first_name",
    ordinal=3,
    nullable=False,
  )

  group = SourceDatasetGroup.objects.create(
    target_short_name="aw",
    unified_source_dataset_name="Person",
  )
  first_membership = SourceDatasetGroupMembership.objects.create(
    group=group,
    source_dataset=source_one,
    is_primary_system=True,
  )
  first_membership.source_identity_id = "aw1"
  first_membership.save()
  second_membership = SourceDatasetGroupMembership.objects.create(
    group=group,
    source_dataset=source_two,
    is_primary_system=False,
  )
  second_membership.source_identity_id = "aw2"
  second_membership.save()

  service = TargetGenerationService(
    pepper="test-pepper",
  )
  _generate(
    service,
    stage_schema,
  )
  _generate(
    service,
    rawcore_schema,
  )

  stage_dataset = TargetDataset.objects.get(
    target_schema=stage_schema,
    target_dataset_name=naming.build_physical_dataset_name(
      target_schema=stage_schema,
      source_dataset=source_one,
    ),
  )
  rawcore_dataset = TargetDataset.objects.get(
    target_schema=rawcore_schema,
    target_dataset_name=naming.build_physical_dataset_name(
      target_schema=rawcore_schema,
      source_dataset=source_one,
    ),
  )

  stage_missing = _target_column(
    stage_dataset,
    "person_type_code",
  )
  rawcore_missing = _target_column(
    rawcore_dataset,
    "person_type_code",
  )

  assert stage_missing.nullable is True
  assert rawcore_missing.nullable is True
  assert _target_column(
    stage_dataset,
    "title_name",
  ).nullable is True
  assert _target_column(
    rawcore_dataset,
    "title_name",
  ).nullable is True
  assert _target_column(
    stage_dataset,
    "first_name",
  ).nullable is False
  assert _target_column(
    rawcore_dataset,
    "first_name",
  ).nullable is False
  assert _target_column(
    stage_dataset,
    "source_identity_id",
  ).nullable is False

  # Simulate the incorrect legacy metadata state and verify regeneration
  # restores the effective UNION contract deterministically.
  stage_missing.nullable = False
  stage_missing.save(update_fields=["nullable"])
  rawcore_missing.nullable = False
  rawcore_missing.save(update_fields=["nullable"])

  _generate(
    service,
    stage_schema,
  )
  _generate(
    service,
    rawcore_schema,
  )

  stage_missing.refresh_from_db()
  rawcore_missing.refresh_from_db()

  assert stage_missing.nullable is True
  assert rawcore_missing.nullable is True
