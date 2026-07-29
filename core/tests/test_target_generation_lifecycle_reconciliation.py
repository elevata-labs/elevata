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
  TargetDataset,
  TargetSchema,
)


def _target_schema(
  short_name: str,
  *,
  default_historize: bool = False,
  surrogate_keys_enabled: bool = False,
) -> TargetSchema:
  """
  Return a deterministic system-managed generation schema.
  """
  prefix_by_schema = {
    "raw": "raw",
    "stage": "stg",
    "rawcore": "rc",
  }
  schema, _ = TargetSchema.objects.get_or_create(
    short_name=short_name,
    defaults={
      "display_name": short_name.title(),
      "database_name": "dw",
      "schema_name": short_name,
    },
  )
  schema.display_name = short_name.title()
  schema.database_name = "dw"
  schema.schema_name = short_name
  schema.physical_prefix = prefix_by_schema[short_name]
  schema.generate_layer = True
  schema.is_system_managed = True
  schema.default_historize = default_historize
  schema.surrogate_keys_enabled = surrogate_keys_enabled
  schema.save()
  return schema


def _source_system(
  short_name: str = "lifecycle",
) -> System:
  """
  Return a source system that requires RAW landing.
  """
  return System.objects.create(
    short_name=short_name,
    name=f"{short_name.title()} Source",
    type="db",
    is_source=True,
    is_target=False,
    target_short_name=short_name,
    generate_raw_tables=True,
  )


def _source_dataset(
  source_system: System,
  *,
  dataset_name: str = "Customer",
) -> SourceDataset:
  """
  Create one active and integrated source dataset with a key column.
  """
  source_dataset = SourceDataset.objects.create(
    source_system=source_system,
    schema_name="dbo",
    source_dataset_name=dataset_name,
    integrate=True,
    active=True,
    generate_raw_table=True,
  )
  SourceColumn.objects.create(
    source_dataset=source_dataset,
    source_column_name="customer_id",
    ordinal_position=1,
    datatype="INTEGER",
    nullable=False,
    primary_key_column=True,
    integrate=True,
  )
  return source_dataset


def _run_complete_generation(
  service: TargetGenerationService,
  schemas,
) -> None:
  """
  Run complete schema-level generation with lifecycle reconciliation.
  """
  for schema in schemas:
    eligible = service.get_eligible_source_datasets_for_schema(
      schema
    )
    service.apply_all(
      eligible,
      schema,
      reconcile_lifecycle=True,
    )


@pytest.mark.django_db
@pytest.mark.parametrize(
  "lifecycle_field",
  (
    "active",
    "integrate",
  ),
)
def test_source_lifecycle_retires_and_reactivates_generated_chain(
  lifecycle_field,
) -> None:
  """
  Verify source exclusion retires generated datasets without replacing them.
  """
  raw_schema = _target_schema("raw")
  stage_schema = _target_schema("stage")
  rawcore_schema = _target_schema(
    "rawcore",
    default_historize=True,
    surrogate_keys_enabled=True,
  )
  schemas = (
    raw_schema,
    stage_schema,
    rawcore_schema,
  )
  source_system = _source_system()
  source_dataset = _source_dataset(
    source_system
  )
  service = TargetGenerationService(
    pepper="test-pepper",
  )

  _run_complete_generation(
    service,
    schemas,
  )

  generated = TargetDataset.objects.filter(
    target_schema__in=schemas,
    is_system_managed=True,
  )
  original_ids = set(
    generated.values_list(
      "pk",
      flat=True,
    )
  )

  assert len(original_ids) == 4
  assert generated.filter(active=True).count() == 4
  assert generated.filter(
    target_dataset_name__endswith="_hist",
  ).count() == 1

  setattr(
    source_dataset,
    lifecycle_field,
    False,
  )
  source_dataset.save()

  _run_complete_generation(
    service,
    schemas,
  )

  retired = TargetDataset.objects.filter(
    pk__in=original_ids,
  )

  assert retired.count() == 4
  assert retired.filter(active=False).count() == 4
  assert retired.filter(
    retired_at__isnull=False,
  ).count() == 4

  setattr(
    source_dataset,
    lifecycle_field,
    True,
  )
  source_dataset.save()

  _run_complete_generation(
    service,
    schemas,
  )

  reactivated = TargetDataset.objects.filter(
    pk__in=original_ids,
  )
  current_ids = set(
    TargetDataset.objects
    .filter(
      target_schema__in=schemas,
      is_system_managed=True,
    )
    .values_list(
      "pk",
      flat=True,
    )
  )

  assert current_ids == original_ids
  assert reactivated.filter(active=True).count() == 4
  assert reactivated.filter(
    retired_at__isnull=True,
  ).count() == 4


@pytest.mark.django_db
def test_reconciliation_does_not_touch_foreign_or_manual_targets() -> None:
  """
  Verify reconciliation is limited to generator-owned system metadata.
  """
  stage_schema = _target_schema("stage")
  service = TargetGenerationService(
    pepper="test-pepper",
  )

  generated = TargetDataset.objects.create(
    target_schema=stage_schema,
    target_dataset_name="stg_generated_orphan",
    lineage_key=f"{stage_schema.pk}:999999",
    is_system_managed=True,
    active=True,
  )
  foreign_managed = TargetDataset.objects.create(
    target_schema=stage_schema,
    target_dataset_name="stg_foreign_managed",
    lineage_key="external-workflow:999999",
    is_system_managed=True,
    active=True,
  )
  manual = TargetDataset.objects.create(
    target_schema=stage_schema,
    target_dataset_name="stg_manual",
    lineage_key=f"{stage_schema.pk}:888888",
    is_system_managed=False,
    active=True,
  )

  service.apply_all(
    [],
    stage_schema,
    reconcile_lifecycle=True,
  )

  generated.refresh_from_db()
  foreign_managed.refresh_from_db()
  manual.refresh_from_db()

  assert generated.active is False
  assert generated.retired_at is not None
  assert foreign_managed.active is True
  assert foreign_managed.retired_at is None
  assert manual.active is True
  assert manual.retired_at is None


@pytest.mark.django_db
def test_partial_multi_source_withdrawal_keeps_shared_target_active() -> None:
  """
  Verify a shared target survives while one eligible source remains.
  """
  stage_schema = _target_schema("stage")
  source_system = _source_system(
    short_name="multi",
  )
  source_one = _source_dataset(
    source_system,
    dataset_name="PersonOne",
  )
  source_two = _source_dataset(
    source_system,
    dataset_name="PersonTwo",
  )
  group = SourceDatasetGroup.objects.create(
    target_short_name="person",
    unified_source_dataset_name="Person",
  )
  SourceDatasetGroupMembership.objects.create(
    group=group,
    source_dataset=source_one,
    is_primary_system=True,
  )
  SourceDatasetGroupMembership.objects.create(
    group=group,
    source_dataset=source_two,
    is_primary_system=False,
  )
  service = TargetGenerationService(
    pepper="test-pepper",
  )

  eligible = service.get_eligible_source_datasets_for_schema(
    stage_schema
  )
  service.apply_all(
    eligible,
    stage_schema,
    reconcile_lifecycle=True,
  )

  expected_name_one = naming.build_physical_dataset_name(
    target_schema=stage_schema,
    source_dataset=source_one,
  )
  expected_name_two = naming.build_physical_dataset_name(
    target_schema=stage_schema,
    source_dataset=source_two,
  )

  assert expected_name_one == expected_name_two, (
    "Both grouped SourceDatasets must resolve to the same "
    "physical STAGE dataset name."
  )

  shared_target = TargetDataset.objects.get(
    target_schema=stage_schema,
    target_dataset_name=expected_name_one,
  )
  original_id = shared_target.pk

  source_two.integrate = False
  source_two.save()

  eligible = service.get_eligible_source_datasets_for_schema(
    stage_schema
  )
  service.apply_all(
    eligible,
    stage_schema,
    reconcile_lifecycle=True,
  )

  shared_target.refresh_from_db()
  source_input_ids = set(
    shared_target.input_links
    .exclude(source_dataset__isnull=True)
    .values_list(
      "source_dataset_id",
      flat=True,
    )
  )

  assert shared_target.pk == original_id
  assert shared_target.active is True
  assert shared_target.retired_at is None
  assert source_input_ids == {
    source_one.pk,
  }
