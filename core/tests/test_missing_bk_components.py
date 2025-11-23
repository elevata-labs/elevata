"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025 Ilona Tag

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

import pytest

from metadata.models import (
  TargetSchema,
  TargetDataset,
  TargetColumn,
  TargetDatasetReference,
  TargetDatasetReferenceComponent,
)

@pytest.mark.django_db
def test_missing_bk_components_detected():
  # Schemas
  stage_schema, _ = TargetSchema.objects.get_or_create(
    short_name="stage", defaults={"display_name": "Stage", "database_name": "dw", "schema_name": "stage"}
  )
  raw_schema, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore", defaults={"display_name": "Raw Core", "database_name": "dw", "schema_name": "rawcore"}
  )

  # Parent Stage → RawCore
  parent_stage = TargetDataset.objects.create(target_schema=stage_schema, target_dataset_name="stg_p")
  parent_raw = TargetDataset.objects.create(target_schema=raw_schema, target_dataset_name="raw_p")

  # Parent BK columns
  bk1 = TargetColumn.objects.create(target_dataset=parent_raw, target_column_name="bk1", business_key_column=True)
  bk2 = TargetColumn.objects.create(target_dataset=parent_raw, target_column_name="bk2", business_key_column=True)

  # Child dataset
  child = TargetDataset.objects.create(target_schema=raw_schema, target_dataset_name="raw_c")

  # Reference
  ref = TargetDatasetReference.objects.create(
    referencing_dataset=child,
    referenced_dataset=parent_raw
  )

  # Provide ONLY ONE BK mapping
  TargetDatasetReferenceComponent.objects.create(
    reference=ref,
    from_column=bk1,    # reuse for minimal setup
    to_column=bk1,
    ordinal_position=1,
  )

  # Test: missing component detected
  missing = ref.validate_key_components()
  assert missing == ["bk2"]


@pytest.mark.django_db
def test_no_missing_bk_components():
  stage_schema, _ = TargetSchema.objects.get_or_create(
    short_name="stage", defaults={"display_name": "Stage", "database_name": "dw", "schema_name": "stage"}
  )
  raw_schema, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore", defaults={"display_name": "Raw Core", "database_name": "dw", "schema_name": "rawcore"}
  )

  parent_raw = TargetDataset.objects.create(target_schema=raw_schema, target_dataset_name="raw_p")
  bk1 = TargetColumn.objects.create(target_dataset=parent_raw, target_column_name="bk1", business_key_column=True)
  bk2 = TargetColumn.objects.create(target_dataset=parent_raw, target_column_name="bk2", business_key_column=True)

  child = TargetDataset.objects.create(target_schema=raw_schema, target_dataset_name="raw_c")

  ref = TargetDatasetReference.objects.create(
    referencing_dataset=child,
    referenced_dataset=parent_raw
  )

  TargetDatasetReferenceComponent.objects.create(
    reference=ref, from_column=bk1, to_column=bk1, ordinal_position=1
  )
  TargetDatasetReferenceComponent.objects.create(
    reference=ref, from_column=bk2, to_column=bk2, ordinal_position=2
  )

  assert ref.validate_key_components() == []
