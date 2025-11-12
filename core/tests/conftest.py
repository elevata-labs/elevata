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
  SourceSystem,
  SourceDataset,
  TargetSchema,
  TargetDataset,
  TargetDatasetInput,
)
from metadata.generation.target_generation_service import TargetGenerationService


@pytest.fixture
def target_generation_service():
  """Provide a TargetGenerationService instance."""
  return TargetGenerationService()


# -------------------------------------------------------------------
# Basic source setup
# -------------------------------------------------------------------
@pytest.fixture
def source_system_sap(db):
  """
  Minimal SourceSystem for tests.
  Only required (non-null) fields are populated.
  """
  return SourceSystem.objects.create(
    short_name="sap",
    name="SAP",
    type="db",           # 'type' has choices, but Django does not enforce them at DB level
    target_short_name="sap",
  )


@pytest.fixture
def source_dataset_sap_customer(db, source_system_sap):
  """
  Minimal SourceDataset connected to the SAP system.
  """
  return SourceDataset.objects.create(
    source_system=source_system_sap,
    schema_name="sap_raw",
    source_dataset_name="customer",
  )


# -------------------------------------------------------------------
# Target schemas (raw / stage / rawcore)
# -------------------------------------------------------------------
@pytest.fixture
def target_schemas(db):
  """
  Use the existing TargetSchema entries for raw, stage, and rawcore.

  This assumes you already have initial data that creates
  short_name='raw', 'stage', 'rawcore'.
  """
  raw = TargetSchema.objects.get(short_name="raw")
  stage = TargetSchema.objects.get(short_name="stage")
  rawcore = TargetSchema.objects.get(short_name="rawcore")

  return {
    "raw": raw,
    "stage": stage,
    "rawcore": rawcore,
  }


# -------------------------------------------------------------------
# Target datasets + basic lineage: Raw -> Stage -> Rawcore
# -------------------------------------------------------------------
@pytest.fixture
def raw_stage_rawcore_datasets(db, target_schemas, source_dataset_sap_customer):
  """
  Create a minimal Raw -> Stage -> Rawcore target dataset chain:

    raw_customer (raw)   <- from SourceDataset
    stage_customer       <- from raw_customer
    rawcore_customer     <- from stage_customer

  and wire them via TargetDatasetInput (dataset-level lineage).
  """
  # Raw layer dataset fed from the source dataset
  raw_ds = TargetDataset.objects.create(
    target_schema=target_schemas["raw"],
    target_dataset_name="sap_customer_raw",
  )

  TargetDatasetInput.objects.create(
    target_dataset=raw_ds,
    source_dataset=source_dataset_sap_customer,
    upstream_target_dataset=None,
    role="primary",  # Expected to be valid in TARGET_DATASET_INPUT_ROLE_CHOICES
  )

  # Stage layer dataset, fed from raw
  stage_ds = TargetDataset.objects.create(
    target_schema=target_schemas["stage"],
    target_dataset_name="sap_customer_stage",
  )

  TargetDatasetInput.objects.create(
    target_dataset=stage_ds,
    source_dataset=None,
    upstream_target_dataset=raw_ds,
    role="primary",
  )

  # Rawcore layer dataset, fed from stage
  rawcore_ds = TargetDataset.objects.create(
    target_schema=target_schemas["rawcore"],
    target_dataset_name="sap_customer_rawcore",
  )

  TargetDatasetInput.objects.create(
    target_dataset=rawcore_ds,
    source_dataset=None,
    upstream_target_dataset=stage_ds,
    role="primary",
  )

  return raw_ds, stage_ds, rawcore_ds
