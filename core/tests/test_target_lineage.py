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

# core/tests/test_target_lineage.py
import pytest

from metadata.models import TargetDatasetInput
from metadata.rendering.builder import build_logical_select_for_target


@pytest.mark.django_db
def test_raw_stage_rawcore_lineage(raw_stage_rawcore_datasets):
  raw_ds, stage_ds, rawcore_ds = raw_stage_rawcore_datasets

  # Stage should have raw as its only upstream target
  stage_inputs = TargetDatasetInput.objects.filter(target_dataset=stage_ds)
  assert stage_inputs.count() == 1
  assert stage_inputs.first().upstream_target_dataset == raw_ds

  # Rawcore should have stage as its only upstream target
  rawcore_inputs = TargetDatasetInput.objects.filter(target_dataset=rawcore_ds)
  assert rawcore_inputs.count() == 1
  assert rawcore_inputs.first().upstream_target_dataset == stage_ds


@pytest.mark.django_db
def test_raw_stage_rawcore_lineage(raw_stage_rawcore_datasets):
  raw_ds, stage_ds, rawcore_ds = raw_stage_rawcore_datasets

  # Stage should have raw as its only upstream target
  stage_inputs = TargetDatasetInput.objects.filter(target_dataset=stage_ds)
  assert stage_inputs.count() == 1
  assert stage_inputs.first().upstream_target_dataset == raw_ds

  # Rawcore should have stage as its only upstream target
  rawcore_inputs = TargetDatasetInput.objects.filter(target_dataset=rawcore_ds)
  assert rawcore_inputs.count() == 1
  assert rawcore_inputs.first().upstream_target_dataset == stage_ds


@pytest.mark.django_db
def test_raw_select_reads_from_source_dataset(raw_stage_rawcore_datasets, source_dataset_sap_customer):
  """
  For a RAW TargetDataset that has a SourceDataset input, the logical
  SELECT should read FROM the physical source dataset (schema + name),
  not from the raw target itself.
  """
  raw_ds, stage_ds, rawcore_ds = raw_stage_rawcore_datasets

  plan = build_logical_select_for_target(raw_ds)

  # We expect a single LogicalSelect with a SourceTable as FROM
  from_table = plan.from_

  assert from_table.schema == source_dataset_sap_customer.schema_name
  assert from_table.name == source_dataset_sap_customer.source_dataset_name
