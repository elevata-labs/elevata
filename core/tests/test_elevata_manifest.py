"""
elevata - Metadata-driven Data Platform Framework
Copyright © 202-2026 Ilona Tag

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

from metadata.execution.manifest import build_manifest
from metadata.models import TargetDataset, TargetDatasetInput, TargetSchema, System, SourceDataset


@pytest.mark.django_db
def test_manifest_includes_source_edges_and_upstream_target_edges():
  # Schemas: raw + stage
  raw_schema, _ = TargetSchema.objects.get_or_create(short_name="raw", schema_name="raw")
  stage_schema, _ = TargetSchema.objects.get_or_create(short_name="stage", schema_name="stage")

  # Source system + dataset
  src_sys = System.objects.create(short_name="crm", name="CRM")
  src_ds = SourceDataset.objects.create(
    source_system=src_sys,
    schema_name="public",
    source_dataset_name="customer",
  )

  # raw target depends on source
  raw_td = TargetDataset.objects.create(
    target_schema=raw_schema,
    target_dataset_name="raw_customer",
    incremental_strategy="full",
    is_system_managed=False,
  )
  TargetDatasetInput.objects.create(
    target_dataset=raw_td,
    source_dataset=src_ds,
    upstream_target_dataset=None,
    role="primary",
    active=True,
  )

  # stage target depends on raw target (upstream_target_dataset)
  stage_td = TargetDataset.objects.create(
    target_schema=stage_schema,
    target_dataset_name="stg_customer",
    incremental_strategy="full",
    is_system_managed=False,
  )
  TargetDatasetInput.objects.create(
    target_dataset=stage_td,
    source_dataset=None,
    upstream_target_dataset=raw_td,
    role="primary",
    active=True,
  )

  manifest = build_manifest(
    profile_name="dev",
    target_system_short="dbdwh",
    include_system_managed=True,
    include_sources=True,
  )
  node_index = {n.id: n for n in manifest.nodes}

  # Source node exists
  # ID format: source.<system_short>.<schema_or_default>.<source_dataset_name>
  source_id = "source.crm.public.customer"
  assert source_id in node_index
  assert node_index[source_id].type == "source"

  raw_id = "raw.raw_customer"
  stage_id = "stage.stg_customer"

  assert raw_id in node_index
  assert stage_id in node_index

  # raw depends on source
  assert source_id in node_index[raw_id].deps

  # stage depends on raw
  assert raw_id in node_index[stage_id].deps

@pytest.mark.django_db
def test_manifest_stage_can_depend_directly_on_source():
  stage_schema, _ = TargetSchema.objects.get_or_create(short_name="stage", schema_name="stage")

  src_sys = System.objects.create(short_name="crm", name="CRM")
  src_ds = SourceDataset.objects.create(
    source_system=src_sys,
    schema_name="public",
    source_dataset_name="person",
  )

  stage_td = TargetDataset.objects.create(
    target_schema=stage_schema,
    target_dataset_name="stg_person",
    incremental_strategy="full",
    is_system_managed=False,
  )
  TargetDatasetInput.objects.create(
    target_dataset=stage_td,
    source_dataset=src_ds,
    upstream_target_dataset=None,
    role="primary",
    active=True,
  )

  manifest = build_manifest(
    profile_name="dev",
    target_system_short="dbdwh",
    include_system_managed=True,
    include_sources=True,
  )
  node_index = {n.id: n for n in manifest.nodes}

  source_id = "source.crm.public.person"
  stage_id = "stage.stg_person"

  assert source_id in node_index
  assert stage_id in node_index
  assert source_id in node_index[stage_id].deps
