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

import pytest

from metadata.execution.manifest import (
  EXECUTION_DEPENDENCY_SOURCE_INPUT,
  MANIFEST_VERSION,
  build_manifest,
  manifest_to_dict,
)
from metadata.execution.load_graph import EXECUTION_DEPENDENCY_LINEAGE_INPUT
from metadata.models import TargetDataset, TargetDatasetInput, TargetSchema, System, SourceDataset


def _execution_dep_tuples(node):
  """
  Return execution dependencies as comparable tuples.
  """
  return {(dep.id, dep.reason, dep.reference_id) for dep in node.execution_deps}


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

  # raw depends on source for manifest completeness.
  assert source_id in node_index[raw_id].deps
  assert source_id in node_index[raw_id].lineage_deps
  assert (source_id, EXECUTION_DEPENDENCY_SOURCE_INPUT, None) in _execution_dep_tuples(
    node_index[raw_id]
  )

  # stage depends on raw through execution dependencies, not only lineage wording.
  assert raw_id in node_index[stage_id].deps
  assert raw_id in node_index[stage_id].lineage_deps
  assert (raw_id, EXECUTION_DEPENDENCY_LINEAGE_INPUT, None) in _execution_dep_tuples(
    node_index[stage_id]
  )

  payload = manifest_to_dict(manifest)
  assert payload["manifest_version"] == MANIFEST_VERSION
  payload_nodes = {n["id"]: n for n in payload["nodes"]}
  assert payload_nodes[stage_id]["deps"] == [raw_id]
  assert payload_nodes[stage_id]["lineage_deps"] == [raw_id]
  assert payload_nodes[stage_id]["execution_deps"] == [{
    "id": raw_id,
    "reason": EXECUTION_DEPENDENCY_LINEAGE_INPUT,
    "reference_id": None,
  }]


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
  assert source_id in node_index[stage_id].lineage_deps
  assert (source_id, EXECUTION_DEPENDENCY_SOURCE_INPUT, None) in _execution_dep_tuples(
    node_index[stage_id]
  )


@pytest.mark.django_db
def test_manifest_uses_effective_materialization_type():
  """Manifest materialization exposes the effective value, not the raw override field."""
  raw_schema, _ = TargetSchema.objects.get_or_create(
    short_name="raw",
    defaults={
      "display_name": "Raw",
      "schema_name": "raw",
      "default_materialization_type": "table",
    },
  )

  update_fields = []
  if raw_schema.default_materialization_type != "table":
    raw_schema.default_materialization_type = "table"
    update_fields.append("default_materialization_type")
  if not raw_schema.schema_name:
    raw_schema.schema_name = "raw"
    update_fields.append("schema_name")
  if update_fields:
    raw_schema.save(update_fields=update_fields)

  TargetDataset.objects.create(
    target_schema=raw_schema,
    target_dataset_name="raw_effective_materialization_default",
    incremental_strategy="full",
    materialization_type=None,
    is_system_managed=False,
  )
  TargetDataset.objects.create(
    target_schema=raw_schema,
    target_dataset_name="raw_effective_materialization_override",
    incremental_strategy="full",
    materialization_type="view",
    is_system_managed=False,
  )

  manifest = build_manifest(
    profile_name="dev",
    target_system_short="dbdwh",
    include_system_managed=True,
    include_sources=False,
  )
  node_index = {n.id: n for n in manifest.nodes}

  assert node_index["raw.raw_effective_materialization_default"].materialization == "table"
  assert node_index["raw.raw_effective_materialization_override"].materialization == "view"
