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
from metadata.execution.load_graph import (
  EXECUTION_DEPENDENCY_HIST_BASE_READY,
  EXECUTION_DEPENDENCY_LINEAGE_INPUT,
)
from metadata.models import (
  PartialLoad,
  SourceDataset,
  System,
  TargetDataset,
  TargetDatasetInput,
  TargetSchema,
)


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
def test_manifest_excludes_inactive_target_datasets():
  """
  Verify retired TargetDatasets do not become scheduler tasks.
  """
  raw_schema, _ = TargetSchema.objects.get_or_create(
    short_name="raw",
    schema_name="raw",
  )
  active_target = TargetDataset.objects.create(
    target_schema=raw_schema,
    target_dataset_name="raw_active",
    incremental_strategy="full",
    is_system_managed=False,
    active=True,
  )
  inactive_target = TargetDataset.objects.create(
    target_schema=raw_schema,
    target_dataset_name="raw_retired",
    incremental_strategy="full",
    is_system_managed=False,
    active=False,
  )

  manifest = build_manifest(
    profile_name="dev",
    target_system_short="dbdwh",
    include_system_managed=True,
    include_sources=False,
  )
  node_ids = {
    node.id
    for node in manifest.nodes
  }

  assert (
    f"{active_target.target_schema.short_name}."
    f"{active_target.target_dataset_name}"
  ) in node_ids
  assert (
    f"{inactive_target.target_schema.short_name}."
    f"{inactive_target.target_dataset_name}"
  ) not in node_ids


@pytest.mark.django_db
def test_manifest_rejects_active_target_with_inactive_upstream():
  """
  Verify an inactive required upstream cannot be silently reintroduced.
  """
  raw_schema, _ = TargetSchema.objects.get_or_create(
    short_name="raw",
    schema_name="raw",
  )
  stage_schema, _ = TargetSchema.objects.get_or_create(
    short_name="stage",
    schema_name="stage",
  )
  inactive_upstream = TargetDataset.objects.create(
    target_schema=raw_schema,
    target_dataset_name="raw_retired",
    incremental_strategy="full",
    is_system_managed=False,
    active=False,
  )
  active_downstream = TargetDataset.objects.create(
    target_schema=stage_schema,
    target_dataset_name="stg_active",
    incremental_strategy="full",
    is_system_managed=False,
    active=True,
  )
  TargetDatasetInput.objects.create(
    target_dataset=active_downstream,
    source_dataset=None,
    upstream_target_dataset=inactive_upstream,
    role="primary",
    active=True,
  )

  with pytest.raises(
    ValueError,
    match="stage\\.stg_active -> raw\\.raw_retired",
  ):
    build_manifest(
      profile_name="dev",
      target_system_short="dbdwh",
      include_system_managed=True,
      include_sources=False,
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


@pytest.mark.django_db
def test_manifest_models_history_companion_as_execution_only_dependency():
  rawcore_schema, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={"schema_name": "rawcore"},
  )
  if rawcore_schema.schema_name != "rawcore":
    rawcore_schema.schema_name = "rawcore"
    rawcore_schema.save(update_fields=["schema_name"])
  base = TargetDataset.objects.create(
    target_schema=rawcore_schema,
    target_dataset_name="rc_customer_manifest",
    incremental_strategy="full",
    historize=True,
    lineage_key="generated:rawcore:customer-manifest-hist",
    is_system_managed=True,
  )
  TargetDataset.objects.create(
    target_schema=rawcore_schema,
    target_dataset_name="rc_customer_manifest_hist",
    incremental_strategy="historize",
    historize=False,
    lineage_key=base.lineage_key,
    is_system_managed=True,
  )

  manifest = build_manifest(
    profile_name="dev",
    target_system_short="dbdwh",
    include_system_managed=True,
    include_sources=False,
  )
  node_index = {n.id: n for n in manifest.nodes}

  base_id = "rawcore.rc_customer_manifest"
  hist_id = "rawcore.rc_customer_manifest_hist"

  assert base_id in node_index[hist_id].deps
  assert base_id not in node_index[hist_id].lineage_deps
  assert (
    base_id,
    EXECUTION_DEPENDENCY_HIST_BASE_READY,
    None,
  ) in _execution_dep_tuples(node_index[hist_id])

  level_index = {
    node_id: index
    for index, level in enumerate(manifest.levels)
    for node_id in level
  }
  assert level_index[base_id] < level_index[hist_id]


@pytest.mark.django_db
def test_manifest_exposes_full_and_resolved_partial_load_scopes():
  """
  Verify named load scopes are fully resolved and projected onto target nodes.
  """
  raw_schema, _ = TargetSchema.objects.get_or_create(
    short_name="raw",
    defaults={"schema_name": "raw"},
  )
  stage_schema, _ = TargetSchema.objects.get_or_create(
    short_name="stage",
    defaults={"schema_name": "stage"},
  )
  rawcore_schema, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={"schema_name": "rawcore"},
  )
  bizcore_schema, _ = TargetSchema.objects.get_or_create(
    short_name="bizcore",
    defaults={"schema_name": "bizcore"},
  )

  raw = TargetDataset.objects.create(
    target_schema=raw_schema,
    target_dataset_name="raw_sales_manifest",
    incremental_strategy="full",
    is_system_managed=False,
  )
  stage = TargetDataset.objects.create(
    target_schema=stage_schema,
    target_dataset_name="stg_sales_manifest",
    incremental_strategy="full",
    is_system_managed=False,
  )
  TargetDatasetInput.objects.create(
    target_dataset=stage,
    upstream_target_dataset=raw,
    source_dataset=None,
    role="primary",
    active=True,
  )

  rawcore = TargetDataset.objects.create(
    target_schema=rawcore_schema,
    target_dataset_name="rc_sales_manifest",
    incremental_strategy="full",
    historize=True,
    lineage_key="generated:rawcore:sales-manifest",
    is_system_managed=False,
  )
  TargetDatasetInput.objects.create(
    target_dataset=rawcore,
    upstream_target_dataset=stage,
    source_dataset=None,
    role="primary",
    active=True,
  )
  TargetDataset.objects.create(
    target_schema=rawcore_schema,
    target_dataset_name="rc_sales_manifest_hist",
    incremental_strategy="historize",
    historize=False,
    lineage_key=rawcore.lineage_key,
    is_system_managed=True,
  )

  bizcore = TargetDataset.objects.create(
    target_schema=bizcore_schema,
    target_dataset_name="bc_sales_manifest",
    incremental_strategy="full",
    is_system_managed=False,
  )
  TargetDatasetInput.objects.create(
    target_dataset=bizcore,
    upstream_target_dataset=rawcore,
    source_dataset=None,
    role="primary",
    active=True,
  )

  TargetDataset.objects.create(
    target_schema=raw_schema,
    target_dataset_name="raw_unrelated_manifest",
    incremental_strategy="full",
    is_system_managed=False,
  )

  sales = PartialLoad.objects.create(
    name="sales",
    description="Sales execution scope",
  )
  sales.datasets.add(bizcore)

  finance = PartialLoad.objects.create(
    name="finance",
    description="Finance execution scope",
  )
  finance.datasets.add(rawcore)

  manifest = build_manifest(
    profile_name="dev",
    target_system_short="dbdwh",
    include_system_managed=True,
    include_sources=False,
  )

  scope_index = {
    load_scope.name: load_scope
    for load_scope in manifest.load_scopes
  }
  assert list(scope_index) == ["full", "finance", "sales"]

  full_scope = scope_index["full"]
  assert full_scope.scope_mode == "all"
  assert full_scope.root_dataset_ids == []
  assert set(full_scope.dataset_ids) == {
    "raw.raw_sales_manifest",
    "stage.stg_sales_manifest",
    "rawcore.rc_sales_manifest",
    "rawcore.rc_sales_manifest_hist",
    "bizcore.bc_sales_manifest",
    "raw.raw_unrelated_manifest",
  }

  finance_scope = scope_index["finance"]
  assert finance_scope.scope_mode == "partial_load"
  assert finance_scope.root_dataset_ids == [
    "rawcore.rc_sales_manifest",
  ]
  assert set(finance_scope.dataset_ids) == {
    "raw.raw_sales_manifest",
    "stage.stg_sales_manifest",
    "rawcore.rc_sales_manifest",
    "rawcore.rc_sales_manifest_hist",
  }
  assert "bizcore.bc_sales_manifest" not in finance_scope.dataset_ids

  sales_scope = scope_index["sales"]
  assert sales_scope.scope_mode == "partial_load"
  assert sales_scope.root_dataset_ids == [
    "bizcore.bc_sales_manifest",
  ]
  assert set(sales_scope.dataset_ids) == {
    "raw.raw_sales_manifest",
    "stage.stg_sales_manifest",
    "rawcore.rc_sales_manifest",
    "rawcore.rc_sales_manifest_hist",
    "bizcore.bc_sales_manifest",
  }
  assert "raw.raw_unrelated_manifest" not in sales_scope.dataset_ids
  assert sales_scope.dataset_ids.index(
    "rawcore.rc_sales_manifest"
  ) < sales_scope.dataset_ids.index(
    "rawcore.rc_sales_manifest_hist"
  )

  node_index = {node.id: node for node in manifest.nodes}
  assert node_index[
    "raw.raw_sales_manifest"
  ].load_scopes == ["full", "finance", "sales"]
  assert node_index[
    "rawcore.rc_sales_manifest"
  ].load_scopes == ["full", "finance", "sales"]
  assert node_index[
    "rawcore.rc_sales_manifest_hist"
  ].load_scopes == ["full", "finance", "sales"]
  assert node_index[
    "bizcore.bc_sales_manifest"
  ].load_scopes == ["full", "sales"]
  assert node_index[
    "raw.raw_unrelated_manifest"
  ].load_scopes == ["full"]

  payload = manifest_to_dict(manifest)
  payload_scope_index = {
    load_scope["name"]: load_scope
    for load_scope in payload["load_scopes"]
  }
  assert payload_scope_index["sales"] == {
    "name": "sales",
    "scope_mode": "partial_load",
    "root_dataset_ids": ["bizcore.bc_sales_manifest"],
    "dataset_ids": sales_scope.dataset_ids,
  }
  payload_node_index = {
    node["id"]: node
    for node in payload["nodes"]
  }
  assert payload_node_index[
    "rawcore.rc_sales_manifest"
  ]["load_scopes"] == ["full", "finance", "sales"]


@pytest.mark.django_db
def test_manifest_source_nodes_are_not_members_of_execution_load_scopes():
  """SourceDataset manifest nodes remain explanatory and never become load tasks."""
  raw_schema, _ = TargetSchema.objects.get_or_create(
    short_name="raw",
    defaults={"schema_name": "raw"},
  )
  source_system = System.objects.create(
    short_name="scope_src",
    name="Scope Source",
  )
  source = SourceDataset.objects.create(
    source_system=source_system,
    schema_name="public",
    source_dataset_name="orders",
  )
  raw = TargetDataset.objects.create(
    target_schema=raw_schema,
    target_dataset_name="raw_scope_orders",
    incremental_strategy="full",
    is_system_managed=False,
  )
  TargetDatasetInput.objects.create(
    target_dataset=raw,
    source_dataset=source,
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
  node_index = {node.id: node for node in manifest.nodes}

  assert node_index["raw.raw_scope_orders"].load_scopes == ["full"]
  assert node_index[
    "source.scope_src.public.orders"
  ].load_scopes == []


@pytest.mark.django_db
def test_manifest_fails_closed_when_filter_makes_partial_load_incomplete():
  """
  Verify manifest filters cannot silently remove mandatory Partial Load members.
  """
  rawcore_schema, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={"schema_name": "rawcore"},
  )
  base = TargetDataset.objects.create(
    target_schema=rawcore_schema,
    target_dataset_name="rc_filtered_scope",
    incremental_strategy="full",
    historize=True,
    lineage_key="generated:rawcore:filtered-scope",
    is_system_managed=False,
  )
  TargetDataset.objects.create(
    target_schema=rawcore_schema,
    target_dataset_name="rc_filtered_scope_hist",
    incremental_strategy="historize",
    historize=False,
    lineage_key=base.lineage_key,
    is_system_managed=True,
  )

  partial_load = PartialLoad.objects.create(
    name="filtered",
  )
  partial_load.datasets.add(base)

  with pytest.raises(
    ValueError,
    match=(
      "Partial Load 'filtered'.*"
      "rawcore\\.rc_filtered_scope_hist"
    ),
  ):
    build_manifest(
      profile_name="dev",
      target_system_short="dbdwh",
      include_system_managed=False,
      include_sources=False,
    )
