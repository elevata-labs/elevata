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

from metadata.promotion.contracts import (
  MetadataDependencyPhase as Phase,
  MetadataIdentityContract,
  MetadataLifecycleStrategy as Lifecycle,
  MetadataManagedApplyMode,
  MetadataModelContract,
  MetadataRelationshipContract,
  MetadataTransportRegistry,
)


AUDIT_EXCLUDED_FIELDS = frozenset({
  "id",
  "created_at",
  "updated_at",
  "created_by",
  "updated_by",
})

REQUIRED_SYSTEM_MANAGED_TARGET_SCHEMA_KEYS = (
  "raw",
  "stage",
  "rawcore",
  "bizcore",
  "serving",
)


def _identity(
  *components: str,
  fallback_components: tuple[tuple[str, ...], ...] = (),
  normalize_empty_components: frozenset[str] = frozenset(),
) -> MetadataIdentityContract:
  return MetadataIdentityContract(
    components=tuple(components),
    fallback_components=fallback_components,
    normalize_empty_components=normalize_empty_components,
  )


def _contract(
  model_name: str,
  *,
  identity: MetadataIdentityContract,
  fields: tuple[str, ...],
  phase: Phase,
  lifecycle: Lifecycle = Lifecycle.DELETE,
  transport_filter: tuple[tuple[str, object], ...] = (),
  deferred_fields: tuple[str, ...] = (),
  relationships: tuple[str, ...] = (),
  excluded: tuple[str, ...] = (),
  managed_field: str | None = None,
  system_managed_apply_mode: MetadataManagedApplyMode = MetadataManagedApplyMode.NORMAL,
  system_managed_update_fields: tuple[str, ...] = (),
) -> MetadataModelContract:
  return MetadataModelContract(
    model_name=model_name,
    identity=identity,
    transport_fields=frozenset(fields),
    relationship_fields=frozenset(relationships),
    excluded_fields=AUDIT_EXCLUDED_FIELDS | frozenset(excluded),
    dependency_phase=phase,
    lifecycle=lifecycle,
    transport_filter=transport_filter,
    deferred_fields=frozenset(deferred_fields),
    managed_field=managed_field,
    system_managed_apply_mode=system_managed_apply_mode,
    system_managed_update_fields=frozenset(system_managed_update_fields),
  )


MODEL_CONTRACTS = {
  "PartialLoad": _contract(
    "PartialLoad",
    identity=_identity("name"),
    fields=("name", "description"),
    phase=Phase.FOUNDATION,
  ),
  "Team": _contract(
    "Team",
    identity=_identity("name"),
    fields=("name", "description"),
    phase=Phase.FOUNDATION,
  ),
  "Person": _contract(
    "Person",
    identity=_identity("email"),
    fields=("email", "name"),
    relationships=("team",),
    phase=Phase.FOUNDATION,
  ),
  "System": _contract(
    "System",
    identity=_identity("short_name"),
    fields=(
      "short_name", "name", "description", "type", "target_short_name",
      "is_source", "is_target", "include_ingest", "generate_raw_tables",
      "active",
    ),
    excluded=("retired_at",),
    phase=Phase.FOUNDATION,
    lifecycle=Lifecycle.RETIRE,
  ),
  "SourceDataset": _contract(
    "SourceDataset",
    identity=_identity(
      "source_system_key",
      "schema_name",
      "source_dataset_name",
      normalize_empty_components=frozenset({"schema_name"}),
    ),
    fields=(
      "source_system", "schema_name", "source_dataset_name", "description",
      "integrate", "static_filter", "incremental", "increment_filter",
      "manual_model", "distinct_select", "generate_raw_table",
      "ingestion_config", "active",
    ),
    excluded=("owner", "retired_at"),
    phase=Phase.SOURCE,
    lifecycle=Lifecycle.RETIRE,
  ),
  "SourceDatasetIncrementPolicy": _contract(
    "SourceDatasetIncrementPolicy",
    identity=_identity("source_dataset_key", "environment"),
    fields=(
      "source_dataset", "environment", "increment_interval_length",
      "increment_interval_unit", "active",
    ),
    phase=Phase.SOURCE_CHILD,
    lifecycle=Lifecycle.DEACTIVATE,
    transport_filter=(("active", True),),
  ),
  "SourceDatasetGroup": _contract(
    "SourceDatasetGroup",
    identity=_identity("target_short_name", "unified_source_dataset_name"),
    fields=("target_short_name", "unified_source_dataset_name", "description"),
    relationships=("owner",),
    phase=Phase.SOURCE,
  ),
  "SourceDatasetGroupMembership": _contract(
    "SourceDatasetGroupMembership",
    identity=_identity("group_key", "source_dataset_key"),
    fields=(
      "group", "source_dataset", "is_primary_system",
      "source_identity_id", "source_identity_ordinal",
    ),
    phase=Phase.SOURCE_RELATION,
  ),
  "SourceDatasetOwnership": _contract(
    "SourceDatasetOwnership",
    identity=_identity("source_dataset_key", "person_email", "role"),
    fields=(
      "source_dataset", "person", "role", "is_primary_owner",
      "since", "until", "remark",
    ),
    phase=Phase.GOVERNANCE_ASSIGNMENT,
  ),
  "SourceColumn": _contract(
    "SourceColumn",
    identity=_identity("source_dataset_key", "source_column_name"),
    fields=(
      "source_dataset", "source_column_name", "ordinal_position",
      "source_datatype_raw", "datatype", "max_length", "decimal_precision",
      "decimal_scale", "nullable", "primary_key_column",
      "referenced_source_dataset_name", "json_path", "description",
      "integrate", "pii_level", "remark",
    ),
    phase=Phase.SOURCE_CHILD,
  ),
  "TargetSchema": _contract(
    "TargetSchema",
    identity=_identity("short_name"),
    fields=(
      "short_name", "display_name", "description", "schema_name",
      "physical_prefix", "generate_layer", "is_user_visible",
      "default_materialization_type", "default_historize",
      "incremental_strategy_default", "sensitivity_default",
      "access_intent_default", "surrogate_keys_enabled",
      "surrogate_key_algorithm", "surrogate_key_null_token",
      "surrogate_key_pair_separator", "surrogate_key_component_separator",
      "pepper_strategy", "is_system_managed",
    ),
    phase=Phase.TARGET_SCHEMA,
    managed_field="is_system_managed",
    system_managed_apply_mode=MetadataManagedApplyMode.VERIFY_ONLY,
    system_managed_update_fields=(
      "display_name",
      "description",
      "sensitivity_default",
      "access_intent_default",
    ),
  ),
  "TargetDataset": _contract(
    "TargetDataset",
    identity=_identity(
      "target_schema_key",
      "lineage_key",
      "dataset_variant",
      fallback_components=(("target_schema_key", "target_dataset_name"),),
    ),
    fields=(
      "target_schema", "target_dataset_name", "description", "handle_deletes",
      "historize", "combination_mode", "biz_entity_role", "biz_grain_note",
      "incremental_strategy", "incremental_source", "manual_model",
      "distinct_select", "static_filter", "query_root", "query_head",
      "materialization_type", "sensitivity", "access_intent", "active",
      "lineage_key", "former_names", "is_system_managed",
    ),
    relationships=("partial_load",),
    excluded=(
      "source_datasets", "upstream_datasets", "owner", "retired_at",
    ),
    phase=Phase.TARGET_DATASET,
    lifecycle=Lifecycle.RETIRE,
    deferred_fields=("query_root", "query_head"),
  ),
  "QueryNode": _contract(
    "QueryNode",
    identity=_identity("logical_key"),
    fields=("target_dataset", "logical_key", "node_type", "name", "active"),
    phase=Phase.QUERY_NODE,
    lifecycle=Lifecycle.DEACTIVATE,
  ),
  "QuerySelectNode": _contract(
    "QuerySelectNode",
    identity=_identity("node_key"),
    fields=("node", "use_dataset_definition"),
    phase=Phase.QUERY_DEFINITION,
  ),
  "QueryAggregateNode": _contract(
    "QueryAggregateNode",
    identity=_identity("node_key"),
    fields=("node", "input_node", "mode"),
    phase=Phase.QUERY_DEFINITION,
  ),
  "QueryAggregateGroupKey": _contract(
    "QueryAggregateGroupKey",
    identity=_identity("aggregate_node_key", "ordinal_position"),
    fields=(
      "aggregate_node", "input_column_name", "output_name",
      "ordinal_position",
    ),
    phase=Phase.QUERY_CHILD,
  ),
  "QueryAggregateMeasure": _contract(
    "QueryAggregateMeasure",
    identity=_identity("aggregate_node_key", "ordinal_position"),
    fields=(
      "aggregate_node", "output_name", "function", "input_column_name",
      "delimiter", "order_by", "distinct", "ordinal_position",
    ),
    phase=Phase.QUERY_CHILD,
  ),
  "OrderByExpression": _contract(
    "OrderByExpression",
    identity=_identity("target_dataset_key", "name"),
    fields=("target_dataset", "name", "active"),
    phase=Phase.QUERY_NODE,
    lifecycle=Lifecycle.DEACTIVATE,
  ),
  "OrderByItem": _contract(
    "OrderByItem",
    identity=_identity("order_by_key", "ordinal_position"),
    fields=(
      "order_by", "input_column_name", "direction", "nulls_placement",
      "ordinal_position",
    ),
    phase=Phase.QUERY_CHILD,
  ),
  "QueryUnionNode": _contract(
    "QueryUnionNode",
    identity=_identity("node_key"),
    fields=("node", "mode"),
    phase=Phase.QUERY_DEFINITION,
  ),
  "QueryUnionOutputColumn": _contract(
    "QueryUnionOutputColumn",
    identity=_identity("union_node_key", "ordinal_position"),
    fields=(
      "union_node", "output_name", "ordinal_position", "datatype",
      "max_length", "decimal_precision", "decimal_scale",
    ),
    phase=Phase.QUERY_CHILD,
  ),
  "QueryUnionBranch": _contract(
    "QueryUnionBranch",
    identity=_identity("union_node_key", "ordinal_position"),
    fields=("union_node", "input_node", "ordinal_position"),
    phase=Phase.QUERY_BRANCH,
  ),
  "QueryUnionBranchMapping": _contract(
    "QueryUnionBranchMapping",
    identity=_identity("branch_key", "output_column_key"),
    fields=("branch", "output_column", "input_column_name"),
    phase=Phase.QUERY_MAPPING,
  ),
  "QueryWindowNode": _contract(
    "QueryWindowNode",
    identity=_identity("node_key"),
    fields=("node", "input_node"),
    phase=Phase.QUERY_DEFINITION,
  ),
  "PartitionByExpression": _contract(
    "PartitionByExpression",
    identity=_identity("target_dataset_key", "name"),
    fields=("target_dataset", "name", "active"),
    phase=Phase.QUERY_NODE,
    lifecycle=Lifecycle.DEACTIVATE,
  ),
  "PartitionByItem": _contract(
    "PartitionByItem",
    identity=_identity("partition_by_key", "ordinal_position"),
    fields=("partition_by", "input_column_name", "ordinal_position"),
    phase=Phase.QUERY_CHILD,
  ),
  "QueryWindowColumn": _contract(
    "QueryWindowColumn",
    identity=_identity("window_node_key", "ordinal_position"),
    fields=(
      "window_node", "output_name", "function", "partition_by",
      "order_by", "ordinal_position", "active",
    ),
    phase=Phase.QUERY_CHILD,
    lifecycle=Lifecycle.DEACTIVATE,
  ),
  "QueryWindowColumnArg": _contract(
    "QueryWindowColumnArg",
    identity=_identity("window_column_key", "ordinal_position"),
    fields=(
      "window_column", "arg_type", "column_name", "int_value",
      "str_value", "ordinal_position",
    ),
    phase=Phase.QUERY_MAPPING,
  ),
  "TargetDatasetInput": _contract(
    "TargetDatasetInput",
    identity=_identity("target_dataset_key", "upstream_kind", "upstream_key"),
    fields=(
      "target_dataset", "source_dataset", "upstream_target_dataset",
      "role", "active",
    ),
    phase=Phase.TARGET_INPUT,
    lifecycle=Lifecycle.DEACTIVATE,
  ),
  "TargetDatasetJoin": _contract(
    "TargetDatasetJoin",
    identity=_identity("target_dataset_key", "left_input_key", "right_input_key"),
    fields=(
      "target_dataset", "left_input", "right_input", "join_type",
      "join_order", "description",
    ),
    phase=Phase.TARGET_JOIN,
  ),
  "TargetDatasetJoinPredicate": _contract(
    "TargetDatasetJoinPredicate",
    identity=_identity("join_key", "ordinal_position"),
    fields=(
      "join", "ordinal_position", "left_expr", "operator", "right_expr",
      "right_expr_2",
    ),
    phase=Phase.TARGET_JOIN_PREDICATE,
  ),
  "TargetDatasetOwnership": _contract(
    "TargetDatasetOwnership",
    identity=_identity("target_dataset_key", "person_email", "role"),
    fields=(
      "target_dataset", "person", "role", "is_primary_owner",
      "since", "until", "remark",
    ),
    phase=Phase.GOVERNANCE_ASSIGNMENT,
  ),
  "TargetColumn": _contract(
    "TargetColumn",
    identity=_identity(
      "target_dataset_key",
      "lineage_key",
      fallback_components=(
        ("target_dataset_key", "target_column_name"),
      ),
    ),
    fields=(
      "target_dataset", "target_column_name", "ordinal_position", "datatype",
      "max_length", "decimal_precision", "decimal_scale", "nullable",
      "system_role", "manual_expression", "description", "pii_level",
      "remark", "sensitivity", "lineage_origin", "surrogate_expression",
      "active", "lineage_key", "former_names", "is_system_managed",
    ),
    excluded=(
      "source_columns", "upstream_columns", "profiling_stats", "retired_at",
    ),
    phase=Phase.TARGET_COLUMN,
    lifecycle=Lifecycle.RETIRE,
  ),
  "TargetColumnInput": _contract(
    "TargetColumnInput",
    identity=_identity("target_column_key", "upstream_kind", "upstream_key"),
    fields=(
      "target_column", "source_column", "upstream_target_column",
      "manual_expression", "ordinal_position", "active",
    ),
    phase=Phase.TARGET_COLUMN_RELATION,
    lifecycle=Lifecycle.DEACTIVATE,
  ),
  "TargetDatasetReference": _contract(
    "TargetDatasetReference",
    identity=_identity(
      "referencing_dataset_key",
      "reference_prefix",
      "referenced_dataset_key",
      normalize_empty_components=frozenset({"reference_prefix"}),
    ),
    fields=(
      "referencing_dataset", "referenced_dataset", "reference_prefix",
      "relationship_type", "join_condition_hint", "inferred_members_enabled",
      "default_member_fallback_enabled",
    ),
    phase=Phase.TARGET_REFERENCE,
  ),
  "TargetDatasetReferenceComponent": _contract(
    "TargetDatasetReferenceComponent",
    identity=_identity("reference_key", "from_column_key", "to_column_key"),
    fields=("reference", "from_column", "to_column", "ordinal_position"),
    phase=Phase.TARGET_REFERENCE_COMPONENT,
  ),
}


IMPLICIT_RELATIONSHIPS = (
  MetadataRelationshipContract(
    name="PersonTeamMembership",
    source_model="Person",
    source_field="team",
    target_model="Team",
    source_identity_component="person_email",
    target_identity_component="team_name",
    dependency_phase=Phase.GOVERNANCE_ASSIGNMENT,
  ),
  MetadataRelationshipContract(
    name="SourceDatasetGroupOwner",
    source_model="SourceDatasetGroup",
    source_field="owner",
    target_model="Person",
    source_identity_component="group_key",
    target_identity_component="person_email",
    dependency_phase=Phase.GOVERNANCE_ASSIGNMENT,
  ),
  MetadataRelationshipContract(
    name="TargetDatasetPartialLoadAssignment",
    source_model="TargetDataset",
    source_field="partial_load",
    target_model="PartialLoad",
    source_identity_component="target_dataset_key",
    target_identity_component="partial_load_name",
    dependency_phase=Phase.GOVERNANCE_ASSIGNMENT,
  ),
)


METADATA_TRANSPORT_REGISTRY = MetadataTransportRegistry(
  models=MODEL_CONTRACTS,
  relationships=IMPLICIT_RELATIONSHIPS,
)
