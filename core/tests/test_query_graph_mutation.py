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

from metadata.models import (
  QueryFilterNode,
  QueryNode,
  QuerySelectNode,
  QueryUnionBranch,
  QueryUnionBranchMapping,
  QueryUnionNode,
  QueryUnionOutputColumn,
  TargetColumn,
  TargetDataset,
  TargetSchema,
)
from metadata.services.query_graph_mutation import (
  QueryGraphMutationError,
  insert_unary_node_after,
  insert_unary_node_before,
  insert_unary_node_into_union_branch,
  insert_union_node_after,
  insert_union_node_before,
  insert_union_node_into_union_branch,
  list_query_input_references,
  move_unary_node_after,
  move_unary_node_before,
  move_unary_node_into_union_branch,
  remove_unary_node,
)


def _true_predicate() -> dict:
  return {
    "version": 1,
    "expression": {
      "type": "comparison",
      "operator": "=",
      "left": {"type": "literal", "value": 1},
      "right": {"type": "literal", "value": 1},
    },
  }


def _column_predicate(name: str) -> dict:
  return {
    "version": 1,
    "expression": {
      "type": "comparison",
      "operator": "=",
      "left": {"type": "column", "name": name},
      "right": {"type": "literal", "value": 1},
    },
  }


def _dataset(name: str = "query_graph_mutation") -> tuple[TargetDataset, QueryNode]:
  schema, _ = TargetSchema.objects.get_or_create(
    short_name="bizcore",
    defaults={"schema_name": "bizcore"},
  )
  td = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=name,
  )
  root = QueryNode.objects.create(
    target_dataset=td,
    node_type="select",
    name="Base select",
  )
  QuerySelectNode.objects.create(node=root, use_dataset_definition=True)
  td.query_root = root
  td.query_head = root
  td.save(update_fields=["query_root", "query_head"])
  return td, root


def _filter(td: TargetDataset, input_node: QueryNode, name: str, *, predicate=None) -> QueryNode:
  node = QueryNode.objects.create(
    target_dataset=td,
    node_type="filter",
    name=name,
  )
  QueryFilterNode.objects.create(
    node=node,
    input_node=input_node,
    predicate=predicate or _true_predicate(),
  )
  return node


def _union(
  td: TargetDataset,
  branch_inputs: list[QueryNode],
  name: str = "Union",
) -> tuple[QueryNode, list[QueryUnionBranch]]:
  node = QueryNode.objects.create(
    target_dataset=td,
    node_type="union",
    name=name,
  )
  union = QueryUnionNode.objects.create(node=node, mode="union_all")
  branches = [
    QueryUnionBranch.objects.create(
      union_node=union,
      input_node=input_node,
      ordinal_position=ordinal_position,
    )
    for ordinal_position, input_node in enumerate(branch_inputs)
  ]
  return node, branches


@pytest.mark.django_db(transaction=True)
def test_insert_before_rewires_only_target_input_and_keeps_head():
  td, root = _dataset("query_graph_insert_before")
  later = _filter(td, root, "Later filter")
  td.query_head = later
  td.save(update_fields=["query_head"])

  inserted = _filter(td, root, "Inserted filter")

  insert_unary_node_before(td, inserted, later)

  td.refresh_from_db()
  later.filter.refresh_from_db()
  inserted.filter.refresh_from_db()
  assert td.query_root_id == root.id
  assert td.query_head_id == later.id
  assert later.filter.input_node_id == inserted.id
  assert inserted.filter.input_node_id == root.id


@pytest.mark.django_db(transaction=True)
def test_insert_after_rewires_unique_downstream_edge():
  td, root = _dataset("query_graph_insert_after")
  later = _filter(td, root, "Later filter")
  td.query_head = later
  td.save(update_fields=["query_head"])

  inserted = _filter(td, root, "Inserted filter")

  insert_unary_node_after(td, inserted, root)

  td.refresh_from_db()
  later.filter.refresh_from_db()
  inserted.filter.refresh_from_db()
  assert td.query_head_id == later.id
  assert later.filter.input_node_id == inserted.id
  assert inserted.filter.input_node_id == root.id


@pytest.mark.django_db(transaction=True)
def test_insert_after_head_promotes_inserted_node_to_query_head():
  td, root = _dataset("query_graph_insert_after_head")
  inserted = _filter(td, root, "Inserted filter")

  insert_unary_node_after(td, inserted, root)

  td.refresh_from_db()
  inserted.filter.refresh_from_db()
  assert td.query_head_id == inserted.id
  assert inserted.filter.input_node_id == root.id


@pytest.mark.django_db(transaction=True)
def test_move_head_before_existing_step_reorders_chain_without_positions():
  td, root = _dataset("query_graph_move_before")
  first = _filter(td, root, "First filter")
  second = _filter(td, first, "Second filter")
  td.query_head = second
  td.save(update_fields=["query_head"])

  move_unary_node_before(td, second, first)

  td.refresh_from_db()
  first.filter.refresh_from_db()
  second.filter.refresh_from_db()
  assert td.query_head_id == first.id
  assert first.filter.input_node_id == second.id
  assert second.filter.input_node_id == root.id


@pytest.mark.django_db(transaction=True)
def test_move_step_after_head_promotes_moved_step_to_query_head():
  td, root = _dataset("query_graph_move_after")
  first = _filter(td, root, "First filter")
  second = _filter(td, first, "Second filter")
  td.query_head = second
  td.save(update_fields=["query_head"])

  move_unary_node_after(td, first, second)

  td.refresh_from_db()
  first.filter.refresh_from_db()
  second.filter.refresh_from_db()
  assert td.query_head_id == first.id
  assert first.filter.input_node_id == second.id
  assert second.filter.input_node_id == root.id


@pytest.mark.django_db(transaction=True)
def test_insert_after_rejects_ambiguous_downstream_edges():
  td, root = _dataset("query_graph_ambiguous")
  first = _filter(td, root, "First filter")
  _filter(td, root, "Disconnected consumer")
  td.query_head = first
  td.save(update_fields=["query_head"])

  inserted = _filter(td, root, "Inserted filter")

  with pytest.raises(QueryGraphMutationError, match="expected one downstream edge"):
    insert_unary_node_after(td, inserted, root)

  td.refresh_from_db()
  first.filter.refresh_from_db()
  assert td.query_head_id == first.id
  assert first.filter.input_node_id == root.id


@pytest.mark.django_db(transaction=True)
def test_invalid_mutation_rolls_back_entire_graph_rewrite():
  td, root = _dataset("query_graph_rollback")
  valid = _filter(td, root, "Valid filter")
  td.query_head = valid
  td.save(update_fields=["query_head"])

  invalid = _filter(
    td,
    root,
    "Invalid filter",
    predicate=_column_predicate("missing_column"),
  )

  with pytest.raises(QueryGraphMutationError, match="invalid query contract"):
    insert_unary_node_before(td, invalid, valid)

  td.refresh_from_db()
  valid.filter.refresh_from_db()
  invalid.filter.refresh_from_db()
  assert td.query_head_id == valid.id
  assert valid.filter.input_node_id == root.id
  assert invalid.filter.input_node_id == root.id


@pytest.mark.django_db(transaction=True)
def test_query_input_reference_inventory_is_deterministic():
  td, root = _dataset("query_graph_refs")
  first = _filter(td, root, "First filter")
  second = _filter(td, root, "Second filter")

  refs = list_query_input_references(root)

  assert [ref.consumer_node_id for ref in refs] == sorted([first.id, second.id])
  assert all(ref.input_node_id == root.id for ref in refs)
  assert all(ref.kind == "filter" for ref in refs)


@pytest.mark.django_db(transaction=True)
def test_insert_into_explicit_union_branch_rewires_only_selected_branch():
  td, root = _dataset("query_graph_union_branch_insert")
  union, branches = _union(td, [root, root])
  td.query_head = union
  td.save(update_fields=["query_head"])

  inserted = _filter(td, root, "Branch filter")

  insert_unary_node_into_union_branch(td, inserted, branches[1])

  branches[0].refresh_from_db()
  branches[1].refresh_from_db()
  inserted.filter.refresh_from_db()
  assert branches[0].input_node_id == root.id
  assert branches[1].input_node_id == inserted.id
  assert inserted.filter.input_node_id == root.id


@pytest.mark.django_db(transaction=True)
def test_generic_insert_before_union_requires_explicit_branch_context():
  td, root = _dataset("query_graph_union_before_ambiguous")
  union, _branches = _union(td, [root, root])
  td.query_head = union
  td.save(update_fields=["query_head"])
  inserted = _filter(td, root, "Branch filter")

  with pytest.raises(QueryGraphMutationError, match="choose the specific UNION branch"):
    insert_unary_node_before(td, inserted, union)


@pytest.mark.django_db(transaction=True)
def test_insert_after_union_applies_step_to_union_result():
  td, root = _dataset("query_graph_union_after")
  union, _branches = _union(td, [root, root])
  td.query_head = union
  td.save(update_fields=["query_head"])
  inserted = _filter(td, root, "After union")

  insert_unary_node_after(td, inserted, union)

  td.refresh_from_db()
  inserted.filter.refresh_from_db()
  assert td.query_head_id == inserted.id
  assert inserted.filter.input_node_id == union.id


@pytest.mark.django_db(transaction=True)
def test_move_branch_step_after_union_crosses_union_boundary():
  td, root = _dataset("query_graph_union_move_after")
  branch_filter = _filter(td, root, "Branch filter")
  union, branches = _union(td, [branch_filter, root])
  td.query_head = union
  td.save(update_fields=["query_head"])

  move_unary_node_after(td, branch_filter, union)

  td.refresh_from_db()
  branches[0].refresh_from_db()
  branch_filter.filter.refresh_from_db()
  assert branches[0].input_node_id == root.id
  assert td.query_head_id == branch_filter.id
  assert branch_filter.filter.input_node_id == union.id


@pytest.mark.django_db(transaction=True)
def test_move_step_after_union_into_explicit_branch():
  td, root = _dataset("query_graph_union_move_into_branch")
  union, branches = _union(td, [root, root])
  after_union = _filter(td, union, "After union")
  td.query_head = after_union
  td.save(update_fields=["query_head"])

  move_unary_node_into_union_branch(td, after_union, branches[0])

  td.refresh_from_db()
  branches[0].refresh_from_db()
  branches[1].refresh_from_db()
  after_union.filter.refresh_from_db()
  assert td.query_head_id == union.id
  assert branches[0].input_node_id == after_union.id
  assert branches[1].input_node_id == root.id
  assert after_union.filter.input_node_id == root.id


@pytest.mark.django_db(transaction=True)
def test_move_step_between_union_branches_is_explicit_and_deterministic():
  td, root = _dataset("query_graph_union_move_between_branches")
  branch_filter = _filter(td, root, "Branch filter")
  union, branches = _union(td, [branch_filter, root])
  td.query_head = union
  td.save(update_fields=["query_head"])

  move_unary_node_into_union_branch(td, branch_filter, branches[1])

  branches[0].refresh_from_db()
  branches[1].refresh_from_db()
  branch_filter.filter.refresh_from_db()
  assert branches[0].input_node_id == root.id
  assert branches[1].input_node_id == branch_filter.id
  assert branch_filter.filter.input_node_id == root.id


def _detached_union(td: TargetDataset, name: str = "Inserted union") -> QueryNode:
  node = QueryNode.objects.create(
    target_dataset=td,
    node_type="union",
    name=name,
  )
  QueryUnionNode.objects.create(node=node, mode="union_all")
  return node


@pytest.mark.django_db(transaction=True)
def test_insert_union_after_head_preserves_existing_contract_as_first_branch():
  td, root = _dataset("query_graph_insert_union_after")
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="customer_no",
    ordinal_position=1,
    datatype="INTEGER",
  )
  inserted = _detached_union(td)

  branch = insert_union_node_after(td, inserted, root)

  td.refresh_from_db()
  assert td.query_head_id == inserted.id
  assert branch.input_node_id == root.id
  outputs = list(inserted.union.output_columns.order_by("ordinal_position"))
  assert [column.output_name for column in outputs] == ["customer_no"]
  assert outputs[0].datatype == "INTEGER"
  assert list(branch.mappings.values_list("input_column_name", flat=True)) == ["customer_no"]


@pytest.mark.django_db(transaction=True)
def test_insert_union_before_unary_step_keeps_downstream_step_connected():
  td, root = _dataset("query_graph_insert_union_before")
  later = _filter(td, root, "Later filter")
  td.query_head = later
  td.save(update_fields=["query_head"])
  inserted = _detached_union(td)

  branch = insert_union_node_before(td, inserted, later)

  later.filter.refresh_from_db()
  assert later.filter.input_node_id == inserted.id
  assert branch.input_node_id == root.id


@pytest.mark.django_db(transaction=True)
def test_insert_union_into_explicit_union_branch_changes_only_selected_branch():
  td, root = _dataset("query_graph_insert_union_nested")
  outer_union, branches = _union(td, [root, root])
  td.query_head = outer_union
  td.save(update_fields=["query_head"])
  inserted = _detached_union(td)

  primary_branch = insert_union_node_into_union_branch(td, inserted, branches[1])

  branches[0].refresh_from_db()
  branches[1].refresh_from_db()
  assert branches[0].input_node_id == root.id
  assert branches[1].input_node_id == inserted.id
  assert primary_branch.input_node_id == root.id
@pytest.mark.django_db(transaction=True)
def test_remove_middle_unary_step_reconnects_graph_and_deletes_node():
  td, root = _dataset("query_graph_remove_middle")
  first = _filter(td, root, "First filter")
  middle = _filter(td, first, "Middle filter")
  last = _filter(td, middle, "Last filter")
  td.query_head = last
  td.save(update_fields=["query_head"])

  remove_unary_node(td, middle)

  td.refresh_from_db()
  last.filter.refresh_from_db()
  assert td.query_head_id == last.id
  assert last.filter.input_node_id == first.id
  assert not QueryNode.objects.filter(pk=middle.pk).exists()


@pytest.mark.django_db(transaction=True)
def test_remove_head_promotes_previous_input_to_query_head():
  td, root = _dataset("query_graph_remove_head")
  first = _filter(td, root, "First filter")
  last = _filter(td, first, "Last filter")
  td.query_head = last
  td.save(update_fields=["query_head"])

  remove_unary_node(td, last)

  td.refresh_from_db()
  assert td.query_head_id == first.id
  assert not QueryNode.objects.filter(pk=last.pk).exists()


@pytest.mark.django_db(transaction=True)
def test_insert_local_step_into_foreign_union_branch_uses_foreign_output_boundary():
  td, root = _dataset("query_graph_foreign_branch_owner")
  foreign_td, foreign_root = _dataset("query_graph_foreign_branch_source")
  TargetColumn.objects.create(
    target_dataset=foreign_td,
    target_column_name="product_model_name",
    ordinal_position=1,
    datatype="VARCHAR",
    max_length=100,
  )
  union, branches = _union(td, [root, foreign_root])
  td.query_head = union
  td.save(update_fields=["query_head"])
  inserted = _filter(td, root, "Local branch filter", predicate=_column_predicate("product_model_name"))

  insert_unary_node_into_union_branch(td, inserted, branches[1])

  branches[1].refresh_from_db()
  inserted.filter.refresh_from_db()
  assert branches[1].input_node_id == inserted.id
  assert inserted.filter.input_node_id == foreign_root.id


@pytest.mark.django_db(transaction=True)
def test_external_consumer_does_not_block_extending_dataset_query_head():
  source_td, source_root = _dataset("query_graph_external_source")
  consumer_td, consumer_root = _dataset("query_graph_external_consumer")
  union, _branches = _union(consumer_td, [consumer_root, source_root])
  consumer_td.query_head = union
  consumer_td.save(update_fields=["query_head"])
  inserted = _filter(source_td, source_root, "Source extension")

  insert_unary_node_after(source_td, inserted, source_root)

  source_td.refresh_from_db()
  inserted.filter.refresh_from_db()
  assert source_td.query_head_id == inserted.id
  assert inserted.filter.input_node_id == source_root.id
  assert QueryUnionBranch.objects.get(union_node=union.union, input_node=source_root)


@pytest.mark.django_db(transaction=True)
def test_insert_into_union_branch_can_preserve_preexisting_missing_mapping_for_repair():
  td, root = _dataset("query_graph_union_repair_owner")
  foreign_td, foreign_root = _dataset("query_graph_union_repair_source")
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="prior_product_model_name",
    ordinal_position=1,
    datatype="VARCHAR",
    max_length=100,
  )
  TargetColumn.objects.create(
    target_dataset=foreign_td,
    target_column_name="product_model_name",
    ordinal_position=1,
    datatype="VARCHAR",
    max_length=100,
  )
  union, branches = _union(td, [root, foreign_root])
  output = QueryUnionOutputColumn.objects.create(
    union_node=union.union,
    output_name="prior_product_model_name",
    ordinal_position=1,
    datatype="VARCHAR",
    max_length=100,
  )
  QueryUnionBranchMapping.objects.create(
    branch=branches[0],
    output_column=output,
    input_column_name="prior_product_model_name",
  )
  td.query_head = union
  td.save(update_fields=["query_head"])
  inserted = _filter(td, root, "Repair branch step")

  insert_unary_node_into_union_branch(td, inserted, branches[1])

  branches[1].refresh_from_db()
  inserted.filter.refresh_from_db()
  assert branches[1].input_node_id == inserted.id
  assert inserted.filter.input_node_id == foreign_root.id
