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
  QueryAggregateNode,
  QueryFilterNode,
  QueryNode,
  QuerySelectNode,
  QueryUnionBranch,
  QueryUnionNode,
  QueryUnionOutputColumn,
  TargetDataset,
  TargetSchema,
)
from metadata.views import _query_builder_structure


def _dataset(name: str) -> tuple[TargetDataset, QueryNode]:
  schema, _ = TargetSchema.objects.get_or_create(
    short_name="bizcore",
    defaults={"schema_name": "bizcore"},
  )
  dataset = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=name,
  )
  root = QueryNode.objects.create(
    target_dataset=dataset,
    node_type="select",
    name="Base select",
  )
  QuerySelectNode.objects.create(node=root, use_dataset_definition=True)
  dataset.query_root = root
  dataset.query_head = root
  dataset.save(update_fields=["query_root", "query_head"])
  return dataset, root


@pytest.mark.django_db(transaction=True)
def test_query_builder_structure_follows_unary_input_order():
  dataset, root = _dataset("builder_structure_unary")

  filter_node = QueryNode.objects.create(
    target_dataset=dataset,
    node_type="filter",
    name="Active rows",
  )
  QueryFilterNode.objects.create(
    node=filter_node,
    input_node=root,
    predicate={"version": 1, "expression": {"kind": "literal", "value": True}},
  )

  aggregate_node = QueryNode.objects.create(
    target_dataset=dataset,
    node_type="aggregate",
    name="Daily totals",
  )
  QueryAggregateNode.objects.create(
    node=aggregate_node,
    input_node=filter_node,
  )

  structure = _query_builder_structure(aggregate_node)

  assert structure["type"] == "aggregate"
  assert structure["input"]["type"] == "filter"
  assert structure["input"]["input"]["type"] == "select"
  assert structure["summary"] == "0 group keys · 0 measures"


@pytest.mark.django_db(transaction=True)
def test_query_builder_structure_exposes_ordered_union_branches():
  dataset, root = _dataset("builder_structure_union")

  branch_filter = QueryNode.objects.create(
    target_dataset=dataset,
    node_type="filter",
    name="Filtered branch",
  )
  QueryFilterNode.objects.create(
    node=branch_filter,
    input_node=root,
    predicate={"version": 1, "expression": {"kind": "literal", "value": True}},
  )

  union_node_header = QueryNode.objects.create(
    target_dataset=dataset,
    node_type="union",
    name="Combined sources",
  )
  union_node = QueryUnionNode.objects.create(
    node=union_node_header,
    mode="union_all",
  )
  QueryUnionBranch.objects.create(
    union_node=union_node,
    input_node=branch_filter,
    ordinal_position=2,
  )
  QueryUnionBranch.objects.create(
    union_node=union_node,
    input_node=root,
    ordinal_position=1,
  )
  QueryUnionOutputColumn.objects.create(
    union_node=union_node,
    output_name="customer_id",
    ordinal_position=1,
  )

  structure = _query_builder_structure(union_node_header)

  assert structure["type"] == "union"
  assert structure["mode_label"] == "UNION ALL"
  assert structure["summary"] == "2 branches · 1 output columns"
  assert [item["position"] for item in structure["branches"]] == [1, 2]
  assert structure["branches"][0]["structure"]["type"] == "select"
  assert structure["branches"][1]["structure"]["type"] == "filter"

@pytest.mark.django_db(transaction=True)
def test_query_builder_structure_exposes_union_reconciliation_status(monkeypatch):
  dataset, root = _dataset("builder_structure_union_reconciliation")

  union_node_header = QueryNode.objects.create(
    target_dataset=dataset,
    node_type="union",
    name="Combined sources",
  )
  union_node = QueryUnionNode.objects.create(
    node=union_node_header,
    mode="union_all",
  )
  QueryUnionBranch.objects.create(
    union_node=union_node,
    input_node=root,
    ordinal_position=1,
  )

  class Assessment:
    def __init__(self, status):
      self.status = status

  class Report:
    assessments = (Assessment("valid"), Assessment("automap_candidate"), Assessment("missing"))

  monkeypatch.setattr(
    "metadata.services.union_reconciliation.assess_union_branch_reconciliation",
    lambda branch: Report(),
  )

  structure = _query_builder_structure(union_node_header)

  assert structure["reconciliation"] == {
    "valid": 1,
    "safe_matches": 1,
    "attention": 1,
    "status": "review",
  }
  assert structure["branches"][0]["reconciliation"]["safe_matches"] == 1
