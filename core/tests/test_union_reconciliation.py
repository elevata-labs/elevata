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

from types import SimpleNamespace

import pytest

from metadata.models import (
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
from metadata.generation.query_contract import (
  infer_query_node_contract,
  union_branch_selectable_columns,
)
from metadata.services.union_reconciliation import (
  apply_safe_union_branch_automap,
  assess_union_branch_reconciliation,
)


def _dataset(name: str) -> tuple[TargetDataset, QueryNode]:
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


def _union_branch(td: TargetDataset, input_node: QueryNode) -> tuple[QueryUnionNode, QueryUnionBranch]:
  node = QueryNode.objects.create(
    target_dataset=td,
    node_type="union",
    name="Union",
  )
  union = QueryUnionNode.objects.create(node=node, mode="union_all")
  branch = QueryUnionBranch.objects.create(
    union_node=union,
    input_node=input_node,
    ordinal_position=1,
  )
  return union, branch


def _patch_branch_contract(monkeypatch, columns: list[str], types: dict[str, str] | None = None) -> None:
  from metadata.services import union_reconciliation

  monkeypatch.setattr(
    union_reconciliation,
    "infer_query_node_contract",
    lambda _node: SimpleNamespace(columns=columns, issues=[]),
  )
  monkeypatch.setattr(
    union_reconciliation,
    "_input_type_map",
    lambda _node: {
      name.lower(): {"datatype": datatype}
      for name, datatype in (types or {}).items()
    },
  )


@pytest.mark.django_db(transaction=True)
def test_safe_automap_creates_only_missing_unique_same_name_mapping(monkeypatch):
  td, root = _dataset("union_reconcile_create")
  union, branch = _union_branch(td, root)
  out = QueryUnionOutputColumn.objects.create(
    union_node=union,
    output_name="customer_id",
    ordinal_position=1,
    datatype="BIGINT",
  )
  _patch_branch_contract(monkeypatch, ["customer_id"], {"customer_id": "BIGINT"})

  report = apply_safe_union_branch_automap(branch)

  mapping = QueryUnionBranchMapping.objects.get(branch=branch, output_column=out)
  assert mapping.input_column_name == "customer_id"
  assert report.output_schema == ("customer_id",)
  assert report.assessments[0].status == "valid"


@pytest.mark.django_db(transaction=True)
def test_safe_automap_preserves_existing_explicit_mapping(monkeypatch):
  td, root = _dataset("union_reconcile_preserve")
  union, branch = _union_branch(td, root)
  out = QueryUnionOutputColumn.objects.create(
    union_node=union,
    output_name="customer_id",
    ordinal_position=1,
    datatype="BIGINT",
  )
  mapping = QueryUnionBranchMapping.objects.create(
    branch=branch,
    output_column=out,
    input_column_name="legacy_customer_key",
  )
  _patch_branch_contract(
    monkeypatch,
    ["customer_id", "legacy_customer_key"],
    {"customer_id": "BIGINT", "legacy_customer_key": "BIGINT"},
  )

  report = apply_safe_union_branch_automap(branch)

  mapping.refresh_from_db()
  assert mapping.input_column_name == "legacy_customer_key"
  assert report.assessments[0].status == "valid"


@pytest.mark.django_db(transaction=True)
def test_stale_explicit_mapping_is_reported_but_never_overwritten(monkeypatch):
  td, root = _dataset("union_reconcile_stale")
  union, branch = _union_branch(td, root)
  out = QueryUnionOutputColumn.objects.create(
    union_node=union,
    output_name="customer_id",
    ordinal_position=1,
    datatype="BIGINT",
  )
  mapping = QueryUnionBranchMapping.objects.create(
    branch=branch,
    output_column=out,
    input_column_name="removed_customer_key",
  )
  _patch_branch_contract(monkeypatch, ["customer_id"], {"customer_id": "BIGINT"})

  before_schema = tuple(union.output_columns.values_list("output_name", flat=True))
  report = apply_safe_union_branch_automap(branch)
  after_schema = tuple(union.output_columns.values_list("output_name", flat=True))

  mapping.refresh_from_db()
  assert mapping.input_column_name == "removed_customer_key"
  assert before_schema == after_schema == ("customer_id",)
  assert report.assessments[0].status == "stale_mapping"
  assert report.assessments[0].suggested_input_name == "customer_id"
  assert report.requires_attention is True


@pytest.mark.django_db(transaction=True)
def test_type_mismatch_is_not_auto_mapped(monkeypatch):
  td, root = _dataset("union_reconcile_type")
  union, branch = _union_branch(td, root)
  out = QueryUnionOutputColumn.objects.create(
    union_node=union,
    output_name="amount",
    ordinal_position=1,
    datatype="DECIMAL",
    decimal_precision=18,
    decimal_scale=2,
  )
  _patch_branch_contract(monkeypatch, ["amount"], {"amount": "STRING"})

  report = apply_safe_union_branch_automap(branch)

  assert not QueryUnionBranchMapping.objects.filter(branch=branch, output_column=out).exists()
  item = report.assessments[0]
  assert item.status == "type_mismatch"
  assert item.expected_datatype == "DECIMAL"
  assert item.actual_datatype == "STRING"


@pytest.mark.django_db(transaction=True)
def test_ambiguous_same_name_match_is_not_auto_mapped(monkeypatch):
  td, root = _dataset("union_reconcile_ambiguous")
  union, branch = _union_branch(td, root)
  out = QueryUnionOutputColumn.objects.create(
    union_node=union,
    output_name="customer_id",
    ordinal_position=1,
  )
  _patch_branch_contract(monkeypatch, ["customer_id", "CUSTOMER_ID"])

  report = apply_safe_union_branch_automap(branch)

  assert not QueryUnionBranchMapping.objects.filter(branch=branch, output_column=out).exists()
  assert report.assessments[0].status == "ambiguous"


@pytest.mark.django_db(transaction=True)
def test_reconciliation_can_preview_a_proposed_branch_input(monkeypatch):
  td, root = _dataset("union_reconcile_preview")
  union, branch = _union_branch(td, root)
  QueryUnionOutputColumn.objects.create(
    union_node=union,
    output_name="customer_id",
    ordinal_position=1,
  )
  proposed = QueryNode.objects.create(
    target_dataset=td,
    node_type="select",
    name="Proposed input",
  )
  QuerySelectNode.objects.create(node=proposed, use_dataset_definition=True)
  _patch_branch_contract(monkeypatch, ["customer_id"])

  report = assess_union_branch_reconciliation(branch, input_node=proposed)

  assert report.assessments[0].status == "automap_candidate"


@pytest.mark.django_db(transaction=True)
def test_union_contract_rejects_known_mapping_datatype_mismatch():
  td, root = _dataset("union_contract_type_mismatch")
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="amount",
    ordinal_position=1,
    datatype="STRING",
  )
  union, branch = _union_branch(td, root)
  out = QueryUnionOutputColumn.objects.create(
    union_node=union,
    output_name="amount",
    ordinal_position=1,
    datatype="DECIMAL",
    decimal_precision=18,
    decimal_scale=2,
  )
  QueryUnionBranchMapping.objects.create(
    branch=branch,
    output_column=out,
    input_column_name="amount",
  )

  contract = infer_query_node_contract(union.node)

  assert any(
    issue.startswith("ERROR:") and "datatype STRING" in issue and "requires DECIMAL" in issue
    for issue in contract.issues
  )


@pytest.mark.django_db(transaction=True)
def test_reconciliation_uses_persisted_target_type_when_union_output_type_is_empty(monkeypatch):
  td, root = _dataset("union_reconcile_target_type_fallback")
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="customer_id",
    ordinal_position=1,
    datatype="STRING",
  )
  union, branch = _union_branch(td, root)
  out = QueryUnionOutputColumn.objects.create(
    union_node=union,
    output_name="customer_id",
    ordinal_position=1,
  )
  QueryUnionBranchMapping.objects.create(
    branch=branch,
    output_column=out,
    input_column_name="customer_id",
  )
  _patch_branch_contract(monkeypatch, ["customer_id"], {"customer_id": "STRING"})

  report = assess_union_branch_reconciliation(branch)

  item = report.assessments[0]
  assert item.status == "valid"
  assert item.actual_datatype == "STRING"
  assert item.expected_datatype == "STRING"


def test_union_branch_selectable_columns_keep_extended_inputs_for_own_dataset():
  contract = SimpleNamespace(
    columns=["visible_col"],
    available_input_columns=["visible_col", "raw_only_col"],
  )
  branch = SimpleNamespace(
    input_node=SimpleNamespace(target_dataset_id=11),
    union_node=SimpleNamespace(node=SimpleNamespace(target_dataset_id=11)),
  )

  assert union_branch_selectable_columns(branch, contract) == [
    "visible_col",
    "raw_only_col",
  ]


def test_union_branch_selectable_columns_use_only_output_for_foreign_dataset():
  contract = SimpleNamespace(
    columns=["visible_col"],
    available_input_columns=["visible_col", "raw_only_col"],
  )
  branch = SimpleNamespace(
    input_node=SimpleNamespace(target_dataset_id=22),
    union_node=SimpleNamespace(node=SimpleNamespace(target_dataset_id=11)),
  )

  assert union_branch_selectable_columns(branch, contract) == ["visible_col"]
