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

from dataclasses import dataclass
from typing import Iterable

from django.db import transaction

from metadata.generation.query_contract import infer_query_node_contract
from metadata.services.query_contract_column_sync import QueryContractColumnSyncService
from metadata.models import (
  QueryAggregateNode,
  QueryFilterNode,
  QueryNode,
  QueryUnionBranch,
  QueryUnionBranchMapping,
  QueryUnionNode,
  QueryUnionOutputColumn,
  QueryWindowNode,
  TargetDataset,
)


class QueryGraphMutationError(ValueError):
  """Raised when a requested query-graph mutation is ambiguous or invalid."""


@dataclass(frozen=True)
class QueryInputReference:
  """One persisted edge from a downstream query operator to its input node."""

  kind: str
  object_id: int
  consumer_node_id: int
  input_node_id: int


_UNARY_DETAILS = {
  "filter": ("filter", QueryFilterNode),
  "aggregate": ("aggregate", QueryAggregateNode),
  "window": ("window", QueryWindowNode),
}


def list_query_input_references(node: QueryNode) -> list[QueryInputReference]:
  """Return all persisted query edges that consume ``node`` as an input."""
  refs: list[QueryInputReference] = []

  for detail in QueryFilterNode.objects.filter(input_node=node).select_related("node"):
    refs.append(QueryInputReference(
      kind="filter",
      object_id=int(detail.pk),
      consumer_node_id=int(detail.node_id),
      input_node_id=int(node.pk),
    ))

  for detail in QueryAggregateNode.objects.filter(input_node=node).select_related("node"):
    refs.append(QueryInputReference(
      kind="aggregate",
      object_id=int(detail.pk),
      consumer_node_id=int(detail.node_id),
      input_node_id=int(node.pk),
    ))

  for detail in QueryWindowNode.objects.filter(input_node=node).select_related("node"):
    refs.append(QueryInputReference(
      kind="window",
      object_id=int(detail.pk),
      consumer_node_id=int(detail.node_id),
      input_node_id=int(node.pk),
    ))

  for branch in QueryUnionBranch.objects.filter(input_node=node).select_related("union_node__node"):
    refs.append(QueryInputReference(
      kind="union_branch",
      object_id=int(branch.pk),
      consumer_node_id=int(branch.union_node.node_id),
      input_node_id=int(node.pk),
    ))

  return sorted(refs, key=lambda ref: (ref.consumer_node_id, ref.kind, ref.object_id))



def _dataset_query_input_references(td: TargetDataset, node: QueryNode) -> list[QueryInputReference]:
  """Return downstream edges whose consumer belongs to ``td``.

  Query nodes may be reused by other TargetDatasets. Those external consumers
  must not be mistaken for downstream edges inside the graph being rewritten.
  """
  refs = list_query_input_references(node)
  if not refs:
    return []
  owned_consumer_ids = set(
    QueryNode.objects.filter(
      pk__in=[ref.consumer_node_id for ref in refs],
      target_dataset=td,
    ).values_list("pk", flat=True)
  )
  return [ref for ref in refs if ref.consumer_node_id in owned_consumer_ids]


def _external_query_input_references(td: TargetDataset, node: QueryNode) -> list[QueryInputReference]:
  """Return downstream edges whose consumer belongs to another TargetDataset."""
  internal = {
    (ref.kind, ref.object_id)
    for ref in _dataset_query_input_references(td, node)
  }
  return [
    ref for ref in list_query_input_references(node)
    if (ref.kind, ref.object_id) not in internal
  ]

def _query_head(td: TargetDataset) -> QueryNode | None:
  return getattr(td, "query_head", None) or getattr(td, "query_root", None)


def _unary_detail(node: QueryNode):
  node_type = (getattr(node, "node_type", "") or "").strip().lower()
  spec = _UNARY_DETAILS.get(node_type)
  if spec is None:
    raise QueryGraphMutationError(
      f"Query node type '{node_type or '?'}' cannot be moved as a unary query step."
    )

  related_name, _model = spec
  detail = getattr(node, related_name, None)
  if detail is None:
    raise QueryGraphMutationError(
      f"Query node {node.pk} has no {node_type} details."
    )
  if not getattr(detail, "input_node_id", None):
    raise QueryGraphMutationError(
      f"Query node {node.pk} has no input node."
    )
  return detail


def _set_reference_input(ref: QueryInputReference, input_node: QueryNode) -> None:
  if ref.kind == "filter":
    obj = QueryFilterNode.objects.select_for_update().get(pk=ref.object_id)
  elif ref.kind == "aggregate":
    obj = QueryAggregateNode.objects.select_for_update().get(pk=ref.object_id)
  elif ref.kind == "window":
    obj = QueryWindowNode.objects.select_for_update().get(pk=ref.object_id)
  elif ref.kind == "union_branch":
    obj = QueryUnionBranch.objects.select_for_update().get(pk=ref.object_id)
  else:
    raise QueryGraphMutationError(f"Unsupported query input reference kind: {ref.kind!r}.")

  obj.input_node = input_node
  obj.save(update_fields=["input_node", "updated_at", "updated_by"])


def _set_unary_input(node: QueryNode, input_node: QueryNode) -> None:
  detail = _unary_detail(node)
  detail.input_node = input_node
  detail.save(update_fields=["input_node", "updated_at", "updated_by"])


def _input_nodes(node: QueryNode) -> Iterable[QueryNode]:
  node_type = (getattr(node, "node_type", "") or "").strip().lower()
  if node_type in _UNARY_DETAILS:
    detail = _unary_detail(node)
    if getattr(detail, "input_node", None) is not None:
      yield detail.input_node
    return

  if node_type == "union":
    union = getattr(node, "union", None)
    if union is None:
      return
    for branch in union.branches.all().order_by("ordinal_position", "id"):
      if getattr(branch, "input_node", None) is not None:
        yield branch.input_node


def _reachable_node_ids(head: QueryNode | None) -> set[int]:
  if head is None:
    return set()

  seen: set[int] = set()
  stack = [head]
  while stack:
    node = stack.pop()
    node_id = int(getattr(node, "pk", 0) or 0)
    if not node_id or node_id in seen:
      continue
    seen.add(node_id)
    stack.extend(_input_nodes(node))
  return seen


def _dataset_contract_errors(td: TargetDataset) -> tuple[str, ...]:
  """Return normalized blocking contract errors for the active query head."""
  head = _query_head(td)
  if head is None:
    return ()
  contract = infer_query_node_contract(head)
  return tuple(
    issue[len("ERROR:"):].strip() if issue.startswith("ERROR:") else issue
    for issue in contract.issues
    if (issue or "").startswith("ERROR:")
  )


def _validate_dataset_graph(
  td: TargetDataset,
  *,
  required_node_id: int | None = None,
  tolerated_contract_errors: Iterable[str] = (),
) -> None:
  head = _query_head(td)
  root = getattr(td, "query_root", None)
  if head is None or root is None:
    raise QueryGraphMutationError("Query graph requires both query_root and query_head.")

  reachable = _reachable_node_ids(head)
  if int(root.pk) not in reachable:
    raise QueryGraphMutationError("Query mutation would disconnect the stable query root from query_head.")
  if required_node_id is not None and int(required_node_id) not in reachable:
    raise QueryGraphMutationError("Query mutation would disconnect the moved query node from query_head.")

  tolerated = set(tolerated_contract_errors)
  errors = [
    error for error in _dataset_contract_errors(td)
    if error not in tolerated
  ]
  if errors:
    raise QueryGraphMutationError(
      "Query mutation would create an invalid query contract: " + "; ".join(errors)
    )


def _locked_context(td: TargetDataset, *nodes: QueryNode):
  locked_td = TargetDataset.objects.select_for_update().select_related(
    "query_root",
    "query_head",
  ).get(pk=td.pk)

  locked_nodes = {
    node.pk: QueryNode.objects.select_for_update().get(pk=node.pk)
    for node in nodes
  }
  return locked_td, locked_nodes


def _assert_owned_by(td: TargetDataset, *nodes: QueryNode) -> None:
  for node in nodes:
    if getattr(node, "target_dataset_id", None) != td.pk:
      raise QueryGraphMutationError(
        f"Query node {node.pk} does not belong to target dataset {td.pk}."
      )


def _assert_detached_candidate(td: TargetDataset, node: QueryNode) -> None:
  if getattr(td, "query_root_id", None) == node.pk:
    raise QueryGraphMutationError("The stable SELECT root cannot be inserted or moved.")
  if node.pk in _reachable_node_ids(_query_head(td)):
    raise QueryGraphMutationError(
      f"Query node {node.pk} is already part of the active query graph."
    )
  if list_query_input_references(node):
    raise QueryGraphMutationError(
      f"Query node {node.pk} is already consumed by another query operator."
    )


def _locked_union_branch(td: TargetDataset, branch: QueryUnionBranch) -> QueryUnionBranch:
  locked = (
    QueryUnionBranch.objects
    .select_for_update()
    .select_related("union_node__node", "input_node")
    .get(pk=branch.pk)
  )
  union_query_node = locked.union_node.node
  if union_query_node.target_dataset_id != td.pk:
    raise QueryGraphMutationError(
      f"Union branch {branch.pk} does not belong to target dataset {td.pk}."
    )
  return locked


def _assert_before_target_is_unary(target: QueryNode) -> None:
  node_type = (getattr(target, "node_type", "") or "").strip().lower()
  if node_type == "union":
    raise QueryGraphMutationError(
      "Cannot insert or move generically before UNION: choose the specific UNION branch instead."
    )
  _unary_detail(target)



def _union_detail(node: QueryNode) -> QueryUnionNode:
  node_type = (getattr(node, "node_type", "") or "").strip().lower()
  if node_type != "union":
    raise QueryGraphMutationError(
      f"Query node type '{node_type or '?'}' is not a UNION query step."
    )
  detail = getattr(node, "union", None)
  if detail is None:
    raise QueryGraphMutationError(f"Query node {node.pk} has no union details.")
  return detail


def _assert_detached_union_candidate(td: TargetDataset, node: QueryNode) -> QueryUnionNode:
  if getattr(td, "query_root_id", None) == node.pk:
    raise QueryGraphMutationError("The stable SELECT root cannot be replaced by UNION.")
  if node.pk in _reachable_node_ids(_query_head(td)):
    raise QueryGraphMutationError(
      f"Query node {node.pk} is already part of the active query graph."
    )
  if list_query_input_references(node):
    raise QueryGraphMutationError(
      f"Query node {node.pk} is already consumed by another query operator."
    )
  union = _union_detail(node)
  if union.branches.exists():
    raise QueryGraphMutationError(
      "A UNION candidate must not have branches before it is inserted into the query graph."
    )
  return union


def _create_union_primary_branch(union: QueryUnionNode, input_node: QueryNode) -> QueryUnionBranch:
  """Create the first branch and preserve its input contract as UNION output."""
  branch = QueryUnionBranch(
    union_node=union,
    input_node=input_node,
    ordinal_position=1,
  )
  branch.save()

  contract = infer_query_node_contract(input_node)
  type_map = QueryContractColumnSyncService().infer_query_node_output_types(input_node)
  for position, column_name in enumerate(contract.columns, start=1):
    type_info = type_map.get(column_name.strip().lower(), {})
    output = QueryUnionOutputColumn(
      union_node=union,
      output_name=column_name,
      ordinal_position=position,
      datatype=(type_info.get("datatype") or None),
      max_length=type_info.get("max_length"),
      decimal_precision=type_info.get("decimal_precision"),
      decimal_scale=type_info.get("decimal_scale"),
    )
    output.save()
    mapping = QueryUnionBranchMapping(
      branch=branch,
      output_column=output,
      input_column_name=column_name,
    )
    mapping.save()
  return branch

def insert_unary_node_before(
  td: TargetDataset,
  node: QueryNode,
  target: QueryNode,
) -> None:
  """Insert a detached unary node immediately before ``target``."""
  with transaction.atomic():
    td, locked = _locked_context(td, node, target)
    node = locked[node.pk]
    target = locked[target.pk]
    _assert_owned_by(td, node, target)
    _assert_detached_candidate(td, node)

    _assert_before_target_is_unary(target)
    target_detail = _unary_detail(target)
    previous_input = target_detail.input_node
    if previous_input_id := getattr(previous_input, "pk", None):
      previous_input = QueryNode.objects.select_for_update().get(pk=previous_input_id)

    _set_unary_input(node, previous_input)
    target_detail.input_node = node
    target_detail.save(update_fields=["input_node", "updated_at", "updated_by"])

    td.refresh_from_db(fields=["query_root", "query_head"])
    _validate_dataset_graph(td, required_node_id=int(node.pk))


def insert_unary_node_after(
  td: TargetDataset,
  node: QueryNode,
  target: QueryNode,
) -> None:
  """Insert a detached unary node immediately after ``target`` when the edge is unambiguous."""
  with transaction.atomic():
    td, locked = _locked_context(td, node, target)
    node = locked[node.pk]
    target = locked[target.pk]
    _assert_owned_by(td, node, target)
    _assert_detached_candidate(td, node)

    refs = [
      ref for ref in _dataset_query_input_references(td, target)
      if ref.consumer_node_id != node.pk
    ]
    is_head = getattr(td, "query_head_id", None) == target.pk
    if is_head:
      if refs:
        raise QueryGraphMutationError(
          "query_head has downstream consumers; the query graph is inconsistent."
        )
      _set_unary_input(node, target)
      td.query_head = node
      td.save(update_fields=["query_head", "updated_at", "updated_by"])
    else:
      if len(refs) != 1:
        raise QueryGraphMutationError(
          f"Cannot insert after query node {target.pk}: expected one downstream edge, found {len(refs)}."
        )
      _set_unary_input(node, target)
      _set_reference_input(refs[0], node)

    td.refresh_from_db(fields=["query_root", "query_head"])
    _validate_dataset_graph(td, required_node_id=int(node.pk))



def insert_union_node_before(
  td: TargetDataset,
  node: QueryNode,
  target: QueryNode,
) -> QueryUnionBranch:
  """Insert a detached UNION immediately before one unary target step."""
  with transaction.atomic():
    td, locked = _locked_context(td, node, target)
    node = locked[node.pk]
    target = locked[target.pk]
    _assert_owned_by(td, node, target)
    union = _assert_detached_union_candidate(td, node)

    _assert_before_target_is_unary(target)
    target_detail = _unary_detail(target)
    previous_input = QueryNode.objects.select_for_update().get(pk=target_detail.input_node_id)
    branch = _create_union_primary_branch(union, previous_input)
    target_detail.input_node = node
    target_detail.save(update_fields=["input_node", "updated_at", "updated_by"])

    td.refresh_from_db(fields=["query_root", "query_head"])
    _validate_dataset_graph(td, required_node_id=int(node.pk))
    return branch


def insert_union_node_after(
  td: TargetDataset,
  node: QueryNode,
  target: QueryNode,
) -> QueryUnionBranch:
  """Insert a detached UNION immediately after ``target`` when the edge is unambiguous."""
  with transaction.atomic():
    td, locked = _locked_context(td, node, target)
    node = locked[node.pk]
    target = locked[target.pk]
    _assert_owned_by(td, node, target)
    union = _assert_detached_union_candidate(td, node)

    refs = _dataset_query_input_references(td, target)
    is_head = getattr(td, "query_head_id", None) == target.pk
    if is_head:
      if refs:
        raise QueryGraphMutationError(
          "query_head has downstream consumers; the query graph is inconsistent."
        )
      branch = _create_union_primary_branch(union, target)
      td.query_head = node
      td.save(update_fields=["query_head", "updated_at", "updated_by"])
    else:
      if len(refs) != 1:
        raise QueryGraphMutationError(
          f"Cannot insert UNION after query node {target.pk}: expected one downstream edge, found {len(refs)}."
        )
      branch = _create_union_primary_branch(union, target)
      _set_reference_input(refs[0], node)

    td.refresh_from_db(fields=["query_root", "query_head"])
    _validate_dataset_graph(td, required_node_id=int(node.pk))
    return branch


def insert_union_node_into_union_branch(
  td: TargetDataset,
  node: QueryNode,
  branch: QueryUnionBranch,
) -> QueryUnionBranch:
  """Insert a detached UNION at the end of one explicit UNION branch."""
  with transaction.atomic():
    td, locked = _locked_context(td, node)
    node = locked[node.pk]
    _assert_owned_by(td, node)
    union = _assert_detached_union_candidate(td, node)

    branch = _locked_union_branch(td, branch)
    previous_input = QueryNode.objects.select_for_update().get(pk=branch.input_node_id)
    primary_branch = _create_union_primary_branch(union, previous_input)
    branch.input_node = node
    branch.save(update_fields=["input_node", "updated_at", "updated_by"])

    td.refresh_from_db(fields=["query_root", "query_head"])
    _validate_dataset_graph(td, required_node_id=int(node.pk))
    return primary_branch

def _detach_unary_node(td: TargetDataset, node: QueryNode) -> QueryNode:
  detail = _unary_detail(node)
  previous_input = detail.input_node
  refs = _dataset_query_input_references(td, node)
  is_head = getattr(td, "query_head_id", None) == node.pk

  if is_head:
    if refs:
      raise QueryGraphMutationError(
        "query_head has downstream consumers; the query graph is inconsistent."
      )
    td.query_head = previous_input
    td.save(update_fields=["query_head", "updated_at", "updated_by"])
  else:
    if len(refs) != 1:
      raise QueryGraphMutationError(
        f"Cannot move query node {node.pk}: expected one downstream edge, found {len(refs)}."
      )
    _set_reference_input(refs[0], previous_input)

  return previous_input


def insert_unary_node_into_union_branch(
  td: TargetDataset,
  node: QueryNode,
  branch: QueryUnionBranch,
) -> None:
  """Insert a detached unary step at the end of one explicit UNION branch.

  The inserted step consumes the branch's previous input and becomes the new
  input of that branch. Other UNION branches remain unchanged.
  """
  with transaction.atomic():
    td, locked = _locked_context(td, node)
    node = locked[node.pk]
    _assert_owned_by(td, node)
    _assert_detached_candidate(td, node)

    # A UNION may already be invalid because this branch is missing an output
    # mapping. Adding a branch-local step can be part of repairing that exact
    # condition (for example, adding a WINDOW column before mapping it). Keep
    # those pre-existing contract errors as the repair baseline, while still
    # rejecting every new error introduced by the mutation.
    preexisting_contract_errors = _dataset_contract_errors(td)

    branch = _locked_union_branch(td, branch)
    previous_input = QueryNode.objects.select_for_update().get(pk=branch.input_node_id)
    _set_unary_input(node, previous_input)
    branch.input_node = node
    branch.save(update_fields=["input_node", "updated_at", "updated_by"])

    td.refresh_from_db(fields=["query_root", "query_head"])
    _validate_dataset_graph(
      td,
      required_node_id=int(node.pk),
      tolerated_contract_errors=preexisting_contract_errors,
    )


def move_unary_node_into_union_branch(
  td: TargetDataset,
  node: QueryNode,
  branch: QueryUnionBranch,
) -> None:
  """Move one active unary step to the end of one explicit UNION branch.

  The step is detached from its existing unique downstream edge (or from the
  query head), then inserted between the selected branch input and the UNION.
  """
  with transaction.atomic():
    td, locked = _locked_context(td, node)
    node = locked[node.pk]
    _assert_owned_by(td, node)
    if getattr(td, "query_root_id", None) == node.pk:
      raise QueryGraphMutationError("The stable SELECT root cannot be moved.")
    _unary_detail(node)

    reachable = _reachable_node_ids(_query_head(td))
    if node.pk not in reachable:
      raise QueryGraphMutationError("The query node must belong to the active query graph.")

    branch = _locked_union_branch(td, branch)
    _detach_unary_node(td, node)

    branch = _locked_union_branch(td, branch)
    previous_input = QueryNode.objects.select_for_update().get(pk=branch.input_node_id)
    if previous_input.pk == node.pk:
      raise QueryGraphMutationError(
        f"Query node {node.pk} is already the input of UNION branch {branch.pk}."
      )

    _set_unary_input(node, previous_input)
    branch.input_node = node
    branch.save(update_fields=["input_node", "updated_at", "updated_by"])

    td.refresh_from_db(fields=["query_root", "query_head"])
    _validate_dataset_graph(td, required_node_id=int(node.pk))


def remove_unary_node(td: TargetDataset, node: QueryNode) -> None:
  """Remove one active unary query step and reconnect its surrounding graph."""
  with transaction.atomic():
    td, locked = _locked_context(td, node)
    node = locked[node.pk]
    _assert_owned_by(td, node)
    if getattr(td, "query_root_id", None) == node.pk:
      raise QueryGraphMutationError("The stable SELECT root cannot be removed.")
    _unary_detail(node)

    reachable = _reachable_node_ids(_query_head(td))
    if node.pk not in reachable:
      raise QueryGraphMutationError("The query node must belong to the active query graph.")

    external_refs = _external_query_input_references(td, node)
    if external_refs:
      raise QueryGraphMutationError(
        "This query step is used by another target dataset and cannot be removed."
      )

    _detach_unary_node(td, node)
    td.refresh_from_db(fields=["query_root", "query_head"])
    _validate_dataset_graph(td)
    node.delete()



def move_unary_node_before(
  td: TargetDataset,
  node: QueryNode,
  target: QueryNode,
) -> None:
  """Move one active unary query step immediately before another unary step."""
  if node.pk == target.pk:
    raise QueryGraphMutationError("A query node cannot be moved relative to itself.")

  with transaction.atomic():
    td, locked = _locked_context(td, node, target)
    node = locked[node.pk]
    target = locked[target.pk]
    _assert_owned_by(td, node, target)
    if getattr(td, "query_root_id", None) == node.pk:
      raise QueryGraphMutationError("The stable SELECT root cannot be moved.")
    _unary_detail(node)
    _assert_before_target_is_unary(target)

    reachable = _reachable_node_ids(_query_head(td))
    if node.pk not in reachable or target.pk not in reachable:
      raise QueryGraphMutationError("Both query nodes must belong to the active query graph.")

    _detach_unary_node(td, node)

    target = QueryNode.objects.select_for_update().get(pk=target.pk)
    target_detail = _unary_detail(target)
    previous_input = target_detail.input_node
    _set_unary_input(node, previous_input)
    target_detail.input_node = node
    target_detail.save(update_fields=["input_node", "updated_at", "updated_by"])

    td.refresh_from_db(fields=["query_root", "query_head"])
    _validate_dataset_graph(td, required_node_id=int(node.pk))


def move_unary_node_after(
  td: TargetDataset,
  node: QueryNode,
  target: QueryNode,
) -> None:
  """Move one active unary query step immediately after another node when the edge is unambiguous."""
  if node.pk == target.pk:
    raise QueryGraphMutationError("A query node cannot be moved relative to itself.")

  with transaction.atomic():
    td, locked = _locked_context(td, node, target)
    node = locked[node.pk]
    target = locked[target.pk]
    _assert_owned_by(td, node, target)
    if getattr(td, "query_root_id", None) == node.pk:
      raise QueryGraphMutationError("The stable SELECT root cannot be moved.")
    _unary_detail(node)

    reachable = _reachable_node_ids(_query_head(td))
    if node.pk not in reachable or target.pk not in reachable:
      raise QueryGraphMutationError("Both query nodes must belong to the active query graph.")

    _detach_unary_node(td, node)

    target = QueryNode.objects.select_for_update().get(pk=target.pk)
    refs = [
      ref for ref in _dataset_query_input_references(td, target)
      if ref.consumer_node_id != node.pk
    ]
    is_head = getattr(td, "query_head_id", None) == target.pk

    if is_head:
      if refs:
        raise QueryGraphMutationError(
          "query_head has downstream consumers; the query graph is inconsistent."
        )
      _set_unary_input(node, target)
      td.query_head = node
      td.save(update_fields=["query_head", "updated_at", "updated_by"])
    else:
      if len(refs) != 1:
        raise QueryGraphMutationError(
          f"Cannot move after query node {target.pk}: expected one downstream edge, found {len(refs)}."
        )
      _set_unary_input(node, target)
      _set_reference_input(refs[0], node)

    td.refresh_from_db(fields=["query_root", "query_head"])
    _validate_dataset_graph(td, required_node_id=int(node.pk))
