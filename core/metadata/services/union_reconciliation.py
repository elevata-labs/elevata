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

from django.db import transaction

from metadata.generation.query_contract import (
  infer_query_node_contract,
  union_branch_selectable_columns,
)
from metadata.models import QueryUnionBranch, QueryUnionBranchMapping, QueryUnionOutputColumn
from metadata.services.query_contract_column_sync import QueryContractColumnSyncService


def _normalize_name(value: str) -> str:
  return (value or "").strip().lower()


def _normalize_type(value: str) -> str:
  return (value or "").strip().upper()


@dataclass(frozen=True)
class UnionMappingAssessment:
  """Reconciliation state for one UNION output column in one branch."""

  output_column_id: int
  output_name: str
  status: str
  existing_input_name: str = ""
  suggested_input_name: str = ""
  expected_datatype: str = ""
  actual_datatype: str = ""
  message: str = ""


@dataclass(frozen=True)
class UnionBranchReconciliation:
  """Read-only reconciliation report for one UNION branch."""

  branch_id: int
  output_schema: tuple[str, ...]
  assessments: tuple[UnionMappingAssessment, ...]

  @property
  def requires_attention(self) -> bool:
    return any(item.status not in {"valid", "automap_candidate"} for item in self.assessments)

  @property
  def automap_candidates(self) -> tuple[UnionMappingAssessment, ...]:
    return tuple(item for item in self.assessments if item.status == "automap_candidate")


def _input_type_map(input_node) -> dict[str, dict]:
  return QueryContractColumnSyncService().infer_query_node_output_types(input_node)


def _type_pair(
  union_node,
  output_column: QueryUnionOutputColumn,
  input_name: str,
  type_map: dict[str, dict],
) -> tuple[str, str]:
  type_service = QueryContractColumnSyncService()
  expected_meta = type_service.resolve_union_output_type(union_node, output_column)
  expected = _normalize_type(expected_meta.get("datatype", ""))
  inferred = type_map.get(_normalize_name(input_name)) or {}
  actual = _normalize_type(inferred.get("datatype", ""))
  return expected, actual


def _types_compatible(expected: str, actual: str) -> bool:
  """Treat unknown type metadata as unresolved, not as an incompatibility."""
  return not expected or not actual or expected == actual


def assess_union_branch_reconciliation(
  branch: QueryUnionBranch,
  *,
  input_node=None,
) -> UnionBranchReconciliation:
  """Assess branch mappings without changing architecture metadata.

  ``input_node`` may be a proposed branch input, allowing the UI to preview
  reconciliation before a graph mutation is committed.
  """
  union_node = branch.union_node
  candidate_input = input_node or branch.input_node
  contract = infer_query_node_contract(candidate_input)
  errors = [issue for issue in contract.issues if str(issue or "").startswith("ERROR:")]
  if errors:
    raise ValueError("Cannot reconcile an invalid branch input contract: " + "; ".join(errors))

  input_columns = union_branch_selectable_columns(branch, contract)
  input_by_norm: dict[str, list[str]] = {}
  for name in input_columns:
    input_by_norm.setdefault(_normalize_name(name), []).append(name)

  type_map = _input_type_map(candidate_input)
  outputs = list(union_node.output_columns.all().order_by("ordinal_position", "id"))
  mappings = {
    mapping.output_column_id: mapping
    for mapping in branch.mappings.select_related("output_column").all()
  }

  assessments: list[UnionMappingAssessment] = []
  for output in outputs:
    output_name = (getattr(output, "output_name", "") or "").strip()
    mapping = mappings.get(output.id)

    if mapping is not None:
      existing = (getattr(mapping, "input_column_name", "") or "").strip()
      matches = input_by_norm.get(_normalize_name(existing), []) if existing else []
      if len(matches) == 1:
        expected, actual = _type_pair(union_node, output, matches[0], type_map)
        if _types_compatible(expected, actual):
          assessments.append(UnionMappingAssessment(
            output_column_id=int(output.id),
            output_name=output_name,
            status="valid",
            existing_input_name=existing,
            expected_datatype=expected,
            actual_datatype=actual,
          ))
        else:
          assessments.append(UnionMappingAssessment(
            output_column_id=int(output.id),
            output_name=output_name,
            status="type_mismatch",
            existing_input_name=existing,
            expected_datatype=expected,
            actual_datatype=actual,
            message=f"Existing mapping has datatype {actual}; UNION output requires {expected}.",
          ))
        continue

      same_name = input_by_norm.get(_normalize_name(output_name), [])
      suggestion = same_name[0] if len(same_name) == 1 else ""
      expected, _actual = _type_pair(union_node, output, "", type_map)
      assessments.append(UnionMappingAssessment(
        output_column_id=int(output.id),
        output_name=output_name,
        status="stale_mapping",
        existing_input_name=existing,
        suggested_input_name=suggestion,
        expected_datatype=expected,
        message="Stored mapping no longer resolves against the branch output.",
      ))
      continue

    same_name = input_by_norm.get(_normalize_name(output_name), [])
    if not same_name:
      assessments.append(UnionMappingAssessment(
        output_column_id=int(output.id),
        output_name=output_name,
        status="missing",
        message="No same-named branch column is available for automatic mapping.",
      ))
      continue
    if len(same_name) > 1:
      assessments.append(UnionMappingAssessment(
        output_column_id=int(output.id),
        output_name=output_name,
        status="ambiguous",
        message="Multiple same-named branch columns are available; choose the mapping explicitly.",
      ))
      continue

    match = same_name[0]
    expected, actual = _type_pair(union_node, output, match, type_map)
    if not _types_compatible(expected, actual):
      assessments.append(UnionMappingAssessment(
        output_column_id=int(output.id),
        output_name=output_name,
        status="type_mismatch",
        suggested_input_name=match,
        expected_datatype=expected,
        actual_datatype=actual,
        message=f"Same-named branch column has datatype {actual}; UNION output requires {expected}.",
      ))
      continue

    assessments.append(UnionMappingAssessment(
      output_column_id=int(output.id),
      output_name=output_name,
      status="automap_candidate",
      suggested_input_name=match,
      expected_datatype=expected,
      actual_datatype=actual,
    ))

  return UnionBranchReconciliation(
    branch_id=int(branch.id),
    output_schema=tuple((getattr(col, "output_name", "") or "").strip() for col in outputs),
    assessments=tuple(assessments),
  )


def apply_safe_union_branch_automap(
  branch: QueryUnionBranch,
  *,
  actor=None,
) -> UnionBranchReconciliation:
  """Create only missing deterministic mappings and preserve explicit mappings."""
  report = assess_union_branch_reconciliation(branch)
  candidates = report.automap_candidates
  if not candidates:
    return report

  outputs = {
    output.id: output
    for output in branch.union_node.output_columns.all()
  }
  with transaction.atomic():
    for item in candidates:
      defaults = {"input_column_name": item.suggested_input_name}
      if actor is not None:
        defaults.update({"created_by": actor, "updated_by": actor})
      QueryUnionBranchMapping.objects.get_or_create(
        branch=branch,
        output_column=outputs[item.output_column_id],
        defaults=defaults,
      )

  return assess_union_branch_reconciliation(branch)
