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

import pytest

from metadata.architecture.execution_impact import (
  ExecutionImpactEvidenceReference,
  ExecutionImpactItem,
  ExecutionImpactPlan,
  ExecutionImpactPropagation,
  strongest_execution_impact_decision,
)


def _available_state_evidence(
  dataset_key: str,
  fingerprint: str,
) -> ExecutionImpactEvidenceReference:
  return ExecutionImpactEvidenceReference(
    evidence_type="architecture_state",
    evidence_key=dataset_key,
    status="available",
    fingerprint=fingerprint,
  )


def _item(
  dataset_key: str,
  *,
  decision: str = "REUSE",
  fingerprint: str = "dataset-fingerprint",
) -> ExecutionImpactItem:
  return ExecutionImpactItem(
    dataset_key=dataset_key,
    decision=decision,
    reason_codes=("NO_RELEVANT_CHANGE",),
    architecture_fingerprint=fingerprint,
    evidence=(_available_state_evidence(dataset_key, fingerprint),),
  )


def test_strongest_execution_impact_decision_uses_conservative_priority() -> None:
  assert strongest_execution_impact_decision(
    "REUSE",
    "INCREMENTAL_EXECUTE",
    "REVALIDATE",
  ) == "INCREMENTAL_EXECUTE"

  assert strongest_execution_impact_decision(
    "FULL_REBUILD",
    "BLOCKED",
  ) == "BLOCKED"


def test_execution_impact_item_normalizes_explainability_inputs() -> None:
  item = ExecutionImpactItem(
    dataset_key="serving.customer",
    decision="INCREMENTAL_EXECUTE",
    reason_codes=(
      "UPSTREAM_EXECUTION_REQUIRED",
      "DATASET_DEFINITION_CHANGED",
      "UPSTREAM_EXECUTION_REQUIRED",
    ),
    architecture_fingerprint="dataset-current",
    last_successful_execution_fingerprint="execution-previous",
    evidence=(
      ExecutionImpactEvidenceReference(
        evidence_type="execution_dependency",
        evidence_key="rawcore.customer->serving.customer",
        status="available",
        fingerprint="dependency-fingerprint",
      ),
      _available_state_evidence("serving.customer", "dataset-current"),
    ),
    upstream_dataset_keys=("rawcore.customer", "raw.customer", "rawcore.customer"),
    downstream_dataset_keys=("mart.customer", "mart.customer"),
    propagations=(
      ExecutionImpactPropagation(
        upstream_dataset_key="rawcore.customer",
        upstream_decision="INCREMENTAL_EXECUTE",
        dependency_reason="lineage_input",
        reason_code="UPSTREAM_EXECUTION_REQUIRED",
      ),
    ),
  )

  assert item.reason_codes == (
    "DATASET_DEFINITION_CHANGED",
    "UPSTREAM_EXECUTION_REQUIRED",
  )
  assert item.upstream_dataset_keys == ("raw.customer", "rawcore.customer")
  assert item.downstream_dataset_keys == ("mart.customer",)
  assert item.decision_label == "Incremental execute"
  assert item.recommended_action == "execute_incremental"


def test_unavailable_required_evidence_requires_blocked_decision() -> None:
  unavailable = ExecutionImpactEvidenceReference(
    evidence_type="architecture_execution_record",
    evidence_key="serving.customer:last_successful",
    status="unavailable",
    required=True,
    message="No successful execution evidence is available.",
  )

  with pytest.raises(ValueError, match="requires a BLOCKED"):
    ExecutionImpactItem(
      dataset_key="serving.customer",
      decision="REUSE",
      reason_codes=("EXECUTION_EVIDENCE_UNAVAILABLE",),
      architecture_fingerprint="dataset-current",
      evidence=(unavailable,),
    )

  item = ExecutionImpactItem(
    dataset_key="serving.customer",
    decision="BLOCKED",
    reason_codes=("EXECUTION_EVIDENCE_UNAVAILABLE",),
    architecture_fingerprint="dataset-current",
    evidence=(unavailable,),
  )

  assert item.has_unavailable_required_evidence is True
  assert item.recommended_action == "resolve_blocker"


def test_not_applicable_optional_evidence_does_not_block_reuse() -> None:
  source_evidence = ExecutionImpactEvidenceReference(
    evidence_type="source_evidence",
    evidence_key="serving.customer",
    status="not_applicable",
    required=False,
    message="Source evidence is outside the first dataset-level impact scope.",
  )

  item = ExecutionImpactItem(
    dataset_key="serving.customer",
    decision="REUSE",
    reason_codes=("NO_RELEVANT_CHANGE",),
    architecture_fingerprint="dataset-current",
    evidence=(
      _available_state_evidence("serving.customer", "dataset-current"),
      source_evidence,
    ),
  )

  assert item.has_unavailable_required_evidence is False


def test_execution_impact_contract_rejects_unknown_evidence_and_reason_codes() -> None:
  with pytest.raises(ValueError, match="Unsupported execution impact evidence type"):
    ExecutionImpactEvidenceReference(
      evidence_type="unknown",
      evidence_key="serving.customer",
      status="available",
      fingerprint="evidence-fingerprint",
    )

  with pytest.raises(ValueError, match="Unsupported execution impact reason code"):
    ExecutionImpactItem(
      dataset_key="serving.customer",
      decision="REUSE",
      reason_codes=("UNKNOWN_REASON",),
      architecture_fingerprint="dataset-current",
      evidence=(
        _available_state_evidence("serving.customer", "dataset-current"),
      ),
    )


def test_execution_impact_plan_is_order_independent_and_counts_all_decisions() -> None:
  first = ExecutionImpactPlan(
    scope_key="all",
    architecture_fingerprint="architecture-current",
    baseline_fingerprint="architecture-previous",
    report_fingerprint="report-current",
    items=(
      _item("serving.customer"),
      _item(
        "rawcore.customer",
        decision="FULL_REBUILD",
        fingerprint="rawcore-current",
      ),
    ),
  )
  second = ExecutionImpactPlan(
    scope_key="all",
    architecture_fingerprint="architecture-current",
    baseline_fingerprint="architecture-previous",
    report_fingerprint="report-current",
    items=tuple(reversed(first.items)),
  )

  assert tuple(item.dataset_key for item in first.items) == (
    "rawcore.customer",
    "serving.customer",
  )
  assert first.decision_counts == {
    "REUSE": 1,
    "REVALIDATE": 0,
    "INCREMENTAL_EXECUTE": 0,
    "FULL_REBUILD": 1,
    "BLOCKED": 0,
  }
  assert first.plan_fingerprint == second.plan_fingerprint


def test_execution_impact_plan_fingerprint_changes_with_decision() -> None:
  reuse_plan = ExecutionImpactPlan(
    scope_key="all",
    architecture_fingerprint="architecture-current",
    baseline_fingerprint="architecture-previous",
    report_fingerprint="report-current",
    items=(_item("serving.customer", decision="REUSE"),),
  )
  rebuild_plan = ExecutionImpactPlan(
    scope_key="all",
    architecture_fingerprint="architecture-current",
    baseline_fingerprint="architecture-previous",
    report_fingerprint="report-current",
    items=(_item("serving.customer", decision="FULL_REBUILD"),),
  )

  assert reuse_plan.plan_fingerprint != rebuild_plan.plan_fingerprint
