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
)
from metadata.architecture.execution_impact_presentation import (
  ExecutionImpactPresentationError,
  build_execution_impact_plan_presentation,
)


def _evidence(
  dataset_key: str,
  *,
  status: str = "available",
  required: bool = True,
) -> ExecutionImpactEvidenceReference:
  """
  Build one Architecture State evidence reference for presentation tests.
  """
  return ExecutionImpactEvidenceReference(
    evidence_type="architecture_state",
    evidence_key=f"current:{dataset_key}",
    status=status,
    required=required,
    fingerprint=(f"fingerprint:{dataset_key}" if status == "available" else None),
  )


def _item(
  index: int,
  *,
  decision: str = "REUSE",
  evidence_status: str = "available",
  with_propagation: bool = False,
) -> ExecutionImpactItem:
  """
  Build one deterministic impact item for presentation tests.
  """
  dataset_key = f"raw.dataset_{index:02d}"
  reason_codes = {
    "REUSE": ("NO_RELEVANT_CHANGE",),
    "REVALIDATE": ("REVALIDATION_REQUIRED",),
    "INCREMENTAL_EXECUTE": ("UPSTREAM_EXECUTION_REQUIRED",),
    "FULL_REBUILD": ("DATASET_DEFINITION_CHANGED",),
    "BLOCKED": ("BASELINE_UNAVAILABLE",),
  }[decision]
  propagations = ()
  if with_propagation:
    propagations = (
      ExecutionImpactPropagation(
        upstream_dataset_key="raw.upstream",
        upstream_decision="INCREMENTAL_EXECUTE",
        dependency_reason="lineage_input",
        reason_code="UPSTREAM_EXECUTION_REQUIRED",
      ),
    )

  return ExecutionImpactItem(
    dataset_key=dataset_key,
    decision=decision,
    reason_codes=reason_codes,
    architecture_fingerprint=f"architecture:{dataset_key}",
    evidence=(
      _evidence(
        dataset_key,
        status=evidence_status,
      ),
    ),
    propagations=propagations,
  )


def _plan(*items: ExecutionImpactItem) -> ExecutionImpactPlan:
  """
  Build one Execution Impact Plan for presentation tests.
  """
  return ExecutionImpactPlan(
    scope_key="all",
    architecture_fingerprint="architecture-state",
    baseline_fingerprint="baseline-state",
    report_fingerprint="report-1",
    items=tuple(items),
    evidence=(
      ExecutionImpactEvidenceReference(
        evidence_type="architecture_change_report",
        evidence_key="report:report-1",
        status="available",
        fingerprint="report-1",
      ),
    ),
  )


def test_build_presentation_uses_stable_decision_order_and_styles() -> None:
  """
  Verify stable summary order, labels, counts and UI metadata.
  """
  plan = _plan(
    _item(1, decision="BLOCKED", evidence_status="unavailable"),
    _item(2, decision="FULL_REBUILD"),
    _item(3, decision="INCREMENTAL_EXECUTE"),
    _item(4, decision="REVALIDATE"),
    _item(5, decision="REUSE"),
  )

  presentation = build_execution_impact_plan_presentation(plan)

  assert [item.decision for item in presentation.decision_counts] == [
    "REUSE",
    "REVALIDATE",
    "INCREMENTAL_EXECUTE",
    "FULL_REBUILD",
    "BLOCKED",
  ]
  assert [item.count for item in presentation.decision_counts] == [1, 1, 1, 1, 1]
  assert [item.badge_class for item in presentation.decision_counts] == [
    "text-bg-success",
    "text-bg-info",
    "text-bg-primary",
    "text-bg-warning",
    "text-bg-danger",
  ]


def test_build_presentation_splits_preview_and_remaining_items() -> None:
  """
  Verify that only remaining decisions appear behind explicit expansion.
  """
  plan = _plan(*(_item(index) for index in range(15, 0, -1)))

  presentation = build_execution_impact_plan_presentation(plan)

  assert presentation.assessed_count == 15
  assert len(presentation.preview_items) == 12
  assert presentation.preview_items[0].dataset_key == "raw.dataset_01"
  assert presentation.preview_items[-1].dataset_key == "raw.dataset_12"
  assert [item.dataset_key for item in presentation.remaining_items] == [
    "raw.dataset_13",
    "raw.dataset_14",
    "raw.dataset_15",
  ]
  assert presentation.has_remaining_items is True
  assert presentation.remaining_count == 3


def test_build_presentation_uses_execution_order_before_preview_split() -> None:
  """
  Verify UI rows and bounded expansion follow controlled execution order.
  """
  plan = _plan(*(_item(index) for index in range(1, 16)))
  execution_dataset_keys = tuple(
    f"raw.dataset_{index:02d}"
    for index in range(15, 0, -1)
  )

  presentation = build_execution_impact_plan_presentation(
    plan,
    execution_dataset_keys=execution_dataset_keys,
  )

  assert [
    item.dataset_key
    for item in presentation.preview_items
  ] == list(execution_dataset_keys[:12])
  assert [
    item.dataset_key
    for item in presentation.remaining_items
  ] == list(execution_dataset_keys[12:])
  assert presentation.plan_fingerprint == plan.plan_fingerprint


def test_build_presentation_rejects_duplicate_execution_order_keys() -> None:
  """
  Verify one impact dataset cannot occur twice in presentation order.
  """
  with pytest.raises(
    ExecutionImpactPresentationError,
    match="contains duplicate dataset keys",
  ):
    build_execution_impact_plan_presentation(
      _plan(_item(1), _item(2)),
      execution_dataset_keys=(
        "raw.dataset_01",
        "raw.dataset_01",
      ),
    )


def test_build_presentation_rejects_execution_scope_mismatch() -> None:
  """
  Verify Impact Plan and Execution Preview scopes must match exactly.
  """
  with pytest.raises(
    ExecutionImpactPresentationError,
    match="does not match the assessed dataset scope",
  ):
    build_execution_impact_plan_presentation(
      _plan(_item(1), _item(2)),
      execution_dataset_keys=(
        "raw.dataset_01",
        "raw.dataset_03",
      ),
    )


def test_build_presentation_exposes_compact_evidence_hints() -> None:
  """
  Verify evidence, propagation and blocker hints for one dataset row.
  """
  plan = _plan(
    _item(
      1,
      decision="BLOCKED",
      evidence_status="unavailable",
      with_propagation=True,
    ),
  )

  presentation = build_execution_impact_plan_presentation(plan)
  item = presentation.preview_items[0]

  assert item.decision_label == "Blocked"
  assert item.decision_badge_class == "text-bg-danger"
  assert item.reason_codes == ("BASELINE_UNAVAILABLE",)
  assert item.evidence_count == 1
  assert item.propagation_count == 1
  assert item.has_unavailable_required_evidence is True


def test_build_presentation_exposes_plan_fingerprints() -> None:
  """
  Verify that the UI presentation remains bound to the complete plan contract.
  """
  plan = _plan(_item(1))

  presentation = build_execution_impact_plan_presentation(plan)

  assert presentation.scope_key == "all"
  assert presentation.plan_fingerprint == plan.plan_fingerprint
  assert presentation.report_fingerprint == "report-1"
  assert presentation.architecture_fingerprint == "architecture-state"
  assert presentation.baseline_fingerprint == "baseline-state"


def test_build_presentation_supports_empty_plan() -> None:
  """
  Verify a valid empty assessed scope remains renderable.
  """
  presentation = build_execution_impact_plan_presentation(_plan())

  assert presentation.assessed_count == 0
  assert [item.count for item in presentation.decision_counts] == [0, 0, 0, 0, 0]
  assert presentation.preview_items == ()
  assert presentation.remaining_items == ()
  assert presentation.has_remaining_items is False
  assert presentation.remaining_count == 0


def test_build_presentation_rejects_negative_preview_limit() -> None:
  """
  Verify invalid presentation limits fail explicitly.
  """
  with pytest.raises(
    ExecutionImpactPresentationError,
    match="preview limit must not be negative",
  ):
    build_execution_impact_plan_presentation(
      _plan(_item(1)),
      preview_limit=-1,
    )
