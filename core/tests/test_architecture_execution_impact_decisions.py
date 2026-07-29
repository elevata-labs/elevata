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

from metadata.architecture.execution_impact import (
  ExecutionImpactEvidenceReference,
)
from metadata.architecture.execution_impact_decisions import (
  build_local_execution_impact_item,
  build_local_execution_impact_plan,
)
from metadata.architecture.execution_impact_evidence import (
  ExecutionImpactDatasetEvidence,
  ExecutionImpactEvidenceResolution,
)


def _evidence_reference(
  evidence_type: str,
  evidence_key: str,
  *,
  status: str = "available",
  required: bool = True,
  fingerprint: str | None = None,
) -> ExecutionImpactEvidenceReference:
  return ExecutionImpactEvidenceReference(
    evidence_type=evidence_type,
    evidence_key=evidence_key,
    status=status,
    required=required,
    fingerprint=fingerprint if status == "available" else None,
    message=f"{evidence_type} evidence",
  )


def _dataset_evidence(
  dataset_key: str = "rawcore.customer",
  *,
  architecture_fingerprint: str = "current-fingerprint",
  baseline_fingerprint: str | None = "current-fingerprint",
  review_status: str = "no_changes",
  materialization_type: str = "table",
  incremental_strategy: str = "full",
  approval_status: str = "not_applicable",
  approval_required: bool = False,
  dataset_change_types: tuple[str, ...] = (),
  dataset_changed_fields: tuple[str, ...] = (),
  column_change_types: tuple[str, ...] = (),
  migration_action_types: tuple[str, ...] = (),
  policy_statuses: tuple[str, ...] = (),
  policy_codes: tuple[str, ...] = (),
) -> ExecutionImpactDatasetEvidence:
  baseline_status = (
    "available" if baseline_fingerprint is not None else "unavailable"
  )
  evidence = (
    _evidence_reference(
      "architecture_state",
      f"current:{dataset_key}",
      fingerprint=architecture_fingerprint,
    ),
    _evidence_reference(
      "architecture_state",
      f"baseline:{dataset_key}",
      status=baseline_status,
      required=False,
      fingerprint=baseline_fingerprint,
    ),
    _evidence_reference(
      "architecture_change_report",
      f"dataset:{dataset_key}",
      fingerprint="dataset-report-fingerprint",
    ),
    _evidence_reference(
      "architecture_review_status",
      f"dataset:{dataset_key}",
      fingerprint=f"review-{review_status}",
    ),
    _evidence_reference(
      "architecture_approval",
      f"dataset:{dataset_key}",
      status=approval_status,
      required=approval_required,
      fingerprint=(
        "approval-fingerprint" if approval_status == "available" else None
      ),
    ),
    _evidence_reference(
      "policy_decision",
      f"dataset:{dataset_key}",
      fingerprint="policy-fingerprint",
    ),
    _evidence_reference(
      "architecture_execution_record",
      f"dataset:{dataset_key}",
      status="unavailable",
      required=False,
    ),
  )

  return ExecutionImpactDatasetEvidence(
    dataset_key=dataset_key,
    architecture_fingerprint=architecture_fingerprint,
    baseline_fingerprint=baseline_fingerprint,
    report_evidence_fingerprint="dataset-report-fingerprint",
    review_status=review_status,
    materialization_type=materialization_type,
    incremental_strategy=incremental_strategy,
    dataset_change_types=dataset_change_types,
    dataset_changed_fields=dataset_changed_fields,
    column_change_types=column_change_types,
    migration_action_types=migration_action_types,
    policy_statuses=policy_statuses,
    policy_codes=policy_codes,
    evidence=evidence,
  )


def _resolution(
  *datasets: ExecutionImpactDatasetEvidence,
  baseline_source: str = "recorded_state",
  baseline_can_execute: bool = True,
) -> ExecutionImpactEvidenceResolution:
  return ExecutionImpactEvidenceResolution(
    scope_key="all",
    architecture_fingerprint="architecture-current",
    baseline_fingerprint=(
      "architecture-previous" if baseline_can_execute else None
    ),
    baseline_source=baseline_source,
    baseline_can_execute=baseline_can_execute,
    report_fingerprint="report-fingerprint",
    report_has_changes=any(item.has_architecture_changes for item in datasets),
    report_is_blocked=any(
      item.has_blocking_policy_decision for item in datasets
    ),
    review_status="approved" if any(
      item.has_architecture_changes for item in datasets
    ) else "no_changes",
    datasets=tuple(datasets),
  )


def test_unchanged_full_refresh_dataset_requires_full_rebuild() -> None:
  item = build_local_execution_impact_item(
    dataset_evidence=_dataset_evidence(),
    baseline_source="recorded_state",
    baseline_can_execute=True,
  )

  assert item.decision == "FULL_REBUILD"
  assert item.reason_codes == ("FULL_REFRESH_STRATEGY",)
  baseline = next(
    evidence
    for evidence in item.evidence
    if evidence.evidence_key == "baseline:rawcore.customer"
  )
  assert baseline.required is True


def test_unchanged_incremental_dataset_requires_incremental_execution() -> None:
  item = build_local_execution_impact_item(
    dataset_evidence=_dataset_evidence(
      incremental_strategy="merge",
    ),
    baseline_source="recorded_state",
    baseline_can_execute=True,
  )

  assert item.decision == "INCREMENTAL_EXECUTE"
  assert item.reason_codes == ("INCREMENTAL_LOAD_STRATEGY",)


def test_unchanged_historized_dataset_requires_incremental_execution() -> None:
  item = build_local_execution_impact_item(
    dataset_evidence=_dataset_evidence(
      dataset_key="rawcore.customer_hist",
      incremental_strategy="historize",
    ),
    baseline_source="recorded_state",
    baseline_can_execute=True,
  )

  assert item.decision == "INCREMENTAL_EXECUTE"
  assert item.reason_codes == ("INCREMENTAL_LOAD_STRATEGY",)


def test_unchanged_view_is_reused() -> None:
  item = build_local_execution_impact_item(
    dataset_evidence=_dataset_evidence(
      materialization_type="view",
    ),
    baseline_source="recorded_state",
    baseline_can_execute=True,
  )

  assert item.decision == "REUSE"
  assert item.reason_codes == (
    "NO_RELEVANT_CHANGE",
    "VIRTUAL_MATERIALIZATION_REUSED",
  )


def test_unknown_execution_semantics_block_instead_of_guessing() -> None:
  item = build_local_execution_impact_item(
    dataset_evidence=_dataset_evidence(
      materialization_type="unknown",
      incremental_strategy="unknown",
    ),
    baseline_source="recorded_state",
    baseline_can_execute=True,
  )

  assert item.decision == "BLOCKED"
  assert item.reason_codes == ("MANUAL_REVIEW_REQUIRED",)


def test_missing_baseline_blocks_local_decision() -> None:
  item = build_local_execution_impact_item(
    dataset_evidence=_dataset_evidence(baseline_fingerprint=None),
    baseline_source="missing_or_unsupported",
    baseline_can_execute=False,
  )

  assert item.decision == "BLOCKED"
  assert item.reason_codes == ("BASELINE_UNAVAILABLE",)
  assert item.has_unavailable_required_evidence is True


def test_inconsistent_no_change_report_blocks_manual_review() -> None:
  item = build_local_execution_impact_item(
    dataset_evidence=_dataset_evidence(
      architecture_fingerprint="current-fingerprint",
      baseline_fingerprint="different-fingerprint",
    ),
    baseline_source="recorded_state",
    baseline_can_execute=True,
  )

  assert item.decision == "BLOCKED"
  assert item.reason_codes == ("MANUAL_REVIEW_REQUIRED",)


def test_new_approved_dataset_requires_full_rebuild() -> None:
  item = build_local_execution_impact_item(
    dataset_evidence=_dataset_evidence(
      baseline_fingerprint=None,
      review_status="approved",
      approval_status="available",
      approval_required=True,
      dataset_change_types=("DATASET_ADDED",),
      column_change_types=("COLUMN_ADDED",),
      migration_action_types=("CREATE_DATASET", "ADD_COLUMN"),
      policy_statuses=("ALLOW",),
      policy_codes=("CREATE_DATASET_ALLOWED", "ADD_COLUMN_ALLOWED"),
    ),
    baseline_source="recorded_state",
    baseline_can_execute=True,
  )

  assert item.decision == "FULL_REBUILD"
  assert item.reason_codes == ("DATASET_ADDED",)


def test_materialization_and_incremental_strategy_changes_are_explicit() -> None:
  item = build_local_execution_impact_item(
    dataset_evidence=_dataset_evidence(
      architecture_fingerprint="changed-fingerprint",
      review_status="approved",
      approval_status="available",
      approval_required=True,
      dataset_change_types=("DATASET_CHANGED",),
      dataset_changed_fields=(
        "materialization_type",
        "incremental_strategy",
      ),
      migration_action_types=("REBUILD_DATASET",),
      policy_statuses=("REQUIRES_PREFLIGHT",),
      policy_codes=("REBUILD_DATASET_PREFLIGHT_REQUIRED",),
    ),
    baseline_source="recorded_state",
    baseline_can_execute=True,
  )

  assert item.decision == "FULL_REBUILD"
  assert item.reason_codes == (
    "DATASET_DEFINITION_CHANGED",
    "INCREMENTAL_STRATEGY_CHANGED",
    "MATERIALIZATION_CHANGED",
  )


def test_metadata_only_column_change_requires_revalidation() -> None:
  item = build_local_execution_impact_item(
    dataset_evidence=_dataset_evidence(
      architecture_fingerprint="changed-fingerprint",
      review_status="approved",
      approval_status="available",
      approval_required=True,
      column_change_types=("COLUMN_CHANGED",),
      migration_action_types=("RETIRE_COLUMN",),
      policy_statuses=("METADATA_ONLY",),
      policy_codes=("RETIRE_COLUMN_METADATA_ONLY",),
    ),
    baseline_source="recorded_state",
    baseline_can_execute=True,
  )

  assert item.decision == "REVALIDATE"
  assert item.reason_codes == (
    "COLUMN_CONTRACT_CHANGED",
    "REVALIDATION_REQUIRED",
  )


def test_pending_approval_blocks_changed_dataset() -> None:
  item = build_local_execution_impact_item(
    dataset_evidence=_dataset_evidence(
      architecture_fingerprint="changed-fingerprint",
      review_status="pending",
      approval_status="unavailable",
      approval_required=True,
      dataset_change_types=("DATASET_CHANGED",),
      migration_action_types=("REBUILD_DATASET",),
      policy_statuses=("REQUIRES_PREFLIGHT",),
      policy_codes=("REBUILD_DATASET_PREFLIGHT_REQUIRED",),
    ),
    baseline_source="recorded_state",
    baseline_can_execute=True,
  )

  assert item.decision == "BLOCKED"
  assert item.reason_codes == ("APPROVAL_REQUIRED",)


def test_local_policy_block_has_highest_priority() -> None:
  item = build_local_execution_impact_item(
    dataset_evidence=_dataset_evidence(
      architecture_fingerprint="changed-fingerprint",
      review_status="blocked",
      approval_status="unavailable",
      approval_required=True,
      column_change_types=("COLUMN_REMOVED",),
      migration_action_types=("DROP_COLUMN",),
      policy_statuses=("BLOCKED_BY_POLICY",),
      policy_codes=("COLUMN_DROP_DISABLED",),
    ),
    baseline_source="recorded_state",
    baseline_can_execute=True,
  )

  assert item.decision == "BLOCKED"
  assert item.reason_codes == ("POLICY_BLOCKED",)


def test_invalid_review_blocks_manual_review_without_approval_claim() -> None:
  item = build_local_execution_impact_item(
    dataset_evidence=_dataset_evidence(
      architecture_fingerprint="changed-fingerprint",
      review_status="invalid",
      approval_status="unavailable",
      approval_required=True,
      dataset_change_types=("DATASET_CHANGED",),
      migration_action_types=("REBUILD_DATASET",),
      policy_statuses=("REQUIRES_PREFLIGHT",),
      policy_codes=("REBUILD_DATASET_PREFLIGHT_REQUIRED",),
    ),
    baseline_source="recorded_state",
    baseline_can_execute=True,
  )

  assert item.decision == "BLOCKED"
  assert item.reason_codes == ("MANUAL_REVIEW_REQUIRED",)


def test_incomplete_change_evidence_blocks_instead_of_guessing() -> None:
  item = build_local_execution_impact_item(
    dataset_evidence=_dataset_evidence(
      architecture_fingerprint="changed-fingerprint",
      review_status="approved",
      approval_status="available",
      approval_required=True,
      column_change_types=("COLUMN_ADDED",),
      migration_action_types=(),
      policy_statuses=(),
    ),
    baseline_source="recorded_state",
    baseline_can_execute=True,
  )

  assert item.decision == "BLOCKED"
  assert item.reason_codes == ("MANUAL_REVIEW_REQUIRED",)


def test_unknown_policy_status_blocks_instead_of_being_interpreted() -> None:
  item = build_local_execution_impact_item(
    dataset_evidence=_dataset_evidence(
      architecture_fingerprint="changed-fingerprint",
      review_status="approved",
      approval_status="available",
      approval_required=True,
      column_change_types=("COLUMN_ADDED",),
      migration_action_types=("ADD_COLUMN",),
      policy_statuses=("UNKNOWN",),
      policy_codes=("UNKNOWN_POLICY_STATUS",),
    ),
    baseline_source="recorded_state",
    baseline_can_execute=True,
  )

  assert item.decision == "BLOCKED"
  assert item.reason_codes == ("MANUAL_REVIEW_REQUIRED",)


def test_local_plan_is_deterministic_across_execution_postures() -> None:
  customer = _dataset_evidence(
    "rawcore.customer",
    materialization_type="view",
  )
  order = _dataset_evidence(
    "rawcore.order",
    architecture_fingerprint="order-changed",
    review_status="approved",
    approval_status="available",
    approval_required=True,
    column_change_types=("COLUMN_ADDED",),
    migration_action_types=("ADD_COLUMN",),
    policy_statuses=("ALLOW",),
    policy_codes=("ADD_COLUMN_ALLOWED",),
  )

  first = build_local_execution_impact_plan(
    evidence_resolution=_resolution(customer, order),
  )
  second = build_local_execution_impact_plan(
    evidence_resolution=_resolution(order, customer),
  )

  assert first.decision_counts == {
    "REUSE": 1,
    "REVALIDATE": 0,
    "INCREMENTAL_EXECUTE": 0,
    "FULL_REBUILD": 1,
    "BLOCKED": 0,
  }
  assert first.plan_fingerprint == second.plan_fingerprint
  assert tuple(item.dataset_key for item in first.items) == (
    "rawcore.customer",
    "rawcore.order",
  )
