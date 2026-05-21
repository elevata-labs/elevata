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
from pathlib import Path
from typing import Any

from metadata.architecture.approval import (
  ArchitectureApprovalArtifact,
  ArchitectureApprovalCheckResult,
  ArchitectureApprovalError,
  ArchitectureApprovalStore,
  build_architecture_approval_artifact,
  check_architecture_approval,
)
from metadata.architecture.renderers import (
  render_architecture_report_json,
  render_architecture_report_text,
)
from metadata.architecture.report import ArchitectureChangeReport
from metadata.architecture.report_builder import build_architecture_change_report
from metadata.architecture.review_status import (
  ArchitectureReviewStatus,
  build_architecture_review_status_for_report,
)
from metadata.architecture.scope import (
  ArchitectureScopeError,
  resolve_dataset_keys_from_state,
)
from metadata.architecture.service import ArchitectureStateService
from metadata.architecture.store import ArchitectureStateStore
from metadata.materialization.policy import load_materialization_policy


class ArchitectureOperationsError(ValueError):
  """
  Raised when architecture operations cannot be completed.
  """


@dataclass(frozen=True)
class ArchitectureOperationsContext:
  """
  Runtime context for architecture operations on a TargetDataset scope.
  """
  target_dataset: Any
  dataset_key: str
  report: ArchitectureChangeReport
  review_status: ArchitectureReviewStatus
  approval_store: ArchitectureApprovalStore


@dataclass(frozen=True)
class ArchitectureApprovalOperationResult:
  """
  Result of creating an approval artifact from the Architecture Operations UI.
  """
  context: ArchitectureOperationsContext
  artifact: ArchitectureApprovalArtifact
  approval_path: Path


def _resolve_target_dataset_scope(target_dataset: Any) -> tuple[str, str, str]:
  """
  Resolve schema short name, target dataset name and dataset key.
  """
  schema_short = getattr(
    getattr(target_dataset, "target_schema", None),
    "short_name",
    None,
  )
  target_name = getattr(target_dataset, "target_dataset_name", None)

  if not schema_short or not target_name:
    raise ArchitectureOperationsError(
      "TargetDataset must have a target schema and dataset name."
    )

  return schema_short, target_name, f"{schema_short}.{target_name}"


def build_target_dataset_architecture_report(
  target_dataset: Any,
) -> tuple[str, ArchitectureChangeReport]:
  """
  Build the scoped Architecture Change Report for a TargetDataset.
  """
  schema_short, target_name, dataset_key = _resolve_target_dataset_scope(
    target_dataset,
  )

  try:
    current_state = ArchitectureStateService().build_current_state()
    previous_state = ArchitectureStateStore().load()
    relevant_dataset_keys = resolve_dataset_keys_from_state(
      state=current_state,
      target_name=target_name,
      schema_short=schema_short,
      all_datasets=False,
    )
  except ArchitectureScopeError as exc:
    raise ArchitectureOperationsError(str(exc)) from exc

  report = build_architecture_change_report(
    previous_state=previous_state,
    current_state=current_state,
    policy=load_materialization_policy(),
    relevant_dataset_keys=relevant_dataset_keys,
    schema_short=schema_short,
    target_name=target_name,
    scope_mode="scoped",
  )

  return dataset_key, report


def build_target_dataset_architecture_operations_context(
  target_dataset: Any,
  *,
  approval_store: ArchitectureApprovalStore | None = None,
) -> ArchitectureOperationsContext:
  """
  Build the shared Architecture Operations context for a TargetDataset.
  """
  store = approval_store or ArchitectureApprovalStore()
  dataset_key, report = build_target_dataset_architecture_report(target_dataset)

  review_status = build_architecture_review_status_for_report(
    dataset_key=dataset_key,
    report=report,
    approval_store=store,
  )

  return ArchitectureOperationsContext(
    target_dataset=target_dataset,
    dataset_key=dataset_key,
    report=report,
    review_status=review_status,
    approval_store=store,
  )


def render_target_dataset_architecture_report_json(target_dataset: Any) -> str:
  """
  Render the scoped Architecture Change Report as deterministic JSON.
  """
  _, report = build_target_dataset_architecture_report(target_dataset)
  return render_architecture_report_json(report)


def render_target_dataset_architecture_report_text(target_dataset: Any) -> str:
  """
  Render the scoped Architecture Change Report as deterministic text.
  """
  _, report = build_target_dataset_architecture_report(target_dataset)
  return render_architecture_report_text(report)


def create_target_dataset_architecture_approval(
  target_dataset: Any,
  *,
  approved_by: str,
  note: str = "",
  approval_store: ArchitectureApprovalStore | None = None,
) -> ArchitectureApprovalOperationResult:
  """
  Create and store an approval artifact for the scoped report.
  """
  context = build_target_dataset_architecture_operations_context(
    target_dataset,
    approval_store=approval_store,
  )

  if not context.report.has_changes:
    raise ArchitectureOperationsError(
      "No architecture changes are present for this dataset scope."
    )

  if context.report.is_blocked:
    raise ArchitectureOperationsError(
      "Architecture approval is disabled because the report is blocked by policy."
    )

  if context.review_status.status == "approved":
    raise ArchitectureOperationsError(
      "A matching approval artifact already exists for this report."
    )

  try:
    artifact = build_architecture_approval_artifact(
      report_payload=context.report.to_dict(),
      decided_by=approved_by,
      note=note,
    )
    approval_path = context.approval_store.save(artifact)
  except ArchitectureApprovalError as exc:
    raise ArchitectureOperationsError(str(exc)) from exc

  return ArchitectureApprovalOperationResult(
    context=context,
    artifact=artifact,
    approval_path=approval_path,
  )


def check_target_dataset_architecture_approval(
  target_dataset: Any,
  *,
  approval_store: ArchitectureApprovalStore | None = None,
) -> ArchitectureApprovalCheckResult:
  """
  Check the stored approval artifact against the scoped report.
  """
  context = build_target_dataset_architecture_operations_context(
    target_dataset,
    approval_store=approval_store,
  )

  try:
    artifact = context.approval_store.load_for_report_fingerprint(
      context.report.report_fingerprint,
    )
  except ArchitectureApprovalError as exc:
    return ArchitectureApprovalCheckResult(
      is_valid=False,
      status="invalid",
      message=str(exc),
      report_fingerprint=context.report.report_fingerprint,
    )

  if artifact is None:
    if context.review_status.status == "drift":
      return ArchitectureApprovalCheckResult(
        is_valid=False,
        status="drift",
        message=context.review_status.message,
        report_fingerprint=context.report.report_fingerprint,
        approval_id=context.review_status.approval_id,
        artifact_fingerprint=context.review_status.artifact_fingerprint,
      )

    return ArchitectureApprovalCheckResult(
      is_valid=False,
      status="missing",
      message="No approval artifact exists for the report fingerprint.",
      report_fingerprint=context.report.report_fingerprint,
    )

  return check_architecture_approval(
    report_payload=context.report.to_dict(),
    approval_payload=artifact.to_dict(),
  )