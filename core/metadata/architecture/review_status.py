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
from typing import Any

from metadata.architecture.approval import (
  ArchitectureApprovalArtifact,
  ArchitectureApprovalError,
  ArchitectureApprovalStore,
  check_architecture_approval,
)
from metadata.architecture.report import ArchitectureChangeReport
from metadata.architecture.report_builder import build_architecture_change_report
from metadata.architecture.scope import (
  ArchitectureScopeError,
  resolve_dataset_keys_from_state,
)
from metadata.architecture.service import ArchitectureStateService
from metadata.architecture.store import ArchitectureStateStore
from metadata.materialization.policy import load_materialization_policy


class ArchitectureReviewStatusError(ValueError):
  """
  Raised when architecture review status cannot be built.
  """


@dataclass(frozen=True)
class ArchitectureReviewStatus:
  """
  User-facing review status for a target dataset architecture scope.
  """
  status: str
  label: str
  message: str
  badge_class: str
  icon: str
  dataset_key: str
  report_fingerprint: str
  approval_id: str | None
  artifact_fingerprint: str | None
  review_decision: str | None
  decided_by: str | None
  decided_at: str | None
  note: str | None
  approval_directory: str
  has_changes: bool
  is_blocked: bool
  scope: dict[str, Any]
  state: dict[str, Any]
  summary: dict[str, Any]


def build_target_dataset_architecture_review_status(
  target_dataset,
) -> ArchitectureReviewStatus:
  """
  Build the architecture review status for a TargetDataset.
  """
  schema_short = getattr(
    getattr(target_dataset, "target_schema", None),
    "short_name",
    None,
  )
  target_name = getattr(target_dataset, "target_dataset_name", None)

  if not schema_short or not target_name:
    raise ArchitectureReviewStatusError(
      "TargetDataset must have a target schema and dataset name."
    )

  dataset_key = f"{schema_short}.{target_name}"

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
    raise ArchitectureReviewStatusError(str(exc)) from exc

  report = build_architecture_change_report(
    previous_state=previous_state,
    current_state=current_state,
    policy=load_materialization_policy(),
    relevant_dataset_keys=relevant_dataset_keys,
    schema_short=schema_short,
    target_name=target_name,
    scope_mode="scoped",
  )

  return build_architecture_review_status_for_report(
    dataset_key=dataset_key,
    report=report,
    approval_store=ArchitectureApprovalStore(),
  )


def build_architecture_review_status_for_report(
  *,
  dataset_key: str,
  report: ArchitectureChangeReport,
  approval_store: ArchitectureApprovalStore,
) -> ArchitectureReviewStatus:
  """
  Build review status from a report and an approval artifact store.
  """
  report_payload = report.to_dict()
  approval_directory = str(approval_store.base_path)
  exact_artifact: ArchitectureApprovalArtifact | None = None
  stale_artifact: ArchitectureApprovalArtifact | None = None
  invalid_message: str | None = None

  try:
    exact_artifact = approval_store.load_for_report_fingerprint(
      report.report_fingerprint,
    )
  except ArchitectureApprovalError as exc:
    invalid_message = str(exc)

  if exact_artifact is None and invalid_message is None:
    stale_artifact = _find_approval_for_scope(
      approval_store.load_all(),
      report_payload.get("scope") or {},
      report.report_fingerprint,
    )

  if exact_artifact is not None:
    check_result = check_architecture_approval(
      report_payload=report_payload,
      approval_payload=exact_artifact.to_dict(),
    )
    if not check_result.is_valid:
      invalid_message = check_result.message

  artifact = exact_artifact or stale_artifact

  if invalid_message:
    status = "invalid"
    message = invalid_message
  elif report.is_blocked:
    status = "blocked"
    message = (
      "The architecture report contains blocking policy decisions. "
      "Approval does not override execution policy."
    )
  elif not report.has_changes:
    status = "no_changes"
    message = "No architecture changes are present for this dataset scope."
  elif exact_artifact is not None:
    status = "approved"
    message = "Approval artifact matches the architecture change report."
  elif stale_artifact is not None:
    status = "drift"
    message = (
      "An approval exists for this scope, but it is bound to a different "
      "architecture report fingerprint."
    )
  else:
    status = "pending"
    message = "Architecture changes are present and have no matching approval."

  label, badge_class, icon = _status_metadata(status)

  return ArchitectureReviewStatus(
    status=status,
    label=label,
    message=message,
    badge_class=badge_class,
    icon=icon,
    dataset_key=dataset_key,
    report_fingerprint=report.report_fingerprint,
    approval_id=artifact.approval_id if artifact else None,
    artifact_fingerprint=artifact.artifact_fingerprint if artifact else None,
    review_decision=artifact.review.decision if artifact else None,
    decided_by=artifact.review.decided_by if artifact else None,
    decided_at=artifact.review.decided_at if artifact else None,
    note=artifact.review.note if artifact else None,
    approval_directory=approval_directory,
    has_changes=report.has_changes,
    is_blocked=report.is_blocked,
    scope=report_payload["scope"],
    state=report_payload["state"],
    summary=report_payload["summary"],
  )


def _find_approval_for_scope(
  artifacts: tuple[ArchitectureApprovalArtifact, ...],
  scope: dict[str, Any],
  report_fingerprint: str,
) -> ArchitectureApprovalArtifact | None:
  """
  Return an approval artifact for the same scope and another report fingerprint.
  """
  matching_artifacts = [
    artifact
    for artifact in artifacts
    if artifact.report.get("scope") == scope
    and artifact.report.get("report_fingerprint") != report_fingerprint
  ]

  if not matching_artifacts:
    return None

  return sorted(
    matching_artifacts,
    key=lambda artifact: artifact.review.decided_at,
    reverse=True,
  )[0]


def _status_metadata(status: str) -> tuple[str, str, str]:
  """
  Return label, badge class and icon for a review status.
  """
  if status == "approved":
    return "Approved and matching", "badge-health-ok", "bi-shield-check"
  if status == "pending":
    return "Pending review", "badge-health-warning", "bi-hourglass-split"
  if status == "drift":
    return "Approval drift", "badge-health-warning", "bi-exclamation-diamond"
  if status == "blocked":
    return "Blocked by policy", "badge-health-error", "bi-shield-exclamation"
  if status == "no_changes":
    return "No architecture changes", "badge-lineage-inactive", "bi-check2-circle"
  if status == "invalid":
    return "Invalid approval artifact", "badge-health-error", "bi-x-octagon"

  return "Review status", "badge-lineage-inactive", "bi-info-circle"