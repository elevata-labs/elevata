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
from typing import Any, Literal

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


ArchitectureControlScopeMode = Literal[
  "target_dataset",
  "schema",
  "all",
]


class ArchitectureControlError(ValueError):
  """
  Raised when Architecture Control cannot complete an operation.
  """


@dataclass(frozen=True)
class ArchitectureControlScope:
  """
  Scope definition for Architecture Control review and execution workflows.
  """
  mode: ArchitectureControlScopeMode
  schema_short: str | None = None
  target_name: str | None = None
  dataset_key: str | None = None
  include_related_hist: bool = True

  @property
  def key(self) -> str:
    """
    Return the stable scope key used for status and UI references.
    """
    if self.mode == "target_dataset":
      return self.dataset_key or ""

    if self.mode == "schema":
      return f"schema:{self.schema_short or ''}"

    return "all"

  @property
  def label(self) -> str:
    """
    Return a human-readable scope label.
    """
    if self.mode == "target_dataset":
      return self.dataset_key or "Target dataset"

    if self.mode == "schema":
      return f"Schema: {self.schema_short or ''}"

    return "All datasets"

  @classmethod
  def from_target_dataset(
    cls,
    target_dataset: Any,
    *,
    include_related_hist: bool = True,
  ) -> ArchitectureControlScope:
    """
    Build an Architecture Control scope from a TargetDataset-shaped object.
    """
    schema_short = getattr(
      getattr(target_dataset, "target_schema", None),
      "short_name",
      None,
    )
    target_name = getattr(target_dataset, "target_dataset_name", None)

    if not schema_short or not target_name:
      raise ArchitectureControlError(
        "TargetDataset must have a target schema and dataset name."
      )

    dataset_key = f"{schema_short}.{target_name}"

    return cls(
      mode="target_dataset",
      schema_short=schema_short,
      target_name=target_name,
      dataset_key=dataset_key,
      include_related_hist=include_related_hist,
    )

  @classmethod
  def for_schema(
    cls,
    schema_short: str,
    *,
    include_related_hist: bool = True,
  ) -> ArchitectureControlScope:
    """
    Build an Architecture Control scope for all datasets in one schema.
    """
    schema_value = (schema_short or "").strip()
    if not schema_value:
      raise ArchitectureControlError("Schema scope requires a schema short name.")

    return cls(
      mode="schema",
      schema_short=schema_value,
      include_related_hist=include_related_hist,
    )

  @classmethod
  def for_all(
    cls,
    *,
    include_related_hist: bool = True,
  ) -> ArchitectureControlScope:
    """
    Build an Architecture Control scope for all datasets.
    """
    return cls(
      mode="all",
      include_related_hist=include_related_hist,
    )


@dataclass(frozen=True)
class ArchitectureControlContext:
  """
  Runtime context for Architecture Control on one architecture scope.
  """
  scope: ArchitectureControlScope
  report: ArchitectureChangeReport
  review_status: ArchitectureReviewStatus
  approval_store: ArchitectureApprovalStore


@dataclass(frozen=True)
class ArchitectureControlApprovalResult:
  """
  Result of creating an Architecture Control approval artifact.
  """
  context: ArchitectureControlContext
  artifact: ArchitectureApprovalArtifact
  approval_path: Path


def build_architecture_control_report(
  scope: ArchitectureControlScope,
) -> ArchitectureChangeReport:
  """
  Build the Architecture Change Report for an Architecture Control scope.
  """
  try:
    current_state = ArchitectureStateService().build_current_state()
    previous_state = ArchitectureStateStore().load()
    relevant_dataset_keys = _resolve_relevant_dataset_keys(
      scope=scope,
      current_state=current_state,
    )
  except ArchitectureScopeError as exc:
    raise ArchitectureControlError(str(exc)) from exc

  return build_architecture_change_report(
    previous_state=previous_state,
    current_state=current_state,
    policy=load_materialization_policy(),
    relevant_dataset_keys=relevant_dataset_keys,
    schema_short=scope.schema_short,
    target_name=scope.target_name,
    scope_mode=_report_scope_mode(scope),
  )


def build_architecture_control_context(
  scope: ArchitectureControlScope,
  *,
  approval_store: ArchitectureApprovalStore | None = None,
) -> ArchitectureControlContext:
  """
  Build the shared Architecture Control context for one scope.
  """
  store = approval_store or ArchitectureApprovalStore()
  report = build_architecture_control_report(scope)

  review_status = build_architecture_review_status_for_report(
    dataset_key=scope.key,
    report=report,
    approval_store=store,
  )

  return ArchitectureControlContext(
    scope=scope,
    report=report,
    review_status=review_status,
    approval_store=store,
  )


def render_architecture_control_report_json(
  scope: ArchitectureControlScope,
) -> str:
  """
  Render the Architecture Control report as deterministic JSON.
  """
  report = build_architecture_control_report(scope)
  return render_architecture_report_json(report)


def render_architecture_control_report_text(
  scope: ArchitectureControlScope,
) -> str:
  """
  Render the Architecture Control report as deterministic text.
  """
  report = build_architecture_control_report(scope)
  return render_architecture_report_text(report)


def create_architecture_control_approval(
  scope: ArchitectureControlScope,
  *,
  approved_by: str,
  note: str = "",
  approval_store: ArchitectureApprovalStore | None = None,
) -> ArchitectureControlApprovalResult:
  """
  Create and store an approval artifact for an Architecture Control scope.
  """
  context = build_architecture_control_context(
    scope,
    approval_store=approval_store,
  )

  if not context.report.has_changes:
    raise ArchitectureControlError(
      "No architecture changes are present for this architecture scope."
    )

  if context.report.is_blocked:
    raise ArchitectureControlError(
      "Architecture approval is disabled because the report is blocked by policy."
    )

  if context.review_status.status == "approved":
    raise ArchitectureControlError(
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
    raise ArchitectureControlError(str(exc)) from exc

  return ArchitectureControlApprovalResult(
    context=context,
    artifact=artifact,
    approval_path=approval_path,
  )


def check_architecture_control_approval(
  scope: ArchitectureControlScope,
  *,
  approval_store: ArchitectureApprovalStore | None = None,
) -> ArchitectureApprovalCheckResult:
  """
  Check the stored approval artifact against the Architecture Control report.
  """
  context = build_architecture_control_context(
    scope,
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


def _resolve_relevant_dataset_keys(
  *,
  scope: ArchitectureControlScope,
  current_state,
) -> set[str] | None:
  """
  Resolve dataset keys for an Architecture Control report.
  """
  if scope.mode == "all":
    return None

  if scope.mode == "schema":
    return resolve_dataset_keys_from_state(
      state=current_state,
      target_name=None,
      schema_short=scope.schema_short,
      all_datasets=True,
      include_related_hist=scope.include_related_hist,
    )

  return resolve_dataset_keys_from_state(
    state=current_state,
    target_name=scope.target_name,
    schema_short=scope.schema_short,
    all_datasets=False,
    include_related_hist=scope.include_related_hist,
  )


def _report_scope_mode(scope: ArchitectureControlScope) -> Literal["all", "scoped"]:
  """
  Return the report scope mode for an Architecture Control scope.
  """
  if scope.mode == "all":
    return "all"

  return "scoped"