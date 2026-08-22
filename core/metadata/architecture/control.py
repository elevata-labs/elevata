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

from dataclasses import dataclass, replace
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
from metadata.architecture.execution_impact import ExecutionImpactPlan
from metadata.architecture.execution_impact_service import (
  ExecutionImpactPlanError,
  build_execution_impact_plan,
)
from metadata.architecture.execution_record import ArchitectureExecutionRecordStore
from metadata.architecture.paths import (
  ArchitectureArtifactContext,
  resolve_architecture_artifact_context,
)
from metadata.architecture.physical_state import (
  ArchitectureBaselineResolution,
  resolve_architecture_baseline,
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
from metadata.architecture.state import ArchitectureState
from metadata.architecture.store import ArchitectureStateStore
from metadata.execution.load_scope import LoadScopeError, resolve_partial_load_scope
from metadata.materialization.policy import load_materialization_policy
from metadata.models import TargetDataset


ArchitectureControlScopeMode = Literal[
  "target_dataset",
  "schema",
  "partial_load",
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
  partial_load_name: str | None = None
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

    if self.mode == "partial_load":
      return f"partial_load:{self.partial_load_name or ''}"

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

    if self.mode == "partial_load":
      return f"Partial load: {self.partial_load_name or ''}"

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
  def for_partial_load(
    cls,
    partial_load_name: str,
    *,
    include_related_hist: bool = True,
  ) -> ArchitectureControlScope:
    """
    Build an Architecture Control scope for one named reusable Partial Load.
    """
    name = (partial_load_name or "").strip()
    if not name:
      raise ArchitectureControlError("Partial Load scope requires a Partial Load name.")

    return cls(
      mode="partial_load",
      partial_load_name=name,
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
  artifact_context: ArchitectureArtifactContext
  report: ArchitectureChangeReport
  review_status: ArchitectureReviewStatus
  approval_store: ArchitectureApprovalStore
  state_store: ArchitectureStateStore
  baseline_resolution: ArchitectureBaselineResolution
  current_state: ArchitectureState | None = None
  execution_impact_plan: ExecutionImpactPlan | None = None
  execution_impact_plan_error: str | None = None
  execution_dependency_mode: str = "with_dependencies"
  execution_dataset_keys: tuple[str, ...] = ()


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
  *,
  artifact_context: ArchitectureArtifactContext | None = None,
  state_store: ArchitectureStateStore | None = None,
) -> ArchitectureChangeReport:
  """
  Build the Architecture Change Report for an Architecture Control scope.
  """
  report, _baseline = build_architecture_control_report_with_baseline(
    scope,
    artifact_context=artifact_context,
    state_store=state_store,
  )
  return report


def build_architecture_control_report_with_baseline(
  scope: ArchitectureControlScope,
  *,
  artifact_context: ArchitectureArtifactContext | None = None,
  state_store: ArchitectureStateStore | None = None,
  current_state: ArchitectureState | None = None,
) -> tuple[ArchitectureChangeReport, ArchitectureBaselineResolution]:
  """
  Build the Architecture Change Report and its resolved comparison baseline.
  """
  runtime_context = artifact_context or resolve_architecture_artifact_context()
  store = state_store or ArchitectureStateStore(context=runtime_context)

  resolved_current_state = (
    current_state
    if current_state is not None
    else ArchitectureStateService().build_current_state()
  )

  try:
    current_dataset_keys = _resolve_relevant_dataset_keys(
      scope=scope,
      current_state=resolved_current_state,
    )
    baseline_resolution = resolve_architecture_baseline(
      current_state=resolved_current_state,
      artifact_context=runtime_context,
      state_store=store,
      relevant_dataset_keys=current_dataset_keys,
    )
    review_dataset_keys = _resolve_review_dataset_keys(
      scope=scope,
      current_dataset_keys=current_dataset_keys,
      previous_state=baseline_resolution.previous_state,
    )
  except (ArchitectureScopeError, LoadScopeError) as exc:
    raise ArchitectureControlError(str(exc)) from exc

  report = build_architecture_change_report(
    previous_state=baseline_resolution.previous_state,
    current_state=resolved_current_state,
    policy=load_materialization_policy(),
    relevant_dataset_keys=review_dataset_keys,
    schema_short=scope.schema_short,
    target_name=scope.target_name,
    scope_mode=_report_scope_mode(scope),
  )
  return report, baseline_resolution


def build_architecture_control_context(
  scope: ArchitectureControlScope,
  *,
  approval_store: ArchitectureApprovalStore | None = None,
  artifact_context: ArchitectureArtifactContext | None = None,
  execution_dataset_keys: tuple[str, ...] | None = None,
  dependency_mode: str = "with_dependencies",
) -> ArchitectureControlContext:
  """
  Build the shared Architecture Control context for one scope.
  """
  runtime_context = artifact_context or resolve_architecture_artifact_context()
  state_store = ArchitectureStateStore(context=runtime_context)
  store = approval_store or ArchitectureApprovalStore(context=runtime_context)
  current_state = ArchitectureStateService().build_current_state()
  report, baseline_resolution = build_architecture_control_report_with_baseline(
    scope,
    artifact_context=runtime_context,
    state_store=state_store,
    current_state=current_state,
  )

  review_status = build_architecture_review_status_for_report(
    dataset_key=scope.key,
    report=report,
    approval_store=store,
    baseline_resolution=baseline_resolution,
  )

  execution_impact_plan, execution_impact_plan_error = (
    _build_architecture_control_execution_impact_plan(
      scope=scope,
      artifact_context=runtime_context,
      current_state=current_state,
      baseline_resolution=baseline_resolution,
      report=report,
      review_status=review_status,
      execution_dataset_keys=execution_dataset_keys,
      dependency_mode=dependency_mode,
    )
  )

  bound_execution_dataset_keys = (
    tuple(execution_dataset_keys)
    if execution_dataset_keys is not None
    else tuple(
      item.dataset_key
      for item in getattr(execution_impact_plan, "items", ()) or ()
    )
  )

  return ArchitectureControlContext(
    scope=scope,
    artifact_context=runtime_context,
    report=report,
    review_status=review_status,
    approval_store=store,
    state_store=state_store,
    baseline_resolution=baseline_resolution,
    current_state=current_state,
    execution_impact_plan=execution_impact_plan,
    execution_impact_plan_error=execution_impact_plan_error,
    execution_dependency_mode=dependency_mode,
    execution_dataset_keys=bound_execution_dataset_keys,
  )


def bind_architecture_control_execution_scope(
  context: ArchitectureControlContext,
  *,
  execution_dataset_keys: tuple[str, ...],
  dependency_mode: str,
) -> ArchitectureControlContext:
  """
  Rebind an existing Architecture Control context to one concrete execution.

  Current State, baseline, report and review status remain authoritative and
  are not rebuilt. Only the read-only Execution Impact Plan is reassembled for
  the exact dataset scope and dependency mode used by Execution Preview.
  """
  normalized_dataset_keys = tuple(
    str(dataset_key or "").strip()
    for dataset_key in execution_dataset_keys
    if str(dataset_key or "").strip()
  )
  if not normalized_dataset_keys:
    raise ArchitectureControlError(
      "Controlled execution scope must contain at least one TargetDataset."
    )

  if len(normalized_dataset_keys) != len(set(normalized_dataset_keys)):
    raise ArchitectureControlError(
      "Controlled execution scope contains duplicate TargetDatasets."
    )

  normalized_dependency_mode = str(dependency_mode or "").strip()
  if normalized_dependency_mode not in {
    "with_dependencies",
    "target_only",
  }:
    raise ArchitectureControlError(
      "Unsupported controlled execution dependency mode: "
      f"{normalized_dependency_mode}"
    )

  if context.current_state is None:
    raise ArchitectureControlError(
      "Architecture Control current state is unavailable for impact planning."
    )

  execution_impact_plan, execution_impact_plan_error = (
    _build_architecture_control_execution_impact_plan(
      scope=context.scope,
      artifact_context=context.artifact_context,
      current_state=context.current_state,
      baseline_resolution=context.baseline_resolution,
      report=context.report,
      review_status=context.review_status,
      execution_dataset_keys=normalized_dataset_keys,
      dependency_mode=normalized_dependency_mode,
    )
  )

  return replace(
    context,
    execution_impact_plan=execution_impact_plan,
    execution_impact_plan_error=execution_impact_plan_error,
    execution_dependency_mode=normalized_dependency_mode,
    execution_dataset_keys=normalized_dataset_keys,
  )


def _build_architecture_control_execution_impact_plan(
  *,
  scope: ArchitectureControlScope,
  artifact_context: ArchitectureArtifactContext,
  current_state: ArchitectureState,
  baseline_resolution: ArchitectureBaselineResolution,
  report: ArchitectureChangeReport,
  review_status: ArchitectureReviewStatus,
  execution_dataset_keys: tuple[str, ...] | None = None,
  dependency_mode: str = "with_dependencies",
) -> tuple[ExecutionImpactPlan | None, str | None]:
  """
  Build the optional read-only Execution Impact Plan for the control context.

  Impact planning failures remain isolated from report, review and approval
  workflows so Architecture Control can still render its authoritative state.
  """
  try:
    target_datasets = (
      TargetDataset.objects
      .select_related("target_schema")
      .filter(active=True)
      .order_by(
        "target_schema__short_name",
        "target_dataset_name",
        "id",
      )
    )
    execution_record_store = ArchitectureExecutionRecordStore(
      context=artifact_context,
    )
    plan = build_execution_impact_plan(
      scope_key=scope.key,
      current_state=current_state,
      baseline_resolution=baseline_resolution,
      report=report,
      review_status=review_status,
      target_datasets=target_datasets,
      execution_record_store=execution_record_store,
      execution_dataset_keys=execution_dataset_keys,
      dependency_mode=dependency_mode,
    )
  except ExecutionImpactPlanError as exc:
    return None, str(exc)
  except Exception as exc:
    return None, f"Execution Impact Plan integration failed: {exc}"

  return plan, None


def render_architecture_control_report_json(
  scope: ArchitectureControlScope,
  *,
  artifact_context: ArchitectureArtifactContext | None = None,
) -> str:
  """
  Render the Architecture Control report as deterministic JSON.
  """
  report = build_architecture_control_report(
    scope,
    artifact_context=artifact_context,
  )
  return render_architecture_report_json(report)


def render_architecture_control_report_text(
  scope: ArchitectureControlScope,
  *,
  artifact_context: ArchitectureArtifactContext | None = None,
) -> str:
  """
  Render the Architecture Control report as deterministic text.
  """
  report = build_architecture_control_report(
    scope,
    artifact_context=artifact_context,
  )
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

  if context.review_status.status == "initial_deployment":
    raise ArchitectureControlError(
      "An Approval Artifact is not required for a verified initial deployment. "
      "Run the complete controlled scope to establish the first Architecture State."
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

  if scope.mode == "partial_load":
    resolved = resolve_partial_load_scope(scope.partial_load_name or "")
    return set(resolved.execution_dataset_keys)

  return resolve_dataset_keys_from_state(
    state=current_state,
    target_name=scope.target_name,
    schema_short=scope.schema_short,
    all_datasets=False,
    include_related_hist=scope.include_related_hist,
  )


def _resolve_review_dataset_keys(
  *,
  scope: ArchitectureControlScope,
  current_dataset_keys: set[str] | None,
  previous_state: ArchitectureState | None,
) -> set[str] | None:
  """
  Resolve dataset keys for Architecture Change Report review.

  Schema review includes datasets that only remain in the recorded baseline so
  retirement is visible without exposing inactive datasets as selectable target
  scopes. Physical discovery and controlled execution remain current-state only.
  """
  if scope.mode != "schema" or previous_state is None:
    return current_dataset_keys

  previous_dataset_keys = resolve_dataset_keys_from_state(
    state=previous_state,
    target_name=None,
    schema_short=scope.schema_short,
    all_datasets=True,
    include_related_hist=scope.include_related_hist,
  )
  return set(current_dataset_keys or ()) | previous_dataset_keys


def _report_scope_mode(scope: ArchitectureControlScope) -> Literal["all", "scoped"]:
  """
  Return the report scope mode for an Architecture Control scope.
  """
  if scope.mode == "all":
    return "all"

  return "scoped"