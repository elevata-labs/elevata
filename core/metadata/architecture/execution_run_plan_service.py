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
from datetime import datetime, timezone
import uuid

from metadata.architecture.control import (
  ArchitectureControlContext,
  ArchitectureControlError,
  ArchitectureControlScope,
  build_architecture_control_context,
)
from metadata.architecture.execution_impact import (
  ExecutionImpactSelection,
  build_execution_impact_selection,
)
from metadata.architecture.execution_preview import (
  ArchitectureExecutionPreview,
  ArchitectureExecutionPreviewError,
  build_architecture_execution_preview,
  resolve_architecture_execution_scope,
)
from metadata.architecture.execution_run_plan import (
  ExecutionRunPlan,
  build_execution_plan_fingerprint,
  build_execution_run_plan,
)
from metadata.architecture.paths import (
  ArchitectureArtifactContext,
  resolve_architecture_artifact_context,
)


class ArchitectureExecutionRunPlanError(ValueError):
  """
  Raised when a safe public Execution Run Plan cannot be assembled.
  """


SCHEDULER_METADATA_CHANGE_GUIDANCE = (
  "Next steps for scheduled execution:\n"
  "1. Review the affected scope in Architecture Control.\n"
  "2. Resolve blocking policy decisions by changing metadata or policy. "
  "An Approval Artifact does not override blocking policy decisions.\n"
  "3. When the report is approvable and the change is intentional, "
  "create a matching Approval Artifact.\n"
  "4. If active datasets or execution dependencies changed, regenerate "
  "the execution manifest outside an active DAG run and allow the "
  "scheduler to reparse the DAG.\n"
  "5. Start a new DAG run. Do not reuse an existing Execution Run Plan."
)


def build_execution_run_plan_block_message(
  message: object,
) -> str:
  """
  Add actionable scheduler guidance to an Architecture Control failure.

  The original control message remains authoritative. The additional
  guidance explains how metadata review, policy blockers, approvals,
  manifest regeneration and scheduler reruns relate to each other.
  """
  detail = str(
    message or ""
  ).strip()

  if not detail:
    detail = (
      "Execution Run Plan creation is blocked by "
      "Architecture Control."
    )

  return (
    f"{detail}\n\n"
    f"{SCHEDULER_METADATA_CHANGE_GUIDANCE}"
  )


@dataclass(frozen=True)
class ArchitectureExecutionRunPlanResult:
  """
  Result of assembling one public Execution Run Plan.
  """
  run_plan: ExecutionRunPlan
  control_context: ArchitectureControlContext
  preview: ArchitectureExecutionPreview


def build_architecture_execution_run_plan(
  scope: ArchitectureControlScope,
  *,
  profile_name: str | None = None,
  target_system_short: str | None = None,
  no_deps: bool = False,
  artifact_context: ArchitectureArtifactContext | None = None,
  run_plan_id: str | None = None,
  batch_run_id: str | None = None,
  created_at: str | None = None,
) -> ArchitectureExecutionRunPlanResult:
  """
  Assemble one immutable scheduler run plan for an executable scope.

  Architecture state, review status, approval evidence, execution scope,
  preview and impact decisions are resolved once and bound into one public
  artifact. A blocked or revalidation-required scope fails before a run plan
  can be returned.
  """
  runtime_context = (
    artifact_context
    or resolve_architecture_artifact_context(
      profile_name=profile_name,
      target_system_short=target_system_short,
    )
  )

  try:
    execution_scope = resolve_architecture_execution_scope(
      scope,
      no_deps=no_deps,
    )

    control_context = build_architecture_control_context(
      scope,
      artifact_context=runtime_context,
      execution_dataset_keys=(
        execution_scope.execution_dataset_keys
      ),
      dependency_mode=execution_scope.dependency_mode,
    )

    preview = build_architecture_execution_preview(
      scope,
      control_context=control_context,
      no_deps=no_deps,
      execution_scope_resolution=execution_scope,
    )
  except (
    ArchitectureControlError,
    ArchitectureExecutionPreviewError,
  ) as exc:
    raise ArchitectureExecutionRunPlanError(
      build_execution_run_plan_block_message(
        exc
      )
    ) from exc
  except ValueError as exc:
    raise ArchitectureExecutionRunPlanError(
      str(exc)
    ) from exc

  if not preview.gate.can_execute:
    raise ArchitectureExecutionRunPlanError(
      build_execution_run_plan_block_message(
        preview.gate.message
      )
    )

  impact_plan = control_context.execution_impact_plan
  if impact_plan is None:
    detail = str(
      control_context.execution_impact_plan_error
      or ""
    ).strip()
    raise ArchitectureExecutionRunPlanError(
      detail
      or (
        "Execution Run Plan creation requires an available "
        "Execution Impact Plan."
      )
    )

  try:
    impact_selection = build_execution_impact_selection(
      impact_plan,
      execution_dataset_keys=preview.execution_dataset_keys,
    )
  except ValueError as exc:
    raise ArchitectureExecutionRunPlanError(str(exc)) from exc

  _validate_preview_impact_binding(
    preview=preview,
    impact_selection=impact_selection,
  )

  try:
    run_plan = build_execution_run_plan(
      run_plan_id=run_plan_id or uuid.uuid4().hex,
      batch_run_id=batch_run_id or str(uuid.uuid4()),
      created_at=(
        created_at
        or datetime.now(timezone.utc).isoformat()
      ),
      profile_name=runtime_context.profile_name,
      target_system_short=runtime_context.target_system_short,
      scope_mode=scope.mode,
      scope_key=preview.scope_key,
      scope_label=preview.scope_label,
      dependency_mode=preview.dependency_mode,
      review_status=preview.review_status,
      approval_id=preview.approval_id,
      architecture_fingerprint=(
        impact_plan.architecture_fingerprint
      ),
      report_fingerprint=preview.report_fingerprint,
      preview_fingerprint=preview.preview_fingerprint,
      execution_plan_fingerprint=(
        build_execution_plan_fingerprint(
          execution_scope.execution_order
        )
      ),
      root_dataset_keys=preview.root_dataset_keys,
      impact_selection=impact_selection,
    )
  except ValueError as exc:
    raise ArchitectureExecutionRunPlanError(str(exc)) from exc

  return ArchitectureExecutionRunPlanResult(
    run_plan=run_plan,
    control_context=control_context,
    preview=preview,
  )


def _validate_preview_impact_binding(
  *,
  preview: ArchitectureExecutionPreview,
  impact_selection: ExecutionImpactSelection,
) -> None:
  """
  Verify that the public run plan still references the previewed impact plan.
  """
  binding = preview.impact_plan_binding
  if binding is None:
    raise ArchitectureExecutionRunPlanError(
      "Execution Run Plan creation requires an impact-bound preview."
    )

  if binding.plan_fingerprint != impact_selection.plan_fingerprint:
    raise ArchitectureExecutionRunPlanError(
      "Execution Impact Selection does not match the bound "
      "execution preview."
    )

  if preview.execution_dataset_keys != impact_selection.dataset_keys:
    raise ArchitectureExecutionRunPlanError(
      "Execution Impact Selection order does not match the "
      "execution preview."
    )
