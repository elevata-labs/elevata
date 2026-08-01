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
import json
from typing import Any

from django.db import transaction

from metadata.generation.target_generation_control import (
  TargetGenerationApprovalArtifact,
  TargetGenerationControlError,
  build_target_generation_review,
  check_target_generation_approval,
)
from metadata.generation.target_generation_plan import TargetGenerationPlan
from metadata.models import (
  SourceColumn,
  SourceDataset,
  SourceDatasetGroup,
  SourceDatasetGroupMembership,
  System,
  TargetColumn,
  TargetColumnInput,
  TargetDataset,
  TargetDatasetInput,
  TargetDatasetReference,
  TargetDatasetReferenceComponent,
  TargetSchema,
)


class TargetGenerationPlanApplyError(ValueError):
  """Base error for guarded Target Generation apply failures."""


class TargetGenerationPlanDriftError(TargetGenerationPlanApplyError):
  """Raised when current metadata no longer matches a supplied plan."""

  def __init__(self, drift_fields: tuple[str, ...]) -> None:
    self.drift_fields = tuple(drift_fields)
    super().__init__(
      "Target Generation Plan drift detected: "
      + ", ".join(self.drift_fields)
      + ". Create and review a new plan before applying generation."
    )


class TargetGenerationPlanResultError(TargetGenerationPlanApplyError):
  """Raised when the guarded apply result contradicts its plan."""


@dataclass(frozen=True)
class TargetGenerationApplyResult:
  """Structured evidence returned after one guarded plan apply."""

  plan_fingerprint: str
  generation_review_fingerprint: str
  generation_approval_id: str | None
  source_metadata_fingerprint: str
  target_metadata_fingerprint_before: str
  target_metadata_fingerprint_after: str
  planned_action_count: int
  consumed_action_count: int
  residual_action_count: int
  residual_plan_fingerprint: str
  processed_dataset_count: int
  processed_column_count: int
  retired_dataset_count: int
  reactivated_dataset_count: int

  @property
  def converged(self) -> bool:
    """Return whether no follow-up generation action remains."""
    return self.residual_action_count == 0

  @property
  def summary_text(self) -> str:
    """Return a concise operator-facing guarded apply summary."""
    convergence = (
      "converged"
      if self.converged
      else f"{self.residual_action_count} residual actions"
    )
    return (
      f"Applied Target Generation Plan {self.plan_fingerprint[:12]}: "
      f"{self.consumed_action_count} planned actions consumed; "
      f"{convergence}."
    )


@dataclass(frozen=True)
class _GuardedProcessingResult:
  """Internal normalized processing counters for guarded apply."""

  processed_dataset_count: int = 0
  processed_column_count: int = 0
  retired_dataset_count: int = 0
  reactivated_dataset_count: int = 0


def apply_target_generation_plan(
  service,
  plan: TargetGenerationPlan,
  *,
  approval: TargetGenerationApprovalArtifact | None = None,
  require_approval: bool = False,
) -> TargetGenerationApplyResult:
  """
  Apply one exact schema-scoped Target Generation Plan with drift guards.

  The caller must not already be inside a transaction. This guarantees that
  existing on_commit-based rename and history signals run before the final
  plan/result validation callback.
  """
  if not isinstance(plan, TargetGenerationPlan):
    raise TargetGenerationPlanApplyError(
      "Guarded Target Generation apply requires a validated plan."
    )
  if plan.scope_mode != "schema":
    raise TargetGenerationPlanApplyError(
      "Guarded Target Generation apply currently supports schema plans only."
    )
  if len(plan.target_schema_short_names) != 1:
    raise TargetGenerationPlanApplyError(
      "Guarded schema apply requires exactly one target schema."
    )
  if not isinstance(require_approval, bool):
    raise TargetGenerationPlanApplyError(
      "Target Generation approval requirement must be a boolean."
    )

  generation_review_fingerprint, generation_approval_id = (
    _validate_generation_approval(
      plan=plan,
      approval=approval,
      require_approval=require_approval,
    )
  )

  connection = transaction.get_connection()
  if connection.in_atomic_block:
    raise TargetGenerationPlanApplyError(
      "Guarded Target Generation apply must own the outer transaction so "
      "all on_commit generation signals can be validated before it returns."
    )

  final_state: dict[str, Any] = {}
  with transaction.atomic():
    target_schema = _resolve_target_schema(plan)
    eligible = service.get_eligible_source_datasets_for_schema(target_schema)
    _lock_generation_scope(
      target_schema=target_schema,
      source_datasets=eligible,
    )

    current_plan = service.build_plan(
      eligible,
      target_schema,
      reconcile_lifecycle=plan.reconcile_lifecycle,
    )
    _validate_preflight_plan(
      supplied=plan,
      current=current_plan,
    )
    _validate_representable_side_effects(
      plan=plan,
      target_schema=target_schema,
    )

    apply_sources = _source_datasets_for_direct_actions(
      plan=plan,
      eligible=eligible,
    )
    if apply_sources:
      direct_result = service.apply_all_result(
        apply_sources,
        target_schema,
        reconcile_lifecycle=False,
      )
      processing_result = _GuardedProcessingResult(
        processed_dataset_count=direct_result.processed_dataset_count,
        processed_column_count=direct_result.processed_column_count,
      )
    else:
      processing_result = _GuardedProcessingResult()

    if plan.reconcile_lifecycle:
      expected_dataset_ids = _expected_dataset_ids(
        service=service,
        eligible=eligible,
        target_schema=target_schema,
      )
      retired_count, reactivated_count = (
        service._reconcile_generated_target_lifecycle(
          target_schema=target_schema,
          expected_dataset_ids=expected_dataset_ids,
        )
      )
      processing_result = _GuardedProcessingResult(
        processed_dataset_count=(
          processing_result.processed_dataset_count
        ),
        processed_column_count=processing_result.processed_column_count,
        retired_dataset_count=retired_count,
        reactivated_dataset_count=reactivated_count,
      )

    _validate_processing_result(
      service=service,
      apply_sources=apply_sources,
      target_schema=target_schema,
      plan=plan,
      processing_result=processing_result,
    )

    def _finalize_after_generation_signals() -> None:
      residual_plan = service.build_plan(
        eligible,
        target_schema,
        reconcile_lifecycle=plan.reconcile_lifecycle,
      )
      _validate_post_apply_plan(
        supplied=plan,
        residual=residual_plan,
      )
      final_state["residual_plan"] = residual_plan

    # Signal callbacks registered by existing model saves are already queued.
    # Register validation last so it observes their complete semantic outcome.
    transaction.on_commit(_finalize_after_generation_signals)

  residual_plan = final_state.get("residual_plan")
  if residual_plan is None:
    raise TargetGenerationPlanResultError(
      "Guarded Target Generation result validation did not run."
    )

  return TargetGenerationApplyResult(
    plan_fingerprint=plan.plan_fingerprint,
    generation_review_fingerprint=generation_review_fingerprint,
    generation_approval_id=generation_approval_id,
    source_metadata_fingerprint=plan.source_metadata_fingerprint,
    target_metadata_fingerprint_before=plan.target_metadata_fingerprint,
    target_metadata_fingerprint_after=(
      residual_plan.target_metadata_fingerprint
    ),
    planned_action_count=plan.action_count,
    consumed_action_count=plan.action_count,
    residual_action_count=residual_plan.action_count,
    residual_plan_fingerprint=residual_plan.plan_fingerprint,
    processed_dataset_count=processing_result.processed_dataset_count,
    processed_column_count=processing_result.processed_column_count,
    retired_dataset_count=processing_result.retired_dataset_count,
    reactivated_dataset_count=processing_result.reactivated_dataset_count,
  )


def _validate_generation_approval(
  *,
  plan: TargetGenerationPlan,
  approval: TargetGenerationApprovalArtifact | None,
  require_approval: bool,
) -> tuple[str, str | None]:
  """Validate approval before mutation and return review/approval evidence."""
  try:
    review = build_target_generation_review(plan)
  except TargetGenerationControlError as exc:
    raise TargetGenerationPlanApplyError(str(exc)) from exc

  if approval is None:
    if require_approval:
      raise TargetGenerationPlanApplyError(
        "A matching Generation Approval is required for this Target Generation Plan."
      )
    return review.review_fingerprint, None

  try:
    result = check_target_generation_approval(
      review=review,
      approval=approval,
    )
  except TargetGenerationControlError as exc:
    raise TargetGenerationPlanApplyError(str(exc)) from exc

  if not result.is_valid:
    raise TargetGenerationPlanApplyError(result.message)

  return review.review_fingerprint, approval.approval_id


def _resolve_target_schema(plan: TargetGenerationPlan) -> TargetSchema:
  short_name = plan.target_schema_short_names[0]
  try:
    return TargetSchema.objects.select_for_update().get(
      short_name=short_name,
    )
  except TargetSchema.DoesNotExist as exc:
    raise TargetGenerationPlanDriftError(("target schema",)) from exc


def _validate_preflight_plan(
  *,
  supplied: TargetGenerationPlan,
  current: TargetGenerationPlan,
) -> None:
  drift_fields: list[str] = []

  if supplied.target_schema_short_names != current.target_schema_short_names:
    drift_fields.append("target schema scope")
  if supplied.source_dataset_keys != current.source_dataset_keys:
    drift_fields.append("source dataset scope")
  if supplied.reconcile_lifecycle != current.reconcile_lifecycle:
    drift_fields.append("lifecycle scope")
  if (
    supplied.generator_contract_version
    != current.generator_contract_version
  ):
    drift_fields.append("generator contract version")
  if (
    supplied.source_metadata_fingerprint
    != current.source_metadata_fingerprint
  ):
    drift_fields.append("source metadata")
  if (
    supplied.target_metadata_fingerprint
    != current.target_metadata_fingerprint
  ):
    drift_fields.append("target metadata")
  if supplied.plan_fingerprint != current.plan_fingerprint:
    drift_fields.append("generation decisions")

  if drift_fields:
    raise TargetGenerationPlanDriftError(tuple(drift_fields))


def _source_datasets_for_direct_actions(
  *,
  plan: TargetGenerationPlan,
  eligible: list,
) -> list:
  """Return complete source buckets required by non-lifecycle actions."""
  selected_ids: set[int] = set()
  for action in plan.actions:
    if action.effect_origin == "GENERATED_LIFECYCLE":
      continue
    for source_key in action.source_keys:
      source_id = _source_dataset_id(source_key)
      if source_id is not None:
        selected_ids.add(source_id)

  return [
    source_dataset
    for source_dataset in eligible
    if source_dataset.pk in selected_ids
  ]


def _expected_dataset_ids(
  *,
  service,
  eligible: list,
  target_schema: TargetSchema,
) -> set[int]:
  """Resolve the complete generated dataset set for lifecycle reconciliation."""
  expected_ids: set[int] = set()
  buckets = service._bucket_source_datasets(eligible, target_schema)
  for src_list in buckets.values():
    lineage_key = service.build_lineage_key_for_bucket(
      target_schema,
      src_list,
    )
    base_qs = TargetDataset.objects.filter(
      target_schema=target_schema,
      lineage_key=lineage_key,
    )
    if target_schema.short_name == "rawcore":
      base_qs = base_qs.exclude(target_dataset_name__endswith="_hist")
    base_dataset = base_qs.first()
    if base_dataset is None:
      raise TargetGenerationPlanResultError(
        "Guarded Target Generation did not materialize an expected target "
        "dataset from the verified plan."
      )

    expected_ids.add(base_dataset.pk)
    if target_schema.short_name != "rawcore":
      continue
    if not base_dataset.historize or base_dataset.is_hist:
      continue

    hist_dataset = (
      TargetDataset.objects
      .filter(
        target_schema=target_schema,
        lineage_key=lineage_key,
        target_dataset_name__endswith="_hist",
      )
      .first()
    )
    if hist_dataset is None:
      raise TargetGenerationPlanResultError(
        "Guarded Target Generation did not materialize an expected history "
        "companion from the verified plan."
      )
    expected_ids.add(hist_dataset.pk)

  return expected_ids


def _source_dataset_id(source_key: str) -> int | None:
  prefix = "source_dataset:"
  if not str(source_key).startswith(prefix):
    return None
  raw_id = str(source_key)[len(prefix):]
  if not raw_id.isdigit():
    return None
  return int(raw_id)


def _validate_processing_result(
  *,
  service,
  apply_sources,
  target_schema,
  plan: TargetGenerationPlan,
  processing_result,
) -> None:
  expected_processed = len(
    service._bucket_source_datasets(apply_sources, target_schema)
  )
  if processing_result.processed_dataset_count != expected_processed:
    raise TargetGenerationPlanResultError(
      "Guarded Target Generation processed dataset count does not match "
      "the verified plan scope."
    )

  expected_retired = plan.action_counts["RETIRE_TARGET_DATASET"]
  expected_reactivated = plan.action_counts["REACTIVATE_TARGET_DATASET"]
  if processing_result.retired_dataset_count != expected_retired:
    raise TargetGenerationPlanResultError(
      "Guarded Target Generation retired dataset count does not match "
      "the verified plan."
    )
  if processing_result.reactivated_dataset_count != expected_reactivated:
    raise TargetGenerationPlanResultError(
      "Guarded Target Generation reactivated dataset count does not match "
      "the verified plan."
    )


def _validate_post_apply_plan(
  *,
  supplied: TargetGenerationPlan,
  residual: TargetGenerationPlan,
) -> None:
  if (
    supplied.source_metadata_fingerprint
    != residual.source_metadata_fingerprint
  ):
    raise TargetGenerationPlanResultError(
      "Guarded Target Generation unexpectedly changed Source Metadata."
    )

  supplied_actions = {
    _canonical_action(action)
    for action in supplied.actions
  }
  residual_actions = {
    _canonical_action(action)
    for action in residual.actions
  }
  unchanged_actions = supplied_actions & residual_actions
  if unchanged_actions:
    raise TargetGenerationPlanResultError(
      "Guarded Target Generation left one or more supplied plan actions "
      "completely unapplied."
    )


def _validate_representable_side_effects(
  *,
  plan: TargetGenerationPlan,
  target_schema: TargetSchema,
) -> None:
  """
  Reject known model effects not represented by the v1 action vocabulary.

  Existing references must never be silently deleted or rewritten by a guarded
  history rebuild or dataset rename. The legacy apply path remains unchanged;
  only guarded apply rejects such an incomplete plan contract.
  """
  sensitive_dataset_ids: set[int] = set()
  for action in plan.actions:
    payload = action.to_dict()
    before = payload.get("before") or {}
    after = payload.get("after") or {}
    renamed = (
      before.get("target_dataset_name")
      and after.get("target_dataset_name")
      and before.get("target_dataset_name")
      != after.get("target_dataset_name")
    )
    is_existing_history = (
      action.effect_origin == "HISTORY_COMPANION"
      and action.action_type != "CREATE_TARGET_DATASET"
    )
    if not renamed and not is_existing_history:
      continue

    dataset = _dataset_for_plan_key(
      target_schema=target_schema,
      dataset_key=action.dataset_key,
    )
    if dataset is not None:
      sensitive_dataset_ids.add(dataset.pk)

  if not sensitive_dataset_ids:
    return

  has_references = (
    TargetDatasetReference.objects
    .filter(referencing_dataset_id__in=sensitive_dataset_ids)
    .exists()
    or TargetDatasetReference.objects
    .filter(referenced_dataset_id__in=sensitive_dataset_ids)
    .exists()
  )
  if has_references:
    raise TargetGenerationPlanApplyError(
      "Guarded Target Generation cannot apply this plan because existing "
      "dataset references would be changed by model/history side effects that "
      "are not represented by the generation plan contract."
    )


def _dataset_for_plan_key(
  *,
  target_schema: TargetSchema,
  dataset_key: str,
) -> TargetDataset | None:
  prefix = f"{target_schema.short_name}:"
  if not dataset_key.startswith(prefix):
    return None

  if dataset_key.endswith(":hist"):
    lineage_key = dataset_key[len(prefix):-5]
    return (
      TargetDataset.objects
      .filter(
        target_schema=target_schema,
        lineage_key=lineage_key,
      )
      .filter(target_dataset_name__endswith="_hist")
      .first()
    )
  if dataset_key.endswith(":base"):
    lineage_key = dataset_key[len(prefix):-5]
    return (
      TargetDataset.objects
      .filter(
        target_schema=target_schema,
        lineage_key=lineage_key,
      )
      .exclude(target_dataset_name__endswith="_hist")
      .first()
    )
  return None


def _lock_generation_scope(
  *,
  target_schema: TargetSchema,
  source_datasets: list,
) -> None:
  source_dataset_ids = [
    source_dataset.pk
    for source_dataset in source_datasets
  ]
  source_system_ids = [
    source_dataset.source_system_id
    for source_dataset in source_datasets
  ]

  list(
    SourceDataset.objects
    .select_for_update()
    .filter(pk__in=source_dataset_ids)
    .values_list("pk", flat=True)
  )
  list(
    SourceColumn.objects
    .select_for_update()
    .filter(source_dataset_id__in=source_dataset_ids)
    .values_list("pk", flat=True)
  )
  list(
    System.objects
    .select_for_update()
    .filter(pk__in=source_system_ids)
    .values_list("pk", flat=True)
  )

  memberships = list(
    SourceDatasetGroupMembership.objects
    .select_for_update()
    .filter(source_dataset_id__in=source_dataset_ids)
    .values_list("pk", "group_id")
  )
  group_ids = [group_id for _pk, group_id in memberships]
  list(
    SourceDatasetGroup.objects
    .select_for_update()
    .filter(pk__in=group_ids)
    .values_list("pk", flat=True)
  )

  schema_short_names = {target_schema.short_name}
  if target_schema.short_name in {"stage", "rawcore"}:
    schema_short_names.add("raw")
  if target_schema.short_name == "rawcore":
    schema_short_names.add("stage")

  target_schema_ids = list(
    TargetSchema.objects
    .select_for_update()
    .filter(short_name__in=schema_short_names)
    .values_list("pk", flat=True)
  )
  target_dataset_ids = list(
    TargetDataset.objects
    .select_for_update()
    .filter(target_schema_id__in=target_schema_ids)
    .values_list("pk", flat=True)
  )
  target_column_ids = list(
    TargetColumn.objects
    .select_for_update()
    .filter(target_dataset_id__in=target_dataset_ids)
    .values_list("pk", flat=True)
  )
  list(
    TargetDatasetInput.objects
    .select_for_update()
    .filter(target_dataset_id__in=target_dataset_ids)
    .values_list("pk", flat=True)
  )
  list(
    TargetColumnInput.objects
    .select_for_update()
    .filter(target_column_id__in=target_column_ids)
    .values_list("pk", flat=True)
  )

  reference_ids = list(
    TargetDatasetReference.objects
    .select_for_update()
    .filter(referencing_dataset_id__in=target_dataset_ids)
    .values_list("pk", flat=True)
  )
  reference_ids.extend(
    TargetDatasetReference.objects
    .select_for_update()
    .filter(referenced_dataset_id__in=target_dataset_ids)
    .exclude(pk__in=reference_ids)
    .values_list("pk", flat=True)
  )
  list(
    TargetDatasetReferenceComponent.objects
    .select_for_update()
    .filter(reference_id__in=reference_ids)
    .values_list("pk", flat=True)
  )


def _canonical_action(action) -> str:
  return json.dumps(
    action.to_dict(),
    sort_keys=True,
    ensure_ascii=False,
    allow_nan=False,
    separators=(",", ":"),
  )
