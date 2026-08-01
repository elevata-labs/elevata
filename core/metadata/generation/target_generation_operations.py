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
from pathlib import Path
from typing import Any

from metadata.generation.security import get_runtime_pepper
from metadata.generation.target_generation_control import (
  TargetGenerationApprovalArtifact,
  TargetGenerationApprovalCheckResult,
  TargetGenerationApprovalStore,
  TargetGenerationControlError,
  TargetGenerationReview,
  build_target_generation_approval,
  build_target_generation_review,
  check_target_generation_approval,
)
from metadata.generation.target_generation_guarded_apply import (
  TargetGenerationApplyResult,
  TargetGenerationPlanApplyError,
)
from metadata.generation.target_generation_plan import TargetGenerationPlan
from metadata.generation.target_generation_service import TargetGenerationService


CONTROLLED_GENERATION_SCHEMA_SHORT_NAMES = ("raw", "stage", "rawcore")
TARGET_GENERATION_ACTION_PREVIEW_LIMIT = 100


class TargetGenerationOperationsError(ValueError):
  """Raised when the UI-oriented Target Generation workflow is invalid."""


@dataclass(frozen=True)
class TargetGenerationSourcePresentation:
  """Human-readable source label with its stable internal key."""

  source_key: str
  label: str


@dataclass(frozen=True)
class TargetGenerationDatasetImpactPresentation:
  """Human-readable Source-to-Target impact for Architecture Control."""

  dataset_key: str
  target_dataset_label: str
  sources: tuple[TargetGenerationSourcePresentation, ...]
  action_count: int
  change_classification: str
  effect_origins: tuple[str, ...]


@dataclass(frozen=True)
class TargetGenerationActionPresentation:
  """Presentation-safe representation of one planned generation action."""

  action_type: str
  action_type_label: str
  object_label: str
  dataset_key: str
  object_key: str
  source_keys: tuple[str, ...]
  change_classification: str
  effect_origin: str
  reason: str
  before_json: str
  after_json: str


@dataclass(frozen=True)
class TargetGenerationOperationsContext:
  """Current schema-scoped generation plan, review and approval state."""

  schema_short_name: str
  plan: TargetGenerationPlan
  review: TargetGenerationReview
  approval: TargetGenerationApprovalArtifact | None
  approval_check: TargetGenerationApprovalCheckResult
  eligible_source_count: int
  upstream_pending_schema_short_name: str | None
  dataset_impact_preview: tuple[TargetGenerationDatasetImpactPresentation, ...]
  action_preview: tuple[TargetGenerationActionPresentation, ...]
  action_preview_limit: int
  approval_store: TargetGenerationApprovalStore

  @property
  def action_counts(self) -> dict[str, int]:
    """Return action counts for template presentation."""
    return dict(self.review.action_counts)

  @property
  def classification_counts(self) -> dict[str, int]:
    """Return change-classification counts for template presentation."""
    return dict(self.review.classification_counts)

  @property
  def effect_origin_counts(self) -> dict[str, int]:
    """Return effect-origin counts for template presentation."""
    return dict(self.review.effect_origin_counts)

  @property
  def source_count(self) -> int:
    """Return the number of directly impacted sources."""
    return len(self.review.source_impacts)

  @property
  def target_dataset_count(self) -> int:
    """Return the number of impacted target datasets."""
    return len(self.review.dataset_impacts)

  @property
  def has_actions(self) -> bool:
    """Return whether the current plan contains metadata mutations."""
    return self.review.action_count > 0

  @property
  def approval_required(self) -> bool:
    """Return whether breaking generation intent requires approval in the UI."""
    return self.review.has_breaking_changes

  @property
  def approval_valid(self) -> bool:
    """Return whether an exact stored Generation Approval is available."""
    return bool(self.approval_check.is_valid)

  @property
  def approval_invalid(self) -> bool:
    """Return whether a stored artifact exists but fails exact validation."""
    return self.approval is not None and not self.approval_valid

  @property
  def upstream_ready(self) -> bool:
    """Return whether all earlier generated layers are currently converged."""
    return self.upstream_pending_schema_short_name is None

  @property
  def can_approve(self) -> bool:
    """Return whether a new approval can be created for the current review."""
    return (
      self.has_actions
      and self.upstream_ready
      and not self.approval_valid
    )

  @property
  def can_apply(self) -> bool:
    """Return whether guarded apply is currently available in the UI."""
    return (
      self.has_actions
      and self.upstream_ready
      and not self.approval_invalid
      and (
        not self.approval_required
        or self.approval_valid
      )
    )

  @property
  def remaining_action_count(self) -> int:
    """Return the number of actions omitted from the compact UI preview."""
    return max(0, self.review.action_count - len(self.action_preview))

  @property
  def approval_directory(self) -> Path:
    """Return the context-scoped Generation Approval directory."""
    return self.approval_store.base_path


@dataclass(frozen=True)
class TargetGenerationApprovalOperationResult:
  """Result of creating one UI-driven Generation Approval."""

  context: TargetGenerationOperationsContext
  artifact: TargetGenerationApprovalArtifact
  approval_path: Path


@dataclass(frozen=True)
class TargetGenerationApplyOperationResult:
  """Result of one UI-driven guarded apply plus its residual review."""

  context: TargetGenerationOperationsContext
  apply_result: TargetGenerationApplyResult
  residual_context: TargetGenerationOperationsContext


@dataclass(frozen=True)
class TargetGenerationLayerSequenceItem:
  """Read-only status for one layer in the controlled generation sequence."""

  schema_short_name: str
  display_name: str
  status: str
  status_label: str
  badge_class: str
  action_count: int | None
  breaking_action_count: int
  is_selected: bool
  is_actionable: bool
  blocked_by_schema_short_name: str | None = None
  error_message: str | None = None


@dataclass(frozen=True)
class TargetGenerationSequenceContext:
  """Ordered RAW-to-RAWCORE guidance for controlled target generation."""

  items: tuple[TargetGenerationLayerSequenceItem, ...]
  selected_schema_short_name: str
  next_actionable_schema_short_name: str | None

  @property
  def next_actionable_item(self) -> TargetGenerationLayerSequenceItem | None:
    """Return the first layer whose current plan can be reviewed safely."""
    for item in self.items:
      if item.schema_short_name == self.next_actionable_schema_short_name:
        return item
    return None

  @property
  def all_up_to_date(self) -> bool:
    """Return whether every available generated layer is converged."""
    return bool(self.items) and all(
      item.status == "up_to_date"
      for item in self.items
    )

  @property
  def selected_item(self) -> TargetGenerationLayerSequenceItem | None:
    """Return the sequence item matching the current schema scope."""
    for item in self.items:
      if item.is_selected:
        return item
    return None

  @property
  def continuation_required(self) -> bool:
    """Return whether another layer is the next controlled generation step."""
    return bool(
      self.selected_schema_short_name
      and self.next_actionable_schema_short_name
      and self.next_actionable_schema_short_name
      != self.selected_schema_short_name
    )


def build_target_generation_sequence_context(
  *,
  selected_schema_short_name: str = "",
  actor=None,
  service: TargetGenerationService | None = None,
  selected_context: TargetGenerationOperationsContext | None = None,
) -> TargetGenerationSequenceContext:
  """Build ordered guidance without treating downstream previews as final."""
  selected_schema = str(selected_schema_short_name or "").strip()
  if (
    selected_schema
    and selected_schema not in CONTROLLED_GENERATION_SCHEMA_SHORT_NAMES
  ):
    selected_schema = ""

  generation_service = service or TargetGenerationService(
    pepper=get_runtime_pepper(),
    actor=actor,
  )
  schema_map = _controlled_target_schema_map(generation_service)
  items = []
  blocking_schema = None
  next_actionable_schema = None

  for schema_short_name in CONTROLLED_GENERATION_SCHEMA_SHORT_NAMES:
    schema = schema_map.get(schema_short_name)
    display_name = schema_short_name.upper()
    is_selected = schema_short_name == selected_schema

    if schema is None:
      items.append(TargetGenerationLayerSequenceItem(
        schema_short_name=schema_short_name,
        display_name=display_name,
        status="unavailable",
        status_label="Not configured",
        badge_class="text-bg-secondary",
        action_count=None,
        breaking_action_count=0,
        is_selected=is_selected,
        is_actionable=False,
        error_message=(
          f"TargetSchema '{schema_short_name}' is not enabled for generation."
        ),
      ))
      continue

    if blocking_schema is not None:
      items.append(TargetGenerationLayerSequenceItem(
        schema_short_name=schema_short_name,
        display_name=display_name,
        status="recalculate",
        status_label=f"Recalculate after {blocking_schema.upper()}",
        badge_class="text-bg-secondary",
        action_count=None,
        breaking_action_count=0,
        is_selected=is_selected,
        is_actionable=False,
        blocked_by_schema_short_name=blocking_schema,
      ))
      continue

    try:
      if (
        selected_context is not None
        and selected_context.schema_short_name == schema_short_name
      ):
        review = selected_context.review
      else:
        _, review = _build_plan_and_review(generation_service, schema)
    except TargetGenerationOperationsError as exc:
      blocking_schema = schema_short_name
      items.append(TargetGenerationLayerSequenceItem(
        schema_short_name=schema_short_name,
        display_name=display_name,
        status="error",
        status_label="Preview failed",
        badge_class="text-bg-danger",
        action_count=None,
        breaking_action_count=0,
        is_selected=is_selected,
        is_actionable=False,
        error_message=str(exc),
      ))
      continue

    classification_counts = dict(review.classification_counts)
    if review.action_count:
      blocking_schema = schema_short_name
      next_actionable_schema = schema_short_name
      items.append(TargetGenerationLayerSequenceItem(
        schema_short_name=schema_short_name,
        display_name=display_name,
        status="review",
        status_label=(
          f"Review {review.action_count} action"
          if review.action_count == 1
          else f"Review {review.action_count} actions"
        ),
        badge_class="text-bg-info",
        action_count=review.action_count,
        breaking_action_count=classification_counts.get("BREAKING", 0),
        is_selected=is_selected,
        is_actionable=True,
      ))
      continue

    items.append(TargetGenerationLayerSequenceItem(
      schema_short_name=schema_short_name,
      display_name=display_name,
      status="up_to_date",
      status_label="Up to date",
      badge_class="text-bg-success",
      action_count=0,
      breaking_action_count=0,
      is_selected=is_selected,
      is_actionable=False,
    ))

  return TargetGenerationSequenceContext(
    items=tuple(items),
    selected_schema_short_name=selected_schema,
    next_actionable_schema_short_name=next_actionable_schema,
  )


def build_target_generation_operations_context(
  schema_short_name: str,
  *,
  actor=None,
  approval_store: TargetGenerationApprovalStore | None = None,
  service: TargetGenerationService | None = None,
) -> TargetGenerationOperationsContext:
  """Build the current controlled generation context for one target schema."""
  normalized_schema = _controlled_schema_short_name(schema_short_name)
  generation_service = service or TargetGenerationService(
    pepper=get_runtime_pepper(),
    actor=actor,
  )
  schema_map = _controlled_target_schema_map(generation_service)
  target_schema = _resolve_target_schema(
    generation_service,
    normalized_schema,
    schema_map=schema_map,
  )
  upstream_pending_schema = _first_pending_upstream_schema(
    generation_service,
    normalized_schema,
    schema_map=schema_map,
  )
  eligible_sources, plan, review = _build_plan_review_and_sources(
    generation_service,
    target_schema,
  )

  store = approval_store or TargetGenerationApprovalStore()
  try:
    approval = store.load_for_review_fingerprint(review.review_fingerprint)
  except TargetGenerationControlError as exc:
    raise TargetGenerationOperationsError(str(exc)) from exc

  if approval is None:
    approval_check = TargetGenerationApprovalCheckResult(
      is_valid=False,
      status="missing",
      message=(
        "No Generation Approval exists for the current Target Generation Review."
      ),
      plan_fingerprint=plan.plan_fingerprint,
      review_fingerprint=review.review_fingerprint,
    )
  else:
    approval_check = check_target_generation_approval(
      review=review,
      approval=approval,
    )

  source_labels = _build_source_label_map(
    plan.source_dataset_keys,
    eligible_sources,
  )
  dataset_impact_preview = _build_dataset_impact_preview(
    plan,
    review=review,
    target_schema=target_schema,
    source_labels=source_labels,
  )
  target_dataset_labels = {
    impact.dataset_key: impact.target_dataset_label
    for impact in dataset_impact_preview
  }
  return TargetGenerationOperationsContext(
    schema_short_name=normalized_schema,
    plan=plan,
    review=review,
    approval=approval,
    approval_check=approval_check,
    eligible_source_count=len(eligible_sources),
    upstream_pending_schema_short_name=upstream_pending_schema,
    dataset_impact_preview=dataset_impact_preview,
    action_preview=_build_action_preview(
      plan,
      target_dataset_labels=target_dataset_labels,
    ),
    action_preview_limit=TARGET_GENERATION_ACTION_PREVIEW_LIMIT,
    approval_store=store,
  )


def create_target_generation_operations_approval(
  schema_short_name: str,
  *,
  expected_review_fingerprint: str,
  approved_by: str,
  note: str = "",
  actor=None,
  approval_store: TargetGenerationApprovalStore | None = None,
  service: TargetGenerationService | None = None,
) -> TargetGenerationApprovalOperationResult:
  """Create and store approval for the exact review shown in the UI."""
  context = build_target_generation_operations_context(
    schema_short_name,
    actor=actor,
    approval_store=approval_store,
    service=service,
  )
  _require_expected_review(
    context,
    expected_review_fingerprint=expected_review_fingerprint,
  )

  if context.upstream_pending_schema_short_name:
    raise TargetGenerationOperationsError(
      f"{context.upstream_pending_schema_short_name.upper()} generation must "
      "converge before this downstream review can be approved."
    )
  if not context.has_actions:
    raise TargetGenerationOperationsError(
      "No Target Generation actions are present for this schema."
    )
  if context.approval_valid:
    raise TargetGenerationOperationsError(
      "A matching Generation Approval already exists for this review."
    )

  try:
    artifact = build_target_generation_approval(
      review=context.review,
      decided_by=approved_by,
      note=note,
    )
    approval_path = context.approval_store.save(artifact)
  except TargetGenerationControlError as exc:
    raise TargetGenerationOperationsError(str(exc)) from exc

  return TargetGenerationApprovalOperationResult(
    context=context,
    artifact=artifact,
    approval_path=approval_path,
  )


def check_target_generation_operations_approval(
  schema_short_name: str,
  *,
  expected_review_fingerprint: str,
  actor=None,
  approval_store: TargetGenerationApprovalStore | None = None,
  service: TargetGenerationService | None = None,
) -> TargetGenerationApprovalCheckResult:
  """Check the stored approval against the exact review shown in the UI."""
  context = build_target_generation_operations_context(
    schema_short_name,
    actor=actor,
    approval_store=approval_store,
    service=service,
  )
  _require_expected_review(
    context,
    expected_review_fingerprint=expected_review_fingerprint,
  )
  return context.approval_check


def apply_target_generation_operations_plan(
  schema_short_name: str,
  *,
  expected_review_fingerprint: str,
  actor=None,
  approval_store: TargetGenerationApprovalStore | None = None,
  service: TargetGenerationService | None = None,
) -> TargetGenerationApplyOperationResult:
  """Apply the exact reviewed plan and rebuild the residual UI context."""
  generation_service = service or TargetGenerationService(
    pepper=get_runtime_pepper(),
    actor=actor,
  )
  context = build_target_generation_operations_context(
    schema_short_name,
    actor=actor,
    approval_store=approval_store,
    service=generation_service,
  )
  _require_expected_review(
    context,
    expected_review_fingerprint=expected_review_fingerprint,
  )

  if context.upstream_pending_schema_short_name:
    raise TargetGenerationOperationsError(
      f"{context.upstream_pending_schema_short_name.upper()} generation must "
      "converge before this downstream plan can be applied."
    )
  if not context.has_actions:
    raise TargetGenerationOperationsError(
      "No Target Generation actions are present for this schema."
    )
  if context.approval_invalid:
    raise TargetGenerationOperationsError(
      "The stored Generation Approval is invalid for the current review. "
      "Create a new exact approval before guarded apply."
    )
  if context.approval_required and not context.approval_valid:
    raise TargetGenerationOperationsError(
      "A matching Generation Approval is required for breaking generation changes."
    )

  approval = context.approval if context.approval_valid else None
  try:
    apply_result = generation_service.apply_plan(
      context.plan,
      approval=approval,
      require_approval=context.approval_required,
    )
  except (TargetGenerationPlanApplyError, TargetGenerationControlError) as exc:
    raise TargetGenerationOperationsError(str(exc)) from exc

  residual_context = build_target_generation_operations_context(
    schema_short_name,
    actor=actor,
    approval_store=context.approval_store,
    service=generation_service,
  )

  if (
    apply_result.residual_plan_fingerprint
    != residual_context.plan.plan_fingerprint
  ):
    raise TargetGenerationOperationsError(
      "Guarded apply result does not match the current residual generation plan."
    )

  return TargetGenerationApplyOperationResult(
    context=context,
    apply_result=apply_result,
    residual_context=residual_context,
  )


def _controlled_schema_short_name(value: str) -> str:
  schema_short_name = str(value or "").strip()
  if schema_short_name not in CONTROLLED_GENERATION_SCHEMA_SHORT_NAMES:
    supported = ", ".join(CONTROLLED_GENERATION_SCHEMA_SHORT_NAMES)
    raise TargetGenerationOperationsError(
      "Controlled Target Generation requires an explicit generated schema "
      f"scope ({supported})."
    )
  return schema_short_name


def _controlled_target_schema_map(service) -> dict[str, Any]:
  """Return configured generated schemas indexed by their stable short name."""
  schema_map = {}
  for schema in service.get_target_schemas_in_scope():
    schema_short_name = str(getattr(schema, "short_name", "") or "").strip()
    if schema_short_name not in CONTROLLED_GENERATION_SCHEMA_SHORT_NAMES:
      continue
    if schema_short_name in schema_map:
      raise TargetGenerationOperationsError(
        f"TargetSchema '{schema_short_name}' is not unique in generation scope."
      )
    schema_map[schema_short_name] = schema
  return schema_map


def _resolve_target_schema(
  service,
  schema_short_name: str,
  *,
  schema_map: dict[str, Any] | None = None,
):
  schemas = schema_map or _controlled_target_schema_map(service)
  target_schema = schemas.get(schema_short_name)
  if target_schema is None:
    raise TargetGenerationOperationsError(
      f"TargetSchema '{schema_short_name}' is not available for target generation."
    )
  return target_schema


def _build_plan_review_and_sources(service, target_schema):
  """Build one read-only schema plan and its Architecture Control review."""
  eligible_sources = tuple(
    service.get_eligible_source_datasets_for_schema(target_schema)
  )
  try:
    plan = service.build_plan(
      eligible_sources,
      target_schema,
      reconcile_lifecycle=True,
    )
    review = build_target_generation_review(plan)
  except (TargetGenerationControlError, ValueError) as exc:
    raise TargetGenerationOperationsError(str(exc)) from exc
  return eligible_sources, plan, review


def _build_plan_and_review(service, target_schema):
  """Build one read-only schema plan and review without returning sources."""
  _, plan, review = _build_plan_review_and_sources(service, target_schema)
  return plan, review


def _first_pending_upstream_schema(
  service,
  schema_short_name: str,
  *,
  schema_map: dict[str, Any],
) -> str | None:
  """Return the first earlier generated layer with unapplied plan actions."""
  for candidate in CONTROLLED_GENERATION_SCHEMA_SHORT_NAMES:
    if candidate == schema_short_name:
      break
    target_schema = schema_map.get(candidate)
    if target_schema is None:
      continue
    _, review = _build_plan_and_review(service, target_schema)
    if review.action_count:
      return candidate
  return None


def _require_expected_review(
  context: TargetGenerationOperationsContext,
  *,
  expected_review_fingerprint: str,
) -> None:
  expected = str(expected_review_fingerprint or "").strip()
  if not expected:
    raise TargetGenerationOperationsError(
      "Target Generation review fingerprint is required. Refresh the preview."
    )
  if expected != context.review.review_fingerprint:
    raise TargetGenerationOperationsError(
      "Target Generation preview changed. Review the refreshed plan before "
      "approving or applying it."
    )


def _build_source_label_map(
  source_keys: tuple[str, ...],
  eligible_sources: tuple[Any, ...],
) -> dict[str, str]:
  labels = {
    _source_dataset_key(source): _source_dataset_label(source)
    for source in eligible_sources
    if getattr(source, "pk", None) is not None
  }

  missing_ids = []
  for source_key in source_keys:
    if source_key in labels:
      continue
    source_id = _source_dataset_id(source_key)
    if source_id is not None:
      missing_ids.append(source_id)
  if missing_ids:
    try:
      from metadata.models import SourceDataset

      for source in (
        SourceDataset.objects
        .select_related("source_system")
        .filter(pk__in=missing_ids)
      ):
        labels[_source_dataset_key(source)] = _source_dataset_label(source)
    except Exception:
      pass

  return {
    source_key: labels.get(source_key, source_key)
    for source_key in source_keys
  }


def _build_dataset_impact_preview(
  plan: TargetGenerationPlan,
  *,
  review: TargetGenerationReview,
  target_schema: Any,
  source_labels: dict[str, str],
) -> tuple[TargetGenerationDatasetImpactPresentation, ...]:
  schema_short_name = str(
    getattr(target_schema, "short_name", "")
    or ""
  ).strip()
  target_names = {}
  for action in plan.actions:
    payload = action.to_dict()
    target_name = _target_dataset_name_from_action(payload)
    if target_name:
      target_names[payload["dataset_key"]] = target_name

  manager = getattr(target_schema, "target_datasets", None)
  rows = []
  for impact in review.dataset_impacts:
    target_name = target_names.get(impact.dataset_key, "")
    if not target_name and manager is not None:
      target_dataset = _resolve_current_target_dataset(
        manager,
        dataset_key=impact.dataset_key,
        schema_short_name=schema_short_name,
      )
      target_name = str(
        getattr(target_dataset, "target_dataset_name", None)
        or ""
      ).strip()

    rows.append(
      TargetGenerationDatasetImpactPresentation(
        dataset_key=impact.dataset_key,
        target_dataset_label=(
          _target_dataset_label(schema_short_name, target_name)
          if target_name
          else f"{schema_short_name} target dataset"
        ),
        sources=tuple(
          TargetGenerationSourcePresentation(
            source_key=source_key,
            label=(
              source_labels.get(source_key)
              or "Source dataset name unavailable"
            ),
          )
          for source_key in impact.source_keys
        ),
        action_count=impact.action_count,
        change_classification=impact.change_classification,
        effect_origins=tuple(impact.effect_origins),
      )
    )
  return tuple(rows)


def _resolve_current_target_dataset(
  manager: Any,
  *,
  dataset_key: str,
  schema_short_name: str,
) -> Any | None:
  """Resolve the existing TargetDataset represented by one plan dataset key."""
  lineage_key, is_hist = _generated_dataset_identity(
    dataset_key,
    schema_short_name=schema_short_name,
  )
  try:
    if lineage_key is not None:
      queryset = manager.filter(lineage_key=lineage_key)
      if is_hist:
        queryset = queryset.filter(target_dataset_name__endswith="_hist")
      else:
        queryset = queryset.exclude(target_dataset_name__endswith="_hist")
      return queryset.first()

    target_dataset_id = _target_dataset_id_from_key(
      dataset_key,
      schema_short_name=schema_short_name,
    )
    if target_dataset_id is not None:
      return manager.filter(pk=target_dataset_id).first()
  except Exception:
    return None
  return None


def _generated_dataset_identity(
  dataset_key: str,
  *,
  schema_short_name: str,
) -> tuple[str | None, bool]:
  """Return lineage key and history flag encoded in a generated dataset key."""
  prefix = f"{schema_short_name}:"
  value = str(dataset_key or "")
  if not value.startswith(prefix):
    return None, False

  remainder = value[len(prefix):]
  for suffix, is_hist in ((":hist", True), (":base", False)):
    if remainder.endswith(suffix):
      lineage_key = remainder[:-len(suffix)]
      return (lineage_key or None), is_hist
  return None, False


def _target_dataset_id_from_key(
  dataset_key: str,
  *,
  schema_short_name: str,
) -> int | None:
  """Return the database identifier encoded in a non-generated dataset key."""
  prefix = f"{schema_short_name}:target_dataset:"
  value = str(dataset_key or "")
  if not value.startswith(prefix):
    return None
  try:
    return int(value[len(prefix):])
  except (TypeError, ValueError):
    return None


def _build_action_preview(
  plan: TargetGenerationPlan,
  *,
  target_dataset_labels: dict[str, str],
) -> tuple[TargetGenerationActionPresentation, ...]:
  payloads = tuple(action.to_dict() for action in plan.actions)
  target_column_names = {
    payload["object_key"]: target_column_name
    for payload in payloads
    if (target_column_name := _target_column_name_from_action(payload))
  }

  rows: list[TargetGenerationActionPresentation] = []
  for payload in payloads[:TARGET_GENERATION_ACTION_PREVIEW_LIMIT]:
    action_type = payload["action_type"]
    dataset_key = payload["dataset_key"]
    target_dataset_label = (
      target_dataset_labels.get(dataset_key)
      or "Target dataset"
    )
    rows.append(
      TargetGenerationActionPresentation(
        action_type=action_type,
        action_type_label=_action_type_label(action_type),
        object_label=_action_object_label(
          payload,
          target_dataset_label=target_dataset_label,
          target_column_name=target_column_names.get(
            payload["object_key"],
            "",
          ),
        ),
        dataset_key=dataset_key,
        object_key=payload["object_key"],
        source_keys=tuple(payload["source_keys"]),
        change_classification=payload["change_classification"],
        effect_origin=payload["effect_origin"],
        reason=payload["reason"],
        before_json=_render_action_state(payload.get("before")),
        after_json=_render_action_state(payload.get("after")),
      )
    )
  return tuple(rows)


def _action_type_label(action_type: str) -> str:
  """Return a concise human-readable label for one plan action type."""
  labels = {
    "CREATE_TARGET_DATASET": "Create target dataset",
    "UPDATE_TARGET_DATASET": "Update target dataset",
    "RETIRE_TARGET_DATASET": "Retire target dataset",
    "REACTIVATE_TARGET_DATASET": "Reactivate target dataset",
    "SYNC_TARGET_DATASET_INPUTS": "Synchronize dataset inputs",
    "CREATE_TARGET_COLUMN": "Create target column",
    "UPDATE_TARGET_COLUMN": "Update target column",
    "RETIRE_TARGET_COLUMN": "Retire target column",
    "REACTIVATE_TARGET_COLUMN": "Reactivate target column",
    "SYNC_TARGET_COLUMN_INPUTS": "Synchronize column inputs",
  }
  return labels.get(
    action_type,
    str(action_type or "Planned generation action")
      .replace("_", " ")
      .strip()
      .title(),
  )


def _action_object_label(
  payload: dict[str, Any],
  *,
  target_dataset_label: str,
  target_column_name: str,
) -> str:
  """Return the business-facing object affected by one plan action."""
  action_type = str(payload.get("action_type") or "")
  if "TARGET_COLUMN" in action_type:
    if target_column_name:
      return f"{target_dataset_label}.{target_column_name}"
    return f"Column in {target_dataset_label}"
  if action_type == "SYNC_TARGET_DATASET_INPUTS":
    return f"Inputs of {target_dataset_label}"
  return target_dataset_label


def _target_column_name_from_action(payload: dict[str, Any]) -> str:
  """Return the target column name carried by an action state, if available."""
  for state_name in ("after", "before"):
    state = payload.get(state_name)
    if not isinstance(state, dict):
      continue
    target_column_name = str(
      state.get("target_column_name")
      or ""
    ).strip()
    if target_column_name:
      return target_column_name
  return ""


def _source_dataset_key(source: Any) -> str:
  return f"source_dataset:{getattr(source, 'pk')}"


def _source_dataset_id(source_key: str) -> int | None:
  prefix = "source_dataset:"
  if not str(source_key).startswith(prefix):
    return None
  try:
    return int(str(source_key)[len(prefix):])
  except (TypeError, ValueError):
    return None


def _source_dataset_label(source: Any) -> str:
  source_system = getattr(source, "source_system", None)
  system_short_name = str(
    getattr(source_system, "short_name", None)
    or source_system
    or ""
  ).strip()
  schema_name = str(getattr(source, "schema_name", None) or "").strip()
  dataset_name = str(
    getattr(source, "source_dataset_name", None)
    or ""
  ).strip()

  qualified_dataset_name = ".".join(
    value
    for value in (schema_name, dataset_name)
    if value
  )
  if system_short_name and qualified_dataset_name:
    return f"{system_short_name} · {qualified_dataset_name}"
  return qualified_dataset_name or system_short_name or _source_dataset_key(source)


def _target_dataset_name_from_action(payload: dict[str, Any]) -> str:
  for state_name in ("after", "before"):
    state = payload.get(state_name)
    if isinstance(state, dict):
      target_name = str(state.get("target_dataset_name") or "").strip()
      if target_name:
        return target_name
  return ""


def _target_dataset_label(schema_short_name: str, target_name: str) -> str:
  return ".".join(
    value
    for value in (schema_short_name, str(target_name or "").strip())
    if value
  )


def _render_action_state(value: Any) -> str:
  if value is None:
    return "–"
  return json.dumps(
    value,
    ensure_ascii=False,
    sort_keys=True,
    indent=2,
  )
