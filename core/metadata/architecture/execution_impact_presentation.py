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

from metadata.architecture.execution_impact import (
  ExecutionImpactDecision,
  ExecutionImpactItem,
  ExecutionImpactPlan,
  execution_impact_decision_label,
)


EXECUTION_IMPACT_ITEM_PREVIEW_LIMIT = 12

_DECISION_ORDER: tuple[ExecutionImpactDecision, ...] = (
  "REUSE",
  "REVALIDATE",
  "INCREMENTAL_EXECUTE",
  "FULL_REBUILD",
  "BLOCKED",
)

_DECISION_PRESENTATION: dict[
  ExecutionImpactDecision,
  tuple[str, str],
] = {
  "REUSE": ("text-bg-success", "bi-recycle"),
  "REVALIDATE": ("text-bg-info", "bi-check2-square"),
  "INCREMENTAL_EXECUTE": ("text-bg-primary", "bi-arrow-repeat"),
  "FULL_REBUILD": ("text-bg-warning", "bi-arrow-clockwise"),
  "BLOCKED": ("text-bg-danger", "bi-slash-circle"),
}


@dataclass(frozen=True)
class ExecutionImpactDecisionCountPresentation:
  """
  One stable decision count shown in the Execution Impact Plan summary.
  """
  decision: ExecutionImpactDecision
  label: str
  count: int
  badge_class: str
  icon: str


@dataclass(frozen=True)
class ExecutionImpactItemPresentation:
  """
  Compact read-only presentation of one dataset impact decision.
  """
  dataset_key: str
  decision: ExecutionImpactDecision
  decision_label: str
  decision_badge_class: str
  decision_icon: str
  reason_codes: tuple[str, ...]
  evidence_count: int
  propagation_count: int
  has_unavailable_required_evidence: bool


@dataclass(frozen=True)
class ExecutionImpactPlanPresentation:
  """
  Compact Architecture Control presentation of an Execution Impact Plan.
  """
  scope_key: str
  assessed_count: int
  plan_fingerprint: str
  report_fingerprint: str
  architecture_fingerprint: str
  baseline_fingerprint: str | None
  decision_counts: tuple[ExecutionImpactDecisionCountPresentation, ...]
  preview_items: tuple[ExecutionImpactItemPresentation, ...]
  remaining_items: tuple[ExecutionImpactItemPresentation, ...]

  @property
  def has_remaining_items(self) -> bool:
    """
    Return True when more dataset decisions exist than the compact preview shows.
    """
    return bool(self.remaining_items)

  @property
  def remaining_count(self) -> int:
    """
    Return the number of dataset decisions hidden behind explicit expansion.
    """
    return len(self.remaining_items)


class ExecutionImpactPresentationError(ValueError):
  """
  Raised when an Execution Impact Plan cannot be prepared for presentation.
  """


def build_execution_impact_plan_presentation(
  plan: ExecutionImpactPlan,
  *,
  preview_limit: int = EXECUTION_IMPACT_ITEM_PREVIEW_LIMIT,
  execution_dataset_keys: tuple[str, ...] | None = None,
) -> ExecutionImpactPlanPresentation:
  """
  Build a deterministic read-only presentation for Architecture Control.

  When an execution order is supplied, dataset rows follow that concrete
  controlled-execution order. The canonical Execution Impact Plan and its
  fingerprint remain unchanged.
  """
  if preview_limit < 0:
    raise ExecutionImpactPresentationError(
      "Execution Impact Plan preview limit must not be negative."
    )

  decision_counts = tuple(
    _build_decision_count_presentation(
      decision=decision,
      count=int(plan.decision_counts[decision]),
    )
    for decision in _DECISION_ORDER
  )
  ordered_plan_items = _order_items_for_presentation(
    tuple(plan.items),
    execution_dataset_keys=execution_dataset_keys,
  )
  items = tuple(
    _build_item_presentation(item)
    for item in ordered_plan_items
  )

  return ExecutionImpactPlanPresentation(
    scope_key=plan.scope_key,
    assessed_count=plan.assessed_count,
    plan_fingerprint=plan.plan_fingerprint,
    report_fingerprint=plan.report_fingerprint,
    architecture_fingerprint=plan.architecture_fingerprint,
    baseline_fingerprint=plan.baseline_fingerprint,
    decision_counts=decision_counts,
    preview_items=items[:preview_limit],
    remaining_items=items[preview_limit:],
  )


def _order_items_for_presentation(
  items: tuple[ExecutionImpactItem, ...],
  *,
  execution_dataset_keys: tuple[str, ...] | None,
) -> tuple[ExecutionImpactItem, ...]:
  """
  Order impact items by the exact controlled-execution dataset sequence.

  Without an explicit execution sequence, the canonical plan order is
  preserved for backwards-compatible standalone presentation use.
  """
  if execution_dataset_keys is None:
    return items

  normalized_keys = tuple(
    str(dataset_key or "").strip()
    for dataset_key in execution_dataset_keys
  )
  if any(not dataset_key for dataset_key in normalized_keys):
    raise ExecutionImpactPresentationError(
      "Execution Impact Plan presentation order contains an empty dataset key."
    )

  if len(normalized_keys) != len(set(normalized_keys)):
    raise ExecutionImpactPresentationError(
      "Execution Impact Plan presentation order contains duplicate dataset keys."
    )

  item_by_key = {
    item.dataset_key: item
    for item in items
  }
  plan_keys = set(item_by_key)
  execution_keys = set(normalized_keys)

  missing_keys = tuple(sorted(plan_keys - execution_keys))
  unexpected_keys = tuple(sorted(execution_keys - plan_keys))
  if missing_keys or unexpected_keys:
    details: list[str] = []
    if missing_keys:
      details.append(
        "missing from execution order: " + ", ".join(missing_keys)
      )
    if unexpected_keys:
      details.append(
        "not present in impact plan: " + ", ".join(unexpected_keys)
      )

    raise ExecutionImpactPresentationError(
      "Execution Impact Plan presentation order does not match the assessed "
      "dataset scope; "
      + "; ".join(details)
      + "."
    )

  return tuple(
    item_by_key[dataset_key]
    for dataset_key in normalized_keys
  )


def _build_decision_count_presentation(
  *,
  decision: ExecutionImpactDecision,
  count: int,
) -> ExecutionImpactDecisionCountPresentation:
  """
  Build one stable decision count presentation.
  """
  badge_class, icon = _decision_presentation(decision)
  label = execution_impact_decision_label(decision)
  return ExecutionImpactDecisionCountPresentation(
    decision=decision,
    label=label,
    count=count,
    badge_class=badge_class,
    icon=icon,
  )


def _build_item_presentation(
  item: ExecutionImpactItem,
) -> ExecutionImpactItemPresentation:
  """
  Build one compact dataset impact presentation.
  """
  badge_class, icon = _decision_presentation(item.decision)
  return ExecutionImpactItemPresentation(
    dataset_key=item.dataset_key,
    decision=item.decision,
    decision_label=item.decision_label,
    decision_badge_class=badge_class,
    decision_icon=icon,
    reason_codes=tuple(item.reason_codes),
    evidence_count=len(item.evidence),
    propagation_count=len(item.propagations),
    has_unavailable_required_evidence=(
      item.has_unavailable_required_evidence
    ),
  )


def _decision_presentation(
  decision: ExecutionImpactDecision,
) -> tuple[str, str]:
  """
  Return the stable badge class and icon for one decision.
  """
  try:
    return _DECISION_PRESENTATION[decision]
  except KeyError as exc:
    raise ExecutionImpactPresentationError(
      f"Unsupported Execution Impact Plan decision: {decision}"
    ) from exc
