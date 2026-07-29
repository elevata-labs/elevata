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
from typing import Any, Literal


ArchitectureReviewBriefingLevel = Literal[
  "success",
  "info",
  "warning",
  "danger",
  "secondary",
]

_ATTENTION_LEVELS = {"warning", "danger"}
_DETAIL_PREVIEW_LIMIT = 6
_DESTRUCTIVE_TOKENS = (
  "drop",
  "delete",
  "remove",
  "truncate",
  "destructive",
)


@dataclass(frozen=True)
class ArchitectureReviewBriefingSignal:
  """
  One deterministic reviewer-facing signal inside an Architecture Review Briefing.
  """
  title: str
  message: str
  level: ArchitectureReviewBriefingLevel
  badge_class: str
  icon: str
  details: tuple[str, ...] = ()

  @property
  def needs_attention(self) -> bool:
    """
    Return True when the signal should attract explicit reviewer attention.
    """
    return self.level in _ATTENTION_LEVELS
  
  @property
  def detail_count(self) -> int:
    """Return the number of detail lines attached to this signal."""
    return len(self.details)

  @property
  def preview_details(self) -> tuple[str, ...]:
    """Return the detail lines shown before explicit expansion."""
    return self.details[:_DETAIL_PREVIEW_LIMIT]

  @property
  def remaining_details(self) -> tuple[str, ...]:
    """Return detail lines hidden behind explicit expansion."""
    return self.details[_DETAIL_PREVIEW_LIMIT:]

  @property
  def has_remaining_details(self) -> bool:
    """Return True when more detail lines exist than the compact preview shows."""
    return bool(self.remaining_details)

  @property
  def remaining_detail_count(self) -> int:
    """Return the number of detail lines hidden behind expansion."""
    return len(self.remaining_details)


@dataclass(frozen=True)
class ArchitectureReviewBriefingSection:
  """
  Group of related Architecture Review Briefing signals.
  """
  key: str
  title: str
  description: str
  signals: tuple[ArchitectureReviewBriefingSignal, ...]

  @property
  def attention_count(self) -> int:
    """
    Return the number of attention signals in this section.
    """
    return sum(1 for signal in self.signals if signal.needs_attention)


@dataclass(frozen=True)
class ArchitectureReviewBriefing:
  """
  Read-only Architecture Control briefing for reviewers.
  """
  scope_key: str
  scope_label: str
  scope_mode: str
  report_fingerprint: str
  review_status: str
  review_label: str
  review_badge_class: str
  review_icon: str
  sections: tuple[ArchitectureReviewBriefingSection, ...]

  @property
  def attention_count(self) -> int:
    """
    Return the total number of attention signals in the briefing.
    """
    return sum(section.attention_count for section in self.sections)

  @property
  def has_attention(self) -> bool:
    """
    Return True when the briefing contains warning or danger signals.
    """
    return self.attention_count > 0



def build_architecture_review_briefing(
  *,
  control_context: Any,
  scope: Any | None = None,
  execution_preview: Any | None = None,
  execution_preview_error: str | None = None,
) -> ArchitectureReviewBriefing:
  """
  Build a deterministic read-only reviewer briefing from Architecture Control signals.
  """
  selected_scope = scope or getattr(control_context, "scope", None)
  report = control_context.report
  review_status = control_context.review_status
  payload = _report_payload(report)
  scope_payload = _dict_value(payload, "scope")
  dataset_keys = _tuple_value(scope_payload.get("dataset_keys"))
  report_fingerprint = _str_value(
    getattr(report, "report_fingerprint", None) or payload.get("report_fingerprint")
  )
  status = _str_value(getattr(review_status, "status", "unknown"))
  has_changes = bool(getattr(report, "has_changes", True))
  is_blocked = bool(getattr(report, "is_blocked", False))
  destructive_actions = _destructive_actions(payload.get("migration_actions"))

  sections = (
    ArchitectureReviewBriefingSection(
      key="scope_summary",
      title="Scope summary",
      description="What architecture scope this review applies to.",
      signals=(
        _signal(
          title="Scope",
          message=(
            f"{_scope_mode_label(getattr(selected_scope, 'mode', ''))} scope: "
            f"{_str_value(getattr(selected_scope, 'label', 'Architecture scope'))}"
          ),
          level="info",
          icon="bi-bounding-box",
        ),
        _signal(
          title="Datasets in scope",
          message=(
            f"{len(dataset_keys)} dataset(s) are represented by this report scope."
          ),
          level="info",
          icon="bi-diagram-3",
          details=_limited_details(dataset_keys),
        ),
        _signal(
          title="Report fingerprint",
          message=report_fingerprint or "No report fingerprint is available.",
          level="secondary",
          icon="bi-fingerprint",
        ),
      ),
    ),
    ArchitectureReviewBriefingSection(
      key="review_state",
      title="Review state",
      description="Whether the selected report scope is approved, pending, drifted or blocked.",
      signals=_review_state_signals(
        report=report,
        review_status=review_status,
        status=status,
        has_changes=has_changes,
        is_blocked=is_blocked,
      ),
    ),
    ArchitectureReviewBriefingSection(
      key="change_summary",
      title="Change summary",
      description="What structural change volume the reviewer should understand.",
      signals=_change_summary_signals(
        payload=payload,
        has_changes=has_changes,
      ),
    ),
    ArchitectureReviewBriefingSection(
      key="policy_attention",
      title="Policy attention",
      description="Whether policy decisions need reviewer attention.",
      signals=(
        _policy_signal(
          payload=payload,
          is_blocked=is_blocked,
        ),
      ),
    ),
    ArchitectureReviewBriefingSection(
      key="destructive_attention",
      title="Destructive / blocking attention",
      description="Whether structural changes may require extra care before approval or execution.",
      signals=_destructive_signals(
        destructive_actions=destructive_actions,
        is_blocked=is_blocked,
      ),
    ),
    ArchitectureReviewBriefingSection(
      key="execution_readiness",
      title="Execution readiness",
      description="Whether the reviewed scope can proceed to controlled execution.",
      signals=(
        _execution_signal(
          execution_preview=execution_preview,
          execution_preview_error=execution_preview_error,
        ),
      ),
    ),
    ArchitectureReviewBriefingSection(
      key="suggested_reviewer_focus",
      title="Suggested reviewer focus",
      description="Deterministic focus points derived from the current Control signals.",
      signals=(
        _signal(
          title="Reviewer focus",
          message="Recommended points to inspect before approval or execution.",
          level="info",
          icon="bi-eyeglasses",
          details=_focus_details(
            status=status,
            has_changes=has_changes,
            is_blocked=is_blocked,
            destructive_actions=destructive_actions,
            payload=payload,
            execution_preview=execution_preview,
            execution_preview_error=execution_preview_error,
          ),
        ),
      ),
    ),
  )

  return ArchitectureReviewBriefing(
    scope_key=_str_value(getattr(selected_scope, "key", "")),
    scope_label=_str_value(getattr(selected_scope, "label", "Architecture scope")),
    scope_mode=_scope_mode_label(getattr(selected_scope, "mode", "")),
    report_fingerprint=report_fingerprint,
    review_status=status,
    review_label=_review_label(review_status),
    review_badge_class=_str_value(
      getattr(review_status, "badge_class", "text-bg-secondary")
    ),
    review_icon=_str_value(getattr(review_status, "icon", "bi-info-circle")),
    sections=sections,
  )


def _review_state_signals(
  *,
  report: Any,
  review_status: Any,
  status: str,
  has_changes: bool,
  is_blocked: bool,
) -> tuple[ArchitectureReviewBriefingSignal, ...]:
  """
  Return review-state signals for the briefing.
  """
  signals = [
    _signal(
      title="Review state",
      message=_str_value(
        getattr(review_status, "message", "Review status is available.")
      ),
      level=_review_level(status, has_changes=has_changes, is_blocked=is_blocked),
      icon=_str_value(getattr(review_status, "icon", "bi-shield-check")),
    )
  ]

  approval_id = _str_value(getattr(review_status, "approval_id", ""))
  if approval_id:
    signals.append(
      _signal(
        title="Approval artifact",
        message=f"Matching approval artifact: {approval_id}",
        level="success" if status == "approved" else "info",
        icon="bi-file-earmark-check",
      )
    )
  elif status == "initial_deployment":
    signals.append(
      _signal(
        title="Initial deployment evidence",
        message=(
          "Read-only physical discovery verified an empty complete managed "
          "target scope. No Approval Artifact is required."
        ),
        level="success",
        icon="bi-database-add",
      )
    )
  elif has_changes:
    signals.append(
      _signal(
        title="Approval artifact",
        message="No matching approval artifact is available for this report fingerprint.",
        level="danger" if is_blocked else "warning",
        icon="bi-file-earmark-x",
      )
    )

  return tuple(signals)


def _change_summary_signals(
  *,
  payload: dict[str, Any],
  has_changes: bool,
) -> tuple[ArchitectureReviewBriefingSignal, ...]:
  """
  Return report change-summary signals for the briefing.
  """
  summary = _dict_value(payload, "summary")
  dataset_count = _int_value(summary.get("dataset_change_count"))
  column_count = _int_value(summary.get("column_change_count"))
  migration_count = _int_value(summary.get("migration_action_count"))

  if not has_changes:
    return (
      _signal(
        title="Change summary",
        message="No architecture changes are present for this scope.",
        level="success",
        icon="bi-check-circle",
      ),
    )

  return (
    _signal(
      title="Change volume",
      message=(
        f"{dataset_count} dataset change(s), {column_count} column change(s) "
        f"and {migration_count} migration action(s) are in scope."
      ),
      level="info",
      icon="bi-list-check",
      details=(
        f"Dataset changes: {dataset_count}",
        f"Column changes: {column_count}",
        f"Migration actions: {migration_count}",
      ),
    ),
    _signal(
      title="Affected examples",
      message="Representative affected objects from the report payload.",
      level="secondary",
      icon="bi-card-list",
      details=_affected_examples(payload),
    ),
  )


def _policy_signal(
  *,
  payload: dict[str, Any],
  is_blocked: bool,
) -> ArchitectureReviewBriefingSignal:
  """
  Return the policy attention signal for the briefing.
  """
  summary = _dict_value(payload, "summary")
  policy_count = _int_value(summary.get("policy_decision_count"))
  blocking_count = _int_value(summary.get("blocking_policy_decision_count"))
  policy_decisions = _tuple_value(payload.get("policy_decisions"))
  status_counts = _policy_status_counts(policy_decisions)
  preflight_count = status_counts.get("REQUIRES_PREFLIGHT", 0)
  recognized_count = sum(
    status_counts.get(status, 0)
    for status in (
      "ALLOW",
      "METADATA_ONLY",
      "REQUIRES_PREFLIGHT",
      "BLOCKED_BY_POLICY",
    )
  )
  unclassified_count = max(policy_count - recognized_count, 0)
  details = _item_examples(policy_decisions, limit=None)

  if blocking_count > 0 or is_blocked:
    if blocking_count > 0:
      message = (
        f"{blocking_count} blocking policy decision(s) require resolution "
        "before approval or execution can proceed."
      )
    else:
      message = (
        "The report is blocked by policy and requires resolution "
        "before approval or execution can proceed."
      )
    return _signal(
      title="Blocking policy attention",
      message=message,
      level="danger",
      icon="bi-shield-exclamation",
      details=details,
    )

  if preflight_count > 0 or unclassified_count > 0:
    attention_count = preflight_count + unclassified_count
    if preflight_count > 0 and unclassified_count == 0:
      message = (
        f"{preflight_count} policy decision(s) require schema preflight before "
        f"execution. {policy_count} policy decision(s) were evaluated in total."
      )
    else:
      message = (
        f"{attention_count} of {policy_count} policy decision(s) require "
        "additional review before execution."
      )
    return _signal(
      title="Policy attention",
      message=message,
      level="warning",
      icon="bi-shield-check",
      details=details,
    )

  if policy_count > 0:
    return _signal(
      title="Policy decisions evaluated",
      message=(
        f"{policy_count} policy decision(s) were evaluated. "
        "All actions are allowed or metadata-only."
      ),
      level="success",
      icon="bi-shield-check",
      details=details,
    )

  return _signal(
    title="Policy attention",
    message="No policy decisions are reported for this scope.",
    level="success",
    icon="bi-shield-check",
  )


def _destructive_signals(
  *,
  destructive_actions: tuple[str, ...],
  is_blocked: bool,
) -> tuple[ArchitectureReviewBriefingSignal, ...]:
  """
  Return destructive and blocking report-state signals for the briefing.
  """
  signals = []
  if destructive_actions:
    signals.append(
      _signal(
        title="Destructive migration attention",
        message=(
          f"{len(destructive_actions)} potentially destructive migration "
          "action(s) are present in this report."
        ),
        level="danger",
        icon="bi-exclamation-triangle",
        details=destructive_actions[:8],
      )
    )
  else:
    signals.append(
      _signal(
        title="Destructive migration attention",
        message="No destructive migration action pattern was detected in the report payload.",
        level="success",
        icon="bi-check-circle",
      )
    )

  if is_blocked:
    signals.append(
      _signal(
        title="Blocking report state",
        message="The Architecture Change Report is blocked by policy.",
        level="danger",
        icon="bi-slash-circle",
      )
    )

  return tuple(signals)


def _execution_signal(
  *,
  execution_preview: Any | None,
  execution_preview_error: str | None,
) -> ArchitectureReviewBriefingSignal:
  """
  Return the execution-readiness signal for the briefing.
  """
  if execution_preview_error:
    return _signal(
      title="Execution preview",
      message=execution_preview_error,
      level="warning",
      icon="bi-play-circle",
    )

  if execution_preview is None:
    return _signal(
      title="Execution preview",
      message="Execution readiness is not available for this scope yet.",
      level="secondary",
      icon="bi-play-circle",
    )

  gate = execution_preview.gate
  can_execute = bool(getattr(gate, "can_execute", False))
  gate_status = _str_value(getattr(gate, "status", "unknown"))
  return _signal(
    title="Execution readiness",
    message=_str_value(getattr(gate, "message", "Execution preview is available.")),
    level=_execution_level(gate_status, can_execute=can_execute),
    icon=_str_value(getattr(gate, "icon", "bi-play-circle")),
    details=(
      f"Gate status: {gate_status}",
      f"Dependency mode: {_str_value(getattr(execution_preview, 'dependency_mode', 'unknown'))}",
      f"Execution steps: {_str_value(getattr(execution_preview, 'step_count', 0))}",
    ),
  )


def _focus_details(
  *,
  status: str,
  has_changes: bool,
  is_blocked: bool,
  destructive_actions: tuple[str, ...],
  payload: dict[str, Any],
  execution_preview: Any | None,
  execution_preview_error: str | None,
) -> tuple[str, ...]:
  """
  Return deterministic reviewer focus points for the current Control signals.
  """
  summary = _dict_value(payload, "summary")
  blocking_count = _int_value(summary.get("blocking_policy_decision_count"))
  details = []

  if not has_changes:
    details.append("Confirm that the selected scope is aligned with the persisted baseline.")
  if status == "drift":
    details.append("Review approval drift before creating or trusting an approval artifact.")
  if status == "pending":
    details.append("Review the change summary and decide whether the report should be approved.")
  if status == "initial_deployment":
    details.append(
      "Verify the complete initial execution scope before establishing the first baseline."
    )
  if status == "approved":
    details.append("Verify the execution preview before running the approved scope.")
  if is_blocked or blocking_count > 0:
    details.append("Resolve blocking policy decisions before approval or execution.")
  if destructive_actions:
    details.append("Inspect destructive migration actions and affected objects carefully.")
  if execution_preview_error:
    details.append("Resolve the execution preview issue before relying on execution readiness.")
  elif execution_preview is not None and not execution_preview.gate.can_execute:
    details.append("Use the execution gate message to resolve readiness before execution.")

  if not details:
    details.append("Review the report, approval state and execution preview for consistency.")

  return tuple(details)


def _report_payload(report: Any) -> dict[str, Any]:
  """
  Return the dictionary payload of an Architecture Change Report-shaped object.
  """
  to_dict = getattr(report, "to_dict", None)
  if callable(to_dict):
    payload = to_dict()
    if isinstance(payload, dict):
      return payload

  return {}


def _affected_examples(payload: dict[str, Any]) -> tuple[str, ...]:
  """
  Return compact examples of affected objects across report payload sections.
  """
  examples = []
  for key in ("dataset_changes", "column_changes", "migration_actions"):
    examples.extend(_item_examples(payload.get(key), limit=3))

  return tuple(dict.fromkeys(examples[:8])) or (
    "No affected object examples are listed in the report payload.",
  )


def _destructive_actions(value: Any) -> tuple[str, ...]:
  """
  Return migration action examples that look destructive by action naming.
  """
  return tuple(
    label
    for label in (_item_label(item) for item in _tuple_value(value))
    if any(token in label.lower() for token in _DESTRUCTIVE_TOKENS)
  )


def _policy_status_counts(value: tuple[Any, ...]) -> dict[str, int]:
  """Return normalized policy decision counts by status."""
  counts: dict[str, int] = {}
  for item in value:
    if not isinstance(item, dict):
      continue
    status = _str_value(item.get("status")).upper()
    if not status:
      continue
    counts[status] = counts.get(status, 0) + 1
  return counts


def _item_examples(
  value: Any,
  *,
  limit: int | None = 6,
) -> tuple[str, ...]:
  """
  Return compact text examples for report item payloads.
  """
  items = _tuple_value(value)
  if limit is not None:
    items = items[:limit]
  return tuple(
    label
    for label in (_item_label(item) for item in items)
    if label
  )


def _item_label(item: Any) -> str:
  """
  Return a compact label for a report payload item.
  """
  if not isinstance(item, dict):
    return str(item)

  parts = []
  for key in (
    "status",
    "action",
    "action_type",
    "change_type",
    "decision",
    "severity",
    "dataset_key",
    "target_dataset_key",
    "target_key",
    "column_key",
    "column_name",
    "name",
    "message",
  ):
    value = item.get(key)
    if value not in (None, ""):
      parts.append(str(value))

  return " · ".join(parts) or str(item)


def _signal(
  *,
  title: str,
  message: str,
  level: ArchitectureReviewBriefingLevel,
  icon: str,
  details: tuple[str, ...] = (),
) -> ArchitectureReviewBriefingSignal:
  """
  Return a briefing signal with the Bootstrap badge class for its level.
  """
  return ArchitectureReviewBriefingSignal(
    title=title,
    message=message,
    level=level,
    badge_class=_badge_class(level),
    icon=icon,
    details=details,
  )


def _limited_details(values: tuple[Any, ...], *, limit: int = 8) -> tuple[str, ...]:
  """
  Return a limited display tuple with an overflow marker when needed.
  """
  details = tuple(str(value) for value in values[:limit])
  if len(values) > limit:
    details += (f"… {len(values) - limit} more",)
  return details


def _review_level(
  status: str,
  *,
  has_changes: bool,
  is_blocked: bool,
) -> ArchitectureReviewBriefingLevel:
  """
  Return the briefing level for a review status.
  """
  if is_blocked or status in {"blocked", "invalid"}:
    return "danger"
  if status in {"pending", "drift"}:
    return "warning"
  if status in {"approved", "initial_deployment"} or not has_changes:
    return "success"
  return "info"


def _execution_level(
  gate_status: str,
  *,
  can_execute: bool,
) -> ArchitectureReviewBriefingLevel:
  """
  Return the briefing level for an execution gate.
  """
  if can_execute:
    return "success"
  if gate_status == "blocked_by_policy":
    return "danger"
  return "warning"


def _scope_mode_label(value: Any) -> str:
  """
  Return the reviewer-facing label for an Architecture Control scope mode.
  """
  mode = _str_value(value)
  return {
    "all": "All datasets",
    "schema": "Schema",
    "target_dataset": "Target dataset",
  }.get(mode, mode or "Architecture")


def _review_label(review_status: Any) -> str:
  """
  Return the reviewer-facing review label.
  """
  label = _str_value(getattr(review_status, "label", ""))
  return label or _str_value(getattr(review_status, "status", "unknown"))


def _badge_class(level: ArchitectureReviewBriefingLevel) -> str:
  """
  Return the Bootstrap badge class for a briefing level.
  """
  return {
    "success": "text-bg-success",
    "info": "text-bg-info",
    "warning": "text-bg-warning",
    "danger": "text-bg-danger",
    "secondary": "text-bg-secondary",
  }[level]


def _dict_value(value: Any, key: str) -> dict[str, Any]:
  """
  Return a nested dictionary value when present.
  """
  nested = value.get(key) if isinstance(value, dict) else None
  return nested if isinstance(nested, dict) else {}


def _tuple_value(value: Any) -> tuple[Any, ...]:
  """
  Return a tuple for list-like values.
  """
  if isinstance(value, tuple):
    return value
  if isinstance(value, list):
    return tuple(value)
  return ()


def _str_value(value: Any) -> str:
  """
  Return a stripped string representation for display values.
  """
  return str(value or "").strip()


def _int_value(value: Any) -> int:
  """
  Return an integer for numeric report summary values.
  """
  try:
    return int(value or 0)
  except (TypeError, ValueError):
    return 0
