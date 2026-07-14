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

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from metadata.architecture.control import (
  ArchitectureControlContext,
  ArchitectureControlScope,
)
from metadata.architecture.review_briefing import (
  ArchitectureReviewBriefing,
  build_architecture_review_briefing,
)


def _artifact_context() -> SimpleNamespace:
  """
  Return an ArchitectureArtifactContext-shaped object for briefing tests.
  """
  return SimpleNamespace(
    profile_name="dev",
    target_system_short="dwh",
    profile_token="dev",
    target_system_token="dwh",
    label="dev/dwh",
  )


def _state_store() -> SimpleNamespace:
  """
  Return an ArchitectureStateStore-shaped object for briefing tests.
  """
  state_file = Path(".elevata/state/dev/dwh/architecture_state.json")
  return SimpleNamespace(
    base_path=state_file.parent,
    state_file_path=lambda: state_file,
  )


def _baseline_resolution() -> SimpleNamespace:
  """
  Return an ArchitectureBaselineResolution-shaped object for briefing tests.
  """
  state_file = Path(".elevata/state/dev/dwh/architecture_state.json")
  return SimpleNamespace(
    previous_state=SimpleNamespace(),
    source="recorded_state",
    can_execute=True,
    message="Recorded architecture baseline is available for this runtime context.",
    state_file=state_file,
    warning_count=0,
    warnings=(),
    is_recorded=True,
    is_discovered=False,
  )


class FakeReport:
  """
  Architecture Change Report test double for briefing tests.
  """

  def __init__(
    self,
    *,
    report_fingerprint: str = "report-1",
    has_changes: bool = True,
    is_blocked: bool = False,
    payload: dict[str, Any] | None = None,
  ):
    self.report_fingerprint = report_fingerprint
    self.has_changes = has_changes
    self.is_blocked = is_blocked
    self.payload = payload or _report_payload(
      report_fingerprint=report_fingerprint,
      has_changes=has_changes,
      is_blocked=is_blocked,
    )
    self.to_dict_calls = 0

  def to_dict(self) -> dict[str, Any]:
    """
    Return the report payload without exposing mutable internal state.
    """
    self.to_dict_calls += 1
    return deepcopy(self.payload)


class MutatingApprovalStore:
  """
  Approval store test double that fails if the briefing tries to mutate state.
  """

  def save(self, artifact: Any) -> None:
    """
    Fail when the briefing tries to persist an artifact.
    """
    raise AssertionError("Review briefing must not save approval artifacts.")

  def load_for_report_fingerprint(self, report_fingerprint: str) -> None:
    """
    Fail when the briefing tries to read approval artifacts.
    """
    raise AssertionError("Review briefing must not read approval artifacts.")

  def load_all(self) -> tuple[Any, ...]:
    """
    Fail when the briefing tries to inspect approval storage.
    """
    raise AssertionError("Review briefing must not inspect approval storage.")


def _report_payload(
  *,
  report_fingerprint: str = "report-1",
  has_changes: bool = True,
  is_blocked: bool = False,
  migration_actions: list[dict[str, Any]] | None = None,
  policy_decisions: list[dict[str, Any]] | None = None,
  summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
  """
  Return an Architecture Change Report-shaped payload.
  """
  payload_summary = summary or {
    "dataset_change_count": 1 if has_changes else 0,
    "column_change_count": 2 if has_changes else 0,
    "migration_action_count": 1 if has_changes else 0,
    "policy_decision_count": len(policy_decisions or []),
    "blocking_policy_decision_count": 0,
  }

  return {
    "report_fingerprint": report_fingerprint,
    "state": {
      "previous_fingerprint": "previous-state",
      "current_fingerprint": "current-state",
      "has_changes": has_changes,
    },
    "scope": {
      "mode": "scoped",
      "schema_short": "serving",
      "target_name": "Customer",
      "dataset_keys": ["serving.Customer"],
    },
    "summary": payload_summary,
    "dataset_changes": [
      {
        "change_type": "changed",
        "dataset_key": "serving.Customer",
      }
    ] if has_changes else [],
    "column_changes": [
      {
        "change_type": "added",
        "column_key": "serving.Customer.customer_id",
      }
    ] if has_changes else [],
    "migration_actions": migration_actions or [
      {
        "action_type": "ADD_COLUMN",
        "dataset_key": "serving.Customer",
      }
    ] if has_changes else [],
    "policy_decisions": policy_decisions or [],
    "is_blocked": is_blocked,
  }


def _status(
  status: str,
  *,
  approval_id: str | None = None,
) -> SimpleNamespace:
  """
  Return an ArchitectureReviewStatus-shaped object.
  """
  return SimpleNamespace(
    status=status,
    label=status.replace("_", " ").title(),
    message=f"Review state is {status}.",
    badge_class="text-bg-secondary",
    icon="bi-shield-check",
    approval_id=approval_id,
  )


def _gate(
  status: str,
  *,
  can_execute: bool,
) -> SimpleNamespace:
  """
  Return an ArchitectureExecutionGate-shaped object.
  """
  return SimpleNamespace(
    status=status,
    can_execute=can_execute,
    label=status.replace("_", " ").title(),
    message=f"Execution gate is {status}.",
    badge_class="text-bg-secondary",
    icon="bi-play-circle",
  )


def _preview(
  gate: SimpleNamespace | None = None,
) -> SimpleNamespace:
  """
  Return an ArchitectureExecutionPreview-shaped object.
  """
  return SimpleNamespace(
    gate=gate or _gate("ready", can_execute=True),
    dependency_mode="target_only",
    step_count=1,
  )


def _context(
  *,
  scope: ArchitectureControlScope | None = None,
  report: FakeReport | None = None,
  status: SimpleNamespace | None = None,
) -> ArchitectureControlContext:
  """
  Return an ArchitectureControlContext for review briefing tests.
  """
  return ArchitectureControlContext(
    scope=scope or ArchitectureControlScope(
      mode="target_dataset",
      schema_short="serving",
      target_name="Customer",
      dataset_key="serving.Customer",
    ),
    artifact_context=_artifact_context(),
    report=report or FakeReport(),
    review_status=status or _status("pending"),
    approval_store=MutatingApprovalStore(),
    state_store=_state_store(),
    baseline_resolution=_baseline_resolution(),
  )


def _section(
  briefing: ArchitectureReviewBriefing,
  key: str,
):
  """
  Return a briefing section by key.
  """
  return next(section for section in briefing.sections if section.key == key)


def test_review_briefing_accepts_explicit_scope_when_context_scope_is_missing() -> None:
  """
  Verify view-level test doubles can pass the resolved scope explicitly.
  """
  context = SimpleNamespace(
    scope=None,
    report=FakeReport(),
    review_status=_status("approved", approval_id="apr_123"),
    approval_store=MutatingApprovalStore(),
  )

  briefing = build_architecture_review_briefing(
    control_context=context,
    scope=ArchitectureControlScope.for_all(),
  )

  assert briefing.scope_key == "all"
  assert briefing.scope_label == "All datasets"
  assert briefing.scope_mode == "All datasets"


def test_review_briefing_summarizes_no_change_scope() -> None:
  """
  Verify briefing output for a no-change architecture scope.
  """
  briefing = build_architecture_review_briefing(
    control_context=_context(
      report=FakeReport(has_changes=False),
      status=_status("approved"),
    ),
    execution_preview=_preview(_gate("ready_no_changes", can_execute=True)),
  )

  assert briefing.has_attention is False
  assert _section(briefing, "change_summary").signals[0].level == "success"
  assert "No architecture changes" in _section(
    briefing,
    "change_summary",
  ).signals[0].message
  assert "aligned with the persisted baseline" in _section(
    briefing,
    "suggested_reviewer_focus",
  ).signals[0].details[0]


def test_review_briefing_highlights_pending_changes() -> None:
  """
  Verify briefing output for pending architecture changes.
  """
  briefing = build_architecture_review_briefing(
    control_context=_context(status=_status("pending")),
    execution_preview=_preview(_gate("pending_approval", can_execute=False)),
  )

  assert briefing.has_attention is True
  assert _section(briefing, "review_state").attention_count == 2
  assert _section(briefing, "execution_readiness").signals[0].level == "warning"
  assert any(
    "approve" in detail.lower()
    for detail in _section(briefing, "suggested_reviewer_focus").signals[0].details
  )


def test_review_briefing_marks_approved_matching_changes_ready() -> None:
  """
  Verify briefing output for approved changes with a ready execution preview.
  """
  briefing = build_architecture_review_briefing(
    control_context=_context(status=_status("approved", approval_id="apr_123")),
    execution_preview=_preview(_gate("ready", can_execute=True)),
  )

  assert _section(briefing, "review_state").signals[0].level == "success"
  assert _section(briefing, "execution_readiness").signals[0].level == "success"
  assert any(
    "execution preview" in detail.lower()
    for detail in _section(briefing, "suggested_reviewer_focus").signals[0].details
  )


def test_review_briefing_highlights_blocked_policy() -> None:
  """
  Verify briefing output for a blocked policy decision.
  """
  report = FakeReport(
    is_blocked=True,
    payload=_report_payload(
      is_blocked=True,
      policy_decisions=[
        {
          "decision": "blocked",
          "severity": "blocking",
          "message": "Column drop is not allowed.",
        }
      ],
      summary={
        "dataset_change_count": 1,
        "column_change_count": 1,
        "migration_action_count": 1,
        "policy_decision_count": 1,
        "blocking_policy_decision_count": 1,
      },
    ),
  )

  briefing = build_architecture_review_briefing(
    control_context=_context(report=report, status=_status("blocked")),
    execution_preview=_preview(_gate("blocked_by_policy", can_execute=False)),
  )

  assert _section(briefing, "policy_attention").signals[0].level == "danger"
  assert _section(briefing, "destructive_attention").attention_count == 1
  assert any(
    "blocking policy" in detail.lower()
    for detail in _section(briefing, "suggested_reviewer_focus").signals[0].details
  )


def test_review_briefing_treats_allowed_policy_decisions_as_evaluated() -> None:
  """
  Verify allowed policy decisions are evidence, not attention signals.
  """
  policy_decisions = [
    {
      "status": "ALLOW",
      "code": "ADD_COLUMN_ALLOWED",
      "action_type": "ADD_COLUMN",
      "dataset_key": "raw.customer",
      "column_name": f"column_{index}",
      "message": "The action is allowed by the active materialization policy.",
    }
    for index in range(8)
  ]
  report = FakeReport(
    payload=_report_payload(
      policy_decisions=policy_decisions,
      summary={
        "dataset_change_count": 0,
        "column_change_count": 8,
        "migration_action_count": 8,
        "policy_decision_count": 8,
        "blocking_policy_decision_count": 0,
      },
    ),
  )

  briefing = build_architecture_review_briefing(
    control_context=_context(report=report, status=_status("pending")),
    execution_preview=_preview(_gate("pending_approval", can_execute=False)),
  )

  signal = _section(briefing, "policy_attention").signals[0]
  assert signal.level == "success"
  assert signal.title == "Policy decisions evaluated"
  assert "All actions are allowed or metadata-only" in signal.message
  assert signal.detail_count == 8
  assert len(signal.preview_details) == 6
  assert signal.remaining_detail_count == 2
  assert signal.has_remaining_details is True
  assert signal.details[0].startswith("ALLOW · ADD_COLUMN")


def test_review_briefing_marks_preflight_policy_decisions_as_attention() -> None:
  """
  Verify preflight policy decisions remain explicit warning signals.
  """
  report = FakeReport(
    payload=_report_payload(
      policy_decisions=[
        {
          "status": "ALLOW",
          "action_type": "ADD_COLUMN",
          "dataset_key": "raw.customer",
          "column_name": "customer_name",
          "message": "The action is allowed by the active materialization policy.",
        },
        {
          "status": "REQUIRES_PREFLIGHT",
          "action_type": "ALTER_COLUMN",
          "dataset_key": "raw.customer",
          "column_name": "customer_id",
          "message": "The action requires schema preflight validation before execution.",
        },
      ],
      summary={
        "dataset_change_count": 0,
        "column_change_count": 2,
        "migration_action_count": 2,
        "policy_decision_count": 2,
        "blocking_policy_decision_count": 0,
      },
    ),
  )

  briefing = build_architecture_review_briefing(
    control_context=_context(report=report, status=_status("pending")),
    execution_preview=_preview(_gate("pending_approval", can_execute=False)),
  )

  signal = _section(briefing, "policy_attention").signals[0]
  assert signal.level == "warning"
  assert "1 policy decision(s) require schema preflight" in signal.message
  assert signal.detail_count == 2


def test_review_briefing_highlights_destructive_attention() -> None:
  """
  Verify destructive migration action detection.
  """
  report = FakeReport(
    payload=_report_payload(
      migration_actions=[
        {
          "action_type": "DROP_COLUMN",
          "dataset_key": "serving.Customer",
          "column_name": "legacy_code",
        }
      ],
      summary={
        "dataset_change_count": 0,
        "column_change_count": 1,
        "migration_action_count": 1,
        "policy_decision_count": 0,
        "blocking_policy_decision_count": 0,
      },
    ),
  )

  briefing = build_architecture_review_briefing(
    control_context=_context(report=report, status=_status("pending")),
    execution_preview=_preview(_gate("pending_approval", can_execute=False)),
  )

  signal = _section(briefing, "destructive_attention").signals[0]
  assert signal.level == "danger"
  assert "DROP_COLUMN" in signal.details[0]


def test_review_briefing_highlights_approval_drift() -> None:
  """
  Verify briefing focus for approval drift.
  """
  briefing = build_architecture_review_briefing(
    control_context=_context(status=_status("drift", approval_id="apr_old")),
    execution_preview=_preview(_gate("pending_approval", can_execute=False)),
  )

  assert _section(briefing, "review_state").signals[0].level == "warning"
  assert any(
    "drift" in detail.lower()
    for detail in _section(briefing, "suggested_reviewer_focus").signals[0].details
  )


def test_review_briefing_is_target_dataset_scope_aware() -> None:
  """
  Verify target dataset scope labels in briefing output.
  """
  briefing = build_architecture_review_briefing(
    control_context=_context(),
    execution_preview=_preview(),
  )

  assert briefing.scope_key == "serving.Customer"
  assert briefing.scope_label == "serving.Customer"
  assert briefing.scope_mode == "Target dataset"


def test_review_briefing_is_schema_and_all_scope_aware() -> None:
  """
  Verify schema and all scope labels in briefing output.
  """
  schema_briefing = build_architecture_review_briefing(
    control_context=_context(
      scope=ArchitectureControlScope.for_schema("serving"),
    ),
    execution_preview=_preview(),
  )
  all_briefing = build_architecture_review_briefing(
    control_context=_context(
      scope=ArchitectureControlScope.for_all(),
    ),
    execution_preview=_preview(),
  )

  assert schema_briefing.scope_key == "schema:serving"
  assert schema_briefing.scope_mode == "Schema"
  assert all_briefing.scope_key == "all"
  assert all_briefing.scope_mode == "All datasets"


def test_review_briefing_does_not_mutate_control_state() -> None:
  """
  Verify briefing construction does not persist or query approval artifacts.
  """
  report = FakeReport()
  context = _context(report=report)

  briefing = build_architecture_review_briefing(
    control_context=context,
    execution_preview=_preview(),
  )

  assert briefing.report_fingerprint == "report-1"
  assert report.to_dict_calls == 1
  assert context.approval_store.__class__ is MutatingApprovalStore
