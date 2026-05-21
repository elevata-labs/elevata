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

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from metadata.architecture import operations
from metadata.architecture.approval import (
  ArchitectureApprovalCheckResult,
  ArchitectureApprovalError,
)

"""
Tests for Architecture Operations service behavior.
"""

class FakeReport:
  """
  Architecture Change Report test double.
  """

  def __init__(
    self,
    *,
    report_fingerprint: str = "report-1",
    has_changes: bool = True,
    is_blocked: bool = False,
  ):
    self.report_fingerprint = report_fingerprint
    self.has_changes = has_changes
    self.is_blocked = is_blocked

  def to_dict(self) -> dict[str, Any]:
    """
    Return a report payload accepted by approval operations.
    """
    return {
      "report_fingerprint": self.report_fingerprint,
      "state": {
        "previous_fingerprint": "previous-state",
        "current_fingerprint": "current-state",
        "has_changes": self.has_changes,
      },
      "scope": {
        "mode": "scoped",
        "schema_short": "serving",
        "target_name": "Customer",
        "dataset_keys": ["serving.Customer"],
      },
      "summary": {
        "dataset_change_count": 0,
        "column_change_count": 1,
        "migration_action_count": 1,
        "policy_decision_count": 1,
        "blocking_policy_decision_count": 0,
      },
      "dataset_changes": [],
      "column_changes": [],
      "migration_actions": [],
      "policy_decisions": [],
      "is_blocked": self.is_blocked,
    }


class FakeApprovalStore:
  """
  Approval store test double.
  """

  def __init__(
    self,
    *,
    artifact: Any | None = None,
    load_error: ArchitectureApprovalError | None = None,
    save_path: Path | None = None,
  ):
    self.artifact = artifact
    self.load_error = load_error
    self.save_path = save_path or Path(".elevata/approvals/report-1.approval.json")
    self.base_path = self.save_path.parent
    self.saved_artifact = None
    self.loaded_report_fingerprint = None

  def save(self, artifact: Any) -> Path:
    """
    Store the artifact passed by the operation under test.
    """
    self.saved_artifact = artifact
    return self.save_path

  def load_for_report_fingerprint(self, report_fingerprint: str) -> Any | None:
    """
    Return the configured artifact for the requested report fingerprint.
    """
    self.loaded_report_fingerprint = report_fingerprint
    if self.load_error is not None:
      raise self.load_error
    return self.artifact


def _target_dataset() -> SimpleNamespace:
  """
  Return a TargetDataset-shaped object for service tests.
  """
  return SimpleNamespace(
    target_schema=SimpleNamespace(short_name="serving"),
    target_dataset_name="Customer",
  )


def _status(
  status: str,
  *,
  message: str = "status message",
  approval_id: str | None = None,
  artifact_fingerprint: str | None = None,
) -> SimpleNamespace:
  """
  Return an ArchitectureReviewStatus-shaped object for service tests.
  """
  return SimpleNamespace(
    status=status,
    message=message,
    approval_id=approval_id,
    artifact_fingerprint=artifact_fingerprint,
  )


def _context(
  *,
  report: FakeReport | None = None,
  status: SimpleNamespace | None = None,
  store: FakeApprovalStore | None = None,
) -> operations.ArchitectureOperationsContext:
  """
  Return an ArchitectureOperationsContext with test doubles.
  """
  return operations.ArchitectureOperationsContext(
    target_dataset=_target_dataset(),
    dataset_key="serving.Customer",
    report=report or FakeReport(),
    review_status=status or _status("pending"),
    approval_store=store or FakeApprovalStore(),
  )


def _patch_operations_context(
  monkeypatch: pytest.MonkeyPatch,
  context: operations.ArchitectureOperationsContext,
) -> None:
  """
  Patch context construction for operation-level tests.
  """
  monkeypatch.setattr(
    operations,
    "build_target_dataset_architecture_operations_context",
    lambda target_dataset, *, approval_store=None: context,
  )


def test_build_target_dataset_architecture_report_uses_scoped_target_dataset(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify scoped report construction for one TargetDataset.
  """
  current_state = object()
  previous_state = object()
  policy = object()
  report = FakeReport()
  calls: dict[str, Any] = {}

  class FakeStateService:
    """
    Architecture state service test double.
    """

    def build_current_state(self) -> object:
      """
      Return the configured current state.
      """
      return current_state

  class FakeStateStore:
    """
    Architecture state store test double.
    """

    def load(self) -> object:
      """
      Return the configured previous state.
      """
      return previous_state

  def fake_resolve_dataset_keys_from_state(**kwargs) -> set[str]:
    """
    Capture scope resolution input and return the dataset scope.
    """
    calls["scope"] = kwargs
    return {"serving.Customer"}

  def fake_build_architecture_change_report(**kwargs) -> FakeReport:
    """
    Capture report construction input and return the report.
    """
    calls["report"] = kwargs
    return report

  monkeypatch.setattr(operations, "ArchitectureStateService", FakeStateService)
  monkeypatch.setattr(operations, "ArchitectureStateStore", FakeStateStore)
  monkeypatch.setattr(
    operations,
    "resolve_dataset_keys_from_state",
    fake_resolve_dataset_keys_from_state,
  )
  monkeypatch.setattr(
    operations,
    "build_architecture_change_report",
    fake_build_architecture_change_report,
  )
  monkeypatch.setattr(operations, "load_materialization_policy", lambda: policy)

  dataset_key, result = operations.build_target_dataset_architecture_report(
    _target_dataset(),
  )

  assert dataset_key == "serving.Customer"
  assert result is report
  assert calls["scope"] == {
    "state": current_state,
    "target_name": "Customer",
    "schema_short": "serving",
    "all_datasets": False,
  }
  assert calls["report"] == {
    "previous_state": previous_state,
    "current_state": current_state,
    "policy": policy,
    "relevant_dataset_keys": {"serving.Customer"},
    "schema_short": "serving",
    "target_name": "Customer",
    "scope_mode": "scoped",
  }


def test_build_target_dataset_architecture_report_requires_schema_and_name() -> None:
  """
  Verify target scope validation.
  """
  target_dataset = SimpleNamespace(
    target_schema=SimpleNamespace(short_name=""),
    target_dataset_name="Customer",
  )

  with pytest.raises(
    operations.ArchitectureOperationsError,
    match="TargetDataset must have a target schema and dataset name.",
  ):
    operations.build_target_dataset_architecture_report(target_dataset)


def test_build_target_dataset_architecture_operations_context_uses_report_and_store(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify context construction from report, status and store.
  """
  report = FakeReport()
  status = _status("pending")
  store = FakeApprovalStore()

  monkeypatch.setattr(
    operations,
    "build_target_dataset_architecture_report",
    lambda target_dataset: ("serving.Customer", report),
  )
  monkeypatch.setattr(
    operations,
    "build_architecture_review_status_for_report",
    lambda **kwargs: status,
  )

  context = operations.build_target_dataset_architecture_operations_context(
    _target_dataset(),
    approval_store=store,
  )

  assert context.dataset_key == "serving.Customer"
  assert context.report is report
  assert context.review_status is status
  assert context.approval_store is store


def test_render_target_dataset_architecture_report_json_uses_report_renderer(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify deterministic JSON rendering through the report service.
  """
  report = FakeReport()

  monkeypatch.setattr(
    operations,
    "build_target_dataset_architecture_report",
    lambda target_dataset: ("serving.Customer", report),
  )
  monkeypatch.setattr(
    operations,
    "render_architecture_report_json",
    lambda value: '{"report_fingerprint":"report-1"}\n',
  )

  rendered = operations.render_target_dataset_architecture_report_json(
    _target_dataset(),
  )

  assert rendered == '{"report_fingerprint":"report-1"}\n'


def test_render_target_dataset_architecture_report_text_uses_report_renderer(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify text rendering through the report service.
  """
  report = FakeReport()

  monkeypatch.setattr(
    operations,
    "build_target_dataset_architecture_report",
    lambda target_dataset: ("serving.Customer", report),
  )
  monkeypatch.setattr(
    operations,
    "render_architecture_report_text",
    lambda value: "Architecture Change Report\n",
  )

  rendered = operations.render_target_dataset_architecture_report_text(
    _target_dataset(),
  )

  assert rendered == "Architecture Change Report\n"


def test_create_target_dataset_architecture_approval_saves_artifact(
  monkeypatch: pytest.MonkeyPatch,
  tmp_path: Path,
) -> None:
  """
  Verify approval artifact creation and storage.
  """
  artifact = SimpleNamespace(
    approval_id="apr_123",
    report={"report_fingerprint": "report-1"},
  )
  save_path = tmp_path / "report-1.approval.json"
  store = FakeApprovalStore(save_path=save_path)
  context = _context(store=store)
  calls: dict[str, Any] = {}

  def fake_build_architecture_approval_artifact(**kwargs) -> SimpleNamespace:
    """
    Capture approval artifact input and return the artifact.
    """
    calls.update(kwargs)
    return artifact

  _patch_operations_context(monkeypatch, context)
  monkeypatch.setattr(
    operations,
    "build_architecture_approval_artifact",
    fake_build_architecture_approval_artifact,
  )

  result = operations.create_target_dataset_architecture_approval(
    _target_dataset(),
    approved_by="Ilona",
    note="Reviewed.",
  )

  assert result.context is context
  assert result.artifact is artifact
  assert result.approval_path == save_path
  assert store.saved_artifact is artifact
  assert calls["report_payload"]["report_fingerprint"] == "report-1"
  assert calls["decided_by"] == "Ilona"
  assert calls["note"] == "Reviewed."


@pytest.mark.parametrize(
  ("context", "message"),
  [
    (
      _context(report=FakeReport(has_changes=False)),
      "No architecture changes are present for this dataset scope.",
    ),
    (
      _context(report=FakeReport(is_blocked=True)),
      "Architecture approval is disabled because the report is blocked by policy.",
    ),
    (
      _context(status=_status("approved")),
      "A matching approval artifact already exists for this report.",
    ),
  ],
)
def test_create_target_dataset_architecture_approval_rejects_ineligible_reports(
  monkeypatch: pytest.MonkeyPatch,
  context: operations.ArchitectureOperationsContext,
  message: str,
) -> None:
  """
  Verify approval guards for no-change, blocked and approved reports.
  """
  _patch_operations_context(monkeypatch, context)

  with pytest.raises(operations.ArchitectureOperationsError, match=message):
    operations.create_target_dataset_architecture_approval(
      _target_dataset(),
      approved_by="Ilona",
    )


def test_check_target_dataset_architecture_approval_returns_invalid_on_store_error(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify invalid check result for unreadable approval artifacts.
  """
  store = FakeApprovalStore(
    load_error=ArchitectureApprovalError("Approval artifact JSON is invalid."),
  )
  context = _context(store=store)
  _patch_operations_context(monkeypatch, context)

  result = operations.check_target_dataset_architecture_approval(
    _target_dataset(),
  )

  assert result.is_valid is False
  assert result.status == "invalid"
  assert result.message == "Approval artifact JSON is invalid."
  assert result.report_fingerprint == "report-1"


def test_check_target_dataset_architecture_approval_returns_drift_status(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify drift result when another approval exists for the same scope.
  """
  context = _context(
    status=_status(
      "drift",
      message="Approval drift detected.",
      approval_id="apr_old",
      artifact_fingerprint="artifact-old",
    ),
  )
  _patch_operations_context(monkeypatch, context)

  result = operations.check_target_dataset_architecture_approval(
    _target_dataset(),
  )

  assert result.is_valid is False
  assert result.status == "drift"
  assert result.message == "Approval drift detected."
  assert result.approval_id == "apr_old"
  assert result.artifact_fingerprint == "artifact-old"


def test_check_target_dataset_architecture_approval_returns_missing_status(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify missing result when no approval artifact exists.
  """
  context = _context(status=_status("pending"))
  _patch_operations_context(monkeypatch, context)

  result = operations.check_target_dataset_architecture_approval(
    _target_dataset(),
  )

  assert result.is_valid is False
  assert result.status == "missing"
  assert result.message == "No approval artifact exists for the report fingerprint."
  assert result.report_fingerprint == "report-1"


def test_check_target_dataset_architecture_approval_validates_loaded_artifact(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify approval validation for an exact stored artifact.
  """
  approval_payload = {"approval_id": "apr_123"}
  artifact = SimpleNamespace(to_dict=lambda: approval_payload)
  store = FakeApprovalStore(artifact=artifact)
  context = _context(store=store)
  expected = ArchitectureApprovalCheckResult(
    is_valid=True,
    status="approved",
    message="Approval artifact matches the architecture change report.",
    report_fingerprint="report-1",
    approval_id="apr_123",
    artifact_fingerprint="artifact-1",
  )
  calls: dict[str, Any] = {}

  def fake_check_architecture_approval(**kwargs) -> ArchitectureApprovalCheckResult:
    """
    Capture approval check input and return the configured result.
    """
    calls.update(kwargs)
    return expected

  _patch_operations_context(monkeypatch, context)
  monkeypatch.setattr(
    operations,
    "check_architecture_approval",
    fake_check_architecture_approval,
  )

  result = operations.check_target_dataset_architecture_approval(
    _target_dataset(),
  )

  assert result is expected
  assert store.loaded_report_fingerprint == "report-1"
  assert calls["report_payload"]["report_fingerprint"] == "report-1"
  assert calls["approval_payload"] == approval_payload