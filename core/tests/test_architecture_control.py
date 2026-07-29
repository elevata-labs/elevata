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

from metadata.architecture import control
from metadata.architecture.approval import (
  ArchitectureApprovalCheckResult,
  ArchitectureApprovalError,
)


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

  def load_all(self) -> tuple[Any, ...]:
    """
    Return stored artifacts for drift detection.
    """
    return ()


def _artifact_context() -> SimpleNamespace:
  """
  Return an ArchitectureArtifactContext-shaped object for service tests.
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
  Return an ArchitectureStateStore-shaped object for service tests.
  """
  state_file = Path(".elevata/state/dev/dwh/architecture_state.json")
  return SimpleNamespace(
    base_path=state_file.parent,
    state_file_path=lambda: state_file,
  )


def _baseline_resolution(
  *,
  previous_state: Any | None = None,
  can_execute: bool = True,
) -> SimpleNamespace:
  """
  Return an ArchitectureBaselineResolution-shaped object for service tests.
  """
  state_file = Path(".elevata/state/dev/dwh/architecture_state.json")
  return SimpleNamespace(
    previous_state=previous_state,
    source="recorded_state" if can_execute else "missing_or_unsupported",
    can_execute=can_execute,
    message="baseline message",
    state_file=state_file,
    warning_count=0,
    warnings=(),
    is_recorded=can_execute,
    is_discovered=False,
  )


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
  scope: control.ArchitectureControlScope | None = None,
  report: FakeReport | None = None,
  status: SimpleNamespace | None = None,
  store: FakeApprovalStore | None = None,
) -> control.ArchitectureControlContext:
  """
  Return an ArchitectureControlContext with test doubles.
  """
  return control.ArchitectureControlContext(
    scope=scope or control.ArchitectureControlScope.from_target_dataset(
      _target_dataset(),
    ),
    artifact_context=_artifact_context(),
    report=report or FakeReport(),
    review_status=status or _status("pending"),
    approval_store=store or FakeApprovalStore(),
    state_store=_state_store(),
    baseline_resolution=_baseline_resolution(),
  )


def _patch_control_context(
  monkeypatch: pytest.MonkeyPatch,
  context: control.ArchitectureControlContext,
) -> None:
  """
  Patch context construction for operation-level tests.
  """
  monkeypatch.setattr(
    control,
    "build_architecture_control_context",
    lambda scope, *, approval_store=None, artifact_context=None: context,
  )


def test_architecture_control_scope_from_target_dataset() -> None:
  """
  Verify TargetDataset scope construction.
  """
  scope = control.ArchitectureControlScope.from_target_dataset(
    _target_dataset(),
  )

  assert scope.mode == "target_dataset"
  assert scope.schema_short == "serving"
  assert scope.target_name == "Customer"
  assert scope.dataset_key == "serving.Customer"
  assert scope.key == "serving.Customer"
  assert scope.label == "serving.Customer"


def test_architecture_control_scope_requires_schema_and_name() -> None:
  """
  Verify TargetDataset scope validation.
  """
  target_dataset = SimpleNamespace(
    target_schema=SimpleNamespace(short_name=""),
    target_dataset_name="Customer",
  )

  with pytest.raises(
    control.ArchitectureControlError,
    match="TargetDataset must have a target schema and dataset name.",
  ):
    control.ArchitectureControlScope.from_target_dataset(target_dataset)


def test_build_architecture_control_report_uses_target_dataset_scope(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify report construction for a TargetDataset scope.
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
      Return the configured metadata-defined state.
      """
      return current_state

  class FakeStateStore:
    """
    Architecture state store test double.
    """

    def __init__(self, *args, **kwargs):
      self.base_path = Path(".elevata/state/dev/dwh")

    def state_file_path(self) -> Path:
      """
      Return the configured persisted state file path.
      """
      return self.base_path / "architecture_state.json"

    def load(self) -> object:
      """
      Return the configured persisted state.
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

  monkeypatch.setattr(control, "ArchitectureStateService", FakeStateService)
  monkeypatch.setattr(control, "ArchitectureStateStore", FakeStateStore)
  monkeypatch.setattr(
    control,
    "resolve_dataset_keys_from_state",
    fake_resolve_dataset_keys_from_state,
  )
  monkeypatch.setattr(
    control,
    "build_architecture_change_report",
    fake_build_architecture_change_report,
  )
  monkeypatch.setattr(control, "load_materialization_policy", lambda: policy)
  monkeypatch.setattr(
    control,
    "resolve_architecture_artifact_context",
    _artifact_context,
  )

  scope = control.ArchitectureControlScope.from_target_dataset(_target_dataset())
  result = control.build_architecture_control_report(scope)

  assert result is report
  assert calls["scope"] == {
    "state": current_state,
    "target_name": "Customer",
    "schema_short": "serving",
    "all_datasets": False,
    "include_related_hist": True,
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


def test_build_architecture_control_report_uses_schema_scope(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify report construction for a schema scope.
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
      Return the configured metadata-defined state.
      """
      return current_state

  class FakeStateStore:
    """
    Architecture state store test double.
    """

    def __init__(self, *args, **kwargs):
      self.base_path = Path(".elevata/state/dev/dwh")

    def state_file_path(self) -> Path:
      """
      Return the configured persisted state file path.
      """
      return self.base_path / "architecture_state.json"

    def load(self) -> object:
      """
      Return the configured persisted state.
      """
      return previous_state

  def fake_resolve_dataset_keys_from_state(**kwargs) -> set[str]:
    """
    Return current and recorded schema members for review-scope assembly.
    """
    calls.setdefault("scope", []).append(kwargs)
    if kwargs["state"] is current_state:
      return {"serving.Customer"}
    if kwargs["state"] is previous_state:
      return {"serving.Customer", "serving.Retired"}
    raise AssertionError("Unexpected Architecture State used for scope resolution.")

  def fake_build_architecture_change_report(**kwargs) -> FakeReport:
    """
    Capture report construction input and return the report.
    """
    calls["report"] = kwargs
    return report

  monkeypatch.setattr(control, "ArchitectureStateService", FakeStateService)
  monkeypatch.setattr(control, "ArchitectureStateStore", FakeStateStore)
  monkeypatch.setattr(
    control,
    "resolve_dataset_keys_from_state",
    fake_resolve_dataset_keys_from_state,
  )
  monkeypatch.setattr(
    control,
    "build_architecture_change_report",
    fake_build_architecture_change_report,
  )
  monkeypatch.setattr(control, "load_materialization_policy", lambda: policy)
  monkeypatch.setattr(
    control,
    "resolve_architecture_artifact_context",
    _artifact_context,
  )

  scope = control.ArchitectureControlScope.for_schema("serving")
  result = control.build_architecture_control_report(scope)

  assert result is report
  assert calls["scope"] == [
    {
      "state": current_state,
      "target_name": None,
      "schema_short": "serving",
      "all_datasets": True,
      "include_related_hist": True,
    },
    {
      "state": previous_state,
      "target_name": None,
      "schema_short": "serving",
      "all_datasets": True,
      "include_related_hist": True,
    },
  ]
  assert calls["report"] == {
    "previous_state": previous_state,
    "current_state": current_state,
    "policy": policy,
    "relevant_dataset_keys": {"serving.Customer", "serving.Retired"},
    "schema_short": "serving",
    "target_name": None,
    "scope_mode": "scoped",
  }


def test_build_architecture_control_report_uses_all_scope(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify report construction for all datasets.
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
      Return the configured metadata-defined state.
      """
      return current_state

  class FakeStateStore:
    """
    Architecture state store test double.
    """

    def __init__(self, *args, **kwargs):
      self.base_path = Path(".elevata/state/dev/dwh")

    def state_file_path(self) -> Path:
      """
      Return the configured persisted state file path.
      """
      return self.base_path / "architecture_state.json"

    def load(self) -> object:
      """
      Return the configured persisted state.
      """
      return previous_state

  def fake_build_architecture_change_report(**kwargs) -> FakeReport:
    """
    Capture report construction input and return the report.
    """
    calls["report"] = kwargs
    return report

  monkeypatch.setattr(control, "ArchitectureStateService", FakeStateService)
  monkeypatch.setattr(control, "ArchitectureStateStore", FakeStateStore)
  monkeypatch.setattr(
    control,
    "resolve_dataset_keys_from_state",
    lambda **kwargs: pytest.fail("All scope must not resolve a scoped key set."),
  )
  monkeypatch.setattr(
    control,
    "build_architecture_change_report",
    fake_build_architecture_change_report,
  )
  monkeypatch.setattr(control, "load_materialization_policy", lambda: policy)
  monkeypatch.setattr(
    control,
    "resolve_architecture_artifact_context",
    _artifact_context,
  )

  scope = control.ArchitectureControlScope.for_all()
  result = control.build_architecture_control_report(scope)

  assert result is report
  assert calls["report"] == {
    "previous_state": previous_state,
    "current_state": current_state,
    "policy": policy,
    "relevant_dataset_keys": None,
    "schema_short": None,
    "target_name": None,
    "scope_mode": "all",
  }


def test_build_architecture_control_report_reuses_supplied_current_state(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify report construction does not rebuild an explicitly supplied state.
  """
  current_state = object()
  previous_state = object()
  report = FakeReport()
  calls: dict[str, Any] = {}

  monkeypatch.setattr(
    control.ArchitectureStateService,
    "build_current_state",
    lambda self: pytest.fail("Current state must not be rebuilt."),
  )

  def fake_resolve_relevant_dataset_keys(**kwargs) -> set[str]:
    calls["current_state"] = kwargs["current_state"]
    return {"serving.Customer"}

  monkeypatch.setattr(
    control,
    "_resolve_relevant_dataset_keys",
    fake_resolve_relevant_dataset_keys,
  )
  monkeypatch.setattr(
    control,
    "resolve_architecture_baseline",
    lambda **kwargs: _baseline_resolution(previous_state=previous_state),
  )
  monkeypatch.setattr(
    control,
    "build_architecture_change_report",
    lambda **kwargs: report,
  )
  monkeypatch.setattr(control, "load_materialization_policy", object)

  result, baseline = control.build_architecture_control_report_with_baseline(
    control.ArchitectureControlScope.from_target_dataset(_target_dataset()),
    artifact_context=_artifact_context(),
    state_store=_state_store(),
    current_state=current_state,
  )

  assert result is report
  assert baseline.previous_state is previous_state
  assert calls["current_state"] is current_state


def test_build_architecture_control_context_uses_shared_state_and_impact_plan(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify context construction shares one current state across all artifacts.
  """
  scope = control.ArchitectureControlScope.from_target_dataset(_target_dataset())
  report = FakeReport()
  status = _status("pending")
  store = FakeApprovalStore()
  current_state = object()
  impact_plan = object()
  artifact_context = _artifact_context()
  baseline_resolution = _baseline_resolution(previous_state=object())
  calls: dict[str, Any] = {}

  class FakeStateService:
    def build_current_state(self) -> object:
      calls["state_build_count"] = calls.get("state_build_count", 0) + 1
      return current_state

  class FakeStateStoreForContext:
    def __init__(self, *args, **kwargs):
      self.base_path = Path(".elevata/state/dev/dwh")

    def state_file_path(self) -> Path:
      return self.base_path / "architecture_state.json"

  def fake_build_report(value, **kwargs):
    calls["report_current_state"] = kwargs.get("current_state")
    return report, baseline_resolution

  def fake_build_impact(**kwargs):
    calls["impact"] = kwargs
    return impact_plan, None

  def fake_build_review_status(**kwargs):
    """
    Capture authoritative review inputs and return the configured status.
    """
    calls["review"] = kwargs
    return status

  monkeypatch.setattr(control, "ArchitectureStateService", FakeStateService)
  monkeypatch.setattr(
    control,
    "resolve_architecture_artifact_context",
    lambda: artifact_context,
  )
  monkeypatch.setattr(
    control,
    "ArchitectureStateStore",
    FakeStateStoreForContext,
  )
  monkeypatch.setattr(
    control,
    "build_architecture_control_report_with_baseline",
    fake_build_report,
  )
  monkeypatch.setattr(
    control,
    "build_architecture_review_status_for_report",
    fake_build_review_status,
  )
  monkeypatch.setattr(
    control,
    "_build_architecture_control_execution_impact_plan",
    fake_build_impact,
  )

  context = control.build_architecture_control_context(
    scope,
    approval_store=store,
  )

  assert calls["state_build_count"] == 1
  assert calls["report_current_state"] is current_state
  assert calls["review"]["baseline_resolution"] is baseline_resolution
  assert calls["impact"]["current_state"] is current_state
  assert calls["impact"]["baseline_resolution"] is baseline_resolution
  assert calls["impact"]["report"] is report
  assert calls["impact"]["review_status"] is status
  assert context.scope is scope
  assert context.artifact_context is artifact_context
  assert context.report is report
  assert context.review_status is status
  assert context.approval_store is store
  assert isinstance(context.state_store, FakeStateStoreForContext)
  assert context.baseline_resolution is baseline_resolution
  assert context.current_state is current_state
  assert context.execution_impact_plan is impact_plan
  assert context.execution_impact_plan_error is None


def test_execution_impact_plan_context_integration_passes_runtime_inputs(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify the impact service receives active targets and the scoped record store.
  """
  scope = control.ArchitectureControlScope.from_target_dataset(_target_dataset())
  current_state = object()
  baseline_resolution = _baseline_resolution(previous_state=object())
  report = FakeReport()
  status = _status("pending")
  impact_plan = object()
  target_datasets = (object(), object())
  artifact_context = _artifact_context()
  calls: dict[str, Any] = {}

  class FakeTargetDatasetManager:
    def select_related(self, *args):
      calls["select_related"] = args
      return self

    def filter(self, **kwargs):
      calls["filter"] = kwargs
      return self

    def order_by(self, *args):
      calls["order_by"] = args
      return target_datasets

  class FakeExecutionRecordStore:
    def __init__(self, *, context):
      calls["store_context"] = context
      calls["store_instance"] = self

  def fake_build_execution_impact_plan(**kwargs):
    calls["service"] = kwargs
    return impact_plan

  monkeypatch.setattr(
    control.TargetDataset,
    "objects",
    FakeTargetDatasetManager(),
  )
  monkeypatch.setattr(
    control,
    "ArchitectureExecutionRecordStore",
    FakeExecutionRecordStore,
  )
  monkeypatch.setattr(
    control,
    "build_execution_impact_plan",
    fake_build_execution_impact_plan,
  )

  result, error = control._build_architecture_control_execution_impact_plan(
    scope=scope,
    artifact_context=artifact_context,
    current_state=current_state,
    baseline_resolution=baseline_resolution,
    report=report,
    review_status=status,
  )

  assert result is impact_plan
  assert error is None
  assert calls["select_related"] == ("target_schema",)
  assert calls["filter"] == {"active": True}
  assert calls["order_by"] == (
    "target_schema__short_name",
    "target_dataset_name",
    "id",
  )
  assert calls["store_context"] is artifact_context
  assert calls["service"] == {
    "scope_key": scope.key,
    "current_state": current_state,
    "baseline_resolution": baseline_resolution,
    "report": report,
    "review_status": status,
    "target_datasets": target_datasets,
    "execution_record_store": calls["store_instance"],
    "execution_dataset_keys": None,
    "dependency_mode": "with_dependencies",
  }


def test_execution_impact_context_accepts_concrete_execution_scope(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify the context binds the plan to the concrete execution dataset scope.
  """
  scope = control.ArchitectureControlScope.from_target_dataset(
    _target_dataset()
  )
  current_state = object()
  baseline_resolution = _baseline_resolution(previous_state=object())
  report = FakeReport()
  status = _status("no_changes")
  impact_plan = SimpleNamespace(
    items=(
      SimpleNamespace(dataset_key="serving.Customer"),
    ),
  )
  calls: dict[str, Any] = {}

  class FakeStateService:
    def build_current_state(self):
      return current_state

  class FakeStateStore:
    def __init__(self, *args, **kwargs):
      self.base_path = Path(".elevata/state/dev/dwh")

    def state_file_path(self):
      return self.base_path / "architecture_state.json"

  monkeypatch.setattr(control, "ArchitectureStateService", FakeStateService)
  monkeypatch.setattr(control, "ArchitectureStateStore", FakeStateStore)
  monkeypatch.setattr(
    control,
    "resolve_architecture_artifact_context",
    _artifact_context,
  )
  monkeypatch.setattr(
    control,
    "build_architecture_control_report_with_baseline",
    lambda *args, **kwargs: (report, baseline_resolution),
  )
  monkeypatch.setattr(
    control,
    "build_architecture_review_status_for_report",
    lambda **kwargs: status,
  )

  def fake_build_impact(**kwargs):
    calls.update(kwargs)
    return impact_plan, None

  monkeypatch.setattr(
    control,
    "_build_architecture_control_execution_impact_plan",
    fake_build_impact,
  )

  context = control.build_architecture_control_context(
    scope,
    execution_dataset_keys=("serving.Customer",),
    dependency_mode="target_only",
  )

  assert calls["execution_dataset_keys"] == ("serving.Customer",)
  assert calls["dependency_mode"] == "target_only"
  assert context.execution_dataset_keys == ("serving.Customer",)
  assert context.execution_dependency_mode == "target_only"


def test_execution_impact_plan_error_remains_optional_context_evidence(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify a plan-service error remains an explicit optional context outcome.
  """
  class FakeTargetDatasetManager:
    def select_related(self, *args):
      return self

    def filter(self, **kwargs):
      return self

    def order_by(self, *args):
      return ()

  monkeypatch.setattr(
    control.TargetDataset,
    "objects",
    FakeTargetDatasetManager(),
  )
  monkeypatch.setattr(
    control,
    "ArchitectureExecutionRecordStore",
    lambda **kwargs: object(),
  )
  monkeypatch.setattr(
    control,
    "build_execution_impact_plan",
    lambda **kwargs: (_ for _ in ()).throw(
      control.ExecutionImpactPlanError("impact evidence unavailable")
    ),
  )

  plan, error = control._build_architecture_control_execution_impact_plan(
    scope=control.ArchitectureControlScope.for_all(),
    artifact_context=_artifact_context(),
    current_state=object(),
    baseline_resolution=_baseline_resolution(),
    report=FakeReport(),
    review_status=_status("pending"),
  )

  assert plan is None
  assert error == "impact evidence unavailable"


def test_execution_impact_integration_error_is_isolated(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify unexpected integration failures do not remove the control context.
  """
  class FailingTargetDatasetManager:
    def select_related(self, *args):
      raise RuntimeError("target metadata unavailable")

  monkeypatch.setattr(
    control.TargetDataset,
    "objects",
    FailingTargetDatasetManager(),
  )

  plan, error = control._build_architecture_control_execution_impact_plan(
    scope=control.ArchitectureControlScope.for_all(),
    artifact_context=_artifact_context(),
    current_state=object(),
    baseline_resolution=_baseline_resolution(),
    report=FakeReport(),
    review_status=_status("pending"),
  )

  assert plan is None
  assert error == (
    "Execution Impact Plan integration failed: target metadata unavailable"
  )


def test_create_architecture_control_approval_saves_artifact(
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

  _patch_control_context(monkeypatch, context)
  monkeypatch.setattr(
    control,
    "build_architecture_approval_artifact",
    fake_build_architecture_approval_artifact,
  )

  result = control.create_architecture_control_approval(
    context.scope,
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
      "No architecture changes are present for this architecture scope.",
    ),
    (
      _context(report=FakeReport(is_blocked=True)),
      "Architecture approval is disabled because the report is blocked by policy.",
    ),
    (
      _context(status=_status("approved")),
      "A matching approval artifact already exists for this report.",
    ),
    (
      _context(status=_status("initial_deployment")),
      (
        "An Approval Artifact is not required for a verified "
        "initial deployment."
      ),
    ),
  ],
)
def test_create_architecture_control_approval_rejects_ineligible_reports(
  monkeypatch: pytest.MonkeyPatch,
  context: control.ArchitectureControlContext,
  message: str,
) -> None:
  """
  Verify approval guards for no-change, blocked and approved reports.
  """
  _patch_control_context(monkeypatch, context)

  with pytest.raises(control.ArchitectureControlError, match=message):
    control.create_architecture_control_approval(
      context.scope,
      approved_by="Ilona",
    )


def test_check_architecture_control_approval_returns_invalid_on_store_error(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify invalid check result for unreadable approval artifacts.
  """
  store = FakeApprovalStore(
    load_error=ArchitectureApprovalError("Approval artifact JSON is invalid."),
  )
  context = _context(store=store)
  _patch_control_context(monkeypatch, context)

  result = control.check_architecture_control_approval(context.scope)

  assert result.is_valid is False
  assert result.status == "invalid"
  assert result.message == "Approval artifact JSON is invalid."
  assert result.report_fingerprint == "report-1"


def test_check_architecture_control_approval_returns_missing_status(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify missing result when no approval artifact exists.
  """
  context = _context(status=_status("pending"))
  _patch_control_context(monkeypatch, context)

  result = control.check_architecture_control_approval(context.scope)

  assert result.is_valid is False
  assert result.status == "missing"
  assert result.message == "No approval artifact exists for the report fingerprint."
  assert result.report_fingerprint == "report-1"


def test_check_architecture_control_approval_validates_loaded_artifact(
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

  _patch_control_context(monkeypatch, context)
  monkeypatch.setattr(
    control,
    "check_architecture_approval",
    fake_check_architecture_approval,
  )

  result = control.check_architecture_control_approval(context.scope)

  assert result is expected
  assert store.loaded_report_fingerprint == "report-1"
  assert calls["report_payload"]["report_fingerprint"] == "report-1"
  assert calls["approval_payload"] == approval_payload