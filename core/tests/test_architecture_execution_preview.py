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

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from metadata.architecture.control import (
  ArchitectureControlContext,
  ArchitectureControlScope,
)
from metadata.architecture import execution_preview


def _artifact_context() -> SimpleNamespace:
  """
  Return an ArchitectureArtifactContext-shaped object for execution preview tests.
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
  Return an ArchitectureStateStore-shaped object for execution preview tests.
  """
  state_file = Path(".elevata/state/dev/dwh/architecture_state.json")
  return SimpleNamespace(
    base_path=state_file.parent,
    state_file_path=lambda: state_file,
  )


def _baseline_resolution(
  *,
  can_execute: bool = True,
  source: str | None = None,
) -> SimpleNamespace:
  """
  Return an ArchitectureBaselineResolution-shaped object for execution preview tests.
  """
  state_file = Path(".elevata/state/dev/dwh/architecture_state.json")
  resolved_source = (
    source
    or (
      "recorded_state"
      if can_execute
      else "missing_or_unsupported"
    )
  )
  return SimpleNamespace(
    previous_state=SimpleNamespace(),
    source=resolved_source,
    can_execute=can_execute,
    message="baseline message",
    state_file=state_file,
    warning_count=0,
    warnings=(),
    is_recorded=resolved_source == "recorded_state",
    is_discovered=resolved_source in {
      "discovered_physical_state",
      "verified_empty_target",
    },
    is_initial_deployment=resolved_source == "verified_empty_target",
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


def _target_dataset(
  schema_short: str,
  target_name: str,
  pk: int,
) -> SimpleNamespace:
  """
  Return a TargetDataset-shaped object for execution preview tests.
  """
  return SimpleNamespace(
    pk=pk,
    id=pk,
    target_schema=SimpleNamespace(short_name=schema_short),
    target_dataset_name=target_name,
  )


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
    approval_id=approval_id,
  )


def _context(
  *,
  report: FakeReport | None = None,
  status: SimpleNamespace | None = None,
) -> ArchitectureControlContext:
  """
  Return an ArchitectureControlContext for execution preview tests.
  """
  return ArchitectureControlContext(
    scope=ArchitectureControlScope.for_all(),
    artifact_context=_artifact_context(),
    report=report or FakeReport(),
    review_status=status or _status("approved", approval_id="apr_123"),
    approval_store=SimpleNamespace(),
    state_store=_state_store(),
    baseline_resolution=_baseline_resolution(),
  )


def _patch_execution_scope(
  monkeypatch,
  *,
  roots: list[Any],
  execution_order: list[Any],
) -> None:
  """
  Patch root and execution-order resolution.
  """
  monkeypatch.setattr(
    execution_preview,
    "_resolve_execution_roots",
    lambda scope: roots,
  )
  monkeypatch.setattr(
    execution_preview,
    "_resolve_execution_order",
    lambda scope, resolved_roots, *, no_deps=False: (
      resolved_roots if no_deps else execution_order
    ),
  )


def test_build_architecture_execution_preview_marks_approved_scope_ready(
  monkeypatch,
) -> None:
  """
  Verify execution preview readiness for an approved architecture scope.
  """
  root = _target_dataset("serving", "Customer", 1)
  upstream = _target_dataset("rawcore", "Customer", 2)
  context = _context()

  _patch_execution_scope(
    monkeypatch,
    roots=[root],
    execution_order=[upstream, root],
  )

  preview = execution_preview.build_architecture_execution_preview(
    context.scope,
    control_context=context,
  )

  assert preview.scope_key == "all"
  assert preview.report_fingerprint == "report-1"
  assert preview.approval_id == "apr_123"
  assert preview.review_status == "approved"
  assert preview.dependency_mode == "with_dependencies"
  assert preview.root_dataset_keys == ("serving.Customer",)
  assert preview.execution_dataset_keys == (
    "rawcore.Customer",
    "serving.Customer",
  )
  assert preview.step_count == 2
  assert preview.gate.status == "ready"
  assert preview.gate.can_execute is True


def test_build_architecture_execution_preview_blocks_policy_blocked_report(
  monkeypatch,
) -> None:
  """
  Verify execution preview gate for policy-blocked reports.
  """
  root = _target_dataset("serving", "Customer", 1)
  context = _context(
    report=FakeReport(is_blocked=True),
    status=_status("approved", approval_id="apr_123"),
  )

  _patch_execution_scope(
    monkeypatch,
    roots=[root],
    execution_order=[root],
  )

  preview = execution_preview.build_architecture_execution_preview(
    context.scope,
    control_context=context,
  )

  assert preview.gate.status == "blocked_by_policy"
  assert preview.gate.can_execute is False


def test_build_architecture_execution_preview_requires_approval(
  monkeypatch,
) -> None:
  """
  Verify execution preview gate for unapproved reports.
  """
  root = _target_dataset("serving", "Customer", 1)
  context = _context(status=_status("pending"))

  _patch_execution_scope(
    monkeypatch,
    roots=[root],
    execution_order=[root],
  )

  preview = execution_preview.build_architecture_execution_preview(
    context.scope,
    control_context=context,
  )

  assert preview.gate.status == "pending_approval"
  assert preview.gate.can_execute is False


def test_build_architecture_execution_preview_allows_verified_initial_deployment(
  monkeypatch,
) -> None:
  """
  Verify full-scope initial deployment is executable without approval.
  """
  root = _target_dataset("raw", "Customer", 1)
  context = replace(
    _context(
      status=_status("initial_deployment"),
    ),
    baseline_resolution=_baseline_resolution(
      source="verified_empty_target",
    ),
  )

  _patch_execution_scope(
    monkeypatch,
    roots=[root],
    execution_order=[root],
  )

  preview = execution_preview.build_architecture_execution_preview(
    context.scope,
    control_context=context,
  )

  assert preview.approval_id is None
  assert preview.review_status == "initial_deployment"
  assert preview.gate.status == "ready_initial_deployment"
  assert preview.gate.can_execute is True


def test_execution_gate_rejects_unverified_initial_deployment_status() -> None:
  """
  Verify a status string alone cannot bypass baseline verification.
  """
  context = _context(
    status=_status("initial_deployment"),
  )

  gate = execution_preview._build_execution_gate(context)

  assert gate.status == "pending_approval"
  assert gate.can_execute is False


def test_build_architecture_execution_preview_marks_no_change_scope(
  monkeypatch,
) -> None:
  """
  Verify execution preview gate for scopes without architecture changes.
  """
  root = _target_dataset("serving", "Customer", 1)
  context = _context(
    report=FakeReport(has_changes=False),
    status=_status("no_changes"),
  )

  _patch_execution_scope(
    monkeypatch,
    roots=[root],
    execution_order=[root],
  )

  preview = execution_preview.build_architecture_execution_preview(
    context.scope,
    control_context=context,
  )

  assert preview.gate.status == "ready_no_changes"
  assert preview.gate.can_execute is True


def test_execution_gate_blocks_blocked_impact_decisions() -> None:
  """
  Verify no-change scopes cannot bypass blocked impact decisions.
  """
  context = replace(
    _context(
      report=FakeReport(has_changes=False),
      status=_status("no_changes"),
    ),
    execution_impact_plan=SimpleNamespace(
      decision_counts={
        "REUSE": 0,
        "REVALIDATE": 0,
        "INCREMENTAL_EXECUTE": 0,
        "FULL_REBUILD": 0,
        "BLOCKED": 1,
      },
    ),
  )

  gate = execution_preview._build_execution_gate(context)

  assert gate.status == "impact_plan_blocked"
  assert gate.can_execute is False
  assert "1 blocked dataset" in gate.message


def test_execution_gate_blocks_revalidation_decisions() -> None:
  """
  Verify REVALIDATE remains non-executable until runtime support exists.
  """
  context = replace(
    _context(
      report=FakeReport(has_changes=False),
      status=_status("no_changes"),
    ),
    execution_impact_plan=SimpleNamespace(
      decision_counts={
        "REUSE": 0,
        "REVALIDATE": 1,
        "INCREMENTAL_EXECUTE": 0,
        "FULL_REBUILD": 0,
        "BLOCKED": 0,
      },
    ),
  )

  gate = execution_preview._build_execution_gate(context)

  assert gate.status == "impact_revalidation_required"
  assert gate.can_execute is False
  assert "requires revalidation" in gate.message


def test_resolve_execution_order_uses_all_roots_for_schema_scope(
  monkeypatch,
) -> None:
  """
  Verify schema scopes resolve execution order from all selected roots.
  """
  root = _target_dataset("serving", "Customer", 1)
  calls = {}

  def fake_resolve_execution_order_all(roots):
    """
    Capture roots and return them as execution order.
    """
    calls["roots"] = roots
    return roots

  monkeypatch.setattr(
    execution_preview,
    "resolve_execution_order_all",
    fake_resolve_execution_order_all,
  )

  scope = ArchitectureControlScope.for_schema("serving")
  result = execution_preview._resolve_execution_order(
    scope,
    [root],
    no_deps=False,
  )

  assert result == [root]
  assert calls["roots"] == [root]


def test_build_architecture_execution_preview_supports_target_only_execution(
  monkeypatch,
) -> None:
  """
  Verify target-only execution preview for TargetDataset scope.
  """
  root = _target_dataset("bizcore", "Customer", 1)
  scope = ArchitectureControlScope(
    mode="target_dataset",
    schema_short="bizcore",
    target_name="Customer",
    dataset_key="bizcore.Customer",
  )
  context = ArchitectureControlContext(
    scope=scope,
    artifact_context=_artifact_context(),
    report=FakeReport(),
    review_status=_status("approved", approval_id="apr_123"),
    approval_store=SimpleNamespace(),
    state_store=_state_store(),
    baseline_resolution=_baseline_resolution(),
  )

  monkeypatch.setattr(
    execution_preview,
    "_resolve_execution_roots",
    lambda selected_scope: [root],
  )
  monkeypatch.setattr(
    execution_preview,
    "resolve_execution_order",
    lambda selected_root: (_ for _ in ()).throw(
      AssertionError("Target-only preview must not resolve upstream dependencies.")
    ),
  )

  preview = execution_preview.build_architecture_execution_preview(
    scope,
    control_context=context,
    no_deps=True,
  )

  assert preview.dependency_mode == "target_only"
  assert preview.root_dataset_keys == ("bizcore.Customer",)
  assert preview.execution_dataset_keys == ("bizcore.Customer",)
  assert preview.step_count == 1


def test_build_architecture_execution_preview_binds_execution_impact_plan(
  monkeypatch,
) -> None:
  """
  Verify preview fingerprints include deterministic Execution Impact evidence.
  """
  root = _target_dataset("serving", "Customer", 1)
  upstream = _target_dataset("rawcore", "Customer", 2)

  decision_counts = {
    "REUSE": 1,
    "REVALIDATE": 0,
    "INCREMENTAL_EXECUTE": 0,
    "FULL_REBUILD": 1,
    "BLOCKED": 0,
  }
  impact_plan = SimpleNamespace(
    scope_key="all",
    report_fingerprint="report-1",
    plan_fingerprint="impact-plan-1",
    assessed_count=2,
    decision_counts=decision_counts,
    items=(
      SimpleNamespace(dataset_key="rawcore.Customer"),
      SimpleNamespace(dataset_key="serving.Customer"),
    ),
  )
  context = replace(
    _context(),
    execution_impact_plan=impact_plan,
  )

  _patch_execution_scope(
    monkeypatch,
    roots=[root],
    execution_order=[upstream, root],
  )

  preview = execution_preview.build_architecture_execution_preview(
    context.scope,
    control_context=context,
  )

  assert preview.impact_plan_binding is not None
  assert preview.impact_plan_binding.plan_fingerprint == "impact-plan-1"
  assert preview.impact_plan_binding.assessed_count == 2
  assert dict(preview.impact_plan_binding.decision_counts) == decision_counts

  changed_context = replace(
    context,
    execution_impact_plan=SimpleNamespace(
      scope_key="all",
      report_fingerprint="report-1",
      plan_fingerprint="impact-plan-2",
      assessed_count=2,
      decision_counts=decision_counts,
      items=(
        SimpleNamespace(dataset_key="rawcore.Customer"),
        SimpleNamespace(dataset_key="serving.Customer"),
      ),
    ),
  )
  changed_preview = execution_preview.build_architecture_execution_preview(
    changed_context.scope,
    control_context=changed_context,
  )

  assert changed_preview.preview_fingerprint != preview.preview_fingerprint


def test_resolve_architecture_execution_scope_target_only_excludes_dependencies(
  monkeypatch,
) -> None:
  """
  Verify target-only execution resolves exactly the selected root dataset.
  """
  root = _target_dataset("rawcore", "Customer", 1)
  upstream = _target_dataset("stage", "Customer", 2)

  _patch_execution_scope(
    monkeypatch,
    roots=[root],
    execution_order=[upstream, root],
  )

  resolution = (
    execution_preview.resolve_architecture_execution_scope(
      ArchitectureControlScope(
        mode="target_dataset",
        schema_short="rawcore",
        target_name="Customer",
        dataset_key="rawcore.Customer",
      ),
      no_deps=True,
    )
  )

  assert resolution.dependency_mode == "target_only"
  assert resolution.root_dataset_keys == ("rawcore.Customer",)
  assert resolution.execution_dataset_keys == ("rawcore.Customer",)


def test_build_architecture_execution_preview_blocks_impact_plan_errors(
  monkeypatch,
) -> None:
  """
  Verify controlled execution closes when impact planning failed explicitly.
  """
  root = _target_dataset("serving", "Customer", 1)
  context = replace(
    _context(),
    execution_impact_plan=None,
    execution_impact_plan_error="Impact evidence resolution failed.",
  )

  _patch_execution_scope(
    monkeypatch,
    roots=[root],
    execution_order=[root],
  )

  preview = execution_preview.build_architecture_execution_preview(
    context.scope,
    control_context=context,
  )

  assert preview.impact_plan_binding is None
  assert preview.gate.status == "impact_plan_unavailable"
  assert preview.gate.can_execute is False
  assert "Impact evidence resolution failed." in preview.gate.message
