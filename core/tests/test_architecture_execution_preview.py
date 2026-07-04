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
) -> SimpleNamespace:
  """
  Return an ArchitectureBaselineResolution-shaped object for execution preview tests.
  """
  state_file = Path(".elevata/state/dev/dwh/architecture_state.json")
  return SimpleNamespace(
    previous_state=SimpleNamespace(),
    source="recorded_state" if can_execute else "missing_or_unsupported",
    can_execute=can_execute,
    message="baseline message",
    state_file=state_file,
    warning_count=0,
    warnings=(),
    is_recorded=can_execute,
    is_discovered=False,
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