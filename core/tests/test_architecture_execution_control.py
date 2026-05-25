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

from types import SimpleNamespace

import pytest
from django.core.management.base import CommandError

from metadata.architecture.control import ArchitectureControlScope
from metadata.architecture import execution_control


class FakeExecutionRecordStore:
  """
  Execution record store test double.
  """

  def __init__(self):
    self.saved_record = None

  def save(self, record):
    """
    Capture the stored execution record.
    """
    self.saved_record = record
    return f".elevata/executions/{record.execution_id}.execution.json"


def _preview(
  *,
  can_execute: bool = True,
  dependency_mode: str = "with_dependencies",
) -> SimpleNamespace:
  """
  Return an execution-preview-shaped object.
  """
  return SimpleNamespace(
    scope_key="all",
    scope_label="All datasets",
    dependency_mode=dependency_mode,
    report_fingerprint="report-1",
    approval_id="apr_123",
    preview_fingerprint="preview-1",
    gate=SimpleNamespace(
      can_execute=can_execute,
      message="Execution requires a matching approval artifact.",
    ),
  )


def _patch_control_dependencies(
  monkeypatch,
  *,
  can_execute: bool = True,
) -> None:
  """
  Patch Architecture Control context and preview dependencies.
  """
  monkeypatch.setattr(
    execution_control,
    "build_architecture_control_context",
    lambda scope: SimpleNamespace(),
  )
  monkeypatch.setattr(
    execution_control,
    "build_architecture_execution_preview",
    lambda scope, *, control_context=None, no_deps=False: _preview(
      can_execute=can_execute,
      dependency_mode="target_only" if no_deps else "with_dependencies",
    ),
  )


def test_execute_architecture_control_scope_runs_all_scope(
  monkeypatch,
) -> None:
  """
  Verify controlled execution command construction for all datasets.
  """
  _patch_control_dependencies(monkeypatch)

  calls = {}

  def fake_command_runner(command_name, *args, **options):
    """
    Capture command invocation and write command output.
    """
    calls["command_name"] = command_name
    calls["args"] = args
    calls["options"] = options
    options["stdout"].write("done\n")

  result = execution_control.execute_architecture_control_scope(
    ArchitectureControlScope.for_all(),
    actor="Ilona",
    command_runner=fake_command_runner,
    record_store=FakeExecutionRecordStore(),
  )

  assert result.succeeded is True
  assert result.message == "Architecture execution completed for All datasets by Ilona."
  assert calls["command_name"] == "elevata_load"
  assert calls["args"] == ()
  assert calls["options"]["all_datasets"] is True
  assert calls["options"]["execute"] is True
  assert calls["options"]["no_print"] is False
  assert result.output_lines == ("done",)
  assert result.output_tail == ("done",)
  assert result.output_truncated is False
  assert result.execution_id
  assert result.execution_record_path.endswith(".execution.json")
  assert result.execution_record_fingerprint


def test_execute_architecture_control_scope_runs_schema_scope(
  monkeypatch,
) -> None:
  """
  Verify controlled execution command construction for a schema scope.
  """
  _patch_control_dependencies(monkeypatch)

  calls = {}

  def fake_command_runner(command_name, *args, **options):
    """
    Capture command invocation.
    """
    calls["command_name"] = command_name
    calls["args"] = args
    calls["options"] = options

  result = execution_control.execute_architecture_control_scope(
    ArchitectureControlScope.for_schema("serving"),
    command_runner=fake_command_runner,
    record_store=FakeExecutionRecordStore(),
  )

  assert result.succeeded is True
  assert calls["command_name"] == "elevata_load"
  assert calls["args"] == ()
  assert calls["options"]["all_datasets"] is True
  assert calls["options"]["schema_short"] == "serving"
  assert calls["options"]["execute"] is True
  assert calls["options"]["no_print"] is False
  assert calls["options"]["no_deps"] is False


def test_execute_architecture_control_scope_runs_target_dataset_scope(
  monkeypatch,
) -> None:
  """
  Verify controlled execution command construction for a TargetDataset scope.
  """
  _patch_control_dependencies(monkeypatch)

  calls = {}

  def fake_command_runner(command_name, *args, **options):
    """
    Capture command invocation.
    """
    calls["command_name"] = command_name
    calls["args"] = args
    calls["options"] = options

  scope = ArchitectureControlScope(
    mode="target_dataset",
    schema_short="serving",
    target_name="Customer",
    dataset_key="serving.Customer",
  )

  result = execution_control.execute_architecture_control_scope(
    scope,
    command_runner=fake_command_runner,
    record_store=FakeExecutionRecordStore(),
  )

  assert result.succeeded is True
  assert calls["command_name"] == "elevata_load"
  assert calls["args"] == ("Customer",)
  assert calls["options"]["schema_short"] == "serving"
  assert calls["options"]["execute"] is True
  assert calls["options"]["no_print"] is False
  assert calls["options"]["no_deps"] is False


def test_execute_architecture_control_scope_blocks_when_gate_is_closed(
  monkeypatch,
) -> None:
  """
  Verify controlled execution refuses closed execution gates.
  """
  _patch_control_dependencies(monkeypatch, can_execute=False)

  with pytest.raises(
    execution_control.ArchitectureControlledExecutionError,
    match="Execution requires a matching approval artifact.",
  ):
    execution_control.execute_architecture_control_scope(
      ArchitectureControlScope.for_all(),
      record_store=FakeExecutionRecordStore(),
    )


def test_execute_architecture_control_scope_returns_failed_result_on_command_error(
  monkeypatch,
) -> None:
  """
  Verify command errors are returned as failed execution results.
  """
  _patch_control_dependencies(monkeypatch)

  def fake_command_runner(command_name, *args, **options):
    """
    Raise a controlled command error.
    """
    options["stderr"].write("blocked\n")
    raise CommandError("Execution failed.")

  result = execution_control.execute_architecture_control_scope(
    ArchitectureControlScope.for_all(),
    command_runner=fake_command_runner,
    record_store=FakeExecutionRecordStore(),
  )

  assert result.succeeded is False
  assert result.status == "failed"
  assert result.message == "Execution failed."
  assert result.error_lines == (
    "blocked",
    "Execution failed.",
  )
  assert result.error_tail == (
    "blocked",
    "Execution failed.",
  )
  assert result.error_truncated is False


def test_execute_architecture_control_scope_enforces_architecture_guard(
  monkeypatch,
) -> None:
  """
  Verify controlled execution enforces Architecture Guard during command execution.
  """
  _patch_control_dependencies(monkeypatch)
  monkeypatch.delenv("ELEVATA_ARCH_MODE", raising=False)

  captured = {}

  def fake_command_runner(command_name, *args, **options):
    """
    Capture Architecture Guard mode during command execution.
    """
    captured["arch_mode"] = execution_control.os.environ.get("ELEVATA_ARCH_MODE")

  execution_control.execute_architecture_control_scope(
    ArchitectureControlScope.for_all(),
    command_runner=fake_command_runner,
    record_store=FakeExecutionRecordStore(),
  )

  assert captured["arch_mode"] == "enforce"
  assert execution_control.os.environ.get("ELEVATA_ARCH_MODE") is None


def test_execute_architecture_control_scope_runs_target_dataset_without_dependencies(
  monkeypatch,
) -> None:
  """
  Verify controlled execution can run a TargetDataset without upstream dependencies.
  """
  _patch_control_dependencies(monkeypatch)

  calls = {}

  def fake_command_runner(command_name, *args, **options):
    """
    Capture command invocation.
    """
    calls["command_name"] = command_name
    calls["args"] = args
    calls["options"] = options

  scope = ArchitectureControlScope(
    mode="target_dataset",
    schema_short="bizcore",
    target_name="Customer",
    dataset_key="bizcore.Customer",
  )

  result = execution_control.execute_architecture_control_scope(
    scope,
    no_deps=True,
    command_runner=fake_command_runner,
    record_store=FakeExecutionRecordStore(),
  )

  assert result.succeeded is True
  assert calls["command_name"] == "elevata_load"
  assert calls["args"] == ("Customer",)
  assert calls["options"]["schema_short"] == "bizcore"
  assert calls["options"]["execute"] is True
  assert calls["options"]["no_deps"] is True