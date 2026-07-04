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

from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import replace
from datetime import datetime, timezone
from io import StringIO
import os
import re
from time import perf_counter
import uuid
from typing import Any, Callable, Literal

from django.core.management import call_command
from django.core.management.base import CommandError

from metadata.architecture.control import (
  ArchitectureControlScope,
  build_architecture_control_context,
)
from metadata.architecture.execution_preview import (
  ArchitectureExecutionPreview,
  build_architecture_execution_preview,
)
from metadata.architecture.execution_record import (
  ArchitectureExecutionRecordStore,
  build_architecture_execution_record,
)


ArchitectureControlledExecutionStatus = Literal[
  "success",
  "failed",
]


class ArchitectureControlledExecutionError(ValueError):
  """
  Raised when Architecture Control execution cannot run.
  """

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

@dataclass(frozen=True)
class ArchitectureControlledExecutionResult:
  """
  Result of a controlled Architecture Control execution.
  """
  execution_id: str
  started_by: str
  started_at: str
  finished_at: str
  duration_ms: int
  status: ArchitectureControlledExecutionStatus
  message: str
  scope_key: str
  scope_label: str
  dependency_mode: str
  report_fingerprint: str
  approval_id: str | None
  preview_fingerprint: str
  command_name: str
  command_args: tuple[str, ...]
  command_options: dict[str, Any]
  output_lines: tuple[str, ...]
  output_tail: tuple[str, ...]
  output_truncated: bool
  error_lines: tuple[str, ...]
  error_tail: tuple[str, ...]
  error_truncated: bool
  execution_record_path: str | None = None
  execution_record_fingerprint: str | None = None
  execution_record_error: str | None = None

  @property
  def succeeded(self) -> bool:
    """
    Return True when the controlled execution completed successfully.
    """
    return self.status == "success"


def execute_architecture_control_scope(
  scope: ArchitectureControlScope,
  *,
  actor: str = "",
  no_deps: bool = False,
  command_runner: Callable[..., Any] = call_command,
  record_store: ArchitectureExecutionRecordStore | None = None,
) -> ArchitectureControlledExecutionResult:
  """
  Execute an approved Architecture Control scope through the controlled load path.
  """
  execution_id = uuid.uuid4().hex
  started_by = actor
  started_at = _utc_now_iso()
  started_perf = perf_counter()

  context = build_architecture_control_context(scope)
  preview = build_architecture_execution_preview(
    scope,
    control_context=context,
    no_deps=no_deps,
  )

  if not preview.gate.can_execute:
    raise ArchitectureControlledExecutionError(preview.gate.message)

  command_name, command_args, command_options = _build_elevata_load_command(
    scope,
    no_deps=no_deps,
  )

  stdout = StringIO()
  stderr = StringIO()
  command_options = {
    **command_options,
    "stdout": stdout,
    "stderr": stderr,
  }

  try:
    with _architecture_guard_enforced():
      command_runner(
        command_name,
        *command_args,
        **command_options,
      )
  except CommandError as exc:
    _write_error(stderr, str(exc))
    result = _execution_result(
      execution_id=execution_id,
      started_by=started_by,
      started_at=started_at,
      started_perf=started_perf,
      status="failed",
      message=_controlled_error_message(str(exc)),
      preview=preview,
      command_name=command_name,
      command_args=command_args,
      command_options=command_options,
      stdout=stdout,
      stderr=stderr,
    )
    return _attach_execution_record(result, record_store=record_store)
  
  except Exception as exc:
    _write_error(stderr, str(exc))
    result = _execution_result(
      execution_id=execution_id,
      started_by=started_by,
      started_at=started_at,
      started_perf=started_perf,
      status="failed",
      message=str(exc),
      preview=preview,
      command_name=command_name,
      command_args=command_args,
      command_options=command_options,
      stdout=stdout,
      stderr=stderr,
    )
    return _attach_execution_record(result, record_store=record_store)

  actor_suffix = f" by {actor}" if actor else ""
  result = _execution_result(
    execution_id=execution_id,
    started_by=started_by,
    started_at=started_at,
    started_perf=started_perf,
    status="success",
    message=f"Architecture execution completed for {scope.label}{actor_suffix}.",
    preview=preview,
    command_name=command_name,
    command_args=command_args,
    command_options=command_options,
    stdout=stdout,
    stderr=stderr,
  )
  return _attach_execution_record(result, record_store=record_store)


def _build_elevata_load_command(
  scope: ArchitectureControlScope,
  *,
  no_deps: bool = False,
) -> tuple[str, tuple[str, ...], dict[str, Any]]:
  """
  Build the constrained elevata_load invocation for an Architecture Control scope.
  """
  options: dict[str, Any] = {
    "execute": True,
    "no_print": False,
    "no_deps": False,
  }

  if scope.mode == "all":
    options["all_datasets"] = True
    return "elevata_load", (), options

  if scope.mode == "schema":
    if not scope.schema_short:
      raise ArchitectureControlledExecutionError(
        "Schema scope requires a schema short name."
      )

    options["all_datasets"] = True
    options["schema_short"] = scope.schema_short
    return "elevata_load", (), options

  if scope.mode == "target_dataset":
    if not scope.schema_short or not scope.target_name:
      raise ArchitectureControlledExecutionError(
        "Target dataset scope requires a schema and dataset name."
      )

    options["schema_short"] = scope.schema_short
    options["no_deps"] = bool(no_deps)
    return "elevata_load", (scope.target_name,), options

  raise ArchitectureControlledExecutionError(
    f"Unsupported Architecture Control scope: {scope.mode}"
  )


def _execution_result(
  *,
  execution_id: str,
  started_by: str,
  started_at: str,
  started_perf: float,
  status: ArchitectureControlledExecutionStatus,
  message: str,
  preview: ArchitectureExecutionPreview,
  command_name: str,
  command_args: tuple[str, ...],
  command_options: dict[str, Any],
  stdout: StringIO,
  stderr: StringIO,
) -> ArchitectureControlledExecutionResult:
  """
  Build a normalized controlled execution result.
  """
  public_options = {
    key: value
    for key, value in command_options.items()
    if key not in {"stdout", "stderr"}
  }
  output_lines, output_truncated = _captured_lines(stdout.getvalue())
  error_lines, error_truncated = _captured_lines(stderr.getvalue())
  finished_at = _utc_now_iso()
  duration_ms = int((perf_counter() - started_perf) * 1000)

  return ArchitectureControlledExecutionResult(
    execution_id=execution_id,
    started_by=started_by,
    started_at=started_at,
    finished_at=finished_at,
    duration_ms=duration_ms,
    status=status,
    message=message,
    scope_key=preview.scope_key,
    scope_label=preview.scope_label,
    dependency_mode=preview.dependency_mode,
    report_fingerprint=preview.report_fingerprint,
    approval_id=preview.approval_id,
    preview_fingerprint=preview.preview_fingerprint,
    command_name=command_name,
    command_args=command_args,
    command_options=public_options,
    output_lines=output_lines,
    output_tail=_tail_from_lines(output_lines),
    output_truncated=output_truncated,
    error_lines=error_lines,
    error_tail=_tail_from_lines(error_lines),
    error_truncated=error_truncated,
  )


def _attach_execution_record(
  result: ArchitectureControlledExecutionResult,
  *,
  record_store: ArchitectureExecutionRecordStore | None,
) -> ArchitectureControlledExecutionResult:
  """
  Attach a persisted Architecture Execution Record to the result.
  """
  try:
    record = build_architecture_execution_record(result)
    store = record_store or ArchitectureExecutionRecordStore()
    record_path = store.save(record)
  except Exception as exc:
    return replace(
      result,
      execution_record_error=str(exc),
    )

  return replace(
    result,
    execution_record_path=str(record_path),
    execution_record_fingerprint=record.record_fingerprint,
  )


def _captured_lines(value: str, *, limit: int = 2000) -> tuple[tuple[str, ...], bool]:
  """
  Return captured command lines with a bounded retention limit.
  """
  lines = tuple(
    _strip_ansi(line).rstrip()
    for line in str(value or "").splitlines()
    if line.strip()
  )

  if len(lines) <= limit:
    return lines, False

  return lines[-limit:], True


def _tail_from_lines(lines: tuple[str, ...], *, limit: int = 20) -> tuple[str, ...]:
  """
  Return the last lines from captured command output.
  """
  return tuple(lines[-limit:])


def _tail_lines(value: str, *, limit: int = 20) -> tuple[str, ...]:
  """
  Return the last non-empty lines from a command stream.
  """
  lines, _truncated = _captured_lines(value)
  return _tail_from_lines(lines, limit=limit)


def _strip_ansi(value: str) -> str:
  """
  Remove terminal color escape sequences from captured command output.
  """
  return _ANSI_RE.sub("", str(value or ""))


def _write_error(stream: StringIO, message: str) -> None:
  """
  Write a command error message into the captured error stream.
  """
  if message:
    stream.write(f"{message}\n")


def _utc_now_iso() -> str:
  """
  Return the current UTC timestamp in ISO format.
  """
  return datetime.now(timezone.utc).isoformat()


def _controlled_error_message(message: str) -> str:
  """
  Return a user-facing message for controlled execution failures.
  """
  if "Architecture guard blocked execution" in message:
    return (
      "Architecture Guard blocked controlled execution. Architecture Control "
      "runs the guard in enforce mode even when the CLI environment uses compare. "
      "Inspect the execution output for shadow-compare details."
    )

  return message


@contextmanager
def _architecture_guard_enforced():
  """
  Run the controlled load path with Architecture Guard enforcement.

  This is intentionally stricter than direct CLI execution. The previous
  environment value is restored after the controlled command finishes.
  """
  previous = os.environ.get("ELEVATA_ARCH_MODE")
  os.environ["ELEVATA_ARCH_MODE"] = "enforce"

  try:
    yield
  finally:
    if previous is None:
      os.environ.pop("ELEVATA_ARCH_MODE", None)
    else:
      os.environ["ELEVATA_ARCH_MODE"] = previous