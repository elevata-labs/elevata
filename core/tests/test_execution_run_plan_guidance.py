"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2026 Ilona Tag

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

from metadata.architecture.control import (
  ArchitectureControlError,
  ArchitectureControlScope,
)
from metadata.architecture import (
  execution_run_plan_service as service,
)


def _artifact_context() -> SimpleNamespace:
  """
  Return a runtime artifact context test double.
  """
  return SimpleNamespace(
    profile_name="dev",
    target_system_short="msdwh",
  )


def _execution_scope() -> SimpleNamespace:
  """
  Return an execution-scope resolution test double.
  """
  return SimpleNamespace(
    execution_dataset_keys=(
      "raw.customer",
    ),
    dependency_mode="with_dependencies",
  )


def test_block_message_preserves_control_detail() -> None:
  """
  Verify actionable guidance retains the authoritative failure.
  """
  message = (
    service
    .build_execution_run_plan_block_message(
      "Architecture Change Report contains "
      "blocking policy decisions."
    )
  )

  assert message.startswith(
    "Architecture Change Report contains "
    "blocking policy decisions."
  )
  assert (
    "An Approval Artifact does not override "
    "blocking policy decisions."
    in message
  )
  assert (
    "regenerate the execution manifest"
    in message
  )
  assert (
    "Start a new DAG run."
    in message
  )


def test_block_message_uses_default_detail() -> None:
  """
  Verify empty upstream messages still produce useful guidance.
  """
  message = (
    service
    .build_execution_run_plan_block_message(
      ""
    )
  )

  assert message.startswith(
    "Execution Run Plan creation is blocked by "
    "Architecture Control."
  )


def test_control_error_contains_scheduler_guidance(
  monkeypatch,
) -> None:
  """
  Verify Architecture Control failures receive operator guidance.
  """
  monkeypatch.setattr(
    service,
    "resolve_architecture_execution_scope",
    lambda scope, *, no_deps=False: (
      _execution_scope()
    ),
  )

  def fail_control_context(
    scope,
    **kwargs,
  ):
    raise ArchitectureControlError(
      "Architecture Change Report contains "
      "blocking policy decisions."
    )

  monkeypatch.setattr(
    service,
    "build_architecture_control_context",
    fail_control_context,
  )

  with pytest.raises(
    service.ArchitectureExecutionRunPlanError,
  ) as exc_info:
    service.build_architecture_execution_run_plan(
      ArchitectureControlScope.for_all(),
      artifact_context=_artifact_context(),
    )

  message = str(
    exc_info.value
  )

  assert (
    "Architecture Change Report contains "
    "blocking policy decisions."
    in message
  )
  assert (
    "does not override blocking policy decisions"
    in message
  )
  assert (
    "Start a new DAG run."
    in message
  )


def test_closed_execution_gate_contains_guidance(
  monkeypatch,
) -> None:
  """
  Verify missing approval gates receive the same scheduler workflow.
  """
  monkeypatch.setattr(
    service,
    "resolve_architecture_execution_scope",
    lambda scope, *, no_deps=False: (
      _execution_scope()
    ),
  )
  monkeypatch.setattr(
    service,
    "build_architecture_control_context",
    lambda scope, **kwargs: (
      SimpleNamespace()
    ),
  )
  monkeypatch.setattr(
    service,
    "build_architecture_execution_preview",
    lambda scope, **kwargs: SimpleNamespace(
      gate=SimpleNamespace(
        can_execute=False,
        message=(
          "Execution requires a matching "
          "approval artifact."
        ),
      ),
    ),
  )

  with pytest.raises(
    service.ArchitectureExecutionRunPlanError,
  ) as exc_info:
    service.build_architecture_execution_run_plan(
      ArchitectureControlScope.for_all(),
      artifact_context=_artifact_context(),
    )

  message = str(
    exc_info.value
  )

  assert (
    "Execution requires a matching "
    "approval artifact."
    in message
  )
  assert (
    "When the report is approvable"
    in message
  )
  assert (
    "Do not reuse an existing Execution Run Plan."
    in message
  )
