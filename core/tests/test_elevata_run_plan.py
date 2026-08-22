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

from io import StringIO
from types import SimpleNamespace

import pytest
from django.core.management.base import CommandError

from metadata.architecture.execution_impact import (
  ExecutionImpactSelection,
)
from metadata.architecture.execution_run_plan import (
  ExecutionRunPlanStore,
  build_execution_run_plan,
)
from metadata.architecture.execution_run_plan_state import (
  execution_run_plan_planned_state_path,
  save_execution_run_plan_bundle,
)
from metadata.architecture.paths import ArchitectureArtifactContext
from metadata.architecture.state import (
  ArchitectureState,
  DatasetState,
)
from metadata.management.commands.elevata_run_plan import (
  Command,
  _resolve_scope,
)
from metadata.management.commands import elevata_run_plan


def _fingerprint(character: str) -> str:
  """
  Return one deterministic SHA-256-shaped test fingerprint.
  """
  return character * 64


def _planned_state() -> ArchitectureState:
  """
  Return the immutable architecture snapshot used by command tests.
  """
  return ArchitectureState(
    datasets=(
      DatasetState(
        dataset_key="raw.customer",
        schema_short_name="raw",
        dataset_name="customer",
        materialization_type="table",
        incremental_strategy="full",
        historize=False,
        is_hist=False,
        active=True,
      ),
      DatasetState(
        dataset_key="rawcore.customer",
        schema_short_name="rawcore",
        dataset_name="customer",
        materialization_type="table",
        incremental_strategy="full",
        historize=False,
        is_hist=False,
        active=True,
      ),
    ),
  )


def _plan(
  *,
  planned_state: ArchitectureState | None = None,
  profile_name: str = "dev",
  target_system_short: str = "dwh",
):
  """
  Return one valid all-datasets Execution Run Plan.
  """
  state = planned_state or _planned_state()
  selection = ExecutionImpactSelection(
    plan_fingerprint=_fingerprint("c"),
    dataset_decisions=(
      ("raw.customer", "REUSE"),
      ("rawcore.customer", "FULL_REBUILD"),
    ),
  )
  return build_execution_run_plan(
    run_plan_id="run-plan-001",
    batch_run_id="batch-001",
    created_at="2026-07-24T04:15:00+00:00",
    profile_name=profile_name,
    target_system_short=target_system_short,
    scope_mode="all",
    scope_key="all",
    scope_label="All datasets",
    dependency_mode="with_dependencies",
    review_status="no_changes",
    approval_id=None,
    architecture_fingerprint=state.fingerprint,
    report_fingerprint=_fingerprint("b"),
    preview_fingerprint=_fingerprint("d"),
    execution_plan_fingerprint=_fingerprint("e"),
    root_dataset_keys=("raw.customer",),
    impact_selection=selection,
  )


def _options(**overrides):
  """
  Return a complete direct-handle option dictionary.
  """
  options = {
    "all_datasets": True,
    "schema_short": None,
    "dataset_key": None,
    "partial_load_name": None,
    "target_only": False,
    "profile": None,
    "target_system": None,
    "run_plan_id": None,
    "batch_run_id": None,
    "output": None,
    "reuse_existing": False,
    "print_json": False,
  }
  options.update(overrides)
  return options


def test_resolve_scope_supports_all_schema_and_target_only_dataset() -> None:
  """
  Verify every public run-plan scope maps to Architecture Control semantics.
  """
  all_scope, all_no_deps = _resolve_scope(_options())
  schema_scope, schema_no_deps = _resolve_scope(_options(
    all_datasets=False,
    schema_short="rawcore",
  ))
  dataset_scope, dataset_no_deps = _resolve_scope(_options(
    all_datasets=False,
    dataset_key="rawcore.customer",
    target_only=True,
  ))
  partial_scope, partial_no_deps = _resolve_scope(_options(
    all_datasets=False,
    partial_load_name="sales",
  ))

  assert all_scope.mode == "all"
  assert all_no_deps is False

  assert schema_scope.mode == "schema"
  assert schema_scope.schema_short == "rawcore"
  assert schema_no_deps is False

  assert dataset_scope.mode == "target_dataset"
  assert dataset_scope.dataset_key == "rawcore.customer"
  assert dataset_scope.schema_short == "rawcore"
  assert dataset_scope.target_name == "customer"
  assert dataset_no_deps is True

  assert partial_scope.mode == "partial_load"
  assert partial_scope.partial_load_name == "sales"
  assert partial_scope.key == "partial_load:sales"
  assert partial_no_deps is False


def test_resolve_scope_rejects_invalid_selector_combinations() -> None:
  """
  Verify direct use cannot bypass argparse's exclusive-scope contract.
  """
  with pytest.raises(CommandError, match="Select exactly one"):
    _resolve_scope(_options(
      schema_short="rawcore",
    ))

  with pytest.raises(
    CommandError,
    match="supported only together with --dataset",
  ):
    _resolve_scope(_options(
      target_only=True,
    ))

  with pytest.raises(
    CommandError,
    match="<schema>.<dataset>",
  ):
    _resolve_scope(_options(
      all_datasets=False,
      dataset_key="customer",
    ))


def _patch_runtime(monkeypatch) -> ArchitectureArtifactContext:
  """
  Patch active runtime resolution and return the expected context.
  """
  context = ArchitectureArtifactContext(
    profile_name="dev",
    target_system_short="dwh",
  )
  monkeypatch.setattr(
    elevata_run_plan,
    "load_profile",
    lambda value: SimpleNamespace(name="dev"),
  )
  monkeypatch.setattr(
    elevata_run_plan,
    "get_target_system",
    lambda value: SimpleNamespace(short_name="dwh"),
  )
  monkeypatch.setattr(
    elevata_run_plan,
    "resolve_architecture_artifact_context",
    lambda **kwargs: context,
  )
  return context


def test_command_builds_stores_and_reports_run_plan(
  monkeypatch,
  tmp_path,
) -> None:
  """
  Verify the public command assembles and stores one immutable plan.
  """
  expected_context = _patch_runtime(monkeypatch)
  planned_state = _planned_state()
  plan = _plan(planned_state=planned_state)
  captured = {}

  def fake_build(scope, **options):
    captured["scope"] = scope
    captured["options"] = options
    return SimpleNamespace(
      run_plan=plan,
      control_context=SimpleNamespace(
        current_state=planned_state,
      ),
    )

  monkeypatch.setattr(
    elevata_run_plan,
    "build_architecture_execution_run_plan",
    fake_build,
  )

  output_path = tmp_path / "scheduler" / "run-plan.json"
  stdout = StringIO()
  command = Command(stdout=stdout)

  returned_path = command.handle(**_options(
    output=str(output_path),
    run_plan_id="run-plan-001",
    batch_run_id="batch-001",
  ))

  assert returned_path == str(output_path)
  assert output_path.exists()
  assert execution_run_plan_planned_state_path(
    output_path
  ).exists()
  assert ExecutionRunPlanStore(
    base_path=tmp_path,
  ).load_path(output_path) == plan
  assert captured["scope"].mode == "all"
  assert captured["options"]["artifact_context"] == expected_context
  assert captured["options"]["no_deps"] is False
  assert captured["options"]["run_plan_id"] == "run-plan-001"
  assert captured["options"]["batch_run_id"] == "batch-001"

  output = stdout.getvalue()
  assert f"Execution Run Plan written: {output_path}" in output
  assert "Datasets: 2" in output
  assert "REUSE=1" in output
  assert (
    f"Run plan fingerprint: {plan.run_plan_fingerprint}"
    in output
  )


def test_command_reuses_matching_existing_plan_without_rebuilding(
  monkeypatch,
  tmp_path,
) -> None:
  """
  Verify scheduler retries reuse their persisted immutable plan.
  """
  _patch_runtime(monkeypatch)
  planned_state = _planned_state()
  plan = _plan(planned_state=planned_state)
  output_path = tmp_path / "run-plan.json"
  save_execution_run_plan_bundle(
    store=ExecutionRunPlanStore(base_path=tmp_path),
    plan=plan,
    planned_state=planned_state,
    output_path=output_path,
  )

  monkeypatch.setattr(
    elevata_run_plan,
    "build_architecture_execution_run_plan",
    lambda *args, **kwargs: pytest.fail(
      "matching existing plan should be reused"
    ),
  )

  stdout = StringIO()
  command = Command(stdout=stdout)
  returned_path = command.handle(**_options(
    output=str(output_path),
    reuse_existing=True,
    run_plan_id="run-plan-001",
    batch_run_id="batch-001",
  ))

  assert returned_path == str(output_path)
  assert (
    f"Execution Run Plan reused: {output_path}"
    in stdout.getvalue()
  )


def test_command_rejects_existing_plan_from_another_runtime(
  monkeypatch,
  tmp_path,
) -> None:
  """
  Verify retry reuse fails closed when runtime context differs.
  """
  _patch_runtime(monkeypatch)
  output_path = tmp_path / "run-plan.json"
  ExecutionRunPlanStore(base_path=tmp_path).save(
    _plan(target_system_short="other"),
    output_path=output_path,
  )

  command = Command(stdout=StringIO())
  with pytest.raises(
    CommandError,
    match="does not match the requested scheduler context",
  ):
    command.handle(**_options(
      output=str(output_path),
      reuse_existing=True,
    ))


def test_command_translates_run_plan_service_failure(
  monkeypatch,
  tmp_path,
) -> None:
  """
  Verify public CLI callers receive a stable Django CommandError.
  """
  _patch_runtime(monkeypatch)

  def fail_build(*args, **kwargs):
    raise elevata_run_plan.ArchitectureExecutionRunPlanError(
      "Execution requires approval."
    )

  monkeypatch.setattr(
    elevata_run_plan,
    "build_architecture_execution_run_plan",
    fail_build,
  )

  command = Command(stdout=StringIO())
  with pytest.raises(
    CommandError,
    match="Execution requires approval",
  ):
    command.handle(**_options(
      output=str(tmp_path / "run-plan.json"),
    ))
