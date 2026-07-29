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
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from metadata.architecture.control import ArchitectureControlScope
from metadata.architecture.execution_impact import (
  ExecutionImpactSelection,
)
from metadata.architecture.execution_run_plan import (
  ExecutionRunPlanStore,
  build_execution_plan_fingerprint,
  build_execution_run_plan,
)
from metadata.architecture.execution_run_plan_service import (
  ArchitectureExecutionRunPlanError,
  build_architecture_execution_run_plan,
)
from metadata.architecture.paths import ArchitectureArtifactContext
from metadata.architecture import execution_run_plan_service


def _fingerprint(character: str) -> str:
  """
  Return one deterministic SHA-256-shaped test fingerprint.
  """
  return character * 64


class FakeTargetDataset:
  """
  Minimal TargetDataset-shaped value for execution-plan fingerprints.
  """

  def __init__(
    self,
    dataset_key: str,
    *,
    identifier: int,
    incremental_strategy: str = "full",
    materialization_type: str | None = "table",
    updated_at: datetime | None = None,
  ):
    schema_short, dataset_name = dataset_key.split(".", 1)
    self.id = identifier
    self.target_schema = SimpleNamespace(
      short_name=schema_short,
    )
    self.target_dataset_name = dataset_name
    self.incremental_strategy = incremental_strategy
    self.materialization_type = materialization_type
    self.updated_at = (
      updated_at
      or datetime(
        2026,
        7,
        24,
        4,
        15,
        tzinfo=timezone.utc,
      )
    )


def _selection() -> ExecutionImpactSelection:
  """
  Return one executable impact selection.
  """
  return ExecutionImpactSelection(
    plan_fingerprint=_fingerprint("c"),
    dataset_decisions=(
      ("raw.customer", "REUSE"),
      ("rawcore.customer", "FULL_REBUILD"),
    ),
  )


def _run_plan():
  """
  Return one valid run plan for storage tests.
  """
  return build_execution_run_plan(
    run_plan_id="run-plan-001",
    batch_run_id="batch-001",
    created_at="2026-07-24T04:15:00+00:00",
    profile_name="dev",
    target_system_short="dwh",
    scope_mode="all",
    scope_key="all",
    scope_label="All datasets",
    dependency_mode="with_dependencies",
    review_status="no_changes",
    approval_id=None,
    architecture_fingerprint=_fingerprint("a"),
    report_fingerprint=_fingerprint("b"),
    preview_fingerprint=_fingerprint("d"),
    execution_plan_fingerprint=_fingerprint("e"),
    root_dataset_keys=("raw.customer",),
    impact_selection=_selection(),
  )


def test_execution_plan_fingerprint_binds_dataset_metadata() -> None:
  """
  Verify the execution-set fingerprint is stable and order-independent.
  """
  raw = FakeTargetDataset(
    "raw.customer",
    identifier=1,
  )
  rawcore = FakeTargetDataset(
    "rawcore.customer",
    identifier=2,
  )

  first = build_execution_plan_fingerprint((raw, rawcore))
  reversed_order = build_execution_plan_fingerprint(
    (rawcore, raw)
  )
  changed = build_execution_plan_fingerprint((
    raw,
    FakeTargetDataset(
      "rawcore.customer",
      identifier=2,
      materialization_type="view",
    ),
  ))

  assert first == reversed_order
  assert first != changed


def test_execution_run_plan_store_round_trips_immutable_plan(
  tmp_path,
) -> None:
  """
  Verify canonical storage, loading and idempotent repeated writes.
  """
  plan = _run_plan()
  store = ExecutionRunPlanStore(base_path=tmp_path)

  path = store.save(plan)
  second_path = store.save(plan)
  loaded = store.load("run-plan-001")

  assert path == tmp_path / "run-plan-001.run_plan.json"
  assert second_path == path
  assert loaded == plan
  assert store.load_path(path) == plan
  assert store.iter_plan_paths() == (path,)


def test_execution_run_plan_store_rejects_identifier_reuse(
  tmp_path,
) -> None:
  """
  Verify one run-plan identifier cannot be replaced by another plan.
  """
  plan = _run_plan()
  changed_plan = replace(
    plan,
    batch_run_id="batch-002",
  )
  store = ExecutionRunPlanStore(base_path=tmp_path)
  store.save(plan)

  with pytest.raises(
    ValueError,
    match="already contains another immutable plan",
  ):
    store.save(changed_plan)


def _patch_run_plan_service(
  monkeypatch,
  *,
  can_execute: bool = True,
):
  """
  Patch run-plan assembly dependencies and return expected values.
  """
  raw = FakeTargetDataset(
    "raw.customer",
    identifier=1,
  )
  rawcore = FakeTargetDataset(
    "rawcore.customer",
    identifier=2,
  )
  selection = _selection()

  execution_scope = SimpleNamespace(
    roots=(raw,),
    execution_order=(raw, rawcore),
    dependency_mode="with_dependencies",
    root_dataset_keys=("raw.customer",),
    execution_dataset_keys=(
      "raw.customer",
      "rawcore.customer",
    ),
  )
  impact_plan = SimpleNamespace(
    architecture_fingerprint=_fingerprint("a"),
    plan_fingerprint=selection.plan_fingerprint,
  )
  control_context = SimpleNamespace(
    execution_impact_plan=impact_plan,
    execution_impact_plan_error=None,
  )
  preview = SimpleNamespace(
    gate=SimpleNamespace(
      can_execute=can_execute,
      message="Execution is blocked.",
    ),
    scope_key="all",
    scope_label="All datasets",
    dependency_mode="with_dependencies",
    review_status="no_changes",
    approval_id=None,
    report_fingerprint=_fingerprint("b"),
    preview_fingerprint=_fingerprint("d"),
    root_dataset_keys=("raw.customer",),
    execution_dataset_keys=(
      "raw.customer",
      "rawcore.customer",
    ),
    impact_plan_binding=SimpleNamespace(
      plan_fingerprint=selection.plan_fingerprint,
    ),
  )
  captured_context_options = {}

  monkeypatch.setattr(
    execution_run_plan_service,
    "resolve_architecture_execution_scope",
    lambda scope, *, no_deps=False: execution_scope,
  )

  def fake_build_context(scope, **options):
    captured_context_options.update(options)
    return control_context

  monkeypatch.setattr(
    execution_run_plan_service,
    "build_architecture_control_context",
    fake_build_context,
  )
  monkeypatch.setattr(
    execution_run_plan_service,
    "build_architecture_execution_preview",
    lambda scope, **options: preview,
  )
  monkeypatch.setattr(
    execution_run_plan_service,
    "build_execution_impact_selection",
    lambda plan, *, execution_dataset_keys: selection,
  )

  return SimpleNamespace(
    execution_scope=execution_scope,
    control_context=control_context,
    preview=preview,
    selection=selection,
    captured_context_options=captured_context_options,
  )


def test_run_plan_service_binds_scope_preview_and_impact_selection(
  monkeypatch,
) -> None:
  """
  Verify one concrete scope is resolved once and bound into the run plan.
  """
  expected = _patch_run_plan_service(monkeypatch)
  artifact_context = ArchitectureArtifactContext(
    profile_name="dev",
    target_system_short="dwh",
  )

  result = build_architecture_execution_run_plan(
    ArchitectureControlScope.for_all(),
    artifact_context=artifact_context,
    run_plan_id="run-plan-001",
    batch_run_id="batch-001",
    created_at="2026-07-24T04:15:00+00:00",
  )

  assert result.control_context is expected.control_context
  assert result.preview is expected.preview
  assert result.run_plan.run_plan_id == "run-plan-001"
  assert result.run_plan.batch_run_id == "batch-001"
  assert result.run_plan.profile_name == "dev"
  assert result.run_plan.target_system_short == "dwh"
  assert result.run_plan.dataset_keys == (
    "raw.customer",
    "rawcore.customer",
  )
  assert result.run_plan.selection == expected.selection
  assert result.run_plan.execution_plan_fingerprint == (
    build_execution_plan_fingerprint(
      expected.execution_scope.execution_order
    )
  )
  assert expected.captured_context_options[
    "execution_dataset_keys"
  ] == expected.execution_scope.execution_dataset_keys
  assert expected.captured_context_options[
    "dependency_mode"
  ] == "with_dependencies"
  assert expected.captured_context_options[
    "artifact_context"
  ] == artifact_context


def test_run_plan_service_fails_before_artifact_creation_when_blocked(
  monkeypatch,
) -> None:
  """
  Verify a non-executable preview cannot become a scheduler run plan.
  """
  _patch_run_plan_service(
    monkeypatch,
    can_execute=False,
  )

  with pytest.raises(
    ArchitectureExecutionRunPlanError,
    match="Execution is blocked",
  ):
    build_architecture_execution_run_plan(
      ArchitectureControlScope.for_all(),
      artifact_context=ArchitectureArtifactContext(
        profile_name="dev",
        target_system_short="dwh",
      ),
    )
