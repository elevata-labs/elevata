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

from datetime import datetime, timezone

import pytest
from django.core.management.base import CommandError

from metadata.architecture.execution_impact import (
  ExecutionImpactSelection,
)
from metadata.architecture.execution_run_plan import (
  ExecutionRunPlanStore,
  build_execution_plan_fingerprint,
  build_execution_run_plan,
)
from metadata.architecture.state import (
  ArchitectureState,
  DatasetState,
)
from metadata.management.commands import elevata_load
from metadata.management.commands.elevata_load import (
  Command,
  _load_execution_run_plan,
  _resolve_execution_impact_decisions,
  _resolve_execution_scope_dataset_keys,
  _resolve_execution_run_plan_step_decisions,
  _validate_execution_run_plan_options,
)


class _Schema:
  def __init__(self, short_name: str):
    self.short_name = short_name


class _TargetDataset:
  def __init__(
    self,
    dataset_key: str,
    *,
    materialization_type: str = "table",
  ):
    schema_short, dataset_name = dataset_key.split(".", 1)
    self.target_schema = _Schema(schema_short)
    self.target_dataset_name = dataset_name
    self.incremental_strategy = "full"
    self.materialization_type = materialization_type
    self.updated_at = datetime(
      2026,
      7,
      24,
      4,
      15,
      tzinfo=timezone.utc,
    )


def _fingerprint(character: str) -> str:
  """
  Return one deterministic SHA-256-shaped test fingerprint.
  """
  return character * 64


def _dataset_state(
  target_dataset: _TargetDataset,
) -> DatasetState:
  """
  Return the Architecture State representation of one test target.
  """
  return DatasetState(
    dataset_key=(
      f"{target_dataset.target_schema.short_name}."
      f"{target_dataset.target_dataset_name}"
    ),
    schema_short_name=target_dataset.target_schema.short_name,
    dataset_name=target_dataset.target_dataset_name,
    materialization_type=target_dataset.materialization_type,
    incremental_strategy=target_dataset.incremental_strategy,
    historize=False,
    is_hist=False,
    active=True,
  )


def _planned_state(
  target_datasets: tuple[_TargetDataset, ...],
) -> ArchitectureState:
  """
  Return the immutable architecture snapshot for one test plan.
  """
  return ArchitectureState(
    datasets=tuple(
      _dataset_state(target_dataset)
      for target_dataset in target_datasets
    ),
  )


def _run_plan(
  target_datasets: tuple[_TargetDataset, ...],
):
  """
  Return one scheduler run plan bound to the supplied metadata.
  """
  selection = ExecutionImpactSelection(
    plan_fingerprint=_fingerprint("c"),
    dataset_decisions=(
      ("raw.a", "REUSE"),
      ("core.b", "FULL_REBUILD"),
    ),
  )
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
    architecture_fingerprint=_planned_state(target_datasets).fingerprint,
    report_fingerprint=_fingerprint("b"),
    preview_fingerprint=_fingerprint("d"),
    execution_plan_fingerprint=(
      build_execution_plan_fingerprint(
        target_datasets
      )
    ),
    root_dataset_keys=("raw.a",),
    impact_selection=selection,
  )


def test_command_declares_internal_impact_selection_options() -> None:
  """
  Verify Architecture Control contracts are accepted but not exposed as CLI flags.
  """
  assert {
    "execution_impact_selection",
    "execution_outcome_collector",
  }.issubset(set(Command.stealth_options))


def test_resolve_execution_impact_decisions_preserves_execution_order() -> None:
  """
  Verify the bound selection must describe the exact load execution sequence.
  """
  selection = ExecutionImpactSelection(
    plan_fingerprint="impact-plan-1",
    dataset_decisions=(
      ("raw.a", "REUSE"),
      ("core.b", "FULL_REBUILD"),
    ),
  )

  decisions = _resolve_execution_impact_decisions(
    selection=selection,
    execution_order=[
      _TargetDataset("raw.a"),
      _TargetDataset("core.b"),
    ],
  )

  assert decisions == {
    "raw.a": "REUSE",
    "core.b": "FULL_REBUILD",
  }


def test_resolve_execution_impact_decisions_rejects_stale_order() -> None:
  """
  Verify independently resolved load order cannot diverge from Architecture Control.
  """
  selection = ExecutionImpactSelection(
    plan_fingerprint="impact-plan-1",
    dataset_decisions=(
      ("raw.a", "REUSE"),
      ("core.b", "FULL_REBUILD"),
    ),
  )

  with pytest.raises(
    CommandError,
    match="dataset order does not match",
  ):
    _resolve_execution_impact_decisions(
      selection=selection,
      execution_order=[
        _TargetDataset("core.b"),
        _TargetDataset("raw.a"),
      ],
    )


def test_execution_run_plan_options_require_scheduler_step_mode() -> None:
  """
  Verify the public run-plan adapter cannot change direct CLI semantics.
  """
  valid_options = {
    "run_plan_path": "run-plan.json",
    "execute": True,
    "all_datasets": False,
    "no_deps": True,
    "no_plan_guard": False,
    "execution_impact_selection": None,
  }

  _validate_execution_run_plan_options(**valid_options)

  invalid_cases = (
    (
      {"execute": False},
      "requires --execute",
    ),
    (
      {"all_datasets": True},
      "cannot be combined with --all",
    ),
    (
      {"no_deps": False},
      "requires --no-deps",
    ),
    (
      {"no_plan_guard": True},
      "cannot disable the execution plan guard",
    ),
    (
      {
        "execution_impact_selection": ExecutionImpactSelection(
          plan_fingerprint="impact-plan-1",
          dataset_decisions=(("raw.a", "REUSE"),),
        ),
      },
      "cannot be combined with an internal",
    ),
  )

  for overrides, message in invalid_cases:
    options = dict(valid_options)
    options.update(overrides)
    with pytest.raises(CommandError, match=message):
      _validate_execution_run_plan_options(**options)


def test_execution_run_plan_step_loads_and_resolves_bound_decision(
  monkeypatch,
  tmp_path,
) -> None:
  """
  Verify one scheduler step consumes the shared immutable run plan.
  """
  targets = (
    _TargetDataset("raw.a"),
    _TargetDataset("core.b"),
  )
  planned_state = _planned_state(targets)
  plan = _run_plan(targets)
  monkeypatch.setattr(
    elevata_load.ArchitectureStateService,
    "build_dataset_state",
    lambda self, target_dataset: _dataset_state(target_dataset),
  )
  path = ExecutionRunPlanStore(
    base_path=tmp_path,
  ).save(plan)

  loaded = _load_execution_run_plan(path)
  decisions = _resolve_execution_run_plan_step_decisions(
    plan=loaded,
    profile_name="dev",
    target_system_short="dwh",
    execution_order=[targets[0]],
    planned_architecture_state=planned_state,
  )

  assert loaded.batch_run_id == "batch-001"
  assert decisions == {
    "raw.a": "REUSE",
  }


def test_execution_run_plan_step_rejects_runtime_mismatch() -> None:
  """
  Verify a scheduler worker cannot execute a plan for another runtime.
  """
  targets = (
    _TargetDataset("raw.a"),
    _TargetDataset("core.b"),
  )
  planned_state = _planned_state(targets)

  with pytest.raises(
    CommandError,
    match="runtime does not match",
  ):
    _resolve_execution_run_plan_step_decisions(
      plan=_run_plan(targets),
      profile_name="other",
      target_system_short="dwh",
      execution_order=[targets[0]],
      planned_architecture_state=planned_state,
    )


def test_execution_run_plan_step_rejects_dataset_outside_plan(
  monkeypatch,
) -> None:
  """
  Verify a scheduler task cannot inject another dataset into the run.
  """
  targets = (
    _TargetDataset("raw.a"),
    _TargetDataset("core.b"),
  )
  planned_state = _planned_state(targets)
  other_target = _TargetDataset("other.c")
  monkeypatch.setattr(
    elevata_load.ArchitectureStateService,
    "build_dataset_state",
    lambda self, target_dataset: _dataset_state(target_dataset),
  )

  with pytest.raises(
    CommandError,
    match="not part of the Execution Run Plan",
  ):
    _resolve_execution_run_plan_step_decisions(
      plan=_run_plan(targets),
      profile_name="dev",
      target_system_short="dwh",
      execution_order=[other_target],
      planned_architecture_state=planned_state,
    )


def test_execution_run_plan_step_rejects_metadata_drift(
  monkeypatch,
) -> None:
  """
  Verify current metadata must still match the plan-generation fingerprint.
  """
  original_targets = (
    _TargetDataset("raw.a"),
    _TargetDataset("core.b"),
  )
  changed_targets = (
    _TargetDataset("raw.a"),
    _TargetDataset(
      "core.b",
      materialization_type="view",
    ),
  )

  planned_state = _planned_state(original_targets)
  monkeypatch.setattr(
    elevata_load.ArchitectureStateService,
    "build_dataset_state",
    lambda self, target_dataset: _dataset_state(target_dataset),
  )

  with pytest.raises(
    CommandError,
    match="superseded by metadata changes",
  ):
    _resolve_execution_run_plan_step_decisions(
      plan=_run_plan(original_targets),
      profile_name="dev",
      target_system_short="dwh",
      execution_order=[changed_targets[1]],
      planned_architecture_state=planned_state,
    )


def test_execution_scope_uses_local_order_without_run_plan() -> None:
  """
  Verify direct CLI execution retains its locally resolved scope.
  """
  execution_order = [
    _TargetDataset("raw.a"),
    _TargetDataset("core.b"),
  ]

  assert _resolve_execution_scope_dataset_keys(
    execution_order=execution_order,
    execution_run_plan=None,
  ) == {
    "raw.a",
    "core.b",
  }


def test_execution_scope_uses_complete_run_plan_for_scheduler_step() -> None:
  """
  Verify one distributed task retains the full controlled plan scope.
  """
  targets = (
    _TargetDataset("raw.a"),
    _TargetDataset("core.b"),
  )
  plan = _run_plan(targets)

  assert _resolve_execution_scope_dataset_keys(
    execution_order=[targets[1]],
    execution_run_plan=plan,
  ) == {
    "raw.a",
    "core.b",
  }
