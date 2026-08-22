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
from django.core.management.base import CommandError

from metadata.architecture.control import ArchitectureControlScope
from metadata.architecture import execution_control
from metadata.execution.load_scope import LoadScopeError
from metadata.management.commands import elevata_load


class FakeTargetDataset:
  def __init__(self, dataset_key: str):
    schema_short, dataset_name = dataset_key.split(".", 1)
    self.target_schema = SimpleNamespace(short_name=schema_short)
    self.target_dataset_name = dataset_name


def test_elevata_load_parser_accepts_partial_load_scope() -> None:
  command = elevata_load.Command()
  parser = command.create_parser("manage.py", "elevata_load")

  options = vars(parser.parse_args(["--partial-load", "sales"]))

  assert options["partial_load_name"] == "sales"
  assert options["target_name"] is None
  assert options["all_datasets"] is False


def test_partial_load_root_selection_rejects_scope_combinations() -> None:
  command = elevata_load.Command()

  with pytest.raises(CommandError, match="exactly one load scope"):
    command._validate_root_selection(
      target_name="customer",
      all_datasets=False,
      partial_load_name="sales",
    )

  with pytest.raises(CommandError, match="--schema cannot be combined"):
    command._validate_root_selection(
      target_name=None,
      all_datasets=False,
      partial_load_name="sales",
      schema_short="bizcore",
    )

  with pytest.raises(CommandError, match="--no-deps cannot be combined"):
    command._validate_root_selection(
      target_name=None,
      all_datasets=False,
      partial_load_name="sales",
      no_deps=True,
    )


def test_partial_load_root_selection_accepts_named_scope() -> None:
  elevata_load.Command()._validate_root_selection(
    target_name=None,
    all_datasets=False,
    partial_load_name="sales",
  )


def test_partial_load_scope_errors_are_exposed_as_command_errors(monkeypatch) -> None:
  def fail(_name: str):
    raise LoadScopeError("invalid partial load")

  monkeypatch.setattr(elevata_load, "resolve_partial_load_scope", fail)

  with pytest.raises(CommandError, match="invalid partial load"):
    elevata_load.Command()._resolve_partial_load_scope("sales")


def test_partial_load_root_binding_detects_redundant_root_changes() -> None:
  command = elevata_load.Command()
  roots = [
    FakeTargetDataset("bizcore.bc_dim_customer"),
    FakeTargetDataset("bizcore.bc_fact_customer_order"),
  ]

  command._validate_partial_load_root_binding(
    roots=roots,
    expected_root_dataset_keys=(
      "bizcore.bc_dim_customer",
      "bizcore.bc_fact_customer_order",
    ),
  )

  with pytest.raises(CommandError, match="root definition changed"):
    command._validate_partial_load_root_binding(
      roots=roots,
      expected_root_dataset_keys=(
        "bizcore.bc_fact_customer_order",
      ),
    )


def test_controlled_execution_builds_partial_load_command() -> None:
  scope = ArchitectureControlScope.for_partial_load("sales")

  command_name, command_args, command_options = (
    execution_control._build_elevata_load_command(scope)
  )

  assert command_name == "elevata_load"
  assert command_args == ()
  assert command_options == {
    "execute": True,
    "no_print": False,
    "no_deps": False,
    "partial_load_name": "sales",
  }


def test_controlled_partial_load_rejects_target_only_mode() -> None:
  scope = ArchitectureControlScope.for_partial_load("sales")

  with pytest.raises(
    execution_control.ArchitectureControlledExecutionError,
    match="always includes required dependencies",
  ):
    execution_control._build_elevata_load_command(
      scope,
      no_deps=True,
    )


def test_execution_run_plan_step_rejects_partial_load_scope() -> None:
  with pytest.raises(CommandError, match="cannot be combined with --partial-load"):
    elevata_load._validate_execution_run_plan_options(
      run_plan_path=".artifacts/sales.run_plan.json",
      execute=True,
      all_datasets=False,
      partial_load_name="sales",
      no_deps=True,
      no_plan_guard=False,
      execution_impact_selection=None,
    )


def test_controlled_execution_binds_partial_load_root_intent(monkeypatch) -> None:
  scope = ArchitectureControlScope.for_partial_load("sales")
  root_dataset_keys = (
    "bizcore.bc_dim_customer",
    "bizcore.bc_fact_customer_order",
  )
  execution_dataset_keys = (
    "raw.raw_aw1_customer",
    "bizcore.bc_dim_customer",
    "bizcore.bc_fact_customer_order",
  )
  preview = SimpleNamespace(
    gate=SimpleNamespace(can_execute=True, message="ready"),
    scope_key="partial_load:sales",
    scope_label="Partial load: sales",
    dependency_mode="with_dependencies",
    root_dataset_keys=root_dataset_keys,
    execution_dataset_keys=execution_dataset_keys,
    report_fingerprint="report-1",
    approval_id=None,
    preview_fingerprint="preview-1",
    impact_plan_binding=None,
  )

  monkeypatch.setattr(
    execution_control,
    "resolve_architecture_execution_scope",
    lambda selected_scope, *, no_deps=False: SimpleNamespace(
      root_dataset_keys=root_dataset_keys,
      execution_dataset_keys=execution_dataset_keys,
      dependency_mode="with_dependencies",
    ),
  )
  monkeypatch.setattr(
    execution_control,
    "build_architecture_control_context",
    lambda selected_scope, **kwargs: SimpleNamespace(),
  )
  monkeypatch.setattr(
    execution_control,
    "build_architecture_execution_preview",
    lambda selected_scope, **kwargs: preview,
  )

  captured: dict[str, object] = {}

  def fake_command_runner(command_name, *args, **options):
    captured["command_name"] = command_name
    captured["args"] = args
    captured["options"] = options

  class FakeRecordStore:
    def save(self, record):
      captured["record"] = record
      return ".elevata/executions/test.execution.json"

  result = execution_control.execute_architecture_control_scope(
    scope,
    actor="Ilona",
    command_runner=fake_command_runner,
    record_store=FakeRecordStore(),
  )

  assert result.succeeded is True
  assert captured["command_name"] == "elevata_load"
  assert captured["args"] == ()
  options = captured["options"]
  assert options["partial_load_name"] == "sales"
  assert options["expected_partial_load_root_keys"] == root_dataset_keys
  assert result.root_dataset_keys == root_dataset_keys
  assert result.execution_dataset_keys == execution_dataset_keys
  assert "expected_partial_load_root_keys" not in result.command_options
