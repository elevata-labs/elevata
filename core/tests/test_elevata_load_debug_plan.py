"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025 Ilona Tag

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

import io
from types import SimpleNamespace

import pytest

from metadata.management.commands.elevata_load import Command as ElevataLoadCommand


class DummyProfile:
  def __init__(self, name: str):
    self.name = name


class DummySystem:
  def __init__(self, short_name: str, type_: str):
    self.short_name = short_name
    self.type = type_


class DummyDialect:
  # handle() prints dialect.__class__.__name__ into logs only, not stdout
  def __repr__(self):
    return "DummyDialect"


class DummySchema:
  def __init__(self, short_name: str, schema_name: str):
    self.short_name = short_name
    self.schema_name = schema_name


class DummySourceDataset:
  def __init__(self, source_dataset_name: str):
    self.source_dataset_name = source_dataset_name


class DummyTD:
  def __init__(self, target_dataset_name: str, schema_short: str):
    self.target_dataset_name = target_dataset_name
    self.target_schema = DummySchema(short_name=schema_short, schema_name=schema_short)
    self.id = 123
    self.historize = False
    # IMPORTANT: debug_plan prints incremental_source from td.incremental_source.source_dataset_name
    self.incremental_source = DummySourceDataset("sap_src")


def _patch_command_dependencies(monkeypatch, *, dummy_td: DummyTD):
  # Resolve profile / system / dialect
  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.load_profile",
    lambda _profile_name: DummyProfile(name="test_profile"),
  )
  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.get_target_system",
    lambda _name: DummySystem(short_name="test_target", type_="duckdb"),
  )
  # Return a dialect object; handle() may call dialect.get_execution_engine(system) in execute=True mode.
  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.get_active_dialect",
    lambda _name: DummyDialect(),
  )

  # Resolve TD (avoid DB access)
  monkeypatch.setattr(
    ElevataLoadCommand,
    "_resolve_target_dataset",
    lambda self, target_name, schema_short: dummy_td,
  )

  # Avoid dependency reveal (we keep no_deps=True in tests anyway, but safe)
  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.resolve_execution_order",
    lambda root_td: [root_td],
  )

  # Make dataset execution deterministic and “success”
  def _run_single_target_dataset(**kwargs):
    td = kwargs["target_dataset"]
    return {
      "status": "success",
      "kind": "sql",
      "dataset": f"{td.target_schema.short_name}.{td.target_dataset_name}",
      "message": None,
      "sql_length": 0,
      "render_ms": 0.0,
    }

  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.run_single_target_dataset",
    _run_single_target_dataset,
  )


def test_debug_plan_prints_load_plan_overview(monkeypatch):
  cmd = ElevataLoadCommand()
  buffer = io.StringIO()
  cmd.stdout = buffer

  dummy_td = DummyTD(target_dataset_name="sap_customer", schema_short="rawcore")
  _patch_command_dependencies(monkeypatch, dummy_td=dummy_td)

  # build_load_plan exists and is used for mode + handle_deletes only
  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.build_load_plan",
    lambda _td: SimpleNamespace(mode="merge", handle_deletes=True),
  )

  cmd.handle(
    target_name="sap_customer",
    schema_short="rawcore",
    dialect_name=None,
    target_system_name=None,
    execute=False,
    no_print=False,
    debug_plan=True,
    no_deps=True,  # ensures execution_order = [root_td]
    continue_on_error=False,
  )

  out = buffer.getvalue()

  assert "Execution plan" in out
  assert "rawcore.sap_customer" in out

  assert "-- LoadPlan debug:" in out
  assert "mode           = merge" in out
  assert "handle_deletes = True" in out
  assert "incremental_source = sap_src" in out
  assert "delete_detection_enabled = True" in out

  assert "Execution summary" in out
  assert "✔ rawcore.sap_customer" in out


def test_debug_plan_flag_false_has_no_debug_block(monkeypatch):
  cmd = ElevataLoadCommand()
  buffer = io.StringIO()
  cmd.stdout = buffer

  dummy_td = DummyTD(target_dataset_name="sap_customer", schema_short="rawcore")
  _patch_command_dependencies(monkeypatch, dummy_td=dummy_td)

  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.build_load_plan",
    lambda _td: SimpleNamespace(mode="merge", handle_deletes=True),
  )

  cmd.handle(
    target_name="sap_customer",
    schema_short="rawcore",
    dialect_name=None,
    target_system_name=None,
    execute=False,
    no_print=False,
    debug_plan=False,
    no_deps=True,
    continue_on_error=False,
  )

  out = buffer.getvalue()
  assert "-- LoadPlan debug:" not in out

  assert "Execution plan" in out
  assert "rawcore.sap_customer" in out
  assert "Execution summary" in out


def test_debug_plan_delete_detection_disabled_for_non_rawcore(monkeypatch):
  cmd = ElevataLoadCommand()
  buffer = io.StringIO()
  cmd.stdout = buffer

  dummy_td = DummyTD(target_dataset_name="sap_customer", schema_short="stage")
  _patch_command_dependencies(monkeypatch, dummy_td=dummy_td)

  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.build_load_plan",
    lambda _td: SimpleNamespace(mode="merge", handle_deletes=True),
  )

  cmd.handle(
    target_name="sap_customer",
    schema_short="stage",
    dialect_name=None,
    target_system_name=None,
    execute=False,
    no_print=False,
    debug_plan=True,
    no_deps=True,
    continue_on_error=False,
  )

  out = buffer.getvalue()
  assert "delete_detection_enabled = False" in out
