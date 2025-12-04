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

from django.core.management.base import CommandError
import pytest

from metadata.management.commands.elevata_load import Command as ElevataLoadCommand

class DummyTargetDataset:
  def __init__(
    self,
    name: str = "sap_customer",
    schema_short: str = "rawcore",
    incremental_source=None,
  ) -> None:
    self.id = 123
    self.target_dataset_name = name
    self.target_schema = SimpleNamespace(short_name=schema_short)
    self.incremental_source = incremental_source


class DummyProfile:
  def __init__(self, name: str = "test_profile") -> None:
    self.name = name


class DummySystem:
  def __init__(self, short_name: str = "test_target", type_: str = "duckdb") -> None:
    self.short_name = short_name
    self.type = type_


class DummyDialect:
  pass

def test_debug_plan_prints_load_plan_overview(monkeypatch):
  cmd = ElevataLoadCommand()
  buffer = io.StringIO()
  cmd.stdout = buffer

  incr_src = SimpleNamespace(source_dataset_name="sap_src")
  dummy_td = DummyTargetDataset(incremental_source=incr_src)

  # Monkeypatch dependencies used by handle()
  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.load_profile",
    lambda _profile_name: DummyProfile(name="test_profile"),
  )
  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.get_target_system",
    lambda _name: DummySystem(short_name="test_target", type_="duckdb"),
  )
  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.get_active_dialect",
    lambda _name: DummyDialect(),
  )
  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.render_load_sql_for_target",
    lambda td, dialect: "-- generated load sql",
  )
  monkeypatch.setattr(
    ElevataLoadCommand,
    "_resolve_target_dataset",
    lambda self, target_name, schema_short: dummy_td,
  )

  build_calls = {"count": 0}

  def fake_build_load_plan(td):
    build_calls["count"] += 1
    assert td is dummy_td
    return SimpleNamespace(mode="merge", handle_deletes=True)

  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.build_load_plan",
    fake_build_load_plan,
  )

  # Call handle with debug_plan=True
  cmd.handle(
    target_name="sap_customer",
    schema_short="rawcore",
    dialect_name=None,
    target_system_name=None,
    execute=False,
    no_print=False,
    debug_plan=True,
  )

  output = buffer.getvalue()

  # Debug-Block should be contained
  assert "LoadPlan debug" in output
  assert "mode" in output
  assert "merge" in output
  assert "handle_deletes" in output
  assert "True" in output
  assert "incremental_source = sap_src" in output
  assert "delete_detection_enabled = True" in output

  # SQL-Header and SQL itself should also be there
  assert "-- Profile: test_profile" in output
  assert "-- Target system: test_target (type=duckdb)" in output
  assert "-- Dialect: DummyDialect" in output
  assert "-- generated load sql" in output

  # build_load_plan was called exactly once
  assert build_calls["count"] == 1

def test_debug_plan_flag_false_has_no_debug_block(monkeypatch):
  cmd = ElevataLoadCommand()
  buffer = io.StringIO()
  cmd.stdout = buffer

  dummy_td = DummyTargetDataset()

  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.load_profile",
    lambda _profile_name: DummyProfile(name="test_profile"),
  )
  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.get_target_system",
    lambda _name: DummySystem(short_name="test_target", type_="duckdb"),
  )
  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.get_active_dialect",
    lambda _name: DummyDialect(),
  )
  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.render_load_sql_for_target",
    lambda td, dialect: "-- generated load sql",
  )
  monkeypatch.setattr(
    ElevataLoadCommand,
    "_resolve_target_dataset",
    lambda self, target_name, schema_short: dummy_td,
  )
  # A simple plan is enough for this test
  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.build_load_plan",
    lambda td: SimpleNamespace(mode="full", handle_deletes=False),
  )

  # debug_plan won't be set -> should fall to False
  cmd.handle(
    target_name="sap_customer",
    schema_short="rawcore",
    dialect_name=None,
    target_system_name=None,
    execute=False,
    no_print=False,
  )

  output = buffer.getvalue()

  # No Debug-Header
  assert "LoadPlan debug" not in output
  # Header + SQL as usual
  assert "-- Profile: test_profile" in output
  assert "-- Target system: test_target (type=duckdb)" in output
  assert "-- Dialect: DummyDialect" in output
  assert "-- generated load sql" in output


def test_debug_plan_delete_detection_disabled_for_non_rawcore(monkeypatch):
  cmd = ElevataLoadCommand()
  buffer = io.StringIO()
  cmd.stdout = buffer

  dummy_td = DummyTargetDataset(schema_short="stage")

  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.load_profile",
    lambda _profile_name: DummyProfile(name="test_profile"),
  )
  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.get_target_system",
    lambda _name: DummySystem(short_name="test_target", type_="duckdb"),
  )
  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.get_active_dialect",
    lambda _name: DummyDialect(),
  )
  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.render_load_sql_for_target",
    lambda td, dialect: "-- generated load sql",
  )
  monkeypatch.setattr(
    ElevataLoadCommand,
    "_resolve_target_dataset",
    lambda self, target_name, schema_short: dummy_td,
  )
  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.build_load_plan",
    lambda td: SimpleNamespace(mode="merge", handle_deletes=True),
  )

  cmd.handle(
    target_name="sap_customer",
    schema_short="stage",
    dialect_name=None,
    target_system_name=None,
    execute=False,
    no_print=False,
    debug_plan=True,
  )

  output = buffer.getvalue()
  assert "delete_detection_enabled = False" in output
