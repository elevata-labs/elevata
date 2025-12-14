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
import logging
from types import SimpleNamespace

import pytest

from metadata.management.commands.elevata_load import Command as ElevataLoadCommand
from django.core.management.base import CommandError
from metadata.rendering.dialects.base import BaseExecutionEngine  # oder wo BaseExecutionEngine liegt


class DummyExecutionEngine(BaseExecutionEngine):
  def __init__(self):
    pass

  def execute(self, sql: str) -> int | None:
    # For testing we don't actually execute anything.
    # We just pretend it worked and return a fake rowcount.
    return 0


class DummyTargetDataset:
  def __init__(self, name: str = "sap_customer", schema_short: str = "rawcore") -> None:
    self.id = 123
    self.target_dataset_name = name
    self.target_schema = SimpleNamespace(
      short_name=schema_short,
      schema_name=schema_short,  # in tests: schema_name == short_name
    )


class DummyProfile:
  def __init__(self, name: str = "default") -> None:
    self.name = name


class DummySystem:
  def __init__(self, short_name: str = "duckdb_local", type_: str = "duckdb") -> None:
    self.short_name = short_name
    self.type = type_


class DummyDialect:
  DIALECT_NAME = "dummy"

  # ... deine bisherigen Methoden (render_xxx, etc.)

  def get_execution_engine(self, system):
    # In tests we return a no-op execution engine
    return DummyExecutionEngine()

  def render_truncate_table(self, *, schema: str, table: str) -> str:
    # Tests don't execute, but the command expects a truncate statement.
    return f"-- truncate {schema}.{table}"

  def literal(self, value):
    # Minimal literal renderer for tests only.
    if value is None:
      return "NULL"
    if isinstance(value, bool):
      return "1" if value else "0"
    if isinstance(value, (int, float)):
      return str(value)
    s = str(value).replace("'", "''")
    return f"'{s}'"

  def render_literal(self, value):
    # Some code paths prefer render_literal; delegate to literal for tests.
    return self.literal(value)


def test_elevata_load_logs_start_and_finish_and_prints_header(monkeypatch, caplog):
  cmd = ElevataLoadCommand()

  # Capture stdout of the command
  buffer = io.StringIO()
  cmd.stdout = buffer

  # Monkeypatch internals used by handle()
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

  # Run handle() with logging capture
  logger_name = "metadata.management.commands.elevata_load"
  with caplog.at_level(logging.INFO, logger=logger_name):
    cmd.handle(
      target_name="sap_customer",
      schema_short="rawcore",
      dialect_name=None,
      target_system_name=None,
      execute=False,
      no_print=False,
    )

  # --- Assertions: stdout header -------------------------------------------

  output = buffer.getvalue()
  assert "-- Profile: test_profile" in output
  assert "-- Target system: test_target (type=duckdb)" in output
  assert "-- Dialect: DummyDialect" in output
  assert "-- generated load sql" in output

  # --- Assertions: logging --------------------------------------------------

  start_records = [
    r for r in caplog.records if r.getMessage() == "elevata_load starting"
  ]
  finish_records = [
    r for r in caplog.records if r.getMessage() == "elevata_load finished"
  ]

  assert len(start_records) == 1
  assert len(finish_records) == 1

  start = start_records[0]
  finish = finish_records[0]

  # Extra fields should be attached to the log record
  assert getattr(start, "target_dataset_name") == "sap_customer"
  assert getattr(start, "target_schema") == "rawcore"
  assert getattr(start, "profile") == "test_profile"
  assert getattr(start, "target_system") == "test_target"
  assert getattr(start, "target_system_type") == "duckdb"
  assert getattr(start, "execute") is False

  assert getattr(finish, "target_dataset_name") == "sap_customer"
  assert getattr(finish, "sql_length") == len("-- generated load sql")
  assert getattr(finish, "execute") is False

def test_elevata_load_execute_logs_without_raising(monkeypatch, caplog):
  cmd = ElevataLoadCommand()
  cmd.stdout = io.StringIO()

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

  logger_name = "metadata.management.commands.elevata_load"

  # We expect the command to run without raising, but to log start + finish
  with caplog.at_level(logging.INFO, logger=logger_name):
    cmd.handle(
      target_name="sap_customer",
      schema_short="rawcore",
      dialect_name=None,
      target_system_name=None,
      execute=True,
      no_print=True,
    )

  start_records = [r for r in caplog.records if r.getMessage() == "elevata_load starting"]
  finish_records = [r for r in caplog.records if r.getMessage() == "elevata_load finished"]

  assert len(start_records) == 1
  assert len(finish_records) == 1
