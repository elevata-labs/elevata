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

import io
import logging

import pytest

from metadata.management.commands.elevata_load import Command as ElevataLoadCommand
from tests._dialect_test_mixin import DialectTestMixin


class DummyProfile:
  def __init__(self, name: str):
    self.name = name


class DummySystem:
  def __init__(self, short_name: str, type_: str):
    self.short_name = short_name
    self.type = type_


class DummyDialect(DialectTestMixin):
  pass


class DummySchema:
  def __init__(self, short_name: str):
    self.short_name = short_name
    self.schema_name = short_name


class DummyTD:
  def __init__(self, name: str, schema_short: str):
    self.id = 123
    self.target_dataset_name = name
    self.target_schema = DummySchema(schema_short)
    self.historize = False
    self.incremental_source = None


def _patch_command(monkeypatch, td: DummyTD):
  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.load_profile",
    lambda _x: DummyProfile(name="test_profile"),
  )
  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.get_target_system",
    lambda _x: DummySystem(short_name="test_target", type_="duckdb"),
  )
  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.get_active_dialect",
    lambda _x: DummyDialect(),
  )

  monkeypatch.setattr(
    ElevataLoadCommand,
    "_resolve_target_dataset",
    lambda self, target_name, schema_short: td,
  )

  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.resolve_execution_order",
    lambda root_td: [root_td],
  )

  def _run_single_target_dataset(**kwargs):
    td_local = kwargs["target_dataset"]
    return {
      "status": "success",
      "kind": "sql",
      "dataset": f"{td_local.target_schema.short_name}.{td_local.target_dataset_name}",
      "message": None,
      "sql_length": 17,
      "render_ms": 1.0,
      "started_at": None,
      "finished_at": None,
    }

  monkeypatch.setattr(
    "metadata.management.commands.elevata_load.run_single_target_dataset",
    _run_single_target_dataset,
  )


def test_elevata_load_logs_start_and_finish(monkeypatch, caplog):
  cmd = ElevataLoadCommand()
  cmd.stdout = io.StringIO()

  td = DummyTD("sap_customer", "rawcore")
  _patch_command(monkeypatch, td)

  logger_name = "metadata.management.commands.elevata_load"
  with caplog.at_level(logging.INFO, logger=logger_name):
    cmd.handle(
      target_name="sap_customer",
      schema_short="rawcore",
      dialect_name=None,
      target_system_name=None,
      execute=False,
      no_print=True,
      debug_plan=False,
      no_deps=True,
      continue_on_error=False,
    )

  start_records = [r for r in caplog.records if r.getMessage() == "elevata_load starting"]
  finish_records = [r for r in caplog.records if r.getMessage() == "elevata_load finished"]

  assert len(start_records) == 1
  assert len(finish_records) == 1

  start = start_records[0]
  finish = finish_records[0]

  assert getattr(start, "target_dataset_name") == "sap_customer"
  assert getattr(start, "target_schema") == "rawcore"
  assert getattr(start, "profile") == "test_profile"
  assert getattr(start, "target_system") == "test_target"
  assert getattr(start, "target_system_type") == "duckdb"
  assert getattr(start, "execute") is False

  assert getattr(finish, "target_dataset_name") == "sap_customer"
  assert getattr(finish, "execute") is False
  assert getattr(finish, "sql_length") == 17


def test_elevata_load_execute_logs_without_raising(monkeypatch, caplog):
  cmd = ElevataLoadCommand()
  cmd.stdout = io.StringIO()

  td = DummyTD("sap_customer", "rawcore")
  _patch_command(monkeypatch, td)

  logger_name = "metadata.management.commands.elevata_load"
  with caplog.at_level(logging.INFO, logger=logger_name):
    cmd.handle(
      target_name="sap_customer",
      schema_short="rawcore",
      dialect_name=None,
      target_system_name=None,
      execute=True,   # exercise dialect.get_execution_engine(system)
      no_print=True,
      debug_plan=False,
      no_deps=True,
      continue_on_error=False,
    )

  start_records = [r for r in caplog.records if r.getMessage() == "elevata_load starting"]
  finish_records = [r for r in caplog.records if r.getMessage() == "elevata_load finished"]

  assert len(start_records) == 1
  assert len(finish_records) == 1
