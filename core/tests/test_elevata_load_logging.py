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
import metadata.management.commands.elevata_load as elevata_load_mod
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


def test_meta_log_table_is_ensured_once_per_batch(monkeypatch):
  calls = []

  class DummyEngine:
    pass

  class DummyDialectForEnsure:
    DIALECT_NAME = "databricks"

  def fake_ensure_load_run_log_table(**kwargs):
    calls.append(kwargs)

  monkeypatch.setattr(
    elevata_load_mod,
    "ensure_load_run_log_table",
    fake_ensure_load_run_log_table,
  )

  state: set[tuple[str, str]] = set()
  for _ in range(3):
    elevata_load_mod._ensure_load_run_log_table_for_batch(
      engine=DummyEngine(),
      dialect=DummyDialectForEnsure(),
      meta_schema="meta",
      auto_provision=True,
      ensure_state=state,
    )

  assert len(calls) == 1
  assert state == {("databricks", "meta")}


def test_target_schema_is_ensured_once_per_batch(monkeypatch):
  calls = []

  class DummyEngine:
    pass

  class DummyDialectForEnsure:
    DIALECT_NAME = "databricks"

  def fake_ensure_target_schema(**kwargs):
    calls.append(kwargs)

  monkeypatch.setattr(
    elevata_load_mod,
    "ensure_target_schema",
    fake_ensure_target_schema,
  )

  state: set[tuple[str, str]] = set()
  for _ in range(3):
    elevata_load_mod._ensure_target_schema_for_batch(
      engine=DummyEngine(),
      dialect=DummyDialectForEnsure(),
      schema_name="rawcore",
      auto_provision=True,
      ensure_state=state,
    )

  assert len(calls) == 1
  assert state == {("databricks", "rawcore")}


def test_materialization_applier_skips_repeated_ensure_schema_statements():
  from metadata.materialization.applier import apply_materialization_plan
  from metadata.materialization.plan import MaterializationPlan, MaterializationStep

  class DummyEngine:
    def __init__(self):
      self.executed = []

    def execute(self, sql):
      self.executed.append(sql)

  plan = MaterializationPlan(
    dataset_key="rawcore.rc_customer",
    steps=[
      MaterializationStep(
        op="ENSURE_SCHEMA",
        sql="CREATE SCHEMA IF NOT EXISTS rawcore;",
        safe=True,
        reason="ensure schema",
      ),
      MaterializationStep(
        op="ENSURE_SCHEMA",
        sql="CREATE SCHEMA IF NOT EXISTS rawcore;",
        safe=True,
        reason="ensure schema again",
      ),
      MaterializationStep(
        op="CREATE_TABLE_IF_NOT_EXISTS",
        sql="CREATE TABLE IF NOT EXISTS rawcore.rc_customer (id INT);",
        safe=True,
        reason="ensure table",
      ),
    ],
    warnings=[],
    blocking_errors=[],
  )

  engine = DummyEngine()
  ensure_state: set[str] = set()

  apply_materialization_plan(
    plan=plan,
    exec_engine=engine,
    ensure_schema_sql_state=ensure_state,
  )

  assert engine.executed == [
    "CREATE SCHEMA IF NOT EXISTS rawcore;",
    "CREATE TABLE IF NOT EXISTS rawcore.rc_customer (id INT);",
  ]
  assert ensure_state == {"create schema if not exists rawcore"}


def test_ensure_target_table_prefers_table_exists_hook():
  calls = {"table_exists": 0, "introspect": 0, "ddl": 0, "execute": 0}

  class DummyEngine:
    def execute(self, _sql):
      calls["execute"] += 1

  class DummySchema:
    short_name = "rawcore"
    schema_name = "rawcore"

  class DummyTD:
    target_schema = DummySchema()
    target_dataset_name = "rc_customer"

  class DummyDialect:
    def table_exists(self, **_kwargs):
      calls["table_exists"] += 1
      return True

    def introspect_table(self, **_kwargs):
      calls["introspect"] += 1
      raise AssertionError("full introspection should not be used")

    def render_create_table_if_not_exists(self, _td):
      calls["ddl"] += 1
      return "CREATE TABLE rawcore.rc_customer (id INT);"

  elevata_load_mod.ensure_target_table(
    engine=DummyEngine(),
    dialect=DummyDialect(),
    td=DummyTD(),
    auto_provision=True,
  )

  assert calls == {
    "table_exists": 1,
    "introspect": 0,
    "ddl": 0,
    "execute": 0,
  }


def test_execute_raw_via_ingestion_passes_runtime_state(monkeypatch):
  import metadata.management.commands.elevata_load as mod

  td = DummyTD("raw_customer", "raw")
  source_dataset = object()
  target_engine = object()
  meta_state: set[tuple[str, str]] = set()
  schema_state: set[tuple[str, str]] = set()
  captured = {}

  monkeypatch.setattr(mod, "resolve_single_source_dataset_for_raw", lambda _td: source_dataset)
  monkeypatch.setattr(mod, "resolve_ingest_mode", lambda _ds: "native")
  monkeypatch.setattr(mod, "get_active_dialect", lambda _name: DummyDialect())

  def fake_ingest_raw_for_source_dataset(**kwargs):
    captured.update(kwargs)
    return {"status": "success"}

  monkeypatch.setattr(mod, "ingest_raw_for_source_dataset", fake_ingest_raw_for_source_dataset)

  mod.execute_raw_via_ingestion(
    target_dataset=td,
    target_system=DummySystem(short_name="dwh", type_="databricks"),
    profile=DummyProfile(name="dev"),
    batch_run_id="batch",
    load_run_id="load",
    target_system_engine=target_engine,
    meta_log_ensure_state=meta_state,
    schema_ensure_state=schema_state,
  )

  assert captured["target_engine"] is target_engine
  assert captured["meta_log_ensure_state"] is meta_state
  assert captured["schema_ensure_state"] is schema_state
  assert captured["meta_schema"] == "meta"
