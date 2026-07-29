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

import datetime
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


def test_load_run_runtime_state_empty_returns_independent_sets():
  first = elevata_load_mod.LoadRunRuntimeState.empty()
  second = elevata_load_mod.LoadRunRuntimeState.empty()

  first.meta_log_ensure_state.add(("duckdb", "meta"))
  first.schema_ensure_state.add(("duckdb", "rawcore"))
  first.load_run_snapshot_ensure_state.add(("duckdb", "meta"))
  first.materialization_schema_ensure_sql_state.add("create schema rawcore")

  assert second.meta_log_ensure_state == set()
  assert second.schema_ensure_state == set()
  assert second.load_run_snapshot_ensure_state == set()
  assert second.materialization_schema_ensure_sql_state == set()


def test_persist_load_run_snapshot_uses_runtime_state_and_executes_insert(monkeypatch):
  calls = {"ensure": [], "executed": [], "values": None}

  class DummyEngine:
    def execute(self, sql):
      calls["executed"].append(sql)

  class DummyDialectForSnapshot:
    DIALECT_NAME = "duckdb"

    def render_insert_load_run_snapshot(self, *, meta_schema, values):
      calls["values"] = values
      return f"INSERT INTO {meta_schema}.load_run_snapshot VALUES (...)"

  def fake_ensure_load_run_snapshot_table_once(**kwargs):
    calls["ensure"].append(kwargs)
    kwargs["ensure_state"].add(("duckdb", kwargs["meta_schema"]))

  monkeypatch.setattr(
    elevata_load_mod,
    "ensure_load_run_snapshot_table_once",
    fake_ensure_load_run_snapshot_table_once,
  )

  runtime_state = elevata_load_mod.LoadRunRuntimeState.empty()

  elevata_load_mod._persist_load_run_snapshot_best_effort(
    engine=DummyEngine(),
    dialect=DummyDialectForSnapshot(),
    runtime_state=runtime_state,
    snapshot={"batch_run_id": "batch-001"},
    batch_run_id="batch-001",
    created_at="2026-07-09T12:00:00Z",
    root_dataset_key="rawcore.customer",
    execute=True,
    continue_on_error=False,
    max_retries=1,
    had_error=False,
    step_count=2,
    meta_schema="meta",
    auto_provision=True,
  )

  assert len(calls["ensure"]) == 1
  assert calls["ensure"][0]["ensure_state"] is runtime_state.load_run_snapshot_ensure_state
  assert runtime_state.load_run_snapshot_ensure_state == {("duckdb", "meta")}
  assert calls["executed"] == ["INSERT INTO meta.load_run_snapshot VALUES (...)"]
  assert calls["values"]["batch_run_id"] == "batch-001"
  assert calls["values"]["root_dataset_key"] == "rawcore.customer"
  assert calls["values"]["step_count"] == 2
  assert '"batch_run_id": "batch-001"' in calls["values"]["snapshot_json"]


def test_persist_load_run_snapshot_skips_non_execute_runs(monkeypatch):
  calls = []

  def fake_ensure_load_run_snapshot_table_once(**kwargs):
    calls.append(kwargs)

  monkeypatch.setattr(
    elevata_load_mod,
    "ensure_load_run_snapshot_table_once",
    fake_ensure_load_run_snapshot_table_once,
  )

  elevata_load_mod._persist_load_run_snapshot_best_effort(
    engine=object(),
    dialect=object(),
    runtime_state=elevata_load_mod.LoadRunRuntimeState.empty(),
    snapshot={"batch_run_id": "dry-run"},
    batch_run_id="dry-run",
    created_at="2026-07-09T12:00:00Z",
    root_dataset_key="rawcore.customer",
    execute=False,
    continue_on_error=False,
    max_retries=0,
    had_error=None,
    step_count=1,
  )

  assert calls == []


def test_persist_orchestration_skip_rows_uses_runtime_state_and_filters_results(monkeypatch):
  calls = {"ensure": [], "executed": [], "values": []}
  blocked_at = datetime.datetime(
    2026,
    7,
    24,
    4,
    18,
    38,
    123456,
    tzinfo=datetime.timezone.utc,
  )
  aborted_at = datetime.datetime(
    2026,
    7,
    24,
    4,
    18,
    39,
    654321,
    tzinfo=datetime.timezone.utc,
  )

  class DummyEngine:
    def execute(self, sql):
      calls["executed"].append(sql)

  class DummyDialectForLog:
    DIALECT_NAME = "duckdb"

    def render_insert_load_run_log(self, *, meta_schema, values):
      calls["values"].append(values)
      return f"INSERT INTO {meta_schema}.load_run_log VALUES ({values['target_dataset']})"

  def fake_ensure_load_run_log_table_for_batch(**kwargs):
    calls["ensure"].append(kwargs)
    kwargs["ensure_state"].add(("duckdb", kwargs["meta_schema"]))

  monkeypatch.setattr(
    elevata_load_mod,
    "_ensure_load_run_log_table_for_batch",
    fake_ensure_load_run_log_table_for_batch,
  )

  runtime_state = elevata_load_mod.LoadRunRuntimeState.empty()

  elevata_load_mod._persist_orchestration_skip_rows_best_effort(
    engine=DummyEngine(),
    dialect=DummyDialectForLog(),
    runtime_state=runtime_state,
    results=[
      {"dataset": "rawcore.loaded", "status": "success", "kind": "sql"},
      {
        "dataset": "rawcore.blocked_child",
        "status": "skipped",
        "kind": "blocked",
        "message": "Upstream failed",
        "attempt_no": 2,
        "status_reason": "dependency_failed",
        "blocked_by": "rawcore.parent",
        "load_run_id": "blocked-run",
        "started_at": blocked_at,
        "finished_at": blocked_at,
      },
      {
        "dataset": "rawcore.aborted_child",
        "status": "skipped",
        "kind": "aborted",
        "started_at": aborted_at,
        "finished_at": aborted_at,
      },
      {"dataset": "rawcore.other", "status": "skipped", "kind": "not_applicable"},
      {"dataset": "malformed", "status": "skipped", "kind": "blocked"},
    ],
    batch_run_id="batch-001",
    target_system_name="dwh",
    profile_name="dev",
    execute=True,
    meta_schema="meta",
    auto_provision=True,
  )

  assert len(calls["ensure"]) == 1
  assert calls["ensure"][0]["ensure_state"] is runtime_state.meta_log_ensure_state
  assert runtime_state.meta_log_ensure_state == {("duckdb", "meta")}
  assert calls["executed"] == [
    "INSERT INTO meta.load_run_log VALUES (blocked_child)",
    "INSERT INTO meta.load_run_log VALUES (aborted_child)",
  ]

  first, second = calls["values"]
  assert first["batch_run_id"] == "batch-001"
  assert first["load_run_id"] == "blocked-run"
  assert first["target_schema"] == "rawcore"
  assert first["target_dataset"] == "blocked_child"
  assert first["target_system"] == "dwh"
  assert first["profile"] == "dev"
  assert first["run_kind"] == "orchestration"
  assert first["mode"] == "orchestration"
  assert first["status"] == "skipped"
  assert first["attempt_no"] == 2
  assert first["status_reason"] == "dependency_failed"
  assert first["blocked_by"] == "rawcore.parent"
  assert first["started_at"] == blocked_at
  assert first["finished_at"] == blocked_at

  assert second["target_dataset"] == "aborted_child"
  assert second["run_kind"] == "orchestration"
  assert second["status"] == "skipped"
  assert second["started_at"] == aborted_at
  assert second["finished_at"] == aborted_at


def test_persist_orchestration_skip_rows_skips_non_execute_runs(monkeypatch):
  calls = []

  def fake_ensure_load_run_log_table_for_batch(**kwargs):
    calls.append(kwargs)

  monkeypatch.setattr(
    elevata_load_mod,
    "_ensure_load_run_log_table_for_batch",
    fake_ensure_load_run_log_table_for_batch,
  )

  elevata_load_mod._persist_orchestration_skip_rows_best_effort(
    engine=object(),
    dialect=object(),
    runtime_state=elevata_load_mod.LoadRunRuntimeState.empty(),
    results=[{"dataset": "rawcore.blocked_child", "status": "skipped", "kind": "blocked"}],
    batch_run_id="dry-run",
    target_system_name="dwh",
    profile_name="dev",
    execute=False,
  )

  assert calls == []


def test_format_execution_summary_lines_keeps_preflight_output_actionable():
  lines = elevata_load_mod._format_execution_summary_lines({
    "dataset": "rawcore.customer",
    "status": "blocked",
    "kind": "preflight",
    "message": "UNSAFE_TYPE_DRIFT: customer_id narrowing",
  })

  assert lines == [
    " ⚠ rawcore.customer                    preflight – blocked by UNSAFE_TYPE_DRIFT",
    "     hint: use --allow-type-alter to allow explicit narrowing/rebuild",
  ]


def test_raise_for_execution_failures_uses_first_exception_detail():
  with pytest.raises(elevata_load_mod.CommandError) as excinfo:
    elevata_load_mod._raise_for_execution_failures(
      had_error=True,
      continue_on_error=False,
      results=[{
        "dataset": "rawcore.customer",
        "status": "error",
        "kind": "exception",
        "message": "Connection failed",
      }],
    )

  assert str(excinfo.value) == "Load execution failed: rawcore.customer: Connection failed"


def test_raise_for_execution_failures_uses_continue_on_error_prefix():
  with pytest.raises(elevata_load_mod.CommandError) as excinfo:
    elevata_load_mod._raise_for_execution_failures(
      had_error=True,
      continue_on_error=True,
      results=[{
        "dataset": "rawcore.customer",
        "status": "blocked",
        "kind": "preflight",
        "message": "UNSAFE_TYPE_DRIFT",
      }],
    )

  assert str(excinfo.value) == (
    "One or more datasets were blocked by preflight checks: "
    "rawcore.customer: UNSAFE_TYPE_DRIFT"
  )
