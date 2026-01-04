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

import types
import uuid

import pytest

import metadata.management.commands.elevata_load as cmd_mod


class DummyDialect:
  def __init__(self, engine):
    self._engine = engine

  def get_execution_engine(self, system):
    return self._engine

  def render_insert_load_run_log(self, *, meta_schema, values):
    return f"INSERT INTO {meta_schema}.load_run_log (...) VALUES (...);"


class DummyEngine:
  def __init__(self):
    self.executed = []

  def execute(self, sql):
    self.executed.append(sql)


def test_orchestration_only_events_are_persisted(monkeypatch):
  calls = {
    "ensure": 0,
    "build_rows": [],
  }

  def fake_ensure_load_run_log_table(**kwargs):
    calls["ensure"] += 1

  def fake_build_load_run_log_row(**kwargs):
    calls["build_rows"].append(kwargs)
    return kwargs

  monkeypatch.setattr(cmd_mod, "ensure_load_run_log_table", fake_ensure_load_run_log_table)
  monkeypatch.setattr(cmd_mod, "build_load_run_log_row", fake_build_load_run_log_row)

  def fake_execute_plan(**kwargs):
    return ([
      {"status": "success", "kind": "ok", "dataset": "raw.a"},
      {
        "status": "skipped",
        "kind": "blocked",
        "dataset": "core.b",
        "message": "blocked_by_dependency: raw.a",
        "blocked_by": "raw.a",
        "status_reason": "blocked_by_dependency",
        "load_run_id": str(uuid.uuid4()),
        "attempt_no": 1,
      },
      {
        "status": "skipped",
        "kind": "aborted",
        "dataset": "core.c",
        "message": "aborted_due_to_fail_fast",
        "status_reason": "fail_fast_abort",
        "load_run_id": str(uuid.uuid4()),
        "attempt_no": 1,
      },
    ], True)

  monkeypatch.setattr(cmd_mod, "execute_plan", fake_execute_plan)

  # Avoid ORM by patching dataset resolution/order
  class DummySchema:
    short_name = "raw"

  class DummyTD:
    id = 1
    target_schema = DummySchema()
    target_dataset_name = "a"

  monkeypatch.setattr(cmd_mod.Command, "_resolve_target_dataset", lambda self, t, s: DummyTD())
  monkeypatch.setattr(cmd_mod, "resolve_execution_order", lambda root: [DummyTD()])

  # Patch profile/system
  class DummyProfile:
    name = "test"

  class DummySystem:
    short_name = "wh"
    type = "duckdb"

  monkeypatch.setattr(cmd_mod, "load_profile", lambda _: DummyProfile())
  monkeypatch.setattr(cmd_mod, "get_target_system", lambda _: DummySystem())

  engine = DummyEngine()
  dialect = DummyDialect(engine)

  monkeypatch.setattr(cmd_mod, "get_active_dialect", lambda _: dialect)

  # Run command
  c = cmd_mod.Command()
  c.stdout = types.SimpleNamespace(write=lambda *a, **k: None)
  c.style = types.SimpleNamespace(NOTICE=lambda x: x, WARNING=lambda x: x)

  options = {
    "target_name": "a",
    "schema_short": "raw",
    "dialect_name": None,
    "target_system_name": None,
    "execute": True,
    "no_print": True,
    "debug_plan": False,
    "no_deps": True,
    "continue_on_error": True,
    "max_retries": 0,
  }

  # Your handle() raises CommandError if had_error=True (which fake_execute_plan returns)
  with pytest.raises(Exception):
    c.handle(**options)

  # Table ensured once; 2 skipped outcomes inserted (blocked + aborted)
  assert calls["ensure"] == 1
  assert len(calls["build_rows"]) == 2
  assert len(engine.executed) == 2

  # Validate that the two inserted rows are the skipped events
  reasons = sorted([row.get("status_reason") for row in calls["build_rows"]])
  assert reasons == ["blocked_by_dependency", "fail_fast_abort"]
