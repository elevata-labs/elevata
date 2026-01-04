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

import json
import types

import pytest

import metadata.management.commands.elevata_load as cmd_mod


class DummyEngine:
  def __init__(self):
    self.executed = []

  def execute(self, sql):
    self.executed.append(sql)
    # We do not return anything meaningful here because fetch_one_value is patched.
    return None

  def close(self):
    pass


class DummyDialect:
  def __init__(self, engine):
    self._engine = engine

  def get_execution_engine(self, system):
    return self._engine


def test_snapshot_diff_uses_db_baseline_over_file(monkeypatch):
  # --- Arrange: patch execute_plan to avoid running any dataset logic
  def fake_execute_plan(**kwargs):
    return ([
      {"status": "success", "kind": "ok", "dataset": "raw.a"},
    ], False)

  monkeypatch.setattr(cmd_mod, "execute_plan", fake_execute_plan)

  # --- Patch dataset resolution/order (avoid ORM)
  class DummySchema:
    short_name = "raw"

  class DummyTD:
    id = 1
    target_schema = DummySchema()
    target_dataset_name = "a"

  monkeypatch.setattr(cmd_mod.Command, "_resolve_target_dataset", lambda self, t, s: DummyTD())
  monkeypatch.setattr(cmd_mod, "resolve_execution_order", lambda root: [DummyTD()])

  # --- Patch profile/system
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

  # --- Patch plan builder to avoid depending on real TargetDataset structure
  class DummyStep:
    dataset_id = 1
    dataset_key = "raw.a"
    upstream_keys = ()

  class DummyPlan:
    batch_run_id = "BATCH_CURRENT"
    steps = [DummyStep()]

  monkeypatch.setattr(cmd_mod, "build_execution_plan", lambda **kwargs: DummyPlan())

  # --- Patch execution snapshot builder to return a known "current" snapshot
  current_snapshot = {
    "batch_run_id": "BATCH_CURRENT",
    "policy": {"continue_on_error": True, "max_retries": 0},
    "context": {"execute": False},
    "plan": {"steps": [{"dataset_key": "raw.a", "upstream_keys": []}]},
    "outcome": {"results": [{"dataset": "raw.a", "status": "success", "kind": "ok"}]},
  }
  monkeypatch.setattr(cmd_mod, "build_execution_snapshot", lambda **kwargs: current_snapshot)

  # --- DB baseline: render SQL + fetch JSON string
  calls = {"render_select": 0, "fetch_one": 0, "diff": 0, "render_diff": 0}

  def fake_render_select_load_run_snapshot_json(**kwargs):
    calls["render_select"] += 1
    # Ensure we target the correct table name
    return "SELECT snapshot_json FROM meta.load_run_snapshot WHERE batch_run_id = 'BATCH_BASELINE';"

  baseline_snapshot = {
    "batch_run_id": "BATCH_BASELINE",
    "policy": {"continue_on_error": True, "max_retries": 0},
    "context": {"execute": False},
    "plan": {"steps": [{"dataset_key": "raw.a", "upstream_keys": []}]},
    "outcome": {"results": [{"dataset": "raw.a", "status": "error", "kind": "exception"}]},
  }

  def fake_fetch_one_value(_engine, _sql):
    calls["fetch_one"] += 1
    return json.dumps(baseline_snapshot)

  monkeypatch.setattr(cmd_mod, "render_select_load_run_snapshot_json", fake_render_select_load_run_snapshot_json)
  monkeypatch.setattr(cmd_mod, "fetch_one_value", fake_fetch_one_value)

  # --- Patch diff + renderer so we only validate the DB baseline selection path
  def fake_diff_execution_snapshots(*, left, right):
    calls["diff"] += 1
    assert left.get("batch_run_id") == "BATCH_BASELINE"
    assert right.get("batch_run_id") == "BATCH_CURRENT"
    return {
      "summary": {"plan_changed": False, "policy_changed": False, "outcome_changed": True},
      "outcomes": {"status_changes": [{"dataset": "raw.a", "before": "error", "after": "success"}]},
      "plan": {},
      "policy": {},
    }

  def fake_render_execution_snapshot_diff_text(**kwargs):
    calls["render_diff"] += 1
    return "DIFF_OUTPUT\n"

  monkeypatch.setattr(cmd_mod, "diff_execution_snapshots", fake_diff_execution_snapshots)
  monkeypatch.setattr(cmd_mod, "render_execution_snapshot_diff_text", fake_render_execution_snapshot_diff_text)

  # --- Capture stdout writes
  out_lines = []
  c = cmd_mod.Command()
  c.stdout = types.SimpleNamespace(write=lambda s="": out_lines.append(str(s)))
  c.style = types.SimpleNamespace(NOTICE=lambda x: x, WARNING=lambda x: x)

  options = {
    "target_name": "a",
    "schema_short": "raw",
    "dialect_name": None,
    "target_system_name": None,

    # dry-run
    "execute": False,
    "no_print": False,

    # ensure we hit the diff block
    "debug_plan": False,
    "no_deps": True,
    "continue_on_error": True,
    "max_retries": 0,

    # snapshot flags (do not write a file)
    "debug_execution": False,
    "write_execution_snapshot": False,
    "execution_snapshot_dir": ".elevata/execution_snapshots",

    # DB baseline diff
    "diff_against_batch_run_id": "BATCH_BASELINE",
    "diff_against_snapshot": "SHOULD_NOT_BE_USED.json",
    "diff_print": True,
  }

  c.handle(**options)

  # --- Assert: DB baseline was used
  assert calls["render_select"] == 1
  assert calls["fetch_one"] == 1
  assert calls["diff"] == 1
  assert calls["render_diff"] == 1

  # --- Assert: diff output printed
  assert any("DIFF_OUTPUT" in line for line in out_lines)
