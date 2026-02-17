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

import types
import uuid

import pytest

import metadata.management.commands.elevata_load as cmd_mod
from tests._dialect_test_mixin import DialectTestMixin


class DummyEngine:
  def __init__(self):
    self.executed = []

  def execute(self, sql):
    self.executed.append(sql)



class DummyDialect(DialectTestMixin):
  pass


def test_execution_snapshot_is_persisted_best_effort(monkeypatch):
  # Patch execute_plan to avoid touching ORM/SQL
  def fake_execute_plan(**kwargs):
    return ([
      {"status": "success", "kind": "ok", "dataset": "raw.a"},
    ], False)

  monkeypatch.setattr(cmd_mod, "execute_plan", fake_execute_plan)

  # Patch dataset resolution/order
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
  dialect = DummyDialect(engine=engine)

  monkeypatch.setattr(cmd_mod, "get_active_dialect", lambda _: dialect)

  # Make sure snapshot store does not try to introspect real warehouse metadata
  monkeypatch.setattr(cmd_mod, "ensure_load_run_snapshot_table", lambda **kwargs: None)

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
    "debug_execution": False,
    "write_execution_snapshot": False,
    "execution_snapshot_dir": ".elevata/execution_snapshots",
  }

  c.handle(**options)

  # Assert: snapshot INSERT happened at least once
  assert any("load_run_snapshot" in sql for sql in engine.executed)
