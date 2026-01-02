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

from __future__ import annotations

import re

import pytest

from metadata.materialization.planner import build_materialization_plan
from metadata.materialization.policy import MaterializationPolicy

# ---------------------------------------------------------------------
# Minimal doubles
# ---------------------------------------------------------------------

class DummyDialect:
  def map_logical_type(self, datatype, max_length=None, precision=None, scale=None, strict=True):
    dt = (datatype or "").upper()
    if dt in ("INT", "INTEGER"):
      return "INTEGER"
    return "VARCHAR"

  def introspect_table(
    self,
    *,
    schema_name: str,
    table_name: str,
    introspection_engine,
    exec_engine=None,
    debug_plan: bool = False,
  ):
    # This method is monkeypatched in tests to return specific schemas.
    return {
      "table_exists": False,
      "physical_table": table_name,
      "actual_cols_by_norm_name": {},
    }

  def render_create_schema_if_not_exists(self, schema_name: str) -> str:
    return f"CREATE SCHEMA IF NOT EXISTS {schema_name};"

  def render_rename_column(self, schema_name: str, table_name: str, old_col: str, new_col: str) -> str:
    return f'ALTER TABLE {schema_name}.{table_name} RENAME COLUMN "{old_col}" TO "{new_col}"'

  def render_add_column(self, schema_name: str, table_name: str, col_name: str, col_type: str) -> str:
    return f'ALTER TABLE {schema_name}.{table_name} ADD COLUMN "{col_name}" {col_type}'


class DummySchema:
  def __init__(self, short_name="rawcore", schema_name="rawcore"):
    self.short_name = short_name
    self.schema_name = schema_name
    self.surrogate_keys_enabled = True


class DummyCol:
  def __init__(self, name: str, *, datatype="STRING", former_names=None, active=True, ordinal_position=1, id_=1):
    self.target_column_name = name
    self.datatype = datatype
    self.max_length = None
    self.decimal_precision = None
    self.decimal_scale = None
    self.former_names = former_names or []
    self.active = active
    self.ordinal_position = ordinal_position
    self.id = id_


class DummyTargetColumns:
  def __init__(self, items):
    self._items = list(items)

  def filter(self, active=True):
    if active is True:
      return DummyTargetColumns([c for c in self._items if getattr(c, "active", True)])
    return self

  def order_by(self, *_args):
    return sorted(self._items, key=lambda c: (getattr(c, "ordinal_position", 0), getattr(c, "id", 0)))


class DummyTD:
  def __init__(self, dataset_name: str, *, schema=None, cols=None, former_names=None):
    self.target_schema = schema or DummySchema()
    self.target_dataset_name = dataset_name
    self.former_names = former_names or []
    self.target_columns = DummyTargetColumns(cols or [])
    self.historize = True


def _policy() -> MaterializationPolicy:
  return MaterializationPolicy(
    sync_schema_shorts={"rawcore"},
    allow_auto_drop_columns=False,
    allow_type_alter=False,
  )


def _patch_introspection(monkeypatch, *, table_exists: bool, physical_cols: list[tuple[str, str]]):
  """
  Patch dialect.introspect_table() to return a predictable physical schema. 
  """
  cols = {}
  for (n, t) in physical_cols:
    cols[str(n).lower()] = {"name": n, "type": t}

  def _introspect_table(self, *, schema_name, table_name, introspection_engine, exec_engine=None, debug_plan=False):
    return {
      "table_exists": bool(table_exists),
      "physical_table": table_name,
      "actual_cols_by_norm_name": dict(cols),
    }

  monkeypatch.setattr(DummyDialect, "introspect_table", _introspect_table, raising=True)

WARNING_CODE_RE = re.compile(r"^[A-Z0-9_]+: ")

def _assert_categorized(plan):
  # Warn/block messages must be categorized via CODE: prefix.
  for w in plan.warnings:
    assert WARNING_CODE_RE.match(w), f"Warning not categorized: {w}"
  for e in plan.blocking_errors:
    assert WARNING_CODE_RE.match(e), f"Blocking error not categorized: {e}"


# ---------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------

def test_planner_renames_column_when_former_name_matches_physical(monkeypatch):
  # desired missing physically, former exists -> RENAME_COLUMN should be planned
  col = DummyCol("new_col", former_names=["old_col"], ordinal_position=1, id_=1)
  td = DummyTD("rc_test", cols=[col])
  dialect = DummyDialect()

  _patch_introspection(monkeypatch, table_exists=True, physical_cols=[("old_col", "INTEGER")])

  plan = build_materialization_plan(
    td=td,
    introspection_engine=object(),
    exec_engine=None,
    dialect=dialect,
    policy=_policy(),
  )

  ops = [s.op for s in plan.steps]
  assert "ENSURE_SCHEMA" in ops
  assert "RENAME_COLUMN" in ops
  assert "ADD_COLUMN" not in ops

  _assert_categorized(plan)


def test_planner_warns_and_does_not_rename_when_both_desired_and_former_exist(monkeypatch):
  # desired exists physically, former exists physically -> warn, no rename
  col = DummyCol("new_col", former_names=["old_col"], ordinal_position=1, id_=1)
  td = DummyTD("rc_test", cols=[col])
  dialect = DummyDialect()

  _patch_introspection(
    monkeypatch,
    table_exists=True,
    physical_cols=[
      ("new_col", "INTEGER"),
      ("old_col", "INTEGER"),
    ],
  )

  plan = build_materialization_plan(
    td=td,
    introspection_engine=object(),
    exec_engine=None,
    dialect=dialect,
    policy=_policy(),
  )

  ops = [s.op for s in plan.steps]
  assert "ENSURE_SCHEMA" in ops
  assert "RENAME_COLUMN" not in ops
  assert "ADD_COLUMN" not in ops

  assert any("Duplicate physical columns detected" in w for w in plan.warnings)
  _assert_categorized(plan)

def test_planner_renames_and_does_not_add_when_former_exists(monkeypatch):
  col = DummyCol("col_new", former_names=["col_old"], ordinal_position=1, id_=1)
  td = DummyTD("rc_test", cols=[col])
  dialect = DummyDialect()

  _patch_introspection(monkeypatch, table_exists=True, physical_cols=[("col_old", "VARCHAR")])

  plan = build_materialization_plan(
    td=td,
    introspection_engine=object(),
    exec_engine=None,
    dialect=dialect,
    policy=_policy(),
  )

  ops = [s.op for s in plan.steps]
  assert "RENAME_COLUMN" in ops
  assert "ADD_COLUMN" not in ops
  _assert_categorized(plan)


def test_planner_warns_and_does_not_rename_when_multiple_former_exist(monkeypatch):
  col = DummyCol("new_col", former_names=["old_1", "old_2"], ordinal_position=1, id_=1)
  td = DummyTD("rc_test", cols=[col])
  dialect = DummyDialect()

  _patch_introspection(
    monkeypatch,
    table_exists=True,
    physical_cols=[
      ("old_1", "INTEGER"),
      ("old_2", "INTEGER"),
    ],
  )

  plan = build_materialization_plan(
    td=td,
    introspection_engine=object(),
    exec_engine=None,
    dialect=dialect,
    policy=_policy(),
  )

  ops = [s.op for s in plan.steps]
  assert "RENAME_COLUMN" not in ops
  assert "ADD_COLUMN" not in ops

  assert any("Multiple former_names match physical columns" in w for w in plan.warnings)
  _assert_categorized(plan)


def test_planner_warnings_and_blocking_errors_are_categorized(monkeypatch):
  # Create a scenario that yields a warning (ambiguous rename).
  col = DummyCol("new_col", former_names=["old_col"], ordinal_position=1, id_=1)
  td = DummyTD("rc_test", cols=[col])
  dialect = DummyDialect()

  _patch_introspection(
    monkeypatch,
    table_exists=True,
    physical_cols=[
      ("new_col", "INTEGER"),
      ("old_col", "INTEGER"),
    ],
  )

  plan = build_materialization_plan(
    td=td,
    introspection_engine=object(),
    exec_engine=None,
    dialect=dialect,
    policy=_policy(),
  )

  _assert_categorized(plan)
  assert len(plan.warnings) >= 1
