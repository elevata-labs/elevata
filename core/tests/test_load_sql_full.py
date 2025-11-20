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

import types

from metadata.rendering.load_planner import build_load_plan
from metadata.rendering.load_sql import render_full_refresh_sql, render_load_sql_for_target
from metadata.rendering.dialects.duckdb import DuckDBDialect
from metadata.rendering import load_sql

class FakeTargetSchema:
  def __init__(self, schema_name: str, short_name: str = "rawcore"):
    self.schema_name = schema_name
    self.short_name = short_name


class FakeTargetDataset:
  """
  Minimal fake TargetDataset that provides just enough attributes for:

  - build_load_plan (materialization_type, incremental_strategy)
  - render_full_refresh_sql (target_schema, target_dataset_name)
  """
  def __init__(
    self,
    schema_name: str = "rawcore",
    dataset_name: str = "rc_customer",
    materialization_type: str = "table",
    incremental_strategy: str = "full",
  ):
    self.target_schema = FakeTargetSchema(schema_name=schema_name, short_name="rawcore")
    self.target_dataset_name = dataset_name
    self.materialization_type = materialization_type
    self.incremental_strategy = incremental_strategy

    # Attributes used only in merge/delete paths; stay minimal here
    self.handle_deletes = False
    self.historize = False


class FakeDialect(DuckDBDialect):
  """
  Wrapper dialect that allows us to intercept the INSERT call without
  depending on the exact formatting of DuckDBDialect.
  """
  def __init__(self):
    super().__init__()
    self.last_insert_call = None

  def render_insert_into_table(self, schema: str, table: str, select_sql: str) -> str:
    # Capture arguments for assertions
    self.last_insert_call = (schema, table, select_sql)
    # Provide a very simple SQL stub
    return f"-- INSERT INTO {schema}.{table}\n{select_sql}"


def test_build_load_plan_full_mode():
  td = FakeTargetDataset(incremental_strategy="full")
  plan = build_load_plan(td)

  assert plan.mode == "full"
  assert plan.handle_deletes is False
  assert plan.historize is False


def test_render_full_refresh_sql_uses_insert_hook(monkeypatch):
  td = FakeTargetDataset()
  dialect = FakeDialect()

  # Monkeypatch render_select_for_target to return a simple, stable SELECT
  from metadata.rendering import renderer

  def fake_render_select_for_target(target_ds, d):
    # Sanity check: correct objects passed in
    assert target_ds is td
    assert d is dialect
    return "SELECT 1 AS dummy_col"

  monkeypatch.setattr(load_sql, "render_select_for_target", fake_render_select_for_target)

  sql = render_full_refresh_sql(td, dialect)

  # Assert that our FakeDialect was invoked correctly
  assert dialect.last_insert_call == ("rawcore", "rc_customer", "SELECT 1 AS dummy_col")

  # And the returned SQL is based on that
  assert "INSERT INTO rawcore.rc_customer" in sql
  assert "SELECT 1 AS dummy_col" in sql


def test_render_load_sql_for_target_full_mode(monkeypatch):
  td = FakeTargetDataset()
  dialect = FakeDialect()

  # As before: fake the core SELECT
  from metadata.rendering import renderer

  def fake_render_select_for_target(target_ds, d):
    return "SELECT 42 AS answer"

  monkeypatch.setattr(load_sql, "render_select_for_target", fake_render_select_for_target)

  sql = render_load_sql_for_target(td, dialect)

  # Should internally call render_full_refresh_sql and thus our fake insert hook
  assert dialect.last_insert_call == ("rawcore", "rc_customer", "SELECT 42 AS answer")
  assert "INSERT INTO rawcore.rc_customer" in sql
  assert "SELECT 42 AS answer" in sql
