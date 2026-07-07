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

from types import SimpleNamespace
 
from metadata.ingestion import landing
from metadata.ingestion import runtime_state


class DummyEngine:
  def __init__(self):
    self.executed = []

  def execute(self, sql):
    self.executed.append(sql)


class DummyDialect:
  DIALECT_NAME = "dummy"

  def render_create_schema_if_not_exists(self, schema_name):
    return f"CREATE SCHEMA IF NOT EXISTS {schema_name}"


def test_runtime_state_deduplicates_ingestion_schema_and_log_ensures(monkeypatch):
  engine = DummyEngine()
  dialect = DummyDialect()
  log_calls = []

  monkeypatch.setattr(
    runtime_state,
    "ensure_load_run_log_table",
    lambda **kwargs: log_calls.append(kwargs),
  )

  meta_state: set[tuple[str, str]] = set()
  schema_state: set[tuple[str, str]] = set()

  runtime_state.ensure_load_run_log_table_once(
    engine=engine,
    dialect=dialect,
    meta_schema="meta",
    auto_provision=True,
    ensure_state=meta_state,
  )
  runtime_state.ensure_load_run_log_table_once(
    engine=engine,
    dialect=dialect,
    meta_schema="meta",
    auto_provision=True,
    ensure_state=meta_state,
  )

  runtime_state.ensure_target_schema_once(
    engine=engine,
    dialect=dialect,
    schema_name="raw",
    auto_provision=True,
    ensure_state=schema_state,
  )
  runtime_state.ensure_target_schema_once(
    engine=engine,
    dialect=dialect,
    schema_name="raw",
    auto_provision=True,
    ensure_state=schema_state,
  )

  assert len(log_calls) == 1
  assert engine.executed == ["CREATE SCHEMA IF NOT EXISTS raw"]


class LandingEngine:
  def __init__(self):
    self.executed = []
    self.executed_many = []

  def execute(self, sql):
    self.executed.append(sql)

  def execute_many(self, sql, params):
    self.executed_many.append((sql, params))


class LandingDialect:
  DIALECT_NAME = "landing_test"

  def param_placeholder(self):
    return "?"

  def render_insert_values_statement(self, schema_name, table_name, *, target_columns, values_sql):
    return f"INSERT INTO {schema_name}.{table_name} ({', '.join(target_columns)}) VALUES {values_sql}"

  def render_create_schema_if_not_exists(self, schema_name):
    return f"CREATE SCHEMA IF NOT EXISTS {schema_name}"

  def render_drop_table_if_exists(self, *, schema, table, cascade=False):
    return f"DROP TABLE IF EXISTS {schema}.{table}"

  def render_create_table_if_not_exists(self, td):
    return f"CREATE TABLE IF NOT EXISTS {td.target_schema.schema_name}.{td.target_dataset_name}"

  def render_truncate_table(self, *, schema, table):
    return f"DELETE FROM {schema}.{table}"


class RelatedList(list):
  def all(self):
    return self

  def filter(self, **kwargs):
    out = RelatedList()
    for item in self:
      keep = True
      for key, expected in kwargs.items():
        if getattr(item, key, None) != expected:
          keep = False
          break
      if keep:
        out.append(item)
    return out

  def order_by(self, *_args):
    return self


def test_raw_landing_skips_truncate_after_drop_create(monkeypatch):
  engine = LandingEngine()
  dialect = LandingDialect()

  monkeypatch.setattr(landing, "ensure_load_run_log_table", lambda **_kwargs: None)
  monkeypatch.setattr(landing, "build_load_run_log_row", lambda **_kwargs: {})
  monkeypatch.setattr(landing, "extract_json_path", lambda obj, path: obj.get(path.lstrip("$.")))

  source_dataset = SimpleNamespace(
    source_columns=RelatedList([
      SimpleNamespace(
        source_column_name="order_id",
        integrate=True,
        ordinal_position=1,
        json_path="$.order_id",
      ),
    ]),
    source_system=SimpleNamespace(short_name="csv"),
    source_dataset_name="orders",
  )
  td = SimpleNamespace(
    target_schema=SimpleNamespace(schema_name="raw", short_name="raw"),
    target_dataset_name="raw_orders",
    target_columns=RelatedList([
      SimpleNamespace(target_column_name="order_id", system_role="", ordinal_position=1),
      SimpleNamespace(target_column_name="payload", system_role="payload", ordinal_position=2),
      SimpleNamespace(target_column_name="load_run_id", system_role="load_run_id", ordinal_position=3),
      SimpleNamespace(target_column_name="loaded_at", system_role="loaded_at", ordinal_position=4),
    ]),
  )

  landing.land_raw_json_records(
    target_engine=engine,
    target_dialect=dialect,
    td=td,
    records=[{"order_id": 1}],
    batch_run_id="batch",
    load_run_id="load",
    target_system=SimpleNamespace(short_name="dwh"),
    profile=SimpleNamespace(name="dev"),
    source_dataset=source_dataset,
    write_run_log=False,
  )

  assert "DROP TABLE IF EXISTS raw.raw_orders" in engine.executed
  assert "CREATE TABLE IF NOT EXISTS raw.raw_orders" in engine.executed
  assert "DELETE FROM raw.raw_orders" not in engine.executed
