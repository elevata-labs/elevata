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

import sys
import types

import pytest

from metadata.rendering.dialects.dialect_factory import get_active_dialect
from metadata.rendering.dialects.databricks import DatabricksDialect, DatabricksExecutionEngine
from metadata.ingestion.types_map import canonicalize_type, canonical_type_str


def test_databricks_dialect_is_registered():
  d = get_active_dialect("databricks")
  assert isinstance(d, DatabricksDialect)


def test_databricks_quote_ident_uses_backticks():
  d = DatabricksDialect()
  assert d.quote_ident("foo") == "`foo`"
  assert d.quote_ident("a`b") == "`a``b`"


def test_databricks_hash_expression_uses_sha2():
  d = DatabricksDialect()
  sql = d.hash_expression("('x')")
  assert "sha2" in sql.lower()
  assert "256" in sql


def test_databricks_merge_renders_merge_into():
  d = DatabricksDialect()
  sql = d.render_merge_statement(
    target_fqn=d.render_table_identifier("dw", "dim_x"),
    source_select_sql="SELECT 1 AS id, 'a' AS payload",
    key_columns=["id"],
    update_columns=["payload"],
    insert_columns=["id", "payload"],
  )
  assert "merge into" in sql.lower()
  assert "when matched" in sql.lower()
  assert "when not matched" in sql.lower()

def test_databricks_timestamp_ntz_maps_to_timestamp():
  t = canonicalize_type("databricks", "timestamp_ntz")
  assert canonical_type_str(t) == "TIMESTAMP"


def test_databricks_struct_maps_to_json():
  t = canonicalize_type("databricks", "struct<a:int,b:string>")
  assert canonical_type_str(t) == "JSON"


def test_databricks_decimal_single_param_maps_to_precision_scale():
  t = canonicalize_type("databricks", "decimal(12)")
  assert canonical_type_str(t) == "DECIMAL(12,0)"


def test_databricks_drop_column_enables_column_mapping_best_effort():
  d = DatabricksDialect()
  sql = d.render_drop_column("dw", "t", "old_col")
  assert "set tblproperties" in sql.lower()
  assert "drop column" in sql.lower()


def test_databricks_alter_column_type_date_to_timestamp_forces_rebuild():
  d = DatabricksDialect()
  sql = d.render_alter_column_type(
    schema="dw",
    table="t",
    column="ship_date",
    new_type="TIMESTAMP",
    old_type="DATE",
  )
  assert sql == ""


def test_databricks_alter_column_type_invalid_decimal_widening_rule_forces_rebuild():
  d = DatabricksDialect()
  # Delta requires: (p_new - p_old) >= (s_new - s_old) >= 0
  sql = d.render_alter_column_type(
    schema="dw",
    table="t",
    column="amount",
    new_type="DECIMAL(19,4)",
    old_type="DECIMAL(18,2)",
  )
  assert sql == ""


def test_databricks_execution_engine_reuses_connection(monkeypatch):
  calls = {
    "connect": 0,
    "cursor": 0,
    "catalog": 0,
    "cursor_close": 0,
    "connection_close": 0,
  }
  statements: list[str] = []

  class FakeCursor:
    rowcount = 1

    def execute(self, sql, params=None):
      if str(sql).startswith("USE CATALOG"):
        calls["catalog"] += 1
      else:
        statements.append(str(sql))

    def close(self):
      calls["cursor_close"] += 1

  class FakeConnection:
    def __init__(self):
      self.cursor_obj = FakeCursor()

    def cursor(self):
      calls["cursor"] += 1
      return self.cursor_obj

    def close(self):
      calls["connection_close"] += 1

  def fake_connect(**_kwargs):
    calls["connect"] += 1
    return FakeConnection()

  fake_sql_module = types.SimpleNamespace(connect=fake_connect)
  fake_databricks_module = types.SimpleNamespace(sql=fake_sql_module)
  monkeypatch.setitem(sys.modules, "databricks", fake_databricks_module)
  monkeypatch.setitem(sys.modules, "databricks.sql", fake_sql_module)

  system = types.SimpleNamespace(
    short_name="dbx",
    security={
      "server_hostname": "example.cloud.databricks.com",
      "http_path": "/sql/1.0/warehouses/example",
      "access_token": "token",
      "catalog": "main",
    },
  )

  engine = DatabricksExecutionEngine(system)
  engine.execute("SELECT 1;")
  engine.execute("SELECT 2;")
  engine.close()
  engine.close()

  assert calls["connect"] == 1
  assert calls["cursor"] == 1
  assert calls["catalog"] == 1
  assert calls["cursor_close"] == 1
  assert calls["connection_close"] == 1
  assert statements == ["SELECT 1", "SELECT 2"]


def test_databricks_table_exists_uses_show_tables_without_describe():
  dialect = DatabricksDialect()
  calls = []

  class DummyEngine:
    def fetch_all(self, sql):
      calls.append(sql)
      if sql == "SHOW TABLES IN rawcore LIKE 'rc_customer'":
        return [("rawcore", "rc_customer", False)]
      return []

  exists = dialect.table_exists(
    schema_name="rawcore",
    table_name="rc_customer",
    introspection_engine=None,
    exec_engine=DummyEngine(),
  )

  assert exists is True
  assert calls == ["SHOW TABLES IN rawcore LIKE 'rc_customer'"]
  assert all("DESCRIBE TABLE" not in sql for sql in calls)
