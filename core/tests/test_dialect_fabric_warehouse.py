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

import pytest
from types import SimpleNamespace

from metadata.rendering.dialects.dialect_factory import get_active_dialect
from metadata.rendering.dialects.fabric_warehouse import FabricWarehouseDialect


def test_fabric_warehouse_dialect_is_registered():
  d = get_active_dialect("fabric_warehouse")
  assert isinstance(d, FabricWarehouseDialect)


def test_fabric_warehouse_quote_ident_uses_square_brackets():
  d = FabricWarehouseDialect()
  assert d.quote_ident("foo") == "[foo]"
  assert d.quote_ident("a]b") == "[a]]b]"


def test_fabric_warehouse_hash_expression_uses_hashbytes():
  d = FabricWarehouseDialect()
  sql = d.hash_expression("('x')")
  low = sql.lower()
  assert "hashbytes" in low
  assert "sha2_256" in sql.lower()


def test_fabric_warehouse_delete_detection_contains_delete():
  d = FabricWarehouseDialect()
  sql = d.render_delete_detection_statement(
    target_schema="dw",
    target_table="dim_x",
    stage_schema="stg",
    stage_table="dim_x_stage",
    join_predicates=["t.id = s.id"],
  )
  assert "delete" in sql.lower()
  assert "not exists" in sql.lower()


def test_fabric_introspection_empty_sys_columns_is_treated_as_missing_table(monkeypatch):
  """
  Regression test:
  Fabric Warehouse (T-SQL endpoint) introspection can return 0 rows from sys.columns
  when the table does not exist. We must not treat this as "existing table with no columns".
  """
  from metadata.system.introspection import read_table_metadata
  import metadata.system.introspection as intro

  class _DummyResult:
    def mappings(self):
      return self

    def all(self):
      return []  # <-- simulate 0 rows from sys.columns

  class _DummyConn:
    def execute(self, *_args, **_kwargs):
      return _DummyResult()

    def __enter__(self):
      return self

    def __exit__(self, exc_type, exc, tb):
      return False

  class _DummyEngine:
    # Your read_table_metadata routes Fabric via engine.dialect.name
    dialect = SimpleNamespace(name="fabric_warehouse")

    def connect(self):
      return _DummyConn()

  class _DummyInspector:
    def has_table(self, *_args, **_kwargs):
      return False

    def get_pk_constraint(self, *_args, **_kwargs):
      return {}

    def get_foreign_keys(self, *_args, **_kwargs):
      return []

  monkeypatch.setattr(intro, "inspect", lambda _engine: _DummyInspector())

  with pytest.raises(Exception):
    read_table_metadata(_DummyEngine(), "rawcore", "does_not_exist")


def test_fabric_hist_update_and_delete_use_tsql_update_from_syntax():
  d = FabricWarehouseDialect()

  changed = d.render_hist_changed_update_sql(
    schema_name="rawcore",
    hist_table="rc_aw_customer_hist",
    rawcore_table="rc_aw_customer",
  )
  assert changed.startswith("UPDATE h\n")
  assert "FROM rawcore.rc_aw_customer_hist h" in changed
  assert "AS h" not in changed

  deleted = d.render_hist_delete_sql(
    schema_name="rawcore",
    hist_table="rc_aw_customer_hist",
    rawcore_table="rc_aw_customer",
  )
  assert deleted.startswith("UPDATE h\n")
  assert "FROM rawcore.rc_aw_customer_hist h" in deleted

def test_fabric_string_without_max_length_renders_as_varchar_max():
  d = FabricWarehouseDialect()
  t = d._render_canonical_type_fabric_warehouse(datatype="STRING", max_length=None, strict=True)
  assert t.upper() == "VARCHAR(MAX)"

def test_fabric_string_over_threshold_renders_as_varchar_max():
  d = FabricWarehouseDialect()
  t = d._render_canonical_type_fabric_warehouse(datatype="STRING", max_length=10000, strict=True)
  assert t.upper() == "VARCHAR(MAX)"

def test_fabric_json_renders_as_varchar_max():
  d = FabricWarehouseDialect()
  t = d._render_canonical_type_fabric_warehouse(datatype="JSON", strict=True)
  assert t.upper() == "VARCHAR(MAX)"