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

import pytest
from types import SimpleNamespace

from metadata.rendering.dialects.dialect_factory import get_active_dialect
from metadata.rendering.dialects.mssql import MssqlDialect


def test_mssql_dialect_is_registered():
  dialect = get_active_dialect("mssql")
  assert isinstance(dialect, MssqlDialect)


def test_mssql_quote_ident_uses_double_quotes():
  d = MssqlDialect()
  assert d.quote_ident("foo") == '"foo"'
  assert d.quote_ident('a"b') == '"a""b"'


def test_mssql_boolean_literal():
  d = MssqlDialect()
  assert d.render_literal(True) == "1"
  assert d.render_literal(False) == "0"


def test_mssql_date_literal_uses_cast():
  d = MssqlDialect()
  dval = datetime.date(2025, 1, 2)
  sql = d.render_literal(dval)
  assert "CAST(" in sql
  assert " AS DATE)" in sql


def test_mssql_concat_expression_uses_plus_operator():
  d = MssqlDialect()
  expr = d.concat_expression(["'a'", "'b'", "'c'"])
  # e.g. ('a' + 'b' + 'c')
  assert " + " in expr
  assert expr.startswith("(") and expr.endswith(")")


def test_mssql_map_logical_type_boolean_to_bit():
  d = MssqlDialect()
  assert d.map_logical_type(datatype="BOOLEAN") == "BIT"


def test_mssql_map_logical_type_raises_on_unknown():
  d = MssqlDialect()
  with pytest.raises(ValueError):
    d.map_logical_type(datatype="THIS_TYPE_DOES_NOT_EXIST")


def test_mssql_introspection_empty_sys_columns_is_treated_as_missing_table(monkeypatch):
  """
  Regression test:
  MSSQL sys.columns query can return 0 rows for a missing table.
  We must not treat that as "table exists with no columns", otherwise the planner
  will emit ADD COLUMN instead of CREATE TABLE and execution fails.
  """
  # Import inside test to match actual module path in project
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
    dialect = SimpleNamespace(name="mssql")

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

  # Depending on your implementation, this can either:
  # - raise (preferred, so SqlDialect.introspect_table returns table_exists=False), OR
  # - return {"columns": [], ...} but additionally carry a flag that callers interpret.
  #
  # We lock in the "raise on missing" behavior here, because it prevents the
  # "NO_COLUMNS_RETURNED" false-positive.
  with pytest.raises(Exception):
    read_table_metadata(_DummyEngine(), "rawcore", "does_not_exist")


def test_mssql_hist_update_and_delete_use_tsql_update_from_syntax():
  d = MssqlDialect()

  changed = d.render_hist_changed_update_sql(
    schema_name="rawcore",
    hist_table="rc_aw_customer_hist",
    rawcore_table="rc_aw_customer",
  )
  assert changed.startswith("UPDATE h\n")
  assert "FROM rawcore.rc_aw_customer_hist h" in changed
  assert "AS h" not in changed  # no ANSI aliasing

  deleted = d.render_hist_delete_sql(
    schema_name="rawcore",
    hist_table="rc_aw_customer_hist",
    rawcore_table="rc_aw_customer",
  )
  assert deleted.startswith("UPDATE h\n")
  assert "FROM rawcore.rc_aw_customer_hist h" in deleted
