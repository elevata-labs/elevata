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

import datetime

import pytest

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
  assert d.map_logical_type("BOOLEAN") == "BIT"


def test_mssql_map_logical_type_raises_on_unknown():
  d = MssqlDialect()
  with pytest.raises(ValueError):
    d.map_logical_type("THIS_TYPE_DOES_NOT_EXIST")
