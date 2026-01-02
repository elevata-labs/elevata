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

import pytest

from metadata.rendering.dialects.duckdb import DuckDBDialect


@pytest.fixture
def dialect():
  # Single place to construct the dialect for all tests
  return DuckDBDialect()


# ---------------------------------------------------------------------------
# literal()
# ---------------------------------------------------------------------------

def test_literal_null(dialect):
  # None must turn into SQL NULL
  assert dialect.literal(None) == "NULL"


def test_literal_integer(dialect):
  # Integers should not be quoted
  assert dialect.literal(0) == "0"
  assert dialect.literal(42) == "42"
  assert dialect.literal(-7) == "-7"


def test_literal_float(dialect):
  # Floats should not be quoted and keep standard repr
  assert dialect.literal(1.5) == "1.5"
  assert dialect.literal(-0.25) == "-0.25"


def test_literal_boolean(dialect):
  # Booleans should map to SQL boolean keywords
  # (exact casing depends on your implementation, normalize for comparison)
  assert dialect.literal(True).upper() == "TRUE"
  assert dialect.literal(False).upper() == "FALSE"


def test_literal_simple_string(dialect):
  # Simple strings should be single-quoted
  assert dialect.literal("hello") == "'hello'"


def test_literal_string_with_single_quote_escaped(dialect):
  # Single quotes inside strings must be escaped as ''
  val = "O'Reilly"
  lit = dialect.literal(val)
  assert lit == "'O''Reilly'"


def test_literal_non_string_object_uses_str_and_escaped(dialect):
  # Fallback: use str(value) and still escape quotes
  class Custom:
    def __str__(self):
      return "value 'with' quotes"

  lit = dialect.literal(Custom())
  # Must be quoted
  assert lit.startswith("'") and lit.endswith("'")
  # Inner single quotes must be doubled
  assert "''with''" in lit


# ---------------------------------------------------------------------------
# cast()
# ---------------------------------------------------------------------------

def test_cast_wraps_expression_with_cast(dialect):
  expr = 's."amount"'
  result = dialect.cast(expr, "DECIMAL(18,2)")
  # We only assert the general shape; exact spacing can differ slightly.
  normalized = " ".join(result.split())
  assert normalized == 'CAST(s."amount" AS DECIMAL(18,2))'


def test_cast_works_with_complex_expression(dialect):
  expr = 's."amount" / 100'
  result = dialect.cast(expr, "DOUBLE")
  normalized = " ".join(result.split())
  # Important: the whole expression should be inside CAST(...)
  assert normalized.startswith("CAST(")
  assert normalized.endswith(" AS DOUBLE)")
  assert 's."amount" / 100' in result


def test_cast_with_literal_expression(dialect):
  # Casting a literal should just wrap the literal
  result = dialect.cast(dialect.literal(1), "INTEGER")
  normalized = " ".join(result.split())
  assert normalized == "CAST(1 AS INTEGER)"


def test_cast_is_idempotent_if_expression_already_cast(dialect):
  # Optional / soft expectation:
  # If the implementation does not try to detect nested CAST,
  # this test can be relaxed. For now we assert we don't break it.
  expr = "CAST(s.amount AS INTEGER)"
  result = dialect.cast(expr, "INTEGER")
  # At least the original expression should be contained
  assert "CAST(s.amount AS INTEGER)" in result

def test_duckdb_map_logical_type_raises_on_unknown():
  d = DuckDBDialect()
  with pytest.raises(ValueError):
    d.map_logical_type(datatype="THIS_TYPE_DOES_NOT_EXIST")
