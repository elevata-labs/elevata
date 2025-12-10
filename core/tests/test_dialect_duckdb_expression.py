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

from metadata.rendering.dialects.duckdb import DuckDBDialect


def test_concat_expression_basic():
  dialect = DuckDBDialect()

  sql = dialect.concat_expression(["'A'", "'B'", "'C'"])
  assert sql == "('A' || 'B' || 'C')"


def test_concat_expression_single_element():
  dialect = DuckDBDialect()

  sql = dialect.concat_expression(["first_name"])
  assert sql == "(first_name)"


def test_concat_expression_empty_list():
  dialect = DuckDBDialect()

  sql = dialect.concat_expression([])
  assert sql == "''"


def test_hash_expression_sha256_default():
  dialect = DuckDBDialect()

  sql = dialect.hash_expression("customer_id")
  assert sql == "SHA256(customer_id)"


def test_hash_expression_explicit_sha256():
  dialect = DuckDBDialect()

  sql = dialect.hash_expression("email", algo="sha256")
  assert sql == "SHA256(email)"


def test_hash_expression_unknown_algo_falls_back_to_sha256():
  dialect = DuckDBDialect()

  sql = dialect.hash_expression("payload", algo="unknown_algo")
  assert sql == "SHA256(payload)"

def test_render_identifier_quoting():
  d = DuckDBDialect()
  assert d.render_identifier("customer") == "customer"
  assert d.render_identifier("CustomerName") == "CustomerName"
  assert d.render_identifier("1invalid") == '"1invalid"'
  assert d.render_identifier("sales-order") == '"sales-order"'
  # We don't yet detect SQL keywords; we only assert we get *some* string back.
  assert isinstance(d.render_identifier("select"), str)
