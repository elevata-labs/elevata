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

import textwrap

from metadata.rendering.dialects.duckdb import DuckDBDialect


def test_render_delete_detection_statement_basic():
  # Arrange
  dialect = DuckDBDialect()

  sql = dialect.render_delete_detection_statement(
    target_schema="rawcore",
    target_table="rc_customer",
    stage_schema="stage",
    stage_table="stg_customer",
    key_columns=["customer_id"],
    scope_filter="customer_id > 100",
  )

  # Normalize whitespace for easier matching
  normalized = textwrap.dedent(sql).strip()

  # Assert basic structure
  assert "DELETE FROM" in normalized
  assert "rawcore" in normalized
  assert "rc_customer" in normalized
  assert '"stage"."stg_customer" AS s' in normalized

  # Business key join
  assert "t.\"customer_id\" = s.\"customer_id\"" in normalized

  # Scope filter must be present in WHERE
  assert "WHERE (customer_id > 100)" in normalized

  # NOT EXISTS subquery must be present
  assert "NOT EXISTS" in normalized
  assert "SELECT 1" in normalized


def test_render_delete_detection_statement_without_scope_filter():
  # Arrange
  dialect = DuckDBDialect()

  sql = dialect.render_delete_detection_statement(
    target_schema="rawcore",
    target_table="rc_customer",
    stage_schema="stage",
    stage_table="stg_customer",
    key_columns=["customer_id"],
    scope_filter=None,
  )

  normalized = " ".join(sql.split())

  # If scope_filter is None, we expect a TRUE guard (or equivalent)
  assert "WHERE TRUE" in normalized or "WHERE 1=1" in normalized
