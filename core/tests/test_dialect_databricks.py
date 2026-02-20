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

from metadata.rendering.dialects.dialect_factory import get_active_dialect
from metadata.rendering.dialects.databricks import DatabricksDialect


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
