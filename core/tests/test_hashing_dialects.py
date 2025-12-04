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

from metadata.rendering.dsl import parse_surrogate_dsl
from metadata.rendering.dialects.dialect_factory import get_active_dialect


DSL = "HASH256(CONCAT_WS('|', COALESCE({expr:a}, '<NULL>'), 'pepper'))"


def _render(dialect_name: str) -> str:
  dialect = get_active_dialect(dialect_name)
  expr = parse_surrogate_dsl(DSL, table_alias="t")
  return dialect.render_expr(expr)


def test_hash256_duckdb():
  sql = _render("duckdb")
  assert "SHA256(" in sql.upper()


def test_hash256_postgres():
  sql = _render("postgres")
  lower = sql.lower()
  assert "digest(" in lower
  assert "'sha256'" in lower


def test_hash256_mssql():
  sql = _render("mssql")
  upper = sql.upper()
  assert "HASHBYTES('SHA2_256'" in upper
  assert "CONVERT(" in upper
