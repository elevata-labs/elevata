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

from metadata.rendering.dsl import parse_surrogate_dsl
from metadata.rendering.dialects.dialect_factory import get_active_dialect
import pytest


DSL = "HASH256(CONCAT_WS('|', COALESCE({expr:a}, '<NULL>'), 'pepper'))"


def _render(dialect_name: str) -> str:
  dialect = get_active_dialect(dialect_name)
  expr = parse_surrogate_dsl(DSL, table_alias="t")
  return dialect.render_expr(expr)


@pytest.mark.parametrize(
  "dialect_name, must_contain_any",
  [
    ("bigquery", ["sha256(", "to_hex("]),
    ("databricks", ["sha2("]),
    ("duckdb", ["sha256("]),
    ("postgres", ["digest(", "'sha256'"]),
    ("fabric_warehouse", ["hashbytes('sha2_256'", "convert("]),
    ("mssql", ["hashbytes('sha2_256'", "convert("]),
    ("snowflake", ["sha2(", "to_varchar("]),
  ],
)
def test_hash256_contains_expected_primitives(dialect_name: str, must_contain_any: list[str]):
  sql = _render(dialect_name)
  lower = sql.lower()
  for needle in must_contain_any:
    assert needle in lower, f"{dialect_name}: expected '{needle}' in SQL:\n{sql}"


def test_runtime_pepper_token_resolves_only_during_dsl_parsing(monkeypatch):
  monkeypatch.setenv("ELEVATA_PEPPER", "runtime-secret")
  expr = parse_surrogate_dsl(
    "HASH256(CONCAT_WS('|', COALESCE({expr:a}, '<NULL>'), {runtime:pepper}))",
    table_alias="t",
  )
  sql = get_active_dialect("duckdb").render_expr(expr)

  assert "runtime-secret" in sql
  assert "{runtime:pepper}" not in sql
