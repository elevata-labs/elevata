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

from __future__ import annotations

from metadata.rendering.dialects.duckdb import DuckDBDialect


def test_reference_integrity_missing_examples_sql_uses_default_limit_shape() -> None:
  """Default dialect rendering should use anti-join semantics with LIMIT."""
  d = DuckDBDialect()

  sql = d.render_reference_integrity_missing_examples_statement(
    child_schema="bizcore",
    child_table="bc_order",
    parent_schema="rawcore",
    parent_table="rc_customer",
    key_pairs=[
      ("customer_id", "customer_id"),
      ("sales_org", "sales_org"),
    ],
    example_limit=20,
  )

  assert "SELECT DISTINCT" in sql
  assert 'FROM bizcore.bc_order AS c' in sql
  assert 'LEFT JOIN rawcore.rc_customer AS p' in sql
  assert 'c.customer_id = p.customer_id' in sql
  assert 'c.sales_org = p.sales_org' in sql
  assert 'p.customer_id IS NULL' in sql
  assert 'c.customer_id IS NOT NULL' in sql
  assert 'c.sales_org IS NOT NULL' in sql
  assert "ORDER BY" in sql
  assert "LIMIT 20" in sql


def test_reference_integrity_missing_examples_sql_rejects_empty_key_pairs() -> None:
  """Reference Integrity Review SQL requires at least one complete key pair."""
  d = DuckDBDialect()

  try:
    d.render_reference_integrity_missing_examples_statement(
      child_schema="bizcore",
      child_table="bc_order",
      parent_schema="rawcore",
      parent_table="rc_customer",
      key_pairs=[],
      example_limit=20,
    )
  except ValueError as exc:
    assert "key_pairs" in str(exc)
  else:  # pragma: no cover - explicit failure branch for readability
    raise AssertionError("Expected ValueError for empty key_pairs")
