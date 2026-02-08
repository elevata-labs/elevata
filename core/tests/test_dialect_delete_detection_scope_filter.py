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

from core.metadata.rendering.dialects.dialect_factory import get_active_dialect


@pytest.mark.parametrize(
  "dialect_name",
  ["databricks", "snowflake", "fabric_warehouse"],
)
def test_delete_detection_includes_scope_filter_before_not_exists(dialect_name):
  dialect = get_active_dialect(dialect_name)

  sql = dialect.render_delete_detection_statement(
    target_schema="rawcore",
    target_table="rc_test",
    stage_schema="stage",
    stage_table="stg_test",
    join_predicates=["t.bk = s.bk"],
    scope_filter="t.load_run_id = 'abc'",
  )

  lower = sql.lower()

  assert "t.load_run_id = 'abc'" in lower
  assert "not exists" in lower

  where_idx = lower.find("where")
  scope_idx = lower.find("t.load_run_id = 'abc'")
  not_exists_idx = lower.find("not exists")

  assert where_idx >= 0, f"{dialect_name}: expected WHERE in SQL:\n{sql}"
  assert scope_idx >= 0, f"{dialect_name}: expected scope_filter in SQL:\n{sql}"
  assert not_exists_idx >= 0, f"{dialect_name}: expected NOT EXISTS in SQL:\n{sql}"
  assert where_idx < scope_idx < not_exists_idx, (
    f"{dialect_name}: expected scope_filter before NOT EXISTS in WHERE clause:\n{sql}"
  )

  assert ") and (t.load_run_id = 'abc')" not in lower, (
    f"{dialect_name}: scope_filter must not be appended after NOT EXISTS:\n{sql}"
  )

import re

def test_fabric_warehouse_delete_detection_uses_delete_alias_form():
  dialect = get_active_dialect("fabric_warehouse")

  sql = dialect.render_delete_detection_statement(
    target_schema="rawcore",
    target_table="rc_test",
    stage_schema="stage",
    stage_table="stg_test",
    join_predicates=["t.bk = s.bk"],
    scope_filter="t.load_run_id = 'abc'",
  )

  lower = sql.lower().strip()

  # Must start with "delete t" (alias delete form)
  assert lower.startswith("delete t"), f"expected 'DELETE t' prefix:\n{sql}"

  # Must contain a FROM clause that targets the aliased table
  assert re.search(r"\bfrom\b", lower), f"expected FROM clause:\n{sql}"
  assert re.search(r"\bas\s+t\b", lower), f"expected target alias 'AS t':\n{sql}"
  assert re.search(r"^delete\s+t\b.*\bfrom\b.*\bas\s+t\b", lower, re.DOTALL)

