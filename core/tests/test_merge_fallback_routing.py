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

from types import SimpleNamespace

import pytest

from metadata.rendering.load_sql import (
  render_merge_sql,
  _render_update_then_insert_sql,
)

class DummyDialectNoMerge:
  supports_merge = False

  def quote_ident(self, name: str) -> str:
    return f'"{name}"'

  def quote_table(self, schema: str, table: str) -> str:
    return f'{schema}."{table}"'

class DummyDialectWithMerge(DummyDialectNoMerge):
  supports_merge = True

class DummyTargetColumn:
  def __init__(self, name: str) -> None:
    self.target_column_name = name

def test_update_then_insert_sql_builds_expected_structure():
  td = SimpleNamespace()
  dialect = DummyDialectNoMerge()

  target_full = 'rawcore."customer"'
  source_full = 'stage."customer_stage"'

  target_alias = "t"
  source_alias = "s"

  key_cols = ["bk1"]
  target_cols = [
    DummyTargetColumn("bk1"),
    DummyTargetColumn("col_a"),
  ]

  expr_map = {
    "bk1": 's."bk1"',
    "col_a": "UPPER(s.\"col_a\")",
  }

  sql = _render_update_then_insert_sql(
    td=td,
    dialect=dialect,
    source_full=source_full,
    source_alias=source_alias,
    target_full=target_full,
    target_alias=target_alias,
    key_cols=key_cols,
    expr_map=expr_map,
    target_cols=target_cols,
  )

  # Basic structure checks
  assert "UPDATE rawcore.\"customer\" AS t" in sql
  assert "FROM stage.\"customer_stage\" AS s" in sql
  assert "INSERT INTO rawcore.\"customer\"" in sql
  assert "WHERE NOT EXISTS" in sql

  # Join predicate based on business key
  assert 't."bk1" = s."bk1"' in sql

  # Non-key column updated via expression
  assert ' "col_a" = UPPER(s."col_a")' in sql or '\"col_a\" = UPPER' in sql

def test_render_merge_sql_uses_fallback_when_merge_not_supported(monkeypatch):
  # Fake rawcore td
  rawcore_schema = SimpleNamespace(short_name="rawcore", schema_name="rawcore")
  td = SimpleNamespace(
    id=1,
    target_dataset_name="customer",
    target_schema=rawcore_schema,
    natural_key_fields=["bk1"],
  )

  # We avoid real ORM queries by monkeypatching help functions
  stage_schema = SimpleNamespace(short_name="stage", schema_name="stage")
  stage_td = SimpleNamespace(
    target_dataset_name="customer_stage",
    target_schema=stage_schema,
  )

  # _find_stage_upstream_for_rawcore(td) -> stage_td
  monkeypatch.setattr(
    "metadata.rendering.load_sql._find_stage_upstream_for_rawcore",
    lambda _td: stage_td,
  )

  # _get_target_columns_in_order(td) -> defined columns
  target_cols = [DummyTargetColumn("bk1"), DummyTargetColumn("col_a")]
  monkeypatch.setattr(
    "metadata.rendering.load_sql._get_target_columns_in_order",
    lambda _td: target_cols,
  )

  # _get_rendered_column_exprs_for_target(td, dialect) -> simple mapping
  expr_map = {
    "bk1": 's."bk1"',
    "col_a": 's."col_a"',
  }
  monkeypatch.setattr(
    "metadata.rendering.load_sql._get_rendered_column_exprs_for_target",
    lambda _td, _dialect: expr_map,
  )

  dialect = DummyDialectNoMerge()

  sql = render_merge_sql(td, dialect)

  # No MERGE, but UPDATE + INSERT
  assert "MERGE INTO" not in sql
  assert "UPDATE rawcore.\"customer\" AS t" in sql
  assert "INSERT INTO rawcore.\"customer\"" in sql


def test_render_merge_sql_uses_native_merge_when_supported(monkeypatch):
  rawcore_schema = SimpleNamespace(short_name="rawcore", schema_name="rawcore")
  td = SimpleNamespace(
    id=1,
    target_dataset_name="customer",
    target_schema=rawcore_schema,
    natural_key_fields=["bk1"],
  )

  stage_schema = SimpleNamespace(short_name="stage", schema_name="stage")
  stage_td = SimpleNamespace(
    target_dataset_name="customer_stage",
    target_schema=stage_schema,
  )

  monkeypatch.setattr(
    "metadata.rendering.load_sql._find_stage_upstream_for_rawcore",
    lambda _td: stage_td,
  )

  target_cols = [DummyTargetColumn("bk1"), DummyTargetColumn("col_a")]
  monkeypatch.setattr(
    "metadata.rendering.load_sql._get_target_columns_in_order",
    lambda _td: target_cols,
  )

  expr_map = {
    "bk1": 's."bk1"',
    "col_a": 's."col_a"',
  }
  monkeypatch.setattr(
    "metadata.rendering.load_sql._get_rendered_column_exprs_for_target",
    lambda _td, _dialect: expr_map,
  )

  dialect = DummyDialectWithMerge()

  sql = render_merge_sql(td, dialect)

  # Now we expect a MERGE statement
  assert sql.startswith("MERGE INTO rawcore.\"customer\" AS t")
  assert "USING stage.\"customer_stage\" AS s" in sql
  assert "WHEN MATCHED THEN" in sql
  assert "WHEN NOT MATCHED THEN" in sql
