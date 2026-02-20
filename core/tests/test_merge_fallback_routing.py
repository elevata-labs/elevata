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

from types import SimpleNamespace

import pytest

from metadata.rendering.load_sql import render_merge_sql
from tests._dialect_test_mixin import DialectTestMixin


class DummyDialectNoMerge(DialectTestMixin):
  supports_merge = False
  supports_delete_detection = False
  pass


class DummyDialectWithMerge(DialectTestMixin):
  pass


class SpyDialect(DialectTestMixin):
  """
  Captures the keyword arguments passed to render_merge_statement so we can
  assert load_sql delegates semantic ingredients correctly.
  """
  def render_merge_statement(self, **kwargs) -> str:  # type: ignore[override]
    self.calls.append({"merge_kwargs": dict(kwargs)})
    return "-- DIALECT MERGE"


class DummyTargetColumn:
  def __init__(self, name, **kwargs):
    self.target_column_name = name
    # ordinal_position is only used for ordering; we stub it here
    self.ordinal_position = 1
    self.datatype = kwargs.get("datatype", "STRING")
    self.max_length = kwargs.get("max_length")
    self.decimal_precision = kwargs.get("decimal_precision")
    self.decimal_scale = kwargs.get("decimal_scale")


def test_merge_fallback_renders_update_and_insert_when_merge_not_supported():
  """
  When supports_merge=False, the dialect-level render_merge_statement must still
  provide merge semantics via UPDATE + INSERT ... WHERE NOT EXISTS.
  """
  dialect = DummyDialectNoMerge()

  sql = dialect.render_merge_statement(
    target_fqn=dialect.render_table_identifier("rawcore", "customer"),
    source_select_sql="SELECT 1 AS bk1, 'x' AS col_a",
    key_columns=["bk1"],
    update_columns=["col_a"],
    insert_columns=["bk1", "col_a"],
    target_alias="t",
    source_alias="s",
  )

  low = sql.lower()
  assert "update" in low
  assert "insert into" in low
  assert "where not exists" in low
  # Join predicate based on business key
  assert "t.bk1 = s.bk1" in sql


def test_render_merge_sql_uses_fallback_when_merge_not_supported(monkeypatch):
  # Fake rawcore td
  rawcore_schema = SimpleNamespace(short_name="rawcore", schema_name="rawcore")
  td = SimpleNamespace(
    id=1,
    target_dataset_name="customer",
    target_schema=rawcore_schema,
    natural_key_fields=["bk1"],
  )

  # Provide upstream stage dataset to avoid accessing td.input_links in real resolver
  stage_schema = SimpleNamespace(short_name="stage", schema_name="stage")
  stage_td = SimpleNamespace(target_dataset_name="customer_stage", target_schema=stage_schema)
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

  monkeypatch.setattr(
    "metadata.rendering.load_sql._get_rendered_column_exprs_for_target",
    lambda _td, _dialect: {"bk1": "s.bk1", "col_a": "s.col_a"},
  )

  dialect = DummyDialectNoMerge()

  sql = render_merge_sql(td, dialect)

  # No MERGE, but UPDATE + INSERT (fallback)
  low = sql.lower()
  assert "merge into" not in low
  assert "update" in low
  assert "insert into" in low
  assert "where not exists" in low


def test_render_merge_sql_uses_native_merge_when_supported(monkeypatch):
  rawcore_schema = SimpleNamespace(short_name="rawcore", schema_name="rawcore")
  td = SimpleNamespace(
    id=1,
    target_dataset_name="customer",
    target_schema=rawcore_schema,
    natural_key_fields=["bk1"],
  )

  stage_schema = SimpleNamespace(short_name="stage", schema_name="stage")
  stage_td = SimpleNamespace(target_dataset_name="customer_stage", target_schema=stage_schema)
  monkeypatch.setattr(
    "metadata.rendering.load_sql._find_stage_upstream_for_rawcore",
    lambda _td: stage_td,
  )

  target_cols = [DummyTargetColumn("bk1"), DummyTargetColumn("col_a")]
  monkeypatch.setattr(
    "metadata.rendering.load_sql._get_target_columns_in_order",
    lambda _td: target_cols,
  )

  monkeypatch.setattr(
    "metadata.rendering.load_sql._get_rendered_column_exprs_for_target",
    lambda _td, _dialect: {"bk1": "s.bk1", "col_a": "s.col_a"},
  )

  dialect = DummyDialectWithMerge()

  sql = render_merge_sql(td, dialect)

  # Now we expect a MERGE statement
  low = sql.lower()
  assert "merge" in low
  assert "when matched" in low
  assert "when not matched" in low


def test_render_merge_sql_delegates_semantic_ingredients_to_dialect(monkeypatch):
  """
  Ensures load_sql.render_merge_sql does not build SQL shape itself, but passes
  semantic ingredients to dialect.render_merge_statement.
  """
  rawcore_schema = SimpleNamespace(short_name="rawcore", schema_name="rawcore")
  td = SimpleNamespace(
    id=1,
    target_dataset_name="customer",
    target_schema=rawcore_schema,
    natural_key_fields=["bk1"],
  )

  stage_schema = SimpleNamespace(short_name="stage", schema_name="stage")
  stage_td = SimpleNamespace(target_dataset_name="customer_stage", target_schema=stage_schema)
  monkeypatch.setattr(
    "metadata.rendering.load_sql._find_stage_upstream_for_rawcore",
    lambda _td: stage_td,
  )

  target_cols = [DummyTargetColumn("bk1"), DummyTargetColumn("col_a")]
  monkeypatch.setattr(
    "metadata.rendering.load_sql._get_target_columns_in_order",
    lambda _td: target_cols,
  )
  monkeypatch.setattr(
    "metadata.rendering.load_sql._get_rendered_column_exprs_for_target",
    lambda _td, _dialect: {"bk1": "s.bk1", "col_a": "s.col_a"},
  )


  dialect = DummyDialectWithMerge()
  sql = render_merge_sql(td, dialect)

  assert dialect.calls, "Expected merge to be delegated"
  merge_kwargs = dialect.calls[-1]["merge_kwargs"]

  assert merge_kwargs["key_columns"] == ["bk1"]
  assert merge_kwargs["insert_columns"] == ["bk1", "col_a"]