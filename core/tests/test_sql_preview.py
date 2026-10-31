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

"""
Tests for render_preview_sql, especially the *_hist guard.
"""

import pytest

from metadata.rendering import sql_service as sql_service_mod
from metadata.rendering import builder as builder_mod

class DummySchema:
  def __init__(self, schema_short):
    self.schema_short = schema_short
    self.short_name = schema_short
    self.schema_name = schema_short

class DummyTargetDataset:
  def __init__(self, name, schema_short="rawcore"):
    self.target_dataset_name = name
    self.target_schema = DummySchema(schema_short)

class EmptyQS(list):
  """A minimal queryset-like empty collection."""
  def select_related(self, *_args, **_kwargs):
    return self

  def prefetch_related(self, *_args, **_kwargs):
    return self

  def filter(self, *_args, **_kwargs):
    return self

  def order_by(self, *_args, **_kwargs):
    return self

  def all(self):
    return self

  def exists(self):
    return False

  def first(self):
    return None

class DummyTargetColumn:
  def __init__(self, target_column_name: str, system_role: str | None = None):
    self.target_column_name = target_column_name
    self.system_role = system_role
    self.surrogate_expression = None
    self.portable_expression = None
    # builder.py expects tcol.input_links to exist (queryset-like)
    self.input_links = EmptyQS()

class DummyTargetColumnsQS:
  def __init__(self, cols):
    self._cols = cols

  def filter(self, **_kwargs):
    # For tests we treat all as active.
    return self
  
  def exclude(self, **_kwargs):
    # builder/sql preview may exclude system roles; tests can ignore this.
    return self

  def order_by(self, *_args):
    return self._cols

class DummySourceColumn:
  def __init__(self, name: str):
    self.source_column_name = name
    self.column_name = name

class DummySourceDataset:
  def __init__(
    self,
    *,
    portable_static_filter=None,
    incremental: bool = False,
    portable_increment_filter=None,
    source_columns: list[DummySourceColumn] | None = None,
    schema_name: str = "Sales",
    table_name: str = "SalesOrderHeader",    
    source_dataset_name: str | None = None,
  ):
    self.portable_static_filter = portable_static_filter
    self.incremental = incremental
    self.portable_increment_filter = portable_increment_filter
    self.source_columns = source_columns or []
    self.schema_name = schema_name
    self.table_name = table_name
    # Some builder paths use source_dataset_name for the physical table name.
    self.source_dataset_name = source_dataset_name or table_name

class DummyInput:
  def __init__(self, source_dataset=None):
    self.source_dataset = source_dataset

class DummyInputsQS(list):
  # Mimic queryset-ish methods used in builder.py
  def select_related(self, *_args, **_kwargs):
    return self

  def prefetch_related(self, *_args, **_kwargs):
    return self

  def filter(self, *_args, **_kwargs):
    return self

  def order_by(self, *_args, **_kwargs):
    return self
  
def test_render_preview_sql_calls_renderer_for_non_hist(monkeypatch):
  """Non-history datasets should call render_select_for_target as usual."""
  td = DummyTargetDataset("rc_customer")
  dialect = object()

  calls = {"select": 0}

  def fake_render_select(dataset_arg, dialect_arg):
    calls["select"] += 1
    # Sanity: parameters are passed through unchanged
    assert dataset_arg is td
    assert dialect_arg is dialect
    return "SELECT 1"

  monkeypatch.setattr(sql_service_mod, "render_select_for_target", fake_render_select)

  sql = sql_service_mod.render_preview_sql(td, dialect)

  # Whitespace-insensitive check: beautify_sql may introduce line breaks.
  normalized = " ".join(sql.split())
  assert normalized == "SELECT 1"
  assert calls["select"] == 1


def test_render_preview_sql_skips_renderer_for_hist(monkeypatch):
  """History datasets (rawcore *_hist) must not call SQL rendering."""
  td = DummyTargetDataset("rc_customer_hist", schema_short="rawcore")
  dialect = object()

  def exploding(*_args, **_kwargs):
    raise AssertionError("SQL renderer must not be called for *_hist")

  # Patch the function that sql_service uses to render canonical SELECT.
  monkeypatch.setattr(sql_service_mod, "render_select_for_target", exploding)

  sql = sql_service_mod.render_preview_sql(td, dialect)

  assert "SQL preview for history dataset rc_customer_hist is not implemented yet." in sql


def _comparison_contract(column_name, operator, value):
  return {
    "version": 1,
    "expression": {
      "type": "comparison",
      "operator": operator,
      "left": {"type": "column", "name": column_name},
      "right": {"type": "literal", "value": value},
    },
  }


def _delta_cutoff_contract(column_name):
  return {
    "version": 1,
    "expression": {
      "type": "comparison",
      "operator": ">=",
      "left": {"type": "column", "name": column_name},
      "right": {"type": "runtime_value", "name": "delta_cutoff"},
    },
  }


def test_raw_select_applies_filters_and_renders_tech_placeholders(monkeypatch):
  """RAW selecting from SourceDataset must:
  - apply static + incremental filters on the source table
  - render load_run_id/loaded_at as runtime placeholders (not source columns)
  """
  # Arrange a RAW TargetDataset with two business cols and two technical cols.
  td = DummyTargetDataset("raw_salesorderheader", schema_short="raw")
  td.target_schema.schema_name = "raw"  # only if builder expects it (harmless)
  td.outgoing_references = EmptyQS()

  cols = [
    DummyTargetColumn("salesorderid"),
    DummyTargetColumn("modifieddate"),
    DummyTargetColumn("load_run_id", system_role="load_run_id"),
    DummyTargetColumn("loaded_at", system_role="loaded_at"),
  ]
  td.target_columns = DummyTargetColumnsQS(cols)

  # SourceDataset with both portable filters
  sd = DummySourceDataset(
    portable_static_filter=_comparison_contract("OnlineOrderFlag", "=", 0),
    incremental=True,
    portable_increment_filter=_delta_cutoff_contract("ModifiedDate"),
    source_columns=[
      DummySourceColumn("OnlineOrderFlag"),
      DummySourceColumn("ModifiedDate"),
      DummySourceColumn("SalesOrderID"),
    ],
  )

  # Builder reads inputs_qs and picks the first with source_dataset.
  inputs_qs = DummyInputsQS([DummyInput(source_dataset=sd)])

  # We need to force builder into the "raw reads directly from SourceDataset" path.
  # Patch whatever function/attribute builder uses to compute these. The exact hooks
  # vary, so we patch the local variables via small helper functions if present.
  #
  # Most codebases fetch "inputs_qs" from target_dataset.input_links or similar.
  # We'll patch "target_dataset.input_links" to return our dummy list.
  td.input_links = inputs_qs

  # Patch any ORM-ish access used in builder to retrieve inputs (common patterns).
  monkeypatch.setattr(builder_mod, "_get_inputs_qs_for_target", lambda _td: inputs_qs, raising=False)

  # Act
  logical = builder_mod.build_logical_select_for_target(td)

  # Assert WHERE contains both filters and preserves DELTA_CUTOFF
  where = getattr(logical, "where", None)
  assert where is not None
  where_sql = getattr(where, "sql", None) or str(where)
  assert "OnlineOrderFlag" in where_sql
  assert "ModifiedDate" in where_sql
  assert "{{DELTA_CUTOFF}}" in where_sql
  assert "AND" in where_sql

  # Assert select list contains runtime placeholders for technical fields
  select_list = getattr(logical, "select_list", [])
  rendered = " ".join(str(x) for x in select_list)
  assert "{{ load_run_id }}" in rendered
  assert "{{ load_timestamp }}" in rendered


def test_render_preview_sql_fails_closed_for_invalid_query_contract(monkeypatch):
  td = DummyTargetDataset("bc_invalid_union", schema_short="bizcore")
  td.query_head = object()
  td.query_root = None
  dialect = object()

  from metadata.generation import query_contract as query_contract_mod

  monkeypatch.setattr(
    query_contract_mod,
    "infer_query_node_contract",
    lambda _node: type("Contract", (), {
      "issues": ["ERROR: Union branch references missing input column 'catalog_desc'."]
    })(),
  )
  monkeypatch.setattr(
    sql_service_mod,
    "render_select_for_target",
    lambda *_args, **_kwargs: (_ for _ in ()).throw(
      AssertionError("Invalid query contracts must not be rendered.")
    ),
  )

  with pytest.raises(ValueError, match="Query contract is invalid"):
    sql_service_mod.render_preview_sql(td, dialect)


def test_beautify_sql_formats_nested_union_readably():
  raw = (
    "SELECT b.id AS id, b.name AS name FROM "
    "(SELECT s.id AS id, s.name AS name FROM rawcore.source_a AS s) AS b "
    "UNION ALL "
    "SELECT b.id AS id, b.name AS name FROM "
    "(SELECT s.id AS id, s.name AS name FROM rawcore.source_b AS s) AS b"
  )

  formatted = sql_service_mod.beautify_sql(raw)

  assert "\nUNION ALL\n" in formatted
  assert "FROM\n  (\n    SELECT" in formatted
  assert "      s.id AS id," in formatted
  assert "  ) AS b" in formatted


def test_beautify_sql_keeps_function_commas_and_literal_commas_inline():
  raw = (
    "SELECT CONCAT(first_name, ', ', last_name) AS label, "
    "COALESCE(account_no, 'none') AS account_no FROM customer"
  )

  formatted = sql_service_mod.beautify_sql(raw)

  assert "CONCAT(first_name, ', ', last_name) AS label," in formatted
  assert "COALESCE(account_no, 'none') AS account_no" in formatted
  assert "', ',\n" not in formatted


def test_beautify_sql_formats_join_predicate_and_where_conditions():
  raw = (
    "SELECT a.id AS id, b.name AS name FROM alpha AS a "
    "LEFT JOIN beta AS b ON a.id = b.alpha_id "
    "WHERE a.active = 1 AND b.deleted_at IS NULL"
  )

  formatted = sql_service_mod.beautify_sql(raw)

  assert "\n  LEFT JOIN beta AS b ON\n" in formatted
  assert "\n    a.id = b.alpha_id\n" in formatted
  assert "WHERE\n  a.active = 1\n  AND b.deleted_at IS NULL" in formatted


def test_beautify_sql_preserves_runtime_tokens_and_window_expression():
  raw = (
    "SELECT ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC, id) AS rn "
    "FROM customer WHERE changed_at >= {{DELTA_CUTOFF}}"
  )

  formatted = sql_service_mod.beautify_sql(raw)

  assert "{{DELTA_CUTOFF}}" in formatted
  assert "ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY created_at DESC, id) AS rn" in formatted


def test_beautify_sql_preserves_comment_lines():
  raw = "-- first line\n-- second line\n"

  formatted = sql_service_mod.beautify_sql(raw)

  assert formatted == "-- first line\n-- second line"


def test_render_merge_sql_formats_only_presentation_output(monkeypatch):
  canonical = "MERGE INTO target AS t USING source AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name = s.name"
  monkeypatch.setattr(sql_service_mod, "_render_merge_sql", lambda *_args, **_kwargs: canonical)

  assert sql_service_mod.render_merge_sql(object(), object(), presentation=False) == canonical
  presented = sql_service_mod.render_merge_sql(object(), object(), presentation=True)
  assert presented != canonical
  assert "MERGE" in presented
  assert "t.id = s.id" in presented


def test_render_delete_detection_sql_formats_only_presentation_output(monkeypatch):
  canonical = "DELETE FROM target WHERE id NOT IN (SELECT id FROM source)"
  monkeypatch.setattr(
    sql_service_mod,
    "_render_delete_missing_rows_sql",
    lambda *_args, **_kwargs: canonical,
  )

  assert sql_service_mod.render_delete_detection_sql(object(), object(), presentation=False) == canonical
  presented = sql_service_mod.render_delete_detection_sql(object(), object(), presentation=True)
  assert presented != canonical
  assert "DELETE" in presented
  assert "SELECT" in presented
