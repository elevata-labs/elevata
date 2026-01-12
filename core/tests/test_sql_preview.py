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
    self.manual_expression = None
    # builder.py expects tcol.input_links to exist (queryset-like)
    self.input_links = EmptyQS()

class DummyTargetColumnsQS:
  def __init__(self, cols):
    self._cols = cols

  def filter(self, **_kwargs):
    # For tests we treat all as active.
    return self

  def order_by(self, *_args):
    return self._cols

class DummySourceColumn:
  def __init__(self, name: str):
    # builder.qualify_source_filter() expects "source_column_name"
    self.source_column_name = name
    # Keep "column_name" as a convenience alias (harmless)
    self.column_name = name

class DummySourceDataset:
  def __init__(
    self,
    *,
    static_filter: str = "",
    incremental: bool = False,
    increment_filter: str = "",
    source_columns: list[DummySourceColumn] | None = None,
    schema_name: str = "Sales",
    table_name: str = "SalesOrderHeader",    
    source_dataset_name: str | None = None,
  ):
    self.static_filter = static_filter
    self.incremental = incremental
    self.increment_filter = increment_filter
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


def test_build_source_dataset_where_sql_static_only():
  sd = DummySourceDataset(
    static_filter="is_active = 1",
    incremental=False,
    source_columns=[
      DummySourceColumn("is_active"),
      DummySourceColumn("updated_at"),
    ],
  )

  where_sql = builder_mod.build_source_dataset_where_sql(sd, source_alias="s")

  # Identifier should be qualified if it matches known source columns.
  assert "s.is_active" in where_sql
  assert "WHERE" not in where_sql  # function returns expression only, not full clause
  assert "AND" not in where_sql


def test_build_source_dataset_where_sql_incremental_only_preserves_delta_cutoff():
  sd = DummySourceDataset(
    static_filter="",
    incremental=True,
    increment_filter="updated_at >= {{DELTA_CUTOFF}}",
    source_columns=[
      DummySourceColumn("is_active"),
      DummySourceColumn("updated_at"),
    ],
  )

  where_sql = builder_mod.build_source_dataset_where_sql(sd, source_alias="s")

  assert "s.updated_at" in where_sql
  # Guard: DELTA_CUTOFF must remain untouched for runtime resolution.
  assert "{{DELTA_CUTOFF}}" in where_sql
  assert "AND" not in where_sql


def test_build_source_dataset_where_sql_static_and_incremental_combined():
  sd = DummySourceDataset(
    static_filter="is_active = 1",
    incremental=True,
    increment_filter="updated_at >= {{DELTA_CUTOFF}}",
    source_columns=[
      DummySourceColumn("is_active"),
      DummySourceColumn("updated_at"),
    ],
  )

  where_sql = builder_mod.build_source_dataset_where_sql(sd, source_alias="s")

  # Both parts should be qualified and combined.
  assert "s.is_active" in where_sql
  assert "s.updated_at" in where_sql
  assert "{{DELTA_CUTOFF}}" in where_sql
  assert " AND " in where_sql
  # We expect parentheses for safety when combining.
  assert where_sql.count("(") >= 2
  assert where_sql.count(")") >= 2

def test_build_source_dataset_where_sql_does_not_double_qualify_existing_alias():
  sd = DummySourceDataset(
    static_filter="s.is_active = 1",
    incremental=True,
    increment_filter="s.updated_at >= {{DELTA_CUTOFF}}",
    source_columns=[
      DummySourceColumn("is_active"),
      DummySourceColumn("updated_at"),
    ],
  )

  where_sql = builder_mod.build_source_dataset_where_sql(sd, source_alias="s")

  # Must not become "s.s.<col>"
  assert "s.s.is_active" not in where_sql
  assert "s.s.updated_at" not in where_sql

  # Must keep already qualified references intact.
  assert "s.is_active" in where_sql
  assert "s.updated_at" in where_sql
  assert "{{DELTA_CUTOFF}}" in where_sql

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

  # SourceDataset with both filters
  sd = DummySourceDataset(
    static_filter="OnlineOrderFlag = 0",
    incremental=True,
    increment_filter="ModifiedDate >= {{DELTA_CUTOFF}}",
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