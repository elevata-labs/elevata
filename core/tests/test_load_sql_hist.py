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
Tests for history load SQL routing.

For *_hist datasets, render_load_sql_for_target should NOT call the
generic full/merge/append renderers, but a dedicated history renderer
which currently returns a descriptive SQL comment.
"""

import pytest
import textwrap
from types import SimpleNamespace

from metadata.rendering import load_sql
from metadata.rendering.load_sql import _get_hist_insert_columns
from metadata.rendering.dialects.base import SqlDialect
from tests._dialect_test_mixin import DialectTestMixin


class DummyTargetColumnInputQS:
  def __init__(self, items):
    self._items = items

  def select_related(self, *_args, **_kwargs):
    return self

  def order_by(self, *_args, **_kwargs):
    return self._items


def test_hist_insert_columns_do_not_duplicate_row_hash(monkeypatch):
  # Build dummy TD
  td = DummyHistTargetDataset(dataset_name="rc_aw_product_hist")
  dialect = DummyDialect()

  # Upstream columns returned by lineage include row_hash already
  upstream_cols = [
    SimpleNamespace(upstream_target_column=SimpleNamespace(target_column_name="productid", system_role="")),
    SimpleNamespace(upstream_target_column=SimpleNamespace(target_column_name="row_hash", system_role="")),
    SimpleNamespace(upstream_target_column=SimpleNamespace(target_column_name="name", system_role="")),
  ]

  # Monkeypatch TargetColumnInput.objects.filter(...) chain
  import metadata.rendering.load_sql as load_sql_mod

  dummy_manager = SimpleNamespace(
    filter=lambda **_kwargs: DummyTargetColumnInputQS(upstream_cols)
  )
  monkeypatch.setattr(load_sql_mod, "TargetColumnInput", SimpleNamespace(objects=dummy_manager))

  hist_cols, rawcore_cols = _get_hist_insert_columns(td, dialect)

  # Unquote identifiers for robust comparison
  def unq(x: str) -> str:
    x = x.strip()
    if x.startswith('"') and x.endswith('"') and len(x) >= 2:
      return x[1:-1]
    if x.startswith("[") and x.endswith("]") and len(x) >= 2:
      return x[1:-1]
    return x

  cols = [unq(c) for c in hist_cols]
  assert cols.count("row_hash") == 1


class DummyTargetSchema:
  def __init__(self, schema_name: str, short_name: str):
    self.schema_name = schema_name
    self.short_name = short_name


class DummyHistTargetDataset:
  def __init__(
    self,
    schema_name: str = "rawcore",
    schema_short_name: str = "rawcore",
    dataset_name: str = "rc_aw_product_hist",
  ):
    self.target_schema = DummyTargetSchema(
      schema_name=schema_name,
      short_name=schema_short_name,
    )
    self.target_dataset_name = dataset_name
    self.incremental_strategy = "historize"
    self.input_links = []

  @property
  def is_hist(self) -> bool:
    return (
      getattr(getattr(self, "target_schema", None), "short_name", None) == "rawcore"
      and getattr(self, "incremental_strategy", None) == "historize"
    )


class DummyDialect(DialectTestMixin):
  pass


def test_render_load_sql_for_hist_routes_to_hist_renderer(monkeypatch):
  """
  Ensure that *_hist datasets are routed to render_hist_incremental_sql
  and that the returned SQL is a descriptive comment, not a MERGE/FULL statement.
  """

  td = DummyHistTargetDataset()

  dialect = DummyDialect()

  sql = load_sql.render_load_sql_for_target(td, dialect)
  normalized = textwrap.dedent(sql).strip()

  # Basic expectations: comment, schema+table mentioned, SCD wording present.
  assert normalized.startswith("-- History load for rawcore.rc_aw_product_hist")
  assert "SCD Type 2" in normalized or "SCD Type 2" in normalized
  assert "row_hash" in normalized
  assert "version_started_at" in normalized
  assert "version_ended_at" in normalized
  assert "load_run_id" in normalized

def test_hist_sql_contains_changed_update_block():
  td = DummyHistTargetDataset()
  dialect = DummyDialect()

  sql = load_sql.render_hist_incremental_sql(td, dialect)

  # Changed-UPDATE-Block sollte enthalten sein
  assert "version_state    = 'changed'" in sql
  assert "row_hash <>" in sql
  assert "UPDATE rawcore.rc_aw_product_hist AS h" in sql

def test_hist_sql_contains_delete_update_block():
  """
  The history SQL should also contain the DELETE-marking UPDATE block
  that closes rows whose business key disappeared from Rawcore.
  """
  td = DummyHistTargetDataset()
  dialect = DummyDialect()

  sql = load_sql.render_hist_incremental_sql(td, dialect)

  # Delete-UPDATE block should be present
  assert "version_state    = 'deleted'" in sql
  assert "NOT EXISTS (" in sql
  assert "UPDATE rawcore.rc_aw_product_hist AS h" in sql

def test_render_hist_changed_insert_sql_uses_exists_on_changed_rows(monkeypatch):
  """
  render_hist_changed_insert_sql should insert new versions for those
  rows that were just closed as 'changed' in the history table.

  It must:
    - INSERT into the hist table,
    - SELECT from the corresponding rawcore table,
    - use EXISTS on hist with version_ended_at/load_timestamp, version_state='changed'
      and the rawcore surrogate key join.
  """
  from metadata.rendering import load_sql

  td = DummyHistTargetDataset()
  dialect = DummyDialect()

  # Stub hist/ rawcore column mapping to avoid ORM access
  hist_cols = [
    "rc_aw_product_hist_key",
    "attr1",
    "row_hash",
    "version_started_at",
    "version_ended_at",
    "version_state",
    "load_run_id",
  ]
  rawcore_cols = [
    "r.rc_aw_product_key",
    "r.attr1",
    "r.row_hash",
    "{{ load_timestamp }}",
    "NULL",
    "'changed'",
    "{{ load_run_id }}",
  ]

  monkeypatch.setattr(
    load_sql,
    "_get_hist_insert_columns",
    lambda _td, _dialect: (hist_cols, rawcore_cols),
  )

  sql = load_sql.render_hist_changed_insert_sql(td, dialect)

  # Target + source tables
  assert "INSERT INTO rawcore.rc_aw_product_hist" in sql
  assert "FROM rawcore.rc_aw_product AS r" in sql

  # Column list should be present
  assert "rc_aw_product_hist_key" in sql
  assert "attr1" in sql
  assert "row_hash" in sql
  assert "version_state" in sql

  # EXISTS predicate for changed rows
  assert "WHERE EXISTS (" in sql
  assert "FROM rawcore.rc_aw_product_hist AS h" in sql
  assert "h.version_ended_at = {{ load_timestamp }}" in sql
  assert "h.version_state = 'changed'" in sql
  assert "h.rc_aw_product_key = r.rc_aw_product_key" in sql

def test_render_hist_new_insert_sql_uses_not_exists_and_new_state(monkeypatch):
  """
  render_hist_new_insert_sql should insert 'new' history rows for business keys
  that have no history entry yet.

  It must:
    - INSERT into the hist table,
    - SELECT from rawcore,
    - use NOT EXISTS on hist with the surrogate key join,
    - override version_state to 'new' in the SELECT list.
  """
  from metadata.rendering import load_sql

  td = DummyHistTargetDataset()
  dialect = DummyDialect()

  # Same base columns as for changed insert; version_state is overridden to 'new'
  hist_cols = [
    "rc_aw_product_hist_key",
    "attr1",
    "row_hash",
    "version_started_at",
    "version_ended_at",
    "version_state",
    "load_run_id",
  ]
  rawcore_cols = [
    "r.rc_aw_product_key",
    "r.attr1",
    "r.row_hash",
    "{{ load_timestamp }}",
    "NULL",
    "'changed'",  # will be overridden to 'new' inside the function
    "{{ load_run_id }}",
  ]

  monkeypatch.setattr(
    load_sql,
    "_get_hist_insert_columns",
    lambda _td, _dialect: (hist_cols, rawcore_cols),
  )

  sql = load_sql.render_hist_new_insert_sql(td, dialect)

  # Target + source and column list present
  assert "INSERT INTO rawcore.rc_aw_product_hist" in sql
  assert "FROM rawcore.rc_aw_product AS r" in sql
  assert "rc_aw_product_hist_key" in sql
  assert "attr1" in sql

  # NOT EXISTS on the surrogate key
  assert "WHERE NOT EXISTS (" in sql
  assert "FROM rawcore.rc_aw_product_hist AS h" in sql
  assert "h.rc_aw_product_key = r.rc_aw_product_key" in sql

  # version_state should be 'new' in the SELECT (overridden)
  assert "'new'" in sql

def test_hist_incremental_sql_includes_insert_blocks_when_target_has_pk(monkeypatch):
  """
  When the history TargetDataset has a real primary key (id is an int),
  render_hist_incremental_sql should include INSERT blocks for
  changed and new business keys instead of the 'omitted' comment.
  """
  from metadata.rendering import load_sql

  td = DummyHistTargetDataset()
  # Simulate a real ORM-backed TargetDataset by adding an integer id
  td.id = 1

  dialect = DummyDialect()

  # Stub insert column mapping to avoid ORM access in _get_hist_insert_columns
  hist_cols = [
    "rc_aw_product_hist_key",
    "attr1",
    "row_hash",
    "version_started_at",
    "version_ended_at",
    "version_state",
    "load_run_id",
  ]
  rawcore_cols = [
    "r.rc_aw_product_key",
    "r.attr1",
    "r.row_hash",
    "{{ load_timestamp }}",
    "NULL",
    "'changed'",
    "{{ load_run_id }}",
  ]

  monkeypatch.setattr(
    load_sql,
    "_get_hist_insert_columns",
    lambda _td, _dialect: (hist_cols, rawcore_cols),
  )

  sql = load_sql.render_hist_incremental_sql(td, dialect)

  # The 'omitted' comment for dummy datasets must NOT be present anymore
  assert "INSERT statements for changed/new rows are omitted" not in sql

  # Both INSERT blocks should be present
  assert sql.count("INSERT INTO rawcore.rc_aw_product_hist") >= 2

  # We expect one EXISTS block (changed insert) and one NOT EXISTS (new insert)
  assert "WHERE EXISTS (" in sql
  assert "WHERE NOT EXISTS (" in sql

  # Sanity: still contains the changed and deleted UPDATE blocks
  assert "version_state    = 'changed'" in sql
  assert "version_state    = 'deleted'" in sql

def test_hist_default_renderer_uses_ansi_update_as_alias():
  """
  Guard: non-TSQL dialects should use the default base implementation:
  UPDATE <table> AS h ...
  """
  d = DummyDialect()
  td = DummyHistTargetDataset()
  sql = load_sql.render_hist_changed_update_sql(td=td, dialect=d)
  assert "UPDATE rawcore." in sql
  assert " AS h" in sql
  assert sql.startswith("UPDATE")
  