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
===============================================================================
Merge SQL Tests – Scope & Guarantees
===============================================================================

This module tests the *contracted* behavior:

- load_sql.render_merge_sql() must only assemble semantic merge ingredients:
    * source SELECT SQL
    * key columns
    * update columns
    * insert columns
  and then delegate the SQL shape to dialect.render_merge_statement().

- Dialect merge rendering (native MERGE or fallback) is tested separately via
  dialect-specific tests and the matrix smoke test.

===============================================================================
"""

import textwrap

import pytest

from types import SimpleNamespace
from metadata.rendering.load_sql import render_merge_sql
from metadata.rendering.dialects.duckdb import DuckDBDialect
from tests._dialect_test_mixin import DialectTestMixin


class FakeTargetSchema:
  def __init__(self, schema_name: str, short_name: str):
    self.schema_name = schema_name
    self.short_name = short_name


class FakeTargetColumn:
  def __init__(self, name, **kwargs):
    self.target_column_name = name
    # ordinal_position is only used for ordering; we stub it here
    self.ordinal_position = 1
    self.datatype = kwargs.get("datatype", "STRING")
    self.max_length = kwargs.get("max_length")
    self.decimal_precision = kwargs.get("decimal_precision")
    self.decimal_scale = kwargs.get("decimal_scale")


class FakeTargetDataset:
  def __init__(
    self,
    dataset_id: int = 1,
    schema_name: str = "rawcore",
    schema_short_name: str = "rawcore",
    dataset_name: str = "rc_customer",
    natural_key_fields=None,
  ):
    self.id = dataset_id
    self.target_schema = FakeTargetSchema(
      schema_name=schema_name,
      short_name=schema_short_name,
    )
    self.target_dataset_name = dataset_name

    # IMPORTANT: do not use `or [...]` here,
    # otherwise [] turns back into the default.
    if natural_key_fields is None:
      self.natural_key_fields = ["customer_id"]
    else:
      self.natural_key_fields = natural_key_fields


class DummyDialectNoMerge(DialectTestMixin):
  supports_merge = False
  supports_delete_detection = False
  pass


def test_render_merge_sql_basic_happy_path(monkeypatch):
  """
  load_sql must:
    - compute insert/update column lists from target columns + natural keys
    - obtain source_select_sql via render_select_for_target()
    - delegate SQL shape to dialect.render_merge_statement()
  """
  from metadata.rendering import load_sql

  td = FakeTargetDataset()
  dialect = DuckDBDialect()

  monkeypatch.setattr(
    load_sql,
    "_get_target_columns_in_order",
    lambda _td: [
      FakeTargetColumn("customer_id"),
      FakeTargetColumn("name"),
      FakeTargetColumn("city"),
    ],
  )

  # Provide upstream stage dataset
  stage_schema = SimpleNamespace(schema_name="stage", short_name="stage")
  stage_td = SimpleNamespace(target_schema=stage_schema, target_dataset_name="stg_customer")
  monkeypatch.setattr(load_sql, "_find_stage_upstream_for_rawcore", lambda _td: stage_td)

  # Provide upstream stage dataset to avoid accessing td.input_links in real resolver
  stage_schema = SimpleNamespace(schema_name="stage", short_name="stage")
  stage_td = SimpleNamespace(target_schema=stage_schema, target_dataset_name="stg_customer")
  monkeypatch.setattr(load_sql, "_find_stage_upstream_for_rawcore", lambda _td: stage_td)

  # Provide expression map for merge source select
  monkeypatch.setattr(
    load_sql,
    "_get_rendered_column_exprs_for_target",
    lambda _td, _dialect: {
      "customer_id": "s.customer_id",
      "name": "s.name",
      "city": "s.city",
    },
  )

  # Act
  sql = render_merge_sql(td, dialect)

  normalized = textwrap.dedent(sql).strip()

  # Assert basic structure
  assert normalized.startswith("MERGE INTO")
  assert "MERGE INTO" in normalized
  assert "USING" in normalized
  assert "WHEN MATCHED THEN" in normalized
  assert "WHEN NOT MATCHED THEN" in normalized

  # Target table should appear
  assert "rawcore" in normalized
  assert "rc_customer" in normalized

  # Source SELECT should be embedded (shape produced from expr_map)
  assert "SELECT" in normalized
  assert "s.customer_id AS customer_id" in normalized
  assert "FROM" in normalized
  assert "stage.stg_customer AS s" in normalized

  # ON clause should join on key
  assert (
    't."customer_id" = s."customer_id"' in sql
    or "t.customer_id = s.customer_id" in sql
  )

  # UPDATE should assign non-key columns
  assert ('"name" = s."name"' in sql) or ("name = s.name" in sql)
  assert ('"city" = s."city"' in sql) or ("city = s.city" in sql)

  # INSERT should include all columns
  assert "INSERT" in normalized
  assert ('"customer_id"' in normalized) or ("customer_id" in normalized)
  assert ('"name"' in normalized) or ("name" in normalized)
  assert ('"city"' in normalized) or ("city" in normalized)


def test_render_merge_sql_raises_for_non_merge_mode():
  """
  If a dataset has an incremental_strategy other than 'merge',
  render_merge_sql should raise a ValueError.
  """
  td = FakeTargetDataset()
  td.incremental_strategy = "full"  # explicitly not 'merge'
  dialect = DuckDBDialect()

  with pytest.raises(ValueError) as excinfo:
    render_merge_sql(td, dialect)

  assert "non-merge dataset" in str(excinfo.value)


def test_render_merge_sql_raises_for_non_rawcore_schema(monkeypatch):
  """
  Merge is currently only supported for rawcore targets.
  """
  td = FakeTargetDataset(schema_short_name="stage")
  dialect = DuckDBDialect()

  with pytest.raises(ValueError) as excinfo:
    render_merge_sql(td, dialect)

  assert "only supported for rawcore targets" in str(excinfo.value)

def test_render_merge_sql_raises_if_no_natural_key_fields(monkeypatch):
  """
  If natural_key_fields is empty, merge must fail with a clear error.
  """

  td = FakeTargetDataset()
  td.natural_key_fields = []  # force empty list
  dialect = DuckDBDialect()

  with pytest.raises(ValueError) as excinfo:
    render_merge_sql(td, dialect)

  assert "no natural_key_fields defined" in str(excinfo.value)


def test_base_merge_fallback_update_and_insert_shape():
  """
  When supports_merge=False, the base dialect implementation must still provide
  merge semantics via UPDATE + INSERT ... WHERE NOT EXISTS.
  """
  d = DummyDialectNoMerge()

  sql = d.render_merge_statement(
    target_fqn=d.render_table_identifier("rawcore", "rc_customer"),
    source_select_sql="SELECT 1 AS customer_id, 'a' AS name",
    key_columns=["customer_id"],
    update_columns=["name"],
    insert_columns=["customer_id", "name"],
  )

  low = sql.lower()
  assert "update" in low
  assert "insert into" in low
  assert "where not exists" in low

def test_render_merge_sql_fallback_uses_all_key_columns_in_not_exists(monkeypatch):
  """
  For multi-column natural keys, the fallback's NOT EXISTS predicate must join
  on all key columns.
  """
  from metadata.rendering import load_sql

  td = FakeTargetDataset(natural_key_fields=["customer_id", "partner_id"])
  dialect = DummyDialectNoMerge()

  # Provide upstream stage dataset to avoid accessing td.input_links in real resolver
  stage_schema = SimpleNamespace(schema_name="stage", short_name="stage")
  stage_td = SimpleNamespace(target_schema=stage_schema, target_dataset_name="stg_customer")
  monkeypatch.setattr(load_sql, "_find_stage_upstream_for_rawcore", lambda _td: stage_td)

  monkeypatch.setattr(
    load_sql,
    "_get_target_columns_in_order",
    lambda _td: [
      FakeTargetColumn("customer_id"),
      FakeTargetColumn("partner_id"),
      FakeTargetColumn("attr1"),
    ],
  )
  monkeypatch.setattr(
    load_sql,
    "_get_rendered_column_exprs_for_target",
    lambda _td, _dialect: {
      "customer_id": "s.customer_id",
      "partner_id": "s.partner_id",
      "attr1": "s.attr1",
    },
  )

  sql = render_merge_sql(td, dialect)
  assert "WHERE NOT EXISTS" in sql
  assert "t.customer_id = s.customer_id" in sql
  assert "t.partner_id = s.partner_id" in sql
