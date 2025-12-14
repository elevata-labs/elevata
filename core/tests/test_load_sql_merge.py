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

"""
===============================================================================
Merge SQL Tests – Scope & Guarantees
===============================================================================

This test module verifies the SQL generation for merge-based incremental loads
(Stage → Rawcore). The implementation supports two execution paths:

1) Native MERGE path
   Used when the dialect exposes `supports_merge = True`.
   The SQL generator must emit:
       MERGE INTO rawcore.<table> AS t
       USING stage.<table> AS s
       ON <natural key>
       WHEN MATCHED THEN UPDATE ...
       WHEN NOT MATCHED THEN INSERT ...
   These tests assert the structure, quoting, column expressions, and
   correctness of the join condition.

2) Fallback path (UPDATE + INSERT)
   Used when `supports_merge = False`.
   The SQL generator must emulate merge semantics using:
       UPDATE rawcore.<table> AS t
         SET ...
         FROM stage.<table> AS s
         WHERE <key join>;
       INSERT INTO rawcore.<table> (...)
         SELECT <expr list>
         FROM stage.<table> AS s
         WHERE NOT EXISTS (
             SELECT 1
             FROM rawcore.<table> AS t
             WHERE <full natural key join>
         );
   These tests verify that:
       - UPDATE and INSERT statements are both rendered.
       - Column expressions come from the logical plan.
       - All natural key fields appear in the NOT EXISTS join.
       - The fallback path is fully consistent with the conceptual
         merge semantics documented in `incremental_load.md`.

Out of scope:
- Historization logic (handled separately in the history SQL tests).
- Delete detection SQL (tested in its own module).

Together, these tests guarantee that both merge implementations
(native MERGE and fallback) follow identical semantics regarding:
    - new-row detection,
    - existing-row updates,
    - reliance on natural keys,
    - and consistent use of logical-plan expressions.

===============================================================================
"""

import textwrap

import pytest

from types import SimpleNamespace

from metadata.rendering.load_planner import LoadPlan
from metadata.rendering import load_sql
from metadata.rendering.load_sql import (
  render_merge_sql,
  render_load_sql_for_target,
)
from metadata.rendering.dialects.duckdb import DuckDBDialect


class FakeTargetSchema:
  def __init__(self, schema_name: str, short_name: str):
    self.schema_name = schema_name
    self.short_name = short_name


class FakeTargetColumn:
  def __init__(self, name: str):
    self.target_column_name = name
    # ordinal_position is only used for ordering; we stub it here
    self.ordinal_position = 1


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


class FakeStageTargetDataset(FakeTargetDataset):
  """
  Upstream stage dataset used as merge source.
  """
  def __init__(self):
    super().__init__(
      dataset_id=2,
      schema_name="stage",
      schema_short_name="stage",
      dataset_name="stg_customer",
      natural_key_fields=["customer_id"],
    )


class DummyNoMergeDialect:
  """
  Minimal dialect that forces the UPDATE + INSERT fallback
  by disabling native MERGE support.
  """

  supports_merge = False

  def render_identifier(self, name: str) -> str:
    # No quoting, keep tests simple and predictable
    return name

  def render_table_identifier(self, schema: str | None, name: str) -> str:
    if schema:
      return f"{schema}.{name}"
    return name


def test_render_merge_sql_basic_happy_path(monkeypatch):
  """
  Basic happy-path test:

  - LoadPlan.mode == 'merge'
  - target schema = rawcore
  - upstream stage dataset is resolved
  - natural_key_fields and non-key columns are present
  """
  from metadata.rendering import load_sql

  td = FakeTargetDataset()
  stage_td = FakeStageTargetDataset()
  dialect = DuckDBDialect()

  # 1) _find_stage_upstream_for_rawcore → return our fake stage dataset
  def fake_find_stage_upstream_for_rawcore(target_dataset):
    assert target_dataset is td
    return stage_td

  monkeypatch.setattr(load_sql, "_find_stage_upstream_for_rawcore", fake_find_stage_upstream_for_rawcore)

  # 2) _get_target_columns_in_order → define key + non-key columns
  def fake_get_target_columns_in_order(target_dataset):
    assert target_dataset is td
    return [
      FakeTargetColumn("customer_id"),
      FakeTargetColumn("name"),
      FakeTargetColumn("city"),
    ]

  monkeypatch.setattr(load_sql, "_get_target_columns_in_order", fake_get_target_columns_in_order)

  # 3) _get_rendered_column_exprs_for_target → expressions for UPDATE/INSERT
  def fake_get_rendered_column_exprs_for_target(target_dataset, dialect_):
    assert target_dataset is td
    assert isinstance(dialect_, DuckDBDialect)
    return {
      "customer_id": 's."customer_id"',
      "name": 's."name"',
      "city": 's."city"',
    }

  monkeypatch.setattr(
    load_sql,
    "_get_rendered_column_exprs_for_target",
    fake_get_rendered_column_exprs_for_target,
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

  # Target and source tables (with quoting)
  assert "rawcore" in normalized
  assert "rc_customer" in normalized
  assert "stage" in normalized
  assert "stg_customer" in normalized

  # Aliases t and s
  assert "MERGE INTO" in normalized and "AS t" in normalized
  assert "USING" in normalized and "AS s" in normalized

  # ON clause with natural key
  # Left side now uses the smarter render_identifier() and no longer quotes
  assert "t.customer_id = s." in normalized
  assert 's."customer_id"' in normalized

  # UPDATE assigns non-key columns from source
  assert 'UPDATE SET' in normalized
  assert 'name = s."name"' in normalized
  assert 'city = s."city"' in normalized

  # INSERT includes all columns
  assert "INSERT (" in normalized
  assert '"customer_id"' in normalized
  assert '"name"' in normalized
  assert '"city"' in normalized
  assert "VALUES (" in normalized
  assert 's."customer_id"' in normalized
  assert 's."name"' in normalized
  assert 's."city"' in normalized


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
  from metadata.rendering import load_sql

  td = FakeTargetDataset(schema_short_name="stage")
  dialect = DuckDBDialect()

  def fake_build_load_plan(target_dataset):
    return LoadPlan(mode="merge", handle_deletes=False, historize=False)

  monkeypatch.setattr(load_sql, "build_load_plan", fake_build_load_plan)

  with pytest.raises(ValueError) as excinfo:
    render_merge_sql(td, dialect)

  assert "only supported for rawcore targets" in str(excinfo.value)

def test_render_merge_sql_raises_if_no_natural_key_fields(monkeypatch):
  """
  If natural_key_fields is empty, merge must fail with a clear error.
  """
  from metadata.rendering import load_sql

  td = FakeTargetDataset()
  td.natural_key_fields = []  # force empty list
  dialect = DuckDBDialect()

  def fake_build_load_plan(target_dataset):
    return LoadPlan(mode="merge", handle_deletes=False, historize=False)

  monkeypatch.setattr(load_sql, "build_load_plan", fake_build_load_plan)

  # We also need a fake stage dataset to get past upstream resolution
  stage_td = FakeStageTargetDataset()

  def fake_find_stage_upstream_for_rawcore(target_dataset):
    return stage_td

  monkeypatch.setattr(load_sql, "_find_stage_upstream_for_rawcore", fake_find_stage_upstream_for_rawcore)

  with pytest.raises(ValueError) as excinfo:
    render_merge_sql(td, dialect)

  assert "no natural_key_fields defined" in str(excinfo.value)


def test_render_load_sql_for_target_merge_includes_delete_and_merge(monkeypatch):
  """
  For merge load mode, render_load_sql_for_target should prefix the MERGE
  statement with delete-detection SQL when delete detection is active.
  """
  from metadata.rendering import load_sql

  td = FakeTargetDataset()
  dialect = DuckDBDialect()

  # Plan: mode=merge, handle_deletes=True (logic is inside delete renderer)
  plan = SimpleNamespace(mode="merge", handle_deletes=True)
  monkeypatch.setattr(load_sql, "build_load_plan", lambda _td: plan)

  # Stub both renderers to focus purely on routing behavior
  monkeypatch.setattr(
    load_sql,
    "render_delete_missing_rows_sql",
    lambda _td, _dialect: "-- DELETE MISSING ROWS",
  )
  monkeypatch.setattr(
    load_sql,
    "render_merge_sql",
    lambda _td, _dialect: "-- MERGE STATEMENT",
  )

  sql = render_load_sql_for_target(td, dialect)

  # Delete should come first, then a blank line, then MERGE
  assert sql.startswith("-- DELETE MISSING ROWS")
  assert "\n\n-- MERGE STATEMENT" in sql

def test_render_load_sql_for_target_merge_without_delete(monkeypatch):
  """
  When delete detection does not yield any SQL, render_load_sql_for_target
  should return only the MERGE statement.
  """
  from metadata.rendering import load_sql

  td = FakeTargetDataset()
  dialect = DuckDBDialect()

  plan = SimpleNamespace(mode="merge", handle_deletes=False)
  monkeypatch.setattr(load_sql, "build_load_plan", lambda _td: plan)

  # No delete SQL generated
  monkeypatch.setattr(
    load_sql,
    "render_delete_missing_rows_sql",
    lambda _td, _dialect: None,
  )
  monkeypatch.setattr(
    load_sql,
    "render_merge_sql",
    lambda _td, _dialect: "-- MERGE ONLY",
  )

  sql = render_load_sql_for_target(td, dialect)

  assert sql.strip() == "-- MERGE ONLY"


def test_render_merge_sql_fallback_update_and_insert(monkeypatch):
  """
  When the dialect does not support MERGE, render_merge_sql should
  fall back to an UPDATE followed by an INSERT .. SELECT .. WHERE NOT EXISTS
  that uses the natural key to detect new rows.
  """

  td = FakeTargetDataset()
  stage_td = FakeStageTargetDataset()

  # Force resolution of the stage upstream dataset
  def fake_find_stage_upstream_for_rawcore(target_dataset):
    assert target_dataset is td
    return stage_td

  monkeypatch.setattr(
    load_sql,
    "_find_stage_upstream_for_rawcore",
    fake_find_stage_upstream_for_rawcore,
  )

  # Define the target columns in a deterministic order:
  # natural key first, then non-key attributes.
  def fake_get_target_columns_in_order(target_dataset):
    assert target_dataset is td
    return [
      FakeTargetColumn("customer_id"),
      FakeTargetColumn("name"),
      FakeTargetColumn("city"),
    ]

  monkeypatch.setattr(
    load_sql,
    "_get_target_columns_in_order",
    fake_get_target_columns_in_order,
  )

  # Column expressions as they would come from the logical plan
  def fake_get_rendered_column_exprs_for_target(target_dataset, dialect_):
    assert target_dataset is td
    # We don't rely on dialect-specific quoting here
    assert isinstance(dialect_, DummyNoMergeDialect)
    return {
      "customer_id": "s.customer_id",
      "name": "UPPER(s.name)",
      "city": "s.city",
    }

  monkeypatch.setattr(
    load_sql,
    "_get_rendered_column_exprs_for_target",
    fake_get_rendered_column_exprs_for_target,
  )

  dialect = DummyNoMergeDialect()

  # Act
  sql = render_merge_sql(td, dialect)
  normalized = textwrap.dedent(sql)

  # Assert: fallback must not use MERGE
  assert "MERGE INTO" not in normalized

  # UPDATE-part must reference target and source tables
  assert "UPDATE rawcore.rc_customer AS t" in normalized
  assert "FROM stage.stg_customer AS s" in normalized

  # INSERT-part must insert into the Rawcore table
  assert "INSERT INTO rawcore.rc_customer" in normalized

  # INSERT must select all target columns in the correct order,
  # using the expressions supplied by the logical plan.
  assert "SELECT s.customer_id, UPPER(s.name), s.city" in normalized

  # New rows must be detected via NOT EXISTS on the natural key
  assert "WHERE NOT EXISTS (" in normalized
  assert "t.customer_id = s.customer_id" in normalized
  

def test_render_merge_sql_fallback_uses_all_key_columns_in_not_exists(monkeypatch):
  """
  For multi-column natural keys, the NOT EXISTS predicate in the fallback
  INSERT must join on all key columns so that new rows are detected based
  on the full business key.
  """
  from metadata.rendering import load_sql

  # Multi-column natural key
  td = FakeTargetDataset(natural_key_fields=["customer_id", "partner_id"])
  stage_td = FakeStageTargetDataset()

  def fake_find_stage_upstream_for_rawcore(target_dataset):
    return stage_td

  monkeypatch.setattr(
    load_sql,
    "_find_stage_upstream_for_rawcore",
    fake_find_stage_upstream_for_rawcore,
  )

  # Target columns: both key columns + one non-key attribute
  def fake_get_target_columns_in_order(target_dataset):
    return [
      FakeTargetColumn("customer_id"),
      FakeTargetColumn("partner_id"),
      FakeTargetColumn("attr1"),
    ]

  monkeypatch.setattr(
    load_sql,
    "_get_target_columns_in_order",
    fake_get_target_columns_in_order,
  )

  # Expressions taken from the logical plan
  def fake_get_rendered_column_exprs_for_target(target_dataset, dialect_):
    return {
      "customer_id": "s.customer_id",
      "partner_id": "s.partner_id",
      "attr1": "s.attr1",
    }

  monkeypatch.setattr(
    load_sql,
    "_get_rendered_column_exprs_for_target",
    fake_get_rendered_column_exprs_for_target,
  )

  dialect = DummyNoMergeDialect()

  sql = render_merge_sql(td, dialect)

  # We only care about the NOT EXISTS join here
  assert "WHERE NOT EXISTS (" in sql
  assert "t.customer_id = s.customer_id" in sql
  assert "t.partner_id = s.partner_id" in sql
