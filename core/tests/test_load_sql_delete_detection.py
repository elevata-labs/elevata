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

from metadata.rendering.load_sql import render_delete_missing_rows_sql
from metadata.rendering.dialects.duckdb import DuckDBDialect
from tests._dialect_test_mixin import DialectTestMixin


class DummyDialectNoDelete(DialectTestMixin):
  supports_delete_detection = False
  pass


class FakeTargetSchema:
  def __init__(self, schema_name: str, short_name: str):
    self.schema_name = schema_name
    self.short_name = short_name

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
    self.target_schema = FakeTargetSchema(schema_name, schema_short_name)
    self.target_dataset_name = dataset_name
    self.natural_key_fields = natural_key_fields or []
    # keep minimal attributes that render_delete_missing_rows_sql expects
    self.incremental_source = None

    class FakeRelatedManager:
      def select_related(self, *args, **kwargs):
        return self
      def filter(self, *args, **kwargs):
        return self
      def first(self):
        return None

    self.input_links = FakeRelatedManager()


def test_render_delete_missing_rows_sql_raises_if_dialect_has_no_delete_detection():
  """Dialects that opt out of delete detection must raise NotImplementedError."""
  td = FakeTargetDataset()
  dialect = DummyDialectNoDelete()

  with pytest.raises(NotImplementedError) as excinfo:
    render_delete_missing_rows_sql(td, dialect)

  msg = str(excinfo.value)
  assert "does not implement delete detection" in msg
  assert "supports_delete_detection=True" in msg


def test_render_delete_missing_rows_sql_returns_none_for_non_merge_mode(monkeypatch):
  """
  If the LoadPlan is not in merge mode or does not handle deletes,
  render_delete_missing_rows_sql should return None.
  """
  from metadata.rendering import load_sql

  td = FakeTargetDataset()
  dialect = DuckDBDialect()

  # Simulate a full-load plan without delete handling
  plan = SimpleNamespace(mode="full", handle_deletes=False)
  monkeypatch.setattr(load_sql, "build_load_plan", lambda _td: plan)

  result = render_delete_missing_rows_sql(td, dialect)
  assert result is None


def test_render_delete_missing_rows_sql_returns_comment_for_non_rawcore(monkeypatch):
  """
  For non-rawcore targets, delete detection is not active and a diagnostic
  comment SQL should be returned instead of DELETE statements.
  """
  from metadata.rendering import load_sql

  td = FakeTargetDataset(schema_short_name="stage", schema_name="stage")
  dialect = DuckDBDialect()

  # Merge mode with handle_deletes=True so that the function reaches
  # the rawcore guard.
  plan = SimpleNamespace(mode="merge", handle_deletes=True)
  monkeypatch.setattr(load_sql, "build_load_plan", lambda _td: plan)

  sql = render_delete_missing_rows_sql(td, dialect)

  assert "handle_deletes=True" in sql
  assert "only implemented for rawcore" in sql
  assert "No delete detection SQL generated" in sql


def test_render_delete_missing_rows_sql_returns_comment_if_no_incremental_source(monkeypatch):
  """
  If handle_deletes is active but incremental_source is not set,
  the function should return a diagnostic comment instead of DELETE SQL.
  """
  from metadata.rendering import load_sql

  td = FakeTargetDataset()
  dialect = DuckDBDialect()

  # Merge mode + handle_deletes=True so we hit the inner guards
  plan = SimpleNamespace(mode="merge", handle_deletes=True)
  monkeypatch.setattr(load_sql, "build_load_plan", lambda _td: plan)

  # Simulate a valid scope filter so we do not exit earlier
  monkeypatch.setattr(
    load_sql,
    "_build_incremental_scope_filter_for_target",
    lambda _td, **_kwargs:  "1 = 1",
  )

  # incremental_source stays None (default in FakeTargetDataset)

  sql = render_delete_missing_rows_sql(td, dialect)

  assert "incremental_source is not set" in sql
  assert "No delete detection SQL generated" in sql


class DummyDialectWithDelete(DialectTestMixin):
  pass


def test_render_delete_missing_rows_sql_happy_path_calls_dialect(monkeypatch):
  """
  Happy-path integration test: when all preconditions are met,
  render_delete_missing_rows_sql should delegate to the dialect and
  return its SQL string.
  """
  from metadata.rendering import load_sql

  td = FakeTargetDataset(
    schema_name="dw_rawcore",
    schema_short_name="rawcore",
    dataset_name="rc_customer",
    natural_key_fields=["customer_id"],
  )
  dialect = DummyDialectWithDelete()

  monkeypatch.setattr(
    load_sql,
    "_get_rendered_column_exprs_for_target",
    lambda _td, _dialect: {"customer_id": "s.customer_id"},
  )

  class StageSchema:
    def __init__(self):
      self.schema_name = "stage"
      self.short_name = "stage"

  class StageTD:
    def __init__(self):
      self.target_schema = StageSchema()
      self.target_dataset_name = "stg_customer"

  stage_td = StageTD()
  td.incremental_source = stage_td

  # Merge mode with delete handling enabled
  plan = SimpleNamespace(mode="merge", handle_deletes=True)
  monkeypatch.setattr(load_sql, "build_load_plan", lambda _td: plan)

  # Minimal scope filter
  monkeypatch.setattr(
    load_sql,
    "_build_incremental_scope_filter_for_target",
    lambda _td, **_kwargs:  "(t.load_ts > {{DELTA_CUTOFF}})",
  )

  # Stage upstream lookup
  monkeypatch.setattr(
    load_sql,
    "_find_stage_upstream_for_rawcore",
    lambda _td: stage_td,
  )

  sql = render_delete_missing_rows_sql(td, dialect)

  # The SQL comes directly from DummyDialectWithDelete
  assert "-- dummy delete detection sql" in sql
  assert len(dialect.calls) == 1

  call = dialect.calls[0]
  assert call["target_schema"] == "dw_rawcore"
  assert call["target_table"] == "rc_customer"
  assert call["stage_schema"] == "stage"
  assert call["stage_table"] == "stg_customer"
  assert "(t.load_ts > {{DELTA_CUTOFF}})" in call["scope_filter"]
  # One join predicate for our single natural key
  assert call["join_predicates"] == ["t.customer_id = s.customer_id"]
