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

from metadata.rendering.load_sql import render_delete_missing_rows_sql
from metadata.rendering.dialects.duckdb import DuckDBDialect


class DummyDialectNoDelete:
  """Minimal dialect stub without delete detection capability."""
  supports_delete_detection = False


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
