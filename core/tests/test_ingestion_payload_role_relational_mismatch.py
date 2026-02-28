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

from metadata.ingestion import native_raw
from core.tests._dialect_test_mixin import DialectTestMixin


class _QS:
  def __init__(self, items):
    self._items = list(items)

  def filter(self, **kwargs):
    return self

  def order_by(self, *args):
    return self

  def __iter__(self):
    return iter(self._items)


class DummySourceDialect:
  def render_identifier(self, name):
    return name

  def render_table_identifier(self, schema, table):
    return f"{schema}.{table}"


class DummyExecEngine:
  def execute(self, sql):
    return None

  def execute_many(self, sql, params):
    return None


def test_relational_ingest_does_not_count_payload_role_as_business(monkeypatch):
  # Patch functions that would require DB/network.
  monkeypatch.setattr(native_raw, "ensure_load_run_log_table", lambda **kwargs: None)
  monkeypatch.setattr(native_raw, "resolve_delta_cutoff_for_source_dataset", lambda **kwargs: None)
  fake_source_engine = SimpleNamespace(
    dialect=SimpleNamespace(name="mssql"),
    connect=lambda: (_ for _ in ()).throw(AssertionError("should not connect"))
  )
  monkeypatch.setattr(native_raw, "engine_for_source_system", lambda **kwargs: fake_source_engine)
  monkeypatch.setattr(native_raw, "get_active_dialect", lambda name: DummySourceDialect())

  # Source dataset with 1 integrated column.
  src_cols = [SimpleNamespace(source_column_name="a", integrate=True, ordinal_position=1)]
  source_dataset = SimpleNamespace(
    id=1,
    pk=1,
    source_columns=_QS(src_cols),
    source_system=SimpleNamespace(type="mssql", short_name="src"),
    schema_name="dbo",
    source_dataset_name="t",
    incremental=False,
    increment_filter=None,
  )

  # Target dataset columns:
  # - 1 business column ("a")
  # - payload column exists but is NOT named "payload" and must be treated as tech via system_role="payload"
  # - load_run_id/loaded_at are tech
  tgt_cols = [
    SimpleNamespace(target_column_name="raw_payload", system_role="payload", datatype="STRING", nullable=True, ordinal_position=1, active=True),
    SimpleNamespace(target_column_name="a", system_role="", datatype="STRING", nullable=True, ordinal_position=2, active=True),
    SimpleNamespace(target_column_name="load_run_id", system_role="load_run_id", datatype="STRING", nullable=True, ordinal_position=3, active=True),
    SimpleNamespace(target_column_name="loaded_at", system_role="loaded_at", datatype="TIMESTAMP", nullable=True, ordinal_position=4, active=True),
  ]
  
  td = SimpleNamespace(
    id=1,
    pk=1,
    target_schema=SimpleNamespace(schema_name="raw", short_name="raw"),
    target_dataset_name="raw_test",
    target_columns=_QS(tgt_cols),
  )

  # If payload is incorrectly counted as business, ingest_raw_relational would raise mismatch *before* connecting.
  # We intentionally assert that it does NOT raise that mismatch here.
  dialect = DialectTestMixin(engine=DummyExecEngine())

  # The function will attempt to connect later; we patched connect to raise AssertionError.
  # So we only assert the mismatch does not happen (i.e., failure reason must be the forced AssertionError).
  with pytest.raises(AssertionError, match="should not connect"):
    native_raw.ingest_raw_relational(
      source_dataset=source_dataset,
      td=td,
      target_system=SimpleNamespace(short_name="duckdb", type="duckdb"),
      dialect=dialect,
      profile=SimpleNamespace(name="dev"),
      batch_run_id="batch-1",
      load_run_id="load-1",
      chunk_size=1000,
    )