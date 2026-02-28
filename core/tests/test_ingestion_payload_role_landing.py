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

from metadata.ingestion import landing
from core.tests._dialect_test_mixin import DialectTestMixin


class _QS:
  def __init__(self, items):
    self._items = list(items)

  def filter(self, **kwargs):
    return self

  def order_by(self, *args):
    return self

  def all(self):
    return list(self._items)

  def __iter__(self):
    return iter(self._items)


class DummyExecEngine:
  def __init__(self):
    self.executed = []
    self.executed_many = []

  def execute(self, sql):
    self.executed.append(sql)

  def execute_many(self, sql, params):
    self.executed_many.append((sql, params))


def test_land_raw_accepts_payload_via_system_role(monkeypatch):
  # Patch external helpers to keep this test purely unit-level.
  monkeypatch.setattr(landing, "ensure_load_run_log_table", lambda **kwargs: None)
  monkeypatch.setattr(landing, "build_load_run_log_row", lambda **kwargs: {})
  monkeypatch.setattr(landing, "extract_json_path", lambda obj, path: obj.get(path.lstrip("$.")))

  # Source columns define business fields we expect to land.
  src_cols = [
    SimpleNamespace(source_column_name="order_id", integrate=True, ordinal_position=1, json_path="$.order_id"),
    SimpleNamespace(source_column_name="amount", integrate=True, ordinal_position=2, json_path="$.amount"),
  ]
  source_dataset = SimpleNamespace(
    source_columns=_QS(src_cols),
    source_system=SimpleNamespace(short_name="csv"),
    source_dataset_name="orders",
  )

  # Target columns:
  # - payload exists but is named differently, identified by system_role="payload"
  # - load_run_id/loaded_at exist as usual
  tgt_cols = [
    SimpleNamespace(target_column_name="payload", system_role="payload", datatype="STRING", nullable=True, ordinal_position=1),
    SimpleNamespace(target_column_name="order_id", system_role="", datatype="INTEGER", nullable=True, ordinal_position=2),
    SimpleNamespace(target_column_name="amount", system_role="", datatype="STRING", nullable=True, ordinal_position=3),
    SimpleNamespace(target_column_name="load_run_id", system_role="load_run_id", datatype="STRING", nullable=True, ordinal_position=4),
    SimpleNamespace(target_column_name="loaded_at", system_role="loaded_at", datatype="TIMESTAMP", nullable=True, ordinal_position=5),
  ]

  td = SimpleNamespace(
    target_schema=SimpleNamespace(schema_name="raw", short_name="raw"),
    target_dataset_name="raw_csv_orders",
    target_columns=_QS(tgt_cols),
  )

  engine = DummyExecEngine()
  dialect = DialectTestMixin(engine=engine)

  records = [
    {"order_id": 1, "amount": "10.0"},
    {"order_id": 2, "amount": "20.0"},
  ]

  # This must not raise. Previously, name-only checks would require target_column_name == "payload".
  res = landing.land_raw_json_records(
    target_engine=engine,
    target_dialect=dialect,
    td=td,
    records=records,
    batch_run_id="batch-1",
    load_run_id="load-1",
    target_system=SimpleNamespace(short_name="duckdb", type="duckdb"),
    profile=SimpleNamespace(name="dev"),
    meta_schema="meta",
    source_system_short_name="csv",
    source_dataset_name="orders",
    source_object="file:///tmp/orders.csv",
    ingest_mode="csv",
    chunk_size=1000,
    source_dataset=source_dataset,
    strict=False,
    rebuild=True,
    write_run_log=False,
  )

  assert res["rows_inserted"] == 2
  assert len(engine.executed_many) == 1