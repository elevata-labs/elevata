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

import json
from types import SimpleNamespace

import pytest

from metadata.ingestion.landing import land_raw_json_records
from core.tests._dialect_test_mixin import DialectTestMixin


class _FakeEngine:
  def __init__(self):
    self.executed = []
    self.executed_many = []

  def begin(self):
    # Minimal context manager to satisfy landing.py schema/table setup calls.
    return self

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc, tb):
    return False

  def execute(self, sql):
    self.executed.append(sql)

  def execute_many(self, sql, rows):
    # Capture the rendered insert SQL and the inserted row tuples.
    self.executed_many.append((sql, list(rows)))


def _mk_td():
  # Minimal TargetDataset stub used by landing.py
  schema = SimpleNamespace(short_name="raw", schema_name="raw")

  # Target columns: 2 business + payload + tech columns
  cols = [
    SimpleNamespace(target_column_name="brand", system_role=""),
    SimpleNamespace(target_column_name="availability", system_role=""),
    SimpleNamespace(target_column_name="payload", system_role="payload"),
    SimpleNamespace(target_column_name="load_run_id", system_role="load_run_id"),
    SimpleNamespace(target_column_name="loaded_at", system_role="loaded_at"),
  ]

  class _ColMgr:
    def all(self):
      return cols

  return SimpleNamespace(
    id=123,
    target_schema=schema,
    target_dataset_name="raw_csv_products",
    target_columns=_ColMgr(),
  )


def _mk_source_dataset():
  # Minimal SourceDataset stub with integrated SourceColumns including json_path
  src_cols = [
    SimpleNamespace(source_column_name="brand", integrate=True, ordinal_position=1, json_path="$.brand"),
    SimpleNamespace(source_column_name="availability", integrate=True, ordinal_position=2, json_path="$.availability"),
  ]

  class _QS:
    def __init__(self, items):
      self._items = items

    def filter(self, **kwargs):
      # Only integrate=True is used by landing.py in this path
      if "integrate" in kwargs:
        items = [c for c in self._items if getattr(c, "integrate", False) == kwargs["integrate"]]
        return _QS(items)
      return self

    def order_by(self, *_args):
      return sorted(self._items, key=lambda c: getattr(c, "ordinal_position", 0))

  return SimpleNamespace(source_columns=_QS(src_cols))


def test_land_raw_uses_payload_from___payload___but_flattens_from_normalized_keys():
  engine = _FakeEngine()
  dialect = DialectTestMixin(engine=engine)

  td = _mk_td()
  source_dataset = _mk_source_dataset()

  # Record uses normalized keys for flattening but keeps original headers in __payload__.
  records = [
    {
      "brand": "Garner, Boyle and Flynn",
      "availability": "pre_order",
      "__payload__": {
        "Brand": "Garner, Boyle and Flynn",
        "Availability": "pre_order",
        "Internal ID": 56,
      },
    }
  ]

  res = land_raw_json_records(
    target_engine=engine,
    target_dialect=dialect,
    td=td,
    records=records,
    batch_run_id="b1",
    load_run_id="lr1",
    target_system=SimpleNamespace(short_name="duckdb", type="duckdb"),
    profile=SimpleNamespace(name="test"),
    source_system_short_name="csv",
    source_dataset_name="products",
    source_object="https://example.invalid/products.csv",
    ingest_mode="csv",
    source_dataset=source_dataset,
    strict=True,
    chunk_size=10_000,
    rebuild=False,   # Avoid schema/table provisioning noise in this unit test
    write_run_log=False,
  )

  assert res["rows_inserted"] == 1
  assert engine.executed_many, "Expected a batched insert via execute_many()."

  insert_sql, rows = engine.executed_many[-1]
  assert len(rows) == 1

  row = rows[0]

  # Insert order in landing.py is: business cols first, then tech cols.
  # Here: [brand, availability, payload, load_run_id, loaded_at]
  assert row[0] == "Garner, Boyle and Flynn"
  assert row[1] == "pre_order"

  payload_json = row[2]
  payload_obj = json.loads(payload_json)

  # Payload must be the ORIGINAL object (not normalized keys).
  assert "Brand" in payload_obj
  assert "Availability" in payload_obj
  assert "Internal ID" in payload_obj
  assert payload_obj["Brand"] == "Garner, Boyle and Flynn"