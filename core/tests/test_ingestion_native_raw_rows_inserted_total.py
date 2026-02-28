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


def test_ingest_raw_file_parquet_returns_total_rows_inserted(monkeypatch):
  # Fake chunks: 3 chunks à 2 rows = 6 extracted
  chunks = [
    [{"a": 1}, {"a": 2}],
    [{"a": 3}, {"a": 4}],
    [{"a": 5}, {"a": 6}],
  ]

  def _fake_iter_chunks(path, chunk_size):
    for c in chunks:
      yield c

  # Return rows_inserted equal to len(records) per call
  def _fake_land_raw_json_records(*, records, rebuild=False, write_run_log=False, **kwargs):
    return {"rows_inserted": len(records)}

  monkeypatch.setattr(native_raw, "_iter_parquet_record_chunks", _fake_iter_chunks)
  monkeypatch.setattr(native_raw, "land_raw_json_records", _fake_land_raw_json_records)

  # Avoid filesystem check
  monkeypatch.setattr(native_raw.os.path, "exists", lambda p: True)

  source_dataset = SimpleNamespace(
    ingestion_config={"uri": "file:///C:/temp/test.parquet"},
    source_system=SimpleNamespace(type="parquet", short_name="parquet"),
    source_dataset_name="pqt_test",
  )

  td = SimpleNamespace(
    target_schema=SimpleNamespace(short_name="raw", schema_name="raw"),
    target_dataset_name="raw_pqt_test",
  )

  target_system = SimpleNamespace(short_name="duckdb", type="duckdb")
  dialect = SimpleNamespace(get_execution_engine=lambda ts: SimpleNamespace(execute=lambda *_a, **_k: None))
  profile = SimpleNamespace(name="test")

  res = native_raw.ingest_raw_file(
    source_dataset=source_dataset,
    td=td,
    target_system=target_system,
    dialect=dialect,
    profile=profile,
    batch_run_id="b1",
    load_run_id="lr1",
    chunk_size=2,
    file_type="parquet",
  )

  assert res["rows_extracted"] == 6
  assert res["landing"]["rows_inserted"] == 6