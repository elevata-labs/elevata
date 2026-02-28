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

from metadata.ingestion import native_raw


class DummyEngine:
  def execute(self, *args, **kwargs):
    return None

  def execute_many(self, *args, **kwargs):
    return None


class DummyDialect:
  def get_execution_engine(self, target_system):
    return DummyEngine()


def test_ingest_raw_file_parquet_rebuild_only_on_first_chunk(monkeypatch):
  chunks = [
    [{"id": 1}, {"id": 2}, {"id": 3}],
    [{"id": 4}, {"id": 5}],
  ]

  def fake_iter_parquet(path, chunk_size):
    for c in chunks:
      yield c

  monkeypatch.setattr(native_raw, "_iter_parquet_record_chunks", fake_iter_parquet)
  monkeypatch.setattr(native_raw, "_local_path_from_uri", lambda uri: "/tmp/test.parquet")
  monkeypatch.setattr(native_raw.os.path, "exists", lambda p: True)

  calls = []

  def fake_land_raw_json_records(**kwargs):
    calls.append({
      "rebuild": kwargs.get("rebuild", None),
      "write_run_log": kwargs.get("write_run_log", None),
      "records_len": len(kwargs.get("records") or []),
    })
    return {"rows_inserted": len(kwargs.get("records") or [])}

  monkeypatch.setattr(native_raw, "land_raw_json_records", fake_land_raw_json_records)

  source_system = SimpleNamespace(type="parquet", short_name="parq")
  source_dataset = SimpleNamespace(
    source_system=source_system,
    source_dataset_name="test_parquet",
    ingestion_config={"uri": "file:///tmp/test.parquet"},
  )

  td = SimpleNamespace(
    target_schema=SimpleNamespace(schema_name="raw", short_name="raw"),
    target_dataset_name="test_parquet",
  )

  target_system = SimpleNamespace(short_name="duckdb", type="duckdb")
  dialect = DummyDialect()
  profile = SimpleNamespace(name="dev")

  result = native_raw.ingest_raw_file(
    source_dataset=source_dataset,
    td=td,
    target_system=target_system,
    dialect=dialect,
    profile=profile,
    batch_run_id="batch-1",
    load_run_id="load-1",
    meta_schema="meta",
    chunk_size=3,
    file_type="parquet",
  )

  assert result["rows_extracted"] == 5
  assert len(calls) == 2
  assert calls[0]["records_len"] == 3
  assert calls[1]["records_len"] == 2

  # Contract: rebuild only on first chunk
  assert calls[0]["rebuild"] is True
  assert calls[1]["rebuild"] is False

  # If the implementation suppresses per-chunk run log writes, enforce it.
  if calls[0]["write_run_log"] is not None:
    assert calls[0]["write_run_log"] is False
    assert calls[1]["write_run_log"] is False