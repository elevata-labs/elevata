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

from metadata.ingestion import connectors


def test_rest_ingestion_receives_batch_runtime_state(monkeypatch):
  source_dataset = SimpleNamespace(
    source_system=SimpleNamespace(type="rest", short_name="api"),
    source_dataset_name="items",
  )
  td = SimpleNamespace(target_dataset_name="raw_items")
  target_engine = object()
  meta_state: set[tuple[str, str]] = set()
  schema_state: set[tuple[str, str]] = set()
  captured = {}

  def fake_ingest_raw_rest(**kwargs):
    captured.update(kwargs)
    return {"status": "success"}

  import metadata.ingestion.rest as rest_mod

  monkeypatch.setattr(rest_mod, "ingest_raw_rest", fake_ingest_raw_rest)

  # The dispatcher imports ingest_raw_rest locally, so patching the module is
  # enough as long as the import happens after the monkeypatch above.
  connectors.ingest_raw_for_source_dataset(
    source_dataset=source_dataset,
    td=td,
    target_system=SimpleNamespace(short_name="dbdwh", type="databricks"),
    dialect=object(),
    profile=SimpleNamespace(name="dev"),
    batch_run_id="batch",
    load_run_id="load",
    meta_schema="meta",
    target_engine=target_engine,
    meta_log_ensure_state=meta_state,
    schema_ensure_state=schema_state,
  )

  assert captured["target_engine"] is target_engine
  assert captured["meta_log_ensure_state"] is meta_state
  assert captured["schema_ensure_state"] is schema_state
  assert captured["meta_schema"] == "meta"
