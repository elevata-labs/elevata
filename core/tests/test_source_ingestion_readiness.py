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

from __future__ import annotations

from types import SimpleNamespace

from metadata.services.source_ingestion_readiness import (
  build_source_ingestion_readiness,
)


class _Manager:
  def __init__(self, values):
    self._values = list(values)

  def all(self):
    return list(self._values)

  def filter(self, **kwargs):
    values = self._values
    for key, expected in kwargs.items():
      values = [
        value
        for value in values
        if getattr(value, key, None) == expected
      ]
    return _Manager(values)

  def exists(self):
    return bool(self._values)

  def __iter__(self):
    return iter(self._values)


def _source_column(
  name: str = "customer_id",
  *,
  integrate: bool = True,
  json_path: str | None = "$.customer_id",
):
  return SimpleNamespace(
    source_column_name=name,
    integrate=integrate,
    json_path=json_path,
  )


def _raw_link(
  target_name: str = "raw_src_customer",
  *,
  active: bool = True,
):
  return SimpleNamespace(
    active=active,
    target_dataset=SimpleNamespace(
      target_schema=SimpleNamespace(short_name="raw"),
      target_dataset_name=target_name,
    ),
  )


def _increment_policy(*, active: bool = True):
  return SimpleNamespace(active=active)


def _dataset(
  *,
  source_type: str = "mssql",
  include_ingest: str = "native",
  generate_raw_tables: bool = True,
  generate_raw_table=None,
  integrate: bool = True,
  active: bool = True,
  is_source: bool = True,
  ingestion_config=None,
  columns=None,
  output_links=None,
  incremental: bool = False,
  increment_filter: str | None = None,
  increment_policies=None,
):
  return SimpleNamespace(
    source_system=SimpleNamespace(
      short_name="src",
      type=source_type,
      include_ingest=include_ingest,
      generate_raw_tables=generate_raw_tables,
      is_source=is_source,
    ),
    schema_name="dbo",
    source_dataset_name="customer",
    integrate=integrate,
    active=active,
    generate_raw_table=generate_raw_table,
    ingestion_config=ingestion_config,
    source_columns=_Manager(
      columns if columns is not None else [_source_column()]
    ),
    output_links=_Manager(
      output_links if output_links is not None else [_raw_link()]
    ),
    incremental=incremental,
    increment_filter=increment_filter,
    increment_policies=_Manager(increment_policies or []),
  )


def _codes(readiness) -> set[str]:
  return {signal.code for signal in readiness.signals}


def test_readiness_is_not_applicable_outside_integration_scope():
  readiness = build_source_ingestion_readiness(_dataset(
    integrate=False,
    output_links=[],
  ))

  assert readiness.status == "not_applicable"
  assert readiness.landing_required is False
  assert readiness.ingest_mode == "none"
  assert "dataset_not_integrated" in _codes(readiness)
  assert readiness.blocking_signal_count == 0


def test_native_relational_readiness_is_ready_with_required_metadata():
  readiness = build_source_ingestion_readiness(_dataset())

  assert readiness.status == "ready"
  assert readiness.is_ready is True
  assert readiness.source_kind == "relational"
  assert readiness.ingest_mode == "native"
  assert readiness.raw_target_keys == ("raw.raw_src_customer",)
  assert readiness.integrated_column_count == 1
  assert readiness.blocking_signal_count == 0


def test_required_landing_with_ingest_none_is_attention():
  readiness = build_source_ingestion_readiness(_dataset(
    include_ingest="none",
  ))

  assert readiness.status == "attention"
  assert readiness.ingest_mode == "none"
  assert "ingest_mode_inconsistent" in _codes(readiness)
  assert readiness.blocking_signal_count == 1


def test_external_file_ingestion_does_not_require_native_file_config():
  readiness = build_source_ingestion_readiness(_dataset(
    source_type="csv",
    include_ingest="external",
    ingestion_config=None,
  ))

  assert readiness.status == "ready"
  assert readiness.ingest_mode == "external"
  assert "external_ingestion_declared" in _codes(readiness)
  assert "file_uri_missing" not in _codes(readiness)


def test_native_file_alias_does_not_hide_missing_execution_uri():
  readiness = build_source_ingestion_readiness(_dataset(
    source_type="csv",
    ingestion_config={"url": "file:///tmp/customer.csv"},
  ))

  assert readiness.status == "attention"
  assert "file_uri_alias_not_executable" in _codes(readiness)


def test_json_lines_requires_jsonl_source_type_for_native_execution():
  readiness = build_source_ingestion_readiness(_dataset(
    source_type="json",
    ingestion_config={"uri": "file:///tmp/customer.jsonl"},
  ))

  assert readiness.status == "attention"
  assert "json_lines_type_mismatch" in _codes(readiness)


def test_native_rest_requires_dataset_path():
  readiness = build_source_ingestion_readiness(_dataset(
    source_type="rest",
    ingestion_config={"record_path": "data.items"},
  ))

  assert readiness.status == "attention"
  assert readiness.source_kind == "rest"
  assert "rest_path_missing" in _codes(readiness)
  assert "rest_secret_expected" in _codes(readiness)


def test_semi_structured_integrated_columns_require_json_paths():
  readiness = build_source_ingestion_readiness(_dataset(
    source_type="csv",
    ingestion_config={"uri": "file:///tmp/customer.csv"},
    columns=[_source_column(json_path=None)],
  ))

  assert readiness.status == "attention"
  assert "json_paths_missing" in _codes(readiness)


def test_delta_cutoff_requires_active_increment_policy():
  readiness = build_source_ingestion_readiness(_dataset(
    incremental=True,
    increment_filter="changed_at >= {{DELTA_CUTOFF}}",
    increment_policies=[],
  ))

  assert readiness.status == "attention"
  assert "increment_policy_missing" in _codes(readiness)


def test_multiple_raw_targets_require_attention():
  readiness = build_source_ingestion_readiness(_dataset(
    output_links=[
      _raw_link("raw_src_customer_a"),
      _raw_link("raw_src_customer_b"),
    ],
  ))

  assert readiness.status == "attention"
  assert readiness.warning_signal_count == 1
  assert "multiple_raw_targets" in _codes(readiness)
