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

import json
from datetime import datetime, timezone
from types import SimpleNamespace

from metadata.architecture.execution_record import (
  ArchitectureExecutionRecordFilters,
  ArchitectureExecutionRecordStore,
  build_architecture_execution_record,
  render_architecture_execution_record_json,
  render_architecture_execution_record_payload_json,
)


def _result() -> SimpleNamespace:
  """
  Return a controlled-execution-result-shaped object.
  """
  return SimpleNamespace(
    execution_id="exec_123",
    started_by="Ilona",
    started_at="2026-05-24T10:00:00+00:00",
    finished_at="2026-05-24T10:00:02+00:00",
    duration_ms=2000,
    status="success",
    message="Architecture execution completed.",
    scope_key="bizcore.bc_dim_customer",
    scope_label="bizcore.bc_dim_customer",
    dependency_mode="target_only",
    report_fingerprint="report-1",
    approval_id="apr_123",
    preview_fingerprint="preview-1",
    command_name="elevata_load",
    command_args=("bc_dim_customer",),
    command_options={
      "execute": True,
      "schema_short": "bizcore",
      "no_deps": True,
    },
    output_lines=("line 1", "line 2", "line 3"),
    output_tail=("line 2", "line 3"),
    output_truncated=False,
    error_lines=(),
    error_tail=(),
    error_truncated=False,
  )


def test_build_architecture_execution_record_from_result() -> None:
  """
  Verify execution record construction from a controlled execution result.
  """
  record = build_architecture_execution_record(_result())

  assert record.execution_id == "exec_123"
  assert record.started_by == "Ilona"
  assert record.status == "success"
  assert record.scope_key == "bizcore.bc_dim_customer"
  assert record.dependency_mode == "target_only"
  assert record.output_line_count == 3
  assert record.error_line_count == 0
  assert record.record_fingerprint


def test_render_architecture_execution_record_json_is_canonical() -> None:
  """
  Verify execution record JSON contains the audit payload and fingerprint.
  """
  record = build_architecture_execution_record(_result())
  rendered = render_architecture_execution_record_json(record)
  payload = json.loads(rendered)

  assert payload["record_type"] == "architecture_execution_record"
  assert payload["record_version"] == 1
  assert payload["execution_id"] == "exec_123"
  assert payload["command_options"] == {
    "execute": True,
    "no_deps": True,
    "schema_short": "bizcore",
  }
  assert payload["record_fingerprint"] == record.record_fingerprint


def test_architecture_execution_record_store_writes_json(tmp_path) -> None:
  """
  Verify execution record storage writes the canonical payload.
  """
  record = build_architecture_execution_record(_result())
  store = ArchitectureExecutionRecordStore(base_path=tmp_path)

  path = store.save(record)
  payload = json.loads(path.read_text(encoding="utf-8"))

  assert path == tmp_path / "exec_123.execution.json"
  assert payload["execution_id"] == "exec_123"
  assert payload["record_fingerprint"] == record.record_fingerprint
  assert store.iter_record_paths() == (path,)


def _record_for_store(
  *,
  execution_id: str,
  started_at: str,
  status: str = "success",
  scope_key: str = "bizcore.bc_dim_customer",
  dependency_mode: str = "target_only",
):
  """
  Build an Architecture Execution Record for store query tests.
  """
  base = _result().__dict__.copy()
  base.update({
    "execution_id": execution_id,
    "started_at": started_at,
    "finished_at": started_at,
    "status": status,
    "scope_key": scope_key,
    "scope_label": scope_key,
    "dependency_mode": dependency_mode,
  })
  return build_architecture_execution_record(SimpleNamespace(**base))


def test_render_architecture_execution_record_payload_json_is_canonical() -> None:
  """
  Verify stored execution record payloads render as canonical JSON.
  """
  payload = {
    "record_type": "architecture_execution_record",
    "record_version": 1,
    "execution_id": "exec_123",
  }

  rendered = render_architecture_execution_record_payload_json(payload)

  assert json.loads(rendered) == payload
  assert rendered.endswith("\n")


def test_architecture_execution_record_store_loads_payload_and_summary(tmp_path) -> None:
  """
  Verify stored records can be loaded as payloads and summaries.
  """
  record = build_architecture_execution_record(_result())
  store = ArchitectureExecutionRecordStore(base_path=tmp_path)
  store.save(record)

  payload = store.load_payload("exec_123")
  summary = store.load_summary("exec_123")

  assert payload["execution_id"] == "exec_123"
  assert summary.execution_id == "exec_123"
  assert summary.scope_key == "bizcore.bc_dim_customer"
  assert summary.dependency_mode == "target_only"
  assert summary.record_fingerprint == record.record_fingerprint
  assert summary.path == str(tmp_path / "exec_123.execution.json")


def test_architecture_execution_record_store_lists_filtered_records(tmp_path) -> None:
  """
  Verify execution record history filters operate on stored record payloads.
  """
  store = ArchitectureExecutionRecordStore(base_path=tmp_path)
  store.save(_record_for_store(
    execution_id="exec_old_success",
    started_at="2026-05-21T10:00:00+00:00",
    status="success",
    scope_key="bizcore.Customer",
    dependency_mode="target_only",
  ))
  store.save(_record_for_store(
    execution_id="exec_new_failed",
    started_at="2026-05-24T10:00:00+00:00",
    status="failed",
    scope_key="serving.Customer",
    dependency_mode="with_dependencies",
  ))

  failed = store.list_records(
    ArchitectureExecutionRecordFilters(status="failed"),
    limit=None,
  )
  target_only = store.list_records(
    ArchitectureExecutionRecordFilters(dependency_mode="target_only"),
    limit=None,
  )
  serving = store.list_records(
    ArchitectureExecutionRecordFilters(scope_key="serving.Customer"),
    limit=None,
  )

  assert [summary.execution_id for summary in failed] == ["exec_new_failed"]
  assert [summary.execution_id for summary in target_only] == ["exec_old_success"]
  assert [summary.execution_id for summary in serving] == ["exec_new_failed"]


def test_architecture_execution_record_store_filters_by_started_date(tmp_path) -> None:
  """
  Verify execution record history supports inclusive started timestamp filters.
  """
  store = ArchitectureExecutionRecordStore(base_path=tmp_path)
  store.save(_record_for_store(
    execution_id="exec_before",
    started_at="2026-05-20T10:00:00+00:00",
  ))
  store.save(_record_for_store(
    execution_id="exec_after",
    started_at="2026-05-24T10:00:00+00:00",
  ))

  records = store.list_records(
    ArchitectureExecutionRecordFilters(
      started_from=datetime(2026, 5, 23, tzinfo=timezone.utc),
      started_to=datetime(2026, 5, 25, tzinfo=timezone.utc),
    ),
    limit=None,
  )

  assert [summary.execution_id for summary in records] == ["exec_after"]


def test_architecture_execution_record_store_deletes_old_records(tmp_path) -> None:
  """
  Verify retention deletes only records older than the selected cutoff.
  """
  store = ArchitectureExecutionRecordStore(base_path=tmp_path)
  store.save(_record_for_store(
    execution_id="exec_old",
    started_at="2026-05-20T10:00:00+00:00",
  ))
  store.save(_record_for_store(
    execution_id="exec_recent",
    started_at="2026-05-24T10:00:00+00:00",
  ))

  deleted = store.delete_older_than(
    datetime(2026, 5, 22, tzinfo=timezone.utc),
  )

  assert deleted == 1
  assert not (tmp_path / "exec_old.execution.json").exists()
  assert (tmp_path / "exec_recent.execution.json").exists()
