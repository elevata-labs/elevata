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
from types import SimpleNamespace

from metadata.architecture.execution_record import (
  ArchitectureExecutionRecordStore,
  build_architecture_execution_record,
  render_architecture_execution_record_json,
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