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

from dataclasses import dataclass
from pathlib import Path
import hashlib
import json
import os
from typing import Any


ARCHITECTURE_EXECUTION_DIR_ENV = "ELEVATA_ARCH_EXECUTION_DIR"
DEFAULT_ARCHITECTURE_EXECUTION_DIR = ".elevata/executions"


@dataclass(frozen=True)
class ArchitectureExecutionRecord:
  """
  Audit record for one Architecture Control execution.
  """
  execution_id: str
  started_by: str
  started_at: str
  finished_at: str
  duration_ms: int
  status: str
  message: str
  scope_key: str
  scope_label: str
  dependency_mode: str
  report_fingerprint: str
  approval_id: str | None
  preview_fingerprint: str
  command_name: str
  command_args: tuple[str, ...]
  command_options: dict[str, Any]
  output_tail: tuple[str, ...]
  error_tail: tuple[str, ...]
  output_line_count: int
  error_line_count: int
  output_truncated: bool
  error_truncated: bool

  @property
  def record_fingerprint(self) -> str:
    """
    Return the deterministic fingerprint for this execution record.
    """
    return _stable_json_hash(self.to_dict(include_fingerprint=False))

  def to_dict(self, *, include_fingerprint: bool = True) -> dict[str, Any]:
    """
    Return the table-shaped execution record payload.
    """
    payload = {
      "record_type": "architecture_execution_record",
      "record_version": 1,
      "execution_id": self.execution_id,
      "started_by": self.started_by,
      "started_at": self.started_at,
      "finished_at": self.finished_at,
      "duration_ms": self.duration_ms,
      "status": self.status,
      "message": self.message,
      "scope_key": self.scope_key,
      "scope_label": self.scope_label,
      "dependency_mode": self.dependency_mode,
      "report_fingerprint": self.report_fingerprint,
      "approval_id": self.approval_id,
      "preview_fingerprint": self.preview_fingerprint,
      "command_name": self.command_name,
      "command_args": list(self.command_args),
      "command_options": self.command_options,
      "output_tail": list(self.output_tail),
      "error_tail": list(self.error_tail),
      "output_line_count": self.output_line_count,
      "error_line_count": self.error_line_count,
      "output_truncated": self.output_truncated,
      "error_truncated": self.error_truncated,
    }

    if include_fingerprint:
      payload["record_fingerprint"] = self.record_fingerprint

    return payload


class ArchitectureExecutionRecordStore:
  """
  File-backed store for Architecture Execution Records.
  """

  def __init__(self, base_path: str | Path | None = None):
    self.base_path = Path(
      base_path
      or os.environ.get(
        ARCHITECTURE_EXECUTION_DIR_ENV,
        DEFAULT_ARCHITECTURE_EXECUTION_DIR,
      )
    ).expanduser()

  def save(self, record: ArchitectureExecutionRecord) -> Path:
    """
    Store an Architecture Execution Record and return its path.
    """
    self.base_path.mkdir(parents=True, exist_ok=True)
    path = self.path_for(record.execution_id)
    path.write_text(
      render_architecture_execution_record_json(record),
      encoding="utf-8",
    )
    return path

  def path_for(self, execution_id: str) -> Path:
    """
    Return the storage path for an execution identifier.
    """
    safe_execution_id = "".join(
      char
      for char in str(execution_id)
      if char.isalnum() or char in {"-", "_"}
    )
    return self.base_path / f"{safe_execution_id}.execution.json"

  def iter_record_paths(self) -> tuple[Path, ...]:
    """
    Return stored execution record paths ordered by file name.
    """
    if not self.base_path.exists():
      return ()

    return tuple(sorted(self.base_path.glob("*.execution.json")))


def build_architecture_execution_record(result: Any) -> ArchitectureExecutionRecord:
  """
  Build an Architecture Execution Record from a controlled execution result.
  """
  output_lines = tuple(getattr(result, "output_lines", ()) or ())
  error_lines = tuple(getattr(result, "error_lines", ()) or ())

  return ArchitectureExecutionRecord(
    execution_id=getattr(result, "execution_id"),
    started_by=getattr(result, "started_by", ""),
    started_at=getattr(result, "started_at", ""),
    finished_at=getattr(result, "finished_at", ""),
    duration_ms=int(getattr(result, "duration_ms", 0) or 0),
    status=getattr(result, "status", ""),
    message=getattr(result, "message", ""),
    scope_key=getattr(result, "scope_key", ""),
    scope_label=getattr(result, "scope_label", ""),
    dependency_mode=getattr(result, "dependency_mode", ""),
    report_fingerprint=getattr(result, "report_fingerprint", ""),
    approval_id=getattr(result, "approval_id", None),
    preview_fingerprint=getattr(result, "preview_fingerprint", ""),
    command_name=getattr(result, "command_name", ""),
    command_args=tuple(getattr(result, "command_args", ()) or ()),
    command_options=dict(getattr(result, "command_options", {}) or {}),
    output_tail=tuple(getattr(result, "output_tail", ()) or ()),
    error_tail=tuple(getattr(result, "error_tail", ()) or ()),
    output_line_count=len(output_lines),
    error_line_count=len(error_lines),
    output_truncated=bool(getattr(result, "output_truncated", False)),
    error_truncated=bool(getattr(result, "error_truncated", False)),
  )


def render_architecture_execution_record_json(
  record: ArchitectureExecutionRecord,
) -> str:
  """
  Render an Architecture Execution Record as canonical JSON.
  """
  return json.dumps(
    record.to_dict(),
    sort_keys=True,
    ensure_ascii=False,
    indent=2,
    default=str,
  ) + "\n"


def _stable_json_hash(value: Any) -> str:
  """
  Return a deterministic SHA-256 hash for a JSON-serializable value.
  """
  payload = json.dumps(
    value,
    sort_keys=True,
    ensure_ascii=False,
    separators=(",", ":"),
    default=str,
  )
  return hashlib.sha256(payload.encode("utf-8")).hexdigest()