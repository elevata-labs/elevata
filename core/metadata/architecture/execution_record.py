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
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
from typing import Any

from .paths import (
  ARCHITECTURE_EXECUTION_DIR_ENV,
  ArchitectureArtifactContext,
  DEFAULT_ARCHITECTURE_EXECUTION_DIR,
  resolve_architecture_execution_dir,
)


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
  output_lines: tuple[str, ...]
  error_lines: tuple[str, ...]
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
      "output_lines": list(self.output_lines),
      "error_lines": list(self.error_lines),
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
  

@dataclass(frozen=True)
class ArchitectureExecutionRecordFilters:
  """
  Filter set for Architecture Execution Record history queries.
  """
  scope_key: str | None = None
  status: str | None = None
  dependency_mode: str | None = None
  started_from: datetime | None = None
  started_to: datetime | None = None


@dataclass(frozen=True)
class ArchitectureExecutionRecordSummary:
  """
  Table-shaped summary for one stored Architecture Execution Record.
  """
  execution_id: str
  started_at: str
  finished_at: str
  duration_ms: int
  duration_label: str
  status: str
  message: str
  scope_key: str
  scope_label: str
  dependency_mode: str
  report_fingerprint: str
  approval_id: str | None
  preview_fingerprint: str
  record_fingerprint: str
  path: str


class ArchitectureExecutionRecordStore:
  """
  File-backed store for Architecture Execution Records.
  """

  def __init__(
    self,
    base_path: str | Path | None = None,
    *,
    context: ArchitectureArtifactContext | None = None,
  ):
    self.base_path = (
      Path(base_path).expanduser()
      if base_path is not None
      else resolve_architecture_execution_dir(context=context)
    )


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
  

  def load_payload(self, execution_id: str) -> dict[str, Any]:
    """
    Load one stored Architecture Execution Record payload.
    """
    path = self.path_for(execution_id)
    payload = _read_record_payload(path)
    payload_execution_id = str(payload.get("execution_id") or "")
    if payload_execution_id != str(execution_id):
      raise ValueError("Execution record identifier does not match its file path.")
    return payload

  def load_summary(self, execution_id: str) -> ArchitectureExecutionRecordSummary:
    """
    Load one stored Architecture Execution Record summary.
    """
    payload = self.load_payload(execution_id)
    return build_architecture_execution_record_summary(
      payload,
      path=self.path_for(execution_id),
    )

  def list_records(
    self,
    filters: ArchitectureExecutionRecordFilters | None = None,
    *,
    limit: int | None = 50,
  ) -> tuple[ArchitectureExecutionRecordSummary, ...]:
    """
    Return stored Architecture Execution Record summaries.
    """
    summaries: list[ArchitectureExecutionRecordSummary] = []

    for path in self.iter_record_paths():
      try:
        payload = _read_record_payload(path)
      except (OSError, ValueError, json.JSONDecodeError):
        continue

      if not _record_matches_filters(payload, filters):
        continue

      summaries.append(
        build_architecture_execution_record_summary(payload, path=path)
      )

    summaries.sort(
      key=lambda summary: (
        summary.started_at,
        summary.finished_at,
        summary.execution_id,
      ),
      reverse=True,
    )

    if limit is None:
      return tuple(summaries)

    return tuple(summaries[: max(int(limit), 0)])

  def delete_older_than(
    self,
    cutoff: datetime,
    *,
    filters: ArchitectureExecutionRecordFilters | None = None,
  ) -> int:
    """
    Delete stored Architecture Execution Records older than the cutoff timestamp.
    """
    deleted = 0
    cutoff_value = _ensure_utc(cutoff)

    for path in self.iter_record_paths():
      try:
        payload = _read_record_payload(path)
      except (OSError, ValueError, json.JSONDecodeError):
        continue

      if not _record_matches_filters(payload, filters):
        continue

      started_at = _record_datetime(payload, "started_at")
      if started_at is None or started_at >= cutoff_value:
        continue

      try:
        path.unlink()
      except OSError:
        continue

      deleted += 1

    return deleted


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
    output_lines=output_lines,
    error_lines=error_lines,
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
  return render_architecture_execution_record_payload_json(record.to_dict())


def render_architecture_execution_record_payload_json(
  payload: dict[str, Any],
) -> str:
  """
  Render an Architecture Execution Record payload as canonical JSON.
  """
  return json.dumps(
    payload,
    sort_keys=True,
    ensure_ascii=False,
    indent=2,
    default=str,
  ) + "\n"


def build_architecture_execution_record_summary(
  payload: dict[str, Any],
  *,
  path: Path,
) -> ArchitectureExecutionRecordSummary:
  """
  Build an Architecture Execution Record summary from a stored payload.
  """
  return ArchitectureExecutionRecordSummary(
    execution_id=str(payload.get("execution_id") or ""),
    started_at=str(payload.get("started_at") or ""),
    finished_at=str(payload.get("finished_at") or ""),
    duration_ms=int(payload.get("duration_ms") or 0),
    duration_label=format_architecture_execution_duration(
      payload.get("duration_ms"),
    ),
    status=str(payload.get("status") or ""),
    message=str(payload.get("message") or ""),
    scope_key=str(payload.get("scope_key") or ""),
    scope_label=str(payload.get("scope_label") or ""),
    dependency_mode=str(payload.get("dependency_mode") or ""),
    report_fingerprint=str(payload.get("report_fingerprint") or ""),
    approval_id=payload.get("approval_id"),
    preview_fingerprint=str(payload.get("preview_fingerprint") or ""),
    record_fingerprint=str(payload.get("record_fingerprint") or ""),
    path=str(path),
  )


def format_architecture_execution_duration(duration_ms: Any) -> str:
  """
  Return a compact duration label in seconds.
  """
  try:
    value = max(0, int(duration_ms or 0))
  except (TypeError, ValueError):
    value = 0

  return f"{value / 1000:.3f} s"


def _read_record_payload(path: Path) -> dict[str, Any]:
  """
  Read and validate one Architecture Execution Record payload.
  """
  payload = json.loads(path.read_text(encoding="utf-8"))
  if not isinstance(payload, dict):
    raise ValueError("Execution record payload must be a JSON object.")

  if payload.get("record_type") != "architecture_execution_record":
    raise ValueError("JSON payload is not an Architecture Execution Record.")

  return payload


def _record_matches_filters(
  payload: dict[str, Any],
  filters: ArchitectureExecutionRecordFilters | None,
) -> bool:
  """
  Return True when a payload matches the selected history filters.
  """
  if filters is None:
    return True

  if filters.scope_key and payload.get("scope_key") != filters.scope_key:
    return False

  if filters.status and payload.get("status") != filters.status:
    return False

  if (
    filters.dependency_mode
    and payload.get("dependency_mode") != filters.dependency_mode
  ):
    return False

  started_at = _record_datetime(payload, "started_at")

  if filters.started_from is not None:
    if started_at is None or started_at < _ensure_utc(filters.started_from):
      return False

  if filters.started_to is not None:
    if started_at is None or started_at > _ensure_utc(filters.started_to):
      return False

  return True


def _record_datetime(
  payload: dict[str, Any],
  key: str,
) -> datetime | None:
  """
  Return a normalized timestamp from an Architecture Execution Record payload.
  """
  raw_value = str(payload.get(key) or "").strip()
  if not raw_value:
    return None

  if raw_value.endswith("Z"):
    raw_value = f"{raw_value[:-1]}+00:00"

  try:
    value = datetime.fromisoformat(raw_value)
  except ValueError:
    return None

  return _ensure_utc(value)


def _ensure_utc(value: datetime) -> datetime:
  """
  Return a timezone-aware UTC timestamp.
  """
  if value.tzinfo is None:
    return value.replace(tzinfo=timezone.utc)
  return value.astimezone(timezone.utc)


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