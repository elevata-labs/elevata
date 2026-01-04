"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2026 Ilona Tag

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
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

# Domain-level execution snapshot (independent of meta persistence)

def _json_default(obj: Any) -> Any:
  """
  Make common types JSON serializable.
  """
  if isinstance(obj, datetime):
    return obj.isoformat()
  if is_dataclass(obj):
    return asdict(obj)
  return str(obj)

def build_execution_snapshot(
  *,
  batch_run_id: str,
  policy,
  plan,
  execute: bool,
  no_deps: bool,
  continue_on_error: bool,
  max_retries: int,
  profile_name: str,
  target_system_short: str,
  target_system_type: str,
  dialect_name: str,
  root_dataset_key: str,
  created_at: datetime,
  results: list[dict[str, object]] | None = None,
  had_error: bool | None = None,
) -> dict[str, Any]:
  """
  Keep snapshot stable and concise. Never include SQL text.
  """
  steps = []
  for s in plan.steps:
    steps.append({
      "dataset_id": int(s.dataset_id),
      "dataset_key": str(s.dataset_key),
      "upstream_keys": list(s.upstream_keys),
    })

  out: dict[str, Any] = {
    "batch_run_id": batch_run_id,
    "created_at": created_at,
    "context": {
      "execute": bool(execute),
      "no_deps": bool(no_deps),
      "continue_on_error": bool(continue_on_error),
      "max_retries": int(max_retries),
      "profile": profile_name,
      "target_system": target_system_short,
      "target_system_type": target_system_type,
      "dialect": dialect_name,
      "root_dataset": root_dataset_key,
    },
    "policy": {
      "continue_on_error": bool(policy.continue_on_error),
      "max_retries": int(policy.max_retries),
    },
    "plan": {
      "step_count": len(steps),
      "steps": steps,
    },
  }

  if results is not None:
    """
    Summarize results for easy visualization.
    """
    counts_by_status: dict[str, int] = {}
    counts_by_kind: dict[str, int] = {}

    compact_results: list[dict[str, Any]] = []
    for r in results:
      status = str(r.get("status") or "unknown")
      kind = str(r.get("kind") or "unknown")

      counts_by_status[status] = counts_by_status.get(status, 0) + 1
      counts_by_kind[kind] = counts_by_kind.get(kind, 0) + 1

      compact_results.append({
        "dataset": r.get("dataset"),
        "status": status,
        "kind": kind,
        "attempt_no": r.get("attempt_no"),
        "status_reason": r.get("status_reason"),
        "blocked_by": r.get("blocked_by"),
        "message": r.get("message"),
        # Optional perf/observability fields if present
        "render_ms": r.get("render_ms"),
        "execution_ms": r.get("execution_ms"),
        "sql_length": r.get("sql_length"),
        "rows_affected": r.get("rows_affected"),
      })

    out["outcome"] = {
      "had_error": bool(had_error),
      "counts_by_status": counts_by_status,
      "counts_by_kind": counts_by_kind,
      "results": compact_results,
    }

  return out

def render_execution_snapshot_json(snapshot: dict[str, Any]) -> str:
  return json.dumps(snapshot, indent=2, sort_keys=True, default=_json_default)

def write_execution_snapshot_file(
  *,
  snapshot: dict[str, Any],
  snapshot_dir: str,
  batch_run_id: str,
) -> Path:
  """
  Best-effort: caller should wrap in try/except.
  """
  out_dir = Path(snapshot_dir)
  out_dir.mkdir(parents=True, exist_ok=True)

  path = out_dir / f"{batch_run_id}.json"
  path.write_text(render_execution_snapshot_json(snapshot), encoding="utf-8")
  return path
