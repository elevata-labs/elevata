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

from dataclasses import dataclass
from typing import Callable
import uuid

from metadata.models import TargetDataset


@dataclass(frozen=True)
class ExecutionPolicy:
  # Core policy knobs for v0.8.0
  continue_on_error: bool
  max_retries: int  # 0 means: no retries

@dataclass(frozen=True)
class ExecutionStep:
  dataset_id: int
  dataset_key: str
  upstream_keys: tuple[str, ...]

@dataclass
class ExecutionPlan:
  batch_run_id: str
  steps: list[ExecutionStep]


def _dataset_key(td: TargetDataset) -> str:
  return f"{td.target_schema.short_name}.{td.target_dataset_name}"

def build_execution_plan(*, batch_run_id: str, execution_order: list[TargetDataset]) -> ExecutionPlan:
  """
  Keep deterministic order, store upstream_keys for blocked semantics.
  Prefer canonical upstream resolution from metadata.execution.load_graph.
  """
  # Canonical upstream resolver (best-effort safe in your codebase)
  try:
    from metadata.execution.load_graph import resolve_upstream_datasets
  except Exception:
    resolve_upstream_datasets = None  # type: ignore[assignment]

  steps: list[ExecutionStep] = []
  for td in execution_order:
    key = _dataset_key(td)

    ups: list[str] = []
    if resolve_upstream_datasets is not None:
      try:
        upstream_datasets = resolve_upstream_datasets(td)
        ups = sorted(_dataset_key(u) for u in upstream_datasets)
      except Exception:
        # best-effort: never block planning due to upstream resolution issues
        ups = []
    else:
      # fallback (should rarely be used)
      try:
        links_mgr = getattr(td, "input_links", None)
        if links_mgr is not None and hasattr(links_mgr, "all"):
          for link in links_mgr.all():
            up = getattr(link, "upstream_target_dataset", None)
            if up is not None:
              ups.append(_dataset_key(up))
      except Exception:
        ups = []

    steps.append(ExecutionStep(
      dataset_id=int(getattr(td, "id", 0) or 0),
      dataset_key=key,
      upstream_keys=tuple(ups),
    ))

  return ExecutionPlan(batch_run_id=batch_run_id, steps=steps)

def execute_plan(
  *,
  plan: ExecutionPlan,
  execution_order: list[TargetDataset],
  policy: ExecutionPolicy,
  execute: bool,
  root_td: TargetDataset,
  root_load_run_id: str | None,
  root_load_plan: object | None,
  run_dataset_fn: Callable[..., dict[str, object]],
  logger,
) -> tuple[list[dict[str, object]], bool]:
  """
  Execute an ExecutionPlan and return (results, had_error).

  - Blocked semantics: if any upstream dataset has status=error, downstream is skipped(blocked).
  - Retry semantics: retries apply only in execute-mode; dry-run failures are surfaced immediately.
  - Attempt counter: attempt_no starts at 1 and is passed to run_dataset_fn.
  - Best-effort: graph resolution errors never block execution.
  """
  # Map dataset_id -> TargetDataset for plan steps
  by_id: dict[int, TargetDataset] = {}
  for td in execution_order:
    td_id = int(getattr(td, "id", 0) or 0)
    by_id[td_id] = td

  results: list[dict[str, object]] = []
  had_error = False
  status_by_key: dict[str, str] = {}

  def _append_aborted_from_index(start_index: int) -> None:
    # Add synthetic "aborted" entries for remaining steps for reporting/visualization.
    for remaining in plan.steps[start_index:]:
      results.append({
        "status": "skipped",
        "kind": "aborted",
        "dataset": remaining.dataset_key,
        "message": "aborted_due_to_fail_fast",
        "status_reason": "fail_fast_abort",
        "load_run_id": str(uuid.uuid4()),
      })

  for idx, step in enumerate(plan.steps):
    td = by_id.get(step.dataset_id)
    if td is None:
      # Should not happen; treat as error
      had_error = True
      status_by_key[step.dataset_key] = "error"
      results.append({
        "status": "error",
        "kind": "exception",
        "dataset": step.dataset_key,
        "message": "execution_plan_dataset_missing",
      })
      if not policy.continue_on_error:
        _append_aborted_from_index(idx + 1)
        break
      continue

    # Blocked semantics
    blocked_by = None
    for up_key in step.upstream_keys:
      if status_by_key.get(up_key) == "error":
        blocked_by = up_key
        break

    if blocked_by is not None:
      results.append({
        "status": "skipped",
        "kind": "blocked",
        "dataset": step.dataset_key,
        "message": f"blocked_by_dependency: {blocked_by}",
        "blocked_by": blocked_by,
        "status_reason": "blocked_by_dependency",
        "load_run_id": str(uuid.uuid4()),
      })
      status_by_key[step.dataset_key] = "skipped"
      continue

    this_load_run_id = root_load_run_id if td is root_td else None
    this_load_plan = root_load_plan if (td is root_td) else None

    attempt_no = 0
    last_exc: Exception | None = None
    result: dict[str, object] | None = None

    while True:
      attempt_no += 1
      try:
        result = run_dataset_fn(
          target_dataset=td,
          batch_run_id=plan.batch_run_id,
          load_run_id=this_load_run_id,
          load_plan_override=this_load_plan,
          attempt_no=attempt_no,
        )
        last_exc = None
        break
      except Exception as exc:
        last_exc = exc
        should_retry = bool(execute) and attempt_no <= policy.max_retries
        if not should_retry:
          break

    if last_exc is not None and result is None:
      had_error = True
      status_by_key[step.dataset_key] = "error"

      results.append({
        "status": "error",
        "kind": "exception",
        "dataset": step.dataset_key,
        "message": str(last_exc),
      })

      # We are outside the except block here, so use exc_info explicitly.
      logger.error(
        "elevata_load dataset failed",
        extra={
          "batch_run_id": plan.batch_run_id,
          "dataset": step.dataset_key,
          "attempt_no": attempt_no,
        },
        exc_info=last_exc,
      )

      if not policy.continue_on_error:
        _append_aborted_from_index(idx + 1)
        break

      continue

    # Normal success / dry_run
    results.append(result)
    status = str((result or {}).get("status") or "unknown")
    status_by_key[step.dataset_key] = status

    if status == "error":
      had_error = True
      if not policy.continue_on_error:
        _append_aborted_from_index(idx + 1)
        break

  return results, had_error
