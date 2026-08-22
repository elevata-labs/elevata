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

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Callable
import uuid

from django.core.management.base import CommandError
from django.utils.timezone import now

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
  Prefer canonical execution upstream resolution from metadata.execution.load_graph.
  """
  # Canonical execution upstream resolver. Legacy resolution failures remain
  # best-effort, but mandatory execution-contract errors must fail closed.
  try:
    from metadata.execution.load_graph import (
      ExecutionGraphError,
      resolve_execution_upstream_datasets,
    )
  except Exception:
    ExecutionGraphError = None  # type: ignore[assignment,misc]
    resolve_execution_upstream_datasets = None  # type: ignore[assignment]

  steps: list[ExecutionStep] = []
  for td in execution_order:
    key = _dataset_key(td)

    ups: list[str] = []
    if resolve_execution_upstream_datasets is not None:
      try:
        upstream_datasets = resolve_execution_upstream_datasets(td)
        ups = sorted(_dataset_key(u) for u in upstream_datasets)
      except Exception as exc:
        if ExecutionGraphError is not None and isinstance(exc, ExecutionGraphError):
          raise
        # Preserve legacy best-effort handling for unrelated resolution issues.
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


def _normalize_impact_decisions(
  *,
  plan: ExecutionPlan,
  impact_decisions: Mapping[str, str] | None,
) -> dict[str, str] | None:
  """
  Validate an optional impact-driven selection against the execution plan.
  """
  if impact_decisions is None:
    return None

  normalized = {
    str(dataset_key or "").strip(): str(decision or "").strip()
    for dataset_key, decision in impact_decisions.items()
  }
  if any(not dataset_key for dataset_key in normalized):
    raise ValueError(
      "Execution impact decisions contain an empty dataset key."
    )

  plan_keys = tuple(step.dataset_key for step in plan.steps)
  if (
    set(normalized) != set(plan_keys)
    or len(normalized) != len(plan_keys)
  ):
    raise ValueError(
      "Execution impact decisions do not match the ExecutionPlan scope."
    )

  allowed_decisions = {
    "REUSE",
    "INCREMENTAL_EXECUTE",
    "FULL_REBUILD",
  }
  unsupported = tuple(
    sorted(
      (dataset_key, decision)
      for dataset_key, decision in normalized.items()
      if decision not in allowed_decisions
    )
  )
  if unsupported:
    details = ", ".join(
      f"{dataset_key}={decision}"
      for dataset_key, decision in unsupported
    )
    raise ValueError(
      "ExecutionPlan received non-executable impact decisions: "
      f"{details}."
    )

  return normalized


def _with_impact_decision(
  result: dict[str, object],
  *,
  decision: str | None,
) -> dict[str, object]:
  """
  Attach the planned impact decision to one runtime outcome.
  """
  normalized = dict(result)
  if decision is not None:
    normalized["impact_decision"] = decision
  return normalized


def _with_orchestration_timestamp(
  result: dict[str, object],
) -> dict[str, object]:
  """
  Attach the UTC decision instant to one orchestration-only outcome.

  Synthetic outcomes do not execute dataset work, therefore their start and
  finish timestamps intentionally describe the same decision instant.
  """
  timestamp = now()
  return {
    **result,
    "started_at": timestamp,
    "finished_at": timestamp,
  }


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
  impact_decisions: Mapping[str, str] | None = None,
) -> tuple[list[dict[str, object]], bool]:
  """
  Execute an ExecutionPlan and return (results, had_error).

  - Blocked semantics: if any upstream dataset has status=error, downstream is skipped(blocked).
  - Retry semantics: retries apply only in execute-mode; dry-run failures are surfaced immediately.
  - Attempt counter: attempt_no starts at 1 and is passed to run_dataset_fn.
  - Graph resolution remains best-effort for legacy errors; mandatory execution-contract errors fail closed.
  """
  resolved_impact_decisions = _normalize_impact_decisions(
    plan=plan,
    impact_decisions=impact_decisions,
  )

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
      decision = (
        resolved_impact_decisions.get(remaining.dataset_key)
        if resolved_impact_decisions is not None
        else None
      )
      results.append(_with_impact_decision(
        _with_orchestration_timestamp({
          "status": "skipped",
          "kind": "aborted",
          "dataset": remaining.dataset_key,
          "message": "aborted_due_to_fail_fast",
          "status_reason": "fail_fast_abort",
          "load_run_id": str(uuid.uuid4()),
        }),
        decision=decision,
      ))

  for idx, step in enumerate(plan.steps):
    impact_decision = (
      resolved_impact_decisions.get(step.dataset_key)
      if resolved_impact_decisions is not None
      else None
    )
    td = by_id.get(step.dataset_id)
    if td is None:
      # Should not happen; treat as error
      had_error = True
      status_by_key[step.dataset_key] = "error"
      results.append(_with_impact_decision({
          "status": "error",
          "kind": "exception",
          "dataset": step.dataset_key,
          "message": "execution_plan_dataset_missing",
        },
        decision=impact_decision,
      ))
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
      results.append(_with_impact_decision(
        _with_orchestration_timestamp({
          "status": "skipped",
          "kind": "blocked",
          "dataset": step.dataset_key,
          "message": f"blocked_by_dependency: {blocked_by}",
          "blocked_by": blocked_by,
          "status_reason": "blocked_by_dependency",
          "load_run_id": str(uuid.uuid4()),
        }),
        decision=impact_decision,
      ))
      status_by_key[step.dataset_key] = "skipped"
      continue

    if impact_decision == "REUSE":
      results.append(_with_orchestration_timestamp({
        "status": "skipped",
        "kind": "impact_reuse",
        "dataset": step.dataset_key,
        "message": "reused_by_execution_impact_plan",
        "status_reason": "execution_impact_reuse",
        "load_run_id": str(uuid.uuid4()),
        "impact_decision": "REUSE",
      }))
      # Reused materialization remains available to downstream consumers.
      status_by_key[step.dataset_key] = "success"
      continue

    this_load_run_id = root_load_run_id if td is root_td else None
    this_load_plan = root_load_plan if (td is root_td) else None

    attempt_no = 0
    last_exc: Exception | None = None
    result: dict[str, object] | None = None

    while True:
      attempt_no += 1
      try:
        run_kwargs = {
          "target_dataset": td,
          "batch_run_id": plan.batch_run_id,
          "load_run_id": this_load_run_id,
          "load_plan_override": this_load_plan,
          "attempt_no": attempt_no,
        }
        if impact_decision is not None:
          run_kwargs["impact_decision"] = impact_decision

        result = run_dataset_fn(**run_kwargs)
        last_exc = None
        break

      except CommandError as exc:
        # Controlled failure (e.g., preflight blocked). Do not treat as exception noise.
        last_exc = None
        result = {
          "status": "blocked",
          "kind": "preflight",
          "dataset": step.dataset_key,
          "message": str(exc),
        }
        result = _with_impact_decision(
          result,
          decision=impact_decision,
        )
        break

      except Exception as exc:
        last_exc = exc
        should_retry = bool(execute) and attempt_no <= policy.max_retries
        if not should_retry:
          break

    if last_exc is not None and result is None:
      had_error = True
      status_by_key[step.dataset_key] = "error"

      results.append(_with_impact_decision({
          "status": "error",
          "kind": "exception",
          "dataset": step.dataset_key,
          "message": str(last_exc),
        },
        decision=impact_decision,
      ))

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
    result = _with_impact_decision(
      dict(result or {}),
      decision=impact_decision,
    )
    results.append(result)
    status = str((result or {}).get("status") or "unknown")
    status_by_key[step.dataset_key] = status

    if status in ("error", "blocked"):
      had_error = True
      if not policy.continue_on_error:
        _append_aborted_from_index(idx + 1)
        break

  return results, had_error
