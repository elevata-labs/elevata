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

from typing import Any


def _get(d: dict[str, Any], path: str, default=None):
  cur = d
  for part in path.split("."):
    if not isinstance(cur, dict):
      return default
    cur = cur.get(part)
    if cur is None:
      return default
  return cur


def _index_plan_steps(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
  steps = _get(snapshot, "plan.steps", []) or []
  out: dict[str, dict[str, Any]] = {}
  for s in steps:
    key = str(s.get("dataset_key") or "")
    if key:
      out[key] = s
  return out


def _plan_order(snapshot: dict[str, Any]) -> list[str]:
  steps = _get(snapshot, "plan.steps", []) or []
  return [str(s.get("dataset_key")) for s in steps if s.get("dataset_key")]


def _index_outcomes(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
  results = _get(snapshot, "outcome.results", []) or []
  out: dict[str, dict[str, Any]] = {}
  for r in results:
    key = str(r.get("dataset") or "")
    if key:
      out[key] = r
  return out


def diff_execution_snapshots(*, left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
  """
  left  = baseline (older)
  right = current  (newer)
  """
  # ---- plan
  left_steps = _index_plan_steps(left)
  right_steps = _index_plan_steps(right)

  left_keys = set(left_steps.keys())
  right_keys = set(right_steps.keys())

  datasets_added = sorted(right_keys - left_keys)
  datasets_removed = sorted(left_keys - right_keys)

  left_order = _plan_order(left)
  right_order = _plan_order(right)
  order_changed = left_order != right_order

  dependency_changes = []
  for key in sorted(left_keys & right_keys):
    l_deps = tuple(left_steps[key].get("upstream_keys") or ())
    r_deps = tuple(right_steps[key].get("upstream_keys") or ())
    if l_deps != r_deps:
      dependency_changes.append({
        "dataset": key,
        "before": list(l_deps),
        "after": list(r_deps),
      })

  plan_changed = bool(datasets_added or datasets_removed or order_changed or dependency_changes)

  # ---- policy (prefer snapshot["policy"], fallback to snapshot["context"])
  policy_changed: dict[str, dict[str, Any]] = {}
  for k in ("continue_on_error", "max_retries"):
    l = _get(left, f"policy.{k}", _get(left, f"context.{k}"))
    r = _get(right, f"policy.{k}", _get(right, f"context.{k}"))
    if l != r:
      policy_changed[k] = {"before": l, "after": r}

  l_exec = _get(left, "context.execute")
  r_exec = _get(right, "context.execute")
  if l_exec != r_exec:
    policy_changed["execute"] = {"before": l_exec, "after": r_exec}

  # ---- outcomes
  left_out = _index_outcomes(left)
  right_out = _index_outcomes(right)

  status_changes = []
  kind_changes = []
  attempt_changes = []
  newly_blocked = []
  newly_aborted = []

  common = sorted(set(left_out.keys()) & set(right_out.keys()))
  for key in common:
    l = left_out[key]
    r = right_out[key]

    l_status = str(l.get("status") or "unknown")
    r_status = str(r.get("status") or "unknown")
    if l_status != r_status:
      status_changes.append({
        "dataset": key,
        "before": l_status,
        "after": r_status,
      })

    l_kind = str(l.get("kind") or "unknown")
    r_kind = str(r.get("kind") or "unknown")
    if l_kind != r_kind:
      kind_changes.append({
        "dataset": key,
        "before": l_kind,
        "after": r_kind,
      })

    l_attempt = l.get("attempt_no")
    r_attempt = r.get("attempt_no")
    if l_attempt != r_attempt and (l_attempt is not None or r_attempt is not None):
      attempt_changes.append({
        "dataset": key,
        "before": l_attempt,
        "after": r_attempt,
      })

    if r_kind == "blocked" and l_kind != "blocked":
      newly_blocked.append(key)
    if r_kind == "aborted" and l_kind != "aborted":
      newly_aborted.append(key)

  outcome_changed = bool(status_changes or kind_changes or attempt_changes or newly_blocked or newly_aborted)

  return {
    "summary": {
      "plan_changed": plan_changed,
      "policy_changed": bool(policy_changed),
      "outcome_changed": outcome_changed,
    },
    "plan": {
      "datasets_added": datasets_added,
      "datasets_removed": datasets_removed,
      "order_changed": order_changed,
      "dependency_changes": dependency_changes,
    },
    "policy": {
      "changed": policy_changed,
    },
    "outcomes": {
      "status_changes": status_changes,
      "kind_changes": kind_changes,
      "attempt_changes": attempt_changes,
      "newly_blocked": sorted(newly_blocked),
      "newly_aborted": sorted(newly_aborted),
    },
  }


def render_execution_snapshot_diff_text(
  *,
  diff: dict[str, Any],
  left_batch_run_id: str | None = None,
  right_batch_run_id: str | None = None,
) -> str:
  lines: list[str] = []

  title = "Execution snapshot diff"
  if left_batch_run_id and right_batch_run_id:
    title += f" ({left_batch_run_id} → {right_batch_run_id})"
  lines.append(title)
  lines.append("-" * len(title))

  s = diff.get("summary") or {}
  lines.append(
    f"Changed: plan={bool(s.get('plan_changed'))}, policy={bool(s.get('policy_changed'))}, outcome={bool(s.get('outcome_changed'))}"
  )

  # ---- policy
  pol = (diff.get("policy") or {}).get("changed") or {}
  if pol:
    lines.append("")
    lines.append("Policy changes:")
    for k in sorted(pol.keys()):
      before = pol[k].get("before")
      after = pol[k].get("after")
      lines.append(f"  - {k}: {before} → {after}")

  # ---- plan
  plan = diff.get("plan") or {}
  added = plan.get("datasets_added") or []
  removed = plan.get("datasets_removed") or []
  deps = plan.get("dependency_changes") or []
  if added or removed or plan.get("order_changed") or deps:
    lines.append("")
    lines.append("Plan changes:")
    if added:
      lines.append(f"  + datasets_added ({len(added)}):")
      for d in added:
        lines.append(f"    + {d}")
    if removed:
      lines.append(f"  - datasets_removed ({len(removed)}):")
      for d in removed:
        lines.append(f"    - {d}")
    if plan.get("order_changed"):
      lines.append("  * execution_order: changed")
    if deps:
      lines.append(f"  * dependency_changes ({len(deps)}):")
      for ch in deps:
        lines.append(f"    ~ {ch['dataset']}: {ch['before']} → {ch['after']}")

  # ---- outcomes
  out = diff.get("outcomes") or {}
  status_changes = out.get("status_changes") or []
  kind_changes = out.get("kind_changes") or []
  attempt_changes = out.get("attempt_changes") or []
  newly_blocked = out.get("newly_blocked") or []
  newly_aborted = out.get("newly_aborted") or []

  if status_changes or kind_changes or attempt_changes or newly_blocked or newly_aborted:
    lines.append("")
    lines.append("Outcome changes:")
    if status_changes:
      lines.append(f"  * status_changes ({len(status_changes)}):")
      for ch in status_changes:
        lines.append(f"    ~ {ch['dataset']}: {ch['before']} → {ch['after']}")
    if kind_changes:
      lines.append(f"  * kind_changes ({len(kind_changes)}):")
      for ch in kind_changes:
        lines.append(f"    ~ {ch['dataset']}: {ch['before']} → {ch['after']}")
    if attempt_changes:
      lines.append(f"  * attempt_changes ({len(attempt_changes)}):")
      for ch in attempt_changes:
        lines.append(f"    ~ {ch['dataset']}: {ch['before']} → {ch['after']}")
    if newly_blocked:
      lines.append(f"  ! newly_blocked ({len(newly_blocked)}):")
      for d in newly_blocked:
        lines.append(f"    ! {d}")
    if newly_aborted:
      lines.append(f"  ! newly_aborted ({len(newly_aborted)}):")
      for d in newly_aborted:
        lines.append(f"    ! {d}")

  if len(lines) == 3:
    lines.append("")
    lines.append("No differences detected.")

  return "\n".join(lines) + "\n"
