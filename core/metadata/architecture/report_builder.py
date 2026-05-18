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

from metadata.architecture.diff import ArchitectureDiff, diff_architecture_states
from metadata.architecture.migration_planner import MigrationPlanner
from metadata.architecture.policy_decisions import evaluate_migration_policy_decisions
from metadata.architecture.report import (
  ArchitectureChangeReport,
  ArchitectureReportScope,
  ArchitectureReportScopeMode,
  ArchitectureReportSummary,
)
from metadata.architecture.state import ArchitectureState
from metadata.materialization.policy import MaterializationPolicy


def build_architecture_change_report(
  *,
  previous_state: ArchitectureState | None,
  current_state: ArchitectureState,
  policy: MaterializationPolicy,
  relevant_dataset_keys: set[str] | None = None,
  schema_short: str | None = None,
  target_name: str | None = None,
  scope_mode: ArchitectureReportScopeMode | None = None,
) -> ArchitectureChangeReport:
  """
  Build a deterministic architecture change report.
  """
  arch_diff = diff_architecture_states(previous_state, current_state)

  scope_dataset_keys = _resolve_scope_dataset_keys(
    current_state=current_state,
    relevant_dataset_keys=relevant_dataset_keys,
  )

  scoped_diff = _scope_architecture_diff(
    arch_diff=arch_diff,
    scope_dataset_keys=(
      scope_dataset_keys
      if relevant_dataset_keys is not None
      else None
    ),
  )

  migration_plan = MigrationPlanner().plan(
    scoped_diff,
    relevant_dataset_keys=relevant_dataset_keys,
    architecture_state=current_state,
    expand_related_hist=True,
  )

  policy_decisions = evaluate_migration_policy_decisions(
    actions=tuple(migration_plan.actions),
    policy=policy,
  )

  scope = ArchitectureReportScope(
    mode=_resolve_scope_mode(
      relevant_dataset_keys=relevant_dataset_keys,
      scope_mode=scope_mode,
    ),
    schema_short=schema_short,
    target_name=target_name,
    dataset_keys=scope_dataset_keys,
  )

  summary = ArchitectureReportSummary(
    dataset_change_count=len(scoped_diff.dataset_changes),
    column_change_count=len(scoped_diff.column_changes),
    migration_action_count=len(migration_plan.actions),
    policy_decision_count=len(policy_decisions),
    blocking_policy_decision_count=sum(
      1
      for decision in policy_decisions
      if decision.is_blocking
    ),
  )

  return ArchitectureChangeReport(
    scope=scope,
    previous_fingerprint=previous_state.fingerprint if previous_state is not None else None,
    current_fingerprint=current_state.fingerprint,
    has_changes=scoped_diff.has_changes(),
    dataset_changes=tuple(scoped_diff.dataset_changes),
    column_changes=tuple(scoped_diff.column_changes),
    migration_actions=tuple(migration_plan.actions),
    policy_decisions=policy_decisions,
    summary=summary,
  )


def _resolve_scope_dataset_keys(
  *,
  current_state: ArchitectureState,
  relevant_dataset_keys: set[str] | None,
) -> tuple[str, ...]:
  """
  Resolve deterministic dataset keys for report scope.
  """
  if relevant_dataset_keys is None:
    return tuple(sorted(
      dataset.dataset_key
      for dataset in current_state.datasets
    ))

  return tuple(sorted(str(key) for key in relevant_dataset_keys))


def _scope_architecture_diff(
  *,
  arch_diff: ArchitectureDiff,
  scope_dataset_keys: tuple[str, ...] | None,
) -> ArchitectureDiff:
  """
  Restrict an architecture diff to report scope.
  """
  if scope_dataset_keys is None:
    return arch_diff

  scope = set(scope_dataset_keys)
  return ArchitectureDiff(
    dataset_changes=tuple(
      change
      for change in arch_diff.dataset_changes
      if change.dataset_key in scope
    ),
    column_changes=tuple(
      change
      for change in arch_diff.column_changes
      if change.dataset_key in scope
    ),
  )


def _resolve_scope_mode(
  *,
  relevant_dataset_keys: set[str] | None,
  scope_mode: ArchitectureReportScopeMode | None,
) -> ArchitectureReportScopeMode:
  """
  Resolve the public report scope mode.
  """
  if scope_mode in {"all", "scoped"}:
    return scope_mode

  return "all" if relevant_dataset_keys is None else "scoped"