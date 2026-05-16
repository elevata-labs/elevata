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

from metadata.architecture.diff import diff_architecture_states
from metadata.architecture.migration_planner import MigrationPlanner
from metadata.architecture.policy_decisions import evaluate_migration_policy_decisions
from metadata.architecture.report import (
  ArchitectureChangeReport,
  ArchitectureReportScope,
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
) -> ArchitectureChangeReport:
  """
  Build a deterministic architecture change report.
  """
  arch_diff = diff_architecture_states(previous_state, current_state)

  migration_plan = MigrationPlanner().plan(
    arch_diff,
    relevant_dataset_keys=relevant_dataset_keys,
    architecture_state=current_state,
    expand_related_hist=True,
  )

  policy_decisions = evaluate_migration_policy_decisions(
    actions=tuple(migration_plan.actions),
    policy=policy,
  )

  scope_dataset_keys = _resolve_scope_dataset_keys(
    current_state=current_state,
    relevant_dataset_keys=relevant_dataset_keys,
  )

  scope = ArchitectureReportScope(
    mode="all" if relevant_dataset_keys is None else "scoped",
    schema_short=schema_short,
    target_name=target_name,
    dataset_keys=scope_dataset_keys,
  )

  summary = ArchitectureReportSummary(
    dataset_change_count=len(arch_diff.dataset_changes),
    column_change_count=len(arch_diff.column_changes),
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
    has_changes=arch_diff.has_changes(),
    dataset_changes=tuple(arch_diff.dataset_changes),
    column_changes=tuple(arch_diff.column_changes),
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