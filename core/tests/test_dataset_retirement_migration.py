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

from types import SimpleNamespace

from metadata.architecture.diff import (
  diff_architecture_states,
)
from metadata.architecture.migration_plan import (
  MigrationAction,
)
from metadata.architecture.migration_planner import (
  MigrationPlanner,
)
from metadata.architecture.policy_decisions import (
  evaluate_migration_policy_decisions,
)
from metadata.architecture.state import (
  ArchitectureState,
  ColumnState,
  DatasetState,
)
from metadata.materialization.policy import (
  MaterializationPolicy,
)


def _policy() -> MaterializationPolicy:
  """
  Return the default restrictive materialization policy.
  """
  return MaterializationPolicy(
    sync_schema_shorts={
      "rawcore",
      "bizcore",
    },
    allow_auto_drop_columns=False,
    allow_auto_drop_hist_columns=False,
    allow_type_alter=False,
  )


def _removed_dataset_diff():
  """
  Return an ArchitectureDiff-shaped retirement test double.
  """
  return SimpleNamespace(
    dataset_changes=(
      SimpleNamespace(
        change_type="DATASET_REMOVED",
        dataset_key="raw.raw_csv_orders",
        previous_dataset_name=None,
      ),
    ),
    column_changes=(
      SimpleNamespace(
        change_type="COLUMN_REMOVED",
        dataset_key="raw.raw_csv_orders",
        column_name="order_id",
        previous_column_name=None,
      ),
      SimpleNamespace(
        change_type="COLUMN_REMOVED",
        dataset_key="raw.raw_csv_orders",
        column_name="customer_id",
        previous_column_name=None,
      ),
    ),
  )


def test_architecture_diff_reports_dataset_retirement_atomically() -> None:
  """
  Verify retirement does not repeat the former column contract.
  """
  previous = ArchitectureState(datasets=(
    DatasetState(
      dataset_key="raw.raw_csv_orders",
      schema_short_name="raw",
      dataset_name="raw_csv_orders",
      materialization_type="table",
      incremental_strategy="full",
      historize=False,
      is_hist=False,
      active=True,
      column_states=(
        ColumnState(
          column_name="order_id",
          datatype="INTEGER",
          nullable=False,
          active=True,
        ),
        ColumnState(
          column_name="customer_id",
          datatype="INTEGER",
          nullable=False,
          active=True,
        ),
      ),
    ),
  ))
  current = ArchitectureState()

  arch_diff = diff_architecture_states(previous, current)

  assert len(arch_diff.dataset_changes) == 1
  dataset_change = arch_diff.dataset_changes[0]

  assert dataset_change.change_type == "DATASET_REMOVED"
  assert dataset_change.dataset_key == "raw.raw_csv_orders"
  assert dataset_change.details == {}
  assert arch_diff.column_changes == ()

  plan = MigrationPlanner().plan(arch_diff)

  assert len(plan.actions) == 1
  assert plan.actions[0].action_type == "RETIRE_DATASET"
  assert plan.actions[0].strategy == "METADATA_ONLY"


def test_removed_dataset_becomes_metadata_only_retirement() -> None:
  """
  Verify metadata removal does not imply physical dataset or column drops.
  """
  plan = MigrationPlanner().plan(
    _removed_dataset_diff()
  )

  assert len(plan.actions) == 1

  action = plan.actions[0]

  assert action.action_type == "RETIRE_DATASET"
  assert action.strategy == "METADATA_ONLY"
  assert action.dataset_key == "raw.raw_csv_orders"
  assert (
    action.to_summary_line()
    == (
      "~ RETIRE_DATASET (METADATA_ONLY): "
      "raw.raw_csv_orders"
    )
  )
  assert all(
    item.action_type
    not in {
      "DROP_DATASET",
      "DROP_COLUMN",
    }
    for item in plan.actions
  )


def test_retired_dataset_is_allowed_as_metadata_only() -> None:
  """
  Verify Architecture Control can review retirement without destructive DDL.
  """
  plan = MigrationPlanner().plan(
    _removed_dataset_diff()
  )

  decisions = evaluate_migration_policy_decisions(
    actions=plan.actions,
    policy=_policy(),
  )

  assert len(decisions) == 1

  decision = decisions[0]

  assert decision.status == "METADATA_ONLY"
  assert (
    decision.code
    == "RETIRE_DATASET_METADATA_ONLY"
  )
  assert decision.destructive is False
  assert decision.is_blocking is False


def test_explicit_physical_dataset_drop_remains_blocked() -> None:
  """
  Verify the retirement contract does not weaken physical drop protection.
  """
  decisions = evaluate_migration_policy_decisions(
    actions=(
      MigrationAction(
        action_type="DROP_DATASET",
        strategy="DROP_TABLE",
        dataset_key="raw.raw_csv_orders",
        reason="Explicit physical deletion request.",
      ),
    ),
    policy=_policy(),
  )

  assert len(decisions) == 1

  decision = decisions[0]

  assert decision.status == "BLOCKED_BY_POLICY"
  assert decision.code == "DATASET_DROP_DISABLED"
  assert decision.destructive is True
  assert decision.is_blocking is True
