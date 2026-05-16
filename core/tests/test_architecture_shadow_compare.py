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

from metadata.architecture.migration_plan import MigrationAction
from metadata.architecture.shadow_compare import (
  build_actual_schema_op_tokens_from_plan,
  build_expected_schema_op_tokens,
  compare_schema_op_tokens,
  format_schema_op_token,
  make_schema_op_token,
  parse_schema_op_token,
  schema_op_token_for_action,
)
from metadata.materialization.plan import MaterializationPlan, MaterializationStep


def _plan(
  *,
  dataset_key: str = "rawcore.customer",
  steps: list[MaterializationStep] | None = None,
  requires_rebuild: bool = False,
) -> MaterializationPlan:
  """
  Build a materialization plan for shadow compare tests.
  """
  return MaterializationPlan(
    dataset_key=dataset_key,
    steps=steps or [],
    warnings=[],
    blocking_errors=[],
    requires_backfill=False,
    requires_rebuild=requires_rebuild,
  )


def test_schema_op_token_roundtrip_is_deterministic():
  token = make_schema_op_token(
    "ADD_COLUMN",
    ds="rawcore.customer",
    col="customer_name",
  )

  assert token == '{"col":"customer_name","ds":"rawcore.customer","op":"ADD_COLUMN"}'
  assert parse_schema_op_token(token) == {
    "op": "ADD_COLUMN",
    "ds": "rawcore.customer",
    "col": "customer_name",
  }
  assert format_schema_op_token(token) == "ADD_COLUMN rawcore.customer.customer_name"


def test_schema_op_token_for_action_builds_add_column_token():
  action = MigrationAction(
    action_type="ADD_COLUMN",
    strategy="ALTER_TABLE",
    dataset_key="rawcore.customer",
    column_name="customer_name",
  )

  token = schema_op_token_for_action(action)

  assert token == make_schema_op_token(
    "ADD_COLUMN",
    ds="rawcore.customer",
    col="customer_name",
  )


def test_schema_op_token_for_action_builds_rename_dataset_token():
  action = MigrationAction(
    action_type="RENAME_DATASET",
    strategy="RENAME_TABLE",
    dataset_key="rawcore.customer",
    previous_dataset_key="rawcore.customer_old",
  )

  token = schema_op_token_for_action(action)

  assert token == make_schema_op_token(
    "RENAME_DATASET",
    prev="rawcore.customer_old",
    cur="rawcore.customer",
  )


def test_build_expected_schema_op_tokens_filters_non_schema_actions():
  actions = (
    MigrationAction(
      action_type="RETIRE_COLUMN",
      strategy="METADATA_ONLY",
      dataset_key="rawcore.customer",
      column_name="legacy_flag",
    ),
    MigrationAction(
      action_type="ADD_COLUMN",
      strategy="ALTER_TABLE",
      dataset_key="rawcore.customer",
      column_name="customer_name",
    ),
  )

  result = build_expected_schema_op_tokens(actions=actions)

  assert result.tokens == (
    make_schema_op_token(
      "ADD_COLUMN",
      ds="rawcore.customer",
      col="customer_name",
    ),
  )


def test_build_expected_schema_op_tokens_suppresses_full_refresh_column_ops():
  actions = (
    MigrationAction(
      action_type="RENAME_COLUMN",
      strategy="RENAME_COLUMN",
      dataset_key="rawcore.customer",
      previous_column_name="name_old",
      column_name="name",
    ),
    MigrationAction(
      action_type="ADD_COLUMN",
      strategy="ALTER_TABLE",
      dataset_key="rawcore.customer",
      column_name="customer_name",
    ),
    MigrationAction(
      action_type="DROP_COLUMN",
      strategy="ALTER_TABLE",
      dataset_key="rawcore.customer",
      column_name="legacy_flag",
    ),
  )

  result = build_expected_schema_op_tokens(
    actions=actions,
    full_refresh_dataset_keys={"rawcore.customer"},
  )

  assert result.suppressed_full_refresh_col_renames == 1
  assert result.suppressed_full_refresh_add_columns == 1
  assert result.tokens == (
    make_schema_op_token(
      "DROP_COLUMN",
      ds="rawcore.customer",
      col="legacy_flag",
    ),
  )


def test_build_actual_schema_op_tokens_reads_rename_column_step():
  plan = _plan(steps=[
    MaterializationStep(
      op="RENAME_COLUMN",
      sql="ALTER TABLE rawcore.customer RENAME COLUMN name_old TO name",
      safe=True,
      reason="Rename column name_old -> name",
    ),
  ])

  tokens = build_actual_schema_op_tokens_from_plan(
    plan=plan,
    schema_short="rawcore",
  )

  assert tokens == (
    make_schema_op_token(
      "RENAME_COLUMN",
      ds="rawcore.customer",
      prev="name_old",
      cur="name",
    ),
  )


def test_build_actual_schema_op_tokens_reads_rename_dataset_step():
  plan = _plan(steps=[
    MaterializationStep(
      op="RENAME_DATASET",
      sql="ALTER TABLE rawcore.customer_old RENAME TO customer",
      safe=True,
      reason="Dataset rename: customer_old -> customer",
    ),
  ])

  tokens = build_actual_schema_op_tokens_from_plan(
    plan=plan,
    schema_short="rawcore",
  )

  assert tokens == (
    make_schema_op_token(
      "RENAME_DATASET",
      prev="rawcore.customer_old",
      cur="rawcore.customer",
    ),
  )


def test_build_actual_schema_op_tokens_reads_add_column_from_reason():
  plan = _plan(steps=[
    MaterializationStep(
      op="ADD_COLUMN",
      sql="ALTER TABLE rawcore.customer ADD COLUMN customer_name VARCHAR(64)",
      safe=True,
      reason="Column customer_name missing",
    ),
  ])

  tokens = build_actual_schema_op_tokens_from_plan(
    plan=plan,
    schema_short="rawcore",
  )

  assert tokens == (
    make_schema_op_token(
      "ADD_COLUMN",
      ds="rawcore.customer",
      col="customer_name",
    ),
  )


def test_build_actual_schema_op_tokens_reads_add_column_from_sql():
  plan = _plan(steps=[
    MaterializationStep(
      op="ADD_COLUMN",
      sql='ALTER TABLE rawcore.customer ADD COLUMN "Customer Name" VARCHAR(64)',
      safe=True,
      reason="",
    ),
  ])

  tokens = build_actual_schema_op_tokens_from_plan(
    plan=plan,
    schema_short="rawcore",
  )

  assert tokens == (
    make_schema_op_token(
      "ADD_COLUMN",
      ds="rawcore.customer",
      col="Customer Name",
    ),
  )


def test_build_actual_schema_op_tokens_reads_alter_column_step():
  plan = _plan(steps=[
    MaterializationStep(
      op="ALTER_COLUMN_TYPE",
      sql="ALTER TABLE rawcore.customer ALTER COLUMN due_date TYPE TIMESTAMP",
      safe=True,
      reason="alter due_date to <dialect_type>",
    ),
  ])

  tokens = build_actual_schema_op_tokens_from_plan(
    plan=plan,
    schema_short="rawcore",
  )

  assert tokens == (
    make_schema_op_token(
      "ALTER_COLUMN",
      ds="rawcore.customer",
      col="due_date",
    ),
  )


def test_build_actual_schema_op_tokens_reads_drop_column_step():
  plan = _plan(steps=[
    MaterializationStep(
      op="DROP_COLUMN",
      sql="ALTER TABLE rawcore.customer DROP COLUMN legacy_flag",
      safe=True,
      reason="Drop column rawcore.customer.legacy_flag",
    ),
  ])

  tokens = build_actual_schema_op_tokens_from_plan(
    plan=plan,
    schema_short="rawcore",
  )

  assert tokens == (
    make_schema_op_token(
      "DROP_COLUMN",
      ds="rawcore.customer",
      col="legacy_flag",
    ),
  )


def test_build_actual_schema_op_tokens_adds_rebuild_token():
  plan = _plan(requires_rebuild=True)

  tokens = build_actual_schema_op_tokens_from_plan(
    plan=plan,
    schema_short="rawcore",
  )

  assert tokens == (
    make_schema_op_token(
      "REBUILD_DATASET",
      ds="rawcore.customer",
    ),
  )


def test_compare_schema_op_tokens_detects_missing_token():
  expected = [
    make_schema_op_token(
      "ALTER_COLUMN",
      ds="rawcore.customer",
      col="due_date",
    ),
  ]
  actual = []

  result = compare_schema_op_tokens(
    expected=expected,
    actual=actual,
  )

  assert result.is_mismatch is True
  assert result.missing == tuple(expected)
  assert result.unexpected == ()


def test_compare_schema_op_tokens_suppresses_column_ops_when_rebuild_exists():
  expected = [
    make_schema_op_token(
      "ALTER_COLUMN",
      ds="rawcore.customer",
      col="due_date",
    ),
  ]
  actual = [
    make_schema_op_token(
      "REBUILD_DATASET",
      ds="rawcore.customer",
    ),
  ]

  result = compare_schema_op_tokens(
    expected=expected,
    actual=actual,
  )

  assert result.is_mismatch is False
  assert result.missing == ()
  assert result.unexpected == ()
  assert result.suppressed_by_rebuild == tuple(expected)
  assert result.suppressed_rebuild_steps == tuple(actual)


def test_compare_schema_op_tokens_suppresses_hist_drop_when_not_allowed():
  expected = [
    make_schema_op_token(
      "DROP_COLUMN",
      ds="rawcore.customer_hist",
      col="legacy_flag",
    ),
  ]
  actual = []

  result = compare_schema_op_tokens(
    expected=expected,
    actual=actual,
    allow_hist_drop=False,
  )

  assert result.is_mismatch is False
  assert result.missing == ()
  assert result.suppressed_hist_drop_columns == tuple(expected)


def test_compare_schema_op_tokens_keeps_hist_drop_when_allowed():
  expected = [
    make_schema_op_token(
      "DROP_COLUMN",
      ds="rawcore.customer_hist",
      col="legacy_flag",
    ),
  ]
  actual = []

  result = compare_schema_op_tokens(
    expected=expected,
    actual=actual,
    allow_hist_drop=True,
  )

  assert result.is_mismatch is True
  assert result.missing == tuple(expected)
  assert result.suppressed_hist_drop_columns == ()