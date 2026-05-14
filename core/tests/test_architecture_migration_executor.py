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
import types
import uuid

import pytest

from tests._dialect_test_mixin import DialectTestMixin


@dataclass
class _Action:
  action_type: str
  dataset_key: str
  column_name: str | None = None
  previous_column_name: str | None = None
  previous_dataset_key: str | None = None


def _mp(actions: list[_Action]):
  return types.SimpleNamespace(actions=actions)


def _policy(*, allow_drop: bool, allow_alter: bool, allow_hist_drop: bool = False):
  """
  Build a materialization policy for migration-executor tests.
  """
  from metadata.materialization.policy import MaterializationPolicy

  return MaterializationPolicy(
    sync_schema_shorts={"rawcore"},
    allow_auto_drop_columns=allow_drop,
    allow_type_alter=allow_alter,
    allow_auto_drop_hist_columns=allow_hist_drop,
    debug_plan=False,
  )


class BaseDummyDialect(DialectTestMixin):
  """
  Minimal dialect double used by migration-executor tests.
  Implements only the render_* hooks that build_materialization_from_migration_plan calls.
  """

  def introspect_table(self, *, schema_name, table_name, introspection_engine, exec_engine=None, debug_plan=False):
    # Default: table exists, no physical cols required.
    return {"table_exists": True, "actual_cols_by_norm_name": {}}

  def render_rename_table(self, schema, old, new):
    return f"RENAME_TABLE {schema}.{old} -> {schema}.{new}"

  def render_rename_column(self, schema, table, old, new):
    return f"RENAME_COLUMN {schema}.{table}.{old} -> {new}"

  def render_alter_column_type(self, *, schema, table, column, new_type, old_type=None):
    return f"ALTER_COLUMN {schema}.{table}.{column} {old_type} -> {new_type}"

  def render_add_column(self, schema, table, column, new_type):
    return f"ADD_COLUMN {schema}.{table}.{column} {new_type}"

  def render_drop_column(self, schema, table, column):
    return f"DROP_COLUMN {schema}.{table}.{column}"

  # Rebuild helpers
  def render_drop_table_if_exists(self, *, schema, table, cascade=False):
    return f"DROP_IF_EXISTS {schema}.{table}"

  def render_create_table_from_columns(self, *, schema, table, columns):
    return f"CREATE_TABLE {schema}.{table}"

  def render_insert_select_for_rebuild(self, *, schema, src_table, dst_table, columns, lossy_casts, truncate_strings):
    return f"INSERT_SELECT {schema}.{src_table} -> {schema}.{dst_table}"

  def render_drop_table(self, *, schema, table, cascade=False):
    return f"DROP_TABLE {schema}.{table}"


@pytest.mark.django_db
def test_migration_executor_falls_back_to_rebuild_when_alter_sql_missing():
  from metadata.materialization.migration_executor import build_materialization_from_migration_plan
  from metadata.models import TargetDataset, TargetSchema, TargetColumn

  schema = TargetSchema.objects.get_or_create(short_name="rawcore", schema_name="rawcore")[0]
  td = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=f"rc_mig_exec_{uuid.uuid4().hex[:8]}",
    incremental_strategy="full",
    is_system_managed=False,
  )
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="due_date",
    datatype="DATE",
    active=True,
    ordinal_position=1,
    is_system_managed=False,
  )

  ds_key = f"{schema.short_name}.{td.target_dataset_name}"
  mp = _mp([
    _Action(action_type="ALTER_COLUMN", dataset_key=ds_key, column_name="due_date"),
  ])

  class DummyDialect(BaseDummyDialect):
    def introspect_table(self, *, schema_name, table_name, introspection_engine, exec_engine=None, debug_plan=False):
      return {
        "table_exists": True,
        "actual_cols_by_norm_name": {"due_date": {"name": "due_date", "type": "DATE"}},
      }

    def render_alter_column_type(self, *, schema, table, column, new_type, old_type=None):
      # Simulate: dialect cannot safely render this ALTER -> executor must rebuild
      return None

  dialect = DummyDialect()

  policy = _policy(allow_drop=False, allow_alter=True)

  res = build_materialization_from_migration_plan(
    td=td,
    dataset_key=ds_key,
    migration_plan=mp,
    dialect=dialect,
    policy=policy,
    introspection_engine=object(),
    exec_engine=None,
    is_full_refresh=False,
  )

  ops = [s.op for s in (res.steps or [])]
  safes = [bool(getattr(s, "safe", None)) for s in (res.steps or [])]
  assert res.requires_rebuild is True
  assert ops == ["DROP_TABLE_IF_EXISTS", "CREATE_TABLE", "INSERT_SELECT", "DROP_TABLE", "RENAME_TABLE"]
  assert safes == [True, True, True, True, True]


@pytest.mark.django_db
def test_migration_executor_orders_rename_before_add_column():
  from metadata.materialization.migration_executor import build_materialization_from_migration_plan
  from metadata.models import TargetDataset, TargetSchema, TargetColumn

  schema = TargetSchema.objects.get_or_create(short_name="rawcore", schema_name="rawcore")[0]
  td = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=f"rc_mig_exec_{uuid.uuid4().hex[:8]}",
    incremental_strategy="full",
    is_system_managed=False,
  )
  # Metadata already contains the *new* column name (after rename)
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="new_col",
    datatype="STRING",
    active=True,
    ordinal_position=1,
    is_system_managed=False,
  )
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="added_col",
    datatype="INTEGER",
    active=True,
    ordinal_position=2,
    is_system_managed=False,
  )

  ds_key = f"{schema.short_name}.{td.target_dataset_name}"
  mp = _mp([
    _Action(action_type="RENAME_COLUMN", dataset_key=ds_key, column_name="new_col", previous_column_name="old_col"),
    _Action(action_type="ADD_COLUMN", dataset_key=ds_key, column_name="added_col"),
  ])

  class DummyDialect(BaseDummyDialect):
    pass

  dialect = DummyDialect()

  policy = _policy(allow_drop=False, allow_alter=False)

  res = build_materialization_from_migration_plan(
    td=td,
    dataset_key=ds_key,
    migration_plan=mp,
    dialect=dialect,
    policy=policy,
    introspection_engine=object(),
    exec_engine=None,
    is_full_refresh=False,
  )

  ops = [s.op for s in (res.steps or [])]
  assert res.requires_rebuild is False
  assert ops == ["RENAME_COLUMN", "ADD_COLUMN"]

@pytest.mark.django_db
def test_migration_executor_orders_full_chain_non_rebuild():
  """
  Explicit ordering contract (non-rebuild):
    RENAME_DATASET -> RENAME_COLUMN -> ALTER_COLUMN_TYPE -> ADD_COLUMN -> DROP_COLUMN
  The input action order should not matter for the category ordering.
  """
  from metadata.materialization.migration_executor import build_materialization_from_migration_plan
  from metadata.models import TargetDataset, TargetSchema, TargetColumn

  schema = TargetSchema.objects.get_or_create(short_name="rawcore", schema_name="rawcore")[0]
  td = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=f"rc_chain_new_{uuid.uuid4().hex[:6]}",
    incremental_strategy="full",
    is_system_managed=False,
  )
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="new_col",
    datatype="STRING",
    active=True,
    ordinal_position=1,
    is_system_managed=False,
  )
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="due_date",
    datatype="DATE",
    active=True,
    ordinal_position=2,
    is_system_managed=False,
  )
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="added_col",
    datatype="INTEGER",
    active=True,
    ordinal_position=3,
    is_system_managed=False,
  )

  ds_key = f"{schema.short_name}.{td.target_dataset_name}"
  mp = _mp([
    # Deliberately shuffled:
    _Action(action_type="DROP_COLUMN", dataset_key=ds_key, column_name="drop_me"),
    _Action(action_type="ADD_COLUMN", dataset_key=ds_key, column_name="added_col"),
    _Action(action_type="ALTER_COLUMN", dataset_key=ds_key, column_name="due_date"),
    _Action(action_type="RENAME_COLUMN", dataset_key=ds_key, column_name="new_col", previous_column_name="old_col"),
    _Action(action_type="RENAME_DATASET", dataset_key=ds_key, previous_dataset_key="rawcore.rc_chain_old"),
  ])

  class DummyDialect(BaseDummyDialect):
    def introspect_table(self, *, schema_name, table_name, introspection_engine, exec_engine=None, debug_plan=False):
      return {
        "table_exists": True,
        "actual_cols_by_norm_name": {"due_date": {"name": "due_date", "type": "TIMESTAMP"}},
      }

  dialect = DummyDialect()
  policy = _policy(allow_drop=True, allow_alter=True)

  res = build_materialization_from_migration_plan(
    td=td,
    dataset_key=ds_key,
    migration_plan=mp,
    dialect=dialect,
    policy=policy,
    introspection_engine=object(),
    exec_engine=None,
    is_full_refresh=False,
  )

  ops = [s.op for s in (res.steps or [])]
  safes = [bool(getattr(s, "safe", None)) for s in (res.steps or [])]
  assert res.requires_rebuild is False
  assert ops == ["RENAME_DATASET", "RENAME_COLUMN", "ALTER_COLUMN_TYPE", "ADD_COLUMN", "DROP_COLUMN"]
  assert safes == [True, True, True, True, True]


@pytest.mark.django_db
def test_migration_executor_full_refresh_keeps_only_dataset_rename():
  """
  Full refresh (recreate) rule:
  - only dataset rename is emitted as a step
  - column-level ops are intentionally not executed as DDL steps
  """
  from metadata.materialization.migration_executor import build_materialization_from_migration_plan
  from metadata.models import TargetDataset, TargetSchema, TargetColumn

  schema = TargetSchema.objects.get_or_create(short_name="rawcore", schema_name="rawcore")[0]
  td = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=f"rc_fr_new_{uuid.uuid4().hex[:6]}",
    incremental_strategy="full",
    is_system_managed=False,
  )
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="c1",
    datatype="STRING",
    active=True,
    ordinal_position=1,
    is_system_managed=False,
  )

  ds_key = f"{schema.short_name}.{td.target_dataset_name}"
  mp = _mp([
    _Action(action_type="ADD_COLUMN", dataset_key=ds_key, column_name="c1"),
    _Action(action_type="RENAME_DATASET", dataset_key=ds_key, previous_dataset_key="rawcore.rc_fr_old"),
    _Action(action_type="DROP_COLUMN", dataset_key=ds_key, column_name="x"),
  ])

  dialect = BaseDummyDialect()
  policy = _policy(allow_drop=True, allow_alter=True)

  res = build_materialization_from_migration_plan(
    td=td,
    dataset_key=ds_key,
    migration_plan=mp,
    dialect=dialect,
    policy=policy,
    introspection_engine=object(),
    exec_engine=None,
    is_full_refresh=True,
  )

  ops = [s.op for s in (res.steps or [])]
  assert ops == ["RENAME_DATASET"]

@pytest.mark.django_db
def test_migration_executor_blocks_drop_column_when_policy_disallows_auto_drop():
  """
  Policy gate:
  - If MigrationPlan contains DROP_COLUMN but policy disallows auto-drop,
    the executor must surface a blocking error deterministically.
  """
  from metadata.materialization.migration_executor import build_materialization_from_migration_plan
  from metadata.models import TargetDataset, TargetSchema, TargetColumn

  schema = TargetSchema.objects.get_or_create(short_name="rawcore", schema_name="rawcore")[0]
  td = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=f"rc_drop_gate_{uuid.uuid4().hex[:6]}",
    incremental_strategy="full",
    is_system_managed=False,
  )
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="keep_me",
    datatype="STRING",
    active=True,
    ordinal_position=1,
    is_system_managed=False,
  )

  ds_key = f"{schema.short_name}.{td.target_dataset_name}"
  mp = _mp([
    _Action(action_type="DROP_COLUMN", dataset_key=ds_key, column_name="drop_me"),
  ])

  dialect = BaseDummyDialect()
  policy = _policy(allow_drop=False, allow_alter=False)

  res = build_materialization_from_migration_plan(
    td=td,
    dataset_key=ds_key,
    migration_plan=mp,
    dialect=dialect,
    policy=policy,
    introspection_engine=object(),
    exec_engine=None,
    is_full_refresh=False,
  )

  assert res.blocking_errors, "Expected policy gate to produce blocking_errors"
  msg = "\n".join(res.blocking_errors)
  assert "POLICY_AUTO_DROP_DISABLED" in msg

@pytest.mark.django_db
def test_migration_executor_emits_drop_column_step_when_policy_allows_auto_drop():
  """
  Policy gate:
  - If policy allows auto-drop, DROP_COLUMN intent becomes a DROP_COLUMN step.
  - DROP_COLUMN is destructive, but policy-approved -> safe=True.
  """
  from metadata.materialization.migration_executor import build_materialization_from_migration_plan
  from metadata.models import TargetDataset, TargetSchema, TargetColumn

  schema = TargetSchema.objects.get_or_create(short_name="rawcore", schema_name="rawcore")[0]
  td = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=f"rc_drop_ok_{uuid.uuid4().hex[:6]}",
    incremental_strategy="full",
    is_system_managed=False,
  )
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="keep_me",
    datatype="STRING",
    active=True,
    ordinal_position=1,
    is_system_managed=False,
  )

  ds_key = f"{schema.short_name}.{td.target_dataset_name}"
  mp = _mp([
    _Action(action_type="DROP_COLUMN", dataset_key=ds_key, column_name="drop_me"),
  ])

  dialect = BaseDummyDialect()
  policy = _policy(allow_drop=True, allow_alter=False)

  res = build_materialization_from_migration_plan(
    td=td,
    dataset_key=ds_key,
    migration_plan=mp,
    dialect=dialect,
    policy=policy,
    introspection_engine=object(),
    exec_engine=None,
    is_full_refresh=False,
  )

  assert not res.blocking_errors
  ops = [s.op for s in (res.steps or [])]
  assert ops == ["DROP_COLUMN"]
  assert res.steps[0].safe is True
