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

import types
from types import SimpleNamespace
import pytest
from django.core.management.base import CommandError

from metadata.materialization.plan import MaterializationPlan, MaterializationStep
from metadata.models import TargetColumn, TargetDataset, TargetSchema
from tests._dialect_test_mixin import DialectTestMixin


class DummyStdout:
  def write(self, _msg):
    return None


class DummyStyle:
  def NOTICE(self, s):  # noqa: N802
    return s

  def WARNING(self, s):  # noqa: N802
    return s

  def ERROR(self, s):  # noqa: N802
    return s


class DummyExecEngine:
  def execute(self, _sql, _params=None):
    return 0

  def fetch_all(self, _sql, _params=None):
    return []

  def execute_scalar(self, _sql, _params=None):
    return None


@pytest.mark.django_db
def test_incremental_merge_calls_ensure_target_table_when_plan_only_ensures_schema(monkeypatch):
  """
  Regression:
  If build_materialization_plan returns only ENSURE_SCHEMA, this must NOT count as provisioning.
  For incremental/merge, ensure_target_table() must still be called so MERGE/DELETE can run.
  """
  from metadata.management.commands import elevata_load as mod

  # --- Ensure rawcore schema exists (idempotent) ------------------------------
  schema, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={
      "display_name": "Raw Core",
      "schema_name": "rawcore",
    },
  )
  # keep schema_name stable for tests
  if schema.schema_name != "rawcore":
    schema.schema_name = "rawcore"
    schema.save(update_fields=["schema_name"])

  # --- Create or reuse a TargetDataset --------------------------------------
  td, _ = TargetDataset.objects.get_or_create(
    target_schema=schema,
    target_dataset_name="rc_aw_salesorderheader",
    defaults={
      "materialization_type": "incremental",
      "historize": False,        # important: avoids hist-sync ORM calls
      "handle_deletes": True,
    },
  )
  # ensure desired flags
  update_fields = []
  if td.materialization_type != "incremental":
    td.materialization_type = "incremental"
    update_fields.append("materialization_type")
  if td.historize:
    td.historize = False
    update_fields.append("historize")
  if not td.handle_deletes:
    td.handle_deletes = True
    update_fields.append("handle_deletes")
  if update_fields:
    td.save(update_fields=update_fields)

  # --- Patch policy so rawcore gets materialized -----------------------------
  from metadata.materialization.policy import MaterializationPolicy

  monkeypatch.setattr(
    mod,
    "load_materialization_policy",
    lambda: MaterializationPolicy(
      sync_schema_shorts={"rawcore", "bizcore"},
      allow_auto_drop_columns=False,
      allow_type_alter=False,
    ),
    raising=False,
  )
  monkeypatch.setattr(mod, "AUTO_PROVISION_TABLES", True, raising=False)

  # --- Build plan that contains ONLY ENSURE_SCHEMA ---------------------------
  plan = MaterializationPlan(
    dataset_key="rawcore.rc_aw_salesorderheader",
    steps=[MaterializationStep(op="ENSURE_SCHEMA", sql="CREATE SCHEMA IF NOT EXISTS rawcore;", safe=True, reason="")],
    warnings=[],
    blocking_errors=[],
  )
  monkeypatch.setattr(mod, "build_materialization_plan", lambda **_kw: plan, raising=False)
  monkeypatch.setattr(mod, "apply_materialization_plan", lambda **_kw: None, raising=False)

  # Force incremental/merge semantics
  monkeypatch.setattr(
    mod,
    "build_load_plan",
    lambda _td: types.SimpleNamespace(
      mode="merge",
      handle_deletes=True,
      incremental_source="dummy_src",
      delete_detection_enabled=True,
    ),
    raising=False,
  )

  # Make sure we do NOT go into full-refresh/truncate path
  monkeypatch.setattr(mod, "should_truncate_before_load", lambda _td, _lp: False, raising=False)

  # Keep summary consistent (optional but stabilizes log/branches)
  monkeypatch.setattr(
    mod,
    "build_load_run_summary",
    lambda _td, _dialect, _lp: {
      "mode": "merge",
      "handle_deletes": True,
      "historize": False,
    },
    raising=False,
  )

  # --- Spy ensure_target_table ----------------------------------------------
  calls = {"ensure": 0}

  def spy_ensure_target_table(engine, dialect, td, auto_provision, **_kwargs):
    calls["ensure"] += 1

  monkeypatch.setattr(mod, "ensure_target_table", spy_ensure_target_table, raising=False)

  # --- Avoid creating real SQLAlchemy engines / reflection -------------------
  monkeypatch.setattr(
    mod,
    "engine_for_target",
    lambda **_kw: types.SimpleNamespace(
      url=types.SimpleNamespace(database=":memory:"),
      dialect=types.SimpleNamespace(name="duckdb"),
      dispose=lambda: None,
    ),
    raising=False,
  )

  # --- Avoid real SQL rendering / delta cutoff logic -------------------------
  monkeypatch.setattr(
    mod,
    "render_load_sql_for_target",
    lambda _td, _dialect: "SELECT 1;",
    raising=False,
  )

  # --- Execute --------------------------------------------------------------
  stdout = DummyStdout()
  style = DummyStyle()
  exec_engine = DummyExecEngine()
  target_system = types.SimpleNamespace(short_name="dwh", type="duckdb")
  profile = types.SimpleNamespace(name="test_profile")
  dialect = DummyDialect()

  res = mod.run_single_target_dataset(
    stdout=stdout,
    style=style,
    target_dataset=td,
    target_system=target_system,
    target_system_engine=exec_engine,
    profile=profile,
    dialect=dialect,
    execute=True,
    no_print=True,
    debug_plan=False,
    batch_run_id="batch",
    load_run_id="load",
    load_plan_override=None,
    migration_plan=SimpleNamespace(actions=[]),
  )

  assert res["status"] == "success"
  assert calls["ensure"] == 1


@pytest.mark.django_db
def test_full_rebuild_impact_creates_new_merge_target_without_add_column_ddl(monkeypatch):
  """
  Regression:
  A newly added merge dataset receives FULL_REBUILD from Architecture Control.
  Runtime must create the complete table and execute full-refresh SQL instead of
  applying per-column ADD_COLUMN steps to a missing relation.
  """
  from metadata.management.commands import elevata_load as mod
  from metadata.materialization.policy import MaterializationPolicy
  from metadata.rendering.load_planner import LoadPlan

  schema, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={
      "display_name": "Raw Core",
      "schema_name": "rawcore",
      "default_materialization_type": "table",
    },
  )
  schema.schema_name = "rawcore"
  schema.default_materialization_type = "table"
  schema.save(update_fields=["schema_name", "default_materialization_type"])

  td = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name="rc_initial_merge_runtime_binding",
    materialization_type=None,
    incremental_strategy="merge",
    historize=True,
    handle_deletes=True,
  )
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="order_id",
    ordinal_position=1,
    datatype="INTEGER",
    nullable=False,
  )

  monkeypatch.setattr(
    mod,
    "load_materialization_policy",
    lambda: MaterializationPolicy(
      sync_schema_shorts={"rawcore"},
      allow_auto_drop_columns=False,
      allow_type_alter=False,
    ),
    raising=False,
  )
  monkeypatch.setattr(mod, "AUTO_PROVISION_TABLES", True, raising=False)
  monkeypatch.setattr(
    mod,
    "engine_for_target",
    lambda **_kw: types.SimpleNamespace(dispose=lambda: None),
    raising=False,
  )
  monkeypatch.setattr(
    mod,
    "build_load_plan",
    lambda _td: LoadPlan(
      mode="merge",
      handle_deletes=True,
      historize=True,
    ),
    raising=False,
  )

  base_plan = MaterializationPlan(
    dataset_key="rawcore.rc_initial_merge_runtime_binding",
    steps=[
      MaterializationStep(
        op="ENSURE_SCHEMA",
        sql="CREATE SCHEMA rawcore;",
        safe=True,
        reason="Ensure schema",
      ),
    ],
    warnings=[],
    blocking_errors=[],
  )
  monkeypatch.setattr(
    mod,
    "build_materialization_plan",
    lambda **_kw: base_plan,
    raising=False,
  )

  migration_calls = []

  def fake_build_materialization_from_migration_plan(**kwargs):
    migration_calls.append(bool(kwargs["is_full_refresh"]))
    return SimpleNamespace(
      steps=[
        MaterializationStep(
          op="ADD_COLUMN",
          sql=(
            "ALTER TABLE rawcore.rc_initial_merge_runtime_binding "
            "ADD order_id INT;"
          ),
          safe=True,
          reason="Column order_id missing",
        ),
      ],
      warnings=[],
      blocking_errors=[],
      requires_rebuild=False,
    )

  monkeypatch.setattr(
    mod,
    "build_materialization_from_migration_plan",
    fake_build_materialization_from_migration_plan,
    raising=False,
  )

  applied_ops = []

  def fake_apply_materialization_plan(*, plan, exec_engine, **_kwargs):
    applied_ops.extend(step.op for step in plan.steps)

  monkeypatch.setattr(
    mod,
    "apply_materialization_plan",
    fake_apply_materialization_plan,
    raising=False,
  )
  monkeypatch.setattr(
    mod,
    "drop_managed_dependent_views_before_full_refresh",
    lambda **_kw: None,
    raising=False,
  )
  monkeypatch.setattr(
    mod,
    "validate_physical_schema_before_load",
    lambda **_kw: None,
    raising=False,
  )
  monkeypatch.setattr(
    mod,
    "_ensure_target_schema_for_batch",
    lambda **_kw: None,
    raising=False,
  )
  monkeypatch.setattr(
    mod,
    "_ensure_load_run_log_table_for_batch",
    lambda **_kw: None,
    raising=False,
  )

  rendered_plans = []

  def fake_render_load_sql_for_target(
    _td,
    _dialect,
    load_plan_override=None,
  ):
    rendered_plans.append(load_plan_override)
    return "INSERT FULL REFRESH;"

  monkeypatch.setattr(
    mod,
    "render_load_sql_for_target",
    fake_render_load_sql_for_target,
    raising=False,
  )
  monkeypatch.setattr(
    mod,
    "render_controlled_reference_member_sql_for_target",
    lambda _td, _dialect: [],
    raising=False,
  )
  monkeypatch.setattr(
    mod,
    "apply_runtime_placeholders",
    lambda sql, **_kwargs: sql,
    raising=False,
  )

  class RecordingExecEngine(DummyExecEngine):
    def __init__(self):
      self.sql = []

    def execute(self, sql, _params=None):
      self.sql.append(sql)
      return 1

  class FullRebuildDialect:
    DIALECT_NAME = "mssql"

    def render_drop_table_if_exists(self, *, schema, table, cascade=False):
      return f"DROP TABLE IF EXISTS {schema}.{table};"

  def fake_ensure_target_table(engine, dialect, td, auto_provision, **_kwargs):
    engine.execute(
      f"CREATE TABLE {td.target_schema.schema_name}."
      f"{td.target_dataset_name} (order_id INT);"
    )

  monkeypatch.setattr(
    mod,
    "ensure_target_table",
    fake_ensure_target_table,
    raising=False,
  )

  exec_engine = RecordingExecEngine()
  result = mod.run_single_target_dataset(
    stdout=DummyStdout(),
    style=DummyStyle(),
    target_dataset=td,
    target_system=types.SimpleNamespace(short_name="dwh", type="mssql"),
    target_system_engine=exec_engine,
    profile=types.SimpleNamespace(name="test_profile"),
    dialect=FullRebuildDialect(),
    execute=True,
    no_print=True,
    debug_plan=False,
    batch_run_id="batch",
    load_run_id="load",
    load_plan_override=None,
    impact_decision="FULL_REBUILD",
    migration_plan=SimpleNamespace(actions=[]),
    execution_dataset_keys={"rawcore.rc_initial_merge_runtime_binding"},
  )

  assert result["status"] == "success"
  assert result["summary"]["mode"] == "full"
  assert result["summary"]["handle_deletes"] is False
  assert result["summary"]["historize"] is True
  assert migration_calls == [True]
  assert applied_ops == ["ENSURE_SCHEMA"]
  assert len(rendered_plans) == 1
  assert rendered_plans[0].mode == "full"
  assert rendered_plans[0].handle_deletes is False
  assert exec_engine.sql == [
    "DROP TABLE IF EXISTS rawcore.rc_initial_merge_runtime_binding;",
    "CREATE TABLE rawcore.rc_initial_merge_runtime_binding (order_id INT);",
    "INSERT FULL REFRESH;",
  ]


class DummyDialect(DialectTestMixin):
  pass

def test_render_insert_select_for_rebuild_uses_source_name_and_truncate_marker():
  dialect = DummyDialect()
  sql = dialect.render_insert_select_for_rebuild(
    schema="rawcore",
    src_table="t_src",
    dst_table="t_dst",
    lossy_casts=True,
    truncate_strings=True,
    columns=[
      {
        "name": "c1",
        "source_name": "c1_old",
        "type": "STRING",
        "truncate_to_length": 10,
      },
    ],
  )
  # Source column should be used
  assert "c1_old" in sql
  # Truncation should use explicit truncate marker (10)
  assert "LEFT(" in sql
  assert ", 10)" in sql
  # Destination column name should still be c1
  assert " AS c1" in sql


@pytest.mark.django_db
def test_physical_schema_drift_blocks_before_load_sql_execution(monkeypatch):
  """
  Regression:
  If recorded architecture state says there are no migration actions, but the
  physical table is missing active target columns, the load must fail closed
  before generated load SQL is executed.
  """
  from metadata.management.commands import elevata_load as mod

  schema, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={
      "display_name": "Raw Core",
      "schema_name": "rawcore",
    },
  )
  if schema.schema_name != "rawcore":
    schema.schema_name = "rawcore"
    schema.save(update_fields=["schema_name"])

  td, _ = TargetDataset.objects.get_or_create(
    target_schema=schema,
    target_dataset_name="rc_test_hist",
    defaults={
      "materialization_type": "incremental",
      "historize": False,
      "handle_deletes": False,
    },
  )
  TargetColumn.objects.filter(target_dataset=td).delete()
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="rc_test_hist_key",
    ordinal_position=1,
    datatype="STRING",
    max_length=64,
    nullable=False,
  )
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="inferred_member",
    ordinal_position=2,
    datatype="BOOLEAN",
    nullable=True,
  )

  from metadata.materialization.policy import MaterializationPolicy

  monkeypatch.setattr(
    mod,
    "load_materialization_policy",
    lambda: MaterializationPolicy(
      sync_schema_shorts={"rawcore", "bizcore"},
      allow_auto_drop_columns=False,
      allow_type_alter=False,
    ),
    raising=False,
  )
  monkeypatch.setattr(mod, "AUTO_PROVISION_TABLES", True, raising=False)
  monkeypatch.setattr(mod, "ensure_target_schema", lambda **_kw: None, raising=False)
  monkeypatch.setattr(mod, "ensure_load_run_log_table", lambda **_kw: None, raising=False)

  plan = MaterializationPlan(
    dataset_key="rawcore.rc_test_hist",
    steps=[MaterializationStep(op="ENSURE_SCHEMA", sql="CREATE SCHEMA rawcore;", safe=True, reason="")],
    warnings=[],
    blocking_errors=[],
  )
  monkeypatch.setattr(mod, "build_materialization_plan", lambda **_kw: plan, raising=False)
  monkeypatch.setattr(mod, "apply_materialization_plan", lambda **_kw: None, raising=False)

  monkeypatch.setattr(
    mod,
    "build_load_plan",
    lambda _td: types.SimpleNamespace(
      mode="historize",
      handle_deletes=False,
      incremental_source=None,
      delete_detection_enabled=False,
    ),
    raising=False,
  )
  monkeypatch.setattr(mod, "should_truncate_before_load", lambda _td, _lp: False, raising=False)
  monkeypatch.setattr(
    mod,
    "build_load_run_summary",
    lambda _td, _dialect, _lp: {
      "mode": "historize",
      "handle_deletes": False,
      "historize": False,
    },
    raising=False,
  )
  monkeypatch.setattr(mod, "render_load_sql_for_target", lambda _td, _dialect: "SELECT 1;", raising=False)
  monkeypatch.setattr(
    mod,
    "engine_for_target",
    lambda **_kw: types.SimpleNamespace(
      url=types.SimpleNamespace(database=":memory:"),
      dispose=lambda: None,
    ),
    raising=False,
  )

  class DriftDialect(DummyDialect):
    def introspect_table(self, **_kwargs):
      return {
        "table_exists": True,
        "physical_table": "rc_test_hist",
        "actual_cols_by_norm_name": {
          "rc_test_hist_key": {"name": "rc_test_hist_key", "type": "STRING"},
        },
      }

  class FailingExecEngine(DummyExecEngine):
    def execute(self, sql, _params=None):
      raise AssertionError(f"Load SQL must not execute after drift guard failure: {sql}")

  with pytest.raises(CommandError, match="Physical schema drift detected.*inferred_member"):
    mod.run_single_target_dataset(
      stdout=DummyStdout(),
      style=DummyStyle(),
      target_dataset=td,
      target_system=types.SimpleNamespace(short_name="dwh", type="duckdb"),
      target_system_engine=FailingExecEngine(),
      profile=types.SimpleNamespace(name="test_profile"),
      dialect=DriftDialect(),
      execute=True,
      no_print=True,
      debug_plan=False,
      batch_run_id="batch",
      load_run_id="load",
      load_plan_override=None,
      migration_plan=SimpleNamespace(actions=[]),
    )


@pytest.mark.django_db
def test_migration_materialization_skips_add_column_when_physical_column_already_exists():
  """
  Regression:
  A MigrationPlan can be applied to a hist table twice in one full execution:
  once through companion hist sync from the base rawcore dataset, and once when
  the hist dataset itself runs. ADD_COLUMN must therefore be idempotent when
  the physical column has already been applied.
  """
  from metadata.architecture.migration_plan import MigrationAction, MigrationPlan
  from metadata.materialization.migration_executor import build_materialization_from_migration_plan
  from metadata.materialization.policy import MaterializationPolicy

  schema, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={
      "display_name": "Raw Core",
      "schema_name": "rawcore",
    },
  )
  if schema.schema_name != "rawcore":
    schema.schema_name = "rawcore"
    schema.save(update_fields=["schema_name"])

  td, _ = TargetDataset.objects.get_or_create(
    target_schema=schema,
    target_dataset_name="rc_customer_hist",
    defaults={
      "materialization_type": "incremental",
      "historize": False,
      "is_system_managed": True,
    },
  )

  TargetColumn.objects.get_or_create(
    target_dataset=td,
    target_column_name="inferred_member",
    defaults={
      "ordinal_position": 1,
      "datatype": "BOOLEAN",
      "nullable": True,
      "active": True,
    },
  )

  class AlreadyAppliedDialect:
    def introspect_table(self, **_kwargs):
      return {
        "table_exists": True,
        "actual_cols_by_norm_name": {
          "inferred_member": {"name": "inferred_member", "type": "BOOLEAN"},
        },
      }

    def map_logical_type(self, **_kwargs):
      return "BOOLEAN"

    def render_add_column(self, *_args, **_kwargs):
      raise AssertionError("render_add_column must not be called for already-applied columns")

  migration_plan = MigrationPlan(actions=(
    MigrationAction(
      action_type="ADD_COLUMN",
      strategy="ALTER_TABLE",
      dataset_key="rawcore.rc_customer_hist",
      column_name="inferred_member",
      reason="test",
    ),
  ))

  res = build_materialization_from_migration_plan(
    td=td,
    dataset_key="rawcore.rc_customer_hist",
    migration_plan=migration_plan,
    dialect=AlreadyAppliedDialect(),
    policy=MaterializationPolicy(
      sync_schema_shorts={"rawcore"},
      allow_auto_drop_columns=False,
      allow_type_alter=False,
    ),
    introspection_engine=None,
    exec_engine=DummyExecEngine(),
    is_full_refresh=False,
  )

  assert res.steps == []
  assert any("SCHEMA_ACTION_ALREADY_APPLIED" in w for w in res.warnings)


@pytest.mark.django_db
def test_migration_materialization_skips_rename_column_when_physical_column_already_renamed():
  """
  Regression:
  A MigrationPlan can be applied to a hist table twice in one execution flow.
  RENAME_COLUMN must be idempotent when the old physical column is already
  gone and the new physical column exists.
  """
  from metadata.architecture.migration_plan import MigrationAction, MigrationPlan
  from metadata.materialization.migration_executor import build_materialization_from_migration_plan
  from metadata.materialization.policy import MaterializationPolicy

  schema, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={
      "display_name": "Raw Core",
      "schema_name": "rawcore",
    },
  )
  if schema.schema_name != "rawcore":
    schema.schema_name = "rawcore"
    schema.save(update_fields=["schema_name"])

  td = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name="rc_sales_order_hist_rename_idempotent",
    materialization_type="incremental",
    historize=False,
    is_system_managed=True,
  )
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="sales_order_date",
    ordinal_position=1,
    datatype="DATE",
    nullable=True,
    active=True,
  )

  class AlreadyRenamedDialect:
    def introspect_table(self, **_kwargs):
      return {
        "table_exists": True,
        "actual_cols_by_norm_name": {
          "sales_order_date": {"name": "sales_order_date", "type": "DATE"},
        },
      }

    def render_rename_column(self, *_args, **_kwargs):
      raise AssertionError("render_rename_column must not be called for already-applied renames")

  migration_plan = MigrationPlan(actions=(
    MigrationAction(
      action_type="RENAME_COLUMN",
      strategy="RENAME_COLUMN",
      dataset_key="rawcore.rc_sales_order_hist_rename_idempotent",
      previous_column_name="order_date",
      column_name="sales_order_date",
      reason="test",
    ),
  ))

  res = build_materialization_from_migration_plan(
    td=td,
    dataset_key="rawcore.rc_sales_order_hist_rename_idempotent",
    migration_plan=migration_plan,
    dialect=AlreadyRenamedDialect(),
    policy=MaterializationPolicy(
      sync_schema_shorts={"rawcore"},
      allow_auto_drop_columns=False,
      allow_type_alter=False,
    ),
    introspection_engine=None,
    exec_engine=DummyExecEngine(),
    is_full_refresh=False,
  )

  assert res.steps == []
  assert any("SCHEMA_ACTION_ALREADY_APPLIED" in w for w in res.warnings)


@pytest.mark.django_db
def test_migration_materialization_rebuild_uses_current_column_when_rename_already_applied():
  """
  Regression:
  If a rebuild is required after a rename has already reached the physical
  table, the rebuild backfill must read from the current physical column name,
  not from the absent previous name.
  """
  from metadata.architecture.migration_plan import MigrationAction, MigrationPlan
  from metadata.materialization.migration_executor import build_materialization_from_migration_plan
  from metadata.materialization.policy import MaterializationPolicy

  schema, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={
      "display_name": "Raw Core",
      "schema_name": "rawcore",
    },
  )
  if schema.schema_name != "rawcore":
    schema.schema_name = "rawcore"
    schema.save(update_fields=["schema_name"])

  td = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name="rc_sales_order_hist_rebuild_idempotent",
    materialization_type="incremental",
    historize=False,
    is_system_managed=True,
  )
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="sales_order_date",
    ordinal_position=1,
    datatype="TIMESTAMP",
    nullable=True,
    active=True,
  )

  captured = {}

  class AlreadyRenamedRebuildDialect:
    def introspect_table(self, **_kwargs):
      return {
        "table_exists": True,
        "actual_cols_by_norm_name": {
          "sales_order_date": {"name": "sales_order_date", "type": "DATE"},
        },
      }

    def map_logical_type(self, **_kwargs):
      return "TIMESTAMP"

    def render_alter_column_type(self, **_kwargs):
      return ""

    def render_drop_table_if_exists(self, **_kwargs):
      return "DROP TMP"

    def render_create_table_from_columns(self, **_kwargs):
      return "CREATE TMP"

    def render_insert_select_for_rebuild(self, *, columns, **_kwargs):
      captured["columns"] = columns
      return "INSERT SELECT"

    def render_drop_table(self, **_kwargs):
      return "DROP SRC"

    def render_rename_table(self, *_args, **_kwargs):
      return "RENAME TMP"

  migration_plan = MigrationPlan(actions=(
    MigrationAction(
      action_type="RENAME_COLUMN",
      strategy="RENAME_COLUMN",
      dataset_key="rawcore.rc_sales_order_hist_rebuild_idempotent",
      previous_column_name="order_date",
      column_name="sales_order_date",
      reason="test",
    ),
    MigrationAction(
      action_type="ALTER_COLUMN",
      strategy="ALTER_TABLE",
      dataset_key="rawcore.rc_sales_order_hist_rebuild_idempotent",
      column_name="sales_order_date",
      reason="test",
    ),
  ))

  res = build_materialization_from_migration_plan(
    td=td,
    dataset_key="rawcore.rc_sales_order_hist_rebuild_idempotent",
    migration_plan=migration_plan,
    dialect=AlreadyRenamedRebuildDialect(),
    policy=MaterializationPolicy(
      sync_schema_shorts={"rawcore"},
      allow_auto_drop_columns=False,
      allow_type_alter=False,
    ),
    introspection_engine=None,
    exec_engine=DummyExecEngine(),
    is_full_refresh=False,
  )

  assert res.requires_rebuild is True
  assert captured["columns"][0]["source_name"] == "sales_order_date"
  assert captured["columns"][0]["source_name"] != "order_date"
  assert any("SCHEMA_ACTION_ALREADY_APPLIED" in w for w in res.warnings)


@pytest.mark.django_db
def test_full_refresh_drops_managed_dependent_view_before_table_recreate(monkeypatch):
  """
  Regression:
  PostgreSQL blocks DROP TABLE when a managed serving view depends on the table.
  Full-refresh recreate must drop only managed dependent views in the current
  execution scope before dropping the table, never use CASCADE.
  """
  from metadata.management.commands import elevata_load as mod
  from metadata.materialization.policy import MaterializationPolicy

  biz_schema, _ = TargetSchema.objects.get_or_create(
    short_name="bizcore",
    defaults={
      "display_name": "Biz Core",
      "schema_name": "bizcore",
      "default_materialization_type": "table",
    },
  )
  if biz_schema.schema_name != "bizcore":
    biz_schema.schema_name = "bizcore"
    biz_schema.save(update_fields=["schema_name"])

  serving_schema, _ = TargetSchema.objects.get_or_create(
    short_name="serving",
    defaults={
      "display_name": "Serving",
      "schema_name": "serving",
      "default_materialization_type": "view",
    },
  )
  update_fields = []
  if serving_schema.schema_name != "serving":
    serving_schema.schema_name = "serving"
    update_fields.append("schema_name")
  if serving_schema.default_materialization_type != "view":
    serving_schema.default_materialization_type = "view"
    update_fields.append("default_materialization_type")
  if update_fields:
    serving_schema.save(update_fields=update_fields)

  td = TargetDataset.objects.create(
    target_schema=biz_schema,
    target_dataset_name="bc_fact_dependency_test",
    materialization_type="table",
    incremental_strategy="full",
    historize=False,
    handle_deletes=False,
  )
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="customer_id",
    ordinal_position=1,
    datatype="INTEGER",
    nullable=True,
  )

  view_td = TargetDataset.objects.create(
    target_schema=serving_schema,
    target_dataset_name="Dim Customer",
    materialization_type="view",
    incremental_strategy="full",
    historize=False,
    handle_deletes=False,
  )
  view_td.input_links.create(
    upstream_target_dataset=td,
    role="primary",
    active=True,
  )

  monkeypatch.setattr(
    mod,
    "load_materialization_policy",
    lambda: MaterializationPolicy(
      sync_schema_shorts={"bizcore"},
      allow_auto_drop_columns=False,
      allow_type_alter=False,
    ),
    raising=False,
  )
  monkeypatch.setattr(mod, "build_materialization_plan", lambda **_kw: MaterializationPlan(
    dataset_key="bizcore.bc_fact_dependency_test",
    steps=[MaterializationStep(op="ENSURE_SCHEMA", sql="-- ensure", safe=True, reason="")],
    warnings=[],
    blocking_errors=[],
  ), raising=False)
  monkeypatch.setattr(mod, "apply_materialization_plan", lambda **_kw: None, raising=False)
  monkeypatch.setattr(mod, "ensure_target_schema", lambda **_kw: None, raising=False)
  monkeypatch.setattr(mod, "ensure_load_run_log_table", lambda **_kw: None, raising=False)
  monkeypatch.setattr(mod, "render_load_sql_for_target", lambda _td, _dialect: "SELECT 1;", raising=False)
  monkeypatch.setattr(mod, "build_load_plan", lambda _td: types.SimpleNamespace(mode="full"), raising=False)
  monkeypatch.setattr(mod, "build_load_run_summary", lambda _td, _dialect, _lp: {
    "mode": "full",
    "handle_deletes": False,
    "historize": False,
  }, raising=False)
  monkeypatch.setattr(mod, "engine_for_target", lambda **_kw: types.SimpleNamespace(dispose=lambda: None), raising=False)

  class RecordingExecEngine(DummyExecEngine):
    def __init__(self):
      self.sql: list[str] = []

    def execute(self, sql, _params=None):
      self.sql.append(sql)
      return 0

  class DependencyDialect(DummyDialect):
    def introspect_dependent_views(self, **_kwargs):
      return [{"schema": "serving", "name": "Dim Customer", "type": "view"}]

    def render_drop_view_if_exists(self, *, schema, view, materialized=False, cascade=False):
      assert cascade is False
      assert materialized is False
      return f'DROP VIEW IF EXISTS {schema}."{view}"'

    def render_drop_table_if_exists(self, *, schema, table, cascade=False):
      assert cascade is False
      return f"DROP TABLE IF EXISTS {schema}.{table}"

    def introspect_table(self, **_kwargs):
      return {
        "table_exists": True,
        "actual_cols_by_norm_name": {
          "customer_id": {"name": "customer_id", "type": "INTEGER"},
        },
      }

  exec_engine = RecordingExecEngine()
  result = mod.run_single_target_dataset(
    stdout=DummyStdout(),
    style=DummyStyle(),
    target_dataset=td,
    target_system=types.SimpleNamespace(short_name="dwh", type="postgres"),
    target_system_engine=exec_engine,
    profile=types.SimpleNamespace(name="test_profile"),
    dialect=DependencyDialect(),
    execute=True,
    no_print=True,
    debug_plan=False,
    batch_run_id="batch",
    load_run_id="load",
    load_plan_override=None,
    migration_plan=SimpleNamespace(actions=[]),
    execution_dataset_keys={
      "bizcore.bc_fact_dependency_test",
      "serving.Dim Customer",
    },
  )

  assert result["status"] == "success"
  assert exec_engine.sql[:2] == [
    'DROP VIEW IF EXISTS serving."Dim Customer"',
    "DROP TABLE IF EXISTS bizcore.bc_fact_dependency_test",
  ]


@pytest.mark.django_db
def test_full_refresh_blocks_unmanaged_dependent_view(monkeypatch):
  """
  Regression:
  Full refresh must not use CASCADE or drop unmanaged dependent views.
  """
  from metadata.management.commands import elevata_load as mod
  from metadata.materialization.policy import MaterializationPolicy

  biz_schema, _ = TargetSchema.objects.get_or_create(
    short_name="bizcore",
    defaults={
      "display_name": "Biz Core",
      "schema_name": "bizcore",
      "default_materialization_type": "table",
    },
  )
  if biz_schema.schema_name != "bizcore":
    biz_schema.schema_name = "bizcore"
    biz_schema.save(update_fields=["schema_name"])

  td = TargetDataset.objects.create(
    target_schema=biz_schema,
    target_dataset_name="bc_fact_unmanaged_dependency_test",
    materialization_type="table",
    incremental_strategy="full",
    historize=False,
    handle_deletes=False,
  )
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="customer_id",
    ordinal_position=1,
    datatype="INTEGER",
    nullable=True,
  )

  monkeypatch.setattr(
    mod,
    "load_materialization_policy",
    lambda: MaterializationPolicy(
      sync_schema_shorts={"bizcore"},
      allow_auto_drop_columns=False,
      allow_type_alter=False,
    ),
    raising=False,
  )
  monkeypatch.setattr(mod, "build_materialization_plan", lambda **_kw: MaterializationPlan(
    dataset_key="bizcore.bc_fact_unmanaged_dependency_test",
    steps=[MaterializationStep(op="ENSURE_SCHEMA", sql="-- ensure", safe=True, reason="")],
    warnings=[],
    blocking_errors=[],
  ), raising=False)
  monkeypatch.setattr(mod, "apply_materialization_plan", lambda **_kw: None, raising=False)
  monkeypatch.setattr(mod, "ensure_target_schema", lambda **_kw: None, raising=False)
  monkeypatch.setattr(mod, "ensure_load_run_log_table", lambda **_kw: None, raising=False)
  monkeypatch.setattr(mod, "render_load_sql_for_target", lambda _td, _dialect: "SELECT 1;", raising=False)
  monkeypatch.setattr(mod, "build_load_plan", lambda _td: types.SimpleNamespace(mode="full"), raising=False)
  monkeypatch.setattr(mod, "build_load_run_summary", lambda _td, _dialect, _lp: {
    "mode": "full",
    "handle_deletes": False,
    "historize": False,
  }, raising=False)
  monkeypatch.setattr(mod, "engine_for_target", lambda **_kw: types.SimpleNamespace(dispose=lambda: None), raising=False)

  class DependencyDialect(DummyDialect):
    def introspect_dependent_views(self, **_kwargs):
      return [{"schema": "external", "name": "some_view", "type": "view"}]

    def render_drop_table_if_exists(self, *, schema, table, cascade=False):
      raise AssertionError("DROP TABLE must not be rendered while unmanaged dependent views exist")

    def introspect_table(self, **_kwargs):
      return {
        "table_exists": True,
        "actual_cols_by_norm_name": {
          "customer_id": {"name": "customer_id", "type": "INTEGER"},
        },
      }

  with pytest.raises(CommandError, match="unmanaged physical dependent view"):
    mod.run_single_target_dataset(
      stdout=DummyStdout(),
      style=DummyStyle(),
      target_dataset=td,
      target_system=types.SimpleNamespace(short_name="dwh", type="postgres"),
      target_system_engine=DummyExecEngine(),
      profile=types.SimpleNamespace(name="test_profile"),
      dialect=DependencyDialect(),
      execute=True,
      no_print=True,
      debug_plan=False,
      batch_run_id="batch",
      load_run_id="load",
      load_plan_override=None,
      migration_plan=SimpleNamespace(actions=[]),
      execution_dataset_keys={"bizcore.bc_fact_unmanaged_dependency_test"},
    )


@pytest.mark.django_db
def test_managed_dependent_view_resolves_former_physical_name():
  """
  Regression:
  Physical dependency discovery may return a previous view name after a managed
  serving dataset was renamed. The dependency must still be recognized as
  managed through TargetDataset.former_names.
  """
  from metadata.management.commands import elevata_load as mod

  serving_schema, _ = TargetSchema.objects.get_or_create(
    short_name="serving",
    defaults={
      "display_name": "Serving",
      "schema_name": "serving",
      "default_materialization_type": "view",
    },
  )
  update_fields = []
  if serving_schema.schema_name != "serving":
    serving_schema.schema_name = "serving"
    update_fields.append("schema_name")
  if serving_schema.default_materialization_type != "view":
    serving_schema.default_materialization_type = "view"
    update_fields.append("default_materialization_type")
  if update_fields:
    serving_schema.save(update_fields=update_fields)

  view_td = TargetDataset.objects.create(
    target_schema=serving_schema,
    target_dataset_name="Customer",
    materialization_type="view",
    former_names=["Dim Customer"],
  )

  resolved = mod._managed_view_dataset_for_physical_view(
    schema_name="serving",
    view_name="Dim Customer",
  )

  assert resolved.pk == view_td.pk


@pytest.mark.django_db
def test_managed_dependent_view_resolves_unique_lineage_fallback():
  """
  Regression:
  Legacy/friendly physical serving view names may not equal the current
  TargetDataset name. If exactly one managed downstream view in the same schema
  depends on the full-refresh table, the dependency is still managed.
  """
  from metadata.management.commands import elevata_load as mod

  biz_schema, _ = TargetSchema.objects.get_or_create(
    short_name="bizcore",
    defaults={
      "display_name": "Biz Core",
      "schema_name": "bizcore",
      "default_materialization_type": "table",
    },
  )
  serving_schema, _ = TargetSchema.objects.get_or_create(
    short_name="serving",
    defaults={
      "display_name": "Serving",
      "schema_name": "serving",
      "default_materialization_type": "view",
    },
  )
  if serving_schema.default_materialization_type != "view":
    serving_schema.default_materialization_type = "view"
    serving_schema.save(update_fields=["default_materialization_type"])

  base_td = TargetDataset.objects.create(
    target_schema=biz_schema,
    target_dataset_name="bc_fact_lineage_dependency_test",
    materialization_type="table",
  )
  view_td = TargetDataset.objects.create(
    target_schema=serving_schema,
    target_dataset_name="Customer",
    materialization_type="view",
  )
  view_td.input_links.create(
    upstream_target_dataset=base_td,
    role="primary",
    active=True,
  )

  resolved = mod._managed_view_dataset_for_physical_view(
    schema_name="serving",
    view_name="Dim Customer",
    upstream_td=base_td,
  )

  assert resolved.pk == view_td.pk
