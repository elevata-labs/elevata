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
import pytest

from metadata.materialization.plan import MaterializationPlan, MaterializationStep
from metadata.models import TargetDataset, TargetSchema
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
      "database_name": "dwh",
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

  def spy_ensure_target_table(engine, dialect, td, auto_provision):
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
  )

  assert res["status"] == "success"
  assert calls["ensure"] == 1


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