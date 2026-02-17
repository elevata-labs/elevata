"""
elevata - Metadata-driven Data Platform Framework
Copyright © 202-2026 Ilona Tag

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

import types
import pytest

import metadata.management.commands.elevata_load as elevata_load
from metadata.materialization.plan import MaterializationPlan, MaterializationStep
from tests._dialect_test_mixin import DialectTestMixin


class DummySchema:
  def __init__(self, short_name: str, schema_name: str):
    self.short_name = short_name
    self.schema_name = schema_name
    self.default_materialization_type = "table"
    self.default_historize = False


class DummyTD:
  def __init__(self):
    self.pk = 1
    self.id = 1
    self.target_dataset_name = "stg_customer"
    self.target_schema = DummySchema(short_name="stage", schema_name="stage")
    self.materialization_type = "table"
    self.incremental_strategy = "full"
    self.historize = False
    self.lineage_key = None
    self.input_links = _EmptyLinks()

class _EmptyLinks:
  """
  Minimal stub for td.input_links (TargetDatasetInput related_name).
  Must tolerate filter()/all()/exists()/count()/iteration in various codepaths.
  """
  def filter(self, *args, **kwargs):
    return self

  def all(self):
    return self

  def select_related(self, *args, **kwargs):
    return self

  def prefetch_related(self, *args, **kwargs):
    return self

  def exists(self):
    return False

  def count(self):
    return 0

  def __iter__(self):
    return iter([])


class DummyEngine:
  def execute(self, sql: str):
    return 0

  def dispose(self):
    return None


class DummyDialect(DialectTestMixin):
  pass


@pytest.mark.django_db(False)
def test_type_drift_does_not_block_when_full_refresh(monkeypatch):
  td = DummyTD()

  # Make TD lookup return our DummyTD.
  class _QS:
    def select_related(self, *args, **kwargs):
      return self

    def prefetch_related(self, *args, **kwargs):
      return self

    def get(self, pk):
      assert pk == td.pk
      return td

  monkeypatch.setattr(elevata_load.TargetDataset, "objects", _QS())

  # Force full refresh.
  monkeypatch.setattr(elevata_load, "should_truncate_before_load", lambda _td, _lp: True)

  # Avoid real plan creation; return TYPE_DRIFT warning, but no blocking errors.
  def _fake_build_materialization_plan(*, td, introspection_engine, exec_engine, dialect, policy):
    return MaterializationPlan(
      dataset_key=f"{td.target_schema.short_name}.{td.target_dataset_name}",
      steps=[
        # Full refresh should keep only ENSURE_SCHEMA/RENAME_DATASET anyway.
        MaterializationStep(op="ADD_COLUMN", sql="ALTER TABLE x ADD COLUMN y INT", safe=True, reason=""),
      ],
      warnings=[
        "TYPE_DRIFT: kind=widening reason=test col=stage.stg_customer.foo desired=int actual=bigint",
      ],
      blocking_errors=[],
      requires_backfill=False,
    )

  monkeypatch.setattr(elevata_load, "build_materialization_plan", _fake_build_materialization_plan)

  # Make load plan + summary minimal.
  monkeypatch.setattr(elevata_load, "build_load_plan", lambda _td: types.SimpleNamespace(mode="full"))

  # elevata_load uses dataclasses.replace() on load plan objects in some paths.
  # Our unit test uses lightweight stubs, so patch replace to be a no-op.
  monkeypatch.setattr(elevata_load, "replace", lambda obj, **kwargs: obj)

  monkeypatch.setattr(elevata_load, "build_load_run_summary", lambda *_args, **_kwargs: {"mode": "full", "handle_deletes": False, "historize": False})
  monkeypatch.setattr(elevata_load, "format_load_run_summary", lambda *_args, **_kwargs: "")

  # Avoid schema/table provisioning side effects.
  monkeypatch.setattr(elevata_load, "load_materialization_policy", lambda: types.SimpleNamespace(sync_schema_shorts={"stage"}, debug_plan=False))
  monkeypatch.setattr(elevata_load, "_plan_did_provision", lambda _plan: False)

  # SQL rendering/execution: keep it trivial.
  monkeypatch.setattr(elevata_load, "render_load_sql_for_target", lambda *_args, **_kwargs: "SELECT 1")
  monkeypatch.setattr(elevata_load, "apply_runtime_placeholders", lambda sql, **_kwargs: sql)

  # Avoid delta cutoff path.
  monkeypatch.setattr(elevata_load, "resolve_delta_cutoff_for_source_dataset", lambda *_args, **_kwargs: None)

  # Avoid meta log provisioning.
  monkeypatch.setattr(elevata_load, "ensure_load_run_log_table", lambda *_args, **_kwargs: None)
  monkeypatch.setattr(elevata_load, "ensure_load_run_snapshot_table", lambda *_args, **_kwargs: None)

  # No-op materialization applier (we're testing "no block", not DDL execution).
  monkeypatch.setattr(elevata_load, "apply_materialization_plan", lambda *, plan, exec_engine: None)

  # Run
  result = elevata_load.run_single_target_dataset(
    stdout=types.SimpleNamespace(write=lambda *_a, **_k: None),
    style=types.SimpleNamespace(NOTICE=lambda s: s, WARNING=lambda s: s, ERROR=lambda s: s, SUCCESS=lambda s: s),
    target_dataset=td,
    target_system=types.SimpleNamespace(short_name="dbdwh", type="databricks"),
    target_system_engine=DummyEngine(),
    profile=types.SimpleNamespace(name="dev"),
    dialect=DummyDialect(),
    execute=True,
    no_print=True,
    debug_plan=False,
    debug_materialization=False,
    batch_run_id="batch",
    load_run_id="load",
    load_plan_override=None,
    chunk_size=1000,
    attempt_no=1,
    # Critical: even strict mode must not block on full refresh.
    fail_on_type_drift=True,
    allow_lossy_type_drift=False,
    no_type_changes=True,
  )

  assert result["status"] in ("success", "error")  # We only assert "no CommandError"
