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
from types import SimpleNamespace
import uuid

import pytest
from django.core.management.base import CommandError


@dataclass(frozen=True)
class _Action:
  action_type: str
  dataset_key: str
  column_name: str | None = None
  previous_column_name: str | None = None
  previous_dataset_key: str | None = None


class _FakeMigrationPlan:
  def __init__(self, actions):
    self.actions = actions

  def is_empty(self) -> bool:
    return not bool(self.actions)

  def to_summary_lines(self):
    # Keep output stable in CI
    for a in self.actions:
      yield f"{a.action_type}: {a.dataset_key}"


class _FakeArchDiff:
  def __init__(self):
    self.dataset_changes = []
    self.column_changes = []

  def has_changes(self) -> bool:
    return True


@pytest.mark.django_db
def test_arch_mode_enforce_blocks_on_shadow_compare_mismatch(monkeypatch):
  import metadata.management.commands.elevata_load as mod
  from metadata.materialization.plan import MaterializationStep
  from metadata.models import TargetSchema, TargetDataset

  monkeypatch.setenv("ELEVATA_ARCH_MODE", "enforce")
  # Default policy: auto-drop is disabled -> DROP_COLUMN intent will be missing in actual steps.
  monkeypatch.delenv("ELEVATA_ALLOW_AUTO_DROP_COLUMNS", raising=False)

  # Create a minimal dataset in scope
  schema = TargetSchema.objects.get_or_create(short_name="rawcore", schema_name="rawcore")[0]
  td = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=f"rc_guard_{uuid.uuid4().hex[:6]}",
    incremental_strategy="full",
    materialization_type="table",
    is_system_managed=False,
  )
  ds_key = f"{schema.short_name}.{td.target_dataset_name}"

  # ---- Patch: avoid real engines / IO ----
  monkeypatch.setattr(mod, "get_target_system", lambda _name: SimpleNamespace(type="duckdb", short_name="dwh"))


  class _DummyDialect:
    def get_execution_engine(self, _system):
      return SimpleNamespace(close=lambda: None)

  monkeypatch.setattr(mod, "get_active_dialect", lambda _name: _DummyDialect())
  monkeypatch.setattr(mod, "engine_for_target", lambda *args, **kwargs: SimpleNamespace(dispose=lambda: None))

  # Planner returns an empty plan => actual schema-op tokens = 0
  # Minimal plan: ENSURE_SCHEMA only (no schema-op tokens from planner).
  monkeypatch.setattr(
    mod,
    "build_materialization_plan",
    lambda **kwargs: SimpleNamespace(
      steps=[MaterializationStep(op="ENSURE_SCHEMA", sql="-- ensure", reason="ensure", safe=True)],
      warnings=[],
      blocking_errors=[],
      requires_rebuild=False,
    ),
  )

  # Ensure execution order is exactly our dataset (no deps)
  monkeypatch.setattr(mod, "resolve_execution_order", lambda **kwargs: [td])

  # Force "previous_state exists" so MigrationPlanner.plan is called
  monkeypatch.setattr(mod.ArchitectureStateService, "load_previous_state", lambda self: object())
  monkeypatch.setattr(mod.ArchitectureStateService, "diff_against", lambda self, _prev: (SimpleNamespace(fingerprint="x", datasets_by_key={}), _FakeArchDiff()))

  # Intent contains an ALTER_COLUMN (not suppressed by full-refresh rules in shadow compare).
  fake_plan = _FakeMigrationPlan(actions=[
    _Action(action_type="ALTER_COLUMN", dataset_key=ds_key, column_name="due_date"),
  ])
  monkeypatch.setattr(mod.MigrationPlanner, "plan", lambda self, *args, **kwargs: fake_plan)

  # Force a deterministic mismatch: expected has ALTER_COLUMN, actual has none.
  # Shadow compare will see missing>0 and enforce will block before execute_plan.
  def _fake_build_from_mp(**_kwargs):
    return SimpleNamespace(steps=[], warnings=[], blocking_errors=[], requires_rebuild=False)
  monkeypatch.setattr(mod, "build_materialization_from_migration_plan", _fake_build_from_mp)

  cmd = mod.Command()

  with pytest.raises(CommandError) as exc:
    cmd.handle(
      target_name=td.target_dataset_name,
      schema_short="rawcore",
      all_datasets=False,
      dialect_name="duckdb",
      target_system_name="dwh",
      execute=True,
      no_print=True,
      # optional flags (defaults via .get(...) in handle)
      debug_plan=False,
      no_deps=True,
      continue_on_error=False,
      max_retries=0,
      no_plan_guard=False,
      no_type_changes=False,
      fail_on_type_drift=False,
      allow_type_alter=False,
      debug_execution=False,
      write_execution_snapshot=False,
      execution_snapshot_dir="",
      debug_migration=False,
      debug_materialization=False,
      diff_against_batch_run_id=None,
    )

  assert "Architecture guard blocked execution" in str(exc.value)