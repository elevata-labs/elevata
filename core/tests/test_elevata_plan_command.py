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

import json
from io import StringIO

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from metadata.architecture.state import ArchitectureState, ColumnState, DatasetState
from metadata.architecture.store import ArchitectureStateStore
from metadata.materialization.policy import MaterializationPolicy


def _column(
  name: str,
  *,
  datatype: str = "string",
  nullable: bool = True,
  active: bool = True,
  lineage_key: str | None = None,
) -> ColumnState:
  """
  Build a column state for command tests.
  """
  return ColumnState(
    column_name=name,
    datatype=datatype,
    nullable=nullable,
    active=active,
    lineage_key=lineage_key or f"lk_{name}",
    former_names=(),
    is_system_managed=False,
    system_role=None,
  )


def _dataset(
  name: str,
  *,
  columns: tuple[ColumnState, ...],
  schema_short_name: str = "rawcore",
  historize: bool = False,
  is_hist: bool = False,
) -> DatasetState:
  """
  Build a dataset state for command tests.
  """
  return DatasetState(
    dataset_key=f"{schema_short_name}.{name}",
    schema_short_name=schema_short_name,
    dataset_name=name,
    materialization_type="table",
    incremental_strategy="full",
    historize=historize,
    is_hist=is_hist,
    active=True,
    former_names=(),
    column_states=columns,
  )


def _state(*datasets: DatasetState) -> ArchitectureState:
  """
  Build an architecture state for command tests.
  """
  return ArchitectureState(datasets=tuple(datasets))


def _policy(
  *,
  allow_auto_drop_columns: bool = False,
  allow_auto_drop_hist_columns: bool = False,
  allow_type_alter: bool = False,
) -> MaterializationPolicy:
  """
  Build materialization policy for command tests.
  """
  return MaterializationPolicy(
    sync_schema_shorts={"rawcore", "bizcore"},
    allow_auto_drop_columns=allow_auto_drop_columns,
    allow_auto_drop_hist_columns=allow_auto_drop_hist_columns,
    allow_type_alter=allow_type_alter,
  )


def _patch_architecture_states(monkeypatch, *, previous_state, current_state, policy=None):
  """
  Patch architecture state and policy access for command tests.
  """
  import metadata.management.commands.elevata_plan as mod

  if policy is None:
    policy = _policy()

  monkeypatch.setattr(
    mod.ArchitectureStateService,
    "load_previous_state",
    lambda self: previous_state,
  )
  monkeypatch.setattr(
    mod.ArchitectureStateService,
    "build_current_state",
    lambda self: current_state,
  )
  monkeypatch.setattr(
    mod,
    "load_materialization_policy",
    lambda: policy,
  )


def test_elevata_plan_renders_text_report_for_target(monkeypatch):
  previous_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  current_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("customer_name"),
    ),
  ))
  _patch_architecture_states(
    monkeypatch,
    previous_state=previous_state,
    current_state=current_state,
  )

  out = StringIO()
  call_command(
    "elevata_plan",
    "customer",
    "--schema",
    "rawcore",
    stdout=out,
  )

  text = out.getvalue()
  assert "Architecture Change Report" in text
  assert "target: customer" in text
  assert "rawcore.customer" in text
  assert "ADD_COLUMN" in text
  assert "ADD_COLUMN_ALLOWED" in text


def test_elevata_plan_renders_stable_json_report(monkeypatch):
  previous_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  current_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("customer_name"),
    ),
  ))
  _patch_architecture_states(
    monkeypatch,
    previous_state=previous_state,
    current_state=current_state,
  )

  out_1 = StringIO()
  out_2 = StringIO()

  call_command(
    "elevata_plan",
    "rawcore.customer",
    "--format",
    "json",
    stdout=out_1,
  )
  call_command(
    "elevata_plan",
    "rawcore.customer",
    "--format",
    "json",
    stdout=out_2,
  )

  assert out_1.getvalue() == out_2.getvalue()

  data = json.loads(out_1.getvalue())
  assert data["scope"]["dataset_keys"] == ["rawcore.customer"]
  assert data["summary"]["migration_action_count"] == 1
  assert data["migration_actions"][0]["action_type"] == "ADD_COLUMN"
  assert isinstance(data["report_fingerprint"], str)
  assert len(data["report_fingerprint"]) == 64


def test_elevata_plan_fail_on_changes_raises_command_error(monkeypatch):
  previous_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  current_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("customer_name"),
    ),
  ))
  _patch_architecture_states(
    monkeypatch,
    previous_state=previous_state,
    current_state=current_state,
  )

  with pytest.raises(CommandError) as exc_info:
    call_command(
      "elevata_plan",
      "customer",
      "--schema",
      "rawcore",
      "--fail-on-changes",
      stdout=StringIO(),
    )

  assert "contains changes" in str(exc_info.value)


def test_elevata_plan_fail_on_blocked_raises_command_error(monkeypatch):
  previous_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("legacy_flag", datatype="boolean"),
    ),
  ))
  current_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  _patch_architecture_states(
    monkeypatch,
    previous_state=previous_state,
    current_state=current_state,
  )

  with pytest.raises(CommandError) as exc_info:
    call_command(
      "elevata_plan",
      "customer",
      "--schema",
      "rawcore",
      "--fail-on-blocked",
      stdout=StringIO(),
    )

  assert "blocking policy decisions" in str(exc_info.value)


def test_elevata_plan_detects_ambiguous_dataset_name(monkeypatch):
  state = _state(
    _dataset("customer", schema_short_name="rawcore", columns=(_column("id"),)),
    _dataset("customer", schema_short_name="bizcore", columns=(_column("id"),)),
  )
  _patch_architecture_states(
    monkeypatch,
    previous_state=state,
    current_state=state,
  )

  with pytest.raises(CommandError) as exc_info:
    call_command(
      "elevata_plan",
      "customer",
      stdout=StringIO(),
    )

  assert "ambiguous" in str(exc_info.value)


def test_elevata_plan_allows_schema_scoped_dataset_name(monkeypatch):
  state = _state(
    _dataset("customer", schema_short_name="rawcore", columns=(_column("id"),)),
    _dataset("customer", schema_short_name="bizcore", columns=(_column("id"),)),
  )
  _patch_architecture_states(
    monkeypatch,
    previous_state=state,
    current_state=state,
  )

  out = StringIO()
  call_command(
    "elevata_plan",
    "customer",
    "--schema",
    "bizcore",
    stdout=out,
  )

  assert "bizcore.customer" in out.getvalue()
  assert "rawcore.customer" not in out.getvalue()


def test_elevata_plan_uses_explicit_previous_state_file(tmp_path, monkeypatch):
  explicit_previous_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  current_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("customer_name"),
    ),
  ))
  previous_state_path = tmp_path / "baseline" / "architecture_state.json"
  ArchitectureStateStore.save_file(previous_state_path, explicit_previous_state)

  _patch_architecture_states(
    monkeypatch,
    previous_state=current_state,
    current_state=current_state,
  )

  out = StringIO()
  call_command(
    "elevata_plan",
    "customer",
    "--schema",
    "rawcore",
    "--previous-state",
    str(previous_state_path),
    "--format",
    "json",
    stdout=out,
  )

  data = json.loads(out.getvalue())

  assert data["state"]["previous_fingerprint"] == explicit_previous_state.fingerprint
  assert data["state"]["current_fingerprint"] == current_state.fingerprint
  assert data["summary"]["migration_action_count"] == 1
  assert data["migration_actions"][0]["action_type"] == "ADD_COLUMN"
  assert data["migration_actions"][0]["column_name"] == "customer_name"


def test_elevata_plan_raises_for_unreadable_previous_state_file(tmp_path, monkeypatch):
  current_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  missing_path = tmp_path / "missing" / "architecture_state.json"

  _patch_architecture_states(
    monkeypatch,
    previous_state=current_state,
    current_state=current_state,
  )

  with pytest.raises(CommandError) as exc_info:
    call_command(
      "elevata_plan",
      "customer",
      "--schema",
      "rawcore",
      "--previous-state",
      str(missing_path),
      stdout=StringIO(),
    )

  assert "could not be read" in str(exc_info.value)


def test_elevata_plan_all_scope_reports_all_mode(monkeypatch):
  state = _state(
    _dataset("customer", columns=(_column("id"),)),
    _dataset("product", columns=(_column("id"),)),
  )
  _patch_architecture_states(
    monkeypatch,
    previous_state=state,
    current_state=state,
  )

  out = StringIO()
  call_command(
    "elevata_plan",
    "--all",
    "--format",
    "json",
    stdout=out,
  )

  data = json.loads(out.getvalue())
  assert data["scope"]["mode"] == "all"
  assert data["scope"]["dataset_keys"] == [
    "rawcore.customer",
    "rawcore.product",
  ]


def test_elevata_plan_schema_scope_reports_scoped_mode(monkeypatch):
  state = _state(
    _dataset("customer", schema_short_name="rawcore", columns=(_column("id"),)),
    _dataset("customer", schema_short_name="bizcore", columns=(_column("id"),)),
  )
  _patch_architecture_states(
    monkeypatch,
    previous_state=state,
    current_state=state,
  )

  out = StringIO()
  call_command(
    "elevata_plan",
    "--all",
    "--schema",
    "rawcore",
    "--format",
    "json",
    stdout=out,
  )

  data = json.loads(out.getvalue())
  assert data["scope"]["mode"] == "scoped"
  assert data["scope"]["schema_short"] == "rawcore"
  assert data["scope"]["dataset_keys"] == ["rawcore.customer"]