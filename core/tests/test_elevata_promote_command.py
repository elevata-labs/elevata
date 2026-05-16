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


def _patch_policy(monkeypatch, policy=None):
  """
  Patch policy access for command tests.
  """
  import metadata.management.commands.elevata_promote as mod

  if policy is None:
    policy = _policy()

  monkeypatch.setattr(
    mod,
    "load_materialization_policy",
    lambda: policy,
  )


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


def _write_state(path, state: ArchitectureState):
  """
  Write an architecture state for command tests.
  """
  ArchitectureStateStore.save_file(path, state)


def test_elevata_promote_renders_text_report(tmp_path, monkeypatch):
  _patch_policy(monkeypatch)
  source_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  target_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("customer_name"),
    ),
  ))
  source_path = tmp_path / "dev" / "architecture_state.json"
  target_path = tmp_path / "prod" / "architecture_state.json"
  _write_state(source_path, source_state)
  _write_state(target_path, target_state)

  out = StringIO()
  call_command(
    "elevata_promote",
    str(source_path),
    str(target_path),
    "--source-label",
    "dev",
    "--target-label",
    "prod",
    "--schema",
    "rawcore",
    stdout=out,
  )

  text = out.getvalue()
  assert "Architecture Promotion Report" in text
  assert "source: dev" in text
  assert "target: prod" in text
  assert "rawcore.customer" in text
  assert "ADD_COLUMN" in text
  assert "ADD_COLUMN_ALLOWED" in text


def test_elevata_promote_renders_stable_json_report(tmp_path, monkeypatch):
  _patch_policy(monkeypatch)
  source_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  target_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("customer_name"),
    ),
  ))
  source_path = tmp_path / "source.json"
  target_path = tmp_path / "target.json"
  _write_state(source_path, source_state)
  _write_state(target_path, target_state)

  out_1 = StringIO()
  out_2 = StringIO()

  call_command(
    "elevata_promote",
    str(source_path),
    str(target_path),
    "--format",
    "json",
    stdout=out_1,
  )
  call_command(
    "elevata_promote",
    str(source_path),
    str(target_path),
    "--format",
    "json",
    stdout=out_2,
  )

  assert out_1.getvalue() == out_2.getvalue()

  data = json.loads(out_1.getvalue())
  assert data["summary"]["has_changes"] is True
  assert data["change_report"]["migration_actions"][0]["action_type"] == "ADD_COLUMN"
  assert isinstance(data["promotion_fingerprint"], str)
  assert len(data["promotion_fingerprint"]) == 64


def test_elevata_promote_scopes_target_dataset(tmp_path, monkeypatch):
  _patch_policy(monkeypatch)
  source_state = _state(
    _dataset(
      "customer",
      columns=(
        _column("customer_id", datatype="integer", nullable=False),
      ),
    ),
    _dataset(
      "product",
      columns=(
        _column("product_id", datatype="integer", nullable=False),
      ),
    ),
  )
  target_state = _state(
    _dataset(
      "customer",
      columns=(
        _column("customer_id", datatype="integer", nullable=False),
        _column("customer_name"),
      ),
    ),
    _dataset(
      "product",
      columns=(
        _column("product_id", datatype="integer", nullable=False),
        _column("product_name"),
      ),
    ),
  )
  source_path = tmp_path / "source.json"
  target_path = tmp_path / "target.json"
  _write_state(source_path, source_state)
  _write_state(target_path, target_state)

  out = StringIO()
  call_command(
    "elevata_promote",
    str(source_path),
    str(target_path),
    "--target-dataset",
    "customer",
    "--schema",
    "rawcore",
    "--format",
    "json",
    stdout=out,
  )

  data = json.loads(out.getvalue())
  assert data["change_report"]["scope"]["dataset_keys"] == ["rawcore.customer"]
  assert [
    action["column_name"]
    for action in data["change_report"]["migration_actions"]
  ] == ["customer_name"]


def test_elevata_promote_fail_on_changes_raises_command_error(tmp_path, monkeypatch):
  _patch_policy(monkeypatch)
  source_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  target_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("customer_name"),
    ),
  ))
  source_path = tmp_path / "source.json"
  target_path = tmp_path / "target.json"
  _write_state(source_path, source_state)
  _write_state(target_path, target_state)

  with pytest.raises(CommandError) as exc_info:
    call_command(
      "elevata_promote",
      str(source_path),
      str(target_path),
      "--fail-on-changes",
      stdout=StringIO(),
    )

  assert "contains changes" in str(exc_info.value)


def test_elevata_promote_fail_on_blocked_raises_command_error(tmp_path, monkeypatch):
  _patch_policy(monkeypatch)
  source_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("legacy_flag", datatype="boolean"),
    ),
  ))
  target_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  source_path = tmp_path / "source.json"
  target_path = tmp_path / "target.json"
  _write_state(source_path, source_state)
  _write_state(target_path, target_state)

  with pytest.raises(CommandError) as exc_info:
    call_command(
      "elevata_promote",
      str(source_path),
      str(target_path),
      "--fail-on-blocked",
      stdout=StringIO(),
    )

  assert "blocking policy decisions" in str(exc_info.value)


def test_elevata_promote_fail_on_destructive_raises_command_error_when_allowed(
  tmp_path,
  monkeypatch,
):
  _patch_policy(monkeypatch, policy=_policy(allow_auto_drop_columns=True))
  source_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("legacy_flag", datatype="boolean"),
    ),
  ))
  target_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  source_path = tmp_path / "source.json"
  target_path = tmp_path / "target.json"
  _write_state(source_path, source_state)
  _write_state(target_path, target_state)

  with pytest.raises(CommandError) as exc_info:
    call_command(
      "elevata_promote",
      str(source_path),
      str(target_path),
      "--fail-on-destructive",
      stdout=StringIO(),
    )

  assert "destructive actions" in str(exc_info.value)


def test_elevata_promote_raises_for_missing_source_file(tmp_path, monkeypatch):
  _patch_policy(monkeypatch)
  target_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  target_path = tmp_path / "target.json"
  _write_state(target_path, target_state)

  with pytest.raises(CommandError) as exc_info:
    call_command(
      "elevata_promote",
      str(tmp_path / "missing.json"),
      str(target_path),
      stdout=StringIO(),
    )

  assert "could not be read" in str(exc_info.value)


def test_elevata_promote_writes_output_file(tmp_path, monkeypatch):
  _patch_policy(monkeypatch)
  source_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  target_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("customer_name"),
    ),
  ))
  source_path = tmp_path / "source.json"
  target_path = tmp_path / "target.json"
  output_path = tmp_path / "promotion_report.json"
  _write_state(source_path, source_state)
  _write_state(target_path, target_state)

  call_command(
    "elevata_promote",
    str(source_path),
    str(target_path),
    "--format",
    "json",
    "--output",
    str(output_path),
    stdout=StringIO(),
  )

  data = json.loads(output_path.read_text(encoding="utf-8"))
  assert data["summary"]["has_changes"] is True
  assert data["change_report"]["migration_actions"][0]["action_type"] == "ADD_COLUMN"