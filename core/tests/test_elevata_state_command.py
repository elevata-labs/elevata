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

from django.core.management import call_command

from metadata.architecture.state import ArchitectureState, ColumnState, DatasetState
from metadata.architecture.store import ArchitectureStateStore


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


def _patch_current_state(monkeypatch, state: ArchitectureState):
  """
  Patch current architecture state access for command tests.
  """
  import metadata.management.commands.elevata_state as mod

  monkeypatch.setattr(
    mod.ArchitectureStateService,
    "build_current_state",
    lambda self: state,
  )


def test_elevata_state_renders_current_state_json(monkeypatch):
  state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("customer_name"),
    ),
  ))
  _patch_current_state(monkeypatch, state)

  out = StringIO()
  call_command(
    "elevata_state",
    stdout=out,
  )

  data = json.loads(out.getvalue())

  assert data["datasets"][0]["dataset_key"] == "rawcore.customer"
  assert data["datasets"][0]["columns"][0]["column_name"] == "customer_id"
  assert data["datasets"][0]["columns"][1]["column_name"] == "customer_name"


def test_elevata_state_output_file_roundtrip_preserves_fingerprint(tmp_path, monkeypatch):
  state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("customer_name"),
    ),
  ))
  _patch_current_state(monkeypatch, state)
  output_path = tmp_path / "artifacts" / "architecture_state.json"

  call_command(
    "elevata_state",
    "--output",
    str(output_path),
    stdout=StringIO(),
  )

  loaded = ArchitectureStateStore.load_file(output_path)

  assert loaded is not None
  assert loaded.fingerprint == state.fingerprint
  assert loaded.datasets[0].dataset_key == "rawcore.customer"


def test_elevata_state_fingerprint_only(monkeypatch):
  state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("customer_name"),
    ),
  ))
  _patch_current_state(monkeypatch, state)

  out = StringIO()
  call_command(
    "elevata_state",
    "--fingerprint-only",
    stdout=out,
  )

  assert out.getvalue().strip() == state.fingerprint
  assert len(out.getvalue().strip()) == 64


def test_elevata_state_json_output_is_stable(monkeypatch):
  state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("customer_name"),
    ),
  ))
  _patch_current_state(monkeypatch, state)

  out_1 = StringIO()
  out_2 = StringIO()

  call_command(
    "elevata_state",
    stdout=out_1,
  )
  call_command(
    "elevata_state",
    stdout=out_2,
  )

  assert out_1.getvalue() == out_2.getvalue()