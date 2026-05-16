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
  Build a column state for store tests.
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
  Build a dataset state for store tests.
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
  Build an architecture state for store tests.
  """
  return ArchitectureState(datasets=tuple(datasets))


def test_architecture_state_store_serializes_state():
  state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("customer_name"),
    ),
  ))

  data = ArchitectureStateStore.serialize(state)

  assert list(data.keys()) == ["datasets"]
  assert data["datasets"][0]["dataset_key"] == "rawcore.customer"
  assert data["datasets"][0]["columns"][0]["column_name"] == "customer_id"


def test_architecture_state_store_deserializes_state():
  data = {
    "datasets": [
      {
        "dataset_key": "rawcore.customer",
        "schema_short_name": "rawcore",
        "dataset_name": "customer",
        "materialization_type": "table",
        "incremental_strategy": "full",
        "historize": True,
        "is_hist": False,
        "active": True,
        "former_names": ["customer_old"],
        "columns": [
          {
            "column_name": "customer_id",
            "datatype": "integer",
            "nullable": False,
            "active": True,
            "lineage_key": "lk_customer_id",
            "former_names": [],
            "is_system_managed": False,
            "system_role": None,
          },
        ],
      }
    ]
  }

  state = ArchitectureStateStore.deserialize(data)

  assert state.datasets[0].dataset_key == "rawcore.customer"
  assert state.datasets[0].former_names == ("customer_old",)
  assert state.datasets[0].column_states[0].column_name == "customer_id"
  assert state.datasets[0].column_states[0].nullable is False


def test_architecture_state_store_file_roundtrip_preserves_fingerprint(tmp_path):
  state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("customer_name"),
    ),
  ))
  path = tmp_path / "dev" / "architecture_state.json"

  ArchitectureStateStore.save_file(path, state)
  loaded = ArchitectureStateStore.load_file(path)

  assert loaded is not None
  assert loaded.fingerprint == state.fingerprint
  assert loaded.datasets[0].dataset_key == "rawcore.customer"


def test_architecture_state_store_load_file_returns_none_for_missing_file(tmp_path):
  loaded = ArchitectureStateStore.load_file(
    tmp_path / "missing" / "architecture_state.json"
  )

  assert loaded is None


def test_architecture_state_store_load_file_returns_none_for_invalid_json(tmp_path):
  path = tmp_path / "architecture_state.json"
  path.write_text("{invalid", encoding="utf-8")

  loaded = ArchitectureStateStore.load_file(path)

  assert loaded is None


def test_architecture_state_store_save_file_writes_json(tmp_path):
  state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  path = tmp_path / "architecture_state.json"

  ArchitectureStateStore.save_file(path, state)

  data = json.loads(path.read_text(encoding="utf-8"))

  assert data["datasets"][0]["dataset_key"] == "rawcore.customer"


def test_architecture_state_store_uses_environment_state_dir(tmp_path, monkeypatch):
  state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  state_dir = tmp_path / "custom" / "state"
  monkeypatch.setenv("ELEVATA_ARCH_STATE_DIR", str(state_dir))

  store = ArchitectureStateStore()
  store.save(state)

  loaded = store.load()

  assert loaded is not None
  assert loaded.fingerprint == state.fingerprint
  assert (state_dir / "architecture_state.json").exists()