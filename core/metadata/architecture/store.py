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
import os
from json import JSONDecodeError
from pathlib import Path
from typing import Any

from .state import ArchitectureState


DEFAULT_ARCHITECTURE_STATE_DIR = ".elevata/state"
ARCHITECTURE_STATE_DIR_ENV = "ELEVATA_ARCH_STATE_DIR"


def resolve_architecture_state_dir(default: str | Path = DEFAULT_ARCHITECTURE_STATE_DIR) -> Path:
  """
  Resolve the architecture state directory.
  """
  value = os.getenv(ARCHITECTURE_STATE_DIR_ENV)
  if value and value.strip():
    return Path(value.strip())

  return Path(default)


class ArchitectureStateStore:
  """
  Simple file-based persistence for architecture state.

  This is intentionally lightweight and environment-local.
  It allows us to introduce architecture state without
  requiring DB schema changes.
  """

  def __init__(self, base_path: str | Path | None = None):
    self.base_path = Path(base_path) if base_path is not None else resolve_architecture_state_dir()
    self.base_path.mkdir(parents=True, exist_ok=True)

  def _state_file(self) -> Path:
    """
    Return the file path where the architecture state is stored.
    """
    return self.base_path / "architecture_state.json"

  def load(self) -> ArchitectureState | None:
    """
    Load previously persisted architecture state.

    Returns None if no state exists yet.
    """
    path = self._state_file()

    if not path.exists():
      return None

    return self.load_file(path)

  def save(self, state: ArchitectureState) -> None:
    """
    Persist the given architecture state to disk.
    """
    self.save_file(self._state_file(), state)

  @classmethod
  def load_file(cls, path: str | Path) -> ArchitectureState | None:
    """
    Load an architecture state from a JSON file.
    """
    state_path = Path(path)

    if not state_path.exists():
      return None

    try:
      raw = state_path.read_text(encoding="utf-8")
    except OSError:
      return None

    if not raw.strip():
      return None

    try:
      data = json.loads(raw)
      return cls.deserialize(data)
    except (JSONDecodeError, TypeError, ValueError, KeyError, AttributeError):
      return None

  @classmethod
  def save_file(cls, path: str | Path, state: ArchitectureState) -> None:
    """
    Persist an architecture state to a JSON file.
    """
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = state_path.with_suffix(".tmp")

    with tmp_path.open("w", encoding="utf-8") as f:
      json.dump(
        cls.serialize(state),
        f,
        ensure_ascii=False,
        indent=2,
      )

    tmp_path.replace(state_path)

  # ---------------------------
  # Serialization
  # ---------------------------

  @staticmethod
  def serialize(state: ArchitectureState) -> dict[str, Any]:
    """
    Return the JSON-serializable representation of an architecture state.
    """
    return {
      "datasets": [
        {
          "dataset_key": ds.dataset_key,
          "schema_short_name": ds.schema_short_name,
          "dataset_name": ds.dataset_name,
          "materialization_type": ds.materialization_type,
          "incremental_strategy": ds.incremental_strategy,
          "historize": ds.historize,
          "is_hist": ds.is_hist,
          "active": ds.active,
          "former_names": list(ds.former_names),
          "columns": [
            {
              "column_name": col.column_name,
              "datatype": col.datatype,
              "nullable": col.nullable,
              "active": col.active,
              "lineage_key": col.lineage_key,
              "former_names": list(col.former_names),
              "is_system_managed": col.is_system_managed,
              "system_role": col.system_role,
            }
            for col in ds.column_states
          ],
        }
        for ds in state.datasets
      ]
    }

  @staticmethod
  def deserialize(data: dict[str, Any]) -> ArchitectureState:
    """
    Build an architecture state from a JSON-compatible dictionary.
    """
    from .state import DatasetState, ColumnState

    datasets = []

    for ds in data.get("datasets", []):
      columns = [
        ColumnState(
          column_name=col["column_name"],
          datatype=col.get("datatype"),
          nullable=col.get("nullable", True),
          active=col.get("active", True),
          lineage_key=col.get("lineage_key"),
          former_names=tuple(col.get("former_names", [])),
          is_system_managed=col.get("is_system_managed", False),
          system_role=col.get("system_role"),
        )
        for col in ds.get("columns", [])
      ]

      datasets.append(
        DatasetState(
          dataset_key=ds["dataset_key"],
          schema_short_name=ds.get("schema_short_name", ""),
          dataset_name=ds["dataset_name"],
          materialization_type=ds.get("materialization_type"),
          incremental_strategy=ds.get("incremental_strategy"),
          historize=ds.get("historize", False),
          is_hist=ds.get("is_hist", False),
          active=ds.get("active", True),
          former_names=tuple(ds.get("former_names", [])),
          column_states=tuple(columns),
        )
      )

    return ArchitectureState(datasets=tuple(datasets))