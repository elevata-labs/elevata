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

from dataclasses import dataclass, field
from typing import Any
import hashlib
import json


def _stable_json_hash(value: Any) -> str:
  """
  Return a deterministic SHA-256 hash for a JSON-serializable value.

  The function uses sorted keys and compact separators so that semantically
  identical payloads result in the same hash across runs.
  """
  payload = json.dumps(
    value,
    sort_keys=True,
    ensure_ascii=False,
    separators=(",", ":"),
    default=str,
  )
  return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _as_str_tuple(value: Any) -> tuple[str, ...]:
  """
  Normalize a string/list/tuple/None to a tuple[str, ...].

  This is defensive: metadata may contain former_names in different shapes
  depending on serialization or migrations.
  """
  if value is None:
    return ()
  if isinstance(value, tuple):
    return tuple(str(v) for v in value if v)
  if isinstance(value, list):
    return tuple(str(v) for v in value if v)
  if isinstance(value, str):
    return (value,) if value else ()
  return ()


@dataclass(frozen=True)
class ColumnState:
  """
  Runtime-facing semantic state of a single target column.

  This is intentionally smaller and more stable than the full Django model.
  It should only contain attributes that matter for architectural planning.
  """
  column_name: str
  datatype: str | None
  nullable: bool
  active: bool
  lineage_key: str | None = None
  former_names: tuple[str, ...] = ()
  is_system_managed: bool = False
  system_role: str | None = None

  def fingerprint_payload(self) -> dict[str, Any]:
    """
    Return the normalized payload used for hashing and state comparison.
    """
    return {
      "column_name": self.column_name,
      "datatype": self.datatype,
      "nullable": self.nullable,
      "active": self.active,
      "lineage_key": self.lineage_key,
      "former_names": list(sorted(_as_str_tuple(self.former_names))),      
      "is_system_managed": self.is_system_managed,
      "system_role": "" if self.system_role is None else self.system_role,
    }

  @property
  def fingerprint(self) -> str:
    """
    Return a stable semantic hash of the column state.
    """
    return _stable_json_hash(self.fingerprint_payload())


@dataclass(frozen=True)
class DatasetState:
  """
  Runtime-facing semantic state of a target dataset.

  The goal is not to mirror every metadata field, but to capture the subset
  that changes runtime behavior and architectural meaning.
  """
  dataset_key: str
  schema_short_name: str
  dataset_name: str
  materialization_type: str | None
  incremental_strategy: str | None
  historize: bool
  is_hist: bool
  active: bool
  former_names: tuple[str, ...] = ()
  column_states: tuple[ColumnState, ...] = ()

  def dataset_fingerprint_payload(self) -> dict[str, Any]:
    """
    Dataset-level semantic payload (excluding columns).
    """
    return {
      "dataset_key": self.dataset_key,
      "schema_short_name": self.schema_short_name,
      "dataset_name": self.dataset_name,
      "materialization_type": self.materialization_type,
      "incremental_strategy": self.incremental_strategy,
      "historize": self.historize,
      "is_hist": self.is_hist,
      "active": self.active,
      "former_names": list(sorted(_as_str_tuple(self.former_names))),
    }

  def columns_fingerprint_payload(self) -> list[dict[str, Any]]:
    """
    Column-level semantic payload.
    """
    return [c.fingerprint_payload() for c in self.column_states]

  def fingerprint_payload(self) -> dict[str, Any]:
    """
    Full dataset payload (dataset + columns).
    """
    return {
      "dataset": self.dataset_fingerprint_payload(),
      "columns": self.columns_fingerprint_payload(),
    }

  @property
  def dataset_fingerprint(self) -> str:
    """
    Stable hash for dataset-level semantics (excluding columns).
    """
    return _stable_json_hash(self.dataset_fingerprint_payload())

  @property
  def columns_fingerprint(self) -> str:
    """
    Stable hash for column-level semantics.
    """
    return _stable_json_hash(self.columns_fingerprint_payload())

  @property
  def fingerprint(self) -> str:
    """
    Return a stable semantic hash of the full dataset state.
    """
    return _stable_json_hash(self.fingerprint_payload())

  @property
  def columns_by_name(self) -> dict[str, ColumnState]:
    """
    Return active and inactive columns indexed by physical column name.
    """
    return {col.column_name: col for col in self.column_states}

  @property
  def columns_by_lineage_key(self) -> dict[str, ColumnState]:
    """
    Return columns indexed by lineage_key where available.
    """
    return {
      col.lineage_key: col
      for col in self.column_states
      if col.lineage_key
    }


@dataclass(frozen=True)
class ArchitectureState:
  """
  Snapshot of the semantic runtime architecture for all relevant datasets.
  """
  datasets: tuple[DatasetState, ...] = field(default_factory=tuple)

  @property
  def datasets_by_key(self) -> dict[str, DatasetState]:
    """
    Return datasets indexed by stable dataset key.
    """
    return {ds.dataset_key: ds for ds in self.datasets}

  @property
  def fingerprint(self) -> str:
    """
    Return a stable semantic hash of the full architecture state.
    """
    payload = [ds.fingerprint_payload() for ds in self.datasets]
    return _stable_json_hash(payload)