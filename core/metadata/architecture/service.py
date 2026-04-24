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

from metadata.models import TargetDataset

from .state import ArchitectureState, DatasetState, ColumnState
from .diff import ArchitectureDiff, diff_architecture_states
from .store import ArchitectureStateStore


@dataclass
class ArchitectureStateService:
  """
  Build semantic architecture state from Django metadata models.

  This service is intentionally read-only and additive.
  It must not mutate metadata or trigger execution side effects.
  """

  def build_current_state(self) -> ArchitectureState:
    """
    Build the current architecture state for all active target datasets.
    """
    datasets = (
      TargetDataset.objects
      .select_related("target_schema")
      .prefetch_related("target_columns")
      .filter(active=True)
      .order_by("target_schema__short_name", "target_dataset_name", "id")
    )

    dataset_states = tuple(
      self.build_dataset_state(dataset)
      for dataset in datasets
    )
    return ArchitectureState(datasets=dataset_states)

  def build_dataset_state(self, dataset: TargetDataset) -> DatasetState:
    """
    Build semantic runtime state for a single target dataset.
    """
    schema = getattr(dataset, "target_schema", None)
    schema_short_name = getattr(schema, "short_name", "") or ""

    columns_qs = (
      dataset.target_columns
      .all()
      .order_by("ordinal_position", "id")
    )

    column_states = tuple(
      ColumnState(
        column_name=col.target_column_name,
        datatype=getattr(col, "datatype", None),
        nullable=bool(getattr(col, "nullable", True)),
        active=bool(getattr(col, "active", True)),
        lineage_key=getattr(col, "lineage_key", None),
        former_names=tuple(getattr(col, "former_names", []) or []),
        is_system_managed=bool(getattr(col, "is_system_managed", False)),
        system_role=getattr(col, "system_role", None),
      )
      for col in columns_qs
    )

    dataset_key = f"{schema_short_name}.{dataset.target_dataset_name}"

    return DatasetState(
      dataset_key=dataset_key,
      schema_short_name=schema_short_name,
      dataset_name=dataset.target_dataset_name,
      materialization_type=getattr(dataset, "materialization_type", None),
      incremental_strategy=getattr(dataset, "incremental_strategy", None),
      historize=bool(getattr(dataset, "historize", False)),
      is_hist=bool(getattr(dataset, "is_hist", False)),
      active=bool(getattr(dataset, "active", True)),
      former_names=tuple(getattr(dataset, "former_names", []) or []),
      column_states=column_states,
    )

  def diff_against(
    self,
    previous: ArchitectureState | None,
  ) -> tuple[ArchitectureState, ArchitectureDiff]:
    """
    Build the current state and diff it against a previous state.
    """
    current = self.build_current_state()
    diff = diff_architecture_states(previous=previous, current=current)
    return current, diff
  
  def load_previous_state(self) -> ArchitectureState | None:
    """
    Load the previously persisted architecture state.
    """
    store = ArchitectureStateStore()
    return store.load()

  def persist_state(self, state: ArchitectureState) -> None:
    """
    Persist the given architecture state.
    """
    store = ArchitectureStateStore()
    store.save(state)