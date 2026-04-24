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
from typing import Iterable

from .diff import ArchitectureDiff
from .migration_plan import MigrationAction, MigrationPlan
from .state import ArchitectureState


@dataclass
class MigrationPlanner:
  """
  Derive a semantic MigrationPlan from an ArchitectureDiff.

  Patch A: preview-only. No execution is wired to this plan yet.
  """

  def plan(
    self,
    arch_diff: ArchitectureDiff,
    *,
    relevant_dataset_keys: set[str] | None = None,
    architecture_state: ArchitectureState | None = None,
    expand_related_hist: bool = True,
  ) -> MigrationPlan:
    actions: list[MigrationAction] = []

    scope_keys = relevant_dataset_keys
    if (
      relevant_dataset_keys is not None
      and architecture_state is not None
      and expand_related_hist
    ):
      scope_keys = self._expand_scope_with_hist(
        architecture_state=architecture_state,
        relevant_dataset_keys=relevant_dataset_keys,
      )

    def _in_scope(dataset_key: str) -> bool:
      if scope_keys is None:
        return True
      return dataset_key in scope_keys

    # Dataset-level changes
    for ch in arch_diff.dataset_changes:
      if not _in_scope(ch.dataset_key):
        continue

      if ch.change_type == "DATASET_RENAMED":
        actions.append(MigrationAction(
          action_type="RENAME_DATASET",
          strategy="RENAME_TABLE",
          dataset_key=ch.dataset_key,
          previous_dataset_key=ch.previous_dataset_name,
          reason="Architecture diff detected dataset rename (former_names match).",
        ))
      elif ch.change_type == "DATASET_ADDED":
        actions.append(MigrationAction(
          action_type="CREATE_DATASET",
          strategy="CREATE_TABLE",
          dataset_key=ch.dataset_key,
          reason="Architecture diff detected new dataset.",
        ))
      elif ch.change_type == "DATASET_REMOVED":
        actions.append(MigrationAction(
          action_type="DROP_DATASET",
          strategy="DROP_TABLE",
          dataset_key=ch.dataset_key,
          reason="Architecture diff detected removed dataset.",
        ))
      elif ch.change_type == "DATASET_CHANGED":
        # Conservative default: changed dataset-level semantics may require ALTER/REBUILD.
        actions.append(MigrationAction(
          action_type="REBUILD_DATASET",
          strategy="REBUILD",
          dataset_key=ch.dataset_key,
          reason="Dataset-level semantics changed (materialization/incremental/historization/etc.).",
        ))

    # Column-level changes
    for ch in arch_diff.column_changes:
      if not _in_scope(ch.dataset_key):
        continue

      if ch.change_type == "COLUMN_RENAMED":
        actions.append(MigrationAction(
          action_type="RENAME_COLUMN",
          strategy="RENAME_COLUMN",
          dataset_key=ch.dataset_key,
          column_name=ch.column_name,
          previous_column_name=ch.previous_column_name,
          reason="Architecture diff detected column rename (former_names/lineage_key match).",
        ))
      elif ch.change_type == "COLUMN_ADDED":
        actions.append(MigrationAction(
          action_type="ADD_COLUMN",
          strategy="ALTER_TABLE",
          dataset_key=ch.dataset_key,
          column_name=ch.column_name,
          reason="Architecture diff detected added column.",
        ))
      elif ch.change_type == "COLUMN_REMOVED":
        actions.append(MigrationAction(
          action_type="DROP_COLUMN",
          strategy="ALTER_TABLE",
          dataset_key=ch.dataset_key,
          column_name=ch.column_name,
          reason="Architecture diff detected removed column.",
        ))
      elif ch.change_type == "COLUMN_CHANGED":
        actions.append(MigrationAction(
          action_type="ALTER_COLUMN",
          strategy="ALTER_TABLE",
          dataset_key=ch.dataset_key,
          column_name=ch.column_name,
          reason="Architecture diff detected semantic column change (type/nullability/etc.).",
        ))

    return MigrationPlan(actions=tuple(actions))


  def _expand_scope_with_hist(
    self,
    *,
    architecture_state: ArchitectureState,
    relevant_dataset_keys: set[str],
  ) -> set[str]:
    """
    Expand execution scope with related historization datasets.

    In elevata, a dataset may implicitly manage a corresponding *_hist dataset.
    When a user runs --no-deps, execution_order may not include the *_hist dataset,
    but migration preview should still surface related actions.
    """
    expanded = set(relevant_dataset_keys)
    by_key = architecture_state.datasets_by_key

    for base_key in list(relevant_dataset_keys):
      ds = by_key.get(base_key)
      if ds is None:
        continue

      # Only expand from base datasets that historize (not from hist datasets themselves)
      if not getattr(ds, "historize", False):
        continue
      if getattr(ds, "is_hist", False):
        continue

      # Heuristic candidate keys (only add if present in current architecture state)
      candidates = [
        f"{base_key}_hist",
        f"{ds.schema_short_name}.{ds.dataset_name}_hist",
      ]
      for cand in candidates:
        if cand in by_key:
          expanded.add(cand)

      # Secondary heuristic: search for matching schema + name suffix
      expected_name = f"{ds.dataset_name}_hist"
      for cand_ds in architecture_state.datasets:
        if (
          getattr(cand_ds, "is_hist", False)
          and cand_ds.schema_short_name == ds.schema_short_name
          and cand_ds.dataset_name == expected_name
        ):
          expanded.add(cand_ds.dataset_key)

    return expanded