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
from typing import Any, Iterable, Literal

from .state import ArchitectureState, ColumnState, DatasetState


DatasetChangeType = Literal[
  "DATASET_ADDED",
  "DATASET_REMOVED",
  "DATASET_RENAMED",
  "DATASET_CHANGED",
]

ColumnChangeType = Literal[
  "COLUMN_ADDED",
  "COLUMN_REMOVED",
  "COLUMN_RENAMED",
  "COLUMN_CHANGED",
]


@dataclass(frozen=True)
class ColumnChange:
  """
  Semantic change detected for a target column.
  """
  dataset_key: str
  change_type: ColumnChangeType
  column_name: str
  previous_column_name: str | None = None
  details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DatasetChange:
  """
  Semantic change detected for a target dataset.
  """
  dataset_key: str
  change_type: DatasetChangeType
  dataset_name: str
  previous_dataset_name: str | None = None
  details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ArchitectureDiff:
  """
  Semantic diff between two architecture states.
  """
  dataset_changes: tuple[DatasetChange, ...] = ()
  column_changes: tuple[ColumnChange, ...] = ()

  def has_changes(self) -> bool:
    """
    Return True if any dataset- or column-level changes were detected.
    """
    return bool(self.dataset_changes or self.column_changes)

  def affects_dataset(self, dataset_key: str) -> bool:
    """
    Return True if the diff contains any change for the given dataset.
    """
    return any(ch.dataset_key == dataset_key for ch in self.dataset_changes) or any(
      ch.dataset_key == dataset_key for ch in self.column_changes
    )


def diff_architecture_states(
  previous: ArchitectureState | None,
  current: ArchitectureState,
) -> ArchitectureDiff:
  """
  Compare two architecture states and return a semantic diff.

  The comparison is deliberately conservative:
  - dataset matching uses dataset_key
  - column matching prefers lineage_key if available, then column_name
  - rename detection uses former_names and lineage_key heuristics
  """
  if previous is None:
    dataset_changes = tuple(
      DatasetChange(
        dataset_key=ds.dataset_key,
        change_type="DATASET_ADDED",
        dataset_name=ds.dataset_name,
      )
      for ds in current.datasets
    )
    column_changes = tuple(
      ColumnChange(
        dataset_key=ds.dataset_key,
        change_type="COLUMN_ADDED",
        column_name=col.column_name,
      )
      for ds in current.datasets
      for col in ds.column_states
    )
    return ArchitectureDiff(
      dataset_changes=dataset_changes,
      column_changes=column_changes,
    )

  previous_by_key = previous.datasets_by_key
  current_by_key = current.datasets_by_key

  dataset_changes: list[DatasetChange] = []
  column_changes: list[ColumnChange] = []

  previous_keys = set(previous_by_key.keys())
  current_keys = set(current_by_key.keys())

  added_keys = set(current_keys - previous_keys)
  removed_keys = set(previous_keys - current_keys)

  # --- detect dataset renames across dataset_key changes ---
  # If a dataset was renamed, dataset_key changes (schema.short_name + dataset_name),
  # so it would show up as REMOVED + ADDED unless we reconcile it here.
  rename_pairs: list[tuple[str, str]] = []
  remaining_added = set(added_keys)
  remaining_removed = set(removed_keys)

  for new_key in sorted(added_keys):
    new_ds = current_by_key[new_key]
    new_former_names = _as_str_tuple(getattr(new_ds, "former_names", None))

    for old_key in sorted(remaining_removed):
      old_ds = previous_by_key[old_key]

      # Only match within the same schema namespace.
      if old_ds.schema_short_name != new_ds.schema_short_name:
        continue

      # former_names is usually plain dataset_name, but we also accept fully qualified keys.
      if (
        old_ds.dataset_name in new_former_names
        or old_key in new_former_names
      ):
        rename_pairs.append((old_key, new_key))
        remaining_removed.remove(old_key)
        remaining_added.remove(new_key)
        break

  added_keys = remaining_added
  removed_keys = remaining_removed

  # Emit rename events first, and diff their columns under the new dataset_key.
  for old_key, new_key in rename_pairs:
    prev_ds = previous_by_key[old_key]
    curr_ds = current_by_key[new_key]

    dataset_changes.append(
      DatasetChange(
        dataset_key=new_key,
        change_type="DATASET_RENAMED",
        dataset_name=curr_ds.dataset_name,
        # Store the fully qualified previous key so CLI output is unambiguous.
        previous_dataset_name=old_key,
      )
    )

    # Column changes will often be renames (esp. system-managed key columns),
    # but they should never appear as pure "added" due to the dataset rename.
    column_changes.extend(_diff_dataset_columns(prev_ds, curr_ds))

  for dataset_key in sorted(added_keys):
    ds = current_by_key[dataset_key]
    dataset_changes.append(
      DatasetChange(
        dataset_key=dataset_key,
        change_type="DATASET_ADDED",
        dataset_name=ds.dataset_name,
      )
    )
    for col in ds.column_states:
      column_changes.append(
        ColumnChange(
          dataset_key=dataset_key,
          change_type="COLUMN_ADDED",
          column_name=col.column_name,
        )
      )

  for dataset_key in sorted(removed_keys):
    ds = previous_by_key[dataset_key]
    dataset_changes.append(
      DatasetChange(
        dataset_key=dataset_key,
        change_type="DATASET_REMOVED",
        dataset_name=ds.dataset_name,
      )
    )
    for col in ds.column_states:
      column_changes.append(
        ColumnChange(
          dataset_key=dataset_key,
          change_type="COLUMN_REMOVED",
          column_name=col.column_name,
        )
      )

  for dataset_key in sorted(previous_keys & current_keys):
    prev_ds = previous_by_key[dataset_key]
    curr_ds = current_by_key[dataset_key]

    curr_former_names = _as_str_tuple(getattr(curr_ds, "former_names", None))    
    dataset_renamed = (
      prev_ds.dataset_name != curr_ds.dataset_name
      and (
        prev_ds.dataset_name in curr_former_names
        or prev_ds.dataset_key in curr_former_names
      )
    )

    if dataset_renamed:
      dataset_changes.append(
        DatasetChange(
          dataset_key=dataset_key,
          change_type="DATASET_RENAMED",
          dataset_name=curr_ds.dataset_name,
          previous_dataset_name=prev_ds.dataset_name,
        )
      )

    prev_ds_fp = getattr(prev_ds, "dataset_fingerprint", prev_ds.fingerprint)
    curr_ds_fp = getattr(curr_ds, "dataset_fingerprint", curr_ds.fingerprint)

    if prev_ds_fp != curr_ds_fp and not dataset_renamed:
      dataset_changes.append(
        DatasetChange(
          dataset_key=dataset_key,
          change_type="DATASET_CHANGED",
          dataset_name=curr_ds.dataset_name,
          details={
            "previous_fingerprint": prev_ds_fp,
            "current_fingerprint": curr_ds_fp,
          },
        )
      )

    column_changes.extend(_diff_dataset_columns(prev_ds, curr_ds))

  return ArchitectureDiff(
    dataset_changes=tuple(dataset_changes),
    column_changes=tuple(column_changes),
  )


def _diff_dataset_columns(
  previous: DatasetState,
  current: DatasetState,
) -> list[ColumnChange]:
  """
  Return semantic column changes for a single dataset.
  """
  changes: list[ColumnChange] = []

  matched, removed_columns, added_columns = _match_columns(
    list(previous.column_states),
    list(current.column_states),
  )

  for prev_col, curr_col in matched:
    renamed = _is_column_rename(prev_col, curr_col)

    if renamed:
      changes.append(
        ColumnChange(
          dataset_key=current.dataset_key,
          change_type="COLUMN_RENAMED",
          column_name=curr_col.column_name,
          previous_column_name=prev_col.column_name,
        )
      )

    prev_payload = _normalize_column_payload(prev_col.fingerprint_payload())
    curr_payload = _normalize_column_payload(curr_col.fingerprint_payload())

    if renamed:
      prev_payload = {
        **prev_payload,
        "column_name": curr_col.column_name,
        "former_names": list(curr_payload.get("former_names", [])),
      }

    comparable_prev_payload = {
      k: v for k, v in prev_payload.items()
      if k not in {"former_names"}
    }
    comparable_curr_payload = {
      k: v for k, v in curr_payload.items()
      if k not in {"former_names"}
    }

    if comparable_prev_payload != comparable_curr_payload:
      name_only_change = renamed and {
        k: v for k, v in comparable_prev_payload.items()
        if k != "column_name"
      } == {
        k: v for k, v in comparable_curr_payload.items()
        if k != "column_name"
      }

      if not name_only_change:
        changes.append(
          ColumnChange(
            dataset_key=current.dataset_key,
            change_type="COLUMN_CHANGED",
            column_name=curr_col.column_name,
            details={
              "previous_fingerprint": prev_col.fingerprint,
              "current_fingerprint": curr_col.fingerprint,
            },
          )
        )

  for col in removed_columns:
    changes.append(
      ColumnChange(
        dataset_key=current.dataset_key,
        change_type="COLUMN_REMOVED",
        column_name=col.column_name,
      )
    )

  for col in added_columns:
    changes.append(
      ColumnChange(
        dataset_key=current.dataset_key,
        change_type="COLUMN_ADDED",
        column_name=col.column_name,
      )
    )

  return changes


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


def _match_columns(
  previous: list[ColumnState],
  current: list[ColumnState],
) -> tuple[list[tuple[ColumnState, ColumnState]], list[ColumnState], list[ColumnState]]:
  """
  Match previous and current columns using stable semantic identity.

  Matching priority:
  1. lineage_key
  2. exact column_name
  3. former_names-based rename detection
  """
  matched: list[tuple[ColumnState, ColumnState]] = []

  remaining_previous = list(previous)
  remaining_current = list(current)

  # 1. Match by lineage_key
  prev_by_lineage = {
    c.lineage_key: c
    for c in remaining_previous
    if c.lineage_key
  }
  curr_by_lineage = {
    c.lineage_key: c
    for c in remaining_current
    if c.lineage_key
  }

  shared_lineage_keys = set(prev_by_lineage.keys()) & set(curr_by_lineage.keys())
  for key in sorted(shared_lineage_keys):
    prev_col = prev_by_lineage[key]
    curr_col = curr_by_lineage[key]
    matched.append((prev_col, curr_col))
    remaining_previous.remove(prev_col)
    remaining_current.remove(curr_col)

  # 2. Match by exact current name
  prev_by_name = {c.column_name: c for c in remaining_previous}
  curr_by_name = {c.column_name: c for c in remaining_current}
  shared_names = set(prev_by_name.keys()) & set(curr_by_name.keys())

  for name in sorted(shared_names):
    prev_col = prev_by_name[name]
    curr_col = curr_by_name[name]
    matched.append((prev_col, curr_col))
    remaining_previous.remove(prev_col)
    remaining_current.remove(curr_col)

  # 3. Match by former_names / rename detection
  rename_matches: list[tuple[ColumnState, ColumnState]] = []

  consumed_previous: set[str] = set()
  consumed_current: set[str] = set()

  for curr_col in remaining_current:
    for prev_col in remaining_previous:
      if prev_col.column_name in consumed_previous:
        continue
      if curr_col.column_name in consumed_current:
        continue

      curr_former_names = _as_str_tuple(getattr(curr_col, "former_names", None))

      if prev_col.column_name in curr_former_names:
        rename_matches.append((prev_col, curr_col))
        consumed_previous.add(prev_col.column_name)
        consumed_current.add(curr_col.column_name)
        break

  for prev_col, curr_col in rename_matches:
    if prev_col in remaining_previous:
      remaining_previous.remove(prev_col)
    if curr_col in remaining_current:
      remaining_current.remove(curr_col)
    matched.append((prev_col, curr_col))

  return matched, remaining_previous, remaining_current


def _is_column_rename(previous: ColumnState, current: ColumnState) -> bool:
  """
  Return True if the current column is a semantic rename of the previous one.
  """
  if previous.column_name == current.column_name:
    return False

  current_former_names = _as_str_tuple(getattr(current, "former_names", None))
  if previous.column_name in current_former_names:
    return True

  if previous.lineage_key and previous.lineage_key == current.lineage_key:
    return True

  return False


def _normalize_column_payload(payload: dict[str, Any]) -> dict[str, Any]:
  """
  Normalize payload fields that may vary in representation but not meaning.
  """
  normalized = dict(payload)
  # former_names can vary in shape and ordering; treat as sorted tuple
  normalized["former_names"] = tuple(sorted(_as_str_tuple(payload.get("former_names"))))
  # system_role may appear as "" or None; normalize to ""
  if normalized.get("system_role") is None:
    normalized["system_role"] = ""
  return normalized


def _index_columns(columns: list[ColumnState]) -> dict[str, ColumnState]:
  """
  Index columns by stable identity.

  Matching priority:
  1. lineage_key
  2. column_name
  """
  indexed: dict[str, ColumnState] = {}

  for col in columns:
    key = f"lineage:{col.lineage_key}" if col.lineage_key else f"name:{col.column_name}"
    indexed[key] = col

  return indexed