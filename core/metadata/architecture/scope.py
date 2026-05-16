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

from typing import Any, Iterable

from metadata.architecture.state import ArchitectureState


class ArchitectureScopeError(ValueError):
  """
  Raised when an architecture scope cannot be resolved unambiguously.
  """


def split_dataset_key(dataset_key: str) -> tuple[str | None, str | None]:
  """
  Split a dataset key into schema short name and dataset name.
  """
  key = (dataset_key or "").strip()
  if not key or "." not in key:
    return None, None

  schema_short, dataset_name = key.split(".", 1)
  schema_short = schema_short.strip() or None
  dataset_name = dataset_name.strip() or None

  return schema_short, dataset_name


def dataset_key_from_execution_item(item: Any) -> str | None:
  """
  Return a stable dataset key for an execution or architecture scope item.
  """
  if isinstance(item, str):
    return item

  dataset_key = getattr(item, "dataset_key", None)
  if dataset_key:
    return str(dataset_key)

  target_schema = getattr(item, "target_schema", None)
  target_dataset_name = getattr(item, "target_dataset_name", None)
  schema_short = getattr(target_schema, "short_name", None)
  if schema_short and target_dataset_name:
    return f"{schema_short}.{target_dataset_name}"

  target_dataset = getattr(item, "target_dataset", None)
  if target_dataset is not None:
    nested_schema = getattr(target_dataset, "target_schema", None)
    nested_schema_short = getattr(nested_schema, "short_name", None)
    nested_dataset_name = getattr(target_dataset, "target_dataset_name", None)
    if nested_schema_short and nested_dataset_name:
      return f"{nested_schema_short}.{nested_dataset_name}"

  return None


def dataset_keys_from_execution_items(
  items: Iterable[Any],
  *,
  architecture_state: ArchitectureState | None = None,
  include_related_hist: bool = True,
) -> set[str]:
  """
  Resolve dataset keys from execution items.
  """
  keys = {
    dataset_key
    for item in items
    for dataset_key in [dataset_key_from_execution_item(item)]
    if dataset_key
  }

  if architecture_state is None or not include_related_hist:
    return keys

  return expand_dataset_keys_with_hist(
    state=architecture_state,
    dataset_keys=keys,
  )


def expand_dataset_keys_with_hist(
  *,
  state: ArchitectureState,
  dataset_keys: Iterable[str],
) -> set[str]:
  """
  Add related history dataset keys for historized base datasets.
  """
  by_key = state.datasets_by_key
  expanded = {
    str(dataset_key)
    for dataset_key in dataset_keys
    if dataset_key
  }

  for dataset_key in sorted(tuple(expanded)):
    dataset = by_key.get(dataset_key)
    if dataset is None:
      continue
    if not getattr(dataset, "historize", False):
      continue
    if getattr(dataset, "is_hist", False):
      continue

    candidates = (
      f"{dataset_key}_hist",
      f"{dataset.schema_short_name}.{dataset.dataset_name}_hist",
    )
    for candidate in candidates:
      if candidate in by_key:
        expanded.add(candidate)

  return expanded


def resolve_dataset_keys_from_state(
  *,
  state: ArchitectureState,
  target_name: str | None,
  schema_short: str | None,
  all_datasets: bool,
  include_related_hist: bool = True,
) -> set[str]:
  """
  Resolve architecture dataset keys from state and selection arguments.
  """
  candidates = [
    dataset
    for dataset in state.datasets
    if schema_short is None or dataset.schema_short_name == schema_short
  ]

  if all_datasets:
    keys = {
      dataset.dataset_key
      for dataset in candidates
    }
    if include_related_hist:
      return expand_dataset_keys_with_hist(
        state=state,
        dataset_keys=keys,
      )
    return keys

  if not target_name:
    raise ArchitectureScopeError("Specify a target dataset or use --all.")

  matches = _match_target_datasets(
    candidates=candidates,
    target_name=target_name,
  )

  if not matches:
    scope_hint = f" in schema {schema_short}" if schema_short else ""
    raise ArchitectureScopeError(f"Target dataset not found{scope_hint}: {target_name}")

  if len(matches) > 1:
    keys = ", ".join(sorted(dataset.dataset_key for dataset in matches))
    raise ArchitectureScopeError(
      f"Target dataset name is ambiguous: {target_name}. "
      f"Use --schema or a dataset key. Matches: {keys}"
    )

  keys = {matches[0].dataset_key}

  if include_related_hist:
    return expand_dataset_keys_with_hist(
      state=state,
      dataset_keys=keys,
    )

  return keys


def _match_target_datasets(
  *,
  candidates,
  target_name: str,
):
  """
  Match target datasets by dataset key or dataset name.
  """
  if "." in target_name:
    return [
      dataset
      for dataset in candidates
      if dataset.dataset_key == target_name
    ]

  return [
    dataset
    for dataset in candidates
    if dataset.dataset_name == target_name
  ]