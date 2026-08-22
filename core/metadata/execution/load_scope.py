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
from typing import Literal

from metadata.models import PartialLoad, TargetDataset
from metadata.execution.load_graph import (
  build_load_graph,
  topological_levels,
  topological_sort,
)


LOAD_SCOPE_MODE_PARTIAL_LOAD = "partial_load"
FULL_LOAD_SCOPE_NAME = "full"

LoadScopeMode = Literal["partial_load"]


class LoadScopeError(ValueError):
  """Raised when a reusable execution scope cannot be resolved safely."""


@dataclass(frozen=True)
class ResolvedLoadScope:
  """
  Deterministic resolved execution scope for one named load definition.

  `roots` preserve explicit user intent. `execution_order` is the deduplicated
  runtime scope after resolving all required upstream dependencies and mandatory
  execution companions such as historization datasets.
  """
  scope_mode: LoadScopeMode
  scope_key: str
  roots: tuple[TargetDataset, ...]
  execution_order: tuple[TargetDataset, ...]

  @property
  def root_dataset_keys(self) -> tuple[str, ...]:
    """Return explicit execution roots in deterministic key order."""
    return tuple(_dataset_key(td) for td in self.roots)

  @property
  def execution_dataset_keys(self) -> tuple[str, ...]:
    """Return the exact resolved execution sequence as portable dataset keys."""
    return tuple(_dataset_key(td) for td in self.execution_order)



def resolve_partial_load_scope(
  partial_load: PartialLoad | str,
) -> ResolvedLoadScope:
  """
  Resolve one PartialLoad into its exact execution roots and runtime scope.

  Contract:
  - PartialLoad assignments are explicit TargetDataset execution roots.
  - Required upstream execution dependencies are included automatically.
  - Mandatory runtime companions (for example *_hist) are included by the
    canonical execution graph.
  - Ordinary downstream consumers are never added implicitly.
  - Explicit roots remain part of the definition even when one is currently
    redundant because another root already depends on it.
  - Resolution is fail-closed. No declared root may be silently omitted.
  """
  partial_load_obj = _resolve_partial_load(partial_load)
  _validate_partial_load_identity(partial_load_obj)

  roots = _partial_load_roots(partial_load_obj)
  _validate_partial_load_roots(partial_load_obj, roots)

  graph: dict[TargetDataset, set[TargetDataset]] = {}
  for root in roots:
    try:
      root_graph = build_load_graph(root)
    except Exception as exc:
      raise LoadScopeError(
        "Partial Load execution scope could not be resolved for root "
        f"'{_dataset_key(root)}' in '{partial_load_obj.name}': {exc}"
      ) from exc

    if root not in root_graph:
      raise LoadScopeError(
        "Partial Load execution scope resolution omitted an explicit root: "
        f"'{_dataset_key(root)}' in '{partial_load_obj.name}'."
      )

    for dataset, dependencies in root_graph.items():
      graph.setdefault(dataset, set()).update(dependencies)

  try:
    # topological_levels() is used as an explicit cycle/integrity validation.
    # The canonical linear execution sequence remains topological_sort().
    topological_levels(graph)
    execution_order = tuple(topological_sort(graph))
  except Exception as exc:
    raise LoadScopeError(
      f"Partial Load '{partial_load_obj.name}' has an invalid execution graph: {exc}"
    ) from exc

  resolved_keys = {_dataset_key(td) for td in execution_order}
  missing_roots = [
    _dataset_key(root)
    for root in roots
    if _dataset_key(root) not in resolved_keys
  ]
  if missing_roots:
    raise LoadScopeError(
      "Partial Load execution scope omitted explicit root dataset(s): "
      + ", ".join(sorted(missing_roots))
      + "."
    )

  return ResolvedLoadScope(
    scope_mode=LOAD_SCOPE_MODE_PARTIAL_LOAD,
    scope_key=partial_load_obj.name,
    roots=roots,
    execution_order=execution_order,
  )



def _resolve_partial_load(partial_load: PartialLoad | str) -> PartialLoad:
  """Resolve a persisted PartialLoad object from an object or exact name."""
  if isinstance(partial_load, PartialLoad):
    if partial_load.pk is None:
      raise LoadScopeError("Partial Load must be persisted before it can be resolved.")
    return partial_load

  name = str(partial_load or "").strip()
  if not name:
    raise LoadScopeError("Partial Load name must not be empty.")

  try:
    return PartialLoad.objects.get(name=name)
  except PartialLoad.DoesNotExist as exc:
    raise LoadScopeError(f"Partial Load '{name}' does not exist.") from exc
  except PartialLoad.MultipleObjectsReturned as exc:
    raise LoadScopeError(
      f"Partial Load name '{name}' is ambiguous and cannot be resolved safely."
    ) from exc



def _validate_partial_load_identity(partial_load: PartialLoad) -> None:
  """Validate the reusable load identity before resolving dataset membership."""
  name = str(getattr(partial_load, "name", "") or "").strip()
  if not name:
    raise LoadScopeError("Partial Load name must not be empty.")

  if name.casefold() == FULL_LOAD_SCOPE_NAME:
    raise LoadScopeError(
      f"Partial Load name '{name}' is reserved for the implicit Full Load scope."
    )



def _partial_load_roots(partial_load: PartialLoad) -> tuple[TargetDataset, ...]:
  """Return explicit TargetDataset roots without filtering invalid assignments."""
  try:
    roots = list(
      partial_load.datasets
      .select_related("target_schema")
      .order_by(
        "target_schema__short_name",
        "target_dataset_name",
        "id",
      )
    )
  except Exception as exc:
    raise LoadScopeError(
      f"Partial Load '{partial_load.name}' root assignments could not be read: {exc}"
    ) from exc

  return tuple(roots)



def _validate_partial_load_roots(
  partial_load: PartialLoad,
  roots: tuple[TargetDataset, ...],
) -> None:
  """Validate explicit user-defined roots before execution-scope expansion."""
  if not roots:
    raise LoadScopeError(
      f"Partial Load '{partial_load.name}' must define at least one execution root."
    )

  seen_keys: set[str] = set()
  for root in roots:
    dataset_key = _dataset_key(root)
    if dataset_key in seen_keys:
      raise LoadScopeError(
        f"Partial Load '{partial_load.name}' contains duplicate root '{dataset_key}'."
      )
    seen_keys.add(dataset_key)

    if not bool(getattr(root, "active", True)):
      raise LoadScopeError(
        f"Partial Load '{partial_load.name}' contains inactive execution root "
        f"'{dataset_key}'."
      )

    if bool(getattr(root, "is_hist", False)):
      raise LoadScopeError(
        f"Partial Load '{partial_load.name}' cannot use system-managed history "
        f"dataset '{dataset_key}' as an explicit execution root. Select the base "
        "or a downstream dataset instead; history companions are resolved automatically."
      )



def _dataset_key(target_dataset: TargetDataset) -> str:
  """Return the portable schema-qualified TargetDataset execution key."""
  schema_short = str(
    getattr(getattr(target_dataset, "target_schema", None), "short_name", "")
    or ""
  ).strip()
  dataset_name = str(
    getattr(target_dataset, "target_dataset_name", "") or ""
  ).strip()

  if not schema_short or not dataset_name:
    raise LoadScopeError(
      "Partial Load execution roots require TargetDataset schema and dataset names."
    )

  return f"{schema_short}.{dataset_name}"
