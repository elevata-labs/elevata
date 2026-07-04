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

from dataclasses import dataclass

from metadata.models import TargetSchema, TargetDataset


EXECUTION_DEPENDENCY_LINEAGE_INPUT = "lineage_input"
EXECUTION_DEPENDENCY_SOURCE_RAW_READY = "source_raw_ready"


@dataclass(frozen=True)
class ExecutionDependency:
  """
  One immediate execution dependency for a TargetDataset.

  Execution dependencies are scheduling dependencies. They may overlap with
  semantic lineage, but they are intentionally modeled separately so runtime
  concerns such as reference member readiness can be added without turning them
  into lineage edges.
  """
  upstream: TargetDataset
  reason: str
  reference_id: int | None = None


def _dataset_sort_key(td: TargetDataset) -> tuple[str, str]:
  """
  Return the deterministic ordering key for TargetDataset-shaped objects.
  """
  return (td.target_schema.short_name, td.target_dataset_name)


def _same_dataset(left: TargetDataset, right: TargetDataset) -> bool:
  """
  Return whether two TargetDataset-shaped objects refer to the same dataset.
  """
  left_id = getattr(left, "id", None)
  right_id = getattr(right, "id", None)
  if left_id is not None and right_id is not None:
    return left_id == right_id

  return _dataset_sort_key(left) == _dataset_sort_key(right)

 
def resolve_execution_order(root: TargetDataset) -> list[TargetDataset]:
  graph = build_load_graph(root)
  return topological_sort(graph)


def resolve_execution_order_all(roots: list[TargetDataset]) -> list[TargetDataset]:
  """
  Resolve a deterministic execution order for multiple roots.

  Semantics:
  - Roots define the initial scope, but all required upstream execution
    dependencies are included (even if they live in other schemas).
  - Deterministic ordering is guaranteed via topological_sort() sorting keys.
  """
  graph: dict[TargetDataset, set[TargetDataset]] = {}
  for r in (roots or []):
    try:
      graph.update(build_load_graph(r))
    except Exception:
      # Best-effort: graph building should never block orchestration.
      # If a root cannot be resolved, we simply skip it here; caller can decide
      # how to handle an empty plan.
      continue
  return topological_sort(graph)


def resolve_raw_dataset_for_source(source_dataset) -> TargetDataset | None:
  """
  Resolve the raw TargetDataset for a SourceDataset.

  The result may be None by design for federated or external source setups.
  """
  raw_schema = TargetSchema.objects.get(short_name="raw")

  td = (
    TargetDataset.objects
    .filter(
      target_schema=raw_schema,
      source_datasets=source_dataset,
    )
    .distinct()
    .first()
  )

  return td


def resolve_execution_dependencies(td: TargetDataset) -> tuple[ExecutionDependency, ...]:
  """
  Resolve immediate execution dependencies for a TargetDataset.

  Execution dependencies drive scheduling. Today they are derived from modeled
  upstream inputs and optional SourceDataset -> raw TargetDataset readiness.
  Future reference-member dependencies can be added here without changing
  semantic lineage.
  """
  # Dummy/test datasets may not have input_links; treat as leaf node.
  if not hasattr(td, "input_links"):
    return ()

  dependencies: list[ExecutionDependency] = []
  seen: set[tuple[str, str, int | None]] = set()

  def add_dependency(
    upstream: TargetDataset | None,
    *,
    reason: str,
    reference_id: int | None = None,
  ) -> None:
    """
    Add a dependency once, skipping self-dependencies defensively.
    """
    if upstream is None:
      return

    if _same_dataset(td, upstream):
      return

    upstream_key = f"{upstream.target_schema.short_name}.{upstream.target_dataset_name}"
    key = (upstream_key, reason, reference_id)
    if key in seen:
      return

    seen.add(key)
    dependencies.append(ExecutionDependency(
      upstream=upstream,
      reason=reason,
      reference_id=reference_id,
    ))

  links = td.input_links.select_related(
    "upstream_target_dataset",
    "source_dataset",
  )

  for link in links:
    # TargetDataset -> TargetDataset execution dependency.
    if link.upstream_target_dataset is not None:
      add_dependency(
        link.upstream_target_dataset,
        reason=EXECUTION_DEPENDENCY_LINEAGE_INPUT,
      )
      continue

    # SourceDataset -> raw TargetDataset readiness dependency (optional).
    if link.source_dataset is not None:
      raw_td = resolve_raw_dataset_for_source(link.source_dataset)
      add_dependency(
        raw_td,
        reason=EXECUTION_DEPENDENCY_SOURCE_RAW_READY,
      )
      # raw_td may be None for federated / external source setups.

  return tuple(sorted(
    dependencies,
    key=lambda dep: (_dataset_sort_key(dep.upstream), dep.reason, dep.reference_id or 0),
  ))


def resolve_execution_upstream_datasets(td: TargetDataset) -> set[TargetDataset]:
  """
  Resolve all immediate upstream TargetDatasets required for execution.
  """
  return {dep.upstream for dep in resolve_execution_dependencies(td)}


def resolve_upstream_datasets(td: TargetDataset) -> set[TargetDataset]:
  """
  Resolve all immediate upstream TargetDatasets for execution.

  Kept as a compatibility alias for existing callers. New code should prefer
  resolve_execution_dependencies() when dependency reasons are needed, or
  resolve_execution_upstream_datasets() when only TargetDatasets are needed.
  """
  return resolve_execution_upstream_datasets(td)


def build_load_graph(root: TargetDataset) -> dict[TargetDataset, set[TargetDataset]]:
  """
  Build an execution dependency graph starting from a root TargetDataset.

  Graph direction: dataset -> immediate upstream execution dependencies.
  """
  graph: dict[TargetDataset, set[TargetDataset]] = {}
  stack = [root]

  while stack:
    td = stack.pop()

    if td in graph:
      continue

    deps = resolve_execution_upstream_datasets(td)
    graph[td] = deps
    stack.extend(deps)

  return graph


def topological_sort(graph: dict[TargetDataset, set[TargetDataset]]) -> list[TargetDataset]:
  """
  Return datasets in execution order (upstreams first).
  """
  visited = set()
  result: list[TargetDataset] = []

  def visit(td):
    if td in visited:
      return
    visited.add(td)

    for dep in sorted(graph.get(td, []), key=_dataset_sort_key):
      visit(dep)

    result.append(td)

  for td in sorted(graph.keys(), key=_dataset_sort_key):
    visit(td)

  return result


def topological_levels(graph: dict[TargetDataset, set[TargetDataset]]) -> list[list[TargetDataset]]:
  """
  Return datasets in parallelizable levels (upstreams first).

  Each level contains nodes whose dependencies are fully satisfied by earlier levels.
  Deterministic ordering is guaranteed via sorting keys.
  """
  nodes = set(graph.keys())
  for deps in graph.values():
    nodes.update(deps)

  deps_left: dict[TargetDataset, set[TargetDataset]] = {n: set(graph.get(n, set())) for n in nodes}
  reverse: dict[TargetDataset, set[TargetDataset]] = {n: set() for n in nodes}

  for n, deps in deps_left.items():
    for d in deps:
      reverse.setdefault(d, set()).add(n)

  levels: list[list[TargetDataset]] = []
  visited: set[TargetDataset] = set()

  ready = sorted(
    [n for n in nodes if not deps_left.get(n)],
    key=_dataset_sort_key,
  )

  while ready:
    level = list(ready)
    levels.append(level)

    next_ready: set[TargetDataset] = set()
    for n in level:
      visited.add(n)
      for child in reverse.get(n, set()):
        if child in visited:
          continue
        deps_left[child].discard(n)
        if not deps_left[child]:
          next_ready.add(child)

    ready = sorted(next_ready, key=_dataset_sort_key)

  if len(visited) != len(nodes):
    remaining = sorted(nodes - visited, key=_dataset_sort_key)
    raise ValueError(
      "Cycle detected in dataset execution graph: "
      + ", ".join(f"{d.target_schema.short_name}.{d.target_dataset_name}" for d in remaining)
    )

  return levels
