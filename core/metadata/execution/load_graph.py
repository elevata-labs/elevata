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

from metadata.models import TargetSchema, TargetDataset


def resolve_execution_order(root: TargetDataset) -> list[TargetDataset]:
  graph = build_load_graph(root)
  return topological_sort(graph)

def resolve_execution_order_all(roots: list[TargetDataset]) -> list[TargetDataset]:
  """
  Resolve a deterministic execution order for multiple roots.

  Semantics:
  - Roots define the initial scope, but all required upstream dependencies
    are included (even if they live in other schemas).
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
  Resolved Target dataset may be None by design (federated / external)
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


def resolve_upstream_datasets(td: TargetDataset) -> set[TargetDataset]:
  """
  Resolve all immediate upstream TargetDatasets for a given TargetDataset.
  """
  # Dummy/test datasets may not have input_links; treat as leaf node.
  if not hasattr(td, "input_links"):
    return set()

  upstream = set()

  links = td.input_links.select_related(
    "upstream_target_dataset",
    "source_dataset",
  )

  for link in links:
    # TargetDataset → TargetDataset
    if link.upstream_target_dataset is not None:
      upstream.add(link.upstream_target_dataset)
      continue

    # SourceDataset → RAW TargetDataset (optional)
    if link.source_dataset is not None:
      raw_td = resolve_raw_dataset_for_source(link.source_dataset)
      if raw_td is not None:
        upstream.add(raw_td)
      # else: federated / external → no upstream dataset node

  return upstream


def build_load_graph(root: TargetDataset) -> dict[TargetDataset, set[TargetDataset]]:
  """
  Build a dependency graph starting from a root TargetDataset.
  Graph direction: dataset -> immediate upstream datasets
  """
  graph: dict[TargetDataset, set[TargetDataset]] = {}
  stack = [root]

  while stack:
    td = stack.pop()

    if td in graph:
      continue

    deps = resolve_upstream_datasets(td)
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

    for dep in sorted(
      graph.get(td, []),
      key=lambda d: (d.target_schema.short_name, d.target_dataset_name),
    ):
      visit(dep)

    result.append(td)

  for td in sorted(
    graph.keys(),
    key=lambda d: (d.target_schema.short_name, d.target_dataset_name),
  ):
    visit(td)

  return result
