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
EXECUTION_DEPENDENCY_REFERENCE_PARENT_READY = "reference_parent_ready"
EXECUTION_DEPENDENCY_HIST_BASE_READY = "hist_base_ready"


class ExecutionGraphError(ValueError):
  """Raised when execution dependencies cannot be resolved deterministically."""



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


def _effective_historize(td: TargetDataset) -> bool:
  """Return whether historization is enabled for a base dataset."""
  schema = getattr(td, "target_schema", None)
  return bool(
    getattr(td, "historize", False)
    or getattr(schema, "default_historize", False)
  )


def _is_rawcore_base_dataset(td: TargetDataset) -> bool:
  """Return whether this dataset can own a system-managed history companion."""
  schema_short = getattr(getattr(td, "target_schema", None), "short_name", None)
  return (
    schema_short == "rawcore"
    and not bool(getattr(td, "is_hist", False))
  )


def _dataset_label(td: TargetDataset) -> str:
  return f"{td.target_schema.short_name}.{td.target_dataset_name}"


def resolve_hist_companion_for_base(td: TargetDataset) -> TargetDataset | None:
  """
  Resolve the mandatory history companion for a historized rawcore base dataset.

  The generated metadata contract identifies the companion by the same lineage_key
  and the *_hist naming convention. Name lookup remains as the legacy fallback used
  by target generation when lineage_key is unavailable.
  """
  if not _is_rawcore_base_dataset(td) or not _effective_historize(td):
    return None

  schema = td.target_schema
  expected_name = f"{td.target_dataset_name}_hist"
  lineage_key = str(getattr(td, "lineage_key", "") or "").strip()

  hist_td = None
  if lineage_key:
    candidates = list(
      TargetDataset.objects
      .filter(
        target_schema=schema,
        lineage_key=lineage_key,
        target_dataset_name__endswith="_hist",
      )
      .exclude(pk=getattr(td, "pk", None))
      .order_by("target_dataset_name", "id")[:2]
    )
    if len(candidates) > 1:
      raise ExecutionGraphError(
        "Historized base dataset has multiple history companions for the same "
        f"lineage identity: {_dataset_label(td)} -> "
        + ", ".join(_dataset_label(item) for item in candidates)
        + "."
      )
    if candidates:
      hist_td = candidates[0]

  if hist_td is None:
    hist_td = (
      TargetDataset.objects
      .filter(
        target_schema=schema,
        target_dataset_name=expected_name,
      )
      .exclude(pk=getattr(td, "pk", None))
      .first()
    )

  if hist_td is None:
    raise ExecutionGraphError(
      "Historized base dataset requires a history companion in the execution "
      f"scope, but none exists: {_dataset_label(td)} -> "
      f"{schema.short_name}.{expected_name}. Synchronize target metadata before execution."
    )

  if hist_td.target_dataset_name != expected_name:
    raise ExecutionGraphError(
      "History companion name is not synchronized with its base dataset: "
      f"{_dataset_label(td)} -> {_dataset_label(hist_td)}; "
      f"expected {schema.short_name}.{expected_name}. Synchronize target metadata before execution."
    )

  if not bool(getattr(hist_td, "is_hist", False)):
    raise ExecutionGraphError(
      "Resolved history companion does not use the historize load contract: "
      f"{_dataset_label(td)} -> {_dataset_label(hist_td)}."
    )

  if not bool(getattr(hist_td, "active", True)):
    raise ExecutionGraphError(
      "Historized base dataset requires an active history companion: "
      f"{_dataset_label(td)} -> {_dataset_label(hist_td)}."
    )

  companion_lineage_key = str(getattr(hist_td, "lineage_key", "") or "").strip()
  if lineage_key and companion_lineage_key and companion_lineage_key != lineage_key:
    raise ExecutionGraphError(
      "History companion lineage identity does not match its base dataset: "
      f"{_dataset_label(td)} -> {_dataset_label(hist_td)}."
    )

  return hist_td


def resolve_hist_base_for_companion(td: TargetDataset) -> TargetDataset | None:
  """Resolve the historized rawcore base dataset required by a history dataset."""
  if not bool(getattr(td, "is_hist", False)):
    return None

  schema = td.target_schema
  hist_name = str(getattr(td, "target_dataset_name", "") or "")
  if not hist_name.endswith("_hist"):
    raise ExecutionGraphError(
      "History dataset does not follow the required *_hist naming contract: "
      f"{_dataset_label(td)}."
    )
  expected_base_name = hist_name[:-5]
  lineage_key = str(getattr(td, "lineage_key", "") or "").strip()

  base_td = None
  if lineage_key:
    candidates = list(
      TargetDataset.objects
      .filter(
        target_schema=schema,
        lineage_key=lineage_key,
      )
      .exclude(target_dataset_name__endswith="_hist")
      .exclude(pk=getattr(td, "pk", None))
      .order_by("target_dataset_name", "id")[:2]
    )
    if len(candidates) > 1:
      raise ExecutionGraphError(
        "History dataset has multiple possible base datasets for the same "
        f"lineage identity: {_dataset_label(td)} <- "
        + ", ".join(_dataset_label(item) for item in candidates)
        + "."
      )
    if candidates:
      base_td = candidates[0]

  if base_td is None:
    base_td = (
      TargetDataset.objects
      .filter(
        target_schema=schema,
        target_dataset_name=expected_base_name,
      )
      .exclude(pk=getattr(td, "pk", None))
      .first()
    )

  if base_td is None:
    raise ExecutionGraphError(
      "History dataset requires a corresponding rawcore base dataset, but none "
      f"could be resolved: {_dataset_label(td)}."
    )

  if base_td.target_dataset_name != expected_base_name:
    raise ExecutionGraphError(
      "History dataset name is not synchronized with its base dataset: "
      f"{_dataset_label(td)} <- {_dataset_label(base_td)}; "
      f"expected {schema.short_name}.{expected_base_name}. Synchronize target metadata before execution."
    )

  if not _is_rawcore_base_dataset(base_td) or not _effective_historize(base_td):
    raise ExecutionGraphError(
      "History dataset resolved to a base dataset without an active historization "
      f"contract: {_dataset_label(td)} <- {_dataset_label(base_td)}."
    )

  if not bool(getattr(base_td, "active", True)):
    raise ExecutionGraphError(
      "History dataset requires an active rawcore base dataset: "
      f"{_dataset_label(td)} <- {_dataset_label(base_td)}."
    )

  base_lineage_key = str(getattr(base_td, "lineage_key", "") or "").strip()
  if lineage_key and base_lineage_key and base_lineage_key != lineage_key:
    raise ExecutionGraphError(
      "History dataset lineage identity does not match its base dataset: "
      f"{_dataset_label(td)} <- {_dataset_label(base_td)}."
    )

  return base_td

 
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
    except ExecutionGraphError:
      # Mandatory execution-contract violations must fail closed.
      raise
    except Exception:
      # Preserve legacy best-effort handling for unrelated graph-resolution
      # failures. Mandatory companion errors are re-raised above.
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

  Execution dependencies drive scheduling. They are derived from modeled
  upstream inputs, optional SourceDataset -> raw TargetDataset readiness, and
  controlled reference-member parent readiness where enabled.
  """
  # Dummy/test datasets may not have ORM managers; treat them as leaf nodes.
  has_input_links = hasattr(td, "input_links")
  has_outgoing_references = hasattr(td, "outgoing_references")
  is_hist = bool(getattr(td, "is_hist", False))
  if not has_input_links and not has_outgoing_references and not is_hist:
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

  if has_input_links:
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

  if is_hist:
    add_dependency(
      resolve_hist_base_for_companion(td),
      reason=EXECUTION_DEPENDENCY_HIST_BASE_READY,
    )

  if has_outgoing_references:
    refs_obj = getattr(td, "outgoing_references", None)
    try:
      refs = (
        refs_obj
        .select_related("referenced_dataset", "referenced_dataset__target_schema")
        .all()
      )
    except Exception:
      try:
        refs = list(refs_obj)
      except Exception:
        refs = []

    for ref in refs:
      if not (
        bool(getattr(ref, "inferred_members_enabled", False))
        or bool(getattr(ref, "default_member_fallback_enabled", False))
      ):
        continue

      add_dependency(
        getattr(ref, "referenced_dataset", None),
        reason=EXECUTION_DEPENDENCY_REFERENCE_PARENT_READY,
        reference_id=getattr(ref, "id", None),
      )

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

    # Historization is part of the base dataset execution contract. A history
    # companion is a required scope member even though it is downstream of the
    # base and therefore cannot be discovered through normal upstream traversal.
    hist_companion = resolve_hist_companion_for_base(td)
    if hist_companion is not None and hist_companion not in graph:
      stack.append(hist_companion)

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
