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
from datetime import datetime, timezone
from typing import Dict, List, Set

from django.apps import apps

from metadata.execution.load_graph import (
  EXECUTION_DEPENDENCY_LINEAGE_INPUT,
  EXECUTION_DEPENDENCY_SOURCE_RAW_READY,
  resolve_execution_dependencies,
)
from metadata.execution.load_scope import (
  FULL_LOAD_SCOPE_NAME,
  LOAD_SCOPE_MODE_PARTIAL_LOAD,
  resolve_partial_load_scope,
)

MANIFEST_VERSION = 3
EXECUTION_DEPENDENCY_SOURCE_INPUT = "source_input"


@dataclass(frozen=True)
class ManifestExecutionDependency:
  """
  Structured execution dependency emitted for orchestration transparency.
  """
  id: str
  reason: str
  reference_id: int | None = None

 
@dataclass(frozen=True)
class ManifestNode:
  id: str
  type: str  # "target" | "source"
  schema: str | None
  dataset: str
  mode: str | None
  materialization: str | None
  deps: List[str]
  lineage_deps: List[str]
  execution_deps: List[ManifestExecutionDependency]
  load_scopes: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class ManifestLoadScope:
  """Resolved named execution scope emitted for scheduler discovery."""
  name: str
  scope_mode: str  # "all" | "partial_load"
  root_dataset_ids: List[str]
  dataset_ids: List[str]


@dataclass(frozen=True)
class Manifest:
  generated_at: str
  profile: str
  target_system: str
  nodes: List[ManifestNode]
  levels: List[List[str]]  # parallelizable execution waves by node id
  load_scopes: List[ManifestLoadScope] = field(default_factory=list)


def _now_iso() -> str:
  return datetime.now(timezone.utc).isoformat()


def _target_id(schema_short: str, dataset_name: str) -> str:
  return f"{schema_short}.{dataset_name}"


def _source_id(source_system_short: str, schema_name: str | None, source_dataset_name: str) -> str:
  # Deterministic, URL/JSON-friendly ID
  schema_part = schema_name if schema_name else "default"
  return f"source.{source_system_short}.{schema_part}.{source_dataset_name}"


def _effective_materialization(td) -> str | None:
  """
  Return the effective materialization type for a TargetDataset node.

  The manifest is consumed by execution and orchestration tooling, so it must
  expose the resolved materialization contract rather than the nullable dataset
  override field. When a dataset does not override materialization_type, the
  target schema default is the effective value.
  """
  effective = getattr(td, "effective_materialization_type", None)
  if callable(effective):
    value = effective()
  elif effective:
    value = effective
  else:
    schema = getattr(td, "target_schema", None)
    value = (
      getattr(td, "materialization_type", None)
      or getattr(schema, "default_materialization_type", None)
    )

  if value is None:
    return None

  text = str(value).strip()
  return text or None


def _empty_node(
  *,
  node_id: str,
  node_type: str,
  schema: str | None,
  dataset: str,
  mode: str | None = None,
  materialization: str | None = None,
) -> ManifestNode:
  """
  Build a ManifestNode without dependency fields populated yet.
  """
  return ManifestNode(
    id=node_id,
    type=node_type,
    schema=schema,
    dataset=dataset,
    mode=mode,
    materialization=materialization,
    deps=[],
    lineage_deps=[],
    execution_deps=[],
    load_scopes=[],
  )


def _toposort_levels(node_ids: Set[str], deps_map: Dict[str, Set[str]]) -> List[List[str]]:
  """
  Kahn-level topological sort.
  deps_map[node] contains upstream execution dependencies (node depends on deps).
  """
  deps_left: Dict[str, Set[str]] = {n: set(deps_map.get(n, set())) for n in node_ids}
  reverse: Dict[str, Set[str]] = {n: set() for n in node_ids}

  for n, deps in deps_left.items():
    for d in deps:
      reverse.setdefault(d, set()).add(n)

  levels: List[List[str]] = []
  visited: Set[str] = set()

  ready = sorted([n for n in node_ids if not deps_left.get(n)])

  while ready:
    level = list(ready)
    levels.append(level)

    next_ready: Set[str] = set()

    for n in level:
      visited.add(n)
      for child in reverse.get(n, set()):
        if child in visited:
          continue
        deps_left[child].discard(n)
        if not deps_left[child]:
          next_ready.add(child)

    ready = sorted(next_ready)

  if len(visited) != len(node_ids):
    remaining = sorted(node_ids - visited)

    raise ValueError(f"Cycle detected in manifest execution graph: {remaining}")

  return levels


def _build_load_scopes(
  *,
  target_node_ids: Set[str],
) -> List[ManifestLoadScope]:
  """
  Resolve all scheduler-visible load scopes against the canonical runtime graph.

  The implicit Full Load is always present. Named Partial Loads are resolved by
  metadata.execution.load_scope so manifest generation never duplicates scope
  semantics. A filtered manifest must still be able to represent every resolved
  Partial Load exactly; otherwise generation fails closed instead of silently
  weakening the load contract.
  """
  PartialLoad = apps.get_model("metadata", "PartialLoad")

  scopes: List[ManifestLoadScope] = [
    ManifestLoadScope(
      name=FULL_LOAD_SCOPE_NAME,
      scope_mode="all",
      root_dataset_ids=[],
      dataset_ids=sorted(target_node_ids),
    )
  ]

  partial_loads = sorted(
    list(PartialLoad.objects.all()),
    key=lambda partial_load: (
      str(partial_load.name).casefold(),
      str(partial_load.name),
      int(partial_load.pk or 0),
    ),
  )

  for partial_load in partial_loads:
    resolved = resolve_partial_load_scope(partial_load)
    root_dataset_ids = list(resolved.root_dataset_keys)
    dataset_ids = list(resolved.execution_dataset_keys)

    missing_dataset_ids = [
      dataset_id
      for dataset_id in dataset_ids
      if dataset_id not in target_node_ids
    ]
    if missing_dataset_ids:
      raise ValueError(
        "Execution manifest cannot represent Partial Load "
        f"'{partial_load.name}' because its resolved scope contains "
        "TargetDataset node(s) excluded from this manifest: "
        + ", ".join(sorted(missing_dataset_ids))
        + ". Generate the complete execution manifest or change the "
        "manifest filters."
      )

    scopes.append(ManifestLoadScope(
      name=str(partial_load.name),
      scope_mode=LOAD_SCOPE_MODE_PARTIAL_LOAD,
      root_dataset_ids=root_dataset_ids,
      dataset_ids=dataset_ids,
    ))

  return scopes


def _load_scope_memberships(
  load_scopes: List[ManifestLoadScope],
) -> Dict[str, List[str]]:
  """Return resolved load-scope membership for each TargetDataset node id."""
  memberships: Dict[str, List[str]] = {}

  for load_scope in load_scopes:
    for dataset_id in load_scope.dataset_ids:
      memberships.setdefault(dataset_id, []).append(load_scope.name)

  return memberships


def build_manifest(
  profile_name: str,
  target_system_short: str,
  include_system_managed: bool = True,
  include_sources: bool = True,
) -> Manifest:
  """
  Build a full execution manifest for all TargetDatasets.

  - deps contains execution dependency ids for schedulers and orchestrators.
  - lineage_deps contains semantic lineage dependency ids for explanation.
  - execution_deps contains structured dependency reasons for transparency.
  - load_scopes contains the implicit Full Load plus all resolved Partial Loads.
  - TargetDataset nodes expose their resolved load-scope memberships.
  - SourceDataset nodes are read-only manifest nodes and do not create load tasks.
  """
  TargetDataset = apps.get_model("metadata", "TargetDataset")

  qs = (
    TargetDataset.objects
    .select_related("target_schema")
    .filter(active=True)
  )

  if not include_system_managed:
    qs = qs.filter(is_system_managed=False)

  # Deterministic iteration
  tds = list(qs.order_by("target_schema__short_name", "target_dataset_name"))

  nodes: Dict[str, ManifestNode] = {}
  deps_map: Dict[str, Set[str]] = {}
  lineage_deps_map: Dict[str, Set[str]] = {}
  execution_deps_map: Dict[str, dict[tuple[str, str, int | None], ManifestExecutionDependency]] = {}

  def ensure_maps(node_id: str) -> None:
    """
    Ensure dependency maps exist for one node id.
    """
    deps_map.setdefault(node_id, set())
    lineage_deps_map.setdefault(node_id, set())
    execution_deps_map.setdefault(node_id, {})

  def ensure_target_node(td) -> str:
    """
    Ensure a TargetDataset manifest node exists and return its node id.
    """
    node_id = _target_id(td.target_schema.short_name, td.target_dataset_name)
    ensure_maps(node_id)
    if node_id not in nodes:
      nodes[node_id] = _empty_node(
        node_id=node_id,
        node_type="target",
        schema=td.target_schema.short_name,
        dataset=td.target_dataset_name,
        mode=str(getattr(td, "incremental_strategy", "full") or "full"),
        materialization=_effective_materialization(td),
      )
    return node_id

  def ensure_source_node(src) -> str:
    """
    Ensure a SourceDataset manifest node exists and return its node id.
    """
    sys_short = src.source_system.short_name
    node_id = _source_id(sys_short, src.schema_name, src.source_dataset_name)
    ensure_maps(node_id)
    if node_id not in nodes:
      nodes[node_id] = _empty_node(
        node_id=node_id,
        node_type="source",
        schema=src.schema_name,
        dataset=src.source_dataset_name,
      )
    return node_id

  def add_dependency(
    *,
    node_id: str,
    upstream_id: str,
    reason: str,
    reference_id: int | None = None,
    lineage: bool = False,
  ) -> None:
    """
    Add one dependency to the manifest dependency maps.
    """
    if upstream_id == node_id:
      return

    ensure_maps(node_id)
    deps_map[node_id].add(upstream_id)
    if lineage:
      lineage_deps_map[node_id].add(upstream_id)

    dep = ManifestExecutionDependency(
      id=upstream_id,
      reason=reason,
      reference_id=reference_id,
    )
    execution_deps_map[node_id][(dep.id, dep.reason, dep.reference_id)] = dep

  # --- 1) Add all target nodes and target execution dependencies
  for td in tds:
    tid = ensure_target_node(td)

    for dep in resolve_execution_dependencies(td):
      up = dep.upstream
      if not bool(getattr(up, "active", True)):
        upstream_id = _target_id(
          up.target_schema.short_name,
          up.target_dataset_name,
        )
        raise ValueError(
          "Active TargetDataset execution dependency references an inactive "
          "upstream TargetDataset: "
          f"{tid} -> {upstream_id}. "
          "Reactivate the upstream dataset or retire the downstream "
          "dependency before generating the execution manifest."
        )

      up_id = ensure_target_node(up)
      add_dependency(
        node_id=tid,
        upstream_id=up_id,
        reason=dep.reason,
        reference_id=dep.reference_id,
        lineage=dep.reason in {
          EXECUTION_DEPENDENCY_LINEAGE_INPUT,
          EXECUTION_DEPENDENCY_SOURCE_RAW_READY,
        },
      )

  # --- 2) Add source nodes + source->target deps (read-only nodes)
  if include_sources:
    for td in tds:
      tid = ensure_target_node(td)

      # TargetDatasetInput is the through model for source_datasets.
      # Only active source mappings should contribute to manifest lineage.
      if not hasattr(td, "input_links"):
        continue

      for link in td.input_links.select_related("source_dataset__source_system").filter(active=True):
        src = getattr(link, "source_dataset", None)
        if not src:
          continue

        sid = ensure_source_node(src)
        add_dependency(
          node_id=tid,
          upstream_id=sid,
          reason=EXECUTION_DEPENDENCY_SOURCE_INPUT,
          lineage=True,
        )

  # --- 3) Resolve named load scopes and TargetDataset memberships
  target_node_ids = {
    node_id
    for node_id, node in nodes.items()
    if node.type == "target"
  }
  load_scopes = _build_load_scopes(
    target_node_ids=target_node_ids,
  )
  load_scope_memberships = _load_scope_memberships(load_scopes)

  # --- 4) Finalize nodes with dependency and scope payloads
  finalized_nodes: Dict[str, ManifestNode] = {}
  for node_id, node in nodes.items():
    ensure_maps(node_id)
    finalized_nodes[node_id] = ManifestNode(
      id=node.id,
      type=node.type,
      schema=node.schema,
      dataset=node.dataset,
      mode=node.mode,
      materialization=node.materialization,
      deps=sorted(deps_map[node_id]),
      lineage_deps=sorted(lineage_deps_map[node_id]),
      execution_deps=sorted(
        execution_deps_map[node_id].values(),
        key=lambda dep: (dep.id, dep.reason, dep.reference_id or 0),
      ),
      load_scopes=list(load_scope_memberships.get(node_id, [])),
    )

  all_node_ids = set(finalized_nodes.keys())
  levels = _toposort_levels(all_node_ids, deps_map)

  # Deterministic nodes list
  ordered_nodes = [finalized_nodes[k] for k in sorted(finalized_nodes.keys())]

  return Manifest(
    generated_at=_now_iso(),
    profile=profile_name,
    target_system=target_system_short,
    nodes=ordered_nodes,
    levels=levels,
    load_scopes=load_scopes,
  )


def manifest_to_dict(m: Manifest) -> Dict:
  return {
    "manifest_version": MANIFEST_VERSION,
    "generated_at": m.generated_at,
    "profile": m.profile,
    "target_system": m.target_system,
    "nodes": [
      {
        "id": n.id,
        "type": n.type,
        "schema": n.schema,
        "dataset": n.dataset,
        "mode": n.mode,
        "materialization": n.materialization,
        "deps": n.deps,
        "lineage_deps": n.lineage_deps,
        "execution_deps": [
          {
            "id": dep.id,
            "reason": dep.reason,
            "reference_id": dep.reference_id,
          }
          for dep in n.execution_deps
        ],
        "load_scopes": n.load_scopes,
      }
      for n in m.nodes
    ],
    "levels": m.levels,
    "load_scopes": [
      {
        "name": load_scope.name,
        "scope_mode": load_scope.scope_mode,
        "root_dataset_ids": load_scope.root_dataset_ids,
        "dataset_ids": load_scope.dataset_ids,
      }
      for load_scope in m.load_scopes
    ],
  }
