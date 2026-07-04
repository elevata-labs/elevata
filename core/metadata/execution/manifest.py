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
from datetime import datetime, timezone
from typing import Dict, List, Set

from django.apps import apps

from metadata.execution.load_graph import (
  EXECUTION_DEPENDENCY_LINEAGE_INPUT,
  EXECUTION_DEPENDENCY_SOURCE_RAW_READY,
  resolve_execution_dependencies,
)

MANIFEST_VERSION = 2
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


@dataclass(frozen=True)
class Manifest:
  generated_at: str
  profile: str
  target_system: str
  nodes: List[ManifestNode]
  levels: List[List[str]]  # parallelizable execution waves by node id


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
  - SourceDataset nodes are read-only manifest nodes and do not create load tasks.
  """
  TargetDataset = apps.get_model("metadata", "TargetDataset")

  qs = TargetDataset.objects.select_related("target_schema")

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

  # --- 3) Finalize nodes with dependency payloads
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
      }
      for n in m.nodes
    ],
    "levels": m.levels,
  }
