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


@dataclass(frozen=True)
class ManifestNode:
  id: str
  type: str  # "target" | "source"
  schema: str | None
  dataset: str
  mode: str | None
  materialization: str | None
  deps: List[str]


@dataclass(frozen=True)
class Manifest:
  generated_at: str
  profile: str
  target_system: str
  nodes: List[ManifestNode]
  levels: List[List[str]]  # parallelizable waves by node id


def _now_iso() -> str:
  return datetime.now(timezone.utc).isoformat()


def _target_id(schema_short: str, dataset_name: str) -> str:
  return f"{schema_short}.{dataset_name}"


def _source_id(source_system_short: str, schema_name: str | None, source_dataset_name: str) -> str:
  # Deterministic, URL/JSON-friendly ID
  schema_part = schema_name if schema_name else "default"
  return f"source.{source_system_short}.{schema_part}.{source_dataset_name}"


def _toposort_levels(node_ids: Set[str], deps_map: Dict[str, Set[str]]) -> List[List[str]]:
  """
  Kahn-level topological sort.
  deps_map[node] contains upstream dependencies (node depends on deps).
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

    # temporary DEBUG
    sample = remaining[0] if remaining else None
    deps = sorted(deps_map.get(sample, set())) if sample else []
    raise ValueError(f"Cycle detected. Remaining count={len(remaining)} sample={sample} deps={deps}")
    #-------------------------------------

    # raise ValueError(f"Cycle detected in manifest graph: {remaining}")

  return levels


def build_manifest(
  profile_name: str,
  target_system_short: str,
  include_system_managed: bool = True,
  include_sources: bool = True,
) -> Manifest:
  """
  Build a full execution manifest for all TargetDatasets.

  - TargetDataset dependencies come from load_graph.resolve_upstream_datasets().
  - SourceDataset nodes are read-only and only connected as deps of RAW (stage) targets.
  - RAW layer is identified via target_schema.short_name == "raw".
  """
  TargetDataset = apps.get_model("metadata", "TargetDataset")

  qs = TargetDataset.objects.select_related("target_schema")

  if not include_system_managed:
    qs = qs.filter(is_system_managed=False)

  # Deterministic iteration
  tds = list(qs.order_by("target_schema__short_name", "target_dataset_name"))

  nodes: Dict[str, ManifestNode] = {}
  deps_map: Dict[str, Set[str]] = {}

  # --- 1) Add all target nodes + target->target deps
  for td in tds:
    schema_short = td.target_schema.short_name
    ds_name = td.target_dataset_name
    tid = _target_id(schema_short, ds_name)

    deps_map.setdefault(tid, set())

    # Dependencies are driven by TargetDatasetInput rows:
    # - upstream_target_dataset -> target dependency
    # - source_dataset is modeled as read-only SourceNode (handled in step 2)
    if hasattr(td, "input_links"):
      for link in td.input_links.select_related("upstream_target_dataset__target_schema").filter(active=True):
        up = getattr(link, "upstream_target_dataset", None)
        if not up:
          continue

        up_id = _target_id(up.target_schema.short_name, up.target_dataset_name)
        if up_id == tid:
          # Defensive: never allow self-dependencies in the manifest.
          continue

        # Ensure upstream nodes exist in the manifest even if they were not part of the initial queryset.
        if up_id not in nodes:
          deps_map.setdefault(up_id, set())
          nodes[up_id] = ManifestNode(
            id=up_id,
            type="target",
            schema=up.target_schema.short_name,
            dataset=up.target_dataset_name,
            mode=str(getattr(up, "incremental_strategy", "full") or "full"),
            materialization=str(getattr(up, "materialization_type", None)),
            deps=[],
          )

        deps_map[tid].add(up_id)

    nodes[tid] = ManifestNode(
      id=tid,
      type="target",
      schema=schema_short,
      dataset=ds_name,
      mode=str(getattr(td, "incremental_strategy", "full") or "full"),
      materialization=str(getattr(td, "materialization_type", None)),
      deps=sorted(deps_map[tid]),
    )

  # --- 2) Add source nodes + source->target deps (read-only nodes)
  if include_sources:
    for td in tds:
      tid = _target_id(td.target_schema.short_name, td.target_dataset_name)

      deps_map.setdefault(tid, set())

      # TargetDatasetInput is the through model for source_datasets
      # Only active source mappings should contribute to lineage
      if not hasattr(td, "input_links"):
        continue

      for link in td.input_links.select_related("source_dataset__source_system").filter(active=True):
        src = getattr(link, "source_dataset", None)
        if not src:
          continue

        sys_short = src.source_system.short_name
        sid = _source_id(sys_short, src.schema_name, src.source_dataset_name)

        # Create source node if missing
        if sid not in nodes:
          nodes[sid] = ManifestNode(
            id=sid,
            type="source",
            schema=src.schema_name,
            dataset=src.source_dataset_name,
            mode=None,
            materialization=None,
            deps=[],
          )

        deps_map[tid].add(sid)

      # Refresh deps list on target node
      if tid in nodes:
        nodes[tid] = ManifestNode(
          id=nodes[tid].id,
          type=nodes[tid].type,
          schema=nodes[tid].schema,
          dataset=nodes[tid].dataset,
          mode=nodes[tid].mode,
          materialization=nodes[tid].materialization,
          deps=sorted(deps_map[tid]),
        )

  # --- 3) Finalize: topo levels (includes sources)
  all_node_ids = set(nodes.keys())
  levels = _toposort_levels(all_node_ids, deps_map)

  # Deterministic nodes list
  ordered_nodes = [nodes[k] for k in sorted(nodes.keys())]

  return Manifest(
    generated_at=_now_iso(),
    profile=profile_name,
    target_system=target_system_short,
    nodes=ordered_nodes,
    levels=levels,
  )


def manifest_to_dict(m: Manifest) -> Dict:
  return {
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
      }
      for n in m.nodes
    ],
    "levels": m.levels,
  }
