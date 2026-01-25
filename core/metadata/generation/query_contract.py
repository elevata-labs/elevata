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
from typing import Dict, List, Optional, Set

@dataclass(frozen=True)
class ContractResult:
  """
  Output contract for a query node:
    - columns: ordered list of output column aliases
    - issues: validator-style messages ("ERROR:" / "WARN:")
  """
  columns: List[str]
  issues: List[str]


def infer_query_node_contract(
  node,
  cache: Optional[Dict[int, ContractResult]] = None,
  visiting: Optional[Set[int]] = None,
) -> ContractResult:
  """
  Compute the output contract (column names) of a QueryNode.
  This is purely metadata-driven and deterministic.
  """
  cache = cache or {}
  visiting = visiting or set()
  return _infer_node(node, cache, visiting)


def _infer_node(node, cache: Dict[int, ContractResult], visiting: Set[int]) -> ContractResult:
  if node.id in cache:
    return cache[node.id]
  if node.id in visiting:
    # Cycle in query graph -> contract cannot be inferred safely.
    res = ContractResult(columns=[], issues=[f"ERROR: Query graph cycle detected at node {node.id}."])
    cache[node.id] = res
    return res

  visiting.add(node.id)
  issues: List[str] = []
  cols: List[str] = []

  ntype = (node.node_type or "").strip().lower()

  if ntype == "select":
    # Minimal/select MVP: select node uses owning TargetDataset definition.
    td = node.target_dataset
    # Prefer explicit ordering if available.
    td_cols = list(td.target_columns.all().order_by("ordinal_position", "id"))
    cols = [c.target_column_name for c in td_cols]

  elif ntype == "aggregate":
    agg = getattr(node, "aggregate", None)
    if not agg:
      issues.append(f"ERROR: Aggregate node {node.id} has no aggregate details.")
      cols = []
    else:
      inp = _infer_node(agg.input_node, cache, visiting)
      issues.extend(inp.issues)
      inp_set = {c.lower() for c in inp.columns}

      # Output = group keys + measures (ordinal order)
      group_keys = list(agg.group_keys.all().order_by("ordinal_position", "id"))
      measures = list(agg.measures.all().order_by("ordinal_position", "id"))

      for g in group_keys:
        out_name = (g.output_name or g.input_column_name or "").strip()
        if not out_name:
          issues.append("ERROR: Aggregate group key has empty output name.")
          continue
        in_name = (g.input_column_name or "").strip()
        if in_name and inp_set and in_name.lower() not in inp_set:
          issues.append(f"ERROR: Aggregate group key references missing input column '{in_name}'.")
        cols.append(out_name)

      for m in measures:
        out_name = (m.output_name or "").strip()
        if not out_name:
          issues.append("ERROR: Aggregate measure has empty output_name.")
          continue
        cols.append(out_name)

  elif ntype == "union":
    un = getattr(node, "union", None)
    if not un:
      issues.append(f"ERROR: Union node {node.id} has no union details.")
      cols = []
    else:
      out_cols = list(un.output_columns.all().order_by("ordinal_position", "id"))
      cols = [(c.name or "").strip() for c in out_cols if (c.name or "").strip()]
      if len(cols) != len(out_cols):
        issues.append("ERROR: Union output columns contain empty names.")

      # Governance: every branch must map all output columns, and mappings must reference existing input cols.
      expected = {c.id for c in out_cols}
      for b in un.branches.all().order_by("ordinal_position", "id"):
        inp = _infer_node(b.input_node, cache, visiting)
        issues.extend(inp.issues)
        inp_set = {c.lower() for c in inp.columns}

        mappings = list(b.mappings.all().select_related("output_column"))
        seen_out: Set[int] = set()
        for mp in mappings:
          if not mp.output_column_id:
            issues.append("ERROR: Union branch mapping missing output_column.")
            continue
          seen_out.add(mp.output_column_id)
          in_name = (mp.input_column_name or "").strip()
          if in_name and inp_set and in_name.lower() not in inp_set:
            issues.append(
              f"ERROR: Union branch references missing input column '{in_name}' for output '{mp.output_column.name}'."
            )

        missing = expected - seen_out
        extra = seen_out - expected
        if missing:
          miss_names = [c.name for c in out_cols if c.id in missing]
          issues.append(f"ERROR: Union branch {b.id} missing mappings for: {', '.join(miss_names)}.")
        if extra:
          issues.append(f"ERROR: Union branch {b.id} has mappings to unknown output columns.")

  elif ntype == "window":
    w = getattr(node, "window", None)
    if not w:
      issues.append(f"ERROR: Window node {node.id} has no window details.")
      cols = []
    else:
      inp = _infer_node(w.input_node, cache, visiting)
      issues.extend(inp.issues)
      cols = list(inp.columns)  # passthrough

      existing = {c.lower() for c in cols}
      for wc in w.columns.all().order_by("ordinal_position", "id"):
        out_name = (wc.output_name or "").strip()
        if not out_name:
          issues.append("ERROR: Window column has empty output_name.")
          continue
        if out_name.lower() in existing:
          issues.append(f"ERROR: Window output alias '{out_name}' collides with input projection.")
          continue
        existing.add(out_name.lower())
        cols.append(out_name)

  else:
    issues.append(f"ERROR: Unsupported node_type '{node.node_type}' for node {node.id}.")
    cols = []

  # Final: duplicate output aliases check
  lower_seen: Set[str] = set()
  deduped: List[str] = []
  for c in cols:
    key = (c or "").strip()
    if not key:
      continue
    lk = key.lower()
    if lk in lower_seen:
      issues.append(f"ERROR: Duplicate output column '{key}' in node {node.id}.")
      continue
    lower_seen.add(lk)
    deduped.append(key)

  res = ContractResult(columns=deduped, issues=issues)
  cache[node.id] = res
  visiting.remove(node.id)
  return res
