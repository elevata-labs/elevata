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
    - available_input_columns: columns selectable by downstream operators
  """
  columns: List[str]
  issues: List[str]
  available_input_columns: List[str] | None = None


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


def _select_upstream_column_names(td) -> List[str]:
  """
  Return SELECT-upstream column names in deterministic authoring order.

  Without an explicit join, inputs are emitted in stable input order and each
  dataset keeps its physical column order. For an explicit join, the authoring
  list follows the visible join direction: base/left-side columns first, then
  right-side columns in join order. Duplicate unqualified names resolve to the
  base/left input, so a name shown in the list has the same meaning the SQL
  builder will use.
  """
  try:
    inputs = getattr(td, "input_links", None)
    if inputs is None:
      return []

    items = inputs.all() if hasattr(inputs, "all") else list(inputs)
    items = sorted(
      list(items),
      key=lambda item: getattr(item, "id", 0) or 0,
    )

    def _column_names_for_input(inp) -> List[str]:
      utd = getattr(inp, "upstream_target_dataset", None)
      if utd is not None:
        tcols = getattr(utd, "target_columns", None)
        if tcols is None:
          return []
        tc_items = tcols.filter(active=True) if hasattr(tcols, "filter") else list(tcols)
        if hasattr(tc_items, "order_by"):
          tc_items = tc_items.order_by("ordinal_position", "id")
        else:
          tc_items = sorted(
            list(tc_items),
            key=lambda col: (
              getattr(col, "ordinal_position", 0) or 0,
              getattr(col, "id", 0) or 0,
            ),
          )
        return [
          (getattr(col, "target_column_name", "") or "").strip()
          for col in tc_items
          if (getattr(col, "target_column_name", "") or "").strip()
        ]

      sd = getattr(inp, "source_dataset", None)
      if sd is not None:
        scols = getattr(sd, "source_columns", None)
        if scols is None:
          return []
        sc_items = scols.all() if hasattr(scols, "all") else list(scols)
        if hasattr(sc_items, "order_by"):
          sc_items = sc_items.order_by("ordinal_position", "id")
        else:
          sc_items = sorted(
            list(sc_items),
            key=lambda col: (
              getattr(col, "ordinal_position", 0) or 0,
              getattr(col, "id", 0) or 0,
            ),
          )
        return [
          (getattr(col, "source_column_name", "") or "").strip()
          for col in sc_items
          if (getattr(col, "source_column_name", "") or "").strip()
        ]

      return []

    columns_by_id = {
      getattr(item, "id", None): _column_names_for_input(item)
      for item in items
    }

    joins = getattr(td, "joins", None)
    if joins is not None:
      try:
        join_items = joins.all() if hasattr(joins, "all") else list(joins)
        if hasattr(join_items, "order_by"):
          join_items = list(join_items.order_by("join_order", "id"))
        else:
          join_items = sorted(
            list(join_items),
            key=lambda join: (
              getattr(join, "join_order", 0) or 0,
              getattr(join, "id", 0) or 0,
            ),
          )

        if join_items:
          base_left_id = getattr(join_items[0], "left_input_id", None)
          if base_left_id is None:
            base_left_id = getattr(getattr(join_items[0], "left_input", None), "id", None)

          right_ids: List[int] = []
          for join in join_items:
            right_id = getattr(join, "right_input_id", None)
            if right_id is None:
              right_id = getattr(getattr(join, "right_input", None), "id", None)
            if right_id is not None and right_id not in right_ids:
              right_ids.append(right_id)

          # Namespace resolution is independent from display order: the base/left
          # input wins duplicate unqualified names. Remaining right sides then win
          # in join order.
          winner_by_name: Dict[str, int] = {}
          priority_ids = []
          if base_left_id is not None:
            priority_ids.append(base_left_id)
          priority_ids.extend(input_id for input_id in right_ids if input_id not in priority_ids)
          priority_ids.extend(
            getattr(item, "id", None)
            for item in items
            if getattr(item, "id", None) not in priority_ids
          )
          for input_id in priority_ids:
            for name in columns_by_id.get(input_id, []):
              winner_by_name.setdefault(name.lower(), input_id)

          # UI order follows the join direction: base/left dataset first, then
          # joined/right datasets. This mirrors how users read the join and also
          # makes duplicate-name precedence immediately visible.
          display_ids: List[int] = []
          if base_left_id is not None:
            display_ids.append(base_left_id)
          display_ids.extend(input_id for input_id in right_ids if input_id not in display_ids)
          display_ids.extend(
            getattr(item, "id", None)
            for item in items
            if getattr(item, "id", None) not in display_ids
          )

          result: List[str] = []
          emitted: Set[str] = set()
          for input_id in display_ids:
            for name in columns_by_id.get(input_id, []):
              key = name.lower()
              if key in emitted or winner_by_name.get(key) != input_id:
                continue
              emitted.add(key)
              result.append(name)
          return result
      except Exception:
        pass

    result: List[str] = []
    seen: Set[str] = set()
    for item in items:
      for name in columns_by_id.get(getattr(item, "id", None), []):
        key = name.lower()
        if key in seen:
          continue
        seen.add(key)
        result.append(name)
    return result
  except Exception:
    return []


def _select_authored_column_names(td) -> List[str]:
  """
  Return authored SELECT-level column names, including definitions that are
  inactive only because a later query operator replaced the final output schema.

  Query-derived columns are excluded because their semantics live in the query
  tree itself rather than in the base SELECT definition.
  """
  try:
    cols = getattr(td, "target_columns", None)
    if cols is None:
      return []
    if hasattr(cols, "all"):
      items = cols.all()
    elif hasattr(cols, "order_by"):
      items = cols.order_by("ordinal_position", "id")
    else:
      items = list(cols)
    if hasattr(items, "order_by"):
      items = items.order_by("ordinal_position", "id")

    out: List[str] = []
    seen: Set[str] = set()
    for c in items:
      if (getattr(c, "lineage_origin", "") or "") == "query_derived":
        continue
      name = (getattr(c, "target_column_name", "") or "").strip()
      if not name:
        continue
      key = name.lower()
      if key in seen:
        continue
      seen.add(key)
      out.append(name)
    return out
  except Exception:
    return []

def _infer_node(node, cache: Dict[int, ContractResult], visiting: Set[int]) -> ContractResult:
  if node.id in cache:
    return cache[node.id]
  if node.id in visiting:
    # Cycle in query graph -> contract cannot be inferred safely.
    res = ContractResult(
      columns=[],
      issues=[f"ERROR: Query graph cycle detected at node {node.id}."],
      available_input_columns=[],
    )

    cache[node.id] = res
    return res

  visiting.add(node.id)
  issues: List[str] = []
  cols: List[str] = []
  inp: Optional[ContractResult] = None

  ntype = (node.node_type or "").strip().lower()

  if ntype == "select":
    # Minimal/select MVP: select node uses owning TargetDataset definition.
    td = node.target_dataset
    # SELECT owns the authored non-query-derived projection independently from
    # the final query head. A later aggregate may temporarily deactivate those
    # TargetColumns in the physical output, but FILTER/WINDOW/AGGREGATE nodes
    # still need the SELECT's logical output contract.
    cols = _select_authored_column_names(td)

  elif ntype == "aggregate":
    agg = getattr(node, "aggregate", None)
    if not agg:
      issues.append(f"ERROR: Aggregate node {node.id} has no aggregate details.")
      cols = []
    else:
      inp = _infer_node(agg.input_node, cache, visiting)
      issues.extend(inp.issues)
      selectable_inputs = list(getattr(inp, "available_input_columns", None) or inp.columns or [])
      inp_set = {c.lower() for c in selectable_inputs}

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
        in_name = (m.input_column_name or "").strip()
        if in_name and inp_set and in_name.lower() not in inp_set:
          issues.append(f"ERROR: Aggregate measure references missing input column '{in_name}'.")
        cols.append(out_name)

  elif ntype == "union":
    un = getattr(node, "union", None)
    if not un:
      issues.append(f"ERROR: Union node {node.id} has no union details.")
      cols = []
    else:
      out_cols = list(un.output_columns.all().order_by("ordinal_position", "id"))
      cols = [(c.output_name or "").strip() for c in out_cols if (c.output_name or "").strip()]
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
              f"ERROR: Union branch references missing input column '{in_name}' for output '{mp.output_column.output_name}'."
            )

        missing = expected - seen_out
        extra = seen_out - expected
        if missing:
          miss_names = [c.output_name for c in out_cols if c.id in missing]
          issues.append(f"ERROR: Union branch {b.id} missing mappings for: {', '.join(miss_names)}.")
        if extra:
          issues.append(f"ERROR: Union branch {b.id} has mappings to unknown output columns.")

  elif ntype == "filter":
    flt = getattr(node, "filter", None)
    if not flt:
      issues.append(f"ERROR: Filter node {node.id} has no filter details.")
      cols = []
    else:
      inp = _infer_node(flt.input_node, cache, visiting)
      issues.extend(inp.issues)
      cols = list(inp.columns)

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
      for wc in w.columns.filter(active=True).order_by("ordinal_position", "id"):
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

  # ------------------------------------------------------------
  # Determine available input columns for downstream operators
  # ------------------------------------------------------------
  available_inputs: List[str] = []

  if ntype == "aggregate":
    # AGGREGATE is a projection boundary. Downstream operators may only reference
    # the aggregate output, never hidden columns from before the aggregation.
    available_inputs = list(deduped)
  elif ntype == "filter":
    # FILTER does not change row shape. Keep the visible output contract, but also
    # preserve selectable upstream columns so downstream operators can request
    # non-projected inputs through required-input pushdown.
    upstream_available = list(getattr(inp, "available_input_columns", None) or []) if inp is not None else []
    available_inputs = list(dict.fromkeys(list(deduped) + upstream_available))
  elif ntype == "window":
    # WINDOW adds columns without forming a projection boundary. Its own outputs and
    # selectable upstream columns remain available to downstream operators.
    upstream_available = list(getattr(inp, "available_input_columns", None) or []) if inp is not None else []
    available_inputs = list(dict.fromkeys(list(deduped) + upstream_available))
  elif ntype == "union":
    # Union downstream sees only union output schema.
    available_inputs = list(deduped)
  elif ntype == "select":
    # SELECT exposes its projected outputs plus upstream source columns that can be
    # requested through required-input pushdown. This keeps calculated/projected
    # dataset columns available while allowing downstream operators to reference
    # source columns that are not part of the final SELECT projection.
    td = getattr(node, "target_dataset", None)
    authored_cols = _select_authored_column_names(td) if td is not None else []
    upstream_cols = _select_upstream_column_names(td) if td is not None else []

    # Query/SELECT outputs shadow same-named authored/upstream inputs. Keep one
    # deterministic visible name, preferring the closest query definition.
    merged: List[str] = []
    seen: Set[str] = set()
    for name in list(deduped) + authored_cols + upstream_cols:
      key = (name or "").strip().lower()
      if not key or key in seen:
        continue
      seen.add(key)
      merged.append(name)
    available_inputs = merged
  else:
    available_inputs = list(deduped)

  res = ContractResult(
    columns=deduped,
    issues=issues,
    available_input_columns=available_inputs,
  )

  cache[node.id] = res
  visiting.remove(node.id)
  return res
