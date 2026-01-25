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

from typing import Any, Dict, List, Optional, Tuple

from metadata.generation.query_contract import infer_query_node_contract


def _get_root_input_node(root) -> Optional[object]:
  """
  Return the primary input node for root operators that wrap another node.
  SELECT has no input node (it is the leaf in the MVP).
  UNION has multiple inputs (branches) -> None here.
  """
  if not root:
    return None
  ntype = (getattr(root, "node_type", "") or "").strip().lower()
  if ntype == "aggregate":
    agg = getattr(root, "aggregate", None)
    return getattr(agg, "input_node", None) if agg else None
  if ntype == "window":
    w = getattr(root, "window", None)
    return getattr(w, "input_node", None) if w else None
  return None


def compute_contract_diff(root) -> Dict[str, Any]:
  """
  Compute a high-level input/output contract diff for the query root.
  Useful for governance & explainability, without rendering SQL.
  """
  if not root:
    return {
      "has_query": False,
      "input_columns": [],
      "output_columns": [],
      "added": [],
      "removed": [],
      "unchanged": [],
      "note": "No query root.",
    }

  out_cr = infer_query_node_contract(root)
  out_cols = out_cr.columns

  inp_node = _get_root_input_node(root)
  if not inp_node:
    # SELECT/UNION etc.: input diff is not meaningful at root level
    return {
      "has_query": True,
      "input_columns": [],
      "output_columns": out_cols,
      "added": [],
      "removed": [],
      "unchanged": [],
      "note": "Root has no single input contract (SELECT leaf or UNION).",
    }

  in_cr = infer_query_node_contract(inp_node)
  in_cols = in_cr.columns

  in_map = {c.lower(): c for c in in_cols}
  out_map = {c.lower(): c for c in out_cols}

  added = [out_map[k] for k in out_map.keys() - in_map.keys()]
  removed = [in_map[k] for k in in_map.keys() - out_map.keys()]
  unchanged = [out_map[k] for k in out_map.keys() & in_map.keys()]

  # Stable-ish ordering: keep output order where possible
  def _order_like(ref: List[str], items: List[str]) -> List[str]:
    ref_idx = {c.lower(): i for i, c in enumerate(ref)}
    return sorted(items, key=lambda x: ref_idx.get(x.lower(), 10**9))

  return {
    "has_query": True,
    "input_columns": in_cols,
    "output_columns": out_cols,
    "added": _order_like(out_cols, added),
    "removed": _order_like(in_cols, removed),
    "unchanged": _order_like(out_cols, unchanged),
    "note": "",
  }
