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

from collections import deque
from typing import Any, Dict, Optional, Set

from metadata.generation.query_contract import infer_query_node_contract
from metadata.generation.window_fn_registry import get_window_fn_spec


def analyze_query_governance(root) -> Dict[str, Any]:
  """
  Analyze a query tree for governance-oriented badges:
    - operator usage (aggregate/union/window/select)
    - determinism heuristics (STRING_AGG ordering, window ordering)
    - issue counts from contract inference
  """
  if not root:
    return {
      "has_query": False,
      "uses_select": False,
      "uses_aggregate": False,
      "uses_union": False,
      "uses_window": False,
      "deterministic": True,
      "non_deterministic_reasons": [],
      "issue_counts": {"errors": 0, "warnings": 0},
    }

  cr = infer_query_node_contract(root)
  err_cnt = sum(1 for m in cr.issues if (m or "").startswith("ERROR:"))
  warn_cnt = sum(1 for m in cr.issues if (m or "").startswith("WARN:"))

  uses_select = False
  uses_aggregate = False
  uses_union = False
  uses_window = False

  nondet_reasons = []
  explanations = []

  q = deque([root])
  seen: Set[int] = set()

  while q:
    n = q.popleft()
    nid = int(getattr(n, "id", 0) or 0)
    if not nid or nid in seen:
      continue
    seen.add(nid)

    ntype = (getattr(n, "node_type", "") or "").strip().lower()
    if ntype == "select":
      uses_select = True

    elif ntype == "aggregate":
      uses_aggregate = True
      agg = getattr(n, "aggregate", None)
      if agg:
        # determinism: STRING_AGG should have ORDER BY
        for m in agg.measures.all():
          fn = (getattr(m, "function", "") or "").strip().upper()
          if fn == "STRING_AGG" and not getattr(m, "order_by_id", None):
            out_name = (getattr(m, "output_name", "") or "").strip() or "STRING_AGG"
            nondet_reasons.append(f"STRING_AGG '{out_name}' without ORDER BY")
            explanations.append({
              "severity": "warning",
              "category": "determinism",
              "message": f"STRING_AGG measure '{out_name}' has no ORDER BY (non-deterministic).",
              "model": "QueryAggregateMeasure",
              "id": getattr(m, "id", None),
            })

        if getattr(agg, "input_node", None):
          q.append(agg.input_node)

    elif ntype == "union":
      uses_union = True
      un = getattr(n, "union", None)
      if un:
        for b in un.branches.all():
          if getattr(b, "input_node", None):
            q.append(b.input_node)

    elif ntype == "window":
      uses_window = True
      w = getattr(n, "window", None)
      if w:
        for c in w.columns.all():
          fn = (getattr(c, "function", "") or "").strip().upper()
          spec = get_window_fn_spec(fn)
          if spec and spec.requires_order_by and not getattr(c, "order_by_id", None):
            out_name = (getattr(c, "output_name", "") or "").strip() or fn
            nondet_reasons.append(f"{fn} '{out_name}' without ORDER BY")
            explanations.append({
              "severity": "warning",
              "category": "determinism",
              "message": f"Window column '{out_name}' uses {fn} without ORDER BY (non-deterministic).",
              "model": "QueryWindowColumn",
              "id": getattr(c, "id", None),
            })

        if getattr(w, "input_node", None):
          q.append(w.input_node)

  deterministic = (len(nondet_reasons) == 0)

  return {
    "has_query": True,
    "uses_select": uses_select,
    "uses_aggregate": uses_aggregate,
    "uses_union": uses_union,
    "uses_window": uses_window,
    "deterministic": deterministic,
    "non_deterministic_reasons": nondet_reasons[:8],  # keep UI short
    "issue_counts": {"errors": err_cnt, "warnings": warn_cnt},
    "explanations": explanations[:20],
  }
