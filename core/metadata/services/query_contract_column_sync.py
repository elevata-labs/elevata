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
from typing import Any, Iterable, Optional
import re

from django.db import transaction
from metadata.constants import DATATYPE_CHOICES

from metadata.models import TargetColumn, TargetColumnInput, TargetDataset


def _norm(name: str) -> str:
  return (name or "").strip().lower().replace(" ", "_")


def _merge_former_names(existing: Optional[Iterable[str]], new_names: Optional[Iterable[str]]) -> list[str]:
  seen = set()
  out: list[str] = []
  for x in list(existing or []) + list(new_names or []):
    s = (x or "").strip()
    if not s:
      continue
    k = s.lower()
    if k in seen:
      continue
    seen.add(k)
    out.append(s)
  return out


@dataclass(frozen=True)
class ContractCol:
  name: str
  canonical_type: str
  nullable: bool
  max_length: Optional[int] = None
  decimal_precision: Optional[int] = None
  decimal_scale: Optional[int] = None


class QueryContractColumnSyncService:
  """
  Sync TargetColumn rows to match Query Output Contract.

  Policy:
  - Only touches columns that are query-derived (lineage_origin='query_derived').
  - Adds missing contract columns.
  - Applies rename if desired name matches a former_names entry.
  - Deletes query-derived columns that are no longer in contract (NOT retire).
  - Does not manage elevata tech columns (they already exist elsewhere).
  """

  LINEAGE_ORIGIN_QUERY = "query_derived"

  def _allowed_datatypes(self) -> set[str]:
    return {str(k) for (k, _label) in (DATATYPE_CHOICES or [])}


  def _has_datatype(self, key: str) -> bool:
    return key in self._allowed_datatypes()  


  def _norm_fn(self, fn: str) -> str:
    """
    Normalize function tokens from DB choices / UI (e.g. 'ROW NUMBER', 'ROW_NUMBER()', 'row-number').
    """
    s = (fn or "").strip().lower()
    # Replace any non-alnum sequences with underscore (space, dash, parentheses, etc.)
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s


  def sync_for_dataset(self, td: TargetDataset) -> None:
    if td is None:
      return

    contract_cols = self._infer_contract_cols(td)
    desired = [c for c in contract_cols if c.name]
    desired_norm = [_norm(c.name) for c in desired]

    inferred_type_by_norm = self._infer_query_output_types(td)

    # Load current columns once.
    current = list(TargetColumn.objects.filter(target_dataset=td))
    current_by_norm = {_norm(c.target_column_name): c for c in current}

    # If query_root exists, dataset schema must match query output contract.
    # That means: manage ALL non-tech target columns (not only query_derived).
    query_root = getattr(td, "query_root", None)
    is_query_tree = query_root is not None

    def _is_tech_col(col: TargetColumn) -> bool:
      # "elevata tech columns" are already handled elsewhere and must not be touched here
      role = (getattr(col, "system_column_role", "") or "").strip()
      return bool(role)

    managed_cols = [c for c in current if not _is_tech_col(c)] if is_query_tree else [
      c for c in current
      if (getattr(c, "lineage_origin", "") or "") == self.LINEAGE_ORIGIN_QUERY and not _is_tech_col(c)
    ]

    with transaction.atomic():
      # 1) Apply renames via former_names
      # If desired name does not exist, but matches former_names of an existing query-derived col -> rename that col.
      for dc in desired:
        dn = _norm(dc.name)
        if not dn or dn in current_by_norm:
          continue

        rename_src = None
        for c in managed_cols:
          fn = list(getattr(c, "former_names", None) or [])
          if any(_norm(x) == dn for x in fn):
            rename_src = c
            break

        if rename_src is not None:
          old_name = rename_src.target_column_name
          rename_src.target_column_name = dc.name
          rename_src.former_names = _merge_former_names(rename_src.former_names, [old_name])
          # Keep it query-derived
          rename_src.lineage_origin = self.LINEAGE_ORIGIN_QUERY

          inferred = inferred_type_by_norm.get(_norm(dc.name))
          if inferred:
            rename_src.datatype = self._coerce_datatype(
              inferred.get("datatype") or self._fallback_type(dc.canonical_type)
            )
            rename_src.max_length = inferred.get("max_length")
            rename_src.decimal_precision = inferred.get("decimal_precision")
            rename_src.decimal_scale = inferred.get("decimal_scale")
          else:
            rename_src.datatype = self._coerce_datatype(self._fallback_type(dc.canonical_type))
            rename_src.max_length = dc.max_length
            rename_src.decimal_precision = dc.decimal_precision
            rename_src.decimal_scale = dc.decimal_scale

          rename_src.nullable = bool(dc.nullable)
          rename_src.active = True
          rename_src.save()
          current_by_norm[_norm(dc.name)] = rename_src

      # 2) Create / update desired columns
      # IMPORTANT:
      # Query-derived columns must NOT renumber into 1..N, otherwise we collide with existing
      # dataset columns (often unique per (target_dataset, ordinal_position)).
      # Policy: append query-derived columns after the current max ordinal of NON query-derived columns.
      ord_base = self._next_query_derived_ordinal_base(td)

      created_or_seen: list[TargetColumn] = []
      for idx, dc in enumerate(desired, start=1):
        dn = _norm(dc.name)
        if not dn:
          continue

        obj = current_by_norm.get(dn)
        if obj is None:
          inferred = inferred_type_by_norm.get(dn)

          if inferred:
            dtype = self._coerce_datatype(inferred.get("datatype") or self._fallback_type(dc.canonical_type))

            max_length = inferred.get("max_length")
            decimal_precision = inferred.get("decimal_precision")
            decimal_scale = inferred.get("decimal_scale")
          else:
            dtype = self._coerce_datatype(self._fallback_type(dc.canonical_type))

            max_length = dc.max_length
            decimal_precision = dc.decimal_precision
            decimal_scale = dc.decimal_scale

          obj = TargetColumn.objects.create(
            target_dataset=td,
            target_column_name=dc.name,
            datatype=dtype,
            nullable=bool(dc.nullable),
            max_length=max_length,
            decimal_precision=decimal_precision,
            decimal_scale=decimal_scale,
            lineage_origin=self.LINEAGE_ORIGIN_QUERY,
            # Query-derived columns are still user-editable in UI (no locking).
            is_system_managed=False,
            active=True,
            ordinal_position=ord_base + idx,
            retired_at=None,
          )
          current_by_norm[dn] = obj
        else:
          # Update only if it is query-derived or if you explicitly allow upgrading an existing column.
          # Conservative rule: only mutate columns that are already query_derived.
          if (getattr(obj, "lineage_origin", "") or "") == self.LINEAGE_ORIGIN_QUERY:
            changed = False
            inferred = inferred_type_by_norm.get(dn)
            if inferred:
              t = self._coerce_datatype(inferred.get("datatype") or self._fallback_type(dc.canonical_type))
              max_length = inferred.get("max_length")
              decimal_precision = inferred.get("decimal_precision")
              decimal_scale = inferred.get("decimal_scale")
            else:
              t = self._coerce_datatype(self._fallback_type(dc.canonical_type))
              max_length = dc.max_length
              decimal_precision = dc.decimal_precision
              decimal_scale = dc.decimal_scale

            if obj.datatype != t:
              obj.datatype = t
              changed = True
            if obj.nullable != bool(dc.nullable):
              obj.nullable = bool(dc.nullable)
              changed = True
            if obj.max_length != max_length:
              obj.max_length = max_length
              changed = True
            if obj.decimal_precision != decimal_precision:
              obj.decimal_precision = decimal_precision
              changed = True
            if obj.decimal_scale != decimal_scale:
              obj.decimal_scale = decimal_scale
              changed = True
            if not obj.active:
              obj.active = True
              changed = True
            if changed:
              obj.save()
        created_or_seen.append(obj)

      # 3) Delete query-derived columns removed from contract
      desired_set = set(desired_norm)
      removed = []
      for c in managed_cols:
        cn = _norm(getattr(c, "target_column_name", "") or "")
        if cn and cn not in desired_set:
          removed.append(c)

      if removed:
        # Remove column inputs first (safety).
        TargetColumnInput.objects.filter(target_column__in=removed).delete()
        # Then delete the columns.
        TargetColumn.objects.filter(pk__in=[c.pk for c in removed]).delete()

      # 4) Normalize ordinals of query-derived columns to follow contract order
      # Keep query-derived ordinals stable *within their own appended segment* (no collisions).
      self._normalize_query_derived_ordinals(td, desired_norm)

  # -----------------------------
  # Contract inference + typing
  # -----------------------------

  def _infer_contract_cols(self, td: TargetDataset) -> list[ContractCol]:
    """
    Uses existing contract inference (preferred).
    Fallback: empty.
    """
    try:
      from metadata.generation.query_contract import infer_query_node_contract
    except Exception:
      return []

    head = getattr(td, "query_head", None) or getattr(td, "query_root", None)
    if not head:
      return []

    try:
      contract = infer_query_node_contract(head)
    except Exception:
      return []

    raw_cols = getattr(contract, "columns", None) or getattr(contract, "output_columns", None) or []

    out: list[ContractCol] = []

    for c in raw_cols:
      # Support plain string columns (common minimal contract representation)      
      # Support either dict-like or attribute-like column objects.
      name = None
      canonical_type = None
      nullable = None
      max_length = None
      decimal_precision = None
      decimal_scale = None

      if isinstance(c, str):
        # Plain string column name (minimal contract representation)
        name = c
        canonical_type = ""
        nullable = True
        max_length = None
        decimal_precision = None
        decimal_scale = None
      elif isinstance(c, dict):
        name = c.get("name")
        canonical_type = c.get("canonical_type") or c.get("datatype") or c.get("type")
        nullable = c.get("nullable")
        max_length = c.get("max_length")
        decimal_precision = c.get("decimal_precision")
        decimal_scale = c.get("decimal_scale")
      else:
        name = getattr(c, "name", None) or getattr(c, "output_name", None)
        canonical_type = getattr(c, "canonical_type", None) or getattr(c, "datatype", None) or getattr(c, "type", None)
        nullable = getattr(c, "nullable", None)
        max_length = getattr(c, "max_length", None)
        decimal_precision = getattr(c, "decimal_precision", None)
        decimal_scale = getattr(c, "decimal_scale", None)

      name_s = (str(name) if name is not None else "").strip()
      if not name_s:
        continue

      out.append(
        ContractCol(
          name=name_s,
          canonical_type=(str(canonical_type) if canonical_type else ""),
          nullable=(bool(nullable) if nullable is not None else True),
          max_length=(int(max_length) if isinstance(max_length, int) else None),
          decimal_precision=(int(decimal_precision) if isinstance(decimal_precision, int) else None),
          decimal_scale=(int(decimal_scale) if isinstance(decimal_scale, int) else None),
        )
      )

    return out

  def _fallback_type(self, canonical_type: str) -> str:
    """
    Minimal viable typing: always return something usable for DDL.
    Adjust mapping to your DATATYPE_CHOICES.
    """
    t = (canonical_type or "").strip().lower()

    if not t:
      return "STRING"

    # canonical -> your internal logical types
    if t in ("int", "integer", "bigint", "smallint"):
      return "INTEGER"
    if t in ("float", "double", "real"):
      return "FLOAT"
    if t in ("bool", "boolean"):
      return "BOOLEAN"    
    if t in ("date",):
      return "DATE"
    if t in ("time",):
      return "TIME"    
    if t in ("timestamp", "datetime"):
      return "TIMESTAMP"
    if t in ("decimal", "numeric"):
      return "DECIMAL"
    return "STRING"  

  def _coerce_datatype(self, candidate: str) -> str:
    """
    Ensure the datatype is a valid key from DATATYPE_CHOICES.
    DATATYPE_CHOICES is a list of tuples: [(key, label), ...]
    """
    allowed = self._allowed_datatypes()
    c = (candidate or "").strip()

    if c in allowed:
      return c
    cu = c.upper()
    if cu in allowed:
      return cu

    cl = c.lower()
    for k in allowed:
      if str(k).lower() == cl:
        return str(k)
    # last resort: pick STRING if available, else first allowed, else keep candidate
    if "STRING" in allowed:
      return "STRING"
    return next(iter(allowed), c or "STRING")
  
  # -----------------------------
  # Query-derived type inference
  # -----------------------------

  def _infer_query_output_types(self, td: TargetDataset) -> dict[str, dict[str, Any]]:
    """
    Infer datatypes for query-derived columns by scanning all query nodes of the dataset.
    This avoids relying on query_head/query_root being perfectly maintained.
    """

    try:
      from metadata.models import QueryNode
    except Exception:
      return {}
   
    # ------------------------------------------------------------
    # Tests / non-ORM callers may pass a fake TD without numeric pk.
    # In that case, fall back to head-based inference (no DB access).
    # ------------------------------------------------------------
    td_id = getattr(td, "pk", None)
    if td_id is None:
      td_id = getattr(td, "id", None)
    if not isinstance(td_id, int):
      head = getattr(td, "query_head", None) or getattr(td, "query_root", None)
      if head is None:
        return {}
      return self._infer_types_from_node(head)

    out: dict[str, dict[str, Any]] = {}
    nodes = list(QueryNode.objects.filter(target_dataset_id=td_id).only("id", "node_type"))

    for n in nodes:
      nt = (getattr(n, "node_type", "") or "").strip().lower()

      if nt == "window":
        win = getattr(n, "window", None)
        if not win:
          continue
        try:
          cols = list(win.columns.all().order_by("ordinal_position", "id"))
        except Exception:
          cols = []
        for c in cols:
          out_name = (getattr(c, "output_name", "") or "").strip()
          if not out_name:
            continue
          fn_raw = (
            getattr(c, "function", None)
            or getattr(c, "window_function", None)
            or getattr(c, "op", None)
            or ""
          )

          fn = self._norm_fn(fn_raw)
          if fn in ("row_number", "rownumber", "rank", "dense_rank", "denserank"):
            out[_norm(out_name)] = {"datatype": self._best_integer_datatype()}

      elif nt == "aggregate":
        agg = getattr(n, "aggregate", None)
        if not agg:
          continue
        inp_node = getattr(agg, "input_node", None)
        input_td = self._resolve_select_input_target_dataset(inp_node) or getattr(inp_node, "target_dataset", None)
        try:
          measures = list(agg.measures.all().order_by("ordinal_position", "id"))
        except Exception:
          measures = []
        for m in measures:
          out_name = (getattr(m, "output_name", "") or "").strip()
          if not out_name:
            continue
          fn = (getattr(m, "function", "") or "").strip().lower()
          arg = (getattr(m, "input_column_name", "") or "").strip()
          if fn == "count":
            out[_norm(out_name)] = {"datatype": self._best_integer_datatype()}
            continue
          if fn in ("sum", "min", "max", "avg"):
            inferred = self._infer_type_from_target_dataset(input_td, arg)
            out[_norm(out_name)] = inferred or {"datatype": "STRING"}
            continue
          out[_norm(out_name)] = {"datatype": "STRING"}

    return out 


  # ------------------------------------------------------------
  # Integer datatype resolver (canonical for window/count)
  # ------------------------------------------------------------
  def _best_integer_datatype(self) -> str:
    """
    Return the preferred integer datatype defined in DATATYPE_CHOICES.
    Guarantees a valid physical datatype.
    """
    allowed = self._allowed_datatypes()

    # preferred order
    if "INTEGER" in allowed:
      return "INTEGER"
    if "BIGINT" in allowed:
      return "BIGINT"

    return "STRING" if "STRING" in allowed else next(iter(allowed), "STRING")


  def _resolve_select_input_target_dataset(self, node) -> Optional[TargetDataset]:
    """
    If the given node is a SELECT node, return its upstream TargetDataset (if present).
    This is the dataset whose columns are valid as "available input columns".
    """
    if not node:
      return None
    try:
      nt = (getattr(node, "node_type", "") or "").strip().lower()
      if nt != "select":
        return None
      td = getattr(node, "target_dataset", None)
      if not td:
        return None
      inputs = getattr(td, "input_links", None)
      if inputs is None:
        return None
      items = inputs.all() if hasattr(inputs, "all") else list(inputs)
      for inp in items:
        utd = getattr(inp, "upstream_target_dataset", None)
        if utd is not None:
          return utd
    except Exception:
      return None
    return None

  
  def _infer_types_from_node(self, node) -> dict[str, dict[str, Any]]:
    """
    Recursively infer output column types from query nodes.
    """

    node_type = (getattr(node, "node_type", "") or "").strip().lower()
    out: dict[str, dict[str, Any]] = {}

    # -----------------------------
    # WINDOW NODE
    # -----------------------------
    if node_type == "window":
      win = getattr(node, "window", None)

      # inherit upstream types first
      inp = getattr(win, "input_node", None)
      if inp:
        out.update(self._infer_types_from_node(inp))

      cols = []
      try:
        cols = list(win.columns.all().order_by("ordinal_position", "id"))
      except Exception:
        cols = []

      for c in cols:
        out_name = (getattr(c, "output_name", "") or "").strip()
        if not out_name:
          continue

        fn_raw = (
          getattr(c, "function", None)
          or getattr(c, "window_function", None)
          or getattr(c, "op", None)
          or ""
        )
        fn = self._norm_fn(fn_raw)

        if fn in ("row_number", "rownumber", "rank", "dense_rank", "denserank"):
          out[_norm(out_name)] = {"datatype": self._best_integer_datatype()}
        else:
          # Make window output deterministic (otherwise it falls back later)
          out[_norm(out_name)] = {"datatype": "STRING"}

      return out

    # -----------------------------
    # AGGREGATE NODE
    # -----------------------------
    if node_type == "aggregate":
      agg = getattr(node, "aggregate", None)
      if not agg:
        return {}

      # inherit upstream types first
      inp = getattr(agg, "input_node", None)
      upstream_types = self._infer_types_from_node(inp) if inp else {}
      out.update(upstream_types)

      input_td = getattr(inp, "target_dataset", None)

      measures = []
      try:
        measures = list(agg.measures.all().order_by("ordinal_position", "id"))
      except Exception:
        measures = []

      for m in measures:
        out_name = (getattr(m, "output_name", "") or "").strip()
        if not out_name:
          continue

        fn = (getattr(m, "function", "") or "").strip().lower()
        arg = (getattr(m, "input_column_name", "") or "").strip()

        if fn == "count":
          out[_norm(out_name)] = {"datatype": self._best_integer_datatype()}
          continue

        if fn in ("sum", "min", "max", "avg"):
          inferred = self._infer_type_from_target_dataset(input_td, arg)
          if inferred:
            out[_norm(out_name)] = inferred
          else:
            out[_norm(out_name)] = {"datatype": "STRING"}
          continue

        out[_norm(out_name)] = {"datatype": "STRING"}

      return out

    # -----------------------------
    # UNION NODE
    # -----------------------------
    if node_type == "union":
      un = getattr(node, "union", None)
      if not un:
        return {}

      # union inherits from first branch (all branches must be compatible)
      first_branch = un.branches.first()
      if first_branch and first_branch.input_node:
        return self._infer_types_from_node(first_branch.input_node)

      return {}

    # -----------------------------
    # SELECT or fallback
    # -----------------------------
    # passthrough
    try:
      sel = getattr(node, "select", None)
      inp = getattr(sel, "input_node", None) if sel else None
      if inp:
        return self._infer_types_from_node(inp)
    except Exception:
      pass

    return {}


  def _infer_type_from_target_dataset(self, td: TargetDataset, col_name: str) -> Optional[dict[str, Any]]:
    """
    Take datatype/length/precision/scale from an existing TargetColumn in the given dataset.
    No widening: we keep the existing physical/canonical choice as-is.
    """
    name = (col_name or "").strip()
    if not name or not td:
      return None

    tc = (
      TargetColumn.objects
        .filter(target_dataset=td, active=True, target_column_name=name)
        .order_by("id")
        .first()
    )
    if not tc:
      return None

    dtype = (getattr(tc, "datatype", "") or "").strip()
    if not dtype:
      return None

    return {
      "datatype": dtype,
      "max_length": getattr(tc, "max_length", None),
      "decimal_precision": getattr(tc, "decimal_precision", None),
      "decimal_scale": getattr(tc, "decimal_scale", None),
    }


  # -----------------------------
  # Ordinals
  # -----------------------------
  def _next_query_derived_ordinal_base(self, td: TargetDataset) -> int:
    """
    Base ordinal = max ordinal among NON query_derived columns.
    Query-derived columns are appended after that segment to avoid unique collisions.
    """
    base = 0
    qs = TargetColumn.objects.filter(target_dataset=td).only("ordinal_position", "lineage_origin")
    for c in qs:
      if (getattr(c, "lineage_origin", "") or "") == self.LINEAGE_ORIGIN_QUERY:
        continue
      op = getattr(c, "ordinal_position", None) or 0
      if op > base:
        base = op
    return base


  def _normalize_query_derived_ordinals(self, td: TargetDataset, desired_norm: list[str]) -> None:
    """
    Keep query_derived columns ordered like the contract, but NEVER collide with
    existing dataset ordinals (unique constraint on (target_dataset, ordinal_position)).

    Policy: place query_derived columns AFTER all non-query_derived columns.
    Uses a 2-pass update to avoid collisions among query_derived columns themselves.
    """
    if not desired_norm:
      return

    all_cols = list(
      TargetColumn.objects
      .filter(target_dataset=td)
      .only("id", "target_column_name", "ordinal_position", "lineage_origin")
    )

    qcols = [c for c in all_cols if (getattr(c, "lineage_origin", "") or "") == self.LINEAGE_ORIGIN_QUERY]
    if not qcols:
      return

    by_norm = {_norm(c.target_column_name): c for c in qcols}

    # Base = max ordinal among NON query_derived columns
    base = 0
    for c in all_cols:
      if (getattr(c, "lineage_origin", "") or "") == self.LINEAGE_ORIGIN_QUERY:
        continue
      op = getattr(c, "ordinal_position", None) or 0
      if op > base:
        base = op

    # Safe high range that cannot collide with anything currently in the dataset
    max_ord = 0
    for c in all_cols:
      op = getattr(c, "ordinal_position", None) or 0
      if op > max_ord:
        max_ord = op
    safe_base = max_ord + 1000

    # Pass 1: move query_derived cols into safe high range in contract order
    ordered = []
    for dn in desired_norm:
      c = by_norm.get(dn)
      if c is not None:
        ordered.append(c)
    # Append any remaining query_derived cols not in contract (should be rare; keeps determinism)
    rest = [c for c in qcols if c not in ordered]
    rest.sort(key=lambda x: ((getattr(x, "ordinal_position", None) or 0), x.id))
    ordered.extend(rest)

    for idx, c in enumerate(ordered, start=1):
      c.ordinal_position = safe_base + idx
      c.save(update_fields=["ordinal_position"])

    # Pass 2: final positions directly after non-query columns
    for idx, c in enumerate(ordered, start=1):
      final_pos = base + idx
      if c.ordinal_position != final_pos:
        c.ordinal_position = final_pos
        c.save(update_fields=["ordinal_position"])
