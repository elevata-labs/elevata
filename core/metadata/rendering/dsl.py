"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025 Ilona Tag

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

from typing import Iterable, Optional, List
import re

from .expr import (
  Expr,
  ColumnRef,
  RawSql,
  FuncCall,
  Literal,
  Concat,
  Coalesce,
  Cast,
  row_number_over as _row_number_over,
)


# ---------------------------------------------------------------------------
# Basic DSL helpers that create Expr nodes from expr.py
# ---------------------------------------------------------------------------

def col(name: str, table: Optional[str] = None) -> ColumnRef:
  """
  Convenience helper for a column reference, optionally qualified with a table alias.

  Example:
      col("customer_id")          -> ColumnRef(None, "customer_id")
      col("customer_id", "s")     -> ColumnRef("s", "customer_id")
  """
  return ColumnRef(table_alias=table, column_name=name)


def lit(value: str) -> Literal:
  """Create a literal expression."""
  return Literal(value)


def raw(sql: str) -> RawSql:
  """
  Convenience helper for a raw SQL fragment.

  Use sparingly – raw SQL bypasses dialect rendering.
  """
  return RawSql(sql=sql)


def row_number(
  partition_by: Optional[Iterable[Expr]] = None,
  order_by: Optional[Iterable[Expr]] = None,
) -> Expr:
  """Wrapper to create a ROW_NUMBER() OVER (...) expression."""
  return _row_number_over(
    partition_by=list(partition_by or []),
    order_by=list(order_by or []),
  )


# ---------------------------------------------------------------------------
# Hashing / Concatenation DSL (AST-producing)
# ---------------------------------------------------------------------------

def concat_expr(exprs: List[Expr]) -> Expr:
  """Vendor-neutral concatenation of multiple parts."""
  return Concat(parts=list(exprs))


def concat_ws_expr(sep: str, exprs: List[Expr]) -> Expr:
  """
  CONCAT_WS(sep, ...)

  Represented as FuncCall because expr.py does not have a dedicated
  ConcatWs node.
  """
  return FuncCall(
    name="CONCAT_WS",
    args=[Literal(sep)] + list(exprs),
  )


def coalesce_expr(expr: Expr, null_value: str) -> Expr:
  """
  Vendor-neutral COALESCE(expr, null_value)

  Note:
    For hashing / key generation we normalize to string to avoid type coercion
    errors (e.g. INT + 'null_replaced').
  """
  return Coalesce([Cast(expr=expr, target_type="string"), Literal(null_value)])


def hash_expr(expr: Expr) -> Expr:
  """
  Vendor-neutral hashing function placeholder.
  Implemented by Dialects:
    DuckDB:   hash(expr)
    Postgres: md5(expr)
    MSSQL:    CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', expr), 2)
  """
  return FuncCall(name="HASH", args=[expr])

# ---------------------------------------------------------------------------
# DSL Parser for surrogate key expressions
# ---------------------------------------------------------------------------

def parse_surrogate_dsl(dsl: str, table_alias: str | None = None) -> Expr:
  """
  Parse the elevata surrogate-key DSL into an Expr tree.

  Supported:
    HASH256(expr)
    CONCAT(expr1, expr2, ...)
    CONCAT_WS(sep, expr1, expr2, ...)
    COALESCE(expr, null_literal)
    COL(columnname)
    'literal' / "literal"
    {expr:columnname}
  """
  dsl = dsl.strip()

  # ----- placeholder "{expr:col}" ----------------------------------------
  m = re.fullmatch(r"\{expr:([A-Za-z0-9_]+)\}", dsl)
  if m:
    colname = m.group(1)
    return ColumnRef(table_alias=table_alias, column_name=colname)

  # ----- literal string ---------------------------------------------------
  if (dsl.startswith("'") and dsl.endswith("'")) or \
     (dsl.startswith('"') and dsl.endswith('"')):
    return Literal(dsl[1:-1])

  # ----- COL(x) -----------------------------------------------------------
  if dsl.upper().startswith("COL(") and dsl.endswith(")"):
    inner = dsl[4:-1].strip()

    # strip common identifier quoting styles:
    # "col", 'col', `col`, [col]
    if (
      (inner.startswith('"') and inner.endswith('"')) or
      (inner.startswith("'") and inner.endswith("'")) or
      (inner.startswith("`") and inner.endswith("`")) or
      (inner.startswith("[") and inner.endswith("]"))
    ):
      inner = inner[1:-1].strip()

    return ColumnRef(table_alias=table_alias, column_name=inner)


  # ----- HASH256(expr) ----------------------------------------------------
  if dsl.upper().startswith("HASH256(") and dsl.endswith(")"):
    inner = dsl[len("HASH256("):-1].strip()
    inner_expr = parse_surrogate_dsl(inner, table_alias)
    return FuncCall(name="HASH256", args=[inner_expr])

  # ----- CONCAT_WS(sep, ...) ----------------------------------------------
  if dsl.upper().startswith("CONCAT_WS(") and dsl.endswith(")"):
    inner = dsl[len("CONCAT_WS("):-1].strip()
    first_arg, rest = _split_first_arg(inner)
    sep = parse_surrogate_dsl(first_arg.strip(), table_alias)
    parts = _split_args(rest)
    exprs = [parse_surrogate_dsl(p, table_alias) for p in parts]
    return FuncCall(
      name="CONCAT_WS",
      args=[sep] + exprs
    )

  # ----- CONCAT(expr1, expr2, ...) ---------------------------------------
  if dsl.upper().startswith("CONCAT(") and dsl.endswith(")"):
    inner = dsl[len("CONCAT("):-1].strip()
    parts = _split_args(inner)
    exprs = [parse_surrogate_dsl(p, table_alias) for p in parts]
    return Concat(parts=exprs)

  # ----- COALESCE(expr, null) --------------------------------------------
  if dsl.upper().startswith("COALESCE(") and dsl.endswith(")"):
    inner = dsl[len("COALESCE("):-1].strip()
    parts = _split_args(inner)
    if len(parts) != 2:
      raise ValueError("COALESCE must have exactly 2 arguments")

    left = parse_surrogate_dsl(parts[0], table_alias)
    right = parse_surrogate_dsl(parts[1], table_alias)

    # Cast left side to string to avoid type coercion issues across dialects
    # (e.g. INT columns with 'null_replaced' fallbacks).
    left = Cast(expr=left, target_type="string")

    return Coalesce([left, right])

  raise ValueError(f"Unsupported DSL expression: {dsl!r}")


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _split_args(s: str) -> list[str]:
  """Split arguments at top-level commas."""
  args = []
  depth = 0
  start = 0
  for i, ch in enumerate(s):
    if ch == '(':
      depth += 1
    elif ch == ')':
      depth -= 1
    elif ch == ',' and depth == 0:
      args.append(s[start:i].strip())
      start = i + 1
  args.append(s[start:].strip())
  return args


def _split_first_arg(s: str) -> tuple[str, str]:
  """
  Split first argument from the rest:
     "arg1, arg2, arg3"
  →   ("arg1", "arg2, arg3")
  """
  depth = 0
  for i, ch in enumerate(s):
    if ch == '(':
      depth += 1
    elif ch == ')':
      depth -= 1
    elif ch == ',' and depth == 0:
      return s[:i], s[i+1:]
  return s, ""
