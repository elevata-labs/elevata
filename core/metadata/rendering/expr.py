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
from typing import List, Optional, Dict


class Expr:
  """Marker base class for all logical SQL expression nodes."""
  pass


@dataclass
class Cast(Expr):
  """Vendor-neutral CAST(expr AS target type)."""
  expr: Expr
  target_type: str


@dataclass
class Coalesce(Expr):
  """Vendor-neutral representation for COALESCE(a, b, ...)."""
  parts: List[Expr]


@dataclass
class ColumnRef(Expr):
  """Reference to a column, optionally qualified by a table alias."""
  table_alias: Optional[str]
  column_name: str


@dataclass
class Concat(Expr):
  """Vendor-neutral representation for string concatenation of multiple parts."""
  parts: List[Expr]


@dataclass(frozen=True)
class OrderByExpr(Expr):
  """
  An ORDER BY expression used inside functions (e.g. STRING_AGG(... ORDER BY ...)).
  This allows dialects to render quoted identifiers correctly.
  """
  expr: Expr
  direction: str = "ASC"  # "ASC" | "DESC"


@dataclass(frozen=True)
class OrderByClause:
  """
  Multi-key ORDER BY clause used inside functions.
  Rendered as: "<item1>, <item2>, ..."
  """
  items: List[OrderByExpr]


@dataclass
class FuncCall(Expr):
  """Generic function call expression, e.g. UPPER(col), COALESCE(a,b), HASH256(x)."""
  name: str
  args: List[Expr]


@dataclass
class Literal(Expr):
  """Simple literal value: string, number, bool, or None."""
  value: object


@dataclass
class WindowSpec(Expr):
  """Logical window specification for window functions."""
  partition_by: List[Expr] = field(default_factory=list)
  order_by: List[Expr] = field(default_factory=list)


@dataclass
class WindowFunction(Expr):
  """
  Generic window function expression, e.g.:

    ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
    SUM(amount) OVER (...)
  """
  name: str
  args: List[Expr] = field(default_factory=list)
  window: WindowSpec = field(default_factory=WindowSpec)


@dataclass
class RawSql(Expr):
  """
  Raw SQL expression coming directly from metadata (e.g. surrogate_expression
  or manual_expression).

  Two modes:
  - is_template = True:
      `sql` may contain placeholders like {alias} or {expr:column_name}.
      These will be resolved by the SQL dialect using `expr_bindings`.
  - is_template = False:
      `sql` is treated as already-valid SQL in the target platform and is
      rendered mostly verbatim (apart from optional {alias} replacement).
  """
  sql: str
  default_table_alias: Optional[str] = None
  is_template: bool = False
  expr_bindings: Dict[str, Expr] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Convenience constructors
# ---------------------------------------------------------------------------

def L(value: object) -> Literal:
  """Helper for creating a Literal."""
  return Literal(value=value)


def COL(column_name: str, table_alias: Optional[str] = None) -> ColumnRef:
  """Helper for creating a ColumnRef."""
  return ColumnRef(table_alias=table_alias, column_name=column_name)


def FUNC(name: str, *args: Expr) -> FuncCall:
  """Helper for generic function calls."""
  return FuncCall(name=name, args=list(args))


def HASH256(expr: Expr) -> Expr:
  """Vendor-neutral representation for a 256-bit hash function."""
  return FuncCall(name="HASH256", args=[expr])


def CONCAT(*parts: Expr) -> Expr:
  """Vendor-neutral string concatenation."""
  return Concat(parts=list(parts))


def COALESCE(*parts: Expr) -> Expr:
  """Vendor-neutral COALESCE."""
  return Coalesce(parts=list(parts))


def row_number_over(
  partition_by: List[Expr] | None = None,
  order_by: List[Expr] | None = None,
) -> WindowFunction:
  """
  Convenience helper for ROW_NUMBER() window expressions.
  """
  return WindowFunction(
    name="ROW_NUMBER",
    args=[],
    window=WindowSpec(
      partition_by=partition_by or [],
      order_by=order_by or [],
    ),
  )
