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

from typing import List
import re

from .base import SqlDialect
from ..expr import Expr, Cast, Concat, Coalesce, ColumnRef, FuncCall, Literal, RawSql
from ..logical_plan import LogicalSelect, SelectItem, SourceTable, Join


class DuckDBDialect(SqlDialect):
  """
  DuckDB SQL dialect implementation.

  Assumptions:
  - Identifiers are quoted with double quotes.
  - String literals use single quotes.
  - CONCAT is rendered via the || operator.
  - COALESCE is supported natively.
  - HASH256 is mapped to SHA256(expr) for now (can be adapted if hex encoding is required).
  """

  # ---------------------------------------------------------------------------
  # Identifier & literal helpers
  # ---------------------------------------------------------------------------

  def quote_ident(self, name: str) -> str:
    """
    Quote an identifier using DuckDB's double-quote style.
    Internal double quotes are escaped by doubling them.
    """
    escaped = name.replace('"', '""')
    return f'"{escaped}"'

  def _render_literal(self, lit: Literal) -> str:
    v = lit.value
    if v is None:
      return "NULL"
    if isinstance(v, bool):
      return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
      return repr(v)
    # treat everything else as string
    s = str(v).replace("'", "''")
    return f"'{s}'"
  
  def map_logical_type(
    self,
    logical_type,          # type: str
    max_length=None,       # type: int | None
    precision=None,        # type: int | None
    scale=None,            # type: int | None
  ):
    """
    Map a logical elevata datatype string to a DuckDB type string.

    logical_type can be generic ("string", "int", "decimal")
    or already a concrete DB type ("VARCHAR(100)", "DECIMAL(18,2)").
    """
    if not logical_type:
      return None

    t = str(logical_type).lower()

    if t in ("string", "text", "varchar", "char"):
      if max_length:
        return f"VARCHAR({max_length})"
      return "VARCHAR"

    if t in ("int", "integer", "int32"):
      return "INTEGER"

    if t in ("bigint", "int64", "long"):
      return "BIGINT"

    if t in ("decimal", "numeric"):
      if precision and scale is not None:
        return f"DECIMAL({precision}, {scale})"
      if precision:
        return f"DECIMAL({precision})"
      return "DECIMAL"

    if t in ("float", "double"):
      return "DOUBLE"

    if t in ("bool", "boolean"):
      return "BOOLEAN"

    if t in ("date",):
      return "DATE"

    if t in ("timestamp", "timestamptz", "datetime"):
      return "TIMESTAMP"

    # Fallback: trust the user, he hopefully knows what he does
    return logical_type

  # ---------------------------------------------------------------------------
  # Expression rendering
  # ---------------------------------------------------------------------------

  def render_expr(self, expr: Expr) -> str:
    if isinstance(expr, ColumnRef):
      if expr.table_alias:
        return f"{expr.table_alias}.{self.quote_ident(expr.column_name)}"
      return self.quote_ident(expr.column_name)

    if isinstance(expr, Cast):
      inner = self.render_expr(expr.expr)
      db_type = self.map_logical_type(expr.target_type)
      if db_type is None:
        # no mapping, simply ignore
        return inner
      return f"CAST({inner} AS {db_type})"

    if isinstance(expr, Concat):
      # Use || operator: (part1 || part2 || part3)
      rendered_parts = [self.render_expr(p) for p in expr.parts]
      if not rendered_parts:
        return "''"
      return "(" + " || ".join(rendered_parts) + ")"

    if isinstance(expr, Coalesce):
      args_sql = ", ".join(self.render_expr(p) for p in expr.parts)
      return f"COALESCE({args_sql})"

    if isinstance(expr, FuncCall):
      # Vendor-neutral function names are mapped here.
      name_upper = expr.name.upper()

      if name_upper == "HASH256":
        # DuckDB: SHA256(expr)
        # If you later need hex encoding, you can wrap with encode(..., 'hex').
        if len(expr.args) != 1:
          raise ValueError("HASH256 expects exactly one argument")
        inner = self.render_expr(expr.args[0])
        return f"SHA256({inner})"

      # Fallback: generic CALL(args...)
      args_sql = ", ".join(self.render_expr(a) for a in expr.args)
      return f"{name_upper}({args_sql})"
    
    if isinstance(expr, Literal):
      return self._render_literal(expr)

    if isinstance(expr, RawSql):
      # Start from the raw SQL template string
      sql = expr.sql

      # 1) replace {alias} if exists – works for template and plain sql
      if getattr(expr, "default_table_alias", None):
        sql = sql.replace("{alias}", expr.default_table_alias)

      # 2) Template-Mode: {expr:<name>} via expr_bindings -> build final expression
      if getattr(expr, "is_template", False) and getattr(expr, "expr_bindings", None):
        def repl_expr(match: re.Match) -> str:
          key = match.group(1)  # eg. "productid"
          bound = expr.expr_bindings.get(key)
          if bound is None:
            raise ValueError(
              f"Missing expr_binding for {key} in RawSql template: {expr.sql}"
            )
          return self.render_expr(bound)

        sql = re.sub(r"\{expr:([A-Za-z0-9_]+)\}", repl_expr, sql)

      return sql
   
    raise TypeError(f"Unsupported expression type for DuckDBDialect: {type(expr)!r}")
  

  # ---------------------------------------------------------------------------
  # SELECT rendering
  # ---------------------------------------------------------------------------

  def _render_source_table(self, table: SourceTable) -> str:
    """
    Render schema.table AS alias (schema is optional).
    """
    name_sql = self.quote_ident(table.name)
    if table.schema:
      schema_sql = self.quote_ident(table.schema)
      full_name = f"{schema_sql}.{name_sql}"
    else:
      full_name = name_sql

    return f"{full_name} AS {table.alias}"

  def _render_join(self, join: Join) -> str:
    right_sql = self._render_source_table(join.right)
    join_type = (join.join_type or "inner").upper()
    on_sql = self.render_expr(join.on)
    return f"{join_type} JOIN {right_sql} ON {on_sql}"

  def _render_select_list(self, items: List[SelectItem]) -> str:
    rendered_items = []
    for item in items:
      expr_sql = self.render_expr(item.expr)
      if item.alias:
        rendered_items.append(f"{expr_sql} AS {item.alias}")
      else:
        rendered_items.append(expr_sql)
    return ", ".join(rendered_items) if rendered_items else "*"

  def render_select(self, select: LogicalSelect) -> str:
    """
    Render a LogicalSelect into a DuckDB SELECT statement.
    """
    parts: List[str] = []

    # SELECT [DISTINCT] ...
    select_kw = "SELECT DISTINCT" if select.distinct else "SELECT"
    parts.append(select_kw)
    parts.append("  " + self._render_select_list(select.select_list))

    # FROM ...
    parts.append("FROM")
    parts.append("  " + self._render_source_table(select.from_))

    # JOINs
    for j in select.joins:
      parts.append("  " + self._render_join(j))

    # WHERE
    if select.where is not None:
      where_sql = self.render_expr(select.where)
      parts.append("WHERE")
      parts.append("  " + where_sql)

    # GROUP BY
    if select.group_by:
      gb_sql = ", ".join(self.render_expr(e) for e in select.group_by)
      parts.append("GROUP BY")
      parts.append("  " + gb_sql)

    # ORDER BY
    if select.order_by:
      ob_sql = ", ".join(self.render_expr(e) for e in select.order_by)
      parts.append("ORDER BY")
      parts.append("  " + ob_sql)

    return "\n".join(parts)
