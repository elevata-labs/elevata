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
import datetime
from decimal import Decimal

from .base import SqlDialect
from ..expr import Expr, Cast, Concat, Coalesce, ColumnRef, FuncCall, Literal, RawSql, WindowFunction, WindowSpec
from ..logical_plan import LogicalSelect, SelectItem, SourceTable, Join, SubquerySource


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

  DIALECT_NAME = "duckdb"

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
      rendered_parts = [self.render_expr(p) for p in expr.parts]
      return self.concat_expression(rendered_parts)

    if isinstance(expr, Coalesce):
      args_sql = ", ".join(self.render_expr(p) for p in expr.parts)
      return f"COALESCE({args_sql})"

    if isinstance(expr, FuncCall):
      # Vendor-neutral function names are mapped here.
      name_upper = expr.name.upper()

      if name_upper == "HASH256":
        if len(expr.args) != 1:
          raise ValueError("HASH256 expects exactly one argument")
        inner = self.render_expr(expr.args[0])
        return self.hash_expression(inner, algo="sha256")

      # Fallback: generic CALL(args...)
      args_sql = ", ".join(self.render_expr(a) for a in expr.args)
      return f"{name_upper}({args_sql})"
    
    if isinstance(expr, Literal):
      return self.render_literal(expr.value)

    if isinstance(expr, WindowFunction):
      # Render function name and arguments
      func_name = expr.name.upper()
      if expr.args:
        args_sql = ", ".join(self.render_expr(a) for a in expr.args)
      else:
        args_sql = ""
      func_sql = f"{func_name}({args_sql})"

      # Build OVER clause
      win = expr.window or WindowSpec()
      parts: list[str] = []

      if win.partition_by:
        part_sql = ", ".join(self.render_expr(e) for e in win.partition_by)
        parts.append(f"PARTITION BY {part_sql}")

      if win.order_by:
        order_sql = ", ".join(self.render_expr(e) for e in win.order_by)
        parts.append(f"ORDER BY {order_sql}")

      over_body = " ".join(parts)
      if not over_body:
        over_body = ""  # OVER ()

      if over_body:
        return f"{func_sql} OVER ({over_body})"
      return f"{func_sql} OVER ()"

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
  

  def concat_expression(self, parts):
    # parts are already rendered SQL expressions
    if not parts:
      return "''"
    return "(" + " || ".join(parts) + ")"


  def hash_expression(self, expr: str, algo: str = "sha256") -> str:
    algo_lower = algo.lower()
    if algo_lower in ("sha256", "hash256"):
      return f"SHA256({expr})"
    # fallback: still SHA256 for unknown algos for now
    return f"SHA256({expr})"
  

  def render_literal(self, value):
    if value is None:
      return "NULL"
    if isinstance(value, bool):
      return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
      return str(value)
    if isinstance(value, Decimal):
      return str(value)

    if isinstance(value, str):
      escaped = value.replace("'", "''")
      return f"'{escaped}'"

    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
      return f"DATE '{value.isoformat()}'"

    if isinstance(value, datetime.datetime):
      return f"TIMESTAMP '{value.isoformat(sep=' ', timespec='seconds')}'"

    raise TypeError(f"Unsupported literal type: {type(value)}")


  def cast_expression(self, expr: str, target_type: str) -> str:
    return f"CAST({expr} AS {target_type})"


  # ---------------------------------------------------------------------------
  # SELECT rendering
  # ---------------------------------------------------------------------------

  def _render_source_table(self, table: SourceTable) -> str:
    """
    Render schema.table AS alias (schema is optional).
    """
    return self.render_table_alias(table.schema, table.name, table.alias)
  
  def _render_from_item(self, item: SourceTable | SubquerySource) -> str:
    """
    Render either a base table or a subquery in FROM/JOIN.
    """
    if isinstance(item, SourceTable):
      return self.render_table_alias(item.schema, item.name, item.alias)

    if isinstance(item, SubquerySource):
      inner = item.select
      # LogicalSelect vs LogicalUnion
      if isinstance(inner, LogicalSelect):
        inner_sql = self.render_select(inner)
      else:
        # LogicalUnion or other object with to_sql(dialect)
        inner_sql = inner.to_sql(self)

      return f"(\n{inner_sql}\n) AS {item.alias}"

    raise TypeError(f"Unsupported FROM item: {type(item)!r}")

  def _render_join(self, join: Join) -> str:
    right_sql = self._render_from_item(join.right)
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
    parts.append("  " + self._render_from_item(select.from_))

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

  # ---------------------------------------------------------------------------
  # Incremental / MERGE Rendering
  # ---------------------------------------------------------------------------

  def render_create_replace_table(self, schema: str, table: str, select_sql: str) -> str:
    """
    CREATE OR REPLACE TABLE schema.table AS <select>
    """
    full = self.quote_table(schema, table)
    return f"CREATE OR REPLACE TABLE {full} AS\n{select_sql}"

  def render_insert_into_table(self, schema: str, table: str, select_sql: str) -> str:
    """
    INSERT INTO schema.table <select>
    """
    full = self.quote_table(schema, table)
    return f"INSERT INTO {full}\n{select_sql}"

  def render_merge_statement(
      self,
      schema: str,
      table: str,
      select_sql: str,
      unique_key_columns: list[str],
      update_columns: list[str],
  ) -> str:
    """
    Render a DuckDB MERGE INTO statement.

    Parameters
    ----------
    schema : str
      Schema of target table
    table : str
      Target table name
    select_sql : str
      SQL of the incremental source SELECT
    unique_key_columns : list[str]
      Columns used to match target rows
    update_columns : list[str]
      Columns that should be updated on MATCHED
    """

    full = self.quote_table(schema, table)

    # Build ON condition (t.pk = s.pk AND ...)
    on_clause = " AND ".join(
      f"t.{self.quote_ident(c)} = s.{self.quote_ident(c)}"
      for c in unique_key_columns
    )

    # Build UPDATE clause
    update_assignments = ", ".join(
      f"{self.quote_ident(col)} = s.{self.quote_ident(col)}"
      for col in update_columns
    )

    # INSERT column lists
    all_cols = unique_key_columns + update_columns
    col_list = ", ".join(self.quote_ident(c) for c in all_cols)
    val_list = ", ".join(f"s.{self.quote_ident(c)}" for c in all_cols)

    return f"""
      MERGE INTO {full} AS t
      USING (
      {select_sql}
      ) AS s
      ON {on_clause}
      WHEN MATCHED THEN UPDATE SET {update_assignments}
      WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list});
      """.strip()

  def render_delete_detection_statement(
    self,
    target_schema,
    target_table,
    stage_schema,
    stage_table,
    join_predicates,
    scope_filter=None,
  ):
    """
    DuckDB implementation of delete detection using DELETE + NOT EXISTS.
    """
    q = self.quote_ident

    target_qualified = f'{q(target_schema)}.{q(target_table)}'
    stage_qualified = f'{q(stage_schema)}.{q(stage_table)}'

    join_sql = " AND ".join(join_predicates)

    conditions = []

    if scope_filter:
      # scope_filter is already a full boolean expression,
      # e.g. (ModifiedDate > {{DELTA_CUTOFF}})
      conditions.append(scope_filter)

    # NOT EXISTS subquery using the provided join predicates
    conditions.append(
      f"NOT EXISTS (\n"
      f"    SELECT 1\n"
      f"    FROM {stage_qualified} AS s\n"
      f"    WHERE {join_sql}\n"
      f")"
    )

    where_sql = "\n  AND ".join(conditions)

    return (
      f'DELETE FROM {target_qualified} AS t\n'
      f'WHERE {where_sql};'
    )
