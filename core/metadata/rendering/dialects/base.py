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

from abc import ABC, abstractmethod
from typing import Sequence, Any, Dict, Optional

import datetime
from decimal import Decimal

from ..expr import (
  Cast,
  Concat,
  Coalesce,
  ColumnRef,
  Expr,
  FuncCall,
  Literal,
  RawSql,
  WindowFunction,
  WindowSpec,
)
from ..logical_plan import Join, LogicalSelect, SelectItem, SourceTable, SubquerySource

from metadata.system.introspection import read_table_metadata


class BaseExecutionEngine:
  def execute(self, sql: str) -> int | None:
    raise NotImplementedError
  
  def execute_many(self, sql: str, params_seq) -> int | None:
    """
    Optional bulk execution for parameterized statements.
    Dialects/engines should override if they support executemany().
    """
    raise NotImplementedError


class SqlDialect(ABC):
  """
  Base interface for SQL dialects.
  Implementations translate Expr / LogicalSelect into final SQL strings.
  """

  # ---------------------------------------------------------------------------
  # Execution
  # ---------------------------------------------------------------------------
  def get_execution_engine(self, system) -> "BaseExecutionEngine":
    raise NotImplementedError(
      f"{self.__class__.__name__} does not provide an execution engine."
    )


  # ---------------------------------------------------------------------------
  # Introspection
  # ---------------------------------------------------------------------------
  def introspect_table(
    self,
    *,
    schema_name: str,
    table_name: str,
    introspection_engine: Any,
    exec_engine: Optional["BaseExecutionEngine"] = None,
    debug_plan: bool = False,
  ) -> Dict[str, Any]:
    """
    Default introspection via SQLAlchemy/read_table_metadata.

    Returns:
      {
        "table_exists": bool,
        "physical_table": str,
        "actual_cols_by_norm_name": {norm_name: column_meta_dict}
      }
    """
    # Default path uses SQLAlchemy-based metadata reading.
    try:
      meta = read_table_metadata(introspection_engine, schema_name, table_name)
    except Exception:
      if debug_plan:
        return {
          "table_exists": False,
          "physical_table": table_name,
          "actual_cols_by_norm_name": {},
          "debug": f"read_table_metadata failed for {schema_name}.{table_name}",
        }
      return {
        "table_exists": False,
        "physical_table": table_name,
        "actual_cols_by_norm_name": {},
      }

    cols = {}
    for c in (meta.get("columns") or []):
      nm = (c.get("name") or c.get("column_name") or "").strip().lower()
      if nm:
        cols[nm] = c

    return {
      "table_exists": True,
      "physical_table": table_name,
      "actual_cols_by_norm_name": cols,
    }


  # ---------------------------------------------------------------------------
  # Capabilities (can be overridden by concrete dialects)
  # ---------------------------------------------------------------------------
  @property
  def supports_merge(self) -> bool:
    """Whether this dialect supports a native MERGE statement."""
    # Dialects must explicitly opt in by overriding this property.
    return False

  @property
  def supports_delete_detection(self) -> bool:
    """
    Whether delete detection is supported as a first-class operation
    (either via DELETE ... NOT EXISTS, DELETE USING, etc.).
    """
    # Dialects must explicitly opt in by overriding this property.
    return False
  
  # ---------------------------------------------------------------------------
  # Parameter placeholders
  # ---------------------------------------------------------------------------
  def param_placeholder(self) -> str:
    """
    Placeholder for parameterized SQL statements used by the dialect's execution engine.
    Default matches DB-API qmark style (DuckDB, pyodbc).
    """
    return "?"

  # ---------------------------------------------------------------------------
  # Identifier quoting
  # ---------------------------------------------------------------------------
  @abstractmethod
  def quote_ident(self, name: str) -> str:
    """
    Quote an identifier (schema, table, column) according to the dialect.
    """
    raise NotImplementedError

  def should_quote(self, name: str) -> bool:
    """
    Decide whether identifier must be quoted.
    Rules:
      - empty or None → quote
      - contains whitespace or not alnum/_ → quote
      - starts with digit → quote
      - all-uppercase/lowercase safe sql keywords → quote
    """
    if not name:
      return True
    if name[0].isdigit():
      return True
    if not name.replace("_", "").isalnum():
      return True
    # future: check dialect keyword lists
    return False

  # -------------------------------------------------------------------------
  # Generic helpers built on top of quote_ident
  # -------------------------------------------------------------------------
  def render_identifier(self, name: str) -> str:
    """
    Apply quoting only when necessary.
    """
    if self.should_quote(name):
      return self.quote_ident(name)
    return name

  def render_table_identifier(self, schema: str | None, name: str) -> str:
    """
    Render a table identifier with optional schema.

      render_table_identifier("rawcore", "customer") -> rawcore.customer (quoted as needed)
      render_table_identifier(None, "customer")      -> customer (quoted as needed)
    """
    name_sql = self.render_identifier(name)
    if schema:
      schema_sql = self.render_identifier(schema)
      return f"{schema_sql}.{name_sql}"
    return name_sql

  def literal(self, value: Any) -> str:
    """
    Render a Python value as a SQL literal in a dialect-agnostic way.
    Dialects may override this if they need special handling.
    """
    if value is None:
      return "NULL"

    if isinstance(value, bool):
      return "TRUE" if value else "FALSE"

    if isinstance(value, (int, float)):
      # For now we rely on the default string representation.
      return str(value)

    # Fallback for strings and other objects: use str() and escape single quotes.
    s = str(value)
    s = s.replace("'", "''")
    return f"'{s}'"
  
  def cast(self, expr: str, target_type: str) -> str:
    """
    Wrap an expression in a CAST(... AS ...) construct.
    Dialects may override this if they need a different syntax.
    """
    return f"CAST({expr} AS {target_type})"

  def render_table_alias(
    self,
    schema: str | None,
    name: str,
    alias: str | None,
  ) -> str:
    """
    Render a table reference including optional alias, e.g.:

      render_table_alias("rawcore", "customer", "c")
      -> "rawcore"."customer" AS c
    """
    base = self.render_table_identifier(schema, name)
    if alias:
      return f"{base} AS {alias}"
    return base

  def render_column_list(self, columns: list[str] | None) -> str:
    """
    Render a comma-separated list of column identifiers, with proper quoting.
    If columns is None or empty, '*' is returned.
    """
    if not columns:
      return "*"
    return ", ".join(self.quote_ident(c) for c in columns)

  # ---------------------------------------------------------------------------
  # Expression rendering
  # ---------------------------------------------------------------------------
  def render_expr(self, expr: Expr) -> str:
    """
    Default "ANSI-ish" expression renderer.
    Dialects may override for engine-specific functions or syntax.
    """
    if isinstance(expr, Literal):
      return self.render_literal(expr.value)

    if isinstance(expr, ColumnRef):
      if expr.table_alias:
        return f"{self.render_identifier(expr.table_alias)}.{self.render_identifier(expr.column_name)}"
      return self.render_identifier(expr.column_name)

    if isinstance(expr, RawSql):
      # ------------------------------------------------------------
      # RawSql supports two "modes" seen in the codebase:
      #
      # 1) Structured template: expr.template = [str | Expr, ...]
      # 2) DuckDB-style string template:
      #    - replace "{alias}" via default_table_alias
      #    - replace "{expr:<name>}" via is_template + expr_bindings
      # ------------------------------------------------------------
      if getattr(expr, "template", None):
        parts: list[str] = []
        for p in expr.template:
          if isinstance(p, str):
            parts.append(p)
          else:
            parts.append(self.render_expr(p))
        return "".join(parts)

      sql = str(getattr(expr, "sql", ""))

      # DuckDB-style: replace {alias}
      default_alias = getattr(expr, "default_table_alias", None)
      if default_alias:
        sql = sql.replace("{alias}", str(default_alias))

      # DuckDB-style: replace {expr:<name>} via expr_bindings
      if getattr(expr, "is_template", False) and getattr(expr, "expr_bindings", None):
        import re

        def repl_expr(match: re.Match) -> str:
          key = match.group(1)
          bound = expr.expr_bindings.get(key)
          if bound is None:
            raise ValueError(
              f"Missing expr_binding for {key} in RawSql template: {getattr(expr, 'sql', '')}"
            )
          return self.render_expr(bound)

        sql = re.sub(r"\{expr:([A-Za-z0-9_]+)\}", repl_expr, sql)

      return sql

    if isinstance(expr, Cast):
      inner = self.render_expr(expr.expr)
      tt = getattr(expr, "target_type", None)
      if tt is None:
        # fail-safe
        return self.cast_expression(inner, "varchar")

      # Map logical -> physical type for this dialect.
      # Postgres e.g. doesn't support "string" as a type name.
      db_type = self.map_logical_type(datatype=str(tt), strict=False)
      if not db_type:
        # DuckDB behavior: if we can't map, do not emit a CAST.
        return inner
      return self.cast_expression(inner, db_type)

    if isinstance(expr, Coalesce):
      args_sql = ", ".join(self.render_expr(p) for p in expr.parts)
      return f"COALESCE({args_sql})"

    if isinstance(expr, Concat):
      rendered_parts = [self.render_expr(p) for p in expr.parts]
      return self.concat_expression(rendered_parts)
    
    if isinstance(expr, FuncCall):
      fn = (expr.name or "").strip()
      fn_lc = fn.lower()
      args = list(expr.args or [])

      # Special-case: hash256(...) must use dialect hash implementation
      # so tests see digest(...) / HASHBYTES(...) etc.
      if fn_lc in ("hash256", "sha256") and len(args) == 1:
        return self.hash_expression(self.render_expr(args[0]), algo="sha256")

      args_sql = ", ".join(self.render_expr(a) for a in args)
      return f"{fn}({args_sql})"

    if isinstance(expr, WindowFunction):
      func_name = expr.name.upper()
      args_sql = ", ".join(self.render_expr(a) for a in (expr.args or []))
      func_sql = f"{func_name}({args_sql})"
      win = expr.window or WindowSpec()
      parts = []
      if win.partition_by:
        part_sql = ", ".join(self.render_expr(e) for e in win.partition_by)
        parts.append(f"PARTITION BY {part_sql}")
      if win.order_by:
        order_sql = ", ".join(self.render_expr(e) for e in win.order_by)
        parts.append(f"ORDER BY {order_sql}")
      over_sql = " ".join(parts)
      return f"{func_sql} OVER ({over_sql})" if over_sql else f"{func_sql} OVER ()"

    # Fallback: attempt stringification (kept permissive for DSL growth)
    return str(expr)

  # ---------------------------------------------------------
  # Concatenation
  # ---------------------------------------------------------
  def concat_expression(self, rendered_parts: Sequence[str]) -> str:
    """
    Build a dialect-specific concatenation expression from already-rendered
    parts (each element is a SQL expression string).

    Example DuckDB:  (part1 || part2 || part3)
    Example Snowflake:  CONCAT(part1, part2, part3)
    """
    return " || ".join(rendered_parts)

  # ---------------------------------------------------------
  # Hash Expression
  # ---------------------------------------------------------
  def hash_expression(self, expr: str, algo: str = "sha256") -> str:
    """
    Build a dialect-specific hashing expression around `expr`.

    Default implementation raises; dialects that support hashing should
    override this method.
    """
    raise NotImplementedError(
      f"{self.__class__.__name__} does not implement hash_expression()"
    )

  # ---------------------------------------------------------------------------
  # Literal Rendering
  # ---------------------------------------------------------------------------
  def render_literal(self, value) -> str:
    """
    ANSI-ish default literals. Dialects override where necessary.
    """
    if value is None:
      return "NULL"
    if isinstance(value, bool):
      return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, Decimal)):
      return str(value)
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
      return f"DATE '{value.isoformat()}'"
    if isinstance(value, datetime.datetime):
      # ISO without timezone normalization (dialects can override)
      return f"TIMESTAMP '{value.isoformat(sep=' ', timespec='microseconds')}'"
    # strings
    s = str(value).replace("'", "''")
    return f"'{s}'"

  # ---------------------------------------------------------------------------
  # Type Casting
  # ---------------------------------------------------------------------------
  def cast_expression(self, expr: str, target_type: str) -> str:
    """
    Wrap the given SQL expression in a dialect-specific CAST expression.
    """
    return f"CAST({expr} AS {target_type})"

  # ---------------------------------------------------------------------------
  # SELECT rendering
  # ---------------------------------------------------------------------------
  def _render_from_item(self, item: SourceTable | SubquerySource) -> str:
    if isinstance(item, SourceTable):
      schema = getattr(item, "schema_name", None)
      if schema is None:
        schema = getattr(item, "schema", None)
      table = getattr(item, "table_name", None)
      if table is None:
        table = getattr(item, "name", None)
      tbl = self.render_table_identifier(schema, table)
      if item.alias:
        return f"{tbl} AS {self.render_identifier(item.alias)}"
      return tbl
    if isinstance(item, SubquerySource):
      inner = self.render_select(item.select)
      if item.alias:
        alias = self.render_identifier(item.alias)
        return f"(\n{inner}\n) AS {alias}"
      return f"(\n{inner}\n)"

    return str(item)

  def _render_join(self, j: Join) -> str:
    jt = j.join_type.upper()
    right = self._render_from_item(j.right)
    on_sql = self.render_expr(j.on) if j.on is not None else ""
    if on_sql:
      return f"{jt} JOIN {right} ON {on_sql}"
    return f"{jt} JOIN {right}"

  def render_select(self, select: LogicalSelect) -> str:
    items_sql: list[str] = []

    # DSL compatibility: prefer select.select_list / select.from_
    select_list_obj = getattr(select, "select_list", None)
    if select_list_obj is None:
      select_list_obj = getattr(select, "items", None)
    select_list_obj = select_list_obj or []

    for it in select_list_obj:
      if isinstance(it, SelectItem):
        expr_sql = self.render_expr(it.expr)
        if it.alias:
          items_sql.append(f"{expr_sql} AS {self.render_identifier(it.alias)}")
        else:
          items_sql.append(expr_sql)
      else:
        items_sql.append(self.render_expr(it))

    select_list = ",\n  ".join(items_sql) if items_sql else "*"

    distinct = bool(getattr(select, "distinct", False))
    sql: list[str] = ["SELECT DISTINCT" if distinct else "SELECT", f"  {select_list}"]

    from_item = getattr(select, "from_", None)
    if from_item is None:
      from_item = getattr(select, "from_item", None)

    if from_item is not None:
      from_sql = self._render_from_item(from_item)

      sql.append("FROM")
      sql.append(f"  {from_sql}")

    for j in (getattr(select, "joins", None) or []):
      sql.append(self._render_join(j))

    where_expr = getattr(select, "where", None)
    if where_expr is not None:
      sql.append("WHERE")
      sql.append(f"  {self.render_expr(select.where)}")

    group_by = getattr(select, "group_by", None) or []
    if group_by:
      gb = ", ".join(self.render_expr(e) for e in group_by)
      sql.append(f"GROUP BY {gb}")

    having_expr = getattr(select, "having", None)
    if having_expr is not None:
      sql.append(f"HAVING {self.render_expr(having_expr)}")

    order_by = getattr(select, "order_by", None) or []
    if order_by:
      ob = ", ".join(self.render_expr(e) for e in order_by)
      sql.append(f"ORDER BY {ob}")

    limit_val = getattr(select, "limit", None)
    if limit_val is not None:
      sql.append(f"LIMIT {int(limit_val)}")

    offset_val = getattr(select, "offset", None)
    if offset_val is not None:
      sql.append(f"OFFSET {int(offset_val)}")

    return "\n".join(sql)

  # ---------------------------------------------------------------------------
  # Truncate table
  # ---------------------------------------------------------------------------
  def render_truncate_table(self, *, schema: str, table: str) -> str:
    """
    Default implementation: DELETE FROM (safe, widely supported).
    Dialects can override with TRUNCATE TABLE for performance.
    """
    full = self.render_table_identifier(schema, table)
    return f"DELETE FROM {full};"
    
  # ---------------------------------------------------------------------------
  # Incremental / MERGE Rendering
  # ---------------------------------------------------------------------------
  def render_create_replace_table(self, schema: str, table: str, select_sql: str) -> str:
    """
    Default: CREATE OR REPLACE TABLE <schema>.<table> AS <select>.
    Dialects that don't support this (e.g. Postgres, MSSQL) should override.
    """
    full = self.render_table_identifier(schema, table)
    return f"CREATE OR REPLACE TABLE {full} AS\n{select_sql}"

  def render_insert_into_table(
    self,
    schema_name: str,
    table_name: str,
    select_sql: str,
    *,
    target_columns: list[str] | None = None,
  ) -> str:
    """
    Default: INSERT INTO <schema>.<table> [(col, ...)] <select>.
    """
    table = self.render_table_identifier(schema_name, table_name)
    if target_columns:
      cols = ", ".join(self.render_identifier(c) for c in target_columns)
      return f"INSERT INTO {table} ({cols})\n{select_sql}"
    return f"INSERT INTO {table}\n{select_sql}"

  def render_merge_statement(
    self,
    schema: str,
    table: str,
    select_sql: str,
    unique_key_columns: list[str],
    update_columns: list[str],
  ) -> str:
    full = self.render_table_identifier(schema, table)

    on_clause = " AND ".join(
      f"t.{self.render_identifier(c)} = s.{self.render_identifier(c)}"
      for c in unique_key_columns
    )

    update_assignments = ", ".join(
      f"{self.render_identifier(col)} = s.{self.render_identifier(col)}"
      for col in update_columns
    )

    all_cols = unique_key_columns + update_columns
    col_list = ", ".join(self.render_identifier(c) for c in all_cols)
    val_list = ", ".join(f"s.{self.render_identifier(c)}" for c in all_cols)

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
    Default delete-detection implementation using DELETE + NOT EXISTS.

    join_predicates are strings like: "t.key = s.key"
    scope_filter (optional) is a full boolean expression string.
    """
    target = self.render_table_identifier(target_schema, target_table)
    stage = self.render_table_identifier(stage_schema, stage_table)

    join_sql = " AND ".join(join_predicates) if join_predicates else "1=1"

    conditions: list[str] = []
    if scope_filter:
      # scope_filter is already a boolean expression
      conditions.append(f"({scope_filter})")

    conditions.append(
      "NOT EXISTS (\n"
      "  SELECT 1\n"
      f"  FROM {stage} AS s\n"
      f"  WHERE {join_sql}\n"
      ")"
    )

    where_sql = "\n  AND ".join(conditions)

    sql = (
      f"DELETE FROM {target} AS t\n"
      f"WHERE {where_sql}"
    )

    # Always terminate for safe multi-statement execution (e.g., DELETE + MERGE).
    return sql.rstrip() + ";"

  # ---------------------------------------------------------------------------
  # Type mapping (single source of truth: canonicalize -> render physical)
  # ---------------------------------------------------------------------------
  # Dialects may extend/override via TYPE_ALIASES on the class.
  TYPE_ALIASES: dict[str, str] = {}

  # Canonical type tokens used across dialects (string constants, intentionally simple).
  # NOTE: Dialect renderers typically compare via .upper(), so returning these tokens works.
  _DEFAULT_TYPE_ALIASES: dict[str, str] = {
    # String
    "STRING": "STRING",
    "TEXT": "STRING",
    "VARCHAR": "STRING",
    "CHAR": "STRING",
    "NVARCHAR": "STRING",
    "NCHAR": "STRING",

    # Int
    "INT": "INTEGER",
    "INTEGER": "INTEGER",
    "INT32": "INTEGER",
    "SMALLINT": "INTEGER",

    # Big int
    "BIGINT": "BIGINT",
    "INT64": "BIGINT",
    "LONG": "BIGINT",

    # Decimal / numeric
    "DECIMAL": "DECIMAL",
    "NUMERIC": "DECIMAL",

    # Float
    "FLOAT": "FLOAT",
    "DOUBLE": "FLOAT",
    "FLOAT64": "FLOAT",

    # Bool
    "BOOL": "BOOLEAN",
    "BOOLEAN": "BOOLEAN",

    # Date/time
    "DATE": "DATE",
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP",
    "DATETIME": "TIMESTAMP",
    "TIMESTAMPTZ": "TIMESTAMP",

    # Binary
    "BINARY": "BINARY",
    "BYTES": "BINARY",

    # Others
    "UUID": "UUID",
    "JSON": "JSON",
  }

  def canonicalize_logical_type(self, logical_type, *, strict: bool = True) -> str:
    """
    Convert an input logical type (TargetColumn.datatype or user-specified alias)
    into a canonical type token (e.g. STRING, INTEGER, BIGINT, DECIMAL, FLOAT...).

    - Does NOT apply dialect-specific physical naming.
    - Uses a shared default alias map and allows dialect-specific extensions.
    """
    lt = (logical_type or "").strip().upper()
    alias_map = dict(self._DEFAULT_TYPE_ALIASES)
    alias_map.update(getattr(self, "TYPE_ALIASES", None) or {})

    canonical = alias_map.get(lt)
    if canonical is None:
      if strict:
        raise ValueError(f"Unsupported logical type: {logical_type!r}")
      return lt
    return canonical

  @abstractmethod
  def render_physical_type(
    self,
    *,
    canonical: str,
    max_length=None,
    precision=None,
    scale=None,
    strict: bool = True,
  ) -> str:
    """
    Render a dialect-specific physical SQL type string from a canonical token.
    Example:
      canonical=STRING, max_length=50 -> VARCHAR(50) / STRING / NVARCHAR(50)
    """
    raise NotImplementedError

  def map_logical_type(
    self,
    *,
    datatype: str,
    max_length=None,
    precision=None,
    scale=None,
    strict: bool = True,
  ) -> str:
    canonical = self.canonicalize_logical_type(datatype, strict=strict)
    return self.render_physical_type(
      canonical=canonical,
      max_length=max_length,
      precision=precision,
      scale=scale,
      strict=strict,
    )

  def columns_from_target_dataset(self, td) -> list[dict[str, object]]:
    """
    Build a simple column list from TargetDataset metadata.

    Single source of truth used by dialect DDL (CREATE TABLE / log tables etc.):
      [{"name": str, "type": str, "nullable": bool}, ...]
    """
    cols: list[dict[str, object]] = []
    # Deterministic order
    for c in td.target_columns.all().order_by("ordinal_position"):
      col_type = self.map_logical_type(
        datatype=c.datatype,
        max_length=getattr(c, "max_length", None),
        precision=getattr(c, "precision", None) or getattr(c, "decimal_precision", None),
        scale=getattr(c, "scale", None) or getattr(c, "decimal_scale", None),
        strict=True,
      )
      if not col_type:
        # Fail closed: missing type mapping should be caught by callers/planner.
        continue
      cols.append({
        "name": c.target_column_name,
        "type": col_type,
        "nullable": bool(getattr(c, "nullable", True)),
      })
    return cols

  # ---------------------------------------------------------------------------
  # DDL statements
  # ---------------------------------------------------------------------------
  def render_rename_table(self, schema: str, old_table: str, new_table: str) -> str:
    """
    Default table rename (works for DuckDB/Postgres/BigQuery-style dialects):
      ALTER TABLE <schema>.<old> RENAME TO <new>

    Note: new_table is intentionally unqualified (no schema prefix).
    """
    old_full = self.render_table_identifier(schema, old_table)
    new_name = self.render_identifier(new_table)
    return f"ALTER TABLE {old_full} RENAME TO {new_name}"

  @abstractmethod
  def render_create_schema_if_not_exists(self, schema: str) -> str:
    """
    Return DDL that creates the given schema if it does not exist.
    Must be idempotent and safe to run multiple times.
    """
    raise NotImplementedError(
      f"{self.__class__.__name__} does not implement render_create_schema_if_not_exists()"
    )
  
  def render_create_table_if_not_exists(self, td) -> str:
    """
    Default implementation: TargetDataset/TargetColumn -> column list
    and delegate to render_create_table_if_not_exists_from_columns().
    Dialects override render_create_table_if_not_exists_from_columns() if needed.
    """
    schema_name = td.target_schema.schema_name
    table_name = td.target_dataset_name

    columns: list[dict[str, object]] = []
    for c in self._iter_target_columns(td):
      max_length = getattr(c, "max_length", None)
      precision = getattr(c, "precision", None)
      if precision is None:
        precision = getattr(c, "decimal_precision", None)
      scale = getattr(c, "scale", None)
      if scale is None:
        scale = getattr(c, "decimal_scale", None)

      col_type = self.map_logical_type(
        datatype=c.datatype,
        max_length=max_length,
        precision=precision,
        scale=scale,
        strict=True,
      )
      columns.append({
        "name": c.target_column_name,
        "type": col_type,
        "nullable": bool(getattr(c, "nullable", True)),
      })

    return self.render_create_table_if_not_exists_from_columns(
      schema=schema_name,
      table=table_name,
      columns=columns,
    )

  def _iter_target_columns(self, td) -> list[Any]:
    cols_obj = getattr(td, "target_columns", None)
    if cols_obj is None:
      return []
    if hasattr(cols_obj, "all"):
      try:
        qs = cols_obj.all()
        if hasattr(qs, "order_by"):
          return list(qs.order_by("ordinal_position"))
        return list(qs)
      except Exception:
        pass
    try:
      cols = list(cols_obj)
      cols.sort(key=lambda c: getattr(c, "ordinal_position", 0) or 0)
      return cols
    except Exception:
      return []

  def render_create_table_if_not_exists_from_columns(
    self,
    *,
    schema: str,
    table: str,
    columns: list[dict[str, object]],
  ) -> str:
    """
    Render CREATE TABLE IF NOT EXISTS <schema>.<table> ( ... ) from a simple column list.
    Dialects may override if they don't support CREATE TABLE IF NOT EXISTS.
    """
    target = self.render_table_identifier(schema, table)
    col_defs: list[str] = []
    for c in columns:
      name = self.render_identifier(str(c["name"]))
      ctype = str(c["type"])
      nullable = bool(c.get("nullable", True))
      null_sql = "NULL" if nullable else "NOT NULL"
      col_defs.append(f"{name} {ctype} {null_sql}")
    cols_sql = ",\n  ".join(col_defs)
    return f"CREATE TABLE IF NOT EXISTS {target} (\n  {cols_sql}\n)"

  def render_drop_table_if_exists(self, *, schema: str, table: str, cascade: bool = False) -> str:
    """
    Drop a table if it exists.

    - Default: no CASCADE (dialects may override).
    - cascade flag is supported for engines like Postgres.
    """
    # Default ignores cascade unless overridden by a dialect.
    target = self.render_table_identifier(schema, table)
    return f"DROP TABLE IF EXISTS {target}"

  def render_add_column(self, schema: str, table: str, column: str, column_type: str | None) -> str:
    """
    Default ADD COLUMN DDL. Dialects can override if needed.
    """
    if not column_type:
      # Fail closed: planner should block if type is missing.
      return ""

    # Use dialect-safe identifier rendering.
    tbl = self.render_table_identifier(schema, table)
    col = self.render_identifier(column)
    return f"ALTER TABLE {tbl} ADD COLUMN {col} {column_type}"

  def render_rename_column(self, schema: str, table: str, old: str, new: str) -> str:
    # Default ANSI-ish
    return f"ALTER TABLE {self.render_table_identifier(schema, table)} RENAME COLUMN {self.quote_ident(old)} TO {self.quote_ident(new)}"

  def render_create_or_replace_view(
    self,
    *,
    schema: str,
    view: str,
    select_sql: str,
  ) -> str:
    target = self.render_table_identifier(schema, view)
    return f"CREATE OR REPLACE VIEW {target} AS\n{select_sql}"

  # ---------------------------------------------------------------------------
  # Logging
  # ---------------------------------------------------------------------------
  def render_insert_load_run_log(self, *, meta_schema: str, values: dict[str, object]) -> str | None:
    """
    Render an INSERT INTO meta.load_run_log (...) VALUES (...) statement.

    v0.8.0: schema is canonical via LOAD_RUN_LOG_REGISTRY and the caller passes
    a fully normalized `values` dict (same keys/order as the canonical schema).
    Dialects must only render identifiers and literals safely.
    """
    raise NotImplementedError(f"{self.__class__.__name__} does not implement render_insert_load_run_log()")
