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
import os
import duckdb

from .base import SqlDialect, BaseExecutionEngine
from ..expr import Expr, Cast, Concat, Coalesce, ColumnRef, FuncCall, Literal, RawSql, WindowFunction, WindowSpec
from ..logical_plan import LogicalSelect, SelectItem, SourceTable, Join, SubquerySource
from metadata.ingestion.types_map import (
  STRING, INTEGER, BIGINT, DECIMAL, FLOAT, BOOLEAN, DATE, TIME, TIMESTAMP, BINARY, UUID, JSON
)


class DuckDbExecutionEngine(BaseExecutionEngine):
  """
  Execution engine for DuckDB based on the target system's security configuration.

  Expected patterns for system.security:

  - security is a dict:
      {"connection_string": "duckdb:///./core/dwh.duckdb"}
    or
      {"dsn": "duckdb:///./core/dwh.duckdb"}
    or
      {"url": "duckdb:///./core/dwh.duckdb"}

  - security is a plain string:
      "duckdb:///./core/dwh.duckdb"

  The engine extracts the database path and connects via duckdb.connect(path).
  """

  def __init__(self, system):
    self.system = system
    security = getattr(system, "security", None)

    conn_str = None

    # Case 1: security is a dict with a connection string
    if isinstance(security, dict):
      conn_str = (
        security.get("connection_string")
        or security.get("dsn")
        or security.get("url")
        or security.get("database")
      )

    # Case 2: security is directly a string
    elif isinstance(security, str):
      conn_str = security

    if not conn_str:
      raise ValueError(
        f"DuckDB system '{system.short_name}' has no usable connection string "
        f"in security. Expected security['connection_string'] or a string value."
      )

    # Normalize DuckDB-style connection string:
    #   duckdb:///./core/dwh.duckdb  -> ./core/dwh.duckdb
    #   duckdb:///:memory:           -> :memory:
    # Otherwise treat the value as direct database path.
    if conn_str.startswith("duckdb:///"):
      db_path = conn_str[len("duckdb:///"):]
    else:
      db_path = conn_str

    if not db_path:
      raise ValueError(
        f"No database path could be derived from DuckDB connection string: {conn_str!r}"
      )

    self._database = db_path
    self._conn = None


  def _get_conn(self):
    # Reuse one connection per engine instance to avoid DuckDB "different configuration"
    # errors when multiple connects happen against the same database file in one run.
    if self._conn is None:
      self._conn = duckdb.connect(self._database)
    return self._conn

  def close(self) -> None:
    # Allow callers to close the underlying connection deterministically.
    if self._conn is not None:
      try:
        self._conn.close()
      finally:
        self._conn = None

  def execute(self, sql: str) -> int | None:
    """
    Execute SQL against DuckDB. Supports multi-statement SQL.
    Returns the rowcount of the final statement where applicable.
    """
    if not sql:
      return 0

    conn = self._get_conn()
    cursor = conn.execute(sql)
    rowcount = None
    try:
      rowcount = cursor.rowcount
    except Exception:
      # Some statements don't have a meaningful rowcount
      rowcount = None

    try:
      conn.commit()
    except Exception:
      # Some DuckDB statements don't require/allow commit; ignore safely.
      pass

    return rowcount

  def execute_many(self, sql: str, params_seq) -> int | None:
    """
    Bulk execute parameterized statements in DuckDB.
    """
    con = self._get_conn()
    con.executemany(sql, params_seq)
    try:
      con.commit()
    except Exception:
      pass
    # DuckDB doesn't always provide rowcount reliably; return None is OK
    return None

  def fetch_all(self, sql: str, params=None):
    conn = duckdb.connect(self._database)
    try:
      if params:
        return conn.execute(sql, params).fetchall()
      return conn.execute(sql).fetchall()
    finally:
      conn.close()

  def execute_scalar(self, sql: str, params=None):
    rows = self.fetch_all(sql, params)
    if not rows:
      return None
    return rows[0][0]


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

  def get_execution_engine(self, system):
    return DuckDbExecutionEngine(system)

  # ---------------------------------------------------------------------------
  # Capabilities
  # ---------------------------------------------------------------------------
  @property
  def supports_merge(self) -> bool:
    """DuckDB supports a native MERGE statement."""
    return True

  @property
  def supports_delete_detection(self) -> bool:
    """DuckDB supports delete detection via DELETE + NOT EXISTS."""
    return True

  # ---------------------------------------------------------------------------
  # Identifier quoting
  # ---------------------------------------------------------------------------
  def quote_ident(self, name: str) -> str:
    """
    Quote an identifier using DuckDB's double-quote style.
    Internal double quotes are escaped by doubling them.
    """
    escaped = name.replace('"', '""')
    return f'"{escaped}"'

  # ---------------------------------------------------------------------------
  # Logical type mapping
  # ---------------------------------------------------------------------------
  def map_logical_type(
    self,
    logical_type,          # type: str
    max_length=None,       # type: int | None
    precision=None,        # type: int | None
    scale=None,            # type: int | None
    strict=True,
  ):
    """
    Map a logical elevata datatype string to a DuckDB type string.

    logical_type can be generic ("string", "int", "decimal")
    or already a concrete DB type ("VARCHAR(100)", "DECIMAL(18,2)").
    IMPORTANT: This must match the DDL mapping used in render_create_table_if_not_exists.
    Planner "desired" types come from here; DDL uses _render_canonical_type_duckdb.
    """
    if not logical_type:
      return None

    lt = str(logical_type).strip().upper()

    # Normalize common aliases -> canonical elevata types
    alias_to_canonical = {
      "TEXT": STRING,
      "VARCHAR": STRING,
      "CHAR": STRING,
      "STRING": STRING,

      "INT": INTEGER,
      "INTEGER": INTEGER,
      "INT32": INTEGER,

      "BIGINT": BIGINT,
      "INT64": BIGINT,
      "LONG": BIGINT,

      "DECIMAL": DECIMAL,
      "NUMERIC": DECIMAL,

      "FLOAT": FLOAT,
      "DOUBLE": FLOAT,

      "BOOL": BOOLEAN,
      "BOOLEAN": BOOLEAN,

      "DATE": DATE,
      "TIME": TIME,
      "TIMESTAMP": TIMESTAMP,
      "DATETIME": TIMESTAMP,

      "BINARY": BINARY,
      "BYTES": BINARY,

      "UUID": UUID,
      "JSON": JSON,
    }

    canonical = alias_to_canonical.get(lt)
    if canonical is None:
      if strict:
        raise ValueError(f"Unsupported logical type for DuckDB: {logical_type!r}")
      return lt

    return self._render_canonical_type_duckdb(
      datatype=canonical,
      max_length=max_length,
      decimal_precision=precision,
      decimal_scale=scale,
    )


  # ---------------------------------------------------------------------------
  # Expression rendering
  # ---------------------------------------------------------------------------
  def render_expr(self, expr: Expr) -> str:
    if isinstance(expr, ColumnRef):
      if expr.table_alias:
        return f"{expr.table_alias}.{self.render_identifier(expr.column_name)}"
      return self.render_identifier(expr.column_name)

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
  
  # ---------------------------------------------------------
  # Concatenation
  # ---------------------------------------------------------
  def concat_expression(self, parts):
    # parts are already rendered SQL expressions
    if not parts:
      return "''"
    return "(" + " || ".join(parts) + ")"

  # ---------------------------------------------------------
  # Hash expression
  # ---------------------------------------------------------
  def hash_expression(self, expr: str, algo: str = "sha256") -> str:
    algo_lower = algo.lower()
    if algo_lower in ("sha256", "hash256"):
      return f"SHA256({expr})"
    # fallback: still SHA256 for unknown algos for now
    return f"SHA256({expr})"
  
  # ---------------------------------------------------------------------------
  # Literal rendering
  # ---------------------------------------------------------------------------
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

  # ---------------------------------------------------------------------------
  # Type casting
  # ---------------------------------------------------------------------------
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
  # Truncate table
  # ---------------------------------------------------------------------------
  def render_truncate_table(self, schema: str, table: str) -> str:
    qtbl = self.render_table_identifier
    return f"DELETE FROM {qtbl(schema, table)};"

  # ---------------------------------------------------------------------------
  # Incremental / MERGE Rendering
  # ---------------------------------------------------------------------------
  def render_create_replace_table(self, schema: str, table: str, select_sql: str) -> str:
    """
    CREATE OR REPLACE TABLE schema.table AS <select>
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

    full = self.render_table_identifier(schema, table)

    # Build ON condition (t.pk = s.pk AND ...)
    on_clause = " AND ".join(
      f"t.{self.render_identifier(c)} = s.{self.render_identifier(c)}"
      for c in unique_key_columns
    )

    # Build UPDATE clause
    update_assignments = ", ".join(
      f"{self.render_identifier(col)} = s.{self.render_identifier(col)}"
      for col in update_columns
    )

    # INSERT column lists
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
    DuckDB implementation of delete detection using DELETE + NOT EXISTS.
    """
    q = self.render_identifier

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

  # ---------------------------------------------------------------------------
  # DDL statements
  # ---------------------------------------------------------------------------
  def render_create_schema_if_not_exists(self, schema: str) -> str:
    """
    DuckDB supports CREATE SCHEMA IF NOT EXISTS.
    """
    q = self.render_identifier
    return f"CREATE SCHEMA IF NOT EXISTS {q(schema)};"
  

  def render_create_table_if_not_exists(self, td) -> str:
    """
    Create the target dataset table based on TargetColumn metadata.
    DuckDB supports CREATE TABLE IF NOT EXISTS.
    """
    schema_name = td.target_schema.schema_name
    table_name = td.target_dataset_name

    q = self.render_identifier
    qtbl = self.render_table_identifier

    # Ensure columns are rendered in deterministic order
    cols = []
    for c in td.target_columns.all().order_by("ordinal_position"):
      col_name = c.target_column_name
      
      col_type = self._render_canonical_type_duckdb(
        datatype=c.datatype,
        max_length=c.max_length,
        decimal_precision=c.decimal_precision,
        decimal_scale=c.decimal_scale,
      )

      nullable = "" if c.nullable else " NOT NULL"
      cols.append(f"{q(col_name)} {col_type}{nullable}")

    cols_sql = ",\n  ".join(cols) if cols else ""

    return f"""
      CREATE TABLE IF NOT EXISTS {qtbl(schema_name, table_name)} (
        {cols_sql}
      );
    """.strip()

  
  def render_create_or_replace_view(self, *, schema, view, select_sql):
    return f"""
      CREATE OR REPLACE VIEW {schema}.{view} AS
      {select_sql}
      """.strip()


  def render_add_column(self, schema: str, table: str, column_name: str, column_type: str) -> str:
    # ANSI-ish default: DuckDB supports this form.
    target = self.render_table_identifier(schema, table)
    col = self.render_identifier(column_name)
    return f"ALTER TABLE {target} ADD COLUMN {col} {column_type}"


  def render_drop_table_if_exists(self, schema: str, table: str) -> str:
    target = self.render_table_identifier(schema, table)
    return f"DROP TABLE IF EXISTS {target}"


  def _render_canonical_type_duckdb(
    self,
    *,
    datatype: str,
    max_length=None,
    decimal_precision=None,
    decimal_scale=None,
  ) -> str:
    t = (datatype or "").upper()

    if t == STRING:
      if max_length:
        return f"VARCHAR({int(max_length)})"
      return "VARCHAR"

    if t == INTEGER:
      return "INTEGER"
    if t == BIGINT:
      return "BIGINT"

    if t == DECIMAL:
      if decimal_precision and decimal_scale is not None:
        return f"DECIMAL({int(decimal_precision)},{int(decimal_scale)})"
      if decimal_precision:
        return f"DECIMAL({int(decimal_precision)})"
      return "DECIMAL"

    if t == FLOAT:
      return "DOUBLE"

    if t == BOOLEAN:
      return "BOOLEAN"

    if t == DATE:
      return "DATE"
    if t == TIME:
      return "TIME"
    if t == TIMESTAMP:
      return "TIMESTAMP"

    if t == BINARY:
      return "BLOB"

    if t == UUID:
      return "UUID"

    if t == JSON:
      return "JSON"

    raise ValueError(
      f"Unsupported canonical datatype for DuckDB: {datatype!r}. "
      "Please fix ingestion type mapping or extend the dialect mapping."
    )

  # ---------------------------------------------------------------------------
  # Logging
  # ---------------------------------------------------------------------------
  def render_create_load_run_log_if_not_exists(self, meta_schema: str) -> str:
    """
    Create meta schema and load_run_log table if they do not exist.
    """
    q = self.render_identifier
    full = f"{q(meta_schema)}.{q('load_run_log')}"

    return """
      CREATE SCHEMA IF NOT EXISTS {meta_schema};

      CREATE TABLE IF NOT EXISTS {full} (
        id                 BIGINT,
        batch_run_id       VARCHAR(36) NOT NULL,
        load_run_id        VARCHAR(36) NOT NULL,
        target_schema      VARCHAR(128) NOT NULL,
        target_dataset     VARCHAR(256) NOT NULL,
        target_dataset_id  BIGINT NULL,
        target_system      VARCHAR(128) NOT NULL,
        target_system_type VARCHAR(64) NOT NULL,
        profile            VARCHAR(128) NOT NULL,
        load_mode          VARCHAR(32) NOT NULL,
        handle_deletes     BOOLEAN NOT NULL,
        historize          BOOLEAN NOT NULL,
        dialect            VARCHAR(64) NOT NULL,
        started_at         TIMESTAMP NOT NULL,
        finished_at        TIMESTAMP NOT NULL,
        render_ms          DOUBLE PRECISION NOT NULL,
        execution_ms       DOUBLE PRECISION NULL,
        sql_length         BIGINT NOT NULL,
        rows_affected      BIGINT NULL,
        load_status        VARCHAR(16) NOT NULL,
        error_message      TEXT NULL
      );
    """.strip().format(
      meta_schema=q(meta_schema),
      full=full,
    )


  def render_insert_load_run_log(
    self,
    meta_schema: str,
    batch_run_id: str,
    load_run_id: str,
    summary: dict[str, object],
    profile,
    system,
    started_at,
    finished_at,
    render_ms: float,
    execution_ms: float | None,
    sql_length: int,
    rows_affected: int | None,
    load_status: str,
    error_message: str | None,
  ) -> str:
    qtbl = self.render_table_identifier
    lit = self.render_literal

    table = qtbl(meta_schema, "load_run_log")

    return f"""
      INSERT INTO {table} (
        batch_run_id,
        load_run_id,
        target_schema,
        target_dataset,
        target_dataset_id,
        target_system,
        target_system_type,
        profile,
        load_mode,
        handle_deletes,
        historize,
        dialect,
        started_at,
        finished_at,
        render_ms,
        execution_ms,
        sql_length,
        rows_affected,
        load_status,
        error_message
      )
      VALUES (
        {lit(batch_run_id)},
        {lit(load_run_id)},
        {lit(summary.get("schema"))},
        {lit(summary.get("dataset"))},
        {lit(summary.get("target_dataset_id"))},
        {lit(system.short_name)},
        {lit(system.type)},
        {lit(profile.name)},
        {lit(summary.get("mode"))},
        {lit(summary.get("handle_deletes"))},
        {lit(summary.get("historize"))},
        {lit(self.DIALECT_NAME)},
        {lit(started_at)},
        {lit(finished_at)},
        {lit(render_ms)},
        {lit(execution_ms)},
        {lit(sql_length)},
        {lit(rows_affected)},
        {lit(load_status)},
        {lit(error_message)},
      );
    """.strip()
