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

import pyodbc

import datetime
from decimal import Decimal
from typing import Sequence, Dict, Any, Optional

from .base import BaseExecutionEngine, SqlDialect
from metadata.ingestion.types_map import (
  STRING, INTEGER, BIGINT, DECIMAL, FLOAT, BOOLEAN, DATE, TIME, TIMESTAMP, BINARY, UUID, JSON
)
from metadata.materialization.logging import LOAD_RUN_LOG_REGISTRY


class MssqlExecutionEngine(BaseExecutionEngine):
  def __init__(self, system):
    conn_str = None
    if system.security:
      conn_str = system.security.get("connection_string")

    if not conn_str:
      raise ValueError(
        f"MSSQL system '{system.short_name}' has no usable connection string in security."
      )

    self.conn_str = conn_str

  def execute(self, sql: str) -> int | None:
    conn = pyodbc.connect(self.conn_str, autocommit=False)
    try:
      cursor = conn.cursor()
      cursor.execute(sql)
      conn.commit()

      try:
        return cursor.rowcount
      except Exception:
        return None
    except Exception:
      conn.rollback()
      raise
    finally:
      conn.close()

  def execute_many(self, sql: str, params_seq) -> int | None:
    conn = pyodbc.connect(self.conn_str, autocommit=False)
    try:
      cursor = conn.cursor()
      cursor.executemany(sql, params_seq)
      conn.commit()
      try:
        return cursor.rowcount
      except Exception:
        return None
    except Exception:
      conn.rollback()
      raise
    finally:
      conn.close()

  def execute_scalar(self, sql: str):
    """
    Execute a SELECT returning a single value (first column of first row).
    Returns None if no row is returned.
    """
    conn = pyodbc.connect(self.conn_str, autocommit=False)
    try:
      cursor = conn.cursor()
      cursor.execute(sql)
      row = cursor.fetchone()
      if not row:
        return None
      # pyodbc rows are tuple-like
      return row[0]
    finally:
      conn.close()

  def fetch_all(self, sql: str) -> list[tuple]:
    """
    Execute a SELECT and return all rows as tuples.
    """
    conn = pyodbc.connect(self.conn_str, autocommit=False)
    try:
      cursor = conn.cursor()
      cursor.execute(sql)
      rows = cursor.fetchall()
      return [tuple(r) for r in (rows or [])]
    finally:
      conn.close()      


class MssqlDialect(SqlDialect):
  """
  SQL Server / T-SQL dialect.

  We subclass DuckDBDialect to reuse:
    - expression rendering (ColumnRef, WindowFunction, RawSql, ...)
    - LogicalSelect rendering
    - MERGE / delete detection skeletons

  MSSQL-specific behaviour:
    - Different type mapping
    - String concatenation with +
    - HASHBYTES for hashing
    - Booleans as 1 / 0
    - DATE / DATETIME2 literals via CAST(...)
    - CREATE OR REPLACE emulated via DROP + SELECT INTO
  """

  # ---------------------------------------------------------------------------
  # 1. Class meta / capabilities
  # ---------------------------------------------------------------------------
  DIALECT_NAME = "mssql"

  @property
  def supports_merge(self) -> bool:
    """SQL Server supports native MERGE statements."""
    return True
  
  @property
  def supports_alter_column_type(self) -> bool:
    return True

  @property
  def supports_delete_detection(self) -> bool:
    """Delete detection is implemented via DELETE + NOT EXISTS."""
    return True

  def get_execution_engine(self, system):
    return MssqlExecutionEngine(system)

  # ---------------------------------------------------------------------------
  # 2. Identifier & quoting
  # ---------------------------------------------------------------------------
  def quote_ident(self, name: str) -> str:
    """
    Quote identifiers with double quotes.

    Note:
      SQL Server supports QUOTED_IDENTIFIER and accepts "name" as identifier
      quoting style (similar to Postgres / DuckDB).
    """
    escaped = name.replace('"', '""')
    return f'"{escaped}"'

  # ---------------------------------------------------------------------------
  # 3. Types
  # ---------------------------------------------------------------------------
  def render_physical_type(
    self,
    *,
    canonical: str,
    max_length=None,
    precision=None,
    scale=None,
    strict: bool = True,
  ) -> str:
    return self._render_canonical_type_mssql(
      datatype=canonical,
      max_length=max_length,
      decimal_precision=precision,
      decimal_scale=scale,
    )

  def _render_canonical_type_mssql(
    self,
    *,
    datatype: str,
    max_length=None,
    decimal_precision=None,
    decimal_scale=None,
  ) -> str:
    """
    Map elevata canonical types (TargetColumn.datatype) to SQL Server SQL types.
    """
    t = (datatype or "").upper()

    if t == STRING:
      if max_length:
        return f"NVARCHAR({int(max_length)})"
      return "NVARCHAR(MAX)"

    if t == INTEGER:
      return "INT"
    if t == BIGINT:
      return "BIGINT"

    if t == DECIMAL:
      if decimal_precision and decimal_scale is not None:
        return f"DECIMAL({int(decimal_precision)},{int(decimal_scale)})"
      if decimal_precision:
        return f"DECIMAL({int(decimal_precision)})"
      return "DECIMAL(38,10)"

    if t == FLOAT:
      return "FLOAT"

    if t == BOOLEAN:
      return "BIT"

    if t == DATE:
      return "DATE"
    if t == TIME:
      return "TIME"
    if t == TIMESTAMP:
      return "DATETIME2"

    if t == BINARY:
      return "VARBINARY(MAX)"

    if t == UUID:
      return "UNIQUEIDENTIFIER"

    if t == JSON:
      return "NVARCHAR(MAX)"

    raise ValueError(
      f"Unsupported canonical datatype for MSSQL: {datatype!r}. "
      "Please fix ingestion type mapping or extend the dialect mapping."
    )

  # ---------------------------------------------------------------------------
  # 4. DDL helpers
  # ---------------------------------------------------------------------------
  def render_create_schema_if_not_exists(self, schema: str) -> str:
    """
    SQL Server uses IF NOT EXISTS on sys.schemas.
    """
    return f"""
      IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}')
      BEGIN
        EXEC('CREATE SCHEMA {schema}');
      END;
    """.strip()

  def render_create_table_if_not_exists_from_columns(
    self,
    *,
    schema: str,
    table: str,
    columns: list[dict[str, object]],
  ) -> str:
    q = self.render_identifier
    qtbl = self.render_table_identifier
    col_defs: list[str] = []
    for c in columns:
      name = q(str(c["name"]))
      ctype = str(c["type"])
      nullable = bool(c.get("nullable", True))
      null_sql = "NULL" if nullable else "NOT NULL"
      col_defs.append(f"{name} {ctype} {null_sql}")
    cols_sql = ",\n  ".join(col_defs)
    full_name = f"{schema}.{table}"
    return f"""
      IF OBJECT_ID(N'{full_name}', N'U') IS NULL
      BEGIN
        CREATE TABLE {qtbl(schema=schema, name=table)} (
          {cols_sql}
        );
      END;
    """.strip()


  def render_create_or_replace_view(
    self,
    *,
    schema: str,
    view: str,
    select_sql: str,
  ) -> str:
    target = self.render_table_identifier(schema, view)
    return f"CREATE OR ALTER VIEW {target} AS\n{select_sql}"


  def render_add_column(self, schema: str, table: str, column: str, column_type: str | None) -> str:
    """
    SQL Server syntax: ALTER TABLE <tbl> ADD <col> <type>
    (no COLUMN keyword).
    """
    if not column_type:
      return ""
    tbl = self.render_table_identifier(schema, table)
    col = self.render_identifier(column)
    return f"ALTER TABLE {tbl} ADD {col} {column_type}"
  

  def render_alter_column_type(self, *, schema: str, table: str, column: str, new_type: str) -> str:
    # SQL Server: ALTER TABLE <tbl> ALTER COLUMN <col> <type>
    tbl = self.render_table_identifier(schema, table)
    col = self.render_identifier(column)
    return f"ALTER TABLE {tbl} ALTER COLUMN {col} {new_type}"


  def render_truncate_table(self, schema: str, table: str) -> str:
    qtbl = self.render_table_identifier
    return f"TRUNCATE TABLE {qtbl(schema, table)};"

  def render_rename_table(self, schema: str, old_table: str, new_table: str) -> str:
    old_qualified = f"{self.render_identifier(schema)}.{self.render_identifier(old_table)}"
    new_name = self.render_identifier(new_table)
    # sp_rename wants quoted identifiers inside the string; QUOTED_IDENTIFIER should be ON (typisch).
    return f"EXEC sp_rename N'{old_qualified}', N'{new_name}'"
  
  def render_rename_column(self, schema: str, table: str, old: str, new: str) -> str:
    obj = (
      f"{self.render_identifier(schema)}."
      f"{self.render_identifier(table)}."
      f"{self.render_identifier(old)}"
    )
    new_name = self.render_identifier(new)
    return f"EXEC sp_rename N'{obj}', N'{new_name}', 'COLUMN'"

  # ---------------------------------------------------------------------------
  # 5. DML / load SQL primitives
  # ---------------------------------------------------------------------------
 
  # ---------------------------------------------------------------------------
  # Historization (SCD Type 2) rendering overrides
  #
  # MSSQL / T-SQL requires UPDATE <alias> ... FROM <table> <alias> ...
  # and does not support "UPDATE <table> AS <alias>".
  # ---------------------------------------------------------------------------
  def render_hist_changed_update_sql(
    self,
    *,
    schema_name: str,
    hist_table: str,
    rawcore_table: str,
  ) -> str:
    hist_tbl = self.render_table_identifier(schema_name, hist_table)
    rc_tbl = self.render_table_identifier(schema_name, rawcore_table)

    sk_name = self.render_identifier(f"{rawcore_table}_key")
    row_hash = self.render_identifier("row_hash")

    return (
      "UPDATE h\n"
      "SET\n"
      "  version_ended_at = {{ load_timestamp }},\n"
      "  version_state    = 'changed',\n"
      "  load_run_id      = {{ load_run_id }}\n"
      f"FROM {hist_tbl} h\n"
      "WHERE h.version_ended_at IS NULL\n"
      "  AND EXISTS (\n"
      "    SELECT 1\n"
      f"    FROM {rc_tbl} r\n"
      f"    WHERE r.{sk_name} = h.{sk_name}\n"
      f"      AND r.{row_hash} <> h.{row_hash}\n"
      "  );"
    )

  def render_hist_delete_sql(
    self,
    *,
    schema_name: str,
    hist_table: str,
    rawcore_table: str,
  ) -> str:
    hist_tbl = self.render_table_identifier(schema_name, hist_table)
    rc_tbl = self.render_table_identifier(schema_name, rawcore_table)

    sk_name = self.render_identifier(f"{rawcore_table}_key")

    return (
      "UPDATE h\n"
      "SET\n"
      "  version_ended_at = {{ load_timestamp }},\n"
      "  version_state    = 'deleted',\n"
      "  load_run_id      = {{ load_run_id }}\n"
      f"FROM {hist_tbl} h\n"
      "WHERE h.version_ended_at IS NULL\n"
      "  AND NOT EXISTS (\n"
      "    SELECT 1\n"
      f"    FROM {rc_tbl} r\n"
      f"    WHERE r.{sk_name} = h.{sk_name}\n"
      "  );"
    )


  def render_merge_statement(
    self,
    *,
    target_fqn: str,
    source_select_sql: str,
    key_columns: list[str],
    update_columns: list[str],
    insert_columns: list[str],
    target_alias: str = "t",
    source_alias: str = "s",
  ) -> str:
    """
    Render an MS SQL Server MERGE statement.

    SQL Server supports a native MERGE statement. We render a standard T-SQL MERGE
    using a subquery source:

      MERGE <target> AS t
      USING (<source_select_sql>) AS s
      ON t.k1 = s.k1 AND ...
      WHEN MATCHED THEN UPDATE SET t.col = s.col, ...
      WHEN NOT MATCHED THEN INSERT (c1, c2, ...) VALUES (s.c1, s.c2, ...);

    Notes:
      - T-SQL requires a statement terminator ';' for MERGE.
      - If update_columns is empty (keys only), we omit the WHEN MATCHED clause.
    """
    q = self.render_identifier
    target = str(target_fqn).strip()

    keys = [c for c in (key_columns or []) if c]
    if not keys:
      raise ValueError("MssqlDialect.render_merge_statement requires non-empty key_columns")

    insert_cols = [c for c in (insert_columns or []) if c]
    if not insert_cols:
      seen = set()
      insert_cols = []
      for c in keys + list(update_columns or []):
        if c and c not in seen:
          seen.add(c)
          insert_cols.append(c)

    updates = [c for c in (update_columns or []) if c and c not in set(keys)]

    on_pred = " AND ".join(
      [f"{q(target_alias)}.{q(k)} = {q(source_alias)}.{q(k)}" for k in keys]
    )

    src = f"(\n{source_select_sql.strip()}\n) AS {q(source_alias)}"

    parts: list[str] = []
    parts.append(
      f"MERGE {target} AS {q(target_alias)}\n"
      f"USING {src}\n"
      f"ON {on_pred}"
    )

    if updates:
      update_assignments = ", ".join(
        [f"{q(target_alias)}.{q(c)} = {q(source_alias)}.{q(c)}" for c in updates]
      )
      parts.append(f"WHEN MATCHED THEN UPDATE SET {update_assignments}")

    insert_cols_sql = ", ".join([q(c) for c in insert_cols])
    insert_vals_sql = ", ".join([f"{q(source_alias)}.{q(c)}" for c in insert_cols])
    parts.append(
      f"WHEN NOT MATCHED THEN INSERT ({insert_cols_sql}) VALUES ({insert_vals_sql});"
    )

    return "\n".join(parts).strip()


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
    SQL Server delete detection.

    SQL Server does not allow:
      DELETE FROM tbl AS t ...
    Correct form:
      DELETE t
      FROM tbl AS t
      WHERE ...
    """
    q = self.render_identifier

    target_qualified = f'{q(target_schema)}.{q(target_table)}'
    stage_qualified = f'{q(stage_schema)}.{q(stage_table)}'

    join_sql = " AND ".join(join_predicates)

    conditions = []
    if scope_filter:
      conditions.append(scope_filter)

    conditions.append(
      f"NOT EXISTS (\n"
      f"    SELECT 1\n"
      f"    FROM {stage_qualified} AS s\n"
      f"    WHERE {join_sql}\n"
      f")"
    )

    where_sql = "\n  AND ".join(conditions)

    return (
      f"DELETE t\n"
      f"FROM {target_qualified} AS t\n"
      f"WHERE {where_sql};"
    )

  LOAD_RUN_LOG_TYPE_MAP = {
    "string": "NVARCHAR(255)",
    "bool": "BIT",
    "int": "INT",
    "timestamp": "DATETIME2",
  }

  def map_load_run_log_type(self, col_name: str, canonical_type: str) -> str | None:
    if col_name == "error_message":
      return "NVARCHAR(2000)"
    if col_name == "snapshot_json":
      return "NVARCHAR(MAX)"
    return self.LOAD_RUN_LOG_TYPE_MAP.get(canonical_type)

  def render_insert_load_run_log(self, *, meta_schema: str, values: dict[str, object]) -> str:
    qtbl = self.render_table_identifier
    lit = self.render_literal

    table = qtbl(meta_schema, "load_run_log")

    # Canonical registry order; ignore unknown keys, NULL for missing.
    cols = list(LOAD_RUN_LOG_REGISTRY.keys())    

    col_sql = ",\n        ".join(cols)
    val_sql = ",\n        ".join([lit(values.get(c)) for c in cols])

    return f"""
      INSERT INTO {table} (
        {col_sql}
      )
      VALUES (
        {val_sql}
      );
    """.strip()
  
  def _literal_for_meta_insert(self, *, table: str, column: str, value: object) -> str:
    """
    SQL Server does not support TRUE/FALSE literals. Use 1/0 for BIT.
    Reuse MSSQL literal rendering to keep behavior consistent with load_run_log inserts.
    """
    # If the value is a bool, force BIT-compatible literal.
    if isinstance(value, bool):
      return "1" if value else "0"
    return self.render_literal(value)

  # ---------------------------------------------------------------------------
  # 6. Expression / Select renderer
  # ---------------------------------------------------------------------------
  def render_literal(self, value):
    if value is None:
      return "NULL"

    if isinstance(value, bool):
      # SQL Server has BIT, but no TRUE/FALSE literals
      return "1" if value else "0"

    if isinstance(value, (int, float, Decimal)):
      return str(value)

    if isinstance(value, str):
      escaped = value.replace("'", "''")
      return f"'{escaped}'"

    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
      iso = value.isoformat()
      return f"CAST('{iso}' AS DATE)"

    if isinstance(value, datetime.datetime):
      # Strip microseconds for a cleaner literal
      dt = value.replace(microsecond=0)
      iso = dt.isoformat(sep=" ")
      return f"CAST('{iso}' AS DATETIME2)"

    raise TypeError(f"Unsupported literal type for MssqlDialect: {type(value)}")


  def render_string_agg(self, args) -> str:
    if len(args) < 2:
      raise ValueError("STRING_AGG requires at least 2 arguments: value, delimiter.")
    value_sql = self.render_expr(args[0])
    delim_sql = self.render_expr(args[1])
    if len(args) >= 3 and args[2] is not None:
      order_by_sql = self.render_expr(args[2])
      return f"STRING_AGG({value_sql}, {delim_sql}) WITHIN GROUP (ORDER BY {order_by_sql})"
    return f"STRING_AGG({value_sql}, {delim_sql})"


  def cast_expression(self, expr: str, target_type: str) -> str:
    return f"CAST({expr} AS {target_type})"

  def concat_expression(self, parts: Sequence[str]) -> str:
    """
    SQL Server uses + for string concatenation.

    We keep it simple and assume the inputs are already string-like.    
    """
    if not parts:
      return "''"
    return "(" + " + ".join(parts) + ")"

  def hash_expression(self, expr: str, algo: str = "sha256") -> str:
    """
    SQL Server: map HASH256 to HASHBYTES('SHA2_256', ...),
    and convert to a hex string.
    """
    algo_lower = algo.lower()
    if algo_lower in ("sha256", "hash256"):
      return f"CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', {expr}), 2)"
    # Fallback: still SHA2_256
    return f"CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', {expr}), 2)"

  # ---------------------------------------------------------------------------
  # 7. Introspection hooks
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
    # Use SQLAlchemy-based default introspection for MSSQL.
    return SqlDialect.introspect_table(
      self,
      schema_name=schema_name,
      table_name=table_name,
      introspection_engine=introspection_engine,
      exec_engine=exec_engine,
      debug_plan=debug_plan,
    )
