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

import pyodbc

import datetime
from decimal import Decimal
from typing import Sequence

from .base import BaseExecutionEngine
from .duckdb import DuckDBDialect
from metadata.ingestion.types_map import (
  STRING, INTEGER, BIGINT, DECIMAL, FLOAT, BOOLEAN, DATE, TIME, TIMESTAMP, BINARY, UUID, JSON
)


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
      

class MssqlDialect(DuckDBDialect):
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

  DIALECT_NAME = "mssql"

  def get_execution_engine(self, system):
    return MssqlExecutionEngine(system)

  # ---------------------------------------------------------------------------
  # Capabilities
  # ---------------------------------------------------------------------------
  @property
  def supports_merge(self) -> bool:
    """SQL Server supports native MERGE statements."""
    return True

  @property
  def supports_delete_detection(self) -> bool:
    """Delete detection is implemented via DELETE + NOT EXISTS."""
    return True

  # ---------------------------------------------------------------------------
  # Identifier quoting
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
    Map a logical elevata datatype string to a SQL Server type string.
    """
    if not logical_type:
      return None

    raw = str(logical_type).strip()
    t = raw.lower()

    # Normalize synonyms to canonical elevata types used by _render_canonical_type_mssql.
    canonical = None
    if t in ("string", "text", "varchar", "char"):
      canonical = STRING
    elif t in ("int", "integer", "int32"):
      canonical = INTEGER
    elif t in ("bigint", "int64", "long"):
      canonical = BIGINT
    elif t in ("decimal", "numeric"):
      canonical = DECIMAL
    elif t in ("float", "double"):
      canonical = FLOAT
    elif t in ("bool", "boolean"):
      canonical = BOOLEAN
    elif t in ("date",):
      canonical = DATE
    elif t in ("time",):
      canonical = TIME
    elif t in ("datetime", "timestamp", "timestamptz"):
      canonical = TIMESTAMP
    elif t in ("uuid", "uniqueidentifier"):
      canonical = UUID
    elif t in ("json",):
      canonical = JSON
    else:
      # If it's already one of our canonical constants (STRING/INTEGER/...), keep it.
      upper = raw.upper()
      if upper in (STRING, INTEGER, BIGINT, DECIMAL, FLOAT, BOOLEAN, DATE, TIME, TIMESTAMP, BINARY, UUID, JSON):
        canonical = upper

    if canonical is None:
      if strict:
        raise ValueError(f"Unsupported logical type for MSSQL: {logical_type!r}")
      # passthrough for explicit DB types
      return logical_type

    return self._render_canonical_type_mssql(
      datatype=canonical,
      max_length=max_length,
      decimal_precision=precision,
      decimal_scale=scale,
    )

  # ---------------------------------------------------------
  # Concatenation
  # ---------------------------------------------------------
  def concat_expression(self, parts: Sequence[str]) -> str:
    """
    SQL Server uses + for string concatenation.

    We keep it simple and assume the inputs are already string-like.    
    """
    if not parts:
      return "''"
    return "(" + " + ".join(parts) + ")"

  # ---------------------------------------------------------
  # Hash expression
  # ---------------------------------------------------------
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
  # Literal rendering
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

  # ---------------------------------------------------------------------------
  # Type casting
  # ---------------------------------------------------------------------------
  def cast_expression(self, expr: str, target_type: str) -> str:
    return f"CAST({expr} AS {target_type})"

  # ---------------------------------------------------------------------------
  # Truncate table
  # ---------------------------------------------------------------------------
  def render_truncate_table(self, schema: str, table: str) -> str:
    qtbl = self.render_table_identifier
    return f"TRUNCATE TABLE {qtbl(schema, table)};"

  # ---------------------------------------------------------------------------
  # Incremental / MERGE Rendering
  # ---------------------------------------------------------------------------
  def render_create_replace_table(self, schema: str, table: str, select_sql: str) -> str:
    """
    Emulate CREATE OR REPLACE TABLE via DROP IF EXISTS + SELECT INTO.

    Note:
      OBJECT_ID('<schema>.<table>', 'U') is used to detect existing tables.
    """
    full = self.render_table_identifier(schema, table)

    return (
      f"IF OBJECT_ID('{full}', 'U') IS NOT NULL\n"
      f"  DROP TABLE {full};\n"
      f"SELECT * INTO {full}\n"
      f"FROM (\n{select_sql}\n) AS src;"
    )

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


  # ---------------------------------------------------------------------------
  # DDL statements
  # ---------------------------------------------------------------------------
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
  

  def render_create_table_if_not_exists(self, td) -> str:
    """
    Create the target dataset table based on TargetColumn metadata.
    SQL Server requires IF OBJECT_ID(...) checks (no CREATE TABLE IF NOT EXISTS).
    """
    schema_name = td.target_schema.schema_name
    table_name = td.target_dataset_name

    q = self.render_identifier
    qtbl = self.render_table_identifier

    cols = []
    for c in td.target_columns.all().order_by("ordinal_position"):
      col_name = c.target_column_name

      col_type = self._render_canonical_type_mssql(
        datatype=c.datatype,
        max_length=c.max_length,
        decimal_precision=c.decimal_precision,
        decimal_scale=c.decimal_scale,
      )

      nullable = " NULL" if c.nullable else " NOT NULL"
      cols.append(f"{q(col_name)} {col_type}{nullable}")

    cols_sql = ",\n  ".join(cols) if cols else ""
    full_name = f"{schema_name}.{table_name}"  # used in OBJECT_ID()

    return f"""
      IF OBJECT_ID(N'{full_name}', N'U') IS NULL
      BEGIN
        CREATE TABLE {qtbl(schema_name, table_name)} (
          {cols_sql}
        );
      END;
    """.strip()


  def render_create_or_replace_view(self, *, schema, view, select_sql):
    return f"""
      CREATE OR ALTER VIEW {schema}.{view} AS
      {select_sql}
      """.strip()


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
  # Logging
  # ---------------------------------------------------------------------------
  def render_create_load_run_log_if_not_exists(self, meta_schema: str) -> str:
    """
    Create meta schema and load_run_log table for SQL Server.
    """
    return f"""
      IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{meta_schema}')
      BEGIN
        EXEC('CREATE SCHEMA {meta_schema};');
      END;

      IF NOT EXISTS (
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'load_run_log'
          AND s.name = '{meta_schema}'
      )
      BEGIN
        CREATE TABLE {meta_schema}.load_run_log (
          id                 BIGINT IDENTITY(1,1) PRIMARY KEY,
          batch_run_id       NVARCHAR(36) NOT NULL,
          load_run_id        NVARCHAR(36) NOT NULL,
          target_schema      NVARCHAR(128) NOT NULL,
          target_dataset     NVARCHAR(256) NOT NULL,
          target_dataset_id  BIGINT NULL,
          target_system      NVARCHAR(128) NOT NULL,
          target_system_type NVARCHAR(64) NOT NULL,
          profile            NVARCHAR(128) NOT NULL,
          load_mode          NVARCHAR(32) NOT NULL,
          handle_deletes     BIT NOT NULL,
          historize          BIT NOT NULL,
          dialect            NVARCHAR(64) NOT NULL,
          started_at         DATETIME2 NOT NULL,
          finished_at        DATETIME2 NOT NULL,
          render_ms          FLOAT NOT NULL,
          execution_ms       FLOAT NULL,
          sql_length         BIGINT NOT NULL,
          rows_affected      BIGINT NULL,
          load_status        NVARCHAR(16) NOT NULL,
          error_message      NVARCHAR(MAX) NULL
        );
      END;
    """.strip()


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
        {lit(error_message)}
      );
    """.strip()
