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

import psycopg2

from datetime import date, datetime
from decimal import Decimal
from typing import Sequence

from .base import BaseExecutionEngine
from .duckdb import DuckDBDialect
from metadata.ingestion.types_map import (
  STRING, INTEGER, BIGINT, DECIMAL, FLOAT, BOOLEAN, DATE, TIME, TIMESTAMP, BINARY, UUID, JSON
)


class PostgresExecutionEngine(BaseExecutionEngine):
  def __init__(self, system):
    conn_str = None
    if system.security:
      conn_str = system.security.get("connection_string")

    if not conn_str:
      raise ValueError(
        f"Postgres system '{system.short_name}' has no usable connection string in security."
      )

    self.conn_str = conn_str

  def execute(self, sql: str) -> int | None:
    with psycopg2.connect(self.conn_str) as conn:
      with conn.cursor() as cur:
        cur.execute(sql)

        # rowcount may be -1 depending on statement type
        try:
          return cur.rowcount
        except Exception:
          return None


class PostgresDialect(DuckDBDialect):
  """
  SQL dialect for PostgreSQL.

  Compatible with elevata LogicalPlan:
  - SubquerySource
  - WindowFunction
  - RawSql templates
  """

  DIALECT_NAME = "postgres"

  def get_execution_engine(self, system):
    return PostgresExecutionEngine(system)

  # ---------------------------------------------------------------------------
  # Capabilities
  # ---------------------------------------------------------------------------
  @property
  def supports_merge(self) -> bool:
    """PostgreSQL supports merge via INSERT ... ON CONFLICT."""
    return True

  @property
  def supports_delete_detection(self) -> bool:
    """Delete detection is implemented via generic DELETE ... NOT EXISTS patterns."""
    return True

  # ---------------------------------------------------------
  # Identifier quoting
  # ---------------------------------------------------------
  def quote_ident(self, ident: str) -> str:
    return f"\"{ident}\""

  # ---------------------------------------------------------
  # Logical type mapping
  # ---------------------------------------------------------
  def map_logical_type(
    self,
    logical_type,          # type: str
    max_length=None,       # type: int | None
    precision=None,        # type: int | None
    scale=None,            # type: int | None
  ):
    lt = logical_type.upper()

    if lt in ("STRING", "TEXT", "VARCHAR"):
      return "TEXT"
    if lt in ("INT", "INTEGER"):
      return "INTEGER"
    if lt in ("BIGINT", "LONG"):
      return "BIGINT"
    if lt in ("DECIMAL", "NUMERIC"):
      return "NUMERIC"
    if lt == "DATE":
      return "DATE"
    if lt == "DATETIME":
      return "TIMESTAMPTZ"
    if lt == "BOOLEAN":
      return "BOOLEAN"

    # fallback, unmodified
    return lt

  # ---------------------------------------------------------
  # Concatenation
  # ---------------------------------------------------------
  def concat_expression(self, parts: Sequence[str]) -> str:
    """
    PostgreSQL string concatenation uses || as well, so we can mirror DuckDB.
    """
    if not parts:
      return "''"
    return "(" + " || ".join(parts) + ")"

  # ---------------------------------------------------------
  # Hash expression
  # ---------------------------------------------------------
  def hash_expression(self, expr: str, algo: str = "sha256") -> str:
    """
    Map the logical HASH256 function to the concrete Postgres SQL implementation.
    Needs the extension pgcrypto in the database.
    """
    algo_lower = algo.lower()
    if algo_lower in ("sha256", "hash256"):
      return f"encode(digest(convert_to(({expr})::text, 'UTF8'), 'sha256'), 'hex')"
    # Fallback still sha256
    return f"encode(digest(convert_to(({expr})::text, 'UTF8'), 'sha256'), 'hex')"


  # ---------------------------------------------------------
  # Literal rendering
  # ---------------------------------------------------------
  def render_literal(self, value):
    if value is None:
      return "NULL"

    if isinstance(value, bool):
      return "TRUE" if value else "FALSE"

    if isinstance(value, int):
      return str(value)

    if isinstance(value, float):
      return repr(value)

    if isinstance(value, Decimal):
      return str(value)

    if isinstance(value, date) and not isinstance(value, datetime):
      return f"DATE '{value.isoformat()}'"

    if isinstance(value, datetime):
      ts = value.isoformat(sep=" ", timespec="seconds")
      return f"TIMESTAMPTZ '{ts}'"

    # treat everything else as string
    s = str(value).replace("'", "''")
    return f"'{s}'"

  # ---------------------------------------------------------------------------
  # Truncate table
  # ---------------------------------------------------------------------------
  def render_truncate_table(self, schema: str, table: str) -> str:
    qtbl = self.render_table_identifier
    return f"TRUNCATE TABLE {qtbl(schema, table)};"

  # ---------------------------------------------------------------------------
  # Incremental / MERGE Rendering
  # MERGE / UPSERT
  # (Using INSERT ... ON CONFLICT for broad PG compatibility)
  # ---------------------------------------------------------------------------
  def render_merge_statement(
    self,
    schema: str,
    table: str,
    select_sql: str,
    unique_key_columns: list[str],
    update_columns: list[str],
  ) -> str:
    """
    Implement incremental MERGE via INSERT .. ON CONFLICT.

    Contract:
      - `select_sql` must produce columns whose names match the target columns.
      - The column set must at least cover:
        unique_key_columns + update_columns
    """
    target_qualified = self.render_table_identifier(schema, table)

    # ON CONFLICT uses the unique key columns
    key_list = ", ".join(self.render_identifier(c) for c in unique_key_columns)

    # Insert column order = keys first, then update columns
    all_columns = unique_key_columns + [
      c for c in update_columns if c not in unique_key_columns
    ]
    insert_col_list = ", ".join(self.render_identifier(c) for c in all_columns)

    # ON CONFLICT DO UPDATE SET <col> = EXCLUDED.<col>
    update_assignments = ", ".join(
      f"{self.render_identifier(c)} = EXCLUDED.{self.render_identifier(c)}"
      for c in update_columns
    )

    sql = (
      f"INSERT INTO {target_qualified} ({insert_col_list})\n"
      f"{select_sql}\n"
      f"ON CONFLICT ({key_list})\n"
      f"DO UPDATE SET {update_assignments};"
    )

    return sql.strip()

  # ---------------------------------------------------------------------------
  # DDL statements
  # ---------------------------------------------------------------------------
  def render_create_schema_if_not_exists(self, schema: str) -> str:
    q = self.render_identifier
    return f"CREATE SCHEMA IF NOT EXISTS {q(schema)};"
  

  def render_create_table_if_not_exists(self, td) -> str:
    """
    Create the target dataset table based on TargetColumn metadata.
    PostgreSQL supports CREATE TABLE IF NOT EXISTS.
    """
    schema_name = td.target_schema.schema_name
    table_name = td.target_dataset_name

    q = self.render_identifier
    qtbl = self.render_table_identifier

    cols = []
    for c in td.target_columns.all().order_by("ordinal_position"):
      col_name = c.target_column_name  
      if col_name in ("version_started_at", "version_ended_at"):
        col_type = "TIMESTAMPTZ"
      elif col_name == "version_state":
        col_type = "VARCHAR(16)"
      elif col_name == "load_run_id":
        col_type = "VARCHAR(36)"
      else:
        col_type = self._render_canonical_type_postgres(
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
  

  def _render_canonical_type_postgres(
    self,
    *,
    datatype: str,
    max_length=None,
    decimal_precision=None,
    decimal_scale=None,
  ) -> str:
    """
    Map elevata canonical types (TargetColumn.datatype) to PostgreSQL SQL types.
    """
    t = (datatype or "").upper()

    if t == STRING:
      if max_length:
        return f"VARCHAR({int(max_length)})"
      return "TEXT"

    if t == INTEGER:
      return "INTEGER"
    if t == BIGINT:
      return "BIGINT"

    if t == DECIMAL:
      if decimal_precision and decimal_scale is not None:
        return f"NUMERIC({int(decimal_precision)},{int(decimal_scale)})"
      if decimal_precision:
        return f"NUMERIC({int(decimal_precision)})"
      return "NUMERIC"

    if t == FLOAT:
      return "DOUBLE PRECISION"

    if t == BOOLEAN:
      return "BOOLEAN"

    if t == DATE:
      return "DATE"
    if t == TIME:
      return "TIME"
    if t == TIMESTAMP:
      return "TIMESTAMPTZ"

    if t == BINARY:
      return "BYTEA"

    if t == UUID:
      return "UUID"

    if t == JSON:
      return "JSONB"

    return "TEXT"


  # ---------------------------------------------------------------------------
  # Logging
  # ---------------------------------------------------------------------------
  def render_create_load_run_log_if_not_exists(self, meta_schema: str) -> str:
    q = self.render_identifier
    full = f"{q(meta_schema)}.{q('load_run_log')}"

    return f"""
      CREATE SCHEMA IF NOT EXISTS {q(meta_schema)};

      CREATE TABLE IF NOT EXISTS {full} (
        id                 BIGSERIAL PRIMARY KEY,
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
        started_at         TIMESTAMPTZ NOT NULL,
        finished_at        TIMESTAMPTZ NOT NULL,
        render_ms          DOUBLE PRECISION NOT NULL,
        execution_ms       DOUBLE PRECISION NULL,
        sql_length         BIGINT NOT NULL,
        rows_affected      BIGINT NULL,
        load_status        VARCHAR(16) NOT NULL,
        error_message      TEXT NULL
      );
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
