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

import psycopg2

from datetime import date, datetime
from decimal import Decimal
from typing import Sequence

from .base import BaseExecutionEngine, SqlDialect
from metadata.ingestion.types_map import (
  STRING, INTEGER, BIGINT, DECIMAL, FLOAT, BOOLEAN, DATE, TIME, TIMESTAMP, BINARY, UUID, JSON
)
from metadata.materialization.logging import LOAD_RUN_LOG_REGISTRY

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

  def execute_many(self, sql: str, params_seq) -> int | None:
    with psycopg2.connect(self.conn_str) as conn:
      with conn.cursor() as cur:
        cur.executemany(sql, params_seq)
        try:
          return cur.rowcount
        except Exception:
          return None

  def execute_scalar(self, sql: str):
    """
    Execute a SELECT returning a single value (first column of first row).
    Returns None if no row is returned.
    """
    with psycopg2.connect(self.conn_str) as conn:
      with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        if not row:
          return None
        return row[0]

  def fetch_all(self, sql: str) -> list[tuple]:
    """
    Execute a SELECT and return all rows as tuples.
    """
    with psycopg2.connect(self.conn_str) as conn:
      with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        return list(rows or [])


class PostgresDialect(SqlDialect):
  """
  SQL dialect for PostgreSQL.

  Compatible with elevata LogicalPlan:
  - SubquerySource
  - WindowFunction
  - RawSql templates
  """

  # ---------------------------------------------------------------------------
  # 1. Class meta / capabilities
  # ---------------------------------------------------------------------------
  DIALECT_NAME = "postgres"

  @property
  def supports_merge(self) -> bool:
    """PostgreSQL supports merge via INSERT ... ON CONFLICT."""
    return True

  @property
  def supports_delete_detection(self) -> bool:
    """Delete detection is implemented via generic DELETE ... NOT EXISTS patterns."""
    return True

  def get_execution_engine(self, system):
    return PostgresExecutionEngine(system)

  # ---------------------------------------------------------------------------
  # 2. Identifier & quoting
  # ---------------------------------------------------------------------------
  def quote_ident(self, ident: str) -> str:
    return f"\"{ident}\""

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
    return self._render_canonical_type_postgres(
      datatype=canonical,
      max_length=max_length,
      decimal_precision=precision,
      decimal_scale=scale,
    )

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

    raise ValueError(
      f"Unsupported canonical datatype for Postgres: {datatype!r}. "
      "Please fix ingestion type mapping or extend the dialect mapping."
    )

  # ---------------------------------------------------------------------------
  # 4. DDL helpers
  # ---------------------------------------------------------------------------
  def render_create_schema_if_not_exists(self, schema: str) -> str:
    q = self.render_identifier
    return f"CREATE SCHEMA IF NOT EXISTS {q(schema)};"
  
  def render_drop_table_if_exists(self, *, schema: str, table: str, cascade: bool = False) -> str:
    target = self.render_table_identifier(schema, table)
    cas = " CASCADE" if cascade else ""
    return f"DROP TABLE IF EXISTS {target}{cas}"

  def render_truncate_table(self, schema: str, table: str) -> str:
    qtbl = self.render_table_identifier
    return f"TRUNCATE TABLE {qtbl(schema, table)};"

  # ---------------------------------------------------------------------------
  # 5. DML / load SQL primitives
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

  LOAD_RUN_LOG_TYPE_MAP = {
    "string": "TEXT",
    "bool": "BOOLEAN",
    "int": "INTEGER",
    "timestamp": "TIMESTAMPTZ",
  }

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

  def param_placeholder(self) -> str:
    """
    Placeholder for parameterized SQL statements used by the dialect's execution engine.
    Postgres differs from duckdb.
    """
    return "%s"

  # ---------------------------------------------------------------------------
  # 6. Expression / Select renderer
  # ---------------------------------------------------------------------------
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

  def concat_expression(self, parts: Sequence[str]) -> str:
    """
    PostgreSQL string concatenation uses || as well, so we can mirror DuckDB.
    """
    if not parts:
      return "''"
    return "(" + " || ".join(parts) + ")"

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

  # ---------------------------------------------------------------------------
  # 7. Introspection hooks
  # ---------------------------------------------------------------------------
  def introspect_table(
    self,
    *,
    schema_name: str,
    table_name: str,
    introspection_engine,
    exec_engine=None,
    debug_plan: bool = False,
  ):
    # Use SQLAlchemy-based default introspection for Postgres.
    return SqlDialect.introspect_table(
      self,
      schema_name=schema_name,
      table_name=table_name,
      introspection_engine=introspection_engine,
      exec_engine=exec_engine,
      debug_plan=debug_plan,
    )
