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

try:
  import  psycopg2
except ModuleNotFoundError as e:
   psycopg2 = None

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Sequence

from .base import BaseExecutionEngine, SqlDialect
from metadata.ingestion.types_map import (
  STRING, INTEGER, BIGINT, DECIMAL, FLOAT, BOOLEAN, DATE, TIME, TIMESTAMP, BINARY, UUID, JSON
)
from metadata.materialization.logging import LOAD_RUN_LOG_REGISTRY
from metadata.rendering.dialects.keywords.postgres import RESERVED_KEYWORDS as POSTGRES_RESERVED_KEYWORDS


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
  RESERVED_KEYWORDS = POSTGRES_RESERVED_KEYWORDS

  @property
  def supports_merge(self) -> bool:
    """PostgreSQL supports merge via INSERT ... ON CONFLICT."""
    return True
  
  @property
  def supports_alter_column_type(self) -> bool:
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
  

  def render_alter_column_type(
    self,
    *,
    schema: str,
    table: str,
    column: str,
    new_type: str,
    old_type: str | None = None,
  ) -> str:
    """
    Render PostgreSQL DDL for changing a column's physical type.

    old_type is accepted to match the base dialect contract. PostgreSQL does
    not need it for this SQL shape.
    """
    tbl = self.render_table_identifier(schema, table)
    col = self.render_identifier(column)
    return f"ALTER TABLE {tbl} ALTER COLUMN {col} TYPE {new_type}"

  
  def render_drop_table_if_exists(self, *, schema: str, table: str, cascade: bool = False) -> str:
    target = self.render_table_identifier(schema, table)
    cas = " CASCADE" if cascade else ""
    return f"DROP TABLE IF EXISTS {target}{cas}"


  def render_drop_view_if_exists(
    self,
    *,
    schema: str,
    view: str,
    materialized: bool = False,
    cascade: bool = False,
  ) -> str:
    target = self.render_table_identifier(schema, view)
    obj = "MATERIALIZED VIEW" if materialized else "VIEW"
    cas = " CASCADE" if cascade else ""
    return f"DROP {obj} IF EXISTS {target}{cas}"


  def render_truncate_table(self, schema: str, table: str) -> str:
    qtbl = self.render_table_identifier
    return f"TRUNCATE TABLE {qtbl(schema, table)};"

  # ---------------------------------------------------------------------------
  # 5. DML / load SQL primitives
  # ---------------------------------------------------------------------------
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
    Render a Postgres-compatible merge/upsert statement.

    Important: INSERT ... ON CONFLICT requires a UNIQUE/EXCLUDE constraint (or
    a matching unique index) on the conflict target. In many elevata rawcore
    datasets, business keys are not enforced as unique constraints.

    Therefore, Postgres uses a deterministic UPDATE + INSERT ... WHERE NOT EXISTS
    pattern that does not require constraints, while preserving merge semantics.
    """
    q = self.render_identifier
    target = str(target_fqn).strip()

    keys = [c for c in (key_columns or []) if c]
    if not keys:
      raise ValueError("PostgresDialect.render_merge_statement requires non-empty key_columns")

    insert_cols = [c for c in (insert_columns or []) if c]
    if not insert_cols:
      seen = set()
      insert_cols = []
      for c in keys + list(update_columns or []):
        if c and c not in seen:
          seen.add(c)
          insert_cols.append(c)

    updates = [c for c in (update_columns or []) if c and c not in set(keys)]

    # Wrap the source SELECT as a subquery and alias it.
    src = f"(\n{source_select_sql.strip()}\n) AS {q(source_alias)}"

    on_pred = " AND ".join(
      [f"{q(target_alias)}.{q(k)} = {q(source_alias)}.{q(k)}" for k in keys]
    )

    # UPDATE branch (if there are non-key columns)
    update_sql = ""
    if updates:
      set_sql = ", ".join(
        # Postgres: SET target columns must NOT be qualified with the table alias.
        [f"{q(c)} = {q(source_alias)}.{q(c)}" for c in updates]
      )
      update_sql = (
        f"UPDATE {target} AS {q(target_alias)}\n"
        f"SET {set_sql}\n"
        f"FROM {src}\n"
        f"WHERE {on_pred};"
      )

    # INSERT branch (anti-join)
    insert_cols_sql = ", ".join([q(c) for c in insert_cols])
    select_cols_sql = ", ".join([f"{q(source_alias)}.{q(c)}" for c in insert_cols])
    insert_sql = (
      f"INSERT INTO {target} ({insert_cols_sql})\n"
      f"SELECT {select_cols_sql}\n"
      f"FROM {src}\n"
      f"WHERE NOT EXISTS (\n"
      f"  SELECT 1\n"
      f"  FROM {target} AS {q(target_alias)}\n"
      f"  WHERE {on_pred}\n"
      f");"
    )

    if update_sql:
      return f"{update_sql}\n\n{insert_sql}".strip()
    return insert_sql.strip()


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
  

  def render_string_agg(self, args) -> str:
    if len(args) < 2:
      raise ValueError("STRING_AGG requires at least 2 arguments: value, delimiter.")
    value_sql = self.render_expr(args[0])
    delim_sql = self.render_expr(args[1])
    if len(args) >= 3 and args[2] is not None:
      order_by_sql = self.render_expr(args[2])
      return f"STRING_AGG({value_sql}, {delim_sql} ORDER BY {order_by_sql})"
    return f"STRING_AGG({value_sql}, {delim_sql})"


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
  @staticmethod
  def _sql_string_literal(value: str) -> str:
    s = str(value or "")
    return "'" + s.replace("'", "''") + "'"


  def introspect_dependent_views(
    self,
    *,
    schema_name: str,
    table_name: str,
    exec_engine=None,
  ) -> list[dict[str, str]]:
    """
    Return physical views/materialized views that depend on a table.

    PostgreSQL refuses DROP TABLE when views depend on the target. elevata uses
    this read-only dependency list to drop only managed views explicitly before
    a full-refresh table recreate, instead of using DROP ... CASCADE.
    """
    if exec_engine is None or not hasattr(exec_engine, "fetch_all"):
      return []

    schema_lit = self._sql_string_literal(schema_name)
    table_lit = self._sql_string_literal(table_name)

    sql = f"""
SELECT DISTINCT
  ns_dep.nspname AS dependent_schema,
  dep.relname AS dependent_name,
  CASE dep.relkind
    WHEN 'v' THEN 'view'
    WHEN 'm' THEN 'materialized_view'
    ELSE dep.relkind::text
  END AS dependent_type
FROM pg_depend d
JOIN pg_rewrite r
  ON r.oid = d.objid
JOIN pg_class dep
  ON dep.oid = r.ev_class
JOIN pg_namespace ns_dep
  ON ns_dep.oid = dep.relnamespace
JOIN pg_class base
  ON base.oid = d.refobjid
JOIN pg_namespace ns_base
  ON ns_base.oid = base.relnamespace
WHERE ns_base.nspname = {schema_lit}
  AND base.relname = {table_lit}
  AND dep.relkind IN ('v', 'm')
  AND dep.oid <> base.oid
ORDER BY
  ns_dep.nspname,
  dep.relname;
"""

    rows = exec_engine.fetch_all(sql) or []
    out: list[dict[str, str]] = []
    for row in rows:
      try:
        dep_schema = str(row[0] or "")
        dep_name = str(row[1] or "")
        dep_type = str(row[2] or "view")
      except Exception:
        continue
      if dep_schema and dep_name:
        out.append({
          "schema": dep_schema,
          "name": dep_name,
          "type": dep_type,
        })
    return out


  def _format_information_schema_type(
    self,
    *,
    data_type,
    character_maximum_length=None,
    numeric_precision=None,
    numeric_scale=None,
  ) -> str:
    """
    Render a compact physical type string from PostgreSQL information_schema.
    """
    t = str(data_type or "").strip()
    tl = t.lower()

    if tl in ("character varying", "varchar", "character", "char") and character_maximum_length is not None:
      try:
        return f"{t.upper()}({int(character_maximum_length)})"
      except Exception:
        return t.upper()

    if tl in ("numeric", "decimal") and numeric_precision is not None:
      try:
        if numeric_scale is not None:
          return f"NUMERIC({int(numeric_precision)},{int(numeric_scale)})"
        return f"NUMERIC({int(numeric_precision)})"
      except Exception:
        return "NUMERIC"

    return t.upper()


  def _introspect_table_with_information_schema(
    self,
    *,
    schema_name: str,
    table_name: str,
    exec_engine=None,
  ) -> Dict[str, Any] | None:
    """
    Introspect PostgreSQL tables/views through the active execution engine.

    This keeps execute-mode guards aligned with the same target connection that
    runs the load. It is intentionally Postgres-specific and does not change
    materialization semantics.
    """
    if exec_engine is None or not hasattr(exec_engine, "fetch_all"):
      return None

    schema_lit = self._sql_string_literal(schema_name)
    table_lit = self._sql_string_literal(table_name)

    sql = f"""
SELECT
  t.table_schema,
  t.table_name,
  t.table_type,
  c.column_name,
  c.data_type,
  c.character_maximum_length,
  c.numeric_precision,
  c.numeric_scale,
  c.is_nullable,
  c.ordinal_position
FROM information_schema.tables t
LEFT JOIN information_schema.columns c
  ON c.table_schema = t.table_schema
 AND c.table_name = t.table_name
WHERE t.table_schema = {schema_lit}
  AND t.table_name = {table_lit}
  AND t.table_type IN ('BASE TABLE', 'VIEW')
ORDER BY
  c.ordinal_position NULLS LAST;
"""

    rows = exec_engine.fetch_all(sql) or []
    if not rows:
      return {
        "table_exists": False,
        "physical_table": table_name,
        "physical_object_type": None,
        "actual_cols_by_norm_name": {},
      }

    actual_cols: dict[str, dict[str, object]] = {}
    physical_object_type = None
    for row in rows:
      try:
        physical_object_type = str(row[2] or "").strip() or physical_object_type
        col_name = str(row[3] or "").strip()
        if not col_name:
          continue
        physical_type = self._format_information_schema_type(
          data_type=row[4],
          character_maximum_length=row[5],
          numeric_precision=row[6],
          numeric_scale=row[7],
        )
        actual_cols[col_name.lower()] = {
          "name": col_name,
          "type": physical_type,
          "nullable": str(row[8] or "YES").upper() == "YES",
          "ordinal_position": row[9],
        }
      except Exception:
        continue

    return {
      "table_exists": True,
      "physical_table": table_name,
      "physical_object_type": physical_object_type,
      "actual_cols_by_norm_name": actual_cols,
    }


  def introspect_table(
    self,
    *,
    schema_name: str,
    table_name: str,
    introspection_engine=None,
    exec_engine=None,
    debug_plan: bool = False,
  ):
    info = self._introspect_table_with_information_schema(
      schema_name=schema_name,
      table_name=table_name,
      exec_engine=exec_engine,
    )
    if info is not None:
      return info

    # Fallback to SQLAlchemy-based default introspection when available.
    return SqlDialect.introspect_table(
      self,
      schema_name=schema_name,
      table_name=table_name,
      introspection_engine=introspection_engine,
      exec_engine=exec_engine,
      debug_plan=debug_plan,
    )
