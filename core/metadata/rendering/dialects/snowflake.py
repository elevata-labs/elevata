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

from datetime import date, datetime
from decimal import Decimal
from typing import Sequence

from urllib.parse import urlparse, parse_qs, unquote

from .base import BaseExecutionEngine, SqlDialect
from metadata.ingestion.types_map import (
  STRING, INTEGER, BIGINT, DECIMAL, FLOAT, BOOLEAN, DATE, TIME, TIMESTAMP, BINARY, UUID, JSON
)
from metadata.materialization.logging import LOAD_RUN_LOG_REGISTRY


class SnowflakeExecutionEngine(BaseExecutionEngine):
  """
  Snowflake SQL execution via snowflake-connector-python.

  Expected system.security patterns (dict):
    {
      "account": "...",
      "user": "...",
      "password": "...",
      "warehouse": "...",
      "database": "...",
      "schema": "...",
      "role": "..."          # optional
    }
  """

  def __init__(self, system):
    security = getattr(system, "security", None) or {}
    if not isinstance(security, dict):
      raise ValueError(
        f"Snowflake system '{system.short_name}' expects system.security to be a dict."
      )

    # Support a single SQLAlchemy-style connection string (used in .env)
    # Example:
    # snowflake://USER:PASSWORD@account_identifier/DB/SCHEMA?warehouse=COMPUTE_WH&role=ACCOUNTADMIN
    self.connection_string = security.get("connection_string")
    if self.connection_string:
      u = urlparse(self.connection_string)
      if u.scheme != "snowflake":
        raise ValueError(
          f"Snowflake connection_string must start with snowflake:// (got '{u.scheme}://')."
        )

      # netloc: user:pass@account
      self.user = unquote(u.username) if u.username else None
      self.password = unquote(u.password) if u.password else None
      self.account = u.hostname  # e.g. namckzx-zg82972

      # path: /DB/SCHEMA
      path_parts = [p for p in (u.path or "").split("/") if p]
      self.database = path_parts[0] if len(path_parts) >= 1 else None
      self.schema = path_parts[1] if len(path_parts) >= 2 else None

      q = parse_qs(u.query or "")
      # parse_qs returns lists
      self.warehouse = (q.get("warehouse") or [None])[0]
      self.role = (q.get("role") or [None])[0]

      # Validate minimum for connector-based execution
      if not (self.account and self.user and self.password and self.warehouse and self.database):
        raise ValueError(
          "Snowflake connection_string must include user, password, account, database, warehouse "
          "(and optionally schema, role)."
        )
      return

    self.account = security.get("account")
    self.user = security.get("user")
    self.password = security.get("password")
    self.warehouse = security.get("warehouse")
    self.database = security.get("database")
    self.schema = security.get("schema")
    self.role = security.get("role")

    if not (self.account and self.user and self.password and self.warehouse and self.database):
      raise ValueError(
        "Snowflake system.security must contain account, user, password, warehouse, database."
      )

  def _sf(self):
    try:
      import snowflake.connector as sf
      return sf
    except Exception as exc:
      raise ImportError(
        "Missing dependency for Snowflake execution. Install 'snowflake-connector-python'."
      ) from exc

  def _connect(self):
    sf = self._sf()
    return sf.connect(
      account=self.account,
      user=self.user,
      password=self.password,
      warehouse=self.warehouse,
      database=self.database,
      schema=self.schema,
      role=self.role,
      autocommit=False,   # since you call commit()
    )


  @staticmethod
  def _split_statements(sql: str) -> list[str]:  
    """
    Snowflake connector executes exactly one statement per cursor.execute() call
    unless multi-statement mode is explicitly enabled.

    elevata renders multi-statement SQL for some operations (e.g. DELETE + MERGE),
    so we split on ';' and execute statements sequentially.
    """
    statements: list[str] = []
    for part in (sql or "").split(";"):
      s = part.strip()
      if not s:
        continue
      statements.append(s + ";")
    return statements


  def execute(self, sql: str) -> int | None:
    conn = self._connect()
    try:
      cur = conn.cursor()
      try:
        rowcount = None
        for stmt in SnowflakeExecutionEngine._split_statements(sql):
          cur.execute(stmt)
          # best-effort: keep the last rowcount (MERGE is usually last)
          rowcount = getattr(cur, "rowcount", None)
        conn.commit()
        return rowcount
      finally:
        cur.close()
    finally:
      conn.close()

  def execute_many(self, sql: str, params):
    """
    Batch execution for parameterized INSERTs (RAW ingestion).
    Uses Snowflake connector executemany().
    """
    if not params:
      return 0
    conn = self._connect()
    try:
      cur = conn.cursor()
      try:
        cur.executemany(sql, params)
        conn.commit()
        return getattr(cur, "rowcount", None)
      finally:
        cur.close()
    finally:
      conn.close()


  def fetch_all(self, sql: str) -> list[tuple]:
    conn = self._connect()
    try:
      cur = conn.cursor()
      try:
        cur.execute(sql)
        rows = cur.fetchall()
        return [tuple(r) for r in (rows or [])]
      finally:
        cur.close()
    finally:
      conn.close()
      

  def execute_scalar(self, sql: str):
    rows = self.fetch_all(sql)
    if not rows:
      return None
    return rows[0][0]


class SnowflakeDialect(SqlDialect):
  """
  Snowflake dialect.
  """

  # ---------------------------------------------------------------------------
  # 1. Class meta / capabilities
  # ---------------------------------------------------------------------------
  DIALECT_NAME = "snowflake"

  @property
  def supports_merge(self) -> bool:
    return True
  
  @property
  def supports_alter_column_type(self) -> bool:
    return True

  @property
  def supports_delete_detection(self) -> bool:
    return True

  def get_execution_engine(self, system) -> BaseExecutionEngine:
    return SnowflakeExecutionEngine(system)

  # ---------------------------------------------------------------------------
  # 2. Identifier rendering
  # ---------------------------------------------------------------------------
  def quote_ident(self, name: str) -> str:
    s = str(name or "")
    s = s.replace('"', '""')
    return f'"{s}"'

  # ---------------------------------------------------------------------------
  # 3. Type mapping / DDL helpers
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
    dt = (canonical or "").upper()

    if dt == STRING:
      if max_length is not None:
        n = int(max_length)
        return f"VARCHAR({n})"
      return "VARCHAR"
    if dt == INTEGER:
      return "INTEGER"
    if dt == BIGINT:
      return "BIGINT"
    if dt == DECIMAL:
      p = 38 if precision is None else int(precision)
      s = 0 if scale is None else int(scale)
      return f"NUMBER({p},{s})"
    if dt == FLOAT:
      return "FLOAT"
    if dt == BOOLEAN:
      return "BOOLEAN"
    if dt == DATE:
      return "DATE"
    if dt == TIME:
      return "TIME"
    if dt == TIMESTAMP:
      # Keep neutral (no timezone) unless you explicitly require TZ semantics.
      return "TIMESTAMP_NTZ"
    if dt == BINARY:
      return "BINARY"
    if dt == UUID:
      return "VARCHAR(36)"
    if dt == JSON:
      return "VARIANT"

    raise ValueError(f"Unsupported logical datatype for Snowflake: {canonical!r}")

  def render_create_schema_if_not_exists(self, schema_name: str) -> str:
    sch = self.render_identifier(schema_name)
    return f"CREATE SCHEMA IF NOT EXISTS {sch};"


  # ---------------------------------------------------------------------------
  # 4. DDL helpers
  # ---------------------------------------------------------------------------
  def render_alter_column_type(self, *, schema: str, table: str, column: str, new_type: str) -> str:
    # Snowflake: ALTER TABLE <tbl> ALTER COLUMN <col> SET DATA TYPE <type>
    tbl = self.render_table_identifier(schema, table)
    col = self.render_identifier(column)
    return f"ALTER TABLE {tbl} ALTER COLUMN {col} SET DATA TYPE {new_type}"


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
    Render a dialect-native MERGE / UPSERT statement.

    This method is a core dialect primitive: elevata's load layer supplies only the
    semantic ingredients (source SELECT + key/update/insert column lists), while the
    dialect decides the optimal SQL shape.

    Parameters:
      target_fqn: Fully qualified target table identifier (already rendered/quoted).
      source_select_sql: SELECT statement used as merge source (no trailing ';').
      key_columns: Non-empty list of target key columns used for matching.
      update_columns: Target columns to update on match.
      insert_columns: Target columns to insert for new rows.

    Snowflake renders a native MERGE INTO statement.
    """
    q = self.render_identifier
    target = str(target_fqn).strip()

    keys = [c for c in (key_columns or []) if c]
    if not keys:
      raise ValueError("SnowflakeDialect.render_merge_statement requires non-empty key_columns")

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
      f"MERGE INTO {target} AS {q(target_alias)}\n"
      f"USING {src}\n"
      f"ON {on_pred}"
    )

    if updates:
      update_assignments = ", ".join(
        [f"{q(c)} = {q(source_alias)}.{q(c)}" for c in updates]
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
    *,
    target_schema: str,
    target_table: str,
    stage_schema: str,
    stage_table: str,
    join_predicates: list[str],
    scope_filter: str | None = None,
  ) -> str:
    tgt = self.render_table_identifier(target_schema, target_table)
    stg = self.render_table_identifier(stage_schema, stage_table)
    on_pred = " AND ".join(join_predicates or [])

    where_parts = []
    if scope_filter:
      where_parts.append(f"({scope_filter})")
    where_parts.append(
      "NOT EXISTS (\n"
      "  SELECT 1\n"
      f"  FROM {stg} AS s\n"
      f"  WHERE {on_pred}\n"
      ")"
    )
    where_sql = "\n  AND ".join(where_parts)
    return f"""
      DELETE FROM {tgt} AS t
      WHERE {where_sql};
      """.strip()
  
  # Canonical -> physical types for registry-driven meta logging (meta.load_run_log).
  # This is required by ensure_load_run_log_table(...).
  LOAD_RUN_LOG_TYPE_MAP = {
    "string": "VARCHAR(500)",
    "bool": "BOOLEAN",
    "int": "INTEGER",
    "timestamp": "TIMESTAMP_NTZ",
  }

  def map_load_run_log_type(self, col_name: str, canonical_type: str) -> str | None:
    # Keep it conservative and aligned with other dialects:
    # - allow longer error text
    c = (col_name or "").strip().lower()
    if c == "error_message":
      return "VARCHAR(2000)"
    if c == "snapshot_json":
      return "VARCHAR(4000)"
    
    return self.LOAD_RUN_LOG_TYPE_MAP.get(canonical_type)


  def render_insert_load_run_log(self, *, meta_schema: str, values: dict[str, object]) -> str:
    qtbl = self.render_table_identifier
    lit = self.render_literal

    table = qtbl(meta_schema, "load_run_log")
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
    # Snowflake connector uses %s (pyformat style) in cursor.execute with params.
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
      return f"TIMESTAMP '{ts}'"

    s = str(value).replace("'", "''")
    return f"'{s}'"

  def concat_expression(self, parts: Sequence[str]) -> str:
    if not parts:
      return "''"
    return "(" + " || ".join(parts) + ")"

  def hash_expression(self, expr: str, algo: str = "sha256") -> str:
    # Snowflake SHA2(...) returns a 64-char hex string in this environment.
    # Do NOT HEX-encode again (would double-encode -> 128 chars).    
    algo_lower = (algo or "").lower()
    if algo_lower in ("sha256", "hash256"):
      return f"SHA2(TO_VARCHAR({expr}), 256)"
    return f"SHA2(TO_VARCHAR({expr}), 256)"
