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

from typing import Any, Dict, Optional
import re
try:
  import duckdb
except ModuleNotFoundError as e:
  duckdb = None

from .base import SqlDialect, BaseExecutionEngine
from metadata.ingestion.types_map import (
  STRING, INTEGER, BIGINT, DECIMAL, FLOAT, BOOLEAN, DATE, TIME, TIMESTAMP, BINARY, UUID, JSON
)
from metadata.materialization.logging import LOAD_RUN_LOG_REGISTRY

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

  # ---------------------------------------------------------------------------
  # 1. Class meta / capabilities
  # ---------------------------------------------------------------------------
  DIALECT_NAME = "duckdb"

  @property
  def supports_merge(self) -> bool:
    """DuckDB supports a native MERGE statement."""
    return True
  
  @property
  def supports_alter_column_type(self) -> bool:
    return True

  @property
  def supports_delete_detection(self) -> bool:
    """DuckDB supports delete detection via DELETE + NOT EXISTS."""
    return True

  def get_execution_engine(self, system):
    return DuckDbExecutionEngine(system)

  # ---------------------------------------------------------------------------
  # 2. Identifier & quoting
  # ---------------------------------------------------------------------------
  def quote_ident(self, name: str) -> str:
    """
    Quote an identifier using DuckDB's double-quote style.
    Internal double quotes are escaped by doubling them.
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
    return self._render_canonical_type_duckdb(
      datatype=canonical,
      max_length=max_length,
      decimal_precision=precision,
      decimal_scale=scale,
    )

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
  # 4. DDL helpers
  # ---------------------------------------------------------------------------
  def render_create_schema_if_not_exists(self, schema: str) -> str:
    """
    DuckDB supports CREATE SCHEMA IF NOT EXISTS.
    """
    q = self.render_identifier
    return f"CREATE SCHEMA IF NOT EXISTS {q(schema)};"
  

  def render_alter_column_type(self, *, schema: str, table: str, column: str, new_type: str) -> str:
    # DuckDB: ALTER TABLE <tbl> ALTER COLUMN <col> SET DATA TYPE <type>
    tbl = self.render_table_identifier(schema, table)
    col = self.render_identifier(column)
    return f"ALTER TABLE {tbl} ALTER COLUMN {col} SET DATA TYPE {new_type}"


  # ---------------------------------------------------------------------------
  # 5. DML / load SQL primitives
  # ---------------------------------------------------------------------------
  LOAD_RUN_LOG_TYPE_MAP = {
    "string": "VARCHAR",
    "bool": "BOOLEAN",
    "int": "INTEGER",
    "timestamp": "TIMESTAMP",
  }

  def render_insert_load_run_log(self, *, meta_schema: str, values: dict[str, object]) -> str:

    table = self.render_table_identifier(meta_schema, "load_run_log")

    def s(txt: object | None) -> str:
      if txt is None:
        return "NULL"
      txt = str(txt)
      txt = txt.replace("'", "''")
      txt = txt.replace("\r\n", "\n")
      return f"'{txt}'"

    def lit(col: str, v: object) -> str:
      if v is None:
        return "NULL"
      if col in ("started_at", "finished_at"):
        # DuckDB: prefer TIMESTAMP 'YYYY-MM-DD HH:MM:SS[.ffffff]'
        iso = v.isoformat() if hasattr(v, "isoformat") else str(v)
        iso = iso.replace("T", " ")
        return f"TIMESTAMP {s(iso)}"
      if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
      if isinstance(v, (int, float)):
        return str(int(v))
      return s(v)

    # Use the canonical registry order (single source of truth).
    # Unknown keys in `values` are ignored; missing keys become NULL.
    cols = list(LOAD_RUN_LOG_REGISTRY.keys())

    col_sql = ",\n        ".join(cols)
    val_sql = ",\n        ".join([lit(c, values.get(c)) for c in cols])

    return f"""
      INSERT INTO {table} (
        {col_sql}
      )
      VALUES (
        {val_sql}
      )
      """.strip()

  def _literal_for_meta_insert(self, *, table: str, column: str, value: object) -> str:
    if value is None:
      return "NULL"
    if column in ("created_at", "started_at", "finished_at"):
      iso = value.isoformat() if hasattr(value, "isoformat") else str(value)
      iso = iso.replace("T", " ")
      s = str(iso).replace("'", "''").replace("\r\n", "\n")
      return f"TIMESTAMP '{s}'"
    if isinstance(value, bool):
      return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
      return str(int(value))
    s = str(value).replace("'", "''").replace("\r\n", "\n")
    return f"'{s}'"

  # ---------------------------------------------------------------------------
  # 6. Expression / Select renderer
  # ---------------------------------------------------------------------------
  def render_string_agg(self, args) -> str:
    if len(args) < 2:
      raise ValueError("STRING_AGG requires at least 2 arguments: value, delimiter.")
    value_sql = self.render_expr(args[0])
    delim_sql = self.render_expr(args[1])
    if len(args) >= 3 and args[2] is not None:
      order_by_sql = self.render_expr(args[2])
      return f"string_agg({value_sql}, {delim_sql} ORDER BY {order_by_sql})"
    return f"string_agg({value_sql}, {delim_sql})"


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

  # ---------------------------------------------------------------------------
  # 7. Introspection hooks
  # ---------------------------------------------------------------------------
  def introspect_table(
    self,
    *,
    schema_name: str,
    table_name: str,
    introspection_engine: Any = None,
    exec_engine: Optional["BaseExecutionEngine"] = None,
    debug_plan: bool = False,
  ) -> Dict[str, Any]:
    """
    DuckDB introspection via PRAGMA table_info.

    Important: exec_engine.fetch_all may return either dict rows or tuple rows,
    depending on the connection wrapper. We support both.
    """
    # Prevent accidental inheritance of DuckDB PRAGMA introspection.
    if getattr(self, "DIALECT_NAME", None) != "duckdb":
      raise NotImplementedError(
        f"{self.__class__.__name__} inherits DuckDBDialect; override introspect_table() "
        "to avoid DuckDB PRAGMA-based introspection."
      )

    if exec_engine is None:
      return {"table_exists": False, "physical_table": table_name, "actual_cols_by_norm_name": {}}

    phys = table_name
    full = f"{schema_name}.{phys}" if schema_name else str(phys)
    sql = f"SELECT * FROM pragma_table_info('{full}');"

    try:
      rows = exec_engine.fetch_all(sql)
    except Exception as exc:
      # Missing table is a normal state during planning.
      msg = str(exc)
      if re.search(r"does not exist", msg, flags=re.IGNORECASE):
        return {"table_exists": False, "physical_table": phys, "actual_cols_by_norm_name": {}}
      raise

    if not rows:
      return {"table_exists": False, "physical_table": phys, "actual_cols_by_norm_name": {}}

    def _norm_name(n: object) -> str:
      return str(n or "").strip().lower()

    def _norm_type(t: object) -> str:
      s = str(t or "").strip().lower()
      return " ".join(s.split())

    cols: Dict[str, Dict[str, Any]] = {}
    for r in rows:
      # pragma_table_info returns: (cid, name, type, notnull, dflt_value, pk)
      if isinstance(r, dict):
        name = r.get("name")
        typ = r.get("type")
      elif isinstance(r, (tuple, list)):
        name = r[1] if len(r) > 1 else None
        typ = r[2] if len(r) > 2 else None
      else:
        # Best-effort fallback
        name = getattr(r, "name", None)
        typ = getattr(r, "type", None)

      nm = _norm_name(name)
      if not nm:
        continue
      cols[nm] = {"name": str(name), "type": _norm_type(typ)}

    return {"table_exists": True, "physical_table": phys, "actual_cols_by_norm_name": cols}
