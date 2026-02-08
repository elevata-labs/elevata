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

import datetime
from decimal import Decimal
from typing import Sequence

from .base import BaseExecutionEngine, SqlDialect
from metadata.ingestion.types_map import (
  STRING, INTEGER, BIGINT, DECIMAL, FLOAT, BOOLEAN, DATE, TIME, TIMESTAMP, BINARY, UUID, JSON
)
from metadata.materialization.logging import LOAD_RUN_LOG_REGISTRY


class FabricWarehouseExecutionEngine(BaseExecutionEngine):
  """
  Fabric Warehouse uses T-SQL over TDS; pyodbc connection strings work well.

  Expected system.security:
    {"connection_string": "..."}
  """

  def __init__(self, system):
    try:
      import pyodbc  # local import to avoid hard dependency at import time
    except Exception as exc:
      raise ImportError(
        "Missing dependency for Fabric Warehouse execution. Install 'pyodbc' and an ODBC driver."
      ) from exc

    self._pyodbc = pyodbc

    conn_str = None
    if system.security and isinstance(system.security, dict):
      conn_str = system.security.get("connection_string")
    if not conn_str:
      raise ValueError(
        f"Fabric Warehouse system '{system.short_name}' has no usable connection string in security."
      )
    self.conn_str = conn_str

  def execute(self, sql: str) -> int | None:
    conn = self._pyodbc.connect(self.conn_str, autocommit=False)
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
    conn = self._pyodbc.connect(self.conn_str, autocommit=False)
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
    conn = self._pyodbc.connect(self.conn_str, autocommit=False)
    try:
      cursor = conn.cursor()
      cursor.execute(sql)
      row = cursor.fetchone()
      if not row:
        return None
      return row[0]
    finally:
      conn.close()

  def fetch_all(self, sql: str) -> list[tuple]:
    conn = self._pyodbc.connect(self.conn_str, autocommit=False)
    try:
      cursor = conn.cursor()
      cursor.execute(sql)
      rows = cursor.fetchall()
      return [tuple(r) for r in (rows or [])]
    finally:
      conn.close()


class FabricWarehouseDialect(SqlDialect):
  """
  Microsoft Fabric Warehouse dialect (T-SQL compatible).

  Notes:
  - Fabric Warehouse does not support NVARCHAR; use VARCHAR.
  - Avoid MAX to keep DDL deterministic and aligned with governance expectations.
  """

  # ---------------------------------------------------------------------------
  # 1. Class meta / capabilities
  # ---------------------------------------------------------------------------
  DIALECT_NAME = "fabric_warehouse"

  # Default lengths (no MAX in Fabric Warehouse)
  DEFAULT_VARCHAR_LEN = 4000
  # Keep conservative, deterministic lengths to avoid platform-specific limits.
  LARGE_VARCHAR_LEN = 4000
  DEFAULT_VARBINARY_LEN = 4000

  @property
  def supports_merge(self) -> bool:
    return True

  @property
  def supports_delete_detection(self) -> bool:
    return True

  def get_execution_engine(self, system) -> BaseExecutionEngine:
    return FabricWarehouseExecutionEngine(system)

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
    return self._render_canonical_type_fabric_warehouse(
      datatype=canonical,
      max_length=max_length,
      decimal_precision=precision,
      decimal_scale=scale,
      strict=strict,
    )

  def _render_canonical_type_fabric_warehouse(
    self,
    *,
    datatype: str,
    max_length=None,
    decimal_precision=None,
    decimal_scale=None,
    strict: bool = True,
  ) -> str:
    """
    Map elevata canonical types to Fabric Warehouse SQL types.

    Modeled after MSSQL mapping rules but:
      - VARCHAR only (no NVARCHAR)
      - no MAX: use deterministic default lengths
    """
    t = (datatype or "").upper()

    if t == STRING:
      if max_length:
        return f"VARCHAR({int(max_length)})"
      return f"VARCHAR({self.DEFAULT_VARCHAR_LEN})"

    if t == INTEGER:
      return "INT"
    if t == BIGINT:
      return "BIGINT"

    if t == DECIMAL:
      if decimal_precision and decimal_scale is not None:
        return f"DECIMAL({int(decimal_precision)},{int(decimal_scale)})"
      if decimal_precision:
        return f"DECIMAL({int(decimal_precision)})"
      # Keep same default spirit as MSSQL: wide enough & stable
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
      return f"VARBINARY({self.DEFAULT_VARBINARY_LEN})"

    if t == UUID:
      # Avoid UNIQUEIDENTIFIER due to cross-endpoint semantics:
      # Fabric stores it as binary in Delta Parquet, which can break reading/join semantics
      # across Warehouse vs SQL analytics endpoint. Use a portable string representation.
      return "VARCHAR(36)"    

    if t == JSON:
      # No native JSON type; store as VARCHAR with a stable large length.
      return f"VARCHAR({self.LARGE_VARCHAR_LEN})"

    raise ValueError(
      f"Unsupported canonical datatype for Fabric Warehouse: {datatype!r}. "
      "Please fix ingestion type mapping or extend the dialect mapping."
    )

  def render_create_schema_if_not_exists(self, schema_name: str) -> str:
    sch = str(schema_name or "").replace("'", "''")
    return f"""
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{sch}')
BEGIN
  EXEC('CREATE SCHEMA {self.quote_ident(schema_name)}');
END;
""".strip()


  def render_add_column(self, schema: str, table: str, col_name: str, physical_type: str) -> str:
    """
    T-SQL family (MSSQL/Fabric Warehouse) does NOT use 'ADD COLUMN'.
    MSSQL overrides this as well. :contentReference[oaicite:1]{index=1}
    """
    target = self.render_table_identifier(schema, table)
    col = self.render_identifier(col_name)
    typ = str(physical_type or "").strip()
    if not typ:
      raise ValueError("render_add_column requires a non-empty physical_type")
    return f"ALTER TABLE {target} ADD {col} {typ};"
  

  def render_create_table_if_not_exists_from_columns(self, *, schema: str, table: str, columns: list[dict[str, object]]) -> str:
    """
    Important: columns already contain a mapped physical type in c["type"]
    (see base.py render_create_table_if_not_exists). :contentReference[oaicite:2]{index=2}
    """
    target = self.render_table_identifier(schema, table)

    col_defs: list[str] = []
    for c in (columns or []):
      name = self.render_identifier(str(c["name"]))
      ctype = str(c["type"])
      nullable = bool(c.get("nullable", True))
      null_sql = "NULL" if nullable else "NOT NULL"
      col_defs.append(f"{name} {ctype} {null_sql}")

    cols_sql = ",\n  ".join(col_defs)
    full_name = f"{schema}.{table}".replace("'", "''")

    return f"""
IF OBJECT_ID('{full_name}', 'U') IS NULL
BEGIN
  CREATE TABLE {target} (
    {cols_sql}
  );
END;
""".strip()

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
    target = self.render_table_identifier(schema, table)

    keys = list(unique_key_columns or [])
    updates = [c for c in (update_columns or [])]
    all_cols = keys + [c for c in updates if c not in keys]

    on_pred = " AND ".join([f"t.{self.render_identifier(k)} = s.{self.render_identifier(k)}" for k in keys])

    update_assignments = ", ".join(
      [f"t.{self.render_identifier(c)} = s.{self.render_identifier(c)}" for c in updates]
    )

    insert_cols = ", ".join([self.render_identifier(c) for c in all_cols])
    insert_vals = ", ".join([f"s.{self.render_identifier(c)}" for c in all_cols])

    return f"""
MERGE {target} AS t
USING (
{select_sql}
) AS s
ON {on_pred}
WHEN MATCHED THEN
  UPDATE SET {update_assignments}
WHEN NOT MATCHED THEN
  INSERT ({insert_cols}) VALUES ({insert_vals});
""".strip()

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
      DELETE t
      FROM {tgt} AS t
      WHERE {where_sql};
      """.strip()  


  LOAD_RUN_LOG_TYPE_MAP = {
    "string": "VARCHAR(500)",
    "bool": "BIT",
    "int": "INT",
    "timestamp": "DATETIME2",
  }

  def map_load_run_log_type(self, col_name: str, canonical_type: str) -> str | None:
    # Mirror MSSQL behavior but without NVARCHAR/MAX. 
    if col_name == "error_message":
      return "VARCHAR(2000)"
    if col_name == "snapshot_json":
      return f"VARCHAR({self.LARGE_VARCHAR_LEN})"
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
    return "?"

  # ---------------------------------------------------------------------------
  # 6. Expression / Select renderer
  # ---------------------------------------------------------------------------
  def render_literal(self, value):
    if value is None:
      return "NULL"
    if isinstance(value, bool):
      return "1" if value else "0"
    if isinstance(value, int):
      return str(value)
    if isinstance(value, float):
      return repr(value)
    if isinstance(value, Decimal):
      return str(value)
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
      return f"CAST('{value.isoformat()}' AS DATE)"
    if isinstance(value, datetime.datetime):
      ts = value.replace(microsecond=0).isoformat(sep=" ")
      return f"CAST('{ts}' AS DATETIME2)"
    s = str(value).replace("'", "''")
    return f"'{s}'"

  def concat_expression(self, parts: Sequence[str]) -> str:
    if not parts:
      return "''"
    return "(" + " + ".join(parts) + ")"

  def hash_expression(self, expr: str, algo: str = "sha256") -> str:
    # Return a 64-char hex string (style 2), aligned with MSSQL semantics. 
    algo_lower = (algo or "").lower()
    if algo_lower in ("sha256", "hash256"):
      return (
        "CONVERT(VARCHAR(64), "
        f"HASHBYTES('SHA2_256', CAST(({expr}) AS VARCHAR({self.LARGE_VARCHAR_LEN}))), 2)"
      )
    return (
      "CONVERT(VARCHAR(64), "
      f"HASHBYTES('SHA2_256', CAST(({expr}) AS VARCHAR({self.LARGE_VARCHAR_LEN}))), 2)"
    )
