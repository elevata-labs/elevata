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

from sqlalchemy import inspect, text
from typing import Dict, Any
import re


def _normalize_sa_columns(cols: list[dict]) -> list[dict]:
  """
  Ensure SQLAlchemy inspector columns are consistent across dialects:
    - type is always a string (planner/drift checks rely on stable representations)
    - nullable is normalized to bool when present
  """
  out = []
  for c in cols or []:
    cc = dict(c)
    if "type" in cc:
      try:
        cc["type"] = str(cc["type"])
      except Exception:
        cc["type"] = str(cc.get("type") or "")
    if "nullable" in cc and cc["nullable"] is not None:
      cc["nullable"] = bool(cc["nullable"])
    out.append(cc)
  return out


def read_table_metadata(engine, schema: str, table: str) -> Dict[str, Any]:
  """
  Reflection via SQLAlchemy Inspector, little extra work for
  MSSQL alias/user defined types like dbo.name or dbo.flag etc.
  """
  name = (getattr(engine.dialect, "name", "") or "").lower()
  # Fabric Warehouse exposes a T-SQL endpoint. Introspect with sys.* like MSSQL.
  if name in ("mssql",):
    return _read_table_metadata_mssql(engine, schema, table)

  if name in ("fabric", "fabric_warehouse"):
    return _read_table_metadata_fabric(engine, schema, table)
  
  if engine.dialect.name == "databricks":
    return _read_table_metadata_databricks(engine, schema, table)

  insp = inspect(engine)

  cols = insp.get_columns(table_name=table, schema=schema)
  cols = _normalize_sa_columns(cols)
  pk = insp.get_pk_constraint(table_name=table, schema=schema) or {}
  fks = insp.get_foreign_keys(table_name=table, schema=schema) or []

  pk_cols = set(pk.get("constrained_columns") or [])
  fk_map = {}
  for fk in fks:
    ref_table = fk.get("referred_table")
    for c in fk.get("constrained_columns", []):
      fk_map[c] = ref_table

  return {
    "columns": cols,
    "primary_key_cols": pk_cols,
    "fk_map": fk_map,
  }


def _format_mssql_type(system_type: str, max_length: int | None, precision: int | None, scale: int | None) -> str:
  st = (system_type or "").strip().lower()

  # char/nchar: max_length is bytes (n*2 for nvarchar/nchar)
  if st in ("nvarchar", "nchar"):
    if max_length is None or max_length < 0:
      return f"{st}(max)"
    return f"{st}({int(max_length / 2)})"
  if st in ("varchar", "char", "varbinary", "binary"):
    if max_length is None or max_length < 0:
      return f"{st}(max)"
    return f"{st}({max_length})"

  if st in ("decimal", "numeric"):
    if precision is not None and scale is not None:
      return f"{st}({precision},{scale})"
    if precision is not None:
      return f"{st}({precision})"
    return st

  return st or "nvarchar(max)"


def _read_table_metadata_mssql(engine, schema: str, table: str):
  exists_sql = text("""
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = :schema AND t.name = :table
    """)

  sql = text("""
    SELECT
      c.name AS column_name,
      ut.name AS user_type_name,
      st.name AS system_type_name,
      c.max_length,
      c.precision,
      c.scale,
      c.is_nullable,
      c.collation_name,
      c.column_id
    FROM sys.columns c
    JOIN sys.tables t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    JOIN sys.types ut ON c.user_type_id = ut.user_type_id
    JOIN sys.types st ON c.system_type_id = st.system_type_id AND st.user_type_id = st.system_type_id
    WHERE s.name = :schema AND t.name = :table
    ORDER BY c.column_id
    """)

  # SQLAlchemy 2.0: use a connection
  with engine.connect() as conn:
    table_exists = conn.execute(exists_sql, {"schema": schema, "table": table}).scalar() is not None
    rows = conn.execute(sql, {"schema": schema, "table": table}).mappings().all()

  # PK/FK can still come from Inspector
  insp = inspect(engine)
  pk = insp.get_pk_constraint(table_name=table, schema=schema) or {}
  fks = insp.get_foreign_keys(table_name=table, schema=schema) or []

  pk_cols = set(pk.get("constrained_columns") or [])
  fk_map = {}
  for fk in fks:
    ref_table = fk.get("referred_table")
    for col in fk.get("constrained_columns", []):
      fk_map[col] = ref_table

  cols = []
  for r in rows:
    sys_type = _format_mssql_type(r["system_type_name"], r["max_length"], r["precision"], r["scale"])

    cols.append({
      "name": r["column_name"],
      "type": sys_type,
      "nullable": bool(r["is_nullable"]) if r["is_nullable"] is not None else None,
      "comment": None,
      "raw_type_user": r["user_type_name"],
      "raw_type_system": r["system_type_name"],
      "max_length": r["max_length"],
      "precision": r["precision"],
      "scale": r["scale"],
      "collation": r["collation_name"],
      "ordinal_position": r["column_id"],
    })

  return {
    "columns": cols,
    "primary_key_cols": pk_cols,
    "fk_map": fk_map,
    "table_exists": bool(table_exists),
  }


_TSQL_BASE_RE = re.compile(r"^([a-zA-Z0-9_]+)")

def _format_fabric_type(system_type_name: str, max_length, precision, scale) -> str:
  """Format T-SQL types for Fabric Warehouse.

  Fabric can behave surprisingly when length is omitted; we normalize:
  - VARCHAR/CHAR/VARBINARY must always have an explicit length
  - Any MAX length (-1) is normalized to a safe default (4000)
  - NVARCHAR/NCHAR lengths are stored in bytes, so we divide by 2
  """
  base = (system_type_name or "").strip().lower()
  if not base:
    return "STRING"

  # Normalize MAX to default length for Fabric.
  if max_length == -1:
    if base in ("varchar", "char", "varbinary", "nvarchar", "nchar"):
      # Keep this aligned with FabricWarehouseDialect defaults.
      max_length = 4000

  # If length is missing/zero, apply sane defaults (Fabric otherwise may infer length 1).
  if base in ("varchar", "char") and (max_length is None or max_length <= 0):
    max_length = 500
  if base == "varbinary" and (max_length is None or max_length <= 0):
    max_length = 4000

  # NVARCHAR/NCHAR max_length is bytes (2 bytes per char).
  if base in ("nvarchar", "nchar") and isinstance(max_length, int) and max_length > 0:
    max_length = max_length // 2

  # Reuse MSSQL formatting where possible, then post-normalize to ensure length is present.
  formatted = _format_mssql_type(base, max_length, precision, scale)

  # Ensure explicit lengths if formatter returned bare VARCHAR/VARBINARY/CHAR.
  upper = (formatted or "").upper()
  if "(" not in upper:
    if upper == "VARCHAR":
      return "VARCHAR(500)"
    if upper == "CHAR":
      return "CHAR(1)"
    if upper == "VARBINARY":
      return "VARBINARY(4000)"

  # If MSSQL formatter returns NVARCHAR/MAX, normalize to Fabric choices if you want:
  # e.g., avoid NVARCHAR entirely if your Fabric dialect maps strings to VARCHAR.
  if upper.startswith("NVARCHAR("):
    # Keep length but switch to VARCHAR for your dialect consistency.
    return upper.replace("NVARCHAR", "VARCHAR", 1)

  return formatted

def _read_table_metadata_fabric(engine, schema: str, table: str):
  """Fabric Warehouse: same sys.* metadata query as MSSQL, but Fabric type formatting."""
  exists_sql = text("""
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = :schema AND t.name = :table
    """)

  sql = text("""
    SELECT
      c.name AS column_name,
      ut.name AS user_type_name,
      st.name AS system_type_name,
      c.max_length,
      c.precision,
      c.scale,
      c.is_nullable,
      c.collation_name,
      c.column_id
    FROM sys.columns c
    JOIN sys.tables t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    JOIN sys.types ut ON c.user_type_id = ut.user_type_id
    JOIN sys.types st ON c.system_type_id = st.system_type_id AND st.user_type_id = st.system_type_id
    WHERE s.name = :schema AND t.name = :table
    ORDER BY c.column_id
    """)

  with engine.connect() as conn:
    table_exists = conn.execute(exists_sql, {"schema": schema, "table": table}).scalar() is not None
    rows = conn.execute(sql, {"schema": schema, "table": table}).mappings().all()

  insp = inspect(engine)
  pk = insp.get_pk_constraint(table_name=table, schema=schema) or {}
  fks = insp.get_foreign_keys(table_name=table, schema=schema) or []

  pk_cols = set(pk.get("constrained_columns") or [])
  fk_map = {}
  for fk in fks:
    ref_table = fk.get("referred_table")
    for col in fk.get("constrained_columns", []):
      fk_map[col] = ref_table

  cols = []
  for r in rows:
    sys_type = _format_fabric_type(
      r["system_type_name"], r["max_length"], r["precision"], r["scale"]
    )
    cols.append({
      "name": r["column_name"],
      "type": sys_type,
      "nullable": bool(r["is_nullable"]) if r["is_nullable"] is not None else None,
      "comment": None,
      "raw_type_user": r["user_type_name"],
      "raw_type_system": r["system_type_name"],
      "max_length": r["max_length"],
      "precision": r["precision"],
      "scale": r["scale"],
      "collation": r["collation_name"],
      "ordinal_position": r["column_id"],
    })

  return {
    "columns": cols,
    "primary_key_cols": pk_cols,
    "fk_map": fk_map,
    "table_exists": bool(table_exists),
  }


def _read_table_metadata_databricks(engine, schema: str, table: str):
  sql = f"DESCRIBE TABLE {schema}.{table}"

  with engine.connect() as conn:
    rows = conn.execute(text(sql)).fetchall()

  cols = []
  for r in rows:
    col_name = str(r[0] or "").strip()
    if not col_name:
      continue
    if col_name.startswith("#"):
      break

    data_type = str(r[1] or "").strip() if len(r) > 1 else ""
    comment = str(r[2] or "").strip() if len(r) > 2 else ""

    cols.append({
      "name": col_name,
      "type": data_type or None,
      "nullable": None,
      "comment": comment or None,
    })

  return {
    "columns": cols,
    "primary_key_cols": set(),
    "fk_map": {},
    # read_table_metadata() expects this sometimes; be explicit.
    "table_exists": True,
  }
