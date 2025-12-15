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

from sqlalchemy import inspect, text
from typing import Dict, Any

def read_table_metadata(engine, schema: str, table: str) -> Dict[str, Any]:
  """
  Reflection via SQLAlchemy Inspector, little extra work for
  MSSQL alias/user defined types like dbo.name or dbo.flag etc.
  """
  if engine.dialect.name == "mssql":
    return _read_table_metadata_mssql(engine, schema, table)

  insp = inspect(engine)

  cols = insp.get_columns(table_name=table, schema=schema)
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
  st = (system_type or "").lower()

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
      "nullable": bool(r["is_nullable"]),
      "comment": None,
      "raw_type_user": r["user_type_name"],
      "raw_type_system": r["system_type_name"],
      "max_length": r["max_length"],
      "precision": r["precision"],
      "scale": r["scale"],
      "collation": r["collation_name"],
      "ordinal_position": r["column_id"],
    })

  return {"columns": cols, "primary_key_cols": pk_cols, "fk_map": fk_map}
