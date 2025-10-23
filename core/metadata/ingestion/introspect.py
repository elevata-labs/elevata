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

from sqlalchemy import inspect
from typing import Dict, Any

def read_table_metadata(engine, schema: str, table: str) -> Dict[str, Any]:
  """
  Reflection via SQLAlchemy Inspector
  """
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
