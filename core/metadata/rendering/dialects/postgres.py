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

from datetime import date, datetime
from decimal import Decimal
from typing import Sequence

from .duckdb import DuckDBDialect


class PostgresDialect(DuckDBDialect):
  """
  SQL dialect for PostgreSQL.

  Compatible with elevata LogicalPlan:
  - SubquerySource
  - WindowFunction
  - RawSql templates
  """

  DIALECT_NAME = "postgres"

  # ---------------------------------------------------------
  # Identifier quoting
  # ---------------------------------------------------------
  def quote_ident(self, ident: str) -> str:
    return f"\"{ident}\""

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
      return f"TIMESTAMP '{ts}'"

    # treat everything else as string
    s = str(value).replace("'", "''")
    return f"'{s}'"

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
      return "TIMESTAMP"
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
      return f"encode(digest({expr}, 'sha256'), 'hex')"
    # Fallback still sha256
    return f"encode(digest({expr}, 'sha256'), 'hex')"

  # ---------------------------------------------------------
  # MERGE / UPSERT
  # (Using INSERT ... ON CONFLICT for broad PG compatibility)
  # ---------------------------------------------------------
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
    target_qualified = self.quote_table(schema, table)

    # ON CONFLICT uses the unique key columns
    key_list = ", ".join(self.quote_ident(c) for c in unique_key_columns)

    # Insert column order = keys first, then update columns
    all_columns = unique_key_columns + [
      c for c in update_columns if c not in unique_key_columns
    ]
    insert_col_list = ", ".join(self.quote_ident(c) for c in all_columns)

    # ON CONFLICT DO UPDATE SET <col> = EXCLUDED.<col>
    update_assignments = ", ".join(
      f"{self.quote_ident(c)} = EXCLUDED.{self.quote_ident(c)}"
      for c in update_columns
    )

    sql = (
      f"INSERT INTO {target_qualified} ({insert_col_list})\n"
      f"{select_sql}\n"
      f"ON CONFLICT ({key_list})\n"
      f"DO UPDATE SET {update_assignments};"
    )

    return sql.strip()
