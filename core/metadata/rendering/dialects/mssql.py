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

import datetime
from decimal import Decimal
from typing import Sequence

from .duckdb import DuckDBDialect


class MssqlDialect(DuckDBDialect):
  """
  SQL Server / T-SQL dialect.

  We subclass DuckDBDialect to reuse:
    - expression rendering (ColumnRef, WindowFunction, RawSql, ...)
    - LogicalSelect rendering
    - MERGE / delete detection skeletons

  MSSQL-specific behaviour:
    - Different type mapping
    - String concatenation with +
    - HASHBYTES for hashing
    - Booleans as 1 / 0
    - DATE / DATETIME2 literals via CAST(...)
    - CREATE OR REPLACE emulated via DROP + SELECT INTO
  """

  DIALECT_NAME = "mssql"


  # ---------------------------------------------------------------------------
  # Capabilities
  # ---------------------------------------------------------------------------

  @property
  def supports_merge(self) -> bool:
    """SQL Server supports native MERGE statements."""
    return True

  @property
  def supports_delete_detection(self) -> bool:
    """Delete detection is implemented via DELETE + NOT EXISTS."""
    return True

  # ---------------------------------------------------------------------------
  # Identifier quoting
  # ---------------------------------------------------------------------------

  def quote_ident(self, name: str) -> str:
    """
    Quote identifiers with double quotes.

    Note:
      SQL Server supports QUOTED_IDENTIFIER and accepts "name" as identifier
      quoting style (similar to Postgres / DuckDB).
    """
    escaped = name.replace('"', '""')
    return f'"{escaped}"'

  # ---------------------------------------------------------------------------
  # Type mapping
  # ---------------------------------------------------------------------------

  def map_logical_type(
    self,
    logical_type,          # type: str
    max_length=None,       # type: int | None
    precision=None,        # type: int | None
    scale=None,            # type: int | None
  ):
    """
    Map a logical elevata datatype string to a SQL Server type string.
    """
    if not logical_type:
      return None

    t = str(logical_type).lower()

    if t in ("string", "text", "varchar", "char"):
      if max_length:
        return f"VARCHAR({max_length})"
      return "VARCHAR(255)"

    if t in ("int", "integer", "int32"):
      return "INT"

    if t in ("bigint", "int64", "long"):
      return "BIGINT"

    if t in ("decimal", "numeric"):
      if precision and scale is not None:
        return f"DECIMAL({precision}, {scale})"
      if precision:
        return f"DECIMAL({precision})"
      return "DECIMAL(18, 2)"

    if t in ("float", "double"):
      return "FLOAT"

    if t in ("bool", "boolean"):
      return "BIT"

    if t in ("date",):
      return "DATE"

    if t in ("datetime", "timestamp", "timestamptz"):
      return "DATETIME2"

    # already concrete database type
    return logical_type

  # ---------------------------------------------------------
  # Concatenation
  # ---------------------------------------------------------
  def concat_expression(self, parts: Sequence[str]) -> str:
    """
    SQL Server uses + for string concatenation.

    We keep it simple and assume the inputs are already string-like.    
    """
    if not parts:
      return "''"
    return "(" + " + ".join(parts) + ")"

  # ---------------------------------------------------------
  # Hash expression
  # ---------------------------------------------------------
  def hash_expression(self, expr: str, algo: str = "sha256") -> str:
    """
    SQL Server: map HASH256 to HASHBYTES('SHA2_256', ...),
    and convert to a hex string.
    """
    algo_lower = algo.lower()
    if algo_lower in ("sha256", "hash256"):
      return f"CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', {expr}), 2)"
    # Fallback: still SHA2_256
    return f"CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', {expr}), 2)"

  # ---------------------------------------------------------------------------
  # Literal rendering
  # ---------------------------------------------------------------------------

  def render_literal(self, value):
    if value is None:
      return "NULL"

    if isinstance(value, bool):
      # SQL Server has BIT, but no TRUE/FALSE literals
      return "1" if value else "0"

    if isinstance(value, (int, float, Decimal)):
      return str(value)

    if isinstance(value, str):
      escaped = value.replace("'", "''")
      return f"'{escaped}'"

    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
      iso = value.isoformat()
      return f"CAST('{iso}' AS DATE)"

    if isinstance(value, datetime.datetime):
      # Strip microseconds for a cleaner literal
      dt = value.replace(microsecond=0)
      iso = dt.isoformat(sep=" ")
      return f"CAST('{iso}' AS DATETIME2)"

    raise TypeError(f"Unsupported literal type for MssqlDialect: {type(value)}")

  # ---------------------------------------------------------------------------
  # CAST
  # ---------------------------------------------------------------------------

  def cast_expression(self, expr: str, target_type: str) -> str:
    return f"CAST({expr} AS {target_type})"

  # ---------------------------------------------------------------------------
  # Load helpers (full load / incremental)
  # ---------------------------------------------------------------------------

  def render_create_replace_table(self, schema: str, table: str, select_sql: str) -> str:
    """
    Emulate CREATE OR REPLACE TABLE via DROP IF EXISTS + SELECT INTO.

    Note:
      OBJECT_ID('<schema>.<table>', 'U') is used to detect existing tables.
    """
    full = self.render_table_identifier(schema, table)

    return (
      f"IF OBJECT_ID('{full}', 'U') IS NOT NULL\n"
      f"  DROP TABLE {full};\n"
      f"SELECT * INTO {full}\n"
      f"FROM (\n{select_sql}\n) AS src;"
    )

  def render_insert_into_table(self, schema: str, table: str, select_sql: str) -> str:
    """
    Standard INSERT INTO ... <select> for SQL Server.
    """
    full = self.render_table_identifier(schema, table)
    return f"INSERT INTO {full}\n{select_sql}"
