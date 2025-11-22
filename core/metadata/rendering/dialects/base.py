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

from abc import ABC, abstractmethod
from typing import Sequence

from ..expr import Expr, ColumnRef, Literal, FuncCall, Concat, Coalesce
from ..logical_plan import LogicalSelect, SourceTable, SelectItem, Join


class SqlDialect(ABC):
  """
  Base interface for SQL dialects.
  Implementations translate Expr / LogicalSelect into final SQL strings.
  """

  @abstractmethod
  def quote_ident(self, name: str) -> str:
    """
    Quote an identifier (schema, table, column) according to the dialect.
    """
    raise NotImplementedError

  @abstractmethod
  def render_expr(self, expr: Expr) -> str:
    raise NotImplementedError

  @abstractmethod
  def render_select(self, select: LogicalSelect) -> str:
    raise NotImplementedError
  
  # ---------------------------------------------------------------------------
  # Literal Rendering
  # ---------------------------------------------------------------------------
  @abstractmethod
  def render_literal(self, value) -> str:
    """
    Render a Python value as a SQL literal.
    Must handle None, bool, int, float, str, date, datetime, Decimal.
    """

  # ---------------------------------------------------------------------------
  # Type Casting
  # ---------------------------------------------------------------------------
  @abstractmethod
  def cast_expression(self, expr: str, target_type: str) -> str:
    """
    Wrap the given SQL expression in a dialect-specific CAST expression.
    """
  
  # ---------------------------------------------------------------------------
  # Capabilities (can be overridden by concrete dialects)
  # ---------------------------------------------------------------------------

  @property
  def supports_merge(self) -> bool:
    """Whether this dialect supports a native MERGE statement."""
    return True  # DuckDB, MSSQL, Snowflake etc. → True; others may override to False

  @property
  def supports_delete_detection(self) -> bool:
    """
    Whether delete detection is supported as a first-class operation
    (either via DELETE ... NOT EXISTS, DELETE USING, etc.).
    """
    return True


  # -------------------------------------------------------------------------
  # Generic helpers built on top of quote_ident
  # -------------------------------------------------------------------------

  def quote_table(self, schema: str | None, name: str) -> str:
    """
    Quote a table name with optional schema, e.g.:

      quote_table("rawcore", "customer") -> "rawcore"."customer"
      quote_table(None, "customer")      -> "customer"
    """
    name_sql = self.quote_ident(name)
    if schema:
      schema_sql = self.quote_ident(schema)
      return f"{schema_sql}.{name_sql}"
    return name_sql
  

  def literal(self, value: Any) -> str:
    """
    Render a Python value as a SQL literal in a dialect-agnostic way.
    Dialects may override this if they need special handling.
    """
    if value is None:
      return "NULL"

    if isinstance(value, bool):
      return "TRUE" if value else "FALSE"

    if isinstance(value, (int, float)):
      # For now we rely on the default string representation.
      return str(value)

    # Fallback for strings and other objects: use str() and escape single quotes.
    s = str(value)
    s = s.replace("'", "''")
    return f"'{s}'"
  

  def cast(self, expr: str, target_type: str) -> str:
    """
    Wrap an expression in a CAST(... AS ...) construct.
    Dialects may override this if they need a different syntax.
    """
    return f"CAST({expr} AS {target_type})"


  def render_table_alias(
    self,
    schema: str | None,
    name: str,
    alias: str | None,
  ) -> str:
    """
    Render a table reference including optional alias, e.g.:

      render_table_alias("rawcore", "customer", "c")
      -> "rawcore"."customer" AS c
    """
    base = self.quote_table(schema, name)
    if alias:
      return f"{base} AS {alias}"
    return base

  def render_column_list(self, columns: list[str] | None) -> str:
    """
    Render a comma-separated list of column identifiers, with proper quoting.
    If columns is None or empty, '*' is returned.
    """
    if not columns:
      return "*"
    return ", ".join(self.quote_ident(c) for c in columns)
  
  def map_python_type(self, value) -> str:
    """
    Given a Python object (or a type), return an SQL type string.
    E.g. str → VARCHAR, int → BIGINT, date → DATE.
    """
    raise NotImplementedError

  # ---------------------------------------------------------------------------
  # Expression helpers
  # ---------------------------------------------------------------------------

  @abstractmethod
  def concat_expression(self, parts: Sequence[str]) -> str:
    """
    Build a dialect-specific concatenation expression from already-rendered
    parts (each element is a SQL expression string).

    Example DuckDB:  (part1 || part2 || part3)
    Example Snowflake:  CONCAT(part1, part2, part3)
    """
    raise NotImplementedError

  def hash_expression(self, expr: str, algo: str = "sha256") -> str:
    """
    Build a dialect-specific hashing expression around `expr`.

    Default implementation raises; dialects that support hashing should
    override this method.
    """
    raise NotImplementedError(
      f"{self.__class__.__name__} does not implement hash_expression()"
    )
  

  # -------------------------------------------------------------------------
  # Load SQL helpers (full load / incremental / delete detection)
  # -------------------------------------------------------------------------

  def render_create_replace_table(self, schema: str, table: str, select_sql: str) -> str:
    """
    Optional helper: CREATE OR REPLACE TABLE schema.table AS <select>.
    Dialects that do not support CREATE OR REPLACE can override this and
    emulate the behavior (DROP+CREATE etc.).
    """
    raise NotImplementedError(f"{self.__class__.__name__} does not implement render_create_replace_table()")

  def render_insert_into_table(self, schema: str, table: str, select_sql: str) -> str:
    """
    Optional helper: INSERT INTO schema.table <select>.
    """
    raise NotImplementedError(f"{self.__class__.__name__} does not implement render_insert_into_table()")

  def render_merge_statement(
    self,
    schema: str,
    table: str,
    select_sql: str,
    unique_key_columns: list[str],
    update_columns: list[str],
  ) -> str:
    """
    Render a dialect-specific MERGE/UPSERT statement for an incremental load.
    """
    raise NotImplementedError(f"{self.__class__.__name__} does not implement render_merge_statement()")

  def render_delete_detection_statement(
    self,
    target_schema: str,
    target_table: str,
    stage_schema: str,
    stage_table: str,
    join_predicates: Sequence[str],
    scope_filter: str | None = None,
  ) -> str:
    """
    Render a DELETE statement that removes rows from the target table
    which no longer exist in the stage table.

    join_predicates: list of SQL boolean expressions representing the join
    between target (alias t) and stage (alias s), e.g.:
      ['t."customer_id" = s."customer_id"', 't."partner_id" = s."partner_id"']
    """
    raise NotImplementedError