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
from typing import Sequence, Any

from ..expr import Expr
from ..logical_plan import LogicalSelect


class SqlDialect(ABC):
  """
  Base interface for SQL dialects.
  Implementations translate Expr / LogicalSelect into final SQL strings.
  """

  def get_execution_engine(self, system) -> "BaseExecutionEngine":
    raise NotImplementedError(
      f"{self.__class__.__name__} does not provide an execution engine."
    )

  @abstractmethod
  def quote_ident(self, name: str) -> str:
    """
    Quote an identifier (schema, table, column) according to the dialect.
    """
    raise NotImplementedError


  def should_quote(self, name: str) -> bool:
    """
    Decide whether identifier must be quoted.
    Rules:
      - empty or None → quote
      - contains whitespace or not alnum/_ → quote
      - starts with digit → quote
      - all-uppercase/lowercase safe sql keywords → quote
    """
    if not name:
      return True
    if name[0].isdigit():
      return True
    if not name.replace("_", "").isalnum():
      return True
    # future: check dialect keyword lists
    return False


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
    # Dialects must explicitly opt in by overriding this property.
    return False

  @property
  def supports_delete_detection(self) -> bool:
    """
    Whether delete detection is supported as a first-class operation
    (either via DELETE ... NOT EXISTS, DELETE USING, etc.).
    """
    # Dialects must explicitly opt in by overriding this property.
    return False


  # -------------------------------------------------------------------------
  # Generic helpers built on top of quote_ident
  # -------------------------------------------------------------------------

  def render_identifier(self, name: str) -> str:
    """
    Apply quoting only when necessary.
    """
    if self.should_quote(name):
      return self.quote_ident(name)
    return name


  def render_table_identifier(self, schema: str | None, name: str) -> str:
    """
    Render a table identifier with optional schema.

      render_table_identifier("rawcore", "customer") -> rawcore.customer (quoted as needed)
      render_table_identifier(None, "customer")      -> customer (quoted as needed)
    """
    name_sql = self.render_identifier(name)
    if schema:
      schema_sql = self.render_identifier(schema)
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
    base = self.render_table_identifier(schema, name)
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
  

class BaseExecutionEngine:
  def execute(self, sql: str) -> int | None:
    raise NotImplementedError
