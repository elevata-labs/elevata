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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Sequence, Any, Dict, Optional

import datetime
from decimal import Decimal

from ..expr import (
  Cast,
  Concat,
  Coalesce,
  ColumnRef,
  Expr,
  FuncCall,
  Literal,
  OrderByExpr,
  OrderByClause,
  RawSql,
  WindowFunction,
  WindowSpec,
)
from ..logical_plan import Join, LogicalSelect, LogicalUnion, SelectItem, SourceTable, SubquerySource

from metadata.system.introspection import read_table_metadata
from metadata.materialization.logging import LOAD_RUN_SNAPSHOT_REGISTRY

class BaseExecutionEngine:
  def execute(self, sql: str) -> int | None:
    raise NotImplementedError
  
  def execute_many(self, sql: str, params_seq) -> int | None:
    """
    Optional bulk execution for parameterized statements.
    Dialects/engines should override if they support executemany().
    """
    raise NotImplementedError

  def execute_scalar(self, sql: str):
    """
    Optional: execute a SELECT returning a single scalar value (first column of first row).
    Engines should override if they support fetching results.
    """
    raise NotImplementedError

  def fetch_all(self, sql: str) -> list[tuple]:
    """
    Optional: execute a SELECT and return all rows as tuples.
    Engines should override if they support fetching results.
    """
    raise NotImplementedError
  

class SqlDialect(ABC):
  """
  Base interface for SQL dialects.
  Implementations translate Expr / LogicalSelect into final SQL strings.
  """

  # ---------------------------------------------------------------------------
  # 1. Class meta / capabilities
  # ---------------------------------------------------------------------------
  DIALECT_NAME = "base"

  @dataclass(frozen=True)
  class FunctionSpec:
    name: str
    kind: str  # "scalar" | "aggregate" | "window"

  BASE_FUNCTION_REGISTRY = {
    # Aggregates
    "SUM": FunctionSpec("SUM", "aggregate"),
    "COUNT": FunctionSpec("COUNT", "aggregate"),
    "COUNT_DISTINCT": FunctionSpec("COUNT_DISTINCT", "aggregate"),
    "MIN": FunctionSpec("MIN", "aggregate"),
    "MAX": FunctionSpec("MAX", "aggregate"),
    "AVG": FunctionSpec("AVG", "aggregate"),
    "STRING_AGG": FunctionSpec("STRING_AGG", "aggregate"),

    # Windows (already in AST: WindowFunction, but FuncCall can still appear)
    "ROW_NUMBER": FunctionSpec("ROW_NUMBER", "window"),
  }

  @property
  def supports_merge(self) -> bool:
    """Whether this dialect supports a native MERGE statement."""
    # Dialects must explicitly opt in by overriding this property.
    return False
  

  @property
  def supports_alter_column_type(self) -> bool:
    """
    Whether this dialect supports altering an existing column's physical type
    without rebuilding the table.
    Default: False (fail closed).
    """
    return False


  @property
  def supports_delete_detection(self) -> bool:
    """
    Whether delete detection is supported as a first-class operation
    (either via DELETE ... NOT EXISTS, DELETE USING, etc.).
    """
    # Dialects must explicitly opt in by overriding this property.
    return False

  def get_execution_engine(self, system) -> "BaseExecutionEngine":
    raise NotImplementedError(
      f"{self.__class__.__name__} does not provide an execution engine."
    )

  # ---------------------------------------------------------------------------
  # 2. Identifier & quoting
  # ---------------------------------------------------------------------------
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

  # ---------------------------------------------------------------------------
  # 3. Types
  # ---------------------------------------------------------------------------
  # Dialects may extend/override via TYPE_ALIASES on the class.
  TYPE_ALIASES: dict[str, str] = {}

  # Canonical type tokens used across dialects (string constants, intentionally simple).
  # NOTE: Dialect renderers typically compare via .upper(), so returning these tokens works.
  _DEFAULT_TYPE_ALIASES: dict[str, str] = {
    # String
    "STRING": "STRING",
    "TEXT": "STRING",
    "VARCHAR": "STRING",
    "CHAR": "STRING",
    "NVARCHAR": "STRING",
    "NCHAR": "STRING",

    # Int
    "INT": "INTEGER",
    "INTEGER": "INTEGER",
    "INT32": "INTEGER",
    "SMALLINT": "INTEGER",

    # Big int
    "BIGINT": "BIGINT",
    "INT64": "BIGINT",
    "LONG": "BIGINT",

    # Decimal / numeric
    "DECIMAL": "DECIMAL",
    "NUMERIC": "DECIMAL",

    # Float
    "FLOAT": "FLOAT",
    "DOUBLE": "FLOAT",
    "FLOAT64": "FLOAT",

    # Bool
    "BOOL": "BOOLEAN",
    "BOOLEAN": "BOOLEAN",

    # Date/time
    "DATE": "DATE",
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP",
    "DATETIME": "TIMESTAMP",
    "TIMESTAMPTZ": "TIMESTAMP",

    # Binary
    "BINARY": "BINARY",
    "BYTES": "BINARY",

    # Others
    "UUID": "UUID",
    "JSON": "JSON",
  }

  def canonicalize_logical_type(self, logical_type, *, strict: bool = True) -> str:
    """
    Convert an input logical type (TargetColumn.datatype or user-specified alias)
    into a canonical type token (e.g. STRING, INTEGER, BIGINT, DECIMAL, FLOAT...).

    - Does NOT apply dialect-specific physical naming.
    - Uses a shared default alias map and allows dialect-specific extensions.
    """
    lt = (logical_type or "").strip().upper()
    alias_map = dict(self._DEFAULT_TYPE_ALIASES)
    alias_map.update(getattr(self, "TYPE_ALIASES", None) or {})

    canonical = alias_map.get(lt)
    if canonical is None:
      if strict:
        raise ValueError(f"Unsupported logical type: {logical_type!r}")
      return lt
    return canonical

  def map_logical_type(
    self,
    *,
    datatype: str,
    max_length=None,
    precision=None,
    scale=None,
    strict: bool = True,
  ) -> str:
    canonical = self.canonicalize_logical_type(datatype, strict=strict)
    return self.render_physical_type(
      canonical=canonical,
      max_length=max_length,
      precision=precision,
      scale=scale,
      strict=strict,
    )

  @abstractmethod
  def render_physical_type(
    self,
    *,
    canonical: str,
    max_length=None,
    precision=None,
    scale=None,
    strict: bool = True,
  ) -> str:
    """
    Render a dialect-specific physical SQL type string from a canonical token.
    Example:
      canonical=STRING, max_length=50 -> VARCHAR(50) / STRING / NVARCHAR(50)
    """
    raise NotImplementedError

  # ---------------------------------------------------------------------------
  # 4. DDL helpers
  # ---------------------------------------------------------------------------
  @abstractmethod
  def render_create_schema_if_not_exists(self, schema: str) -> str:
    """
    Return DDL that creates the given schema if it does not exist.
    Must be idempotent and safe to run multiple times.
    """
    raise NotImplementedError(
      f"{self.__class__.__name__} does not implement render_create_schema_if_not_exists()"
    )
  
  def render_create_table_if_not_exists(self, td) -> str:
    """
    Default implementation: TargetDataset/TargetColumn -> column list
    and delegate to render_create_table_if_not_exists_from_columns().
    Dialects override render_create_table_if_not_exists_from_columns() if needed.
    """
    schema_name = td.target_schema.schema_name
    table_name = td.target_dataset_name

    columns: list[dict[str, object]] = []
    for c in self._iter_target_columns(td):
      max_length = getattr(c, "max_length", None)
      precision = getattr(c, "precision", None)
      if precision is None:
        precision = getattr(c, "decimal_precision", None)
      scale = getattr(c, "scale", None)
      if scale is None:
        scale = getattr(c, "decimal_scale", None)

      col_type = self.map_logical_type(
        datatype=c.datatype,
        max_length=max_length,
        precision=precision,
        scale=scale,
        strict=True,
      )
      columns.append({
        "name": c.target_column_name,
        "type": col_type,
        "nullable": bool(getattr(c, "nullable", True)),
      })

    return self.render_create_table_if_not_exists_from_columns(
      schema=schema_name,
      table=table_name,
      columns=columns,
    )

  def _iter_target_columns(self, td) -> list[Any]:
    cols_obj = getattr(td, "target_columns", None)
    if cols_obj is None:
      return []
    if hasattr(cols_obj, "all"):
      try:
        qs = cols_obj.all()
        if hasattr(qs, "order_by"):
          return list(qs.order_by("ordinal_position"))
        return list(qs)
      except Exception:
        pass
    try:
      cols = list(cols_obj)
      cols.sort(key=lambda c: getattr(c, "ordinal_position", 0) or 0)
      return cols
    except Exception:
      return []

  def render_create_table_if_not_exists_from_columns(
    self,
    *,
    schema: str,
    table: str,
    columns: list[dict[str, object]],
  ) -> str:
    """
    Render CREATE TABLE IF NOT EXISTS <schema>.<table> ( ... ) from a simple column list.
    Dialects may override if they don't support CREATE TABLE IF NOT EXISTS.
    """
    target = self.render_table_identifier(schema, table)
    col_defs: list[str] = []
    for c in columns:
      name = self.render_identifier(str(c["name"]))
      ctype = str(c["type"])
      nullable = bool(c.get("nullable", True))
      null_sql = "NULL" if nullable else "NOT NULL"
      col_defs.append(f"{name} {ctype} {null_sql}")
    cols_sql = ",\n  ".join(col_defs)
    return f"CREATE TABLE IF NOT EXISTS {target} (\n  {cols_sql}\n)"
  

  def render_create_table_from_columns(
    self,
    *,
    schema: str,
    table: str,
    columns: list[dict[str, object]],
  ) -> str:
    """
    Render CREATE TABLE <schema>.<table> (...) without IF NOT EXISTS.
    Used for deterministic rebuild flows (temp tables).
    """
    target = self.render_table_identifier(schema, table)
    col_defs: list[str] = []
    for c in columns:
      name = self.render_identifier(str(c["name"]))
      ctype = str(c["type"])
      nullable = bool(c.get("nullable", True))
      null_sql = "NULL" if nullable else "NOT NULL"
      col_defs.append(f"{name} {ctype} {null_sql}")
    cols_sql = ",\n  ".join(col_defs)
    return f"CREATE TABLE {target} (\n  {cols_sql}\n)"


  def render_insert_select_for_rebuild(
    self,
    *,
    schema: str,
    src_table: str,
    dst_table: str,
    columns: list[dict[str, object]],
    lossy_casts: bool = False,
    truncate_strings: bool = False,
  ) -> str:
    """
    Backfill dst_table from src_table using CAST to the desired physical type.
    Default should work for most ANSI-ish engines.
    """
    src = self.render_table_identifier(schema, src_table)
    dst = self.render_table_identifier(schema, dst_table)

    col_names: list[str] = []
    select_exprs: list[str] = []
    for c in (columns or []):
      # Defensive guards: planner should always provide these, but don't crash with KeyError.
      name = str((c or {}).get("name") or "").strip()
      ctype = str((c or {}).get("type") or "").strip()
      if not name or not ctype:
        # Skip invalid column payload entries deterministically.
        continue

      # "name" is the desired column name (destination),
      # "source_name" is the physical source column name in the existing table (optional).
      col = self.render_identifier(name)
      src_col_name = (c.get("source_name") or c.get("name") or "").strip()
      if not src_col_name:
        # fall back defensively to destination name
        src_col_name = str(c["name"])
      src_col = self.render_identifier(src_col_name)
      expr = self.cast_expression(src_col, ctype) if lossy_casts else src_col

      # Optional lossy string shrink support (only when explicitly enabled)
      if truncate_strings:
        # Only truncate when planner explicitly marked this column as shrinking.
        ml = (c or {}).get("truncate_to_length")

        if ml is not None:
          try:
            # Only apply truncation to string-ish physical types (extra safety).
            ctype_u = ctype.upper()
            is_stringish = any(tok in ctype_u for tok in ("CHAR", "STRING", "TEXT", "VARCHAR"))
            if is_stringish:
              expr = self.truncate_string_expression(expr, int(ml))
          except Exception:
            pass

      col_names.append(col)
      select_exprs.append(f"{expr} AS {col}")

    cols_sql = ", ".join(col_names)
    sel_sql = ", ".join(select_exprs)
    return f"INSERT INTO {dst} ({cols_sql}) SELECT {sel_sql} FROM {src};"


  def render_create_or_replace_view(
    self,
    *,
    schema: str,
    view: str,
    select_sql: str,
  ) -> str:
    target = self.render_table_identifier(schema, view)
    return f"CREATE OR REPLACE VIEW {target} AS\n{select_sql}"
  

  def render_add_column(self, schema: str, table: str, column: str, column_type: str | None) -> str:
    """
    Default ADD COLUMN DDL. Dialects can override if needed.
    """
    if not column_type:
      # Fail closed: planner should block if type is missing.
      return ""

    # Use dialect-safe identifier rendering.
    tbl = self.render_table_identifier(schema, table)
    col = self.render_identifier(column)
    return f"ALTER TABLE {tbl} ADD COLUMN {col} {column_type}"


  def render_alter_column_type(
    self,
    *,
    schema: str,
    table: str,
    column: str,
    new_type: str,
  ) -> str:
    """
    Render DDL to change a column's physical type.
    Default: unsupported -> return empty string (planner must rebuild instead).
    """
    return ""


  def render_drop_table(self, *, schema: str, table: str, cascade: bool = False) -> str:
    """
    Drop a table (no IF EXISTS).
    Used for rebuild workflows where the planner has already proven existence.
    Default ignores cascade unless overridden by dialects that support it.
    """
    target = self.render_table_identifier(schema, table)
    return f"DROP TABLE {target}"


  def render_drop_table_if_exists(self, *, schema: str, table: str, cascade: bool = False) -> str:
    """
    Drop a table if it exists.

    - Default: no CASCADE (dialects may override).
    - cascade flag is supported for engines like Postgres.
    """
    # Default ignores cascade unless overridden by a dialect.
    target = self.render_table_identifier(schema, table)
    return f"DROP TABLE IF EXISTS {target}"

  def render_truncate_table(self, *, schema: str, table: str) -> str:
    """
    Default implementation: DELETE FROM (safe, widely supported).
    Dialects can override with TRUNCATE TABLE for performance.
    """
    full = self.render_table_identifier(schema, table)
    return f"DELETE FROM {full};"
  
  def render_rename_table(self, schema: str, old_table: str, new_table: str) -> str:
    """
    Default table rename (works for DuckDB/Postgres/BigQuery-style dialects):
      ALTER TABLE <schema>.<old> RENAME TO <new>

    Note: new_table is intentionally unqualified (no schema prefix).
    """
    old_full = self.render_table_identifier(schema, old_table)
    new_name = self.render_identifier(new_table)
    return f"ALTER TABLE {old_full} RENAME TO {new_name}"

  def render_rename_column(self, schema: str, table: str, old: str, new: str) -> str:
    # Default ANSI-ish
    return (
      f"ALTER TABLE {self.render_table_identifier(schema, table)} "
      f"RENAME COLUMN {self.render_identifier(old)} TO {self.render_identifier(new)}"
    )

  # ---------------------------------------------------------------------------
  # 5. DML / load SQL primitives
  # ---------------------------------------------------------------------------
  def render_insert_into_table(
    self,
    schema_name: str,
    table_name: str,
    select_sql: str,
    *,
    target_columns: list[str] | None = None,
  ) -> str:
    """
    Default: INSERT INTO <schema>.<table> [(col, ...)] <select>.
    """
    table = self.render_table_identifier(schema_name, table_name)
    if target_columns:
      cols = ", ".join(self.render_identifier(c) for c in target_columns)
      return f"INSERT INTO {table} ({cols})\n{select_sql}"
    return f"INSERT INTO {table}\n{select_sql}"
  
  # ---------------------------------------------------------------------------
  # Historization (SCD Type 2) SQL rendering
  # ---------------------------------------------------------------------------
  def render_hist_changed_update_sql(
    self,
    *,
    schema_name: str,
    hist_table: str,
    rawcore_table: str,
  ) -> str:
    """
    Close changed rows in the history table (SCD2):
    - match on business key (rawcore surrogate key)
    - detect changes via row_hash
    - set version_ended_at/version_state/load_run_id

    Default implementation assumes ANSI-ish UPDATE ... FROM syntax.
    Dialects that require T-SQL style UPDATE <alias> ... FROM ... should override.
    """
    hist_tbl = self.render_table_identifier(schema_name, hist_table)
    rc_tbl = self.render_table_identifier(schema_name, rawcore_table)

    sk_name = self.render_identifier(f"{rawcore_table}_key")
    row_hash = self.render_identifier("row_hash")

    return (
      f"UPDATE {hist_tbl} AS h\n"
      "SET\n"
      "  version_ended_at = {{ load_timestamp }},\n"
      "  version_state    = 'changed',\n"
      "  load_run_id      = {{ load_run_id }}\n"
      "WHERE h.version_ended_at IS NULL\n"
      "  AND EXISTS (\n"
      "    SELECT 1\n"
      f"    FROM {rc_tbl} AS r\n"
      f"    WHERE r.{sk_name} = h.{sk_name}\n"
      f"      AND r.{row_hash} <> h.{row_hash}\n"
      "  );"
    )

  def render_hist_delete_sql(
    self,
    *,
    schema_name: str,
    hist_table: str,
    rawcore_table: str,
  ) -> str:
    """
    Mark deleted business keys in the history table (SCD2):

    If a key is present in history (active row), but no longer exists in rawcore,
    we close the active version and mark it as deleted.

    Default implementation assumes ANSI-ish UPDATE ... FROM syntax.
    Dialects that require T-SQL style UPDATE <alias> ... FROM ... should override.
    """
    hist_tbl = self.render_table_identifier(schema_name, hist_table)
    rc_tbl = self.render_table_identifier(schema_name, rawcore_table)

    sk_name = self.render_identifier(f"{rawcore_table}_key")

    return (
      f"UPDATE {hist_tbl} AS h\n"
      "SET\n"
      "  version_ended_at = {{ load_timestamp }},\n"
      "  version_state    = 'deleted',\n"
      "  load_run_id      = {{ load_run_id }}\n"
      "WHERE h.version_ended_at IS NULL\n"
      "  AND NOT EXISTS (\n"
      "    SELECT 1\n"
      f"    FROM {rc_tbl} AS r\n"
      f"    WHERE r.{sk_name} = h.{sk_name}\n"
      "  );"
    )


  def render_hist_insert_statement(
    self,
    *,
    hist_schema: str,
    hist_table: str,
    hist_columns_sql: list[str],
    source_schema: str,
    source_table: str,
    source_alias: str,
    select_exprs_sql: list[str],
    exists_schema: str,
    exists_table: str,
    exists_alias: str,
    exists_predicates: list[str],
    exists_negated: bool,
  ) -> str:
    """Render an INSERT ... SELECT guarded by (NOT) EXISTS.

    elevata convention:
      - load_sql supplies semantic ingredients (already-rendered identifiers and predicates)
      - dialect owns the SQL shape (and can override this method if needed)
    """
    hist_fqn = self.render_table_identifier(hist_schema, hist_table)
    src_fqn = self.render_table_identifier(source_schema, source_table)
    ex_fqn = self.render_table_identifier(exists_schema, exists_table)

    exists_kw = "NOT EXISTS" if exists_negated else "EXISTS"
    preds = [p for p in (exists_predicates or []) if (p or "").strip()]
    pred_sql = "\n    AND ".join(preds) if preds else "1=1"

    cols_sql = ",\n  ".join(hist_columns_sql)
    exprs_sql = ",\n  ".join(select_exprs_sql)

    return (
      f"INSERT INTO {hist_fqn} (\n"
      f"  {cols_sql}\n"
      f")\n"
      "SELECT\n"
      f"  {exprs_sql}\n"
      f"FROM {src_fqn} AS {source_alias}\n"
      f"WHERE {exists_kw} (\n"
      "  SELECT 1\n"
      f"  FROM {ex_fqn} AS {exists_alias}\n"
      f"  WHERE {pred_sql}\n"
      ");"
    )


  def render_hist_incremental_statement(
    self,
    *,
    schema_name: str,
    hist_table: str,
    rawcore_table: str,
    include_comment: bool,
    include_inserts: bool,
    changed_insert_kwargs: dict | None,
    new_insert_kwargs: dict | None,
  ) -> str:
    """Render the full SCD2 incremental pipeline for a *_hist dataset.

    The default orchestration is:
      1) close changed versions (UPDATE)
      2) mark deleted business keys (UPDATE)
      3) insert new versions for changed keys (INSERT..SELECT..EXISTS) [optional]
      4) insert first versions for new keys (INSERT..SELECT..NOT EXISTS) [optional]
    """
    parts: list[str] = []

    if include_comment:
      parts.append(
        f"-- History load for {schema_name}.{hist_table} (SCD Type 2).\n"
        f"-- Real SQL for new, changed and deleted business keys follows below.\n"
      )

    parts.append(
      self.render_hist_changed_update_sql(
        schema_name=schema_name,
        hist_table=hist_table,
        rawcore_table=rawcore_table,
      )
    )
    parts.append("")
    parts.append(
      self.render_hist_delete_sql(
        schema_name=schema_name,
        hist_table=hist_table,
        rawcore_table=rawcore_table,
      )
    )

    if include_inserts:
      if changed_insert_kwargs:
        parts.append("")
        parts.append(self.render_hist_insert_statement(**changed_insert_kwargs))
      if new_insert_kwargs:
        parts.append("")
        parts.append(self.render_hist_insert_statement(**new_insert_kwargs))

    sql = "\n".join([p for p in parts if p is not None]).rstrip()
    return sql + "\n"


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
    Render an UPSERT / merge statement for this dialect.

    This is a *dialect responsibility* primitive: elevata's load layer supplies
    only the semantic ingredients (source SELECT + column lists), while the
    dialect decides the SQL shape (native MERGE, INSERT..ON CONFLICT, or a
    performance-oriented fallback like UPDATE+INSERT).

    Parameters:
      target_fqn:
        Fully-qualified target table identifier, already rendered (quoted) for
        this dialect (e.g. schema.table, or project.dataset.table).
      source_select_sql:
        A SELECT statement that yields columns whose names match `insert_columns`
        (or a superset). It must not end with a trailing semicolon.
      key_columns:
        Non-empty list of target column names used for matching (business key).
      update_columns:
        Target column names to update on match (typically non-key columns).
      insert_columns:
        Target column names to insert for new rows (stable order).
      target_alias / source_alias:
        Aliases used in the rendered statement.

    Semantics:
      - Updates existing rows matched on key_columns.
      - Inserts missing rows.
      - Deterministic output for deterministic inputs.

    Default behavior:
      - If supports_merge is True: render an ANSI-ish MERGE statement.
      - Otherwise: render an UPDATE ... FROM + INSERT ... WHERE NOT EXISTS fallback.
      - Dialects should override for idiomatic/performance-optimized SQL.
    """
    keys = [c for c in (key_columns or []) if c]
    if not keys:
      raise ValueError("render_merge_statement requires non-empty key_columns")

    insert_cols = [c for c in (insert_columns or []) if c]
    if not insert_cols:
      # Minimal insert set: keys + update columns, stable order.
      seen = set()
      insert_cols = []
      for c in keys + list(update_columns or []):
        if c and c not in seen:
          seen.add(c)
          insert_cols.append(c)

    updates = [c for c in (update_columns or []) if c and c not in set(keys)]

    q = self.render_identifier
    tgt = str(target_fqn).strip()
    src = f"(\n{source_select_sql.strip()}\n) AS {q(source_alias)}"

    on_pred = " AND ".join(
      [f"{q(target_alias)}.{q(k)} = {q(source_alias)}.{q(k)}" for k in keys]
    )

    if self.supports_merge:
      parts: list[str] = []
      parts.append(
        f"MERGE INTO {tgt} AS {q(target_alias)}\n"
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

    # Fallback: UPDATE then INSERT (anti-join)
    update_sql = ""
    if updates:
      set_sql = ", ".join(
        [f"{q(target_alias)}.{q(c)} = {q(source_alias)}.{q(c)}" for c in updates]
      )
      update_sql = (
        f"UPDATE {tgt} AS {q(target_alias)}\n"
        f"SET {set_sql}\n"
        f"FROM {src}\n"
        f"WHERE {on_pred};"
      )

    insert_cols_sql = ", ".join([q(c) for c in insert_cols])
    select_cols_sql = ", ".join([f"{q(source_alias)}.{q(c)}" for c in insert_cols])
    insert_sql = (
      f"INSERT INTO {tgt} ({insert_cols_sql})\n"
      f"SELECT {select_cols_sql}\n"
      f"FROM {src}\n"
      f"WHERE NOT EXISTS (\n"
      f"  SELECT 1\n"
      f"  FROM {tgt} AS {q(target_alias)}\n"
      f"  WHERE {on_pred}\n"
      f");"
    )

    if update_sql:
      return f"{update_sql}\n\n{insert_sql}".strip()
    return insert_sql.strip()


  def render_delete_detection_statement(
    self,
    target_schema,
    target_table,
    stage_schema,
    stage_table,
    join_predicates,
    scope_filter=None,
  ):
    """
    Default delete-detection implementation using DELETE + NOT EXISTS.

    join_predicates are strings like: "t.key = s.key"
    scope_filter (optional) is a full boolean expression string.
    """
    target = self.render_table_identifier(target_schema, target_table)
    stage = self.render_table_identifier(stage_schema, stage_table)

    join_sql = " AND ".join(join_predicates) if join_predicates else "1=1"

    conditions: list[str] = []
    if scope_filter:
      # scope_filter is already a boolean expression
      conditions.append(f"({scope_filter})")

    conditions.append(
      "NOT EXISTS (\n"
      "  SELECT 1\n"
      f"  FROM {stage} AS s\n"
      f"  WHERE {join_sql}\n"
      ")"
    )

    where_sql = "\n  AND ".join(conditions)

    sql = (
      f"DELETE FROM {target} AS t\n"
      f"WHERE {where_sql}"
    )

    # Always terminate for safe multi-statement execution (e.g., DELETE + MERGE).
    return sql.rstrip() + ";"
  
  
  def render_insert_load_run_log(self, *, meta_schema: str, values: dict[str, object]) -> str | None:
    """
    Render an INSERT INTO meta.load_run_log (...) VALUES (...) statement.

    v0.8.0: schema is canonical via LOAD_RUN_LOG_REGISTRY and the caller passes
    a fully normalized `values` dict (same keys/order as the canonical schema).
    Dialects must only render identifiers and literals safely.
    """
    raise NotImplementedError(f"{self.__class__.__name__} does not implement render_insert_load_run_log()")

  def _literal_for_meta_insert(self, *, table: str, column: str, value: object) -> str:
    """
    Hook for meta-table inserts.
    Default delegates to dialect.literal(); override in dialects if needed.
    """
    return self.literal(value)

  def render_insert_load_run_snapshot(self, *, meta_schema: str, values: dict[str, object]) -> str:
    """
    Generic INSERT for meta.load_run_snapshot using registry-order columns.
    """
    tbl = self.render_table_identifier(meta_schema, "load_run_snapshot")
    cols = list(LOAD_RUN_SNAPSHOT_REGISTRY.keys())

    col_sql = ",\n        ".join(self.render_identifier(c) for c in cols)
    val_sql = ",\n        ".join(
      self._literal_for_meta_insert(table="load_run_snapshot", column=c, value=values.get(c))
      for c in cols
    )

    sql = f"""
      INSERT INTO {tbl} (
        {col_sql}
      )
      VALUES (
        {val_sql}
      );
    """.strip()

    return sql

  def param_placeholder(self) -> str:
    """
    Placeholder for parameterized SQL statements used by the dialect's execution engine.
    Default matches DB-API qmark style (DuckDB, pyodbc).
    """
    return "?"

  # ---------------------------------------------------------------------------
  # 6. Expression / Select renderer
  # ---------------------------------------------------------------------------
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

  def render_literal(self, value) -> str:
    """
    ANSI-ish default literals. Dialects override where necessary.
    """
    if value is None:
      return "NULL"
    if isinstance(value, bool):
      return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, Decimal)):
      return str(value)
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
      return f"DATE '{value.isoformat()}'"
    if isinstance(value, datetime.datetime):
      # ISO without timezone normalization (dialects can override)
      return f"TIMESTAMP '{value.isoformat(sep=' ', timespec='microseconds')}'"
    # strings
    s = str(value).replace("'", "''")
    return f"'{s}'"

  def render_expr(self, expr: Expr) -> str:
    """
    Default "ANSI-ish" expression renderer.
    Dialects may override for engine-specific functions or syntax.
    """
    if isinstance(expr, Literal):
      return self.render_literal(expr.value)

    if isinstance(expr, ColumnRef):
      if expr.table_alias:
        return f"{self.render_identifier(expr.table_alias)}.{self.render_identifier(expr.column_name)}"
      return self.render_identifier(expr.column_name)

    if isinstance(expr, RawSql):
      # ------------------------------------------------------------
      # RawSql supports two "modes" seen in the codebase:
      #
      # 1) Structured template: expr.template = [str | Expr, ...]
      # 2) DuckDB-style string template:
      #    - replace "{alias}" via default_table_alias
      #    - replace "{expr:<name>}" via is_template + expr_bindings
      # ------------------------------------------------------------
      if getattr(expr, "template", None):
        parts: list[str] = []
        for p in expr.template:
          if isinstance(p, str):
            parts.append(p)
          else:
            parts.append(self.render_expr(p))
        return "".join(parts)

      sql = str(getattr(expr, "sql", ""))

      # DuckDB-style: replace {alias}
      default_alias = getattr(expr, "default_table_alias", None)
      if default_alias:
        sql = sql.replace("{alias}", str(default_alias))

      # DuckDB-style: replace {expr:<name>} via expr_bindings
      if getattr(expr, "is_template", False) and getattr(expr, "expr_bindings", None):
        import re

        def repl_expr(match: re.Match) -> str:
          key = match.group(1)
          bound = expr.expr_bindings.get(key)
          if bound is None:
            raise ValueError(
              f"Missing expr_binding for {key} in RawSql template: {getattr(expr, 'sql', '')}"
            )
          return self.render_expr(bound)

        sql = re.sub(r"\{expr:([A-Za-z0-9_]+)\}", repl_expr, sql)

      return sql

    if isinstance(expr, Cast):
      inner = self.render_expr(expr.expr)
      tt = getattr(expr, "target_type", None)
      if tt is None:
        # fail-safe
        return self.cast_expression(inner, "varchar")

      # Map logical -> physical type for this dialect.
      # Postgres e.g. doesn't support "string" as a type name.
      db_type = self.map_logical_type(datatype=str(tt), strict=False)
      if not db_type:
        # DuckDB behavior: if we can't map, do not emit a CAST.
        return inner
      return self.cast_expression(inner, db_type)

    if isinstance(expr, Coalesce):
      args_sql = ", ".join(self.render_expr(p) for p in expr.parts)
      return f"COALESCE({args_sql})"

    if isinstance(expr, Concat):
      rendered_parts = [self.render_expr(p) for p in expr.parts]
      return self.concat_expression(rendered_parts)
    
    if isinstance(expr, OrderByExpr):
      dir_sql = (expr.direction or "ASC").strip().upper()
      if dir_sql not in ("ASC", "DESC"):
        raise ValueError(f"Invalid ORDER BY direction: {dir_sql}")
      inner_sql = self.render_expr(expr.expr)
      return f"{inner_sql} {dir_sql}"

    if isinstance(expr, OrderByClause):
      if not expr.items:
        raise ValueError("ORDER BY clause requires at least one item.")
      return ", ".join(self.render_expr(i) for i in expr.items)

    if isinstance(expr, FuncCall):
      fn = (expr.name or "").strip()
      fn_lc = fn.lower()
      args = list(expr.args or [])

      # Special-case: hash256(...) must use dialect hash implementation
      # so tests see digest(...) / HASHBYTES(...) etc.
      if fn_lc in ("hash256", "sha256") and len(args) == 1:
        return self.hash_expression(self.render_expr(args[0]), algo="sha256")

      args_sql_list = [self.render_expr(a) for a in args]
      args_sql = ", ".join(args_sql_list)

      # COUNT_DISTINCT(x) -> COUNT(DISTINCT x)
      if fn == "COUNT_DISTINCT":
        if len(args) != 1:
          raise ValueError("COUNT_DISTINCT requires exactly one argument.")
        return f"COUNT(DISTINCT {args_sql_list[0]})"

      # STRING_AGG(value, delimiter[, order_by]) -> dialect-specific rendering
      if fn == "STRING_AGG":
        return self.render_string_agg(args)

      return f"{fn}({args_sql})"

    if isinstance(expr, WindowFunction):
      func_name = expr.name.upper()
      args_sql = ", ".join(self.render_expr(a) for a in (expr.args or []))
      func_sql = f"{func_name}({args_sql})"
      win = expr.window or WindowSpec()
      parts = []
      if win.partition_by:
        part_sql = ", ".join(self.render_expr(e) for e in win.partition_by)
        parts.append(f"PARTITION BY {part_sql}")
      if win.order_by:
        order_sql = ", ".join(self.render_expr(e) for e in win.order_by)
        parts.append(f"ORDER BY {order_sql}")
      over_sql = " ".join(parts)
      return f"{func_sql} OVER ({over_sql})" if over_sql else f"{func_sql} OVER ()"

    # Fallback: attempt stringification (kept permissive for DSL growth)
    return str(expr)


  def render_string_agg(self, args) -> str:
    """
    Render STRING_AGG in a dialect-friendly way.
    Signature: STRING_AGG(value, delimiter, order_by_expr?)
    Default: STRING_AGG(value, delimiter) (no ORDER BY support).
    Dialects may override to support ORDER BY.
    """
    if len(args) < 2:
      raise ValueError("STRING_AGG requires at least 2 arguments: value, delimiter.")
    value_sql = self.render_expr(args[0])
    delim_sql = self.render_expr(args[1])
    # optional order_by expr
    if len(args) >= 3 and args[2] is not None:
      # Base dialect does not implement order-by inside aggregation
      order_by_sql = self.render_expr(args[2])
      raise ValueError("STRING_AGG with ORDER BY is not supported by this dialect.")
    return f"STRING_AGG({value_sql}, {delim_sql})"


  def render_select(self, select: LogicalSelect) -> str:
    items_sql: list[str] = []

    # DSL compatibility: prefer select.select_list / select.from_
    select_list_obj = getattr(select, "select_list", None)
    if select_list_obj is None:
      select_list_obj = getattr(select, "items", None)
    select_list_obj = select_list_obj or []

    for it in select_list_obj:
      if isinstance(it, SelectItem):
        expr_sql = self.render_expr(it.expr)
        if it.alias:
          items_sql.append(f"{expr_sql} AS {self.render_identifier(it.alias)}")
        else:
          items_sql.append(expr_sql)
      else:
        items_sql.append(self.render_expr(it))

    select_list = ",\n  ".join(items_sql) if items_sql else "*"

    distinct = bool(getattr(select, "distinct", False))
    sql: list[str] = ["SELECT DISTINCT" if distinct else "SELECT", f"  {select_list}"]

    from_item = getattr(select, "from_", None)
    if from_item is None:
      from_item = getattr(select, "from_item", None)

    if from_item is not None:
      from_sql = self._render_from_item(from_item)

      sql.append("FROM")
      sql.append(f"  {from_sql}")

    for j in (getattr(select, "joins", None) or []):
      sql.append(self._render_join(j))

    where_expr = getattr(select, "where", None)
    if where_expr is not None:
      sql.append("WHERE")
      sql.append(f"  {self.render_expr(select.where)}")

    group_by = getattr(select, "group_by", None) or []
    if group_by:
      gb = ", ".join(self.render_expr(e) for e in group_by)
      sql.append(f"GROUP BY {gb}")

    having_expr = getattr(select, "having", None)
    if having_expr is not None:
      sql.append(f"HAVING {self.render_expr(having_expr)}")

    order_by = getattr(select, "order_by", None) or []
    if order_by:
      ob = ", ".join(self.render_expr(e) for e in order_by)
      sql.append(f"ORDER BY {ob}")

    limit_val = getattr(select, "limit", None)
    if limit_val is not None:
      sql.append(f"LIMIT {int(limit_val)}")

    offset_val = getattr(select, "offset", None)
    if offset_val is not None:
      sql.append(f"OFFSET {int(offset_val)}")

    return "\n".join(sql)


  def render_plan(self, plan) -> str:
    """
    Render a logical plan into SQL.
    Needed for subqueries that may contain a LogicalUnion.
    """
    if isinstance(plan, LogicalSelect):
      return self.render_select(plan)
    if isinstance(plan, LogicalUnion):
      rendered_parts = [self.render_select(sel) for sel in plan.selects]
      ut = (plan.union_type or "").strip().upper()
      if ut == "ALL":
        sep = "UNION ALL"
      elif ut in ("DISTINCT", ""):
        sep = "UNION"
      else:
        raise ValueError(f"Unsupported union_type: {plan.union_type!r}")
      separator = f"\n{sep}\n"
      return separator.join(rendered_parts)
    raise TypeError(f"Unsupported logical plan type: {type(plan).__name__}")


  def _render_from_item(self, item: SourceTable | SubquerySource) -> str:
    if isinstance(item, SourceTable):
      schema = getattr(item, "schema_name", None)
      if schema is None:
        schema = getattr(item, "schema", None)
      table = getattr(item, "table_name", None)
      if table is None:
        table = getattr(item, "name", None)
      tbl = self.render_table_identifier(schema, table)
      if item.alias:
        return f"{tbl} AS {self.render_identifier(item.alias)}"
      return tbl
    if isinstance(item, SubquerySource):
      inner = self.render_plan(item.select)
      if item.alias:
        alias = self.render_identifier(item.alias)
        return f"(\n{inner}\n) AS {alias}"
      return f"(\n{inner}\n)"

    return str(item)

  def _render_join(self, j: Join) -> str:
    jt = j.join_type.upper()
    right = self._render_from_item(j.right)
    on_sql = self.render_expr(j.on) if j.on is not None else ""
    if on_sql:
      return f"{jt} JOIN {right} ON {on_sql}"
    return f"{jt} JOIN {right}"

  def render_column_list(self, columns: list[str] | None) -> str:
    """
    Render a comma-separated list of column identifiers, with proper quoting.
    If columns is None or empty, '*' is returned.
    """
    if not columns:
      return "*"
    return ", ".join(self.quote_ident(c) for c in columns)
  
  def cast_expression(self, expr: str, target_type: str) -> str:
    """
    Wrap the given SQL expression in a dialect-specific CAST expression.
    """
    return f"CAST({expr} AS {target_type})"
  

  def truncate_string_expression(self, expr: str, max_length: int) -> str:
    """
    Truncate a string expression to max_length (lossy).
    Default uses LEFT(...), which works in many engines.
    Dialects can override (e.g., Databricks uses SUBSTRING).
    """
    return f"LEFT({expr}, {int(max_length)})"


  def concat_expression(self, rendered_parts: Sequence[str]) -> str:
    """
    Build a dialect-specific concatenation expression from already-rendered
    parts (each element is a SQL expression string).

    Example DuckDB:  (part1 || part2 || part3)
    Example Snowflake:  CONCAT(part1, part2, part3)
    """
    return " || ".join(rendered_parts)

  def hash_expression(self, expr: str, algo: str = "sha256") -> str:
    """
    Build a dialect-specific hashing expression around `expr`.

    Default implementation raises; dialects that support hashing should
    override this method.
    """
    raise NotImplementedError(
      f"{self.__class__.__name__} does not implement hash_expression()"
    )

  # ---------------------------------------------------------------------------
  # 7. Introspection hooks
  # ---------------------------------------------------------------------------
  def introspect_table(
    self,
    *,
    schema_name: str,
    table_name: str,
    introspection_engine: Any,
    exec_engine: Optional["BaseExecutionEngine"] = None,
    debug_plan: bool = False,
  ) -> Dict[str, Any]:
    """
    Default introspection via SQLAlchemy/read_table_metadata.

    Returns:
      {
        "table_exists": bool,
        "physical_table": str,
        "actual_cols_by_norm_name": {norm_name: column_meta_dict}
      }
    """
    # Default path uses SQLAlchemy-based metadata reading.
    try:
      meta = read_table_metadata(introspection_engine, schema_name, table_name)
    except Exception:
      if debug_plan:
        return {
          "table_exists": False,
          "physical_table": table_name,
          "actual_cols_by_norm_name": {},
          "debug": f"read_table_metadata failed for {schema_name}.{table_name}",
        }
      return {
        "table_exists": False,
        "physical_table": table_name,
        "actual_cols_by_norm_name": {},
      }

    cols = {}
    for c in (meta.get("columns") or []):
      nm = (c.get("name") or c.get("column_name") or "").strip().lower()
      if nm:
        cols[nm] = c

    return {
      # Do not assume "exists" just because reflection returned without raising.
      # MSSQL/Fabric may return empty columns for missing objects.
      "table_exists": bool(meta.get("table_exists", True)),
      "physical_table": table_name,
      "actual_cols_by_norm_name": cols,
    }
