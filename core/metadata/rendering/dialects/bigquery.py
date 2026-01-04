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

import os
import time
from typing import Any
from datetime import date, datetime, time as dt_time, timezone
import time as time_module
from decimal import Decimal

try:
  from google.cloud import bigquery
  from google.api_core.exceptions import NotFound
except ImportError as exc:
  bigquery = None

from .base import SqlDialect

from metadata.rendering.expr import Expr, FuncCall
from metadata.ingestion.types_map import (
  STRING, INTEGER, BIGINT, DECIMAL, FLOAT, BOOLEAN, DATE, TIME, TIMESTAMP, BINARY, UUID, JSON,
)
from metadata.materialization.logging import LOAD_RUN_LOG_REGISTRY


class BigQueryExecutionEngine:
  """
  Minimal execution adapter for BigQuery.

  Exposes:
    - execute(sql: str) -> rows_affected | None
    - execute_many(insert_sql: str, params: list[tuple]) -> int
  """

  def __init__(self, client: bigquery.Client, *, location: str = "EU"):
    self.client = client
    self.location = location
    # Default project used by insert_rows_json when table_id is not fully qualified.
    self.project_id = getattr(client, "project", None)

  def execute(self, sql: str) -> int | None:
    job = self.client.query(sql, location=self.location)
    result = job.result()

    # BigQuery often does not provide an affected-row count consistently.
    try:
      return result.total_rows
    except Exception:
      return None

  def _jsonify_value(self, v: Any) -> Any:
    # google-cloud-bigquery's insert_rows_json uses json.dumps internally
    # and will fail on datetime objects unless we convert them.
    if v is None:
      return None

    if isinstance(v, datetime):
      if v.tzinfo is None:
        v = v.replace(tzinfo=timezone.utc)
      return v.isoformat()

    if isinstance(v, date):
      return v.isoformat()

    if isinstance(v, dt_time):
      return v.isoformat()

    if isinstance(v, Decimal):
      # Keep precision as string for NUMERIC
      return str(v)

    return v

  def _qualify_table_id(self, table_id: str) -> str:
    """
    Ensure table_id is project-qualified for insert_rows_json().
    Accepts:
      - dataset.table
      - project.dataset.table
    Returns project.dataset.table.
    """
    tid = (table_id or "").strip()
    if not tid:
      return tid

    parts = [p for p in tid.split(".") if p]

    # Already fully qualified
    if len(parts) == 3:
      return tid

    # dataset.table → qualify
    if len(parts) == 2:
      pid = getattr(self, "project_id", None) or getattr(self.client, "project", None)
      if not pid:
        raise RuntimeError(
          "BigQueryExecutionEngine requires a default project "
          "to qualify table_id "
          f"(got table_id={tid!r})"
        )
      return f"{pid}.{parts[0]}.{parts[1]}"

    # Anything else is invalid
    raise ValueError(f"Invalid BigQuery table_id: {tid!r}")

  def execute_many(self, insert_sql: str, params: list[tuple]) -> int:
    """
    Convert INSERT INTO ... VALUES (?, ?, ...) into BigQuery streaming insert.

    Assumption: insert_sql is of the form:
      INSERT INTO `dataset`.`table` (`c1`, `c2`, ...) VALUES (?, ?, ...);
    """
    s = insert_sql.strip().rstrip(";")

    prefix = "INSERT INTO"
    idx = s.upper().find(prefix)
    if idx == -1:
      raise ValueError(f"BigQuery execute_many could not parse INSERT statement:\n{insert_sql}")

    after_into = s[idx + len(prefix):].lstrip()

    paren = after_into.find("(")
    if paren == -1:
      raise ValueError(f"BigQuery execute_many could not find column list in:\n{insert_sql}")

    table_part = after_into[:paren].strip()
    rest = after_into[paren + 1:]

    close = rest.find(")")
    if close == -1:
      raise ValueError(f"BigQuery execute_many could not parse column list in:\n{insert_sql}")

    cols_raw = rest[:close]

    # Normalize: `raw`.`raw_aw1_product` -> raw.raw_aw1_product
    table_id = (
      table_part
        .replace("`", "")
        .replace('"', "")
        .replace("[", "")
        .replace("]", "")
        .replace(" ", "")
    )
    table_id = self._qualify_table_id(table_id)

    cols = [c.strip().strip("`\"[]") for c in cols_raw.split(",")]

    rows = []
    for tup in params:
      row = {}
      for i, col in enumerate(cols):
        row[col] = self._jsonify_value(tup[i])
      rows.append(row)

    # BigQuery can be briefly eventually-consistent between CREATE TABLE (query job)
    # and streaming inserts (insertAll). If we just created the table, insertAll may
    # return 404 for a short window. Retry a few times.
    attempts = 8
    for i in range(attempts):
      try:
        # Extra guard: if table was just dropped/recreated, wait until get_table sees it.
        # (insertAll can 404 even though CREATE TABLE job finished)
        if i > 0:
          try:
            self.client.get_table(table_id)
          except Exception:
            # still not visible -> continue retry/backoff
            pass

        errors = self.client.insert_rows_json(table_id, rows)
        if errors:
          raise RuntimeError(f"BigQuery insert_rows_json failed: {errors}")
        return len(rows)
      except NotFound as exc:

        if i == attempts - 1:
          raise
        time_module.sleep(0.5 * (2 ** i))
      except Exception as exc:
        # Some wrappers surface 404 without raising NotFound directly.
        msg = str(exc).lower()
        if "not found" in msg and i < attempts - 1:
          time_module.sleep(0.5 * (2 ** i))
          continue
        raise

    return len(rows)

  def execute_scalar(self, sql: str):
    """
    Execute a SELECT returning a single value (first column of first row).
    Returns None if no row is returned.
    """
    job = self.client.query(sql, location=self.location)
    result = job.result()
    row = next(iter(result), None)
    if row is None:
      return None
    return row[0]

  def fetch_all(self, sql: str) -> list[tuple]:
    """
    Execute a SELECT and return all rows as tuples.
    """
    job = self.client.query(sql, location=self.location)
    result = job.result()
    return [tuple(r) for r in result]


class BigQueryDialect(SqlDialect):
  """
  BigQuery SQL dialect.

  Scope:
    - schema (dataset) creation
    - table creation
    - truncate
    - SQL execution
    - optional load_run_log insert

  Explicitly out of scope for v0.7.0:
    - native RAW ingestion
    - MERGE / UPSERT optimizations
    - parameter binding
    - project-qualified identifiers beyond basic support
  """

  # ---------------------------------------------------------------------------
  # 1. Class meta / capabilities
  # ---------------------------------------------------------------------------
  DIALECT_NAME = "bigquery"

  @property
  def supports_merge(self) -> bool:
    """BigQuery supports a native MERGE statement."""
    return True

  @property
  def supports_delete_detection(self) -> bool:
    """BigQuery supports delete detection via DELETE + NOT EXISTS."""
    return True

  def get_execution_engine(self, system) -> BigQueryExecutionEngine:
    """
    Create a BigQuery execution engine.

    The target system may optionally define a project.
    """
    if bigquery is None:
      raise RuntimeError(
        "BigQuery support requires the 'google-cloud-bigquery' package. "
        "Install it via: pip install -r requirements/bigquery.txt"
      )

    project = (
      getattr(system, "project", None)
      or os.getenv("GOOGLE_CLOUD_PROJECT")
      or os.getenv("GCLOUD_PROJECT")
    )

    if not project:
      raise EnvironmentError(
        "BigQuery project is missing. Set GOOGLE_CLOUD_PROJECT (or GCLOUD_PROJECT) "
        "or configure a project on the TargetSystem."
      )
      
    location = os.getenv("GOOGLE_BIGQUERY_LOCATION", "EU").strip() or "EU"
    client = bigquery.Client(project=project)
    return BigQueryExecutionEngine(client, location=location)

  # ---------------------------------------------------------------------------
  # 2. Identifier & quoting
  # ---------------------------------------------------------------------------
  def quote_ident(self, name: str) -> str:
    """
    Quote an identifier using BigQuery backtick style.
    Internal backticks are escaped by doubling them.
    """
    escaped = name.replace("`", "``")
    return f"`{escaped}`"

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
    return self._render_canonical_type_bigquery(
      datatype=canonical,
      max_length=max_length,
      decimal_precision=precision,
      decimal_scale=scale,
    )

  def _render_canonical_type_bigquery(
    self,
    *,
    datatype: str,
    max_length: int | None = None,
    decimal_precision: int | None = None,
    decimal_scale: int | None = None,
  ) -> str:
    t = (datatype or "").upper()

    if t == STRING:
      return "STRING"

    if t in (INTEGER, BIGINT):
      return "INT64"

    if t == DECIMAL:
      # BigQuery NUMERIC supports up to 38 digits, 9 decimal places
      return "NUMERIC"

    if t == FLOAT:
      return "FLOAT64"

    if t == BOOLEAN:
      return "BOOL"

    if t == DATE:
      return "DATE"

    if t == TIME:
      return "TIME"

    if t == TIMESTAMP:
      return "TIMESTAMP"

    if t == BINARY:
      return "BYTES"

    if t in (UUID, JSON):
      # BigQuery has native JSON, but STRING is the safest choice
      return "STRING"

    raise ValueError(
      f"Unsupported canonical datatype for BigQuery: {datatype!r}. "
      "Please extend the BigQuery dialect type mapping."
    )

  # ---------------------------------------------------------------------------
  # 4. DDL helpers
  # ---------------------------------------------------------------------------
  def render_create_schema_if_not_exists(self, schema: str) -> str:
    return f"CREATE SCHEMA IF NOT EXISTS `{schema}`"

  def render_create_table_if_not_exists_from_columns(
    self,
    *,
    schema: str,
    table: str,
    columns: list[dict[str, object]],
  ) -> str:
    """
    BigQuery does not accept "NULL"/"NOT NULL" tokens in column definitions
    the same way ANSI dialects do. We therefore omit nullability markers.
    """
    target = self.render_table_identifier(schema, table)
    col_defs: list[str] = []
    for c in columns:
      name = self.render_identifier(str(c["name"]))
      ctype = str(c["type"])
      # omit NULL/NOT NULL
      col_defs.append(f"{name} {ctype}")
    cols_sql = ",\n  ".join(col_defs)
    return f"CREATE TABLE IF NOT EXISTS {target} (\n  {cols_sql}\n)"
  
  def render_truncate_table(self, schema: str, table: str) -> str:
    return f"TRUNCATE TABLE {self.render_table_identifier(schema, table)}"

  # ---------------------------------------------------------------------------
  # 5. DML / load SQL primitives
  # ---------------------------------------------------------------------------
  LOAD_RUN_LOG_TYPE_MAP = {
    "string": "STRING",
    "bool": "BOOL",
    "int": "INT64",
    "timestamp": "TIMESTAMP",
  }

  def render_insert_load_run_log(self, *, meta_schema: str, values: dict[str, Any]) -> str | None:
    table = self.render_table_identifier(meta_schema, "load_run_log")

    def s(val: Any) -> str:
      if val is None:
        return "NULL"
      txt = str(val)
      # BigQuery SQL string literal escaping:
      # - single quotes must be doubled
      # - normalize Windows newlines to avoid weird formatting issues in logs
      txt = txt.replace("'", "''")
      txt = txt.replace("\r\n", "\n")
      return f"'{txt}'"

    def lit(col: str, v: Any) -> str:
      if v is None:
        return "NULL"
      if col in ("started_at", "finished_at"):
        iso = v.isoformat() if hasattr(v, "isoformat") else str(v)
        return f"TIMESTAMP({s(iso)})"
      if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
      if isinstance(v, (int, float)):
        # Registry expects ints for *_ms, sql_length, rows_affected
        return str(int(v))
      return s(v)

    # Canonical registry order; ignore unknown keys, NULL for missing.
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
      s = str(iso).replace("'", "''").replace("\r\n", "\n")
      return f"TIMESTAMP('{s}')"
    if isinstance(value, bool):
      return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
      return str(int(value))
    s = str(value)
    # BigQuery: for large multiline JSON payloads use triple-quoted strings to avoid
    # parser issues with very long single-quoted literals and escape sequences.
    if table == "load_run_snapshot" and column == "snapshot_json":
      s = s.replace("\r\n", "\n").replace("\r", "\n")
      # Escape triple quotes inside content (rare but safe).
      s = s.replace('"""', '\\"""')
      return f'"""{s}"""'

    # Default string literal
    s = s.replace("'", "''").replace("\r\n", "\n")
    return f"'{s}'"

  # ---------------------------------------------------------------------------
  # 6. Expression / Select renderer
  # ---------------------------------------------------------------------------
  def render_expr(self, expr: Expr) -> str:
    """
    Intercept vendor-neutral DSL function calls that DuckDBDialect doesn't map correctly for BigQuery.
    """
    if isinstance(expr, FuncCall):
      name = (expr.name or "").upper()

      if name in {"HASH256", "HASH"}:
        # BigQuery SHA256 returns BYTES → wrap in TO_HEX for 64-char hex parity
        if not expr.args or len(expr.args) != 1:
          raise ValueError(f"{name} expects exactly one argument.")
        inner = self.render_expr(expr.args[0])
        return f"TO_HEX(SHA256({inner}))"

      if name == "CONCAT_WS":
        # CONCAT_WS(sep, a, b, c) -> ARRAY_TO_STRING([a,b,c], sep)
        if not expr.args or len(expr.args) < 2:
          raise ValueError("CONCAT_WS expects at least (sep, a).")

        sep_sql = self.render_expr(expr.args[0])
        parts = []
        for a in expr.args[1:]:
          parts.append(f"CAST({self.render_expr(a)} AS STRING)")
        return f"ARRAY_TO_STRING([{', '.join(parts)}], {sep_sql})"

    return super().render_expr(expr)

  def cast_expression(self, expr: Expr, target_type: str) -> str:
    t = (target_type or "").strip().lower()
    if t in {"string", "varchar", "text"}:
      return f"CAST({self.render_expr(expr)} AS STRING)"
    return super().cast_expression(expr, target_type)

  # ---------------------------------------------------------------------------
  # 7. Introspection hooks
  # ---------------------------------------------------------------------------
  def introspect_table(
    self,
    *,
    schema_name: str,
    table_name: str,
    introspection_engine,
    exec_engine=None,
    debug_plan: bool = False,
  ):
    """
    BigQuery introspection via API (client.get_table), not INFORMATION_SCHEMA.

    Returns:
      {
        "table_exists": bool,
        "physical_table": str,
        "actual_cols_by_norm_name": {lower_name: {"name": ..., "type": ...}}
      }
    """
    # BigQuery API introspection is more reliable than INFORMATION_SCHEMA.
    client = getattr(exec_engine, "client", None) if exec_engine is not None else None
    if client is None:
      # Fallback: if someone wires SQLAlchemy for BQ introspection, allow default.
      return SqlDialect.introspect_table(
        self,
        schema_name=schema_name,
        table_name=table_name,
        introspection_engine=introspection_engine,
        exec_engine=exec_engine,
        debug_plan=debug_plan,
      )

    ds = (schema_name or "").strip()
    tbl = (table_name or "").strip()
    if not ds or not tbl:
      return {
        "table_exists": False,
        "physical_table": table_name,
        "actual_cols_by_norm_name": {},
      }

    pid = (
      getattr(exec_engine, "project_id", None)
      or getattr(client, "project", None)
      or os.getenv("GOOGLE_CLOUD_PROJECT")
      or os.getenv("GCLOUD_PROJECT")
    )
    if not pid:
      return {
        "table_exists": False,
        "physical_table": table_name,
        "actual_cols_by_norm_name": {},
      }

    table_ref = f"{pid}.{ds}.{tbl}"
    try:
      t = client.get_table(table_ref)
    except Exception:
      return {
        "table_exists": False,
        "physical_table": table_name,
        "actual_cols_by_norm_name": {},
      }

    cols = {}
    for f in (getattr(t, "schema", None) or []):
      nm = (getattr(f, "name", "") or "").strip()
      if not nm:
        continue
      # BigQuery field_type is e.g. "STRING", "INT64", ...
      ft = (getattr(f, "field_type", None) or getattr(f, "field_type", "") or "").strip()
      cols[nm.lower()] = {
        "name": nm,
        "type": ft,
      }

    return {
      "table_exists": True,
      "physical_table": table_name,
      "actual_cols_by_norm_name": cols,
    }
