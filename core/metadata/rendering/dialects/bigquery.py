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

import os
import re
from typing import Any
from datetime import date, datetime, time, timezone
from decimal import Decimal

try:
  from google.cloud import bigquery
except ImportError as exc:
  bigquery = None


from metadata.rendering.expr import Expr, FuncCall
from metadata.ingestion.types_map import (
  STRING, INTEGER, BIGINT, DECIMAL, FLOAT, BOOLEAN, DATE, TIME, TIMESTAMP, BINARY, UUID, JSON,
)

from .duckdb import DuckDBDialect


# -----------------------------------------------------------------------------
# Execution Engine
# -----------------------------------------------------------------------------

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

    if isinstance(v, time):
      return v.isoformat()

    if isinstance(v, Decimal):
      # Keep precision as string for NUMERIC
      return str(v)

    return v

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

    cols = [c.strip().strip("`\"[]") for c in cols_raw.split(",")]

    rows = []
    for tup in params:
      row = {}
      for i, col in enumerate(cols):
        row[col] = self._jsonify_value(tup[i])
      rows.append(row)

    errors = self.client.insert_rows_json(table_id, rows)
    if errors:
      raise RuntimeError(f"BigQuery insert_rows_json failed: {errors}")

    return len(rows)

# -----------------------------------------------------------------------------
# Dialect
# -----------------------------------------------------------------------------

class BigQueryDialect(DuckDBDialect):
  """
  BigQuery SQL dialect (MVP for elevata v0.7.0).

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

  DIALECT_NAME = "bigquery"

  # ---------------------------------------------------------------------------
  # Execution
  # ---------------------------------------------------------------------------

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
  # Identifiers
  # ---------------------------------------------------------------------------

  def render_identifier(self, name: str) -> str:
    return f"`{name}`"

  def render_table_identifier(self, schema: str, table: str) -> str:
    return f"`{schema}`.`{table}`"


  # ---------------------------------------------------------------------------
  # Logical type mapping
  # ---------------------------------------------------------------------------

  def map_logical_type(
    self,
    logical_type,
    max_length=None,
    precision=None,
    scale=None,
    strict=True,
  ):
    if not logical_type:
      return None

    t = str(logical_type).lower()

    # Strings
    if t in ("string", "text", "varchar", "char"):
      return "STRING"

    # Integers
    if t in ("int", "integer", "int32"):
      return "INT64"   # BigQuery has INT64
    if t in ("bigint", "int64", "long"):
      return "INT64"

    # Decimal / Numeric
    if t in ("decimal", "numeric"):
      # BigQuery supports NUMERIC (and BIGNUMERIC). Keep it simple:
      return "NUMERIC"

    # Floats
    if t in ("float", "double"):
      return "FLOAT64"

    # Bool
    if t in ("bool", "boolean"):
      return "BOOL"

    # Date/time
    if t in ("date",):
      return "DATE"
    if t in ("timestamp", "timestamptz", "datetime"):
      # Depending on your semantics; usually TIMESTAMP is what you want
      return "TIMESTAMP"

    if strict:
      raise ValueError(f"Unsupported logical type for BigQuery: {logical_type!r}")

    # passthrough
    return logical_type


  # ---------------------------------------------------------------------------
  # Type Casting
  # ---------------------------------------------------------------------------
  def cast(self, expr: str, target_type: str) -> str:
    t = (target_type or "").strip().lower()

    # Normalize common string types
    if t in {"varchar", "text", "string", "char"}:
      target_type = "STRING"

    # Optionally normalize INT/INTEGER too (nice-to-have)
    if t in {"int", "integer", "bigint"}:
      target_type = "INT64"

    return f"CAST({expr} AS {target_type})"


  def cast_expression(self, expr: Expr, target_type: str) -> str:
    t = (target_type or "").strip().lower()
    if t in {"string", "varchar", "text"}:
      return f"CAST({self.render_expr(expr)} AS STRING)"
    return super().cast_expression(expr, target_type)


  # ---------------------------------------------------------------------------
  # Expression rendering
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

  # ---------------------------------------------------------------------------
  # Canonical type mapping
  # ---------------------------------------------------------------------------

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
      # BigQuery has native JSON, but STRING is the safest MVP choice
      return "STRING"

    raise ValueError(
      f"Unsupported canonical datatype for BigQuery: {datatype!r}. "
      "Please extend the BigQuery dialect type mapping."
    )

  # ---------------------------------------------------------------------------
  # DDL
  # ---------------------------------------------------------------------------

  def render_create_schema_if_not_exists(self, schema: str) -> str:
    return f"CREATE SCHEMA IF NOT EXISTS `{schema}`"

  def render_create_table_if_not_exists(self, td) -> str:
    """
    Render CREATE TABLE IF NOT EXISTS for BigQuery.

    Notes:
      - BigQuery does not enforce primary keys.
      - Constraints are intentionally omitted.
    """
    cols: list[str] = []

    for c in td.target_columns.all().order_by("ordinal_position"):
      col_type = self._render_canonical_type_bigquery(
        datatype=c.datatype,
        max_length=c.max_length,
        decimal_precision=c.decimal_precision,
        decimal_scale=c.decimal_scale,
      )
      nullable = "" if c.nullable else " NOT NULL"
      cols.append(
        f"{self.render_identifier(c.target_column_name)} {col_type}{nullable}"
      )

    columns_block = ",\n  ".join(cols)

    return (
      f"CREATE TABLE IF NOT EXISTS "
      f"{self.render_table_identifier(td.target_schema.schema_name, td.target_dataset_name)} (\n"
      f"  {columns_block}\n"
      f")"
    )

  def render_truncate_table(self, schema: str, table: str) -> str:
    return f"TRUNCATE TABLE {self.render_table_identifier(schema, table)}"

  # ---------------------------------------------------------------------------
  # Logging (optional, but recommended)
  # ---------------------------------------------------------------------------
  def render_create_load_run_log_if_not_exists(self, meta_schema: str) -> str:
    # BigQuery: use scripting so we can create schema + table in one execute() call
    table = self.render_table_identifier(meta_schema, "load_run_log")

    return f"""
  BEGIN
    CREATE SCHEMA IF NOT EXISTS `{meta_schema}`;

    CREATE TABLE IF NOT EXISTS {table} (
      batch_run_id STRING,
      load_run_id STRING,
      load_status STRING,
      started_at TIMESTAMP,
      finished_at TIMESTAMP,
      sql_length INT64,
      rows_affected INT64,
      render_ms FLOAT64,
      execution_ms FLOAT64,
      error_message STRING
    );
  END;
  """.strip()


  def render_insert_load_run_log(
    self,
    *,
    meta_schema: str,
    batch_run_id: str,
    load_run_id: str,
    summary: dict[str, Any],
    profile,
    system,
    started_at,
    finished_at,
    render_ms: float | None,
    execution_ms: float | None,
    sql_length: int | None,
    rows_affected: int | None,
    load_status: str,
    error_message: str | None,
  ) -> str | None:
    """
    Insert a row into meta.load_run_log.

    MVP strategy:
      - structured values stored as STRING
      - timestamps stored as TIMESTAMP
    """
    table = self.render_table_identifier(meta_schema, "load_run_log")

    def s(val: Any) -> str:
      if val is None:
        return "NULL"
      return f"'{str(val)}'"

    return f"""
INSERT INTO {table} (
  batch_run_id,
  load_run_id,
  load_status,
  started_at,
  finished_at,
  sql_length,
  rows_affected,
  render_ms,
  execution_ms,
  error_message
)
VALUES (
  {s(batch_run_id)},
  {s(load_run_id)},
  {s(load_status)},
  TIMESTAMP({s(started_at.isoformat())}),
  TIMESTAMP({s(finished_at.isoformat())}),
  {sql_length if sql_length is not None else "NULL"},
  {rows_affected if rows_affected is not None else "NULL"},
  {render_ms if render_ms is not None else "NULL"},
  {execution_ms if execution_ms is not None else "NULL"},
  {s(error_message)}
)
""".strip()
