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

from datetime import date, datetime
from decimal import Decimal
from typing import Sequence
import json
import re
import unicodedata

from .base import BaseExecutionEngine, SqlDialect
from metadata.ingestion.types_map import (
  STRING, INTEGER, BIGINT, DECIMAL, FLOAT, BOOLEAN, DATE, TIME, TIMESTAMP, BINARY, UUID, JSON
)
from metadata.materialization.logging import LOAD_RUN_LOG_REGISTRY


class DatabricksExecutionEngine(BaseExecutionEngine):
  """
  Databricks SQL execution via databricks-sql-connector.

  Expected system.security patterns (dict):
    {
      "server_hostname": "...",
      "http_path": "...",
      "access_token": "..."
    }

  Alternative keys are accepted for convenience:
    - "hostname" (alias for server_hostname)
    - "token" (alias for access_token)
  Additionally, elevata generic secret fields are supported (host/database/password/extra/schema).
  """

  def __init__(self, system):
    security = getattr(system, "security", None) or {}
    if not isinstance(security, dict):
      raise ValueError(
        f"Databricks system '{system.short_name}' expects system.security to be a dict."
      )

    # If we only got a single connection_string (targets.py behavior),
    # allow it to be a JSON object carrying the required Databricks fields.
    if "connection_string" in security and not (
      security.get("server_hostname") or security.get("hostname")
    ):
      raw = security.get("connection_string")
      if isinstance(raw, str) and raw.strip().startswith("{"):
        try:
          parsed = json.loads(raw)
          if isinstance(parsed, dict):
            security = {**security, **parsed}
        except Exception as exc:
          raise ValueError(
            "Databricks connection_string must be valid JSON when provided as a single string."
          ) from exc

    # Allow targets to pass a single connection_string JSON payload
    # (e.g. targets.py sets {"connection_string": "<json>"}).
    if "connection_string" in security:
      raw = security.get("connection_string")
      if isinstance(raw, str) and raw.strip().startswith("{"):
        try:
          parsed = json.loads(raw)
          if isinstance(parsed, dict):
            security = {**security, **parsed}
        except Exception:
          # Leave as-is; validation below will raise a clear error.
          pass

    # Generic-to-Databricks mapping:
    #   host     -> server_hostname
    #   database -> catalog
    #   password -> access_token
    #   extra    -> JSON with http_path
    self.server_hostname = (
      security.get("server_hostname")
      or security.get("hostname")
      or security.get("host")
    )

    self.access_token = (
      security.get("access_token")
      or security.get("token")
      or security.get("password")
    )

    # Catalog context for Unity Catalog (do NOT set schema here; elevata switches schemas constantly)
    self.catalog = security.get("catalog") or security.get("database")

    self.http_path = security.get("http_path")
    if not self.http_path:
      extra = security.get("extra")
      if isinstance(extra, dict):
        if extra.get("http_path"):
          self.http_path = extra.get("http_path")
      elif isinstance(extra, str) and extra.strip().startswith("{"):
        try:
          extra_obj = json.loads(extra)
          if isinstance(extra_obj, dict) and extra_obj.get("http_path"):
            self.http_path = extra_obj.get("http_path")
        except Exception:
          pass

    if not (self.server_hostname and self.http_path and self.access_token):
      raise ValueError(
        "Databricks system.security must contain server_hostname/hostname, http_path, access_token/token."
      )
    
    
  def _sanitize_sql(self, sql: str) -> str:
    """
    Databricks SQL does not allow semicolons inside a multi-row VALUES list.
    Some ingestion paths may generate: VALUES (...);, (...);, ...
    Normalize to: VALUES (...), (...), ...;
    """
    try:
      s = (sql or "").strip()
      low = s.lower()
      if not (low.startswith("insert") and " values " in low):
        return sql

      # Only rewrite if we actually see a semicolon before a comma
      if ");" not in s:
        return sql

      # Replace ");," (with optional whitespace) by "),"
      # Keep it simple & fast; no heavy parsing.
      s2 = s.replace(");,", "),").replace("); ,", "),")

      # If we changed anything, ensure exactly one trailing semicolon
      if s2 != s:
        s2 = s2.rstrip().rstrip(";").rstrip() + ";"
        return s2
      return sql
    except Exception:
      return sql
    

  def _split_statements(self, sql: str) -> list[str]:
    """
    Split multi-statement SQL scripts into individual statements.
    Only splits on semicolons that are not inside single-quoted strings.
    """
    s = (sql or "").strip()
    if ";" not in s:
      return [s] if s else []

    out = []
    buf = []
    in_sq = False
    i = 0
    while i < len(s):
      ch = s[i]
      if ch == "'":
        # Handle escaped single quote in SQL: '' -> literal '
        if in_sq and i + 1 < len(s) and s[i + 1] == "'":
          buf.append("'")
          i += 2
          continue
        in_sq = not in_sq
        buf.append(ch)
        i += 1
        continue

      if ch == ";" and not in_sq:
        stmt = "".join(buf).strip()
        if stmt:
          out.append(stmt)
        buf = []
        i += 1
        continue

      buf.append(ch)
      i += 1

    tail = "".join(buf).strip()
    if tail:
      out.append(tail)
    return out
  
  
  _DBX_TBLPROPS_PREFIX_RE = re.compile(
    r"^\s*ALTER\s+TABLE\s+.+\s+SET\s+TBLPROPERTIES\s*\(\s*'delta\.columnMapping\.mode'\s*=\s*'name'\s*\)\s*$",
    re.IGNORECASE,
  )

  _DBX_TBLPROPS_IGNORABLE_ERR_RE = re.compile(
    r"(" 
    r"FIELD_ALREADY_EXISTS|"                 # UC sometimes reports this style
    r"SQLSTATE:\s*42710|"                    # duplicate-ish
    r"already\s+exists|"                     # generic duplicate
    r"not\s+supported|"                      # feature/edition restrictions
    r"unsupported|"                          # feature/edition restrictions
    r"INVALID_PARAMETER_VALUE|"              # UC validation quirks
    r"permission|not\s+authorized|denied"    # perms (best-effort preflight)
    r")",
    re.IGNORECASE,
  )

  def _is_ignorable_preflight_error(self, stmt: str, exc: Exception) -> bool:
    """
    Only ignore errors for very specific "preflight" statements that are meant
    to prepare the table (e.g. enabling column mapping) but must not block the load.
    """
    try:
      s = (stmt or "").strip().rstrip(";").strip()
      if not self._DBX_TBLPROPS_PREFIX_RE.match(s):
        return False
      msg = str(exc) or ""
      return bool(self._DBX_TBLPROPS_IGNORABLE_ERR_RE.search(msg))
    except Exception:
      return False


  def execute(self, sql: str) -> int | None:
    try:
      from databricks import sql as dbsql
    except Exception as exc:
      raise ImportError(
        "Missing dependency for Databricks execution. Install 'databricks-sql-connector'."
      ) from exc

    with dbsql.connect(
      server_hostname=self.server_hostname,
      http_path=self.http_path,
      access_token=self.access_token,
    ) as conn:
      with conn.cursor() as cur:
        # Unity Catalog: ensure correct catalog context. Do NOT set schema (elevata switches schemas).
        if self.catalog:
          cur.execute(f"USE CATALOG {self.catalog}")

        # Normalize Databricks-specific quirks (e.g. ';' inside multi-row VALUES lists)
        sql = self._sanitize_sql(sql)

        # Databricks connector executes one statement at a time: split scripts like "DELETE ...; MERGE ..."
        statements = self._split_statements(sql)
        last_rowcount = None
        for stmt in statements:
          try:
            cur.execute(stmt)
            try:
              last_rowcount = cur.rowcount
            except Exception:
              last_rowcount = None
          except Exception as exc:
            # Best-effort preflight (e.g. column mapping enablement) must not block.
            if self._is_ignorable_preflight_error(stmt, exc):
              continue
            raise
        return last_rowcount        


  def execute_many(self, sql: str, params_seq) -> int | None:
    try:
      from databricks import sql as dbsql
    except Exception as exc:
      raise ImportError(
        "Missing dependency for Databricks execution. Install 'databricks-sql-connector'."
      ) from exc
    
    # ------------------------------------------------------------------
    # Databricks optimization:
    # The SQL connector's executemany() often results in one INSERT per row.
    # Rewrite INSERT ... VALUES (<row>) into INSERT ... VALUES (<row>),(<row>)...
    # ------------------------------------------------------------------
    params_seq = list(params_seq or [])
    if params_seq:
      try:
        lower_sql = sql.lower()
        if lower_sql.lstrip().startswith("insert") and " values " in lower_sql:
          values_idx = lower_sql.index(" values ")
          prefix = sql[: values_idx + len(" values ")]

          # Everything after VALUES should be a single row template, usually "(?, ?, ...);"
          template = sql[values_idx + len(" values "):].strip()

          # If the template already contains multiple rows, skip rewrite.
          # (e.g. "VALUES (...), (...)" or similar)
          if "),(" in template.replace(" ", "") or "),(" in template:
            raise RuntimeError("Skip multi-row rewrite: already multi-row VALUES.")

          had_semicolon = template.endswith(";")
          template = template.rstrip().rstrip(";").rstrip()

          # Expect "(...)" after stripping.
          if not (template.startswith("(") and template.endswith(")")):
            raise RuntimeError("Skip multi-row rewrite: VALUES template not a single tuple.")

          multi_values = ", ".join([template] * len(params_seq))
          sql = f"{prefix}{multi_values}"
          if had_semicolon:
            sql = sql + ";"

          # Flatten params: [(1,2),(3,4)] -> [1,2,3,4]
          flat_params = []
          for row in params_seq:
            flat_params.extend(list(row))
          params_seq = [flat_params]
      except Exception:
        # Fallback to default behavior if rewrite fails for any reason.
        pass

    with dbsql.connect(
      server_hostname=self.server_hostname,
      http_path=self.http_path,
      access_token=self.access_token,
    ) as conn:
      with conn.cursor() as cur:
        # Unity Catalog: ensure correct catalog context. Do NOT set schema (elevata switches schemas).
        if self.catalog:
          cur.execute(f"USE CATALOG {self.catalog}")
        sql = self._sanitize_sql(sql)
        if len(params_seq) == 1:
          cur.execute(sql, params_seq[0])
        else:
          cur.executemany(sql, params_seq)
        try:
          return cur.rowcount
        except Exception:
          return None


  def fetch_all(self, sql: str) -> list[tuple]:
    try:
      from databricks import sql as dbsql
    except Exception as exc:
      raise ImportError(
        "Missing dependency for Databricks execution. Install 'databricks-sql-connector'."
      ) from exc

    with dbsql.connect(
      server_hostname=self.server_hostname,
      http_path=self.http_path,
      access_token=self.access_token,
    ) as conn:
      with conn.cursor() as cur:
        # Unity Catalog: ensure correct catalog context (same as execute/execute_many).
        if self.catalog:
          cur.execute(f"USE CATALOG {self.catalog}")
        cur.execute(sql)
        rows = cur.fetchall()
        return [tuple(r) for r in (rows or [])]

  def execute_scalar(self, sql: str):
    rows = self.fetch_all(sql)
    if not rows:
      return None
    return rows[0][0]


class DatabricksDialect(SqlDialect):
  """
  Databricks (Spark SQL) dialect, optimized for Delta Lake semantics.
  """

  # ---------------------------------------------------------------------------
  # 1. Class meta / capabilities
  # ---------------------------------------------------------------------------
  DIALECT_NAME = "databricks"

  @property
  def supports_merge(self) -> bool:
    return True
  
  @property
  def supports_alter_column_type(self) -> bool:
    return True

  @property
  def supports_delete_detection(self) -> bool:
    return True

  def get_execution_engine(self, system) -> BaseExecutionEngine:
    return DatabricksExecutionEngine(system)

  # ---------------------------------------------------------------------------
  # 2. Identifier rendering
  # ---------------------------------------------------------------------------
  _UC_NAME_ALLOWED_RE = re.compile(r"[^a-z0-9_]+")
  _UC_MULTI_UNDERSCORE_RE = re.compile(r"_+")

  def _normalize_uc_object_name(self, name: str) -> str:
    """
    Unity Catalog object names must contain only alphanumeric characters and underscores.
    Spaces and most special characters are invalid even when quoted.

    This normalization is applied ONLY to object identifiers (schema/table/view),
    not to column aliases.

    Rules:
    - trim
    - German umlaut transliteration: ä->ae, ö->oe, ü->ue, ß->ss (also uppercase)
    - unicode normalize (strip remaining diacritics)
    - lowercase
    - replace non [a-z0-9_] with underscore
    - collapse multiple underscores
    - strip leading/trailing underscores
    """
    s = str(name or "").strip()
    if not s:
      return s

    # German transliteration (keep deterministic; avoid locale dependencies)
    s = (
      s.replace("Ä", "Ae")
      .replace("Ö", "Oe")
      .replace("Ü", "Ue")
      .replace("ä", "ae")
      .replace("ö", "oe")
      .replace("ü", "ue")
      .replace("ß", "ss")
    )

    # Strip remaining diacritics
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))

    s = s.lower()
    s = self._UC_NAME_ALLOWED_RE.sub("_", s)
    s = self._UC_MULTI_UNDERSCORE_RE.sub("_", s)
    s = s.strip("_")
    return s


  def quote_ident(self, name: str) -> str:
    s = str(name or "")
    s = s.replace("`", "``")
    return f"`{s}`"
  

  def render_table_identifier(self, schema: str | None, name: str) -> str:
    """
    Render Unity Catalog-compatible object identifiers.
    Important: UC does not allow spaces/special chars in object names even when quoted.
    We therefore normalize schema/table/view names to a safe physical identifier.
    """
    obj = self._normalize_uc_object_name(name)
    if schema:
      sch = self._normalize_uc_object_name(schema)
      return f"{sch}.{obj}"
    return obj


  # ---------------------------------------------------------------------------
  # 3. Type mapping / DDL helpers
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
    # Databricks types are mostly length-agnostic; keep deterministic behavior anyway.
    dt = (canonical or "").upper()

    if dt == STRING:
      return "STRING"
    if dt == INTEGER:
      return "INT"
    if dt == BIGINT:
      return "BIGINT"
    if dt == DECIMAL:
      p = 38 if precision is None else int(precision)
      s = 0 if scale is None else int(scale)
      return f"DECIMAL({p},{s})"
    if dt == FLOAT:
      return "DOUBLE"
    if dt == BOOLEAN:
      return "BOOLEAN"
    if dt == DATE:
      return "DATE"
    if dt == TIME:
      # Spark SQL TIME support is inconsistent across runtimes; store as string by default.
      return "STRING"
    if dt == TIMESTAMP:
      return "TIMESTAMP"
    if dt == BINARY:
      return "BINARY"
    if dt == UUID:
      return "STRING"
    if dt == JSON:
      # Spark has no native JSON type; store as STRING unless you use VARIANT-like features.
      return "STRING"

    raise ValueError(f"Unsupported logical datatype for Databricks: {canonical!r}")  

  def render_create_schema_if_not_exists(self, schema_name: str) -> str:
    # UC schema names follow the same naming restrictions as tables/views
    sch = self._normalize_uc_object_name(schema_name)
    return f"CREATE SCHEMA IF NOT EXISTS {sch};"
  

  def render_create_table_if_not_exists_from_columns(
    self,
    *,
    schema: str,
    table: str,
    columns: list[dict[str, object]],
  ) -> str:
    target = self.render_table_identifier(schema, table)

    col_defs: list[str] = []
    for c in columns:
      name = self.render_identifier(str(c["name"]))
      ctype = str(c["type"])
      nullable = bool(c.get("nullable", True))

      # Spark SQL: omit NULL keyword; only emit NOT NULL when needed.
      null_sql = "NOT NULL" if not nullable else ""
      piece = f"{name} {ctype}".rstrip()
      if null_sql:
        piece = f"{piece} {null_sql}"
      col_defs.append(piece)

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
    Spark SQL: do not emit the NULL keyword for nullable columns.
    Only emit NOT NULL when required.
    Used for deterministic rebuild flows (temp tables).
    """
    target = self.render_table_identifier(schema, table)

    col_defs: list[str] = []
    for c in columns:
      name = self.render_identifier(str(c["name"]))
      ctype = str(c["type"])
      nullable = bool(c.get("nullable", True))

      null_sql = "NOT NULL" if not nullable else ""
      piece = f"{name} {ctype}".rstrip()
      if null_sql:
        piece = f"{piece} {null_sql}"
      col_defs.append(piece)

    cols_sql = ",\n  ".join(col_defs)
    return f"CREATE TABLE {target} (\n  {cols_sql}\n)"


  def render_rename_table(self, schema: str, old: str, new: str) -> str:
    """
    Unity Catalog: always fully qualify the source table to avoid accidental
    renames/creates in the current default schema (often `default`).
    """
    src = self.render_table_identifier(schema, old)
    # IMPORTANT (Databricks/UC):
    # The destination MUST be fully qualified, otherwise Databricks may resolve it
    # in the current default schema (often `default`) and you end up with duplicates
    # plus subsequent statements (TBLPROPERTIES / RENAME COLUMN) failing with NOT_FOUND.
    dst = self.render_table_identifier(schema, new)
    return f"ALTER TABLE {src} RENAME TO {dst};"


  def render_rename_column(self, schema: str, table: str, old: str, new: str) -> str:
    """
    Delta Lake: RENAME COLUMN requires Column Mapping (mode 'name').
    Databricks error without this:
      [DELTA_UNSUPPORTED_RENAME_COLUMN] ... enable Column Mapping ... ('delta.columnMapping.mode'='name')
    We enable it best-effort right before renaming.
    """
    tbl = self.render_table_identifier(schema, table)
    old_col = self.render_identifier(old)
    new_col = self.render_identifier(new)
    return (
      f"ALTER TABLE {tbl} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');\n"
      f"ALTER TABLE {tbl} RENAME COLUMN {old_col} TO {new_col};"
    )
  

  # ---------------------------------------------------------------------------
  # 4. DDL helpers
  # ---------------------------------------------------------------------------
  def render_alter_column_type(self, *, schema: str, table: str, column: str, new_type: str) -> str:
    # Databricks SQL: ALTER TABLE <tbl> ALTER COLUMN <col> TYPE <type>
    tbl = self.render_table_identifier(schema, table)
    col = self.render_identifier(column)
    return f"ALTER TABLE {tbl} ALTER COLUMN {col} TYPE {new_type}"


  # ---------------------------------------------------------------------------
  # 5. DML / load SQL primitives
  # ---------------------------------------------------------------------------
  def render_merge_statement(
    self,
    schema: str,
    table: str,
    select_sql: str,
    unique_key_columns: list[str],
    update_columns: list[str],
  ) -> str:
    target = self.render_table_identifier(schema, table)

    keys = list(unique_key_columns or [])
    updates = [c for c in (update_columns or [])]
    all_cols = keys + [c for c in updates if c not in keys]

    on_pred = " AND ".join([f"t.{self.render_identifier(k)} = s.{self.render_identifier(k)}" for k in keys])

    update_assignments = ", ".join(
      [f"{self.render_identifier(c)} = s.{self.render_identifier(c)}" for c in updates]
    )

    insert_cols = ", ".join([self.render_identifier(c) for c in all_cols])
    insert_vals = ", ".join([f"s.{self.render_identifier(c)}" for c in all_cols])

    sql = f"""
      MERGE INTO {target} AS t
      USING (
      {select_sql}
      ) AS s
      ON {on_pred}
      WHEN MATCHED THEN UPDATE SET {update_assignments}
      WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
      """.strip()

    return sql

  def render_delete_detection_statement(
    self,
    *,
    target_schema: str,
    target_table: str,
    stage_schema: str,
    stage_table: str,
    join_predicates: list[str],
    scope_filter: str | None = None,
  ) -> str:
    tgt = self.render_table_identifier(target_schema, target_table)
    stg = self.render_table_identifier(stage_schema, stage_table)
    on_pred = " AND ".join(join_predicates or [])

    # Spark SQL supports DELETE with EXISTS/NOT EXISTS patterns.
    where_parts = []
    if scope_filter:
      where_parts.append(f"({scope_filter})")
    where_parts.append(
      "NOT EXISTS (\n"
      "  SELECT 1\n"
      f"  FROM {stg} AS s\n"
      f"  WHERE {on_pred}\n"
      ")"
    )
    where_sql = "\n  AND ".join(where_parts)
    return f"""
      DELETE FROM {tgt} AS t
      WHERE {where_sql};
      """.strip()
  
  
  LOAD_RUN_LOG_TYPE_MAP = {
    "string": "STRING",
    "bool": "BOOLEAN",
    "int": "INT",
    "timestamp": "TIMESTAMP",
  }

  def map_load_run_log_type(self, col_name: str, canonical_type: str) -> str | None:
    # Databricks/Spark types don't use VARCHAR lengths here.
    return self.LOAD_RUN_LOG_TYPE_MAP.get(canonical_type)


  def render_insert_load_run_log(self, *, meta_schema: str, values: dict[str, object]) -> str:
    qtbl = self.render_table_identifier
    lit = self.render_literal

    table = qtbl(meta_schema, "load_run_log")
    cols = list(LOAD_RUN_LOG_REGISTRY.keys())

    col_sql = ",\n        ".join(cols)
    val_sql = ",\n        ".join([lit(values.get(c)) for c in cols])

    return f"""
      INSERT INTO {table} (
        {col_sql}
      )
      VALUES (
        {val_sql}
      );
    """.strip()

  def param_placeholder(self) -> str:
    # Databricks connector uses Python DB-API paramstyle; keep consistent with %s.
    return "%s"

  # ---------------------------------------------------------------------------
  # 6. Expression / Select renderer
  # ---------------------------------------------------------------------------
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

    s = str(value).replace("'", "''")
    return f"'{s}'"


  def truncate_string_expression(self, expr: str, max_length: int) -> str:
    # Databricks SQL: substring(expr, pos, len)
    return f"SUBSTRING({expr}, 1, {int(max_length)})"


  def concat_expression(self, parts: Sequence[str]) -> str:
    if not parts:
      return "''"
    return "(" + " || ".join(parts) + ")"


  def hash_expression(self, expr: str, algo: str = "sha256") -> str:
    # Spark SQL: SHA2(expr, 256) returns a hex string.
    algo_lower = (algo or "").lower()
    if algo_lower in ("sha256", "hash256"):
      return f"SHA2(CAST(({expr}) AS STRING), 256)"
    return f"SHA2(CAST(({expr}) AS STRING), 256)"

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
  ) -> dict:
    """
    Databricks / Unity Catalog introspection.

    For exec_engine_only runs (no SQLAlchemy engine), SQLAlchemy reflection is unavailable.
    Use SHOW TABLES / SHOW COLUMNS via the execution engine instead.
    """
    # If we have a SQLAlchemy engine, keep the default path.
    if introspection_engine is not None or exec_engine is None:
      return super().introspect_table(
        schema_name=schema_name,
        table_name=table_name,
        introspection_engine=introspection_engine,
        exec_engine=exec_engine,
        debug_plan=debug_plan,
      )

    fetch_all = getattr(exec_engine, "fetch_all", None)
    if not callable(fetch_all):
      return {
        "table_exists": False,
        "physical_table": table_name,
        "actual_cols_by_norm_name": {},
      }

    # Normalize UC object identifiers so introspection matches rendering.
    sch = self._normalize_uc_object_name(schema_name)
    tbl = self._normalize_uc_object_name(table_name)

    # 1) Existence check
    try:
      rows = fetch_all(f"SHOW TABLES IN {sch} LIKE '{tbl}'")
      table_exists = bool(rows)
    except Exception:
      table_exists = False

    if not table_exists:
      out = {
        "table_exists": False,
        "physical_table": tbl,
        "actual_cols_by_norm_name": {},
      }
      if debug_plan:
        out["debug"] = f"SHOW TABLES IN {sch} LIKE '{tbl}' returned no rows"
      return out

    # 2) Columns (names + types)
    cols_by_norm: dict[str, dict] = {}
    try:
      # DESCRIBE TABLE returns (col_name, data_type, comment, ...) and then
      # section headers like "# Partition Information" / "# Detailed Table Information".
      col_rows = fetch_all(f"DESCRIBE TABLE {sch}.{tbl}")
      for r in col_rows or []:
        if not r:
          continue

        col_name = str(r[0] or "").strip()
        data_type = str(r[1] or "").strip() if len(r) > 1 else ""

        if not col_name:
          continue
        if col_name.startswith("#"):
          break

        nm = col_name.strip().strip("`").strip('"').lower()
        if nm:
          cols_by_norm[nm] = {
            "name": col_name,
            "type": data_type or None,
          }

    except Exception as exc:
      out = {
        "table_exists": True,
        "physical_table": tbl,
        "actual_cols_by_norm_name": {},
      }
      if debug_plan:
        out["debug"] = f"DESCRIBE TABLE failed: {exc}"
      return out

    return {
      "table_exists": True,
      "physical_table": tbl,
      "actual_cols_by_norm_name": cols_by_norm,
    }
  