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

from typing import Any
import time
import logging
import os
import re
import uuid
from django.utils.timezone import now
from django.core.management.base import BaseCommand, CommandError
from dataclasses import replace

from metadata.config.profiles import load_profile
from metadata.config.targets import get_target_system
from metadata.models import TargetDataset
from metadata.rendering.dialects import get_active_dialect
from metadata.rendering.load_sql import (
  render_load_sql_for_target,
  build_load_run_summary,
  format_load_run_summary,
)
from metadata.rendering.load_planner import build_load_plan
from metadata.rendering.placeholders import resolve_delta_cutoff_for_source_dataset
from metadata.intent.ingestion import resolve_ingest_mode
from metadata.ingestion.native_raw import ingest_raw_full
from metadata.execution.load_graph import resolve_execution_order

from metadata.materialization.policy import load_materialization_policy
from metadata.materialization.planner import build_materialization_plan
from metadata.materialization.applier import apply_materialization_plan
from metadata.ingestion.connectors import engine_for_target

logger = logging.getLogger(__name__)


def _get_bool_env(name: str, default: bool) -> bool:
  """Read a boolean flag from the environment (supports true/false/1/0/yes/no)."""
  value = os.getenv(name)
  if value is None:
    return default
  value = value.strip().lower()
  if value in ("1", "true", "yes", "y", "on"):
    return True
  if value in ("0", "false", "no", "n", "off"):
    return False
  return default


AUTO_PROVISION_SCHEMAS = _get_bool_env("ELEVATA_AUTO_PROVISION_SCHEMAS", True)
AUTO_PROVISION_TABLES = _get_bool_env("ELEVATA_AUTO_PROVISION_TABLES", True)
AUTO_PROVISION_META_LOG = _get_bool_env("ELEVATA_AUTO_PROVISION_META_LOG", True)
META_SCHEMA_NAME = os.getenv("ELEVATA_META_SCHEMA_NAME", "meta")


def ensure_target_schema(engine, dialect, schema_name: str, auto_provision: bool) -> None:
  """
  Ensure that the physical target schema exists in the warehouse.

  Safe to call multiple times. Does nothing when auto_provision is False
  or when the dialect does not implement the required helper.
  """
  if not auto_provision:
    return

  # Some test dialects (DummyDialect) do not implement this hook.
  if not hasattr(dialect, "render_create_schema_if_not_exists"):
    return

  ddl = dialect.render_create_schema_if_not_exists(schema_name)
  if not ddl:
    return

  engine.execute(ddl)


def ensure_target_table(engine, dialect, td, auto_provision: bool) -> None:
  """
  Ensure the physical target table exists in the warehouse.

  Safe to call multiple times. Does nothing when auto_provision is False
  or when the dialect does not implement the required helper.
  """
  if not auto_provision:
    return

  if not hasattr(dialect, "render_create_table_if_not_exists"):
    return

  ddl = dialect.render_create_table_if_not_exists(td)
  if not ddl:
    return

  engine.execute(ddl)


def ensure_load_run_log_table(engine, dialect, meta_schema: str, auto_provision: bool) -> None:
  """
  Ensure that the warehouse-level load_run_log table exists.

  Safe to call multiple times. Does nothing when auto_provision is False
  or when the dialect does not implement the required helper.
  """
  if not auto_provision:
    return

  if not hasattr(dialect, "render_create_load_run_log_if_not_exists"):
    return

  ddl = dialect.render_create_load_run_log_if_not_exists(meta_schema)
  if not ddl:
    return

  engine.execute(ddl)


def _looks_like_cross_system_sql(sql: str, target_schema: str) -> bool:
  """
  Heuristic: detect SQL that likely references non-target objects.

  We only inspect schema-qualified objects in FROM / JOIN clauses to avoid
  false positives from alias.column expressions in SELECT lists.
  """
  if not sql:
    return False

  tgt = (target_schema or "").strip().strip('"').strip("[").strip("]").lower()

  allowed_schemas = {
    tgt,
    "raw",
    "stage",
    "rawcore",
    "meta",
    "information_schema",
    "pg_catalog",
    "duckdb",
    "main",
    "sys",  # MSSQL system schema
    "dbo",  # common MSSQL default schema (harmless)
  }

  # Match FROM/JOIN <schema>.<table> (quoted or unquoted)
  # Examples:
  #   FROM raw.raw_aw1_product AS s
  #   JOIN [raw].[raw_aw1_product] s
  #   FROM "raw"."raw_aw1_product"
  from_join_pattern = re.compile(
    r"""
    \b(?:from|join)\s+
    (?:
      (?:"(?P<s1>[^"]+)"|\[(?P<s2>[^\]]+)\]|(?P<s3>[A-Za-z_][A-Za-z0-9_]*))
    )
    \s*\.\s*
    (?:
      "(?P<t1>[^"]+)"|\[(?P<t2>[^\]]+)\]|(?P<t3>[A-Za-z_][A-Za-z0-9_]*)
    )
    """,
    re.IGNORECASE | re.VERBOSE,
  )

  for m in from_join_pattern.finditer(sql):
    schema = (m.group("s1") or m.group("s2") or m.group("s3") or "").strip().lower()
    if schema and schema not in allowed_schemas:
      return True

  return False


def _render_literal_for_dialect(dialect, value):
  # prefer dialect.render_literal if available
  fn = getattr(dialect, "render_literal", None)
  if callable(fn):
    return fn(value)
  return dialect.literal(value)


def apply_runtime_placeholders(
  sql: str,
  *,
  dialect,
  load_run_id: str,
  load_timestamp,
  delta_cutoff=None,
) -> str:
  if not sql:
    return sql

  ts_sql = _render_literal_for_dialect(dialect, load_timestamp)
  id_sql = _render_literal_for_dialect(dialect, load_run_id)

  sql = re.sub(r"\{\{\s*load_timestamp\s*\}\}", ts_sql, sql)
  sql = re.sub(r"\{\s*load_timestamp\s*\}", ts_sql, sql)

  sql = re.sub(r"\{\{\s*load_run_id\s*\}\}", id_sql, sql)
  sql = re.sub(r"\{\s*load_run_id\s*\}", id_sql, sql)

  if delta_cutoff is not None:
    cutoff_sql = _render_literal_for_dialect(dialect, delta_cutoff)
    # Support both "{{ DELTA_CUTOFF }}" and "{ DELTA_CUTOFF }"
    sql = re.sub(r"\{\{\s*DELTA_CUTOFF\s*\}\}", cutoff_sql, sql)
    sql = re.sub(r"\{\s*DELTA_CUTOFF\s*\}", cutoff_sql, sql)

  return sql


def should_truncate_before_load(td, load_plan) -> bool:
  """
  Decide whether we should truncate the target object before running the load SQL.

  Rules:
  - Never truncate views (materialization_type=view).
  - Never truncate RAW (ingestion semantics).
  - Never truncate *_hist datasets (SCD2 incremental semantics).
  - Truncate only for "full" mode table-like materializations.
  """
  schema = getattr(td, "target_schema", None)
  schema_short = getattr(schema, "short_name", None)
  ds_name = getattr(td, "target_dataset_name", "") or ""

  # Materialization: dataset overrides schema default; fallback is "table"
  mat = (
    getattr(td, "materialization_type", None)
    or getattr(schema, "default_materialization_type", None)
    or "table"
  )

  # Views are never truncated
  if mat == "view":
    return False

  # RAW is ingested, not truncated via SQL
  if schema_short == "raw":
    return False

  # Hist datasets are incremental/SCD2 by definition
  if schema_short == "rawcore" and isinstance(ds_name, str) and ds_name.endswith("_hist"):
    return False

  mode = getattr(load_plan, "mode", None)

  # Only full refresh truncates
  return mode == "full" and mat in ("table", "incremental")


def resolve_single_source_dataset_for_raw(target_dataset):
  """
  RAW datasets must have exactly one SourceDataset input.
  """
  inputs = list(target_dataset.input_links.select_related("source_dataset", "upstream_target_dataset"))
  src = [i.source_dataset for i in inputs if i.source_dataset is not None]

  if len(src) != 1:
    raise ValueError(
      f"RAW dataset '{target_dataset.target_dataset_name}' must have exactly one SourceDataset input, "
      f"but found {len(src)}."
    )
  return src[0]


def _plan_did_provision(plan) -> bool:
  """
  Return True only if the materialization plan contains steps that actually
  change/provision a table (rename/add/alter/etc.). ENSURE_SCHEMA alone must
  not suppress ensure_target_table().
  """
  steps = list(getattr(plan, "steps", None) or [])
  if not steps:
    return False
  # ENSURE_SCHEMA is intentionally ignored (schema DDL only)
  return any(getattr(s, "op", None) not in (None, "ENSURE_SCHEMA") for s in steps)


# NOTE:
# This function intentionally encapsulates the full lifecycle of a single dataset execution
# (rendering, execution, logging, result normalization).
# It may be split into smaller helpers in a future release once execution semantics stabilize.
def run_single_target_dataset(
  *,
  stdout,
  style,
  target_dataset: TargetDataset,
  target_system,
  target_system_engine,
  profile,
  dialect,
  execute: bool,
  no_print: bool,
  debug_plan: bool,
  debug_materialization: bool = False,
  batch_run_id: str,
  load_run_id: str | None = None,
  load_plan_override=None,
  chunk_size: int = 5000,
) -> dict[str, object]:
  """
  Execute or render exactly one dataset.

  Returns a normalized result dict for the execution summary:
    {
      "status": "success" | "error" | "dry_run" | "skipped",
      "kind": "ingestion" | "sql",
      "dataset": "schema.dataset",
      "message": optional[str],
      "rows_affected": optional[int],
      "load_run_id": str,
    }
  """
  td = target_dataset
  td = TargetDataset.objects.select_related("target_schema").prefetch_related("target_columns").get(pk=td.pk)

  dataset_key = f"{td.target_schema.short_name}.{td.target_dataset_name}"
  mat = getattr(td, "materialization_type", None) or getattr(td.target_schema, "default_materialization_type", None) or "table"

  # Treat rawcore *_hist datasets as table-like for provisioning, regardless of mat.
  # They must exist physically because SCD2 SQL starts with UPDATE/INSERT against the hist table.
  schema_short = getattr(getattr(td, "target_schema", None), "short_name", None)
  ds_name = getattr(td, "target_dataset_name", "") or ""
  is_hist = bool(
    schema_short == "rawcore" and isinstance(ds_name, str) and ds_name.endswith("_hist")
  )  

  # Per-dataset load_run_id (nested under batch_run_id)
  if load_run_id is None:
    load_run_id = str(uuid.uuid4())

  # --- RAW datasets -------------------------------------------------
  if td.target_schema.short_name == "raw":
    if not execute:
      if not no_print:
        stdout.write("")
        stdout.write(style.NOTICE(
          "-- RAW datasets are ingested. Use --execute to run ingestion (extract + load)."
        ))
      return {
        "status": "dry_run",
        "kind": "ingestion",
        "dataset": dataset_key,
        "message": "raw_ingestion_dry_run",
        "load_run_id": load_run_id,
      }

    result = execute_raw_via_ingestion(
      target_dataset=td,
      target_system=target_system,
      profile=profile,
      chunk_size=chunk_size,
      batch_run_id=batch_run_id,
    )

    if not no_print:
      stdout.write(style.NOTICE(f"[OK] {dataset_key}: RAW ingestion result: {result}"))

    # Normalize ingestion outcome (best-effort: ingest_raw_full already returns status)
    status = (result or {}).get("status", "success")
    msg = (result or {}).get("reason") or None

    return {
      "status": "success" if status == "success" else status,
      "kind": "ingestion",
      "dataset": dataset_key,
      "message": msg,
      "rows_affected": (result or {}).get("rows_affected"),
      "load_run_id": (result or {}).get("load_run_id", load_run_id),
    }

  # --- Non-RAW: build load plan & render SQL -----------------------
  load_plan = load_plan_override or build_load_plan(td)
  is_full_refresh = should_truncate_before_load(td, load_plan)
  did_materialization_provision = False
  mat_policy = None

  if debug_plan and not no_print:
    stdout.write(style.NOTICE(f"-- LoadPlan debug for {dataset_key}: {load_plan}"))

  summary = build_load_run_summary(td, dialect, load_plan)

  logger.info(
    "elevata_load dataset starting",
    extra={
      "batch_run_id": batch_run_id,
      "load_run_id": load_run_id,
      "target_dataset_id": getattr(td, "id", None),
      "target_dataset_name": td.target_dataset_name,
      "target_schema": td.target_schema.short_name,
      "profile": profile.name,
      "target_system": target_system.short_name,
      "target_system_type": target_system.type,
      "dialect": dialect.__class__.__name__,
      "execute": execute,
      "load_mode": summary.get("mode"),
      "load_handle_deletes": summary.get("handle_deletes"),
      "load_historize": summary.get("historize"),
    },
  )

  # ------------------------------------------------------------------
  # Materialization (DDL) must happen BEFORE SQL rendering
  # ------------------------------------------------------------------
  if execute and mat in ("table", "incremental"):
    mat_policy = load_materialization_policy()
    mat_policy = replace(mat_policy, debug_plan=bool(debug_plan))

    if AUTO_PROVISION_TABLES and schema_short in mat_policy.sync_schema_shorts:
      # Introspection via SQLAlchemy
      target_sa_engine = engine_for_target(
        target_short_name=target_system.short_name,
        system_type=target_system.type,
      )

      if debug_materialization and not no_print:
        stdout.write(style.NOTICE(
          f"-- Introspection DB absolute: {os.path.abspath(target_sa_engine.url.database or '')}"
        ))
        stdout.write(style.NOTICE(
          f"-- Execution engine: {target_system.short_name} type={target_system.type}"
        ))

      plan = build_materialization_plan(
        td=td,
        introspection_engine=target_sa_engine,
        exec_engine=target_system_engine,
        dialect=dialect,
        policy=mat_policy,
      )

      # do NOT dispose yet - hist_plan uses the same engine below

      if debug_materialization and not no_print:
        stdout.write(style.NOTICE(
          f"-- Materialization debug: dataset={dataset_key} "
          f"steps={len(plan.steps)} warnings={len(plan.warnings)} blocking={len(plan.blocking_errors)} "
          f"planner_file={build_materialization_plan.__code__.co_filename} "
          f"has_render_add_column={hasattr(dialect, 'render_add_column')}"
        ))
        for w in plan.warnings:
          stdout.write(style.WARNING(f"-- Materialization warning: {w}"))
        for e in plan.blocking_errors:
          stdout.write(style.ERROR(f"-- Materialization blocked: {e}"))
        for s in plan.steps:
          stdout.write(style.WARNING(f"-- Materialization step: {s.op}: {s.sql}"))

      apply_materialization_plan(plan=plan, exec_engine=target_system_engine)
      # Treat "materialization provisioned table" as: any step that touches the table itself.
      # ENSURE_SCHEMA alone does NOT provision the table.
      did_materialization_provision = _plan_did_provision(plan)

      # --------------------------------------------------------------
      # rawcore _hist structural sync (DDL only), even if user runs base only
      # --------------------------------------------------------------
      try:
        schema_short_local = td.target_schema.short_name
        historize_enabled = bool(
          getattr(td, "historize", False) or getattr(td.target_schema, "default_historize", False)
        )

        if (
          schema_short_local == "rawcore"
          and historize_enabled
          and isinstance(ds_name, str)
          and not ds_name.endswith("_hist")
        ):
          hist_td = None
          lineage_key = getattr(td, "lineage_key", None)
          if lineage_key:
            hist_td = (
              TargetDataset.objects
              .filter(target_schema__short_name="rawcore", lineage_key=lineage_key, target_dataset_name__endswith="_hist")
              .exclude(pk=td.pk)
              .first()
            )
          if hist_td is None:
            # Fallback: by convention
            hist_td = (
              TargetDataset.objects
              .filter(target_schema__short_name="rawcore", target_dataset_name=f"{ds_name}_hist")
              .first()
            )

          if hist_td is not None:
            hist_plan = build_materialization_plan(
              td=hist_td,
              introspection_engine=target_sa_engine,
              exec_engine=target_system_engine,
              dialect=dialect,
              policy=mat_policy,
            )

            if debug_materialization and not no_print:
              stdout.write(style.NOTICE(
                f"-- Hist materialization debug: dataset=rawcore.{hist_td.target_dataset_name} "
                f"steps={len(hist_plan.steps)} warnings={len(hist_plan.warnings)} blocking={len(hist_plan.blocking_errors)}"
              ))
              for w in hist_plan.warnings:
                stdout.write(style.WARNING(f"-- Hist materialization warning: {w}"))
              for e in hist_plan.blocking_errors:
                stdout.write(style.ERROR(f"-- Hist materialization blocked: {e}"))
              for s in hist_plan.steps:
                stdout.write(style.WARNING(f"-- Hist materialization step: {s.op}: {s.sql}"))

            apply_materialization_plan(plan=hist_plan, exec_engine=target_system_engine)
            # (no need to touch did_materialization_provision here; this is best-effort hist sync)
      except Exception as exc:
        # Never break the load because of best-effort hist sync
        logger.warning("Hist materialization sync failed: %s", exc)

      # dispose AFTER both plans
      dispose = getattr(target_sa_engine, "dispose", None)
      if callable(dispose):
        dispose()

  # ------------------------------------------------------------------
  # 1) Render SQL (now schema is correct)
  # ------------------------------------------------------------------
  render_started_at = now()
  render_start_ts = time.perf_counter()
  sql = render_load_sql_for_target(td, dialect)
  render_ms = (time.perf_counter() - render_start_ts) * 1000.0
  render_finished_at = now()
  sql_length = len(sql or "")

  # "Run" starts with rendering (execution timestamps will extend this later)
  run_started_at = render_started_at
  run_finished_at = render_finished_at

  if not no_print:
    stdout.write("")
    stdout.write(style.NOTICE(f"-- Dataset: {dataset_key}"))
    stdout.write(style.NOTICE(f"-- Profile: {profile.name}"))
    stdout.write(style.NOTICE(f"-- Target system: {target_system.short_name} (type={target_system.type})"))
    stdout.write(style.NOTICE(f"-- Dialect: {dialect.__class__.__name__}"))
    stdout.write(style.NOTICE(f"-- Load summary: {format_load_run_summary(summary)}"))
    stdout.write(style.NOTICE(f"-- SQL length: {sql_length} chars"))
    stdout.write(style.NOTICE(f"-- Render time: {render_ms:.1f} ms"))
    stdout.write("")
    stdout.write(sql)

  logger.info(
    "elevata_load dataset rendered",
    extra={
      "batch_run_id": batch_run_id,
      "load_run_id": load_run_id,
      "target_dataset_id": getattr(td, "id", None),
      "target_dataset_name": td.target_dataset_name,
      "target_schema": td.target_schema.short_name,
      "sql_length": sql_length,
      "execute": execute,
      "render_started_at": render_started_at.isoformat(),
      "render_finished_at": render_finished_at.isoformat(),
      "render_ms": render_ms,
    },
  )

  if not execute:
    return {
      "status": "dry_run",
      "kind": "sql",
      "dataset": dataset_key,
      "message": "sql_rendered",
      "load_run_id": load_run_id,
      "summary": summary,
      "sql_length": sql_length,
      "render_ms": render_ms,
      "render_started_at": render_started_at,
      "render_finished_at": render_finished_at,
      "run_started_at": run_started_at,
      "run_finished_at": run_finished_at,
      "execution_ms": None,
      "exec_started_at": None,
      "exec_finished_at": None,
    }

  # From here on we must have an engine
  if target_system_engine is None:
    raise CommandError("Missing execution engine in execute mode.")

  # 2) Execute SQL in target system
  ensure_target_schema(
    engine=target_system_engine,
    dialect=dialect,
    schema_name=td.target_schema.schema_name,
    auto_provision=AUTO_PROVISION_SCHEMAS,
  )

  if mat in ("table", "incremental"):
    # Avoid double provisioning:
    # - If materialization already created/adjusted the table, skip.
    # - If full refresh will DROP+CREATE below, skip baseline ensure here.
    if is_hist or ((not did_materialization_provision) and (not is_full_refresh)):
      ensure_target_table(
        engine=target_system_engine,
        dialect=dialect,
        td=td,
        auto_provision=AUTO_PROVISION_TABLES,
      )
  elif mat == "view":
    # no table provisioning for views
    pass

  ensure_load_run_log_table(
    engine=target_system_engine,
    dialect=dialect,
    meta_schema=META_SCHEMA_NAME,
    auto_provision=AUTO_PROVISION_META_LOG,
  )

  # Guardrail: warn for stage that is fed directly from sources (no RAW)
  warn_if_stage_exec_without_raw(td)

  if _looks_like_cross_system_sql(sql, td.target_schema.schema_name):
    raise CommandError(
      "Execute mode currently supports target-only SQL. "
      "This dataset SQL appears to reference objects outside the allowed target schemas. "
      "Either enable RAW landing or use a federated/external execution approach."
    )

  if should_truncate_before_load(td, load_plan) and mat in ("table", "incremental"):
    # Full refresh: prefer DROP+CREATE (recreate) so schema drift can be healed.
    if hasattr(dialect, "render_drop_table_if_exists"):
      drop_sql = dialect.render_drop_table_if_exists(
        schema=td.target_schema.schema_name,
        table=td.target_dataset_name,
      )
      if drop_sql:
        target_system_engine.execute(drop_sql)

      # Re-create table according to current metadata
      ensure_target_table(
        engine=target_system_engine,
        dialect=dialect,
        td=td,
        auto_provision=AUTO_PROVISION_TABLES,
      )
    else:
      # Fallback: truncate (older dialects)
      trunc_sql = dialect.render_truncate_table(
        schema=td.target_schema.schema_name,
        table=td.target_dataset_name,
      )
      target_system_engine.execute(trunc_sql)

  exec_started_at = now()
  exec_start_ts = time.perf_counter()

  rows_affected: int | None = None
  load_status = "success"
  error_message: str | None = None

  try:
    exec_ts = now()

    # Compute delta cutoff only if SQL contains the placeholder.
    needs_delta = bool(re.search(r"\{\{?\s*DELTA_CUTOFF\s*\}?\}", sql or ""))
    delta_cutoff = None
    if needs_delta:
      delta_cutoff = resolve_delta_cutoff_for_source_dataset(
        source_dataset=getattr(td, "incremental_source", None),
        profile=profile,
        now_ts=exec_ts,
      )
      if delta_cutoff is None:
        raise CommandError(
          f"SQL contains {{DELTA_CUTOFF}} but no active increment policy exists "
          f"for incremental_source in environment '{profile.name}'."
        )

    sql_exec = apply_runtime_placeholders(
      sql,
      dialect=dialect,
      load_run_id=load_run_id,
      load_timestamp=exec_ts,
      delta_cutoff=delta_cutoff,
    )

    rows_affected = target_system_engine.execute(sql_exec)

  except Exception as exc:
    load_status = "error"
    error_message = str(exc)

  exec_finished_at = now()
  execution_ms = (time.perf_counter() - exec_start_ts) * 1000.0

  # For execute mode, the overall run ends after execution.
  run_finished_at = exec_finished_at

  # Insert warehouse log row (only if the dialect supports it)
  if hasattr(dialect, "render_insert_load_run_log"):
    log_insert_sql = dialect.render_insert_load_run_log(
      meta_schema=META_SCHEMA_NAME,
      batch_run_id=batch_run_id,
      load_run_id=load_run_id,
      summary=summary,
      profile=profile,
      system=target_system,
      started_at=run_started_at,
      finished_at=run_finished_at,
      render_ms=render_ms,
      execution_ms=execution_ms,
      sql_length=sql_length,
      rows_affected=rows_affected,
      load_status=load_status,
      error_message=error_message,
    )
    if log_insert_sql:
      target_system_engine.execute(log_insert_sql)

  if load_status == "error":
    return {
      "status": "error",
      "kind": "sql",
      "dataset": dataset_key,
      "message": error_message,
      "rows_affected": rows_affected,
      "load_run_id": load_run_id,
      "summary": summary,
      "sql_length": sql_length,
      "render_ms": render_ms,
      "render_started_at": render_started_at,
      "render_finished_at": render_finished_at,
      "exec_started_at": exec_started_at,
      "exec_finished_at": exec_finished_at,
      "run_started_at": run_started_at,
      "run_finished_at": run_finished_at,
      "execution_ms": execution_ms,
    }

  return {
    "status": "success",
    "kind": "sql",
    "dataset": dataset_key,
    "message": None,
    "rows_affected": rows_affected,
    "load_run_id": load_run_id,
    "summary": summary,
    "sql_length": sql_length,
    "render_ms": render_ms,
    "render_started_at": render_started_at,
    "render_finished_at": render_finished_at,
    "exec_started_at": exec_started_at,
    "exec_finished_at": exec_finished_at,
    "run_started_at": run_started_at,
    "run_finished_at": run_finished_at,
    "execution_ms": execution_ms,
  }

def execute_raw_via_ingestion(
  *,
  target_dataset,
  target_system,
  profile,
  chunk_size=5000,
  batch_run_id=None,
):
  """
  Execute semantics for RAW: run ingestion instead of load-SQL.
  """
  src_ds = resolve_single_source_dataset_for_raw(target_dataset)

  mode = resolve_ingest_mode(src_ds)

  if mode == "none":
    # Allowed state only if landing_required=False, but then RAW shouldn't exist.
    # Still: be defensive.
    print(
      f"[INFO] RAW ingestion skipped for '{target_dataset.target_dataset_name}': "
      "include_ingest='none' (no ingestion mode)."
    )
    return {"status": "skipped", "reason": "include_ingest_none"}

  if mode == "external":
    # just validate existence (optional) + log
    print(
      f"[INFO] RAW ingestion is external for '{target_dataset.target_dataset_name}'. "
      "Assuming RAW is populated by an external tool."
    )
    return {"status": "skipped", "reason": "external_ingest"}

  if mode != "native":
    raise ValueError(f"Unknown ingest mode: {mode!r}")

  return ingest_raw_full(
    source_dataset=src_ds,
    target_system=target_system,
    profile=profile,
    chunk_size=chunk_size,
    batch_run_id=batch_run_id,
  )


def warn_if_stage_exec_without_raw(target_dataset):
  """
  If a stage dataset is fed directly from SourceDatasets (no upstream RAW),
  executing it inside the target system will usually fail (no cross-system SQL).
  """
  if target_dataset.target_schema.short_name != "stage":
    return

  inputs = list(target_dataset.input_links.select_related("source_dataset", "upstream_target_dataset"))
  has_upstream_raw = any(
    i.upstream_target_dataset is not None
    and i.upstream_target_dataset.target_schema.short_name == "raw"
    for i in inputs
  )
  has_direct_source = any(i.source_dataset is not None for i in inputs)

  if (not has_upstream_raw) and has_direct_source:
    print(
      "[INFO] This STAGE dataset is fed directly from SourceDatasets (no RAW landing upstream). "
      "Executing STAGE inside the target system typically requires either:\n"
      "  - enabling RAW landing (generate_raw_tables / generate_raw_table), or\n"
      "  - providing the source data inside the target context (federated/external).\n"
      "Otherwise target-only execution guards will prevent cross-system SQL."
    )


class Command(BaseCommand):
  help = (
    "Render (and in future: execute) load SQL for a target dataset.\n\n"
    "Example:\n"
    "  python manage.py elevata_load sap_customer --schema rawcore --dry-run\n"
  )

  def add_arguments(self, parser) -> None:
    # Required: target dataset name (logical name in the warehouse)
    parser.add_argument(
      "target_name",
      type=str,
      help="TargetDataset.target_dataset_name to load, e.g. 'sap_customer'.",
    )

    # Optional: disambiguate by target_schema.short_name
    parser.add_argument(
      "--schema",
      dest="schema_short",
      type=str,
      default=None,
      help="Optional target schema short_name to disambiguate, e.g. 'rawcore'.",
    )

    # Optional: explicit dialect override (else: env → profile)
    parser.add_argument(
      "--dialect",
      dest="dialect_name",
      type=str,
      default=None,
      help="Optional SQL dialect override, e.g. 'duckdb', 'postgres'.",
    )

    # Optional: explicit target system name (System.short_name)
    parser.add_argument(
      "--target-system",
      dest="target_system_name",
      type=str,
      default=None,
      help=(
        "Optional target System.short_name. If omitted, the value from "
        "ELEVATA_TARGET_SYSTEM is used (see metadata.config.targets)."
      ),
    )

    # Dry-run: render SQL only (default = True for now)
    parser.add_argument(
      "--execute",
      dest="execute",
      action="store_true",
      help=(
        "Execute the resolved operation.\n"
        "- For RAW targets: run ingestion (extract + load).\n"
        "- For downstream targets: execute generated SQL in the target system."
      ),
    )

    parser.add_argument(
      "--no-print",
      dest="no_print",
      action="store_true",
      help="Do not print the generated SQL (useful for future integrations).",
    )

    parser.add_argument(
      "--debug-plan",
      dest="debug_plan",
      action="store_true",
      help=(
        "Print the resolved LoadPlan (mode, handle_deletes, historize, "
        "delete_detection_enabled) before the SQL."
      ),
    )

    parser.add_argument(
      "--no-deps",
      dest="no_deps",
      action="store_true",
      help=(
        "Execute only the specified target dataset, without resolving or executing "
        "any upstream dependencies."
      ),
    )

    parser.add_argument(
    "--continue-on-error",
    dest="continue_on_error",
    action="store_true",
    help=(
      "Continue executing downstream datasets even if one dataset fails. "
      "The command will still exit with a non-zero status if any error occurred."
      ),
    )


  def _resolve_target_dataset(self, target_name: str, schema_short: str | None) -> TargetDataset:
    """
    Resolve a TargetDataset by target_dataset_name and optional schema short_name.
    """
    qs = TargetDataset.objects.filter(target_dataset_name=target_name)

    if schema_short:
      qs = qs.filter(target_schema__short_name=schema_short)

    try:
      return qs.get()
    except TargetDataset.DoesNotExist as exc:
      raise CommandError(
        f"TargetDataset with name='{target_name}'"
        + (f" and schema='{schema_short}'" if schema_short else "")
        + " not found."
      ) from exc
    except TargetDataset.MultipleObjectsReturned as exc:
      raise CommandError(
        f"Multiple TargetDatasets found for name='{target_name}'. "
        "Please specify --schema to disambiguate."
      ) from exc


  def handle(self, *args: Any, **options: Any) -> None:
    """
    Process load
    """
    target_name: str = options["target_name"]
    schema_short: str | None = options["schema_short"]
    dialect_name: str | None = options["dialect_name"]
    target_system_name: str | None = options["target_system_name"]
    execute: bool = options["execute"]
    no_print: bool = options["no_print"]
    debug_plan: bool = bool(options.get("debug_plan", False))
    no_deps: bool = bool(options.get("no_deps", False))
    continue_on_error: bool = bool(options.get("continue_on_error", False))

    # One batch_run_id for the entire run (all datasets).
    batch_run_id = str(uuid.uuid4())

    # 1) Resolve root dataset
    root_td = self._resolve_target_dataset(target_name, schema_short)

    # 2) Resolve profile
    profile = load_profile(None)

    # 3) Resolve target system
    try:
      system = get_target_system(target_system_name)
    except RuntimeError as exc:
      raise CommandError(str(exc))

    # 4) Resolve dialect (+ engine only in execute mode)
    dialect = get_active_dialect(dialect_name)

    engine = None
    if execute:
      engine = dialect.get_execution_engine(system)

    try:
      # 5) Resolve execution order
      if no_deps:
        execution_order = [root_td]
      else:
        execution_order = resolve_execution_order(root_td)

      # 6) Print plan
      if not no_print:
        self.stdout.write("")
        self.stdout.write(self.style.NOTICE(f"Execution plan (batch_run_id={batch_run_id}):"))
        for i, td in enumerate(execution_order, start=1):
          self.stdout.write(f"  {i}. {td.target_schema.short_name}.{td.target_dataset_name}")
        self.stdout.write("")

      # 7) Debug plan for root only (exact formatting expected by tests)
      root_load_plan = None
      if debug_plan:
        root_load_plan = build_load_plan(root_td)

        self.stdout.write("")
        self.stdout.write(self.style.WARNING("-- LoadPlan debug:"))

        mode = getattr(root_load_plan, "mode", None)
        handle_deletes = bool(getattr(root_load_plan, "handle_deletes", False))
        schema_short_local = getattr(root_td.target_schema, "short_name", None)

        delete_detection_enabled = (
          mode == "merge"
          and handle_deletes
          and schema_short_local == "rawcore"
        )

        self.stdout.write(f"  mode           = {mode}")
        self.stdout.write(f"  handle_deletes = {handle_deletes}")

        incr_src = getattr(root_td, "incremental_source", None)
        incr_src_name = getattr(incr_src, "source_dataset_name", None) if incr_src else None
        self.stdout.write(f"  incremental_source = {incr_src_name}")
        self.stdout.write(f"  delete_detection_enabled = {delete_detection_enabled}")
        self.stdout.write("")

      # Root-level logging: tests expect exactly one "starting" and one "finished"
      root_load_run_id = str(uuid.uuid4())

      logger.info(
        "elevata_load starting",
        extra={
          "batch_run_id": batch_run_id,
          "load_run_id": root_load_run_id,
          "target_dataset_id": getattr(root_td, "id", None),
          "target_dataset_name": root_td.target_dataset_name,
          "target_schema": root_td.target_schema.short_name,
          "profile": profile.name,
          "target_system": system.short_name,
          "target_system_type": system.type,
          "dialect": dialect.__class__.__name__,
          "execute": execute,
        },
      )

      # 8) Execute datasets in order and collect summary
      results: list[dict[str, object]] = []
      had_error = False

      for td in execution_order:
        this_load_run_id = root_load_run_id if td is root_td else None
        this_load_plan = root_load_plan if (td is root_td) else None

        try:
          result = run_single_target_dataset(
            stdout=self.stdout,
            style=self.style,
            target_dataset=td,
            target_system=system,
            target_system_engine=engine,
            profile=profile,
            dialect=dialect,
            execute=execute,
            no_print=no_print,
            debug_plan=False,
            debug_materialization=debug_plan,
            batch_run_id=batch_run_id,
            load_run_id=this_load_run_id,
            load_plan_override=this_load_plan,
            chunk_size=5000,
          )
          results.append(result)

        except Exception as exc:
          had_error = True

          results.append({
            "status": "error",
            "kind": "exception",
            "dataset": f"{td.target_schema.short_name}.{td.target_dataset_name}",
            "message": str(exc),
          })

          logger.exception(
            "elevata_load dataset failed",
            extra={
              "batch_run_id": batch_run_id,
              "target_dataset_name": td.target_dataset_name,
              "target_schema": td.target_schema.short_name,
            },
          )

          if not continue_on_error:
            # Ensure we still emit the "finished" log for root with best-effort fields
            break

      # 9) Execution summary
      if not no_print:
        self.stdout.write("")
        self.stdout.write(self.style.NOTICE(f"Execution summary (batch_run_id={batch_run_id}):"))

        for r in results:
          status = str(r.get("status", "unknown"))
          kind = str(r.get("kind", "unknown"))
          ds = str(r.get("dataset", "unknown"))

          symbol = "✔" if status in ("success", "dry_run", "skipped") else "✖"
          line = f" {symbol} {ds:<35} {kind}"

          msg = r.get("message")
          if msg:
            line += f" – {msg}"

          self.stdout.write(line)

        self.stdout.write("")

      # Root "finished" log should use root result timing/length if available
      root_result = None
      root_dataset_key = f"{root_td.target_schema.short_name}.{root_td.target_dataset_name}"
      for r in results:
        if r.get("dataset") == root_dataset_key:
          root_result = r
          break

      sql_length = int(root_result.get("sql_length", 0)) if root_result else 0
      render_ms = float(root_result.get("render_ms", 0.0)) if root_result else 0.0
      started_at = root_result.get("started_at") if root_result else None
      finished_at = root_result.get("finished_at") if root_result else None

      logger.info(
        "elevata_load finished",
        extra={
          "batch_run_id": batch_run_id,
          "load_run_id": root_load_run_id,
          "target_dataset_id": getattr(root_td, "id", None),
          "target_dataset_name": root_td.target_dataset_name,
          "sql_length": sql_length,
          "execute": execute,
          "started_at": started_at.isoformat() if started_at else None,
          "finished_at": finished_at.isoformat() if finished_at else None,
          "render_ms": render_ms,
        },
      )

      # If we stopped early due to error and continue_on_error is False, re-raise now
      if had_error and not continue_on_error:
        raise CommandError(
          "Load execution failed. See execution summary above for details."
        )

      if had_error:
        raise CommandError(
          "One or more datasets failed during execution. "
          "See execution summary above for details."
        )

    finally:
      # Always close the execution engine if it supports close()
      if engine is not None:
        close = getattr(engine, "close", None)
        if callable(close):
          close()