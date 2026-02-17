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

from typing import Any, Iterable
import json
import time
import logging
import os
import re
import uuid
import hashlib
from pathlib import Path
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
from metadata.execution.load_graph import resolve_execution_order, resolve_execution_order_all
from metadata.execution.executor import build_execution_plan, execute_plan, ExecutionPolicy
from metadata.execution.snapshot import (
  build_execution_snapshot,
  render_execution_snapshot_json,
  write_execution_snapshot_file,
)
from metadata.execution.load_run_snapshot_store import (
  build_load_run_snapshot_row,
  ensure_load_run_snapshot_table,
  render_select_load_run_snapshot_json,
  fetch_one_value,
)
from metadata.execution.snapshot_diff import (
  diff_execution_snapshots,
  render_execution_snapshot_diff_text,
)
from metadata.materialization.policy import load_materialization_policy
from metadata.materialization.planner import build_materialization_plan
from metadata.materialization.applier import apply_materialization_plan
from metadata.materialization.logging import LOAD_RUN_SNAPSHOT_REGISTRY, build_load_run_log_row, ensure_load_run_log_table
from metadata.materialization.schema import ensure_target_schema
from metadata.ingestion.connectors import engine_for_target

logger = logging.getLogger(__name__)


def _resolve_td_timestamp_best_effort(td: TargetDataset) -> str:
  """
  Best-effort last-changed marker for a TargetDataset.
  We intentionally support multiple attribute names because different projects
  use different audit field conventions.
  """
  for attr in ("updated_at", "modified_at", "last_modified", "changed_at", "created_at"):
    v = getattr(td, attr, None)
    if v:
      try:
        return v.isoformat()  # timezone-aware datetime
      except Exception:
        return str(v)
  return "<?>"


def _compute_execution_plan_fingerprint(target_datasets: Iterable[TargetDataset]) -> str:
  """
  Compute a stable fingerprint for the planned execution set.
  Used to detect metadata/contract changes between plan creation and execution.
  """
  parts: list[str] = []
  for td in target_datasets:
    schema = getattr(getattr(td, "target_schema", None), "short_name", "<?>")
    name = getattr(td, "target_dataset_name", "<?>")
    mode = str(getattr(td, "incremental_strategy", "") or "")
    mat = str(getattr(td, "materialization_type", "") or "")
    ts = _resolve_td_timestamp_best_effort(td)
    parts.append(f"{schema}.{name}|{mode}|{mat}|{ts}")

  payload = "\n".join(sorted(parts)).encode("utf-8")
  return hashlib.sha256(payload).hexdigest()


def _fingerprint_for_execution_ids(execution_ids: list[int]) -> str:
  """
  Recompute the fingerprint from the DB for the given TargetDataset ids.
  This detects changes that happened after the plan was built.
  """
  qs = (
    TargetDataset.objects
    .select_related("target_schema")
    .filter(pk__in=execution_ids)
  )
  # Important: the queryset order is not guaranteed; the fingerprint sorts internally.
  return _compute_execution_plan_fingerprint(list(qs))


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
    "bizcore",
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
  attempt_no: int = 1,
  no_type_changes: bool = False,
  fail_on_type_drift: bool = False,
  allow_lossy_type_drift: bool = False,
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
    mat_policy = replace(mat_policy, allow_lossy_type_drift=bool(allow_lossy_type_drift))

    if AUTO_PROVISION_TABLES and schema_short in mat_policy.sync_schema_shorts:
      # Introspection: Databricks should prefer exec_engine-based introspection
      # to respect Unity Catalog session context (USE CATALOG).
      target_sa_engine = None
      if (target_system.type or "").lower() != "databricks":
        target_sa_engine = engine_for_target(
          target_short_name=target_system.short_name,
          system_type=target_system.type,
        )

      if debug_materialization and not no_print:
        # Guard debug output when SQLAlchemy introspection engine is intentionally disabled.
        db = ""
        if target_sa_engine is not None and getattr(target_sa_engine, "url", None) is not None:
          try:
            db = target_sa_engine.url.database or ""
          except Exception:
            db = ""
        stdout.write(style.NOTICE(
          f"-- Introspection engine: {'sqlalchemy' if target_sa_engine is not None else 'exec_engine_only'}"
        ))
        stdout.write(style.NOTICE(
          f"-- Introspection DB absolute: {os.path.abspath(db)}"
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

      # Full refresh will DROP+CREATE later (recreate), so column-level DDL is redundant and can be risky
      # across dialects (e.g., SQL Server does not support "ADD COLUMN").
      # Keep only schema ensure + dataset rename (rename helps us target the right object name).
      if is_full_refresh:
        keep_ops = {"ENSURE_SCHEMA", "RENAME_DATASET"}
        plan.steps = [s for s in plan.steps if getattr(s, "op", None) in keep_ops]

      # do NOT dispose yet - hist_plan uses the same engine below

      # ------------------------------------------------------------------
      # Preflight: deterministic drift findings before applying DDL
      # ------------------------------------------------------------------
      type_drift_warnings = [
        w for w in (plan.warnings or [])
        if str(w).startswith("TYPE_DRIFT:")
      ]

      if type_drift_warnings and not no_print:
        stdout.write(style.WARNING(
          f"-- Preflight: type drift detected for {dataset_key} "
          f"({len(type_drift_warnings)} column(s))."
        ))
        for w in type_drift_warnings:
          stdout.write(style.WARNING(f"-- Preflight warning: {w}"))

      # For full refresh (recreate), type drift is informational only: the table will be rebuilt.
      type_drift_warnings_for_block = [] if is_full_refresh else type_drift_warnings

      # If type changes are disabled, do not execute ALTER/rebuild steps.
      # We block deterministically on any drift (unless overridden via allow-lossy flag).
      if no_type_changes:
        # Remove type-evolution DDL steps from the plan to guarantee no changes happen.
        # This covers ALTER_COLUMN_TYPE and the rebuild ops planned for widening in the planner.
        drop_ops = {
          "ALTER_COLUMN_TYPE",
          "DROP_TABLE_IF_EXISTS",
          "CREATE_TABLE",
          "INSERT_SELECT",
          "DROP_TABLE",
          "RENAME_TABLE",
        }
        plan.steps = [s for s in (plan.steps or []) if getattr(s, "op", None) not in drop_ops]

        if type_drift_warnings_for_block and not allow_lossy_type_drift:
          raise CommandError(
            f"Preflight blocked for {dataset_key}: type drift detected but schema evolution is disabled "
            f"(--no-type-changes). Remove --no-type-changes to allow safe widening remediation."
          )

      # Decide whether to block deterministically.
      # - fail_on_type_drift: blocks on ANY drift
      # - default: block on narrowing/incompatible drift (unless allow_lossy_type_drift)
      if type_drift_warnings_for_block:
        if fail_on_type_drift:
          raise CommandError(f"Preflight blocked for {dataset_key}: type drift detected (--fail-on-type-drift).")

        if not allow_lossy_type_drift:
          lossy = []
          for w in type_drift_warnings_for_block:
            s = str(w)
            m = re.search(r"kind=([a-z_]+)", s)
            kind = (m.group(1) if m else "").strip().lower()
            if kind in ("narrowing", "incompatible"):
              lossy.append(s)
          if lossy:
            raise CommandError(
              f"Preflight blocked for {dataset_key}: narrowing/incompatible type drift detected. "
              "Apply schema evolution (ALTER/rebuild) or override with --allow-lossy-type-drift."
            )

      # ------------------------------------------------------------------
      # allow-lossy-type-drift override:
      # The planner marks narrowing/incompatible drift as blocking (UNSAFE_TYPE_DRIFT).
      # If the user explicitly allows lossy drift, do not block on those findings.
      # The database may still reject the DDL (e.g., VARCHAR length shrink), which
      # will surface as an execution error instead of a preflight block.
      # ------------------------------------------------------------------
      if allow_lossy_type_drift and plan.blocking_errors:
        plan.blocking_errors = [
          e for e in (plan.blocking_errors or [])
          if not str(e).startswith("UNSAFE_TYPE_DRIFT:")
        ]

      # For full refresh (recreate), unsafe type drift is not blocking:
      # the table will be dropped and created with the desired schema anyway.
      # Keep other blocking errors (e.g., missing schema/table metadata) intact.
      if is_full_refresh and plan.blocking_errors:
        plan.blocking_errors = [
          e for e in (plan.blocking_errors or [])
          if not (
            str(e).startswith("UNSAFE_TYPE_DRIFT:")
            or str(e).startswith("UNSUPPORTED_TYPE_EVOLUTION:")
          )
        ]

      # Blocking planner errors should fail deterministically before any DDL/SQL execution.
      if plan.blocking_errors:
        if not no_print:
          for e in plan.blocking_errors:
            stdout.write(style.ERROR(f"-- Preflight blocked: {e}"))
        msg = "; ".join([str(e) for e in plan.blocking_errors])
        raise CommandError(f"Preflight blocked for {dataset_key}: {msg}")

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

            # Apply the same type-change disabling policy to hist plans as well.
            if no_type_changes:
              drop_ops = {
                "ALTER_COLUMN_TYPE",
                "DROP_TABLE_IF_EXISTS",
                "CREATE_TABLE",
                "INSERT_SELECT",
                "DROP_TABLE",
                "RENAME_TABLE",
              }
              hist_plan.steps = [
                s for s in (hist_plan.steps or [])
                if getattr(s, "op", None) not in drop_ops
              ]

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

            # Always apply hist sync (best-effort), regardless of no_type_changes.
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
        cascade=False,
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
      
    def _is_comment_only_sql(stmt: str) -> bool:
      s = (stmt or "").strip()
      if not s:
        return True
      for line in s.splitlines():
        t = line.strip()
        if not t:
          continue
        if t.startswith("--"):
          continue
        return False
      return True

    if _is_comment_only_sql(sql):
      raise CommandError(
        f"Non-executable SQL was rendered for {dataset_key}. "
        f"Please check incremental_strategy and materialization_type.\n"
        f"{sql}"
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

    def _sanitize_sql_string(value: str, max_len: int = 1500) -> str:
      # Make error messages safe for SQL VALUES('...') insertion across dialects.
      # - Replace newlines to avoid multi-line string literal issues in some engines.
      # - Escape single quotes using SQL standard quoting.
      # - Keep it bounded to avoid oversized log rows.
      s = (value or "")
      s = s.replace("\r\n", "\n").replace("\r", "\n")
      s = s.replace("\n", " ")
      s = s.replace("'", "''")
      s = " ".join(s.split())
      if len(s) > max_len:
        s = s[: max_len - 3] + "..."
      return s

    error_message = _sanitize_sql_string(error_message)

  exec_finished_at = now()
  execution_ms = (time.perf_counter() - exec_start_ts) * 1000.0

  # For execute mode, the overall run ends after execution.
  run_finished_at = exec_finished_at

  # Insert warehouse log row (only if the dialect supports it)
  if hasattr(dialect, "render_insert_load_run_log"):
    values = build_load_run_log_row(
      batch_run_id=batch_run_id,
      load_run_id=load_run_id,
      target_schema=td.target_schema.short_name,
      target_dataset=td.target_dataset_name,
      target_system=target_system.short_name,
      profile=profile.name,
      mode=str(summary.get("mode") or getattr(load_plan, "mode", None) or "full"),
      handle_deletes=bool(summary.get("handle_deletes") or False),
      historize=bool(summary.get("historize") or False),
      started_at=run_started_at,
      finished_at=run_finished_at,
      render_ms=render_ms,
      execution_ms=execution_ms,
      sql_length=sql_length,
      rows_affected=rows_affected,
      status=load_status,
      error_message=error_message,
      attempt_no=attempt_no,
      status_reason=("retry_exhausted" if (load_status == "error" and attempt_no > 1) else None),
      blocked_by=None,
    )

    log_insert_sql = dialect.render_insert_load_run_log(
      meta_schema=META_SCHEMA_NAME,
      values=values,
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
  # Stdout guardrails for --all plan printing
  PLAN_PRINT_HEAD = 25
  PLAN_PRINT_TAIL = 10

  help = (
    "Render (and in future: execute) load SQL for a target dataset.\n\n"
    "Example:\n"
    "  python manage.py elevata_load sap_customer --schema rawcore --dry-run\n"
  )

  def add_arguments(self, parser) -> None:
    # Optional: target dataset name (logical name in the warehouse)
    # can be omitted when --all is used.
    parser.add_argument(
      "target_name",
      type=str,
      nargs="?",
      default=None,
      help=(
        "Target dataset to execute (default mode).\n"
        "Executes the dataset and all its upstream dependencies."
        "Omit when using --all."
      ),
    )

    parser.add_argument(
      "--all",
      dest="all_datasets",
      action="store_true",
      help=(
        "Execute all datasets in deterministic dependency order. "
        "Use --schema to scope roots to one target schema (dependencies are still included). "
        "Ignores target_name if provided."
      ),
    )

    # Optional: disambiguate by target_schema.short_name
    parser.add_argument(
      "--schema",
      dest="schema_short",
      type=str,
      default=None,
      help=(
        "Disambiguate dataset selection by schema (for single-dataset mode),\n"
        "or scope execution to a schema when used with --all.\n"
        "Examples:\n"
        "  elevata_load my_dataset --schema raw\n"
        "  elevata_load --all --schema rawcore"
      ),
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
        "- For downstream targets: execute generated SQL in the target system. "
        "If omitted, elevata runs in dry-run mode."
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
      "--no-plan-guard",
      action="store_true",
      help="Disable execution plan predictability guard (not recommended).",
    )

    parser.add_argument(
      "--no-type-changes",
      action="store_true",
      help=(
        "Disable automatic schema evolution for type widening (ALTER/rebuild). "
        "If type drift is detected, preflight will block unless overridden."
      ),
    )

    parser.add_argument(
      "--fail-on-type-drift",
      action="store_true",
      help="Fail the run if any schema type drift is detected (CI/strict mode).",
    )

    parser.add_argument(
      "--allow-lossy-type-drift",
      action="store_true",
      help="Allow narrowing/incompatible type drift to proceed (not recommended).",
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

    parser.add_argument(
      "--max-retries",
      dest="max_retries",
      type=int,
      default=0,
      help=(
        "Retry failed dataset executions up to N additional times (execute-mode only). "
        "0 means: no retries."
      ),
    )

    parser.add_argument(
      "--debug-execution",
      dest="debug_execution",
      action="store_true",
      default=False,
      help="Print an execution snapshot (plan + policy + outcomes) as JSON to stdout.",
    )

    parser.add_argument(
      "--write-execution-snapshot",
      dest="write_execution_snapshot",
      action="store_true",
      default=False,
      help="Write an execution snapshot JSON file to disk (best-effort).",
    )

    parser.add_argument(
      "--execution-snapshot-dir",
      dest="execution_snapshot_dir",
      type=str,
      default="./log/execution_snapshots",
      help="Directory to write execution snapshot JSON files into.",
    )

    parser.add_argument(
      "--diff-against-snapshot",
      dest="diff_against_snapshot",
      type=str,
      default=None,
      help="Path to a baseline load run snapshot JSON file to diff against (best-effort).",
    )

    parser.add_argument(
      "--diff-print",
      dest="diff_print",
      action="store_true",
      default=False,
      help="Print the snapshot diff to stdout (best-effort).",
    )

    parser.add_argument(
      "--diff-against-batch-run-id",
      dest="diff_against_batch_run_id",
      type=str,
      default=None,
      help="Baseline batch_run_id to diff against (loaded from meta.load_run_snapshot).",
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

  def _validate_root_selection(
    self,
    *,
    target_name: str | None,
    all_datasets: bool,
  ) -> None:
    """
    Validate CLI selection rules for root dataset(s).
    """
    if all_datasets and target_name:
      raise CommandError("Invalid arguments: do not pass target_name together with --all.")

    if (not all_datasets) and (not target_name):
      raise CommandError("Missing target_name. Provide a dataset name or use --all.")

  def _style_warning(self, text: str) -> str:
    """
    Best-effort warning style helper.
    Some tests stub style and may not implement WARNING.
    """
    fn = getattr(self.style, "WARNING", None)
    return fn(text) if callable(fn) else text

  def _print_execution_plan(
    self,
    *,
    execution_order: list["TargetDataset"],
    batch_run_id: str,
    all_datasets: bool,
    schema_short: str | None,
    no_print: bool,
  ) -> None:
    """
    Print execution plan in a deterministic, stdout-safe way.
    In --all mode, avoid flooding stdout by printing head+tail with an ellipsis line.
    """
    if no_print:
      return

    self.stdout.write("")

    if all_datasets:
      scope = f", schema={schema_short}" if schema_short else ""
      self.stdout.write(self.style.NOTICE(
        f"Execution plan (all datasets{scope}, batch_run_id={batch_run_id}):"
      ))

      n = len(execution_order)
      head = int(getattr(self, "PLAN_PRINT_HEAD", 25))
      tail = int(getattr(self, "PLAN_PRINT_TAIL", 10))

      def _fmt(td) -> str:
        return f"{td.target_schema.short_name}.{td.target_dataset_name}"

      if n <= (head + tail + 5):
        for i, td in enumerate(execution_order, start=1):
          self.stdout.write(f"  {i}. {_fmt(td)}")
      else:
        for i, td in enumerate(execution_order[:head], start=1):
          self.stdout.write(f"  {i}. {_fmt(td)}")
        self.stdout.write(self._style_warning(f"  ... ({n - head - tail} more)"))

        start_idx = n - tail + 1
        for off, td in enumerate(execution_order[-tail:], start=0):
          self.stdout.write(f"  {start_idx + off}. {_fmt(td)}")
    else:
      self.stdout.write(self.style.NOTICE(f"Execution plan (batch_run_id={batch_run_id}):"))
      for i, td in enumerate(execution_order, start=1):
        self.stdout.write(f"  {i}. {td.target_schema.short_name}.{td.target_dataset_name}")

    self.stdout.write("")

  def handle(self, *args: Any, **options: Any) -> None:
    """
    Process load
    """
    target_name: str | None = options.get("target_name")
    schema_short: str | None = options["schema_short"]
    all_datasets: bool = bool(options.get("all_datasets", False))    
    dialect_name: str | None = options["dialect_name"]
    target_system_name: str | None = options["target_system_name"]
    execute: bool = options["execute"]
    no_print: bool = options["no_print"]
    debug_plan: bool = bool(options.get("debug_plan", False))
    no_deps: bool = bool(options.get("no_deps", False))
    continue_on_error: bool = bool(options.get("continue_on_error", False))
    max_retries: int = int(options.get("max_retries") or 0)
    no_plan_guard = bool(options.get("no_plan_guard"))
    no_type_changes = bool(options.get("no_type_changes"))
    fail_on_type_drift = bool(options.get("fail_on_type_drift"))
    allow_lossy_type_drift = bool(options.get("allow_lossy_type_drift"))
    debug_execution: bool = bool(options.get("debug_execution", False))
    write_execution_snapshot: bool = bool(options.get("write_execution_snapshot", False))
    execution_snapshot_dir: str = str(options.get("execution_snapshot_dir") or ".elevata/execution_snapshots")
    diff_against_snapshot: str | None = options.get("diff_against_snapshot")
    diff_print: bool = bool(options.get("diff_print", False))
    diff_against_batch_run_id: str | None = options.get("diff_against_batch_run_id")

    # One batch_run_id for the entire run (all datasets).
    batch_run_id = str(uuid.uuid4())
    created_at = now()

    # 0) Validate selection
    self._validate_root_selection(
      target_name=target_name,
      all_datasets=all_datasets,
    )    

    # 1) Resolve root dataset(s)
    root_td = None
    roots: list[TargetDataset] = []

    if all_datasets:
      qs = TargetDataset.objects.all()
      # In --all mode, --schema scopes the root set (dependencies are still included).
      if schema_short:
        qs = qs.filter(target_schema__short_name=schema_short)
      roots = list(qs)
      if not roots:
        raise CommandError(
          "No TargetDatasets found"
          + (f" for --schema='{schema_short}'" if schema_short else "")
          + "."
        )
      # Pick a deterministic representative root for existing root-level logging semantics.
      roots_sorted = sorted(roots, key=lambda d: (d.target_schema.short_name, d.target_dataset_name))
      root_td = roots_sorted[0]
    else:
      root_td = self._resolve_target_dataset(str(target_name), schema_short)
      roots = [root_td]

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
    if execute or diff_against_batch_run_id:
      engine = dialect.get_execution_engine(system)

    try:
      # 5) Resolve execution order
      if all_datasets:
        if no_deps:
          # no_deps in --all means: run only the selected roots (no upstream expansion)
          execution_order = sorted(roots, key=lambda d: (d.target_schema.short_name, d.target_dataset_name))
        else:
          execution_order = resolve_execution_order_all(roots)
      else:
        if no_deps:
          execution_order = [root_td]
        else:
          execution_order = resolve_execution_order(root_td)

      # 5.1) Predictability guard baseline: compute a fingerprint for the planned set.
      # We store ids to re-check deterministically against the same planned set.
      # Predictability guard baseline:
      # If we have real DB-backed TargetDataset objects (pk present), re-check against DB.
      # If not (e.g. unit tests with DummyTD), fall back to an in-memory fingerprint.
      execution_ids = []
      missing_pk = False
      for td in execution_order:
        pk = getattr(td, "pk", None)
        if pk is None:
          missing_pk = True
          continue
        execution_ids.append(int(pk))

      plan_fingerprint = None
      use_db_fingerprint = (not missing_pk)

      if not no_plan_guard:
        if use_db_fingerprint:
          plan_fingerprint = _fingerprint_for_execution_ids(execution_ids)
        else:
          plan_fingerprint = _compute_execution_plan_fingerprint(execution_order)

      policy = ExecutionPolicy(
        continue_on_error=continue_on_error,
        max_retries=max_retries,
      )      

      plan = build_execution_plan(batch_run_id=batch_run_id, execution_order=execution_order)

      # 6) Print plan
      self._print_execution_plan(
        execution_order=execution_order,
        batch_run_id=batch_run_id,
        all_datasets=all_datasets,
        schema_short=schema_short,
        no_print=no_print,
      )

      if not no_print:
        if no_plan_guard:
          self.stdout.write(self.style.WARNING("Execution plan guard DISABLED (--no-plan-guard)."))
        else:
          self.stdout.write(self.style.NOTICE(f"Plan fingerprint: {plan_fingerprint}"))
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
      def _run_dataset_fn(*, target_dataset, batch_run_id, load_run_id, load_plan_override, attempt_no):

        # Predictability guard: detect metadata/contract drift after plan creation.
        if not no_plan_guard:
          if use_db_fingerprint:
            current_fingerprint = _fingerprint_for_execution_ids(execution_ids)
          else:
            current_fingerprint = _compute_execution_plan_fingerprint(execution_order)

          if current_fingerprint != plan_fingerprint:
            ds = f"{target_dataset.target_schema.short_name}.{target_dataset.target_dataset_name}"
            raise CommandError(
              "Execution plan is stale: metadata/contract changed after plan creation. "
              f"dataset={ds} expected_fingerprint={plan_fingerprint} current_fingerprint={current_fingerprint}"
            )

        return run_single_target_dataset(
          stdout=self.stdout,
          style=self.style,
          target_dataset=target_dataset,
          target_system=system,
          target_system_engine=engine,
          profile=profile,
          dialect=dialect,
          execute=execute,
          no_print=no_print,
          debug_plan=False,
          debug_materialization=debug_plan,
          batch_run_id=batch_run_id,
          load_run_id=load_run_id,
          load_plan_override=load_plan_override,
          chunk_size=5000,
          attempt_no=attempt_no,
          no_type_changes=no_type_changes,
          fail_on_type_drift=fail_on_type_drift,
          allow_lossy_type_drift=allow_lossy_type_drift,   
        )

      results, had_error = execute_plan(
        plan=plan,
        execution_order=execution_order,
        policy=policy,
        execute=bool(execute),
        root_td=root_td,
        root_load_run_id=root_load_run_id,
        root_load_plan=root_load_plan,
        run_dataset_fn=_run_dataset_fn,
        logger=logger,
      )

      # 8.0) Build + persist load_run_snapshot (best-effort)
      root_dataset_key = f"{root_td.target_schema.short_name}.{root_td.target_dataset_name}"

      snapshot = build_execution_snapshot(
        batch_run_id=batch_run_id,
        policy=policy,
        plan=plan,
        execute=bool(execute),
        no_deps=bool(no_deps),
        continue_on_error=bool(continue_on_error),
        max_retries=int(max_retries),
        profile_name=profile.name,
        target_system_short=system.short_name,
        target_system_type=system.type,
        dialect_name=dialect.__class__.__name__,
        root_dataset_key=root_dataset_key,
        created_at=created_at,
        results=results,
        had_error=had_error,
      )

      if debug_execution and not no_print:
        self.stdout.write("")
        self.stdout.write(self.style.WARNING("-- Execution snapshot (JSON):"))
        self.stdout.write(render_execution_snapshot_json(snapshot))
        self.stdout.write("")

      if write_execution_snapshot:
        try:
          path = write_execution_snapshot_file(
            snapshot=snapshot,
            snapshot_dir=execution_snapshot_dir,
            batch_run_id=batch_run_id,
          )
          if not no_print:
            self.stdout.write(self.style.NOTICE(f"Execution snapshot written: {path}"))
        except Exception:
          # Best-effort only: never block the run because of snapshot writing.
          pass

      # Persist snapshot to meta.load_run_snapshot (best-effort)
      if execute and engine is not None:
        try:
          ensure_load_run_snapshot_table(
            engine=engine,
            dialect=dialect,
            meta_schema=META_SCHEMA_NAME,
            auto_provision=AUTO_PROVISION_META_LOG,
          )

          snapshot_json = render_execution_snapshot_json(snapshot)

          row = build_load_run_snapshot_row(
            batch_run_id=batch_run_id,
            created_at=created_at,
            root_dataset_key=root_dataset_key,
            is_execute=bool(execute),
            continue_on_error=bool(continue_on_error),
            max_retries=int(max_retries),
            had_error=bool(had_error),
            step_count=len(plan.steps),
            snapshot_json=snapshot_json,
          )

          sql = dialect.render_insert_load_run_snapshot(
            meta_schema=META_SCHEMA_NAME,
            values=row,
          )
          if sql:
            engine.execute(sql)

        except Exception:
          # Best-effort: must never block the run
          pass

      # Snapshot diff (best-effort): DB baseline preferred, file baseline fallback
      baseline = None

      # Prefer DB baseline if provided (meta.load_run_snapshot)
      if diff_against_batch_run_id and engine is not None:
        try:
          sql = render_select_load_run_snapshot_json(
            dialect=dialect,
            meta_schema=META_SCHEMA_NAME,
            batch_run_id=str(diff_against_batch_run_id),
          )
          snapshot_json = fetch_one_value(engine, sql)

          if diff_print and not no_print:
            self.stdout.write(self.style.WARNING(
              f"-- Snapshot diff debug: baseline query returned type={type(snapshot_json)} len={len(snapshot_json) if isinstance(snapshot_json, str) else 'n/a'}"
            ))

          if snapshot_json:
            baseline = json.loads(snapshot_json)
        except Exception:
          baseline = None

      # Fallback: file baseline
      if baseline is None and diff_against_snapshot:
        try:
          baseline_json = Path(diff_against_snapshot).read_text(encoding="utf-8")
          baseline = json.loads(baseline_json)
        except Exception:
          baseline = None

      # Optional hint for live runs (only when user explicitly asked to print diffs)
      if diff_print and not no_print and diff_against_batch_run_id and baseline is None:
        self.stdout.write(self.style.WARNING(
          f"-- Snapshot diff: baseline batch_run_id not found in {META_SCHEMA_NAME}.load_run_snapshot: {diff_against_batch_run_id}"
        ))

      if baseline is not None:
        try:
          diff = diff_execution_snapshots(left=baseline, right=snapshot)

          if diff_print and not no_print:
            left_id = str(baseline.get("batch_run_id") or "baseline")
            right_id = str(snapshot.get("batch_run_id") or "current")
            self.stdout.write("")
            self.stdout.write(self.style.WARNING("-- Snapshot diff:"))
            self.stdout.write(render_execution_snapshot_diff_text(
              diff=diff,
              left_batch_run_id=left_id,
              right_batch_run_id=right_id,
            ))
        except Exception:
          pass

      # 8.1) Persist orchestration-only outcomes (blocked/aborted) to meta.load_run_log
      # Best-effort: must never block the load runner.
      if execute and engine is not None and hasattr(dialect, "render_insert_load_run_log"):
        try:
          ensure_load_run_log_table(
            engine=engine,
            dialect=dialect,
            meta_schema=META_SCHEMA_NAME,
            auto_provision=AUTO_PROVISION_META_LOG,
          )

          for r in results:
            if r.get("status") != "skipped":
              continue
            if r.get("kind") not in ("blocked", "aborted"):
              continue

            ds = str(r.get("dataset") or "")
            if "." not in ds:
              continue
            target_schema, target_dataset = ds.split(".", 1)

            # Use per-step load_run_id if provided, otherwise generate one.
            load_run_id = str(r.get("load_run_id") or uuid.uuid4())

            ts = now()
            values = build_load_run_log_row(
              batch_run_id=batch_run_id,
              load_run_id=load_run_id,
              target_schema=target_schema,
              target_dataset=target_dataset,
              target_system=system.short_name,
              profile=profile.name,
              # Orchestration-only rows: still need non-null semantics fields
              mode="orchestration",
              handle_deletes=False,
              historize=False,
              started_at=ts,
              finished_at=ts,
              render_ms=0.0,
              execution_ms=0.0,
              sql_length=0,
              rows_affected=None,
              status="skipped",
              error_message=str(r.get("message") or None),
              # v0.8.0 extra fields (you added these in logging.py)
              attempt_no=int(r.get("attempt_no") or 1),
              status_reason=str(r.get("status_reason") or None),
              blocked_by=str(r.get("blocked_by") or None),
            )

            log_insert_sql = dialect.render_insert_load_run_log(
              meta_schema=META_SCHEMA_NAME,
              values=values,
            )
            if log_insert_sql:
              engine.execute(log_insert_sql)
        except Exception:
          # Never block execution due to meta logging inserts
          pass

      # 9) Execution summary
      if not no_print:
        self.stdout.write("")
        if all_datasets:
          scope = f", schema={schema_short}" if schema_short else ""
          self.stdout.write(self.style.NOTICE(
            f"Execution summary (all datasets{scope}, batch_run_id={batch_run_id}):"
          ))
        else:
          self.stdout.write(self.style.NOTICE(f"Execution summary (batch_run_id={batch_run_id}):"))

        for r in results:
          status = str(r.get("status", "unknown"))
          kind = str(r.get("kind", "unknown"))
          ds = str(r.get("dataset", "unknown"))

          status_lc = (status or "").lower()
          kind_lc = (kind or "").lower()

          if status_lc in ("success", "dry_run"):
            symbol = "✔"
          elif status_lc == "blocked" and kind_lc == "preflight":
            symbol = "⚠"
          elif status_lc == "skipped" and kind_lc in ("blocked", "aborted"):
            symbol = "⏸"
          elif status_lc == "skipped":
            symbol = "⏭"
          else:
            symbol = "✖"

          line = f" {symbol} {ds:<35} {kind}"

          msg = r.get("message")
          if msg:
            # Shorten noisy preflight messages for readability
            if status_lc == "blocked" and kind_lc == "preflight":
              if "UNSAFE_TYPE_DRIFT" in msg:
                line += " – blocked by UNSAFE_TYPE_DRIFT"
              else:
                line += " – blocked by schema preflight checks"
            else:
              line += f" – {msg}"

          self.stdout.write(line)

          # Optional hint line for actionable preflight blocks
          if status_lc == "blocked" and kind_lc == "preflight":
            if msg and "UNSAFE_TYPE_DRIFT" in msg:
              self.stdout.write(
                "     hint: use --allow-lossy-type-drift to allow explicit narrowing/rebuild"
              )          

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

      def _iso(x):
        return x.isoformat() if hasattr(x, "isoformat") else x

      logger.info(
        "elevata_load finished",
        extra={
          "batch_run_id": batch_run_id,
          "load_run_id": root_load_run_id,
          "target_dataset_id": getattr(root_td, "id", None),
          "target_dataset_name": root_td.target_dataset_name,
          "sql_length": sql_length,
          "execute": execute,
          "started_at": _iso(started_at) if started_at else None,
          "finished_at": _iso(finished_at) if finished_at else None,
          "render_ms": render_ms,
        },
      )

      # If we stopped early due to error and continue_on_error is False, re-raise now
      if had_error and not continue_on_error:
        statuses = [str((r or {}).get("status") or "") for r in (results or [])]
        kinds = [str((r or {}).get("kind") or "") for r in (results or [])]
        has_exception = any(s in ("error", "exception") for s in statuses)
        has_preflight_block = any((s == "blocked" and k == "preflight") for s, k in zip(statuses, kinds))

        if has_exception:
          raise CommandError("Load execution failed. See execution summary above for details.")
        if has_preflight_block:
          raise CommandError("Load blocked by preflight checks. See execution summary above for details.")
        raise CommandError("Load execution failed. See execution summary above for details.")      

      if had_error:
        statuses = [str((r or {}).get("status") or "") for r in (results or [])]
        kinds = [str((r or {}).get("kind") or "") for r in (results or [])]
        has_exception = any(s in ("error", "exception") for s in statuses)
        has_preflight_block = any((s == "blocked" and k == "preflight") for s, k in zip(statuses, kinds))

        if has_exception:
          raise CommandError(
            "One or more datasets failed during execution. "
            "See execution summary above for details."
          )
        if has_preflight_block:
          raise CommandError(
            "One or more datasets were blocked by preflight checks. "
            "See execution summary above for details."
          )
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
