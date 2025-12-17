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

from metadata.intent.ingestion import resolve_ingest_mode
from metadata.ingestion.native_raw import ingest_raw_full


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


def apply_runtime_placeholders(sql: str, *, dialect, load_run_id: str, load_timestamp) -> str:
  if not sql:
    return sql

  ts_sql = _render_literal_for_dialect(dialect, load_timestamp)
  id_sql = _render_literal_for_dialect(dialect, load_run_id)

  # support both "{{ load_timestamp }}" and "{ load_timestamp }" variants
  sql = re.sub(r"\{\{\s*load_timestamp\s*\}\}", ts_sql, sql)
  sql = re.sub(r"\{\s*load_timestamp\s*\}", ts_sql, sql)

  sql = re.sub(r"\{\{\s*load_run_id\s*\}\}", id_sql, sql)
  sql = re.sub(r"\{\s*load_run_id\s*\}", id_sql, sql)

  return sql


def should_truncate_before_load(td, load_plan) -> bool:
  schema_short = getattr(td.target_schema, "short_name", None)
  mode = getattr(load_plan, "mode", None)

  if schema_short in ("stage", "raw"):
    return True

  if schema_short == "rawcore" and mode == "full":
    return True

  return False


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
    # MVP: just validate existence (optional) + log
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
    target_name: str = options["target_name"]
    schema_short: str | None = options["schema_short"]
    dialect_name: str | None = options["dialect_name"]
    target_system_name: str | None = options["target_system_name"]
    execute: bool = options["execute"]
    no_print: bool = options["no_print"]
    debug_plan: bool = bool(options.get("debug_plan", False))

    # A single command run may load one or more datasets in the future.
    # We still generate a batch_run_id now so that we have a consistent
    # grouping key for all per-dataset load_run_ids.
    batch_run_id = str(uuid.uuid4())

    # 1) Resolve TargetDataset
    td = self._resolve_target_dataset(target_name, schema_short)

    # 2) Resolve profile (for logging / later connection handling)
    profile = load_profile(None)

    # 3) Resolve target system (for future execute mode)
    try:
      system = get_target_system(target_system_name)
    except RuntimeError as exc:
      raise CommandError(str(exc))

    # 4) Resolve dialect (env → profile → fallback)
    dialect = get_active_dialect(dialect_name)
    # NOTE:
    # We deliberately do NOT resolve an execution engine yet.
    # Execute mode is still unimplemented; tests expect a CommandError
    # and not any interaction with a concrete engine.

    # 5) Build LoadPlan and optionally print debug info
    load_plan = build_load_plan(td)

    if debug_plan:
      self.stdout.write("")
      self.stdout.write(self.style.WARNING("-- LoadPlan debug:"))

      mode = getattr(load_plan, "mode", None)
      handle_deletes = bool(getattr(load_plan, "handle_deletes", False))
      schema_short = getattr(td.target_schema, "short_name", None)

      # sehr einfache Heuristik: wann *kann* Delete Detection greifen?
      delete_detection_enabled = (
        mode == "merge"
        and handle_deletes
        and schema_short == "rawcore"
      )

      self.stdout.write(f"  mode           = {mode}")
      self.stdout.write(f"  handle_deletes = {handle_deletes}")

      incr_src = getattr(td, "incremental_source", None)
      incr_src_name = getattr(incr_src, "source_dataset_name", None) if incr_src else None
      self.stdout.write(f"  incremental_source = {incr_src_name}")
      self.stdout.write(f"  delete_detection_enabled = {delete_detection_enabled}")
      self.stdout.write("")

    # 5a) Build a compact summary used for logging and optional debug output
    summary = build_load_run_summary(td, dialect, load_plan)

    # Per-dataset load_run_id (nested inside the batch)
    load_run_id = str(uuid.uuid4())

    # 6) Logging: Start of run
    logger.info(
      "elevata_load starting",
      extra={
        "batch_run_id": batch_run_id,
        "load_run_id": load_run_id,
        "target_dataset_id": getattr(td, "id", None),
        "target_dataset_name": td.target_dataset_name,
        "target_schema": td.target_schema.short_name,
        "profile": profile.name,
        "target_system": system.short_name,
        "target_system_type": system.type,
        "dialect": dialect.__class__.__name__,
        "execute": execute,
        "load_mode": summary["mode"],
        "load_handle_deletes": summary["handle_deletes"],
        "load_historize": summary["historize"],
      },
    )

    # --- RAW is special: --execute triggers ingestion, not SQL execution ---
    if td.target_schema.short_name == "raw":
      if not execute:
        if not no_print:
          self.stdout.write("")
          self.stdout.write(self.style.NOTICE(
            "-- RAW datasets are ingested. Use --execute to run ingestion (extract + load)."
          ))
        return

      # Execute RAW via ingestion (native/external/none handled internally)
      result = execute_raw_via_ingestion(
        target_dataset=td,
        target_system=system,
        profile=profile,
        chunk_size=5000,
        batch_run_id=batch_run_id,
      )

      if not no_print:
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(f"-- RAW ingestion result: {result}"))
      return

    # 7) Render load SQL
    started_at = now()
    start_ts = time.perf_counter()
    sql = render_load_sql_for_target(td, dialect)
    render_ms = (time.perf_counter() - start_ts) * 1000.0
    finished_at = now()
    sql_length = len(sql)

    # 8) Dry-run vs execute
    if not no_print:
      self.stdout.write("")
      self.stdout.write(self.style.NOTICE(f"-- Profile: {profile.name}"))
      self.stdout.write(self.style.NOTICE(f"-- Target system: {system.short_name} (type={system.type})"))
      self.stdout.write(self.style.NOTICE(f"-- Dialect: {dialect.__class__.__name__}"))
      self.stdout.write(self.style.NOTICE(f"-- Load summary: {format_load_run_summary(summary)}"))
      self.stdout.write(self.style.NOTICE(f"-- SQL length: {sql_length} chars"))
      self.stdout.write(self.style.NOTICE(f"-- Render time: {render_ms:.1f} ms"))
      self.stdout.write("")
      self.stdout.write(sql)

    # 9) Logging: end of run (render phase)
    logger.info(
      "elevata_load finished",
      extra={
        "batch_run_id": batch_run_id,
        "load_run_id": load_run_id,
        "target_dataset_id": getattr(td, "id", None),
        "target_dataset_name": td.target_dataset_name,
        "sql_length": len(sql or ""),
        "execute": execute,
        "load_mode": summary["mode"],
        "load_handle_deletes": summary["handle_deletes"],
        "load_historize": summary["historize"],
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "render_ms": render_ms,
      },
    )

    if not execute:
      return

    # 10) Execute mode: run SQL and log to warehouse
    engine = dialect.get_execution_engine(system)

    # Ensure target schema exists (optional auto-provision)
    ensure_target_schema(
      engine=engine,
      dialect=dialect,
      schema_name=td.target_schema.schema_name,
      auto_provision=AUTO_PROVISION_SCHEMAS,
    )

    # Ensure target table exists (optional auto-provision)
    ensure_target_table(
      engine=engine,
      dialect=dialect,
      td=td,
      auto_provision=AUTO_PROVISION_TABLES,
    )

    # Ensure meta.load_run_log exists (optional auto-provision)
    ensure_load_run_log_table(
      engine=engine,
      dialect=dialect,
      meta_schema=META_SCHEMA_NAME,
      auto_provision=AUTO_PROVISION_META_LOG,
    )

    exec_start_ts = time.perf_counter()
    exec_started_at = now()

    rows_affected: int | None = None
    load_status = "success"
    error_message: str | None = None

    # Guardrail: execute mode is target-only (no cross-system/source connectivity yet)
    warn_if_stage_exec_without_raw(td)

    if _looks_like_cross_system_sql(sql, td.target_schema.schema_name):
      raise CommandError(
        "Execute mode currently supports target-only SQL. "
        "This dataset SQL appears to reference source objects outside the target schema "
        "(e.g. 'Production.Product'). "
        "Please ingest/load the raw table into the target first (or seed it), "
        "then run downstream layers (rawcore/hist) using --execute."
      )

    if should_truncate_before_load(td, load_plan):
      trunc_sql = dialect.render_truncate_table(
        schema=td.target_schema.schema_name,
        table=td.target_dataset_name,
      )
      engine.execute(trunc_sql)

    try:
      exec_ts = now()  # or started_at of execution
      sql_exec = apply_runtime_placeholders(
        sql,
        dialect=dialect,
        load_run_id=load_run_id,
        load_timestamp=exec_ts,
      )
      rows_affected = engine.execute(sql_exec)

    except Exception as exc:
      load_status = "error"
      error_message = str(exc)

    exec_finished_at = now()
    execution_ms = (time.perf_counter() - exec_start_ts) * 1000.0

    # Insert warehouse log row (only if the dialect supports it)
    if hasattr(dialect, "render_insert_load_run_log"):
      log_insert_sql = dialect.render_insert_load_run_log(
        meta_schema=META_SCHEMA_NAME,
        batch_run_id=batch_run_id,
        load_run_id=load_run_id,
        summary=summary,
        profile=profile,
        system=system,
        started_at=started_at,
        finished_at=finished_at,
        render_ms=render_ms,
        execution_ms=execution_ms,
        sql_length=sql_length,
        rows_affected=rows_affected,
        load_status=load_status,
        error_message=error_message,
      )
      if log_insert_sql:
        engine.execute(log_insert_sql)

    # Optionally re-raise error after logging
    if load_status == "error":
      raise CommandError(f"Load execution failed: {error_message}")

