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

logger = logging.getLogger(__name__)


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
        "Execute the generated SQL against the resolved target system. "
        "Not implemented yet; currently only renders SQL."
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

    # 9) Logging: Ende of run
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

    if execute:
      # TODO: implement execution against the target system connection.
      # This will use:
      #   - profile.secret_ref_template / overrides
      #   - the resolved System (type + short_name)
      #   - a secret provider (env, Azure Key Vault, ...)

      raise CommandError(
        "Execute mode is not implemented yet. Use --dry-run (default) to inspect the SQL."
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
