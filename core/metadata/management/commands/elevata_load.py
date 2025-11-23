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

# metadata/management/commands/elevata_load.py

from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand, CommandError

from metadata.config.profiles import load_profile
from metadata.config.targets import get_target_system
from metadata.models import TargetDataset
from metadata.rendering.dialects import get_active_dialect
from metadata.rendering.load_sql import render_load_sql_for_target


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

  def handle(self, *args: Any, **options: Any) -> None:
    target_name: str = options["target_name"]
    schema_short: str | None = options["schema_short"]
    dialect_name: str | None = options["dialect_name"]
    target_system_name: str | None = options["target_system_name"]
    execute: bool = options["execute"]
    no_print: bool = options["no_print"]

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

    # 5) Render load SQL
    sql = render_load_sql_for_target(td, dialect)

    # 6) Dry-run vs execute
    if not no_print:
      self.stdout.write("")
      self.stdout.write(self.style.NOTICE(f"-- Profile: {profile.name}"))
      self.stdout.write(self.style.NOTICE(f"-- Target system: {system.short_name} (type={system.type})"))
      self.stdout.write(self.style.NOTICE(f"-- Dialect: {dialect.__class__.__name__}"))
      self.stdout.write("")
      self.stdout.write(sql)

    if execute:
      # TODO: implement execution against the target system connection.
      # This will use:
      #   - profile.secret_ref_template / overrides
      #   - the resolved System (type + short_name)
      #   - a secret provider (env, Azure Key Vault, ...)
      #
      # For now: make it explicit that this is not yet implemented.
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
