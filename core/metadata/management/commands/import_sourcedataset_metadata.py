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

from django.core.management.base import BaseCommand, CommandError
from models import SourceDataset
from ingestion.import_service import import_metadata_for_datasets

def parse_dataset_key(key: str):
  """Accepts SYSTEM.SCHEMA.TABLE or SYSTEM..TABLE (empty schema) and returns (system, schema, table)."""
  parts = key.split(".")
  if len(parts) < 2:
    raise CommandError(f"Invalid dataset key '{key}'. Use SYSTEM.SCHEMA.TABLE (schema may be empty).")
  system = parts[0]
  if len(parts) == 2:
    # Interpret as SYSTEM.TABLE (no schema provided)
    return (system, None, parts[1])
  # len >= 3
  schema = parts[1] if parts[1] != "" else None
  table = ".".join(parts[2:])  # support dots in table name just in case
  return (system, schema, table)

class Command(BaseCommand):
  help = "Imports SourceColumn metadata for selected SourceDatasets."

  def add_arguments(self, parser):
    parser.add_argument("--all", action="store_true", help="Import all datasets with get_metadata=True")
    parser.add_argument("--system", action="append", help="Filter by SourceSystem short_name (can be repeated)")
    parser.add_argument("--dataset", action="append", help="Filter by dataset key SYSTEM.SCHEMA.TABLE; can be repeated")

  def handle(self, *args, **options):
    qs = SourceDataset.objects.all().select_related("source_system")
    ds_filters_applied = False

    if options["all"]:
      qs = qs.filter(get_metadata=True)
      ds_filters_applied = True

    systems = options.get("system") or []
    if systems:
      qs = qs.filter(source_system__short_name__in=systems)
      ds_filters_applied = True

    dataset_keys = options.get("dataset") or []
    if dataset_keys:
      cond = None
      for key in dataset_keys:
        sys_name, schema, table = parse_dataset_key(key)
        sub = SourceDataset.objects.filter(
          source_system__short_name=sys_name,
          source_dataset=table
        )
        if schema is None:
          sub = sub.filter(schema__isnull=True) | SourceDataset.objects.filter(
            source_system__short_name=sys_name, schema="", source_dataset=table
          )
        else:
          sub = sub.filter(schema=schema)
        cond = sub if cond is None else (cond | sub)
      if cond is not None:
        qs = qs.filter(id__in=cond.values("id"))
      ds_filters_applied = True

    if not ds_filters_applied:
      raise CommandError("No filters provided. Use --all or --system or --dataset.")

    count = qs.count()
    if count == 0:
      self.stdout.write(self.style.WARNING("No SourceDatasets matched the filters. Nothing to do."))
      return

    self.stdout.write(f"Importing metadata for {count} dataset(s)...")

    # Run import (service handles per-system engine reuse + idempotency)
    results = import_metadata_for_datasets(qs)

    # Summarize
    total_cols = sum(results.values()) if results else 0
    self.stdout.write(self.style.SUCCESS(
      f"Done. Imported {total_cols} columns across {len(results)} dataset(s)."
    ))
