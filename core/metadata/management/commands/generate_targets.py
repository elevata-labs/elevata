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

"""
Management command to generate or update target datasets (raw, stage, rawcore, ...).

It reuses TargetGenerationService so that the same logic can be triggered
from CLI, CI/CD, or the web UI.
"""

from django.core.management.base import BaseCommand, CommandError

from django.contrib.auth import get_user_model
from metadata.generation.target_generation_service import TargetGenerationService
from metadata.generation.security import get_runtime_pepper


class Command(BaseCommand):
  help = "Generate or update target datasets for all configured target schemas."

  def add_arguments(self, parser):
    parser.add_argument(
      "--schema",
      "-s",
      dest="schema_short_name",
      help=(
        "Optional short_name of a single TargetSchema to generate "
        "(e.g. 'raw', 'stage', 'rawcore'). If omitted, all schemas "
        "from TargetGenerationService.get_target_schemas_in_scope() are processed."
      ),
    )
    parser.add_argument(
      "--actor-id",
      dest="actor_id",
      type=int,
      required=False,
      help="User ID to attribute system-managed generated metadata to.",
    )
    parser.add_argument(
      "--dry-run",
      action="store_true",
      dest="dry_run",
      help="Only show what would be generated; do not write to the database.",
    )

  def handle(self, *args, **options):
    actor = None
    actor_id = options.get("actor_id")
    if actor_id:
      User = get_user_model()
      actor = User.objects.filter(pk=actor_id).first()

    schema_short_name = options.get("schema_short_name")
    dry_run = options.get("dry_run", False)

    pepper = get_runtime_pepper()
    svc = TargetGenerationService(pepper=pepper, actor=actor)

    schemas = svc.get_target_schemas_in_scope()
    if schema_short_name:
      schemas = [s for s in schemas if s.short_name == schema_short_name]
      if not schemas:
        raise CommandError(f"No TargetSchema with short_name='{schema_short_name}' in scope.")

    if not schemas:
      self.stdout.write(self.style.WARNING("No target schemas in scope. Nothing to do."))
      return

    total_processed_datasets = 0
    total_processed_columns = 0
    total_retired_datasets = 0
    total_reactivated_datasets = 0

    for schema in schemas:
      eligible = svc.get_eligible_source_datasets_for_schema(schema)
      if not eligible:
        if dry_run:
          self.stdout.write(
            self.style.WARNING(
              f"[DRY-RUN] {schema.physical_prefix or schema.short_name}: "
              "no eligible source datasets."
            )
          )
          continue

        self.stdout.write(
          self.style.WARNING(
            f"{schema.physical_prefix or schema.short_name}: "
            "no eligible source datasets; reconciling generated target lifecycle."
          )
        )

      if dry_run:
        # Only show which schema would be processed
        self.stdout.write(
          f"[DRY-RUN] {schema.physical_prefix or schema.short_name}: "
          f"{len(eligible)} eligible source datasets."
        )
        continue

      # Run generation for this schema
      result = svc.apply_all_result(
        eligible,
        schema,
        reconcile_lifecycle=True,
      )
      self.stdout.write(
        self.style.SUCCESS(
          f"{schema.physical_prefix or schema.short_name}: {result.summary_text}"
        )
      )

      total_processed_datasets += result.processed_dataset_count
      total_processed_columns += result.processed_column_count
      total_retired_datasets += result.retired_dataset_count
      total_reactivated_datasets += result.reactivated_dataset_count

    if not dry_run:
      self.stdout.write(
        self.style.SUCCESS(
          f"Done. Total: {total_processed_datasets} target datasets processed and "
          f"{total_processed_columns} target columns processed; "
          f"{total_retired_datasets} target datasets retired and "
          f"{total_reactivated_datasets} reactivated."
        )
      )
    else:
      self.stdout.write(self.style.WARNING("Dry-run completed. No changes were written."))
