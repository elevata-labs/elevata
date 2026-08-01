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

from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from django.contrib.auth import get_user_model
from metadata.generation.target_generation_control import (
  TargetGenerationApprovalStore,
  TargetGenerationControlError,
  build_target_generation_review,
  render_target_generation_review_json,
  render_target_generation_review_text,
)
from metadata.generation.target_generation_guarded_apply import (
  TargetGenerationPlanApplyError,
)
from metadata.generation.target_generation_plan import (
  parse_target_generation_plan_json,
  render_target_generation_plan_json,
)
from metadata.generation.target_generation_service import TargetGenerationService
from metadata.generation.security import get_runtime_pepper


def _render_dry_run_summary(plan) -> str:
  """Render one concise human-readable summary from a generation plan."""
  schema_label = ", ".join(plan.target_schema_short_names)
  action_summary = ", ".join(
    f"{count} {action_type}"
    for action_type, count in plan.action_counts.items()
    if count
  )
  if not action_summary:
    action_summary = "no metadata changes"

  return (
    f"[DRY-RUN] {schema_label}: {plan.action_count} planned metadata "
    f"actions ({action_summary})."
  )


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
    parser.add_argument(
      "--plan-file",
      dest="plan_file",
      help=(
        "Apply one previously generated canonical Target Generation Plan "
        "JSON file with source and target drift guards."
      ),
    )
    parser.add_argument(
      "--plan-output",
      dest="plan_output",
      help=(
        "Write a single-schema dry-run plan as canonical JSON to this file."
      ),
    )
    parser.add_argument(
      "--review-output",
      dest="review_output",
      help=(
        "Write the single-schema Source-to-Target Generation Review as "
        "canonical JSON to this file."
      ),
    )
    parser.add_argument(
      "--generation-approval-file",
      dest="generation_approval_file",
      help=(
        "Validate this Generation Approval artifact before applying --plan-file."
      ),
    )
    parser.add_argument(
      "--require-generation-approval",
      action="store_true",
      dest="require_generation_approval",
      help=(
        "Require a matching Generation Approval. Without an explicit file, "
        "resolve it from the Architecture Control approval store."
      ),
    )

  def handle(self, *args, **options):
    actor = None
    actor_id = options.get("actor_id")
    if actor_id:
      User = get_user_model()
      actor = User.objects.filter(pk=actor_id).first()

    schema_short_name = options.get("schema_short_name")
    dry_run = options.get("dry_run", False)
    plan_file = options.get("plan_file")
    plan_output = options.get("plan_output")
    review_output = options.get("review_output")
    generation_approval_file = options.get("generation_approval_file")
    require_generation_approval = options.get(
      "require_generation_approval",
      False,
    )

    pepper = get_runtime_pepper()
    svc = TargetGenerationService(pepper=pepper, actor=actor)

    if plan_file:
      if dry_run or schema_short_name or plan_output or review_output:
        raise CommandError(
          "--plan-file cannot be combined with --dry-run, --schema, "
          "--plan-output, or --review-output."
        )
      try:
        plan_text = Path(plan_file).read_text(encoding="utf-8")
        plan = parse_target_generation_plan_json(plan_text)
        review = build_target_generation_review(plan)
        approval = None
        if generation_approval_file:
          approval = TargetGenerationApprovalStore.load_file(
            generation_approval_file,
          )
        elif require_generation_approval:
          approval = TargetGenerationApprovalStore().load_for_review_fingerprint(
            review.review_fingerprint,
          )
          if approval is None:
            raise TargetGenerationControlError(
              "No stored Generation Approval exists for the current "
              "Target Generation Review fingerprint."
            )
        result = svc.apply_plan(
          plan,
          approval=approval,
          require_approval=require_generation_approval,
        )
      except (
        OSError,
        ValueError,
        TargetGenerationControlError,
        TargetGenerationPlanApplyError,
      ) as exc:
        raise CommandError(str(exc)) from exc

      if approval is not None:
        self.stdout.write(
          self.style.SUCCESS(
            f"Generation Approval {approval.approval_id} verified for "
            f"review {review.review_fingerprint[:12]}."
          )
        )
      self.stdout.write(self.style.SUCCESS(result.summary_text))
      return

    if generation_approval_file or require_generation_approval:
      raise CommandError(
        "Generation Approval options require --plan-file."
      )
    if plan_output and not dry_run:
      raise CommandError("--plan-output requires --dry-run.")
    if review_output and not dry_run:
      raise CommandError("--review-output requires --dry-run.")
    if (plan_output or review_output) and not schema_short_name:
      raise CommandError(
        "--plan-output and --review-output require --schema so exactly "
        "one artifact is written."
      )

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
    dry_run_plan_count = 0
    dry_run_action_count = 0

    for schema in schemas:
      eligible = svc.get_eligible_source_datasets_for_schema(schema)

      if not eligible and not dry_run:
        self.stdout.write(
          self.style.WARNING(
            f"{schema.physical_prefix or schema.short_name}: "
            "no eligible source datasets; reconciling generated target lifecycle."
          )
        )

      if dry_run:
        plan = svc.build_plan(
          eligible,
          schema,
          reconcile_lifecycle=True,
        )
        self.stdout.write(
          self.style.WARNING(_render_dry_run_summary(plan))
        )
        rendered_plan = render_target_generation_plan_json(plan)
        self.stdout.write(rendered_plan, ending="")
        review = build_target_generation_review(plan)
        self.stdout.write(render_target_generation_review_text(review), ending="")
        if plan_output:
          try:
            Path(plan_output).write_text(
              rendered_plan,
              encoding="utf-8",
            )
          except OSError as exc:
            raise CommandError(
              f"Could not write Target Generation Plan: {exc}"
            ) from exc
          self.stdout.write(
            self.style.SUCCESS(
              f"Target Generation Plan written to {plan_output}."
            )
          )
        if review_output:
          try:
            Path(review_output).write_text(
              render_target_generation_review_json(review),
              encoding="utf-8",
            )
          except OSError as exc:
            raise CommandError(
              f"Could not write Target Generation Review: {exc}"
            ) from exc
          self.stdout.write(
            self.style.SUCCESS(
              f"Target Generation Review written to {review_output}."
            )
          )
        dry_run_plan_count += 1
        dry_run_action_count += plan.action_count
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
      self.stdout.write(
        self.style.WARNING(
          f"Dry-run completed. Plans: {dry_run_plan_count}; planned metadata "
          f"actions: {dry_run_action_count}. No changes were written."
        )
      )
