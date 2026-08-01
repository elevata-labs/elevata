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

from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from metadata.generation.target_generation_control import (
  TargetGenerationApprovalStore,
  TargetGenerationControlError,
  build_target_generation_approval,
  load_plan_and_review,
  render_target_generation_approval_json,
)


class Command(BaseCommand):
  """Create a Generation Approval for one exact Target Generation Plan."""

  help = (
    "Create a distinct Generation Approval that authorizes metadata mutation "
    "of one exact Target Generation Plan."
  )

  def add_arguments(self, parser):
    parser.add_argument(
      "plan_file",
      help="Canonical Target Generation Plan JSON file to review and approve.",
    )
    parser.add_argument(
      "--approved-by",
      dest="approved_by",
      required=True,
      help="Reviewer identity recorded in the Generation Approval.",
    )
    parser.add_argument(
      "--note",
      dest="note",
      default="",
      help="Optional review note.",
    )
    parser.add_argument(
      "--decided-at",
      dest="decided_at",
      help="Optional deterministic UTC decision timestamp.",
    )
    parser.add_argument(
      "--output",
      dest="output",
      help="Optional output path for the canonical approval JSON.",
    )
    parser.add_argument(
      "--store",
      action="store_true",
      dest="store",
      help="Store the approval in the Architecture Control approval store.",
    )

  def handle(self, *args, **options):
    try:
      _plan, review = load_plan_and_review(options["plan_file"])
      approval = build_target_generation_approval(
        review=review,
        decided_by=options["approved_by"],
        note=options.get("note", ""),
        decided_at=options.get("decided_at"),
      )
      rendered = render_target_generation_approval_json(approval)

      output = options.get("output")
      if output:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")
        self.stdout.write(
          self.style.SUCCESS(
            f"Target Generation Approval written to {output_path}."
          )
        )

      if options.get("store"):
        stored_path = TargetGenerationApprovalStore().save(approval)
        self.stdout.write(
          self.style.SUCCESS(
            f"Target Generation Approval stored at {stored_path}."
          )
        )

      if not output and not options.get("store"):
        self.stdout.write(rendered, ending="")

      self.stdout.write(
        self.style.SUCCESS(
          f"Generation Approval {approval.approval_id} binds plan "
          f"{review.plan_fingerprint[:12]} and review "
          f"{review.review_fingerprint[:12]}."
        )
      )
    except (OSError, ValueError, TargetGenerationControlError) as exc:
      raise CommandError(str(exc)) from exc
