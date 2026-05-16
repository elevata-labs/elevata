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

from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from metadata.architecture.promotion import (
  ArchitecturePromotionError,
  build_architecture_promotion_report,
)
from metadata.architecture.renderers import (
  render_architecture_promotion_report_json,
  render_architecture_promotion_report_text,
)
from metadata.architecture.scope import (
  ArchitectureScopeError,
  resolve_dataset_keys_from_state,
)
from metadata.architecture.store import ArchitectureStateStore
from metadata.materialization.policy import load_materialization_policy


class Command(BaseCommand):
  help = "Render deterministic architecture promotion reports."

  def add_arguments(self, parser):
    parser.add_argument(
      "source_state",
      help="Source architecture state JSON file.",
    )
    parser.add_argument(
      "target_state",
      help="Target architecture state JSON file.",
    )
    parser.add_argument(
      "--source-label",
      default="source",
      dest="source_label",
      help="Label used for the source state in the report.",
    )
    parser.add_argument(
      "--target-label",
      default="target",
      dest="target_label",
      help="Label used for the target state in the report.",
    )
    parser.add_argument(
      "--schema",
      dest="schema_short",
      help="Restrict the report to a target schema short name.",
    )
    parser.add_argument(
      "--target-dataset",
      dest="target_dataset",
      help="Restrict the report to a target dataset name or dataset key.",
    )
    parser.add_argument(
      "--format",
      choices=("text", "json"),
      default="text",
      dest="output_format",
      help="Report output format.",
    )
    parser.add_argument(
      "--output",
      dest="output_path",
      help="Write the rendered report to a file.",
    )
    parser.add_argument(
      "--fail-on-changes",
      action="store_true",
      dest="fail_on_changes",
      help="Return a non-zero exit code when architecture changes are present.",
    )
    parser.add_argument(
      "--fail-on-blocked",
      action="store_true",
      dest="fail_on_blocked",
      help="Return a non-zero exit code when policy decisions block execution.",
    )
    parser.add_argument(
      "--fail-on-destructive",
      action="store_true",
      dest="fail_on_destructive",
      help="Return a non-zero exit code when destructive actions are present.",
    )

  def handle(self, *args, **options):
    source_path = options["source_state"]
    target_path = options["target_state"]
    source_label = options.get("source_label") or "source"
    target_label = options.get("target_label") or "target"
    schema_short = options.get("schema_short")
    target_dataset = options.get("target_dataset")
    output_format = options.get("output_format") or "text"
    output_path = options.get("output_path")
    fail_on_changes = bool(options.get("fail_on_changes"))
    fail_on_blocked = bool(options.get("fail_on_blocked"))
    fail_on_destructive = bool(options.get("fail_on_destructive"))

    source_state = ArchitectureStateStore.load_file(source_path)
    if source_state is None:
      raise CommandError(f"Architecture state file could not be read: {source_path}")

    target_state = ArchitectureStateStore.load_file(target_path)
    if target_state is None:
      raise CommandError(f"Architecture state file could not be read: {target_path}")

    try:
      relevant_dataset_keys = resolve_dataset_keys_from_state(
        state=target_state,
        target_name=target_dataset,
        schema_short=schema_short,
        all_datasets=target_dataset is None,
        include_related_hist=True,
      )
    except ArchitectureScopeError as exc:
      raise CommandError(str(exc))

    try:
      report = build_architecture_promotion_report(
        source_state=source_state,
        target_state=target_state,
        policy=load_materialization_policy(),
        source_label=source_label,
        target_label=target_label,
        relevant_dataset_keys=relevant_dataset_keys,
        schema_short=schema_short,
      )
    except ArchitecturePromotionError as exc:
      raise CommandError(str(exc))

    if output_format == "json":
      rendered = render_architecture_promotion_report_json(report)
    else:
      rendered = render_architecture_promotion_report_text(report)

    if output_path:
      Path(output_path).write_text(rendered, encoding="utf-8")
    else:
      self.stdout.write(rendered)

    _apply_exit_policy(
      report=report,
      fail_on_changes=fail_on_changes,
      fail_on_blocked=fail_on_blocked,
      fail_on_destructive=fail_on_destructive,
    )


def _apply_exit_policy(
  *,
  report,
  fail_on_changes: bool,
  fail_on_blocked: bool,
  fail_on_destructive: bool,
) -> None:
  """
  Apply command exit policy after rendering the report.
  """
  if fail_on_blocked and report.is_blocked:
    raise CommandError("Architecture promotion contains blocking policy decisions.", returncode=2)

  if fail_on_destructive and any(d.destructive for d in report.change_report.policy_decisions):
    raise CommandError("Architecture promotion contains destructive actions.", returncode=3)

  if fail_on_changes and report.has_changes:
    raise CommandError("Architecture promotion contains changes.", returncode=1)