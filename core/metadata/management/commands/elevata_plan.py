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

from metadata.architecture.report_builder import build_architecture_change_report
from metadata.architecture.renderers import (
  render_architecture_report_json,
  render_architecture_report_text,
)
from metadata.architecture.scope import (
  ArchitectureScopeError,
  resolve_dataset_keys_from_state,
)
from metadata.architecture.service import ArchitectureStateService
from metadata.architecture.store import ArchitectureStateStore
from metadata.materialization.policy import load_materialization_policy


class Command(BaseCommand):
  help = "Render deterministic architecture change reports."

  def add_arguments(self, parser):
    parser.add_argument(
      "target_name",
      nargs="?",
      help="Target dataset name or dataset key.",
    )
    parser.add_argument(
      "--all",
      action="store_true",
      dest="all_datasets",
      help="Include all active target datasets.",
    )
    parser.add_argument(
      "--schema",
      dest="schema_short",
      help="Restrict the report to a target schema short name.",
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
      "--previous-state",
      dest="previous_state_path",
      help="Architecture state JSON file used as comparison baseline.",
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
    target_name = options.get("target_name")
    all_datasets = bool(options.get("all_datasets"))
    schema_short = options.get("schema_short")
    output_format = options.get("output_format") or "text"
    output_path = options.get("output_path")
    previous_state_path = options.get("previous_state_path")
    fail_on_changes = bool(options.get("fail_on_changes"))
    fail_on_blocked = bool(options.get("fail_on_blocked"))
    fail_on_destructive = bool(options.get("fail_on_destructive"))

    if target_name and all_datasets:
      raise CommandError("Use either a target dataset or --all, not both.")

    if not target_name and not all_datasets:
      raise CommandError("Specify a target dataset or use --all.")

    service = ArchitectureStateService()
    if previous_state_path:
      previous_state = ArchitectureStateStore.load_file(previous_state_path)
      if previous_state is None:
        raise CommandError(
          f"Architecture state file could not be read: {previous_state_path}"
        )
    else:
      previous_state = service.load_previous_state()
    current_state = service.build_current_state()

    try:
      relevant_dataset_keys = resolve_dataset_keys_from_state(
        state=current_state,
        target_name=target_name,
        schema_short=schema_short,
        all_datasets=all_datasets,
        include_related_hist=True,
      )
    except ArchitectureScopeError as exc:
      raise CommandError(str(exc))

    report = build_architecture_change_report(
      previous_state=previous_state,
      current_state=current_state,
      policy=load_materialization_policy(),
      relevant_dataset_keys=relevant_dataset_keys,
      schema_short=schema_short,
      target_name=target_name,
    )

    if output_format == "json":
      rendered = render_architecture_report_json(report)
    else:
      rendered = render_architecture_report_text(report)

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
    raise CommandError("Architecture plan contains blocking policy decisions.", returncode=2)

  if fail_on_destructive and any(d.destructive for d in report.policy_decisions):
    raise CommandError("Architecture plan contains destructive actions.", returncode=3)

  if fail_on_changes and report.has_changes:
    raise CommandError("Architecture plan contains changes.", returncode=1)