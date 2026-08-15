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

from metadata.promotion.snapshot import (
  EnvironmentMetadataSnapshotError,
  serialize_environment_metadata_snapshot,
)
from metadata.promotion.snapshot_builder import (
  EnvironmentMetadataSnapshotBuildError,
  build_environment_metadata_snapshot,
)


class Command(BaseCommand):
  help = "Export the complete portable environment metadata snapshot."

  def add_arguments(self, parser):
    parser.add_argument(
      "--environment-label",
      required=True,
      dest="environment_label",
      help="Logical environment label recorded in the snapshot, e.g. dev or prod.",
    )
    parser.add_argument(
      "--created-by",
      dest="created_by",
      help="Optional actor recorded as snapshot provenance.",
    )
    parser.add_argument(
      "--output",
      dest="output_path",
      help="Write the immutable snapshot JSON to a new file.",
    )
    parser.add_argument(
      "--fingerprint-only",
      action="store_true",
      dest="fingerprint_only",
      help="Print only the stable snapshot fingerprint.",
    )
    parser.add_argument(
      "--compact",
      action="store_true",
      dest="compact",
      help="Render compact canonical JSON instead of indented JSON.",
    )

  def handle(self, *args, **options):
    output_path = options.get("output_path")
    fingerprint_only = bool(options.get("fingerprint_only"))
    compact = bool(options.get("compact"))

    if output_path and fingerprint_only:
      raise CommandError(
        "Use either --output or --fingerprint-only, not both."
      )

    try:
      snapshot = build_environment_metadata_snapshot(
        environment_label=options["environment_label"],
        created_by=options.get("created_by"),
      )
    except (
      EnvironmentMetadataSnapshotBuildError,
      EnvironmentMetadataSnapshotError,
    ) as exc:
      raise CommandError(str(exc)) from exc

    if fingerprint_only:
      self.stdout.write(snapshot.snapshot_fingerprint)
      return

    rendered = serialize_environment_metadata_snapshot(
      snapshot,
      pretty=not compact,
    )

    if output_path:
      path = Path(output_path)
      path.parent.mkdir(parents=True, exist_ok=True)
      try:
        with path.open("x", encoding="utf-8", newline="\n") as handle:
          handle.write(rendered)
          if compact:
            handle.write("\n")
      except FileExistsError as exc:
        raise CommandError(
          f"Snapshot output already exists and will not be overwritten: {path}"
        ) from exc

      self.stdout.write(self.style.SUCCESS(
        f"Environment metadata snapshot written to {path}"
      ))
      self.stdout.write(
        f"Snapshot fingerprint: {snapshot.snapshot_fingerprint}"
      )
      self.stdout.write(
        f"Metadata fingerprint: {snapshot.metadata_fingerprint}"
      )
      return

    self.stdout.write(rendered, ending="")
