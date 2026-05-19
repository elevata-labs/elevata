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

import json
from json import JSONDecodeError
from pathlib import Path
from typing import Any

from django.core.management.base import BaseCommand, CommandError

from metadata.architecture.approval import (
  ArchitectureApprovalError,
  ArchitectureApprovalStore,
  build_architecture_approval_artifact,
)
from metadata.architecture.renderers import (
  render_architecture_approval_json,
  render_architecture_approval_text,
)


class Command(BaseCommand):
  help = "Create deterministic architecture approval artifacts."

  def add_arguments(self, parser):
    parser.add_argument(
      "report",
      help="Architecture Change Report JSON file.",
    )
    parser.add_argument(
      "--approved-by",
      required=True,
      dest="approved_by",
      help="Reviewer name stored in the approval artifact.",
    )
    parser.add_argument(
      "--note",
      default="",
      dest="note",
      help="Review note stored in the approval artifact.",
    )
    parser.add_argument(
      "--decided-at",
      dest="decided_at",
      help="Review decision timestamp in ISO-8601 form.",
    )
    parser.add_argument(
      "--format",
      choices=("json", "text"),
      default="json",
      dest="output_format",
      help="Approval artifact output format.",
    )
    parser.add_argument(
      "--output",
      dest="output_path",
      help="Write the approval artifact to a file.",
    )
    parser.add_argument(
      "--fingerprint-only",
      action="store_true",
      dest="fingerprint_only",
      help="Print only the approval artifact fingerprint.",
    )
    parser.add_argument(
      "--store",
      action="store_true",
      dest="store_artifact",
      help="Store the approval artifact in the approval artifact directory.",
    )
    parser.add_argument(
      "--approval-dir",
      dest="approval_dir",
      help="Directory used when storing approval artifacts.",
    )

  def handle(self, *args, **options):
    report_path = options.get("report")
    approved_by = options.get("approved_by")
    note = options.get("note") or ""
    decided_at = options.get("decided_at")
    output_format = options.get("output_format") or "json"
    output_path = options.get("output_path")
    fingerprint_only = bool(options.get("fingerprint_only"))
    store_artifact = bool(options.get("store_artifact"))
    approval_dir = options.get("approval_dir")

    report_payload = _load_json_object(report_path)

    try:
      artifact = build_architecture_approval_artifact(
        report_payload=report_payload,
        decided_by=approved_by,
        note=note,
        decided_at=decided_at,
      )
    except ArchitectureApprovalError as exc:
      raise CommandError(str(exc))

    if fingerprint_only:
      self.stdout.write(artifact.artifact_fingerprint)
      return

    if output_format == "json":
      rendered = render_architecture_approval_json(artifact)
    else:
      rendered = render_architecture_approval_text(artifact)

    stored_path = None
    if store_artifact:
      try:
        stored_path = ArchitectureApprovalStore(approval_dir).save(artifact)
      except ArchitectureApprovalError as exc:
        raise CommandError(str(exc))

    if output_path:
      _write_text_file(output_path, rendered)
    elif not store_artifact:
      self.stdout.write(rendered)
    else:
      self.stdout.write(
        f"Stored architecture approval artifact: {stored_path}"
      )


def _load_json_object(path: str | Path) -> dict[str, Any]:
  """
  Load a JSON object from a file.
  """
  json_path = Path(path)

  try:
    payload = json.loads(json_path.read_text(encoding="utf-8"))
  except OSError as exc:
    raise CommandError(f"JSON file could not be read: {json_path}") from exc
  except JSONDecodeError as exc:
    raise CommandError(f"JSON file is invalid: {json_path}") from exc

  if not isinstance(payload, dict):
    raise CommandError(f"JSON file must contain an object: {json_path}")

  return payload


def _write_text_file(path: str | Path, content: str) -> None:
  """
  Write text content to a file.
  """
  output_path = Path(path)
  output_path.parent.mkdir(parents=True, exist_ok=True)
  output_path.write_text(content, encoding="utf-8")