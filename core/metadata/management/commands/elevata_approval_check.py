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

from metadata.architecture.approval import check_architecture_approval
from metadata.architecture.renderers import (
  render_architecture_approval_check_json,
  render_architecture_approval_check_text,
)


class Command(BaseCommand):
  help = "Check architecture approval artifacts against architecture reports."

  def add_arguments(self, parser):
    parser.add_argument(
      "report",
      help="Architecture Change Report JSON file.",
    )
    parser.add_argument(
      "approval",
      help="Architecture Approval Artifact JSON file.",
    )
    parser.add_argument(
      "--format",
      choices=("text", "json"),
      default="text",
      dest="output_format",
      help="Approval check output format.",
    )
    parser.add_argument(
      "--output",
      dest="output_path",
      help="Write the approval check result to a file.",
    )

  def handle(self, *args, **options):
    report_path = options.get("report")
    approval_path = options.get("approval")
    output_format = options.get("output_format") or "text"
    output_path = options.get("output_path")

    report_payload = _load_json_object(report_path)
    approval_payload = _load_json_object(approval_path)

    result = check_architecture_approval(
      report_payload=report_payload,
      approval_payload=approval_payload,
    )

    if output_format == "json":
      rendered = render_architecture_approval_check_json(result)
    else:
      rendered = render_architecture_approval_check_text(result)

    if output_path:
      _write_text_file(output_path, rendered)
    else:
      self.stdout.write(rendered)

    if not result.is_valid:
      raise CommandError(result.message, returncode=1)


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