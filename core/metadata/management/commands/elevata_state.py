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
from pathlib import Path

from django.core.management.base import BaseCommand

from metadata.architecture.service import ArchitectureStateService
from metadata.architecture.store import ArchitectureStateStore


class Command(BaseCommand):
  help = "Render current architecture state as deterministic JSON."

  def add_arguments(self, parser):
    parser.add_argument(
      "--output",
      dest="output_path",
      help="Write the architecture state JSON to a file.",
    )
    parser.add_argument(
      "--fingerprint-only",
      action="store_true",
      dest="fingerprint_only",
      help="Print only the current architecture state fingerprint.",
    )

  def handle(self, *args, **options):
    output_path = options.get("output_path")
    fingerprint_only = bool(options.get("fingerprint_only"))

    state = ArchitectureStateService().build_current_state()

    if fingerprint_only:
      self.stdout.write(state.fingerprint)
      return

    if output_path:
      ArchitectureStateStore.save_file(Path(output_path), state)
      return

    rendered = json.dumps(
      ArchitectureStateStore.serialize(state),
      ensure_ascii=False,
      sort_keys=True,
      indent=2,
    )
    self.stdout.write(rendered)