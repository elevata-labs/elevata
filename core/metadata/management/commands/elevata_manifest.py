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

import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from metadata.config.profiles import load_profile
from metadata.config.targets import get_target_system
from metadata.execution.manifest import build_manifest, manifest_to_dict


class Command(BaseCommand):
  help = "Generate an execution manifest (lineage + parallelizable levels) for all datasets."

  def add_arguments(self, parser):
    parser.add_argument(
      "--profile",
      required=False,
      help="Profile name (overrides active profile). If omitted, uses the active profile.",
    )
    parser.add_argument(
      "--target-system",
      required=False,
      help="Target system short name (overrides ELEVATA_TARGET_SYSTEM). If omitted, uses active target system.",
    )
    parser.add_argument(
      "--exclude-system-managed",
      action="store_true",
      help="Exclude system-managed datasets from the manifest.",
    )
    parser.add_argument(
      "--no-sources",
      action="store_true",
      help="Do not include read-only SourceDataset nodes.",
    )

  def handle(self, *args, **options):
    include_system_managed = not bool(options.get("exclude_system_managed"))
    include_sources = not bool(options.get("no_sources"))

    # 1) Resolve active profile (env -> elevata_profiles.yaml -> dev)
    profile = load_profile(options.get("profile"))    

    # 2) Resolve active target system (env ELEVATA_TARGET_SYSTEM)
    try:
      system = get_target_system(options.get("target_system"))

    except RuntimeError as exc:
      raise CommandError(str(exc))

    # 3) Default output path (deterministic)
    # Write into core/.artifacts/elevata/manifest_<profile>_<target>.json
    out_dir = Path(".") / ".artifacts" / "elevata"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"manifest_{profile.name}_{system.short_name}.json"
    
    try:
      manifest = build_manifest(
        profile_name=profile.name,
        target_system_short=system.short_name,
        include_system_managed=include_system_managed,
        include_sources=include_sources,
      )
    except Exception as exc:
      raise CommandError(str(exc))

    payload = manifest_to_dict(manifest)

    with open(str(out_path), "w", encoding="utf-8") as f:
      json.dump(payload, f, indent=2)

    self.stdout.write(self.style.SUCCESS(f"Manifest written to {out_path}"))

    self.stdout.write(f"Nodes: {len(payload['nodes'])}")
    self.stdout.write(f"Levels: {len(payload['levels'])}")
