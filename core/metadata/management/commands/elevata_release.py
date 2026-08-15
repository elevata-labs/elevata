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

from django.core.management.base import BaseCommand, CommandError

from metadata.promotion.release import (
  ArchitectureReleaseBundleError,
  deserialize_architecture_release_bundle,
)
from metadata.promotion.release_service import (
  create_architecture_release_bundle,
  download_architecture_release_bundle,
  list_architecture_release_bundles,
  store_architecture_release_bundle,
)
from metadata.promotion.release_store import (
  ArchitectureReleaseStore,
  ArchitectureReleaseStoreError,
)
from metadata.promotion.release_validation import (
  ArchitectureReleaseValidationError,
  validate_architecture_release_bundle,
)
from metadata.promotion.snapshot import (
  EnvironmentMetadataSnapshotError,
  deserialize_environment_metadata_snapshot,
)
from metadata.promotion.snapshot_builder import (
  EnvironmentMetadataSnapshotBuildError,
)


class Command(BaseCommand):
  help = "Create, list, download or validate Architecture Release Bundles."

  def add_arguments(self, parser):
    parser.add_argument(
      "action",
      choices=("create", "list", "download", "validate"),
    )
    parser.add_argument(
      "reference",
      nargs="?",
      help="Release ID for download or bundle path for validate.",
    )
    parser.add_argument("--release-name", dest="release_name")
    parser.add_argument("--release-version", dest="release_version")
    parser.add_argument("--description", default="", dest="description")
    parser.add_argument("--created-by", dest="created_by")
    parser.add_argument("--environment-label", dest="environment_label")
    parser.add_argument(
      "--snapshot",
      dest="snapshot_path",
      help="Create the release from an existing metadata snapshot file.",
    )
    parser.add_argument("--output", dest="output_path")
    parser.add_argument("--store-dir", dest="store_dir")
    parser.add_argument(
      "--compact",
      action="store_true",
      help="Use compact canonical JSON for downloads.",
    )
    parser.add_argument(
      "--json",
      action="store_true",
      dest="json_output",
      help="Render list or validation output as JSON.",
    )

  def handle(self, *args, **options):
    action = options["action"]
    store = ArchitectureReleaseStore(options.get("store_dir") or None)
    try:
      if action == "create":
        self._create(store=store, options=options)
      elif action == "list":
        self._list(store=store, json_output=bool(options.get("json_output")))
      elif action == "download":
        self._download(store=store, options=options)
      else:
        self._validate(options=options)
    except (
      ArchitectureReleaseBundleError,
      ArchitectureReleaseStoreError,
      ArchitectureReleaseValidationError,
      EnvironmentMetadataSnapshotBuildError,
      EnvironmentMetadataSnapshotError,
      OSError,
      ValueError,
    ) as exc:
      raise CommandError(str(exc)) from exc

  def _create(self, *, store: ArchitectureReleaseStore, options) -> None:
    release_name = _required_option(options, "release_name", "--release-name")
    release_version = _required_option(
      options,
      "release_version",
      "--release-version",
    )
    created_by = _required_option(options, "created_by", "--created-by")
    snapshot = None
    snapshot_path = options.get("snapshot_path")
    if snapshot_path:
      snapshot = deserialize_environment_metadata_snapshot(
        Path(snapshot_path).read_text(encoding="utf-8")
      )
    elif not options.get("environment_label"):
      raise CommandError(
        "create requires --environment-label or --snapshot."
      )

    bundle = create_architecture_release_bundle(
      release_name=release_name,
      release_version=release_version,
      description=options.get("description") or "",
      created_by=created_by,
      environment_label=options.get("environment_label"),
      snapshot=snapshot,
    )
    stored_path = store_architecture_release_bundle(bundle, store=store)

    output_path = options.get("output_path")
    if output_path:
      download_architecture_release_bundle(
        release_id=bundle.release_id,
        output_path=output_path,
        store=store,
        pretty=not bool(options.get("compact")),
      )

    self.stdout.write(self.style.SUCCESS(
      f"Stored Architecture Release Bundle: {stored_path}"
    ))
    self.stdout.write(f"Release ID: {bundle.release_id}")
    self.stdout.write(f"Bundle fingerprint: {bundle.bundle_fingerprint}")
    self.stdout.write(
      f"Metadata fingerprint: {bundle.metadata_fingerprint}"
    )

  def _list(
    self,
    *,
    store: ArchitectureReleaseStore,
    json_output: bool,
  ) -> None:
    bundles = list_architecture_release_bundles(store=store)
    if json_output:
      payload = [
        {
          "release_id": bundle.release_id,
          "release_name": bundle.release_name,
          "release_version": bundle.release_version,
          "source_environment_label": bundle.source_environment_label,
          "created_at": bundle.to_dict()["created_at"],
          "created_by": bundle.created_by,
          "bundle_fingerprint": bundle.bundle_fingerprint,
          "metadata_fingerprint": bundle.metadata_fingerprint,
        }
        for bundle in bundles
      ]
      self.stdout.write(json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        indent=2,
      ))
      return

    if not bundles:
      self.stdout.write("No Architecture Release Bundles stored.")
      return
    for bundle in bundles:
      self.stdout.write(
        f"{bundle.release_id}  {bundle.release_name} "
        f"{bundle.release_version}  {bundle.source_environment_label}"
      )

  def _download(
    self,
    *,
    store: ArchitectureReleaseStore,
    options,
  ) -> None:
    release_id = options.get("reference")
    if not release_id:
      raise CommandError("download requires a release ID.")
    output_path = _required_option(
      options,
      "output_path",
      "--output",
      action="download",
    )
    path = download_architecture_release_bundle(
      release_id=release_id,
      output_path=output_path,
      store=store,
      pretty=not bool(options.get("compact")),
    )
    self.stdout.write(self.style.SUCCESS(
      f"Architecture Release Bundle written to {path}"
    ))

  def _validate(self, *, options) -> None:
    bundle_path = options.get("reference")
    if not bundle_path:
      raise CommandError("validate requires a bundle path.")
    bundle = deserialize_architecture_release_bundle(
      Path(bundle_path).read_text(encoding="utf-8")
    )
    result = validate_architecture_release_bundle(bundle)
    if options.get("json_output"):
      self.stdout.write(json.dumps(
        result.to_dict(),
        sort_keys=True,
        ensure_ascii=False,
        indent=2,
      ))
    elif result.is_valid:
      self.stdout.write(self.style.SUCCESS(
        f"Architecture Release Bundle is valid: {bundle.release_id}"
      ))
    else:
      for issue in result.errors:
        self.stderr.write(
          f"{issue.code}: {issue.message}"
        )
    if not result.is_valid:
      raise CommandError(
        f"Architecture Release Bundle validation failed with "
        f"{len(result.errors)} error(s)."
      )


def _required_option(
  options,
  key: str,
  flag: str,
  *,
  action: str = "create",
) -> str:
  value = options.get(key)
  if not isinstance(value, str) or not value.strip():
    raise CommandError(f"{action} requires {flag}.")
  return value.strip()
