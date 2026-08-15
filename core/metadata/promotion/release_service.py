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

from datetime import datetime
from pathlib import Path

from django.utils import timezone

from metadata.promotion.release import ArchitectureReleaseBundle
from metadata.promotion.release_store import ArchitectureReleaseStore
from metadata.promotion.release_validation import (
  require_valid_architecture_release_bundle,
)
from metadata.promotion.snapshot import EnvironmentMetadataSnapshot
from metadata.promotion.snapshot_builder import (
  build_environment_metadata_snapshot,
)


def create_architecture_release_bundle(
  *,
  release_name: str,
  release_version: str,
  created_by: str,
  environment_label: str | None = None,
  snapshot: EnvironmentMetadataSnapshot | None = None,
  description: str = "",
  created_at: datetime | None = None,
) -> ArchitectureReleaseBundle:
  """
  Build and validate one immutable Architecture Release Bundle.
  """
  if snapshot is None:
    if not environment_label or not environment_label.strip():
      raise ValueError(
        "environment_label is required when no snapshot is supplied."
      )
    snapshot = build_environment_metadata_snapshot(
      environment_label=environment_label,
      created_by=created_by,
      created_at=created_at,
    )
  elif (
    environment_label
    and environment_label.strip() != snapshot.environment_label
  ):
    raise ValueError(
      "environment_label does not match the supplied snapshot."
    )

  bundle = ArchitectureReleaseBundle(
    release_name=release_name,
    release_version=release_version,
    description=description,
    created_at=created_at or timezone.now(),
    created_by=created_by,
    snapshot=snapshot,
  )
  require_valid_architecture_release_bundle(bundle)
  return bundle


def store_architecture_release_bundle(
  bundle: ArchitectureReleaseBundle,
  *,
  store: ArchitectureReleaseStore | None = None,
) -> Path:
  """
  Store one validated immutable release bundle.
  """
  return (store or ArchitectureReleaseStore()).save(bundle)


def list_architecture_release_bundles(
  *,
  store: ArchitectureReleaseStore | None = None,
) -> tuple[ArchitectureReleaseBundle, ...]:
  """
  List all stored immutable release bundles.
  """
  return (store or ArchitectureReleaseStore()).load_all()


def download_architecture_release_bundle(
  *,
  release_id: str,
  output_path: str | Path,
  store: ArchitectureReleaseStore | None = None,
  pretty: bool = True,
) -> Path:
  """
  Export one stored release bundle to a new file.
  """
  return (store or ArchitectureReleaseStore()).export(
    release_id,
    output_path,
    pretty=pretty,
  )
