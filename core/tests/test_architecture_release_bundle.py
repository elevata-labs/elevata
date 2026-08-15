"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2026 Ilona Tag

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

from datetime import datetime, timedelta, timezone
from io import StringIO
import json
from uuid import uuid4

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from metadata.generation.hashing import build_surrogate_expression
from metadata.models import (
  SourceDataset,
  System,
  TargetColumn,
  TargetDataset,
  TargetSchema,
)
from metadata.promotion.release import (
  ArchitectureReleaseBundleError,
  deserialize_architecture_release_bundle,
  serialize_architecture_release_bundle,
)
from metadata.promotion.release_service import (
  create_architecture_release_bundle,
)
from metadata.promotion.release_store import (
  ARCHITECTURE_RELEASE_DIR_ENV,
  ArchitectureReleaseStore,
  ArchitectureReleaseStoreError,
  resolve_architecture_release_dir,
)
from metadata.promotion.release_validation import (
  validate_architecture_release_bundle,
)


@pytest.mark.django_db
def test_architecture_release_bundle_is_immutable_and_roundtrips():
  created_at = datetime(2026, 8, 3, 5, 0, tzinfo=timezone.utc)
  bundle = create_architecture_release_bundle(
    release_name="customer-platform",
    release_version="2026.08.03.1",
    description="Customer platform metadata release",
    created_by="release-owner@example.com",
    environment_label="dev",
    created_at=created_at,
  )

  rendered = serialize_architecture_release_bundle(bundle)
  restored = deserialize_architecture_release_bundle(rendered)

  assert serialize_architecture_release_bundle(restored) == rendered
  assert restored.release_id == bundle.release_id
  assert restored.bundle_fingerprint == bundle.bundle_fingerprint
  assert restored.metadata_fingerprint == bundle.metadata_fingerprint
  assert restored.source_snapshot_fingerprint == bundle.source_snapshot_fingerprint
  assert restored.source_environment_label == "dev"
  assert restored.release_id.startswith("rel-")

  tampered = json.loads(rendered)
  tampered["description"] = "Changed after release creation"
  with pytest.raises(
    ArchitectureReleaseBundleError,
    match="bundle fingerprint mismatch",
  ):
    deserialize_architecture_release_bundle(json.dumps(tampered))


def test_architecture_release_store_directory_uses_environment(monkeypatch, tmp_path):
  configured = tmp_path / "configured-releases"
  monkeypatch.setenv(ARCHITECTURE_RELEASE_DIR_ENV, str(configured))
  assert resolve_architecture_release_dir() == configured


@pytest.mark.django_db
def test_architecture_release_store_is_idempotent_and_blocks_coordinate_reuse(
  tmp_path,
):
  created_at = datetime(2026, 8, 3, 5, 10, tzinfo=timezone.utc)
  store = ArchitectureReleaseStore(tmp_path / "releases")
  first = create_architecture_release_bundle(
    release_name="finance-platform",
    release_version="1.0.0",
    created_by="owner@example.com",
    environment_label="dev",
    created_at=created_at,
  )

  first_path = store.save(first)
  assert store.save(first) == first_path
  assert store.require(first.release_id).bundle_fingerprint == first.bundle_fingerprint

  conflicting = create_architecture_release_bundle(
    release_name="finance-platform",
    release_version="1.0.0",
    description="Different immutable content",
    created_by="owner@example.com",
    environment_label="dev",
    created_at=created_at + timedelta(minutes=1),
  )
  with pytest.raises(
    ArchitectureReleaseStoreError,
    match="coordinate is already used",
  ):
    store.save(conflicting)

  export_path = tmp_path / "download" / "finance.release.json"
  store.export(first.release_id, export_path)
  assert deserialize_architecture_release_bundle(
    export_path.read_text(encoding="utf-8")
  ).release_id == first.release_id
  with pytest.raises(
    ArchitectureReleaseStoreError,
    match="already exists",
  ):
    store.export(first.release_id, export_path)


@pytest.mark.django_db
@pytest.mark.parametrize(
  ("ingestion_config", "expected_code"),
  [
    ({"api_key": "plain-secret"}, "concrete_runtime_value_in_ingestion_config"),
    ({"archive_path": r"C:\\runtime\\archive"}, "absolute_local_path_in_ingestion_config"),
    ({"file_path": "/runtime/orders.csv"}, "absolute_local_path_in_ingestion_config"),
    ({"profile_name": "prod"}, "runtime_binding_in_ingestion_config"),
    (
      {"connection_url": "postgresql://user:secret@db/prod"},
      "concrete_runtime_value_in_ingestion_config",
    ),
  ],
)
def test_architecture_release_validation_blocks_non_portable_ingestion_config(
  ingestion_config,
  expected_code,
):
  suffix = uuid4().hex[:6]
  system = System.objects.create(
    short_name=f"r{suffix}",
    name="Release Validation Source",
    type="rest",
    target_short_name=f"r{suffix}",
  )
  SourceDataset.objects.create(
    source_system=system,
    source_dataset_name=f"customer_{suffix}",
    ingestion_config=ingestion_config,
  )

  with pytest.raises(ValueError, match=expected_code):
    create_architecture_release_bundle(
      release_name=f"invalid-{suffix}",
      release_version="1",
      created_by="owner@example.com",
      environment_label="dev",
    )


@pytest.mark.django_db
def test_architecture_release_validation_accepts_symbolic_runtime_references():
  suffix = uuid4().hex[:6]
  system = System.objects.create(
    short_name=f"v{suffix}",
    name="Portable Release Source",
    type="rest",
    target_short_name=f"v{suffix}",
  )
  SourceDataset.objects.create(
    source_system=system,
    source_dataset_name=f"customer_{suffix}",
    ingestion_config={
      "base_url": "${CUSTOMER_API_BASE_URL}",
      "api_key": "${CUSTOMER_API_KEY}",
      "path": "/customers",
      "uri": "https://example.com/public/customers.json",
      "archive_path": "${CUSTOMER_ARCHIVE_PATH}",
      "pagination": {"mode": "page", "size": 100},
    },
  )

  bundle = create_architecture_release_bundle(
    release_name=f"portable-{suffix}",
    release_version="1",
    created_by="owner@example.com",
    environment_label="dev",
  )
  result = validate_architecture_release_bundle(bundle)
  assert result.is_valid is True
  assert result.errors == ()


@pytest.mark.django_db
def test_elevata_release_command_create_list_download_and_validate(tmp_path):
  store_dir = tmp_path / "release-store"
  create_stdout = StringIO()
  call_command(
    "elevata_release",
    "create",
    release_name="command-release",
    release_version="1.0.0",
    created_by="command@example.com",
    environment_label="dev",
    store_dir=str(store_dir),
    stdout=create_stdout,
  )

  store = ArchitectureReleaseStore(store_dir)
  bundles = store.load_all()
  assert len(bundles) == 1
  bundle = bundles[0]
  assert bundle.release_id in create_stdout.getvalue()

  list_stdout = StringIO()
  call_command(
    "elevata_release",
    "list",
    store_dir=str(store_dir),
    json_output=True,
    stdout=list_stdout,
  )
  listed = json.loads(list_stdout.getvalue())
  assert listed[0]["release_id"] == bundle.release_id

  download_path = tmp_path / "command-release.json"
  call_command(
    "elevata_release",
    "download",
    bundle.release_id,
    output_path=str(download_path),
    store_dir=str(store_dir),
  )
  assert download_path.exists()

  validate_stdout = StringIO()
  call_command(
    "elevata_release",
    "validate",
    str(download_path),
    stdout=validate_stdout,
  )
  assert "is valid" in validate_stdout.getvalue()

  with pytest.raises(CommandError, match="already exists"):
    call_command(
      "elevata_release",
      "download",
      bundle.release_id,
      output_path=str(download_path),
      store_dir=str(store_dir),
    )


@pytest.mark.django_db
def test_architecture_release_validation_blocks_concrete_runtime_pepper_in_key_expression():
  suffix = uuid4().hex[:6]
  schema = TargetSchema.objects.get(short_name="rawcore")
  dataset = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=f"rc_release_pepper_{suffix}",
  )
  TargetColumn.objects.create(
    target_dataset=dataset,
    target_column_name=f"rc_release_pepper_{suffix}_key",
    ordinal_position=1,
    datatype="STRING",
    max_length=64,
    nullable=False,
    system_role="surrogate_key",
    lineage_origin="surrogate_key",
    surrogate_expression=(
      "HASH256(CONCAT_WS('|', "
      "CONCAT('customerid', '~', COALESCE({expr:customerid}, 'null_replaced')), "
      "'plain-demo-pepper'))"
    ),
  )

  with pytest.raises(
    ValueError,
    match="runtime_pepper_not_symbolic_in_surrogate_expression",
  ):
    create_architecture_release_bundle(
      release_name=f"invalid-pepper-{suffix}",
      release_version="1",
      created_by="owner@example.com",
      environment_label="dev",
    )


@pytest.mark.django_db
def test_architecture_release_validation_accepts_symbolic_runtime_pepper_binding():
  suffix = uuid4().hex[:6]
  schema = TargetSchema.objects.get(short_name="rawcore")
  dataset = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=f"rc_release_symbolic_{suffix}",
  )
  TargetColumn.objects.create(
    target_dataset=dataset,
    target_column_name=f"rc_release_symbolic_{suffix}_key",
    ordinal_position=1,
    datatype="STRING",
    max_length=64,
    nullable=False,
    system_role="surrogate_key",
    lineage_origin="surrogate_key",
    surrogate_expression=build_surrogate_expression(
      natural_key_cols=["customerid"],
      pepper="must-not-leak",
      null_token="null_replaced",
      pair_sep="~",
      comp_sep="|",
    ),
  )

  bundle = create_architecture_release_bundle(
    release_name=f"symbolic-pepper-{suffix}",
    release_version="1",
    created_by="owner@example.com",
    environment_label="dev",
  )
  result = validate_architecture_release_bundle(bundle)

  assert result.is_valid is True
  assert result.errors == ()
  rendered = serialize_architecture_release_bundle(bundle)
  assert "{runtime:pepper}" in rendered
  assert "must-not-leak" not in rendered
