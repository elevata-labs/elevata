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

import importlib
from types import SimpleNamespace
from uuid import uuid4

import pytest
from django.apps import apps as django_apps
from django.db import connection

from metadata.models import (
  TargetColumn,
  TargetColumnInput,
  TargetDataset,
  TargetDatasetReference,
  TargetDatasetReferenceComponent,
  TargetSchema,
)
from metadata.promotion.release import serialize_architecture_release_bundle
from metadata.promotion.release_service import create_architecture_release_bundle


migration = importlib.import_module(
  "metadata.migrations.0013_portable_generated_hash_expressions"
)

RUNTIME_PEPPER_TOKEN = "{runtime:pepper}"


def _legacy_sk_expression(key_name, pepper):
  return (
    "HASH256(CONCAT_WS('|', "
    f"CONCAT('{key_name}', '~', "
    f"COALESCE({{expr:{key_name}}}, 'null_replaced')), "
    f"'{pepper}'))"
  )


def _run_migration():
  migration.migrate_portable_generated_hash_expressions(
    django_apps,
    SimpleNamespace(connection=connection),
  )


@pytest.mark.django_db
def test_migration_updates_active_and_inactive_generated_hash_metadata(
  monkeypatch,
):
  monkeypatch.setenv("ELEVATA_PEPPER", "upgrade-pepper")

  rawcore = TargetSchema.objects.get(short_name="rawcore")
  suffix = uuid4().hex[:8]

  active_dataset = TargetDataset.objects.create(
    target_schema=rawcore,
    target_dataset_name=f"rc_upgrade_active_{suffix}",
    lineage_key=f"generated:rawcore:{'a' * 56}{suffix}",
    is_system_managed=True,
  )
  inactive_dataset = TargetDataset.objects.create(
    target_schema=rawcore,
    target_dataset_name=f"rc_upgrade_inactive_{suffix}",
    lineage_key=f"generated:rawcore:{'b' * 56}{suffix}",
    is_system_managed=True,
    active=False,
  )

  active_key = TargetColumn.objects.create(
    target_dataset=active_dataset,
    target_column_name=f"rc_upgrade_active_{suffix}_key",
    ordinal_position=1,
    datatype="STRING",
    max_length=64,
    nullable=False,
    active=True,
    is_system_managed=True,
    system_role="surrogate_key",
    lineage_origin="surrogate_key",
    surrogate_expression=_legacy_sk_expression(
      "customerid",
      "upgrade-pepper",
    ),
  )
  inactive_key = TargetColumn.objects.create(
    target_dataset=inactive_dataset,
    target_column_name=f"rc_upgrade_inactive_{suffix}_key",
    ordinal_position=1,
    datatype="STRING",
    max_length=64,
    nullable=False,
    active=True,
    is_system_managed=True,
    system_role="surrogate_key",
    lineage_origin="surrogate_key",
    surrogate_expression=_legacy_sk_expression(
      "orderid",
      "upgrade-pepper",
    ),
  )

  active_pk = active_key.pk
  inactive_pk = inactive_key.pk
  retired_at = inactive_dataset.retired_at

  _run_migration()

  active_key.refresh_from_db()
  inactive_key.refresh_from_db()
  inactive_dataset.refresh_from_db()

  assert active_key.pk == active_pk
  assert inactive_key.pk == inactive_pk
  assert RUNTIME_PEPPER_TOKEN in active_key.surrogate_expression
  assert RUNTIME_PEPPER_TOKEN in inactive_key.surrogate_expression
  assert "upgrade-pepper" not in active_key.surrogate_expression
  assert "upgrade-pepper" not in inactive_key.surrogate_expression
  assert inactive_dataset.active is False
  assert inactive_dataset.retired_at == retired_at

  first_active_expression = active_key.surrogate_expression
  first_inactive_expression = inactive_key.surrogate_expression

  _run_migration()

  active_key.refresh_from_db()
  inactive_key.refresh_from_db()
  assert active_key.surrogate_expression == first_active_expression
  assert inactive_key.surrogate_expression == first_inactive_expression


@pytest.mark.django_db
def test_migration_blocks_runtime_pepper_semantics_change(monkeypatch):
  monkeypatch.setenv("ELEVATA_PEPPER", "configured-pepper")

  rawcore = TargetSchema.objects.get(short_name="rawcore")
  suffix = uuid4().hex[:8]
  dataset = TargetDataset.objects.create(
    target_schema=rawcore,
    target_dataset_name=f"rc_upgrade_mismatch_{suffix}",
    lineage_key=f"generated:rawcore:{'c' * 56}{suffix}",
    is_system_managed=True,
  )
  key_column = TargetColumn.objects.create(
    target_dataset=dataset,
    target_column_name=f"rc_upgrade_mismatch_{suffix}_key",
    ordinal_position=1,
    datatype="STRING",
    max_length=64,
    nullable=False,
    is_system_managed=True,
    system_role="surrogate_key",
    lineage_origin="surrogate_key",
    surrogate_expression=_legacy_sk_expression(
      "customerid",
      "persisted-pepper",
    ),
  )
  original_expression = key_column.surrogate_expression

  with pytest.raises(
    RuntimeError,
    match="would change hash semantics",
  ):
    _run_migration()

  key_column.refresh_from_db()
  assert key_column.surrogate_expression == original_expression


@pytest.mark.django_db
def test_migration_reconciles_reference_fk_to_current_builder_contract(
  monkeypatch,
):
  monkeypatch.setenv("ELEVATA_PEPPER", "upgrade-pepper")

  stage = TargetSchema.objects.get(short_name="stage")
  rawcore = TargetSchema.objects.get(short_name="rawcore")
  suffix = uuid4().hex[:8]

  stage_parent = TargetDataset.objects.create(
    target_schema=stage,
    target_dataset_name=f"stg_upgrade_parent_{suffix}",
  )
  stage_child = TargetDataset.objects.create(
    target_schema=stage,
    target_dataset_name=f"stg_upgrade_child_{suffix}",
  )
  rawcore_parent = TargetDataset.objects.create(
    target_schema=rawcore,
    target_dataset_name=f"rc_upgrade_parent_{suffix}",
    lineage_key=f"generated:rawcore:{'d' * 56}{suffix}",
    is_system_managed=True,
  )
  rawcore_child = TargetDataset.objects.create(
    target_schema=rawcore,
    target_dataset_name=f"rc_upgrade_child_{suffix}",
    lineage_key=f"generated:rawcore:{'e' * 56}{suffix}",
    is_system_managed=True,
  )

  stage_parent_customerid = TargetColumn.objects.create(
    target_dataset=stage_parent,
    target_column_name="customerid",
    ordinal_position=1,
    datatype="INTEGER",
    nullable=False,
  )
  stage_child_customerid = TargetColumn.objects.create(
    target_dataset=stage_child,
    target_column_name="customerid",
    ordinal_position=1,
    datatype="INTEGER",
    nullable=False,
  )

  parent_bk = TargetColumn.objects.create(
    target_dataset=rawcore_parent,
    target_column_name="customerid",
    ordinal_position=2,
    datatype="INTEGER",
    nullable=False,
    system_role="business_key",
  )
  child_customerid = TargetColumn.objects.create(
    target_dataset=rawcore_child,
    target_column_name="customerid",
    ordinal_position=2,
    datatype="INTEGER",
    nullable=False,
  )
  TargetColumnInput.objects.create(
    target_column=parent_bk,
    upstream_target_column=stage_parent_customerid,
  )
  TargetColumnInput.objects.create(
    target_column=child_customerid,
    upstream_target_column=stage_child_customerid,
  )

  parent_sk = TargetColumn.objects.create(
    target_dataset=rawcore_parent,
    target_column_name=f"rc_upgrade_parent_{suffix}_key",
    ordinal_position=1,
    datatype="STRING",
    max_length=64,
    nullable=False,
    active=True,
    is_system_managed=True,
    system_role="surrogate_key",
    lineage_origin="surrogate_key",
    surrogate_expression=_legacy_sk_expression(
      "customerid",
      "upgrade-pepper",
    ),
  )

  reference = TargetDatasetReference.objects.create(
    referencing_dataset=rawcore_child,
    referenced_dataset=rawcore_parent,
  )
  TargetDatasetReferenceComponent.objects.create(
    reference=reference,
    from_column=child_customerid,
    to_column=parent_bk,
  )
  fk_column = reference.sync_child_fk_column()
  assert fk_column is not None
  assert (
    migration._reference_fk_lineage_key(reference)
    == reference.child_fk_lineage_key
  )

  stale_fk_expression = _legacy_sk_expression(
    "customerid",
    "upgrade-pepper",
  )
  TargetColumn.objects.filter(pk=fk_column.pk).update(
    surrogate_expression=stale_fk_expression
  )

  fk_pk = fk_column.pk

  _run_migration()

  parent_sk.refresh_from_db()
  fk_column.refresh_from_db()

  assert parent_sk.surrogate_expression.endswith(
    f", {RUNTIME_PEPPER_TOKEN}))"
  )
  assert fk_column.pk == fk_pk
  assert fk_column.lineage_key == reference.child_fk_lineage_key
  assert fk_column.surrogate_expression == (
    "HASH256(CONCAT_WS('|', "
    "CONCAT('customerid', '~', "
    "COALESCE(col(\"customerid\"), 'null_replaced')), "
    f"{RUNTIME_PEPPER_TOKEN}))"
  )


@pytest.mark.django_db
def test_migration_makes_inactive_legacy_hash_metadata_release_portable(
  monkeypatch,
):
  monkeypatch.setenv("ELEVATA_PEPPER", "upgrade-pepper")

  rawcore = TargetSchema.objects.get(short_name="rawcore")
  suffix = uuid4().hex[:8]
  dataset = TargetDataset.objects.create(
    target_schema=rawcore,
    target_dataset_name=f"rc_upgrade_release_{suffix}",
    lineage_key=f"generated:rawcore:{'f' * 56}{suffix}",
    is_system_managed=True,
    active=False,
  )
  TargetColumn.objects.create(
    target_dataset=dataset,
    target_column_name=f"rc_upgrade_release_{suffix}_key",
    ordinal_position=1,
    datatype="STRING",
    max_length=64,
    nullable=False,
    active=True,
    is_system_managed=True,
    system_role="surrogate_key",
    lineage_origin="surrogate_key",
    surrogate_expression=_legacy_sk_expression(
      "customerid",
      "upgrade-pepper",
    ),
  )

  with pytest.raises(
    ValueError,
    match="runtime_pepper_not_symbolic_in_surrogate_expression",
  ):
    create_architecture_release_bundle(
      release_name=f"upgrade-before-{suffix}",
      release_version="1",
      created_by="upgrade-test@example.com",
      environment_label="dev",
    )

  _run_migration()

  bundle = create_architecture_release_bundle(
    release_name=f"upgrade-after-{suffix}",
    release_version="1",
    created_by="upgrade-test@example.com",
    environment_label="dev",
  )
  rendered = serialize_architecture_release_bundle(bundle)

  assert RUNTIME_PEPPER_TOKEN in rendered
  assert "upgrade-pepper" not in rendered
