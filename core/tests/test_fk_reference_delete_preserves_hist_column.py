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

import uuid

import pytest

from metadata.models import (
  TargetSchema,
  TargetDataset,
  TargetColumn,
  TargetColumnInput,
  TargetDatasetReference,
)
from metadata.generation.target_generation_service import TargetGenerationService


@pytest.mark.django_db
def test_fk_reference_delete_preserves_hist_column_as_inactive_orphan():
  # --- Arrange: rawcore schema + base dataset (historized) ---
  schema, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={
      "is_system_managed": True,
      # leave surrogate_keys_enabled as-is; test must work either way
    },
  )

  suffix = uuid.uuid4().hex[:8]
  base_name = f"rc_fk_delete_{suffix}"

  base = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=base_name,
    historize=True,
    lineage_key=f"lk:{base_name}",
    is_system_managed=True,
  )

  # Ensure SK exists (required if surrogate_keys_enabled=True)
  TargetColumn.objects.create(
    target_dataset=base,
    target_column_name=f"{base_name}_key",
    datatype="STRING",
    nullable=False,
    is_system_managed=True,
    active=True,
    system_role="surrogate_key",
    former_names=[],
  )

  # Create hist once
  svc = TargetGenerationService()
  svc.ensure_hist_dataset_for_rawcore(base)

  hist = TargetDataset.objects.get(
    target_schema=schema,
    lineage_key=base.lineage_key,
    target_dataset_name__endswith="_hist",
  )

  # --- Arrange: referenced dataset (can be dummy; only needed for FK reference row) ---
  ref_name = f"rc_fk_ref_{suffix}"
  referenced = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=ref_name,
    historize=False,
    lineage_key=f"lk:{ref_name}",
    is_system_managed=True,
  )

  # Create the reference (this is what we delete later)
  ref = TargetDatasetReference.objects.create(
    referencing_dataset=base,
    referenced_dataset=referenced,
  )

  fk_col_name = f"{referenced.target_dataset_name}_key"
  fk_lineage_key = f"fk:{ref.id}"

  # FK column on rawcore child (the one that must be dropped)
  fk_col = TargetColumn.objects.create(
    target_dataset=base,
    target_column_name=fk_col_name,
    datatype="STRING",
    nullable=True,
    is_system_managed=True,
    active=True,
    system_role="foreign_key",
    lineage_key=fk_lineage_key,
    former_names=[],
  )

  # Mirror FK exists in hist (simulate that hist currently depends on FK via input link)
  hist_fk = TargetColumn.objects.create(
    target_dataset=hist,
    target_column_name=fk_col_name,
    datatype="STRING",
    nullable=True,
    is_system_managed=True,
    active=True,
    system_role="foreign_key",
    former_names=[],
  )
  TargetColumnInput.objects.create(
    target_column=hist_fk,
    upstream_target_column=fk_col,
  )

  # --- Act: delete reference (should delete FK in base, detach inputs, re-sync hist) ---
  ref.delete()

  # --- Assert: base FK column is deleted ---
  assert not TargetColumn.objects.filter(
    target_dataset=base,
    lineage_key=fk_lineage_key,
  ).exists()

  # --- Assert: hist column still exists but is inactive + retired ---
  hist_fk_refreshed = TargetColumn.objects.get(
    target_dataset=hist,
    target_column_name=fk_col_name,
  )
  assert hist_fk_refreshed.active is False
  assert hist_fk_refreshed.retired_at is not None

  # --- Assert: no lingering inputs to deleted FK ---
  assert not TargetColumnInput.objects.filter(
    target_column__target_dataset=hist,
    target_column__target_column_name=fk_col_name,
  ).exists()

def test_fk_delete_preserves_hist_orphan(db):
  """
  Deleting a TargetDatasetReference must NOT drop the dependent column
  in *_hist. It must remain as an inactive orphan.
  """
  from metadata.models import (
    TargetDataset, TargetColumn, TargetDatasetReference, TargetColumnInput
  )
  from metadata.generation.target_generation_service import TargetGenerationService

  # Setup rawcore parent + child
  raw = TargetDataset.objects.create(
    target_dataset_name="rc_parent",
    target_schema=TargetSchema.objects.get(short_name="rawcore"),
    historize=True,
  )
  child = TargetDataset.objects.create(
    target_dataset_name="rc_child",
    target_schema=raw.target_schema,
    historize=True,
  )

  # Make test robust even if rawcore requires surrogate keys
  TargetColumn.objects.create(
    target_dataset=child,
    target_column_name="rc_child_key",
    datatype="STRING",
    nullable=False,
    is_system_managed=True,
    active=True,
    system_role="surrogate_key",
    former_names=[],
    ordinal_position=1,
  )

  # FK column on child
  fk_col = TargetColumn.objects.create(
    target_dataset=child,
    target_column_name="rc_parent_key",
    system_role="foreign_key",
    lineage_key="fk:1",
    datatype="STRING",
    ordinal_position=2,
  )

  # Create hist first
  svc = TargetGenerationService()
  hist = svc.ensure_hist_dataset_for_rawcore(child)

  hist_fk = TargetColumn.objects.get(
    target_dataset=hist,
    target_column_name="rc_parent_key",
  )
  assert hist_fk.active is True

  # Create reference
  ref = TargetDatasetReference.objects.create(
    referencing_dataset=child,
    referenced_dataset=raw,
  )

  # Now delete reference
  ref.delete()

  # Rawcore FK is gone
  assert not TargetColumn.objects.filter(pk=fk_col.pk).exists()

  # Hist FK still exists but is inactive
  hist_fk.refresh_from_db()
  assert hist_fk.active is False
  assert hist_fk.retired_at is not None

  # And has no upstream inputs
  assert not TargetColumnInput.objects.filter(target_column=hist_fk).exists()
