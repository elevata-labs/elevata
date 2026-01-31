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
from django.db import transaction

from metadata.models import TargetSchema, TargetDataset, TargetColumn


@pytest.mark.django_db(transaction=True)
def test_rawcore_column_rename_propagates_former_names_to_hist():
  schema, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={
      "is_system_managed": True,
      "surrogate_keys_enabled": False,
    },
  )

  suffix = uuid.uuid4().hex[:8]
  base_name = f"rc_customer_{suffix}"

  base = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=base_name,
    historize=True,
    lineage_key=f"lk:{base_name}",
    is_system_managed=True,
  )

  # Make test robust even if rawcore schema requires surrogate keys
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

  col = TargetColumn.objects.create(
    target_dataset=base,
    target_column_name="customer_name",
    datatype="STRING",
    nullable=True,
    is_system_managed=True,
    active=True,
    former_names=[],
  )

  # Ensure hist exists once
  from metadata.generation.target_generation_service import TargetGenerationService
  TargetGenerationService().ensure_hist_dataset_for_rawcore(base)

  hist = TargetDataset.objects.get(
    target_schema=schema,
    lineage_key=base.lineage_key,
    target_dataset_name__endswith="_hist",
  )

  # Rename base column inside atomic so on_commit signal runs
  with transaction.atomic():
    col.target_column_name = "customer_full_name"
    col.former_names = ["customer_name"]
    col.save(update_fields=["target_column_name", "former_names"])

  # Hist must have the renamed column with former_names containing the old name
  hist_col = TargetColumn.objects.get(
    target_dataset=hist,
    target_column_name="customer_full_name",
  )

  assert "customer_name" in (hist_col.former_names or [])

  # And there must NOT be an active old-name column in hist
  assert not TargetColumn.objects.filter(
    target_dataset=hist,
    target_column_name="customer_name",
    active=True,
  ).exists()
