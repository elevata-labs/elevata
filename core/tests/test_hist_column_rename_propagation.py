"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025 Ilona Tag

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

import pytest

from metadata.models import TargetSchema, TargetDataset, TargetColumn
from metadata.generation.target_generation_service import TargetGenerationService


@pytest.fixture
def rawcore_schema(db) -> TargetSchema:
  return TargetSchema.objects.get(short_name="rawcore")


@pytest.fixture
def service() -> TargetGenerationService:
  return TargetGenerationService()


@pytest.mark.django_db
def test_hist_dataset_renamed_when_rawcore_renamed(rawcore_schema, service):
  # Arrange: initial rawcore dataset with a stable lineage_key
  rawcore_td = TargetDataset.objects.create(
    target_schema=rawcore_schema,
    target_dataset_name="rc_customer_rename_test",
    historize=True,
    handle_deletes=True,
    lineage_key="bucket:rc_customer_rename_test",
  )

  TargetColumn.objects.create(
    target_dataset=rawcore_td,
    target_column_name="customer_key",
    ordinal_position=1,
  )

  # Initial hist generation
  hist_td_initial = service.ensure_hist_dataset_for_rawcore(rawcore_td)
  assert hist_td_initial.target_dataset_name == "rc_customer_rename_test_hist"

  initial_hist_id = hist_td_initial.id

  # Act: rename the rawcore dataset (but keep lineage_key stable)
  rawcore_td.target_dataset_name = "rc_customer_rename_test_v2"
  rawcore_td.save()

  # Re-run hist ensure to sync the hist dataset
  hist_td_after = service.ensure_hist_dataset_for_rawcore(rawcore_td)

  # Assert: still the same hist dataset (no new one created)
  assert hist_td_after.id == initial_hist_id
  assert hist_td_after.target_dataset_name == "rc_customer_rename_test_v2_hist"
  assert hist_td_after.lineage_key == rawcore_td.lineage_key

  # The hist surrogate key column should follow the new dataset name
  hist_cols = list(hist_td_after.target_columns.order_by("ordinal_position"))
  assert len(hist_cols) > 0
  assert hist_cols[0].target_column_name == "rc_customer_rename_test_v2_hist_key"

@pytest.mark.django_db
def test_hist_column_rename_propagation(rawcore_schema, service):
  # Arrange
  rawcore_td = TargetDataset.objects.create(
    target_schema=rawcore_schema,
    target_dataset_name="rc_customer_col_rename",
    historize=True,
    handle_deletes=True,
    lineage_key="bucket:rc_customer_col_rename",
  )

  col = TargetColumn.objects.create(
    target_dataset=rawcore_td,
    target_column_name="customer_name",
    ordinal_position=1,
  )

  # Initial hist build
  hist_td = service.ensure_hist_dataset_for_rawcore(rawcore_td)
  assert any(c.target_column_name == "customer_name" for c in hist_td.target_columns.all())

  # Act: rename the rawcore column
  col.target_column_name = "customer_full_name"
  col.save()

  # Re-sync hist schema
  hist_td = service.ensure_hist_dataset_for_rawcore(rawcore_td)
  hist_col_names = {c.target_column_name for c in hist_td.target_columns.all()}

  # Assert: new name visible in hist, old one gone
  assert "customer_full_name" in hist_col_names
  assert "customer_name" not in hist_col_names
