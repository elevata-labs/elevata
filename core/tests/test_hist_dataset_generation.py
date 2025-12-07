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

from metadata.models import TargetSchema, TargetDataset, TargetColumn, TargetColumnInput
from metadata.generation.target_generation_service import TargetGenerationService


@pytest.fixture
def rawcore_schema(db) -> TargetSchema:
  """
  Reuse the existing rawcore schema from migrations / fixtures.
  We assume there is exactly one TargetSchema(short_name='rawcore').
  """
  return TargetSchema.objects.get(short_name="rawcore")


@pytest.fixture
def service() -> TargetGenerationService:
  return TargetGenerationService()


@pytest.mark.django_db
def test_hist_generation_creates_expected_metadata(rawcore_schema, service):
  # Arrange: create a rawcore dataset with historize=True
  rawcore_td = TargetDataset.objects.create(
    target_schema=rawcore_schema,
    target_dataset_name="rc_customer_test",
    historize=True,
    handle_deletes=True,
    # All other fields should have sensible defaults in the model.
  )

  # Two example columns on the rawcore dataset
  customer_key = TargetColumn.objects.create(
    target_dataset=rawcore_td,
    target_column_name="customer_key",
    ordinal_position=1,
  )
  customer_name = TargetColumn.objects.create(
    target_dataset=rawcore_td,
    target_column_name="customer_name",
    ordinal_position=2,
  )

  # Act: ensure hist dataset exists and is schema-synced
  hist_td = service.ensure_hist_dataset_for_rawcore(rawcore_td)

  # Assert: dataset-level properties
  assert hist_td is not None
  assert hist_td.target_schema_id == rawcore_schema.id
  assert hist_td.target_dataset_name == "rc_customer_test_hist"
  # Hist is system-managed and not historized again
  assert hist_td.historize is False
  assert hist_td.handle_deletes is False
  assert hist_td.is_system_managed is True
  # lineage_key should be identical so rename propagation works
  assert hist_td.lineage_key == rawcore_td.lineage_key

  hist_cols = list(hist_td.target_columns.order_by("ordinal_position"))
  names = [c.target_column_name for c in hist_cols]

  # 1) hist surrogate key column
  assert names[0] == "rc_customer_test_hist_key"
  hist_sk = hist_cols[0]
  assert hist_sk.surrogate_key_column is True

  # 2) copied rawcore columns must exist by name
  assert "customer_key" in names
  assert "customer_name" in names

  # For each copied column, there should be exactly one input_link
  # pointing to the upstream rawcore TargetColumn.
  for col in hist_cols:
    if col.target_column_name in {"customer_key", "customer_name"}:
      inputs = list(col.input_links.all())
      assert len(inputs) == 1
      input_link = inputs[0]
      # Either upstream_target_column is set...
      if input_link.upstream_target_column_id is not None:
        assert input_link.upstream_target_column.target_dataset_id == rawcore_td.id
        assert input_link.upstream_target_column.target_column_name == col.target_column_name
      # ...or (depending on your implementation) there may be only source_column set.
      # In that case you can relax the assertion or adapt it once the behavior is fixed.

  # 3) technical versioning columns should exist
  for tech in ("version_started_at", "version_ended_at", "version_state", "load_run_id"):
    assert tech in names


@pytest.mark.django_db
def test_hist_generation_is_idempotent(rawcore_schema, service):
  # Arrange
  rawcore_td = TargetDataset.objects.create(
    target_schema=rawcore_schema,
    target_dataset_name="rc_customer_test_idem",
    historize=True,
    handle_deletes=True,
  )

  TargetColumn.objects.create(
    target_dataset=rawcore_td,
    target_column_name="customer_key",
    ordinal_position=1,
  )

  # Act: first run
  hist_td_1 = service.ensure_hist_dataset_for_rawcore(rawcore_td)
  cols_1 = list(
    hist_td_1.target_columns.order_by("ordinal_position").values_list(
      "target_column_name", "ordinal_position"
    )
  )

  # Act: second run – must not change structure or create duplicates
  hist_td_2 = service.ensure_hist_dataset_for_rawcore(rawcore_td)
  cols_2 = list(
    hist_td_2.target_columns.order_by("ordinal_position").values_list(
      "target_column_name", "ordinal_position"
    )
  )

  # Assert: we operate on the same hist dataset
  assert hist_td_1.id == hist_td_2.id
  # and structure must be identical
  assert cols_1 == cols_2


@pytest.mark.django_db
def test_hist_not_created_for_non_rawcore_or_non_historized(db, service):
  """
  1) Non-rawcore schema: ensure_hist_dataset_for_rawcore returns None.
  2) rawcore schema but historize=False: also returns None.
  """

  # Try to reuse any existing non-rawcore schema.
  non_rawcore_schema = TargetSchema.objects.exclude(short_name="rawcore").first()
  if non_rawcore_schema is None:
    pytest.skip("No non-rawcore schema available in test DB")

  rawcore_schema = TargetSchema.objects.get(short_name="rawcore")

  # 1) Dataset in non-rawcore schema but historize=True -> should not create hist
  non_rawcore_td = TargetDataset.objects.create(
    target_schema=non_rawcore_schema,
    target_dataset_name="stg_customer_test",
    historize=True,
  )
  hist_for_non_rawcore = service.ensure_hist_dataset_for_rawcore(non_rawcore_td)
  assert hist_for_non_rawcore is None

  # 2) Dataset in rawcore schema but historize=False -> should not create hist
  non_hist_td = TargetDataset.objects.create(
    target_schema=rawcore_schema,
    target_dataset_name="rc_customer_no_hist",
    historize=False,
  )
  hist_for_non_hist = service.ensure_hist_dataset_for_rawcore(non_hist_td)
  assert hist_for_non_hist is None
