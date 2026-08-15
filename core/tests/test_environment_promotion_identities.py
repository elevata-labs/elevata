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

from uuid import UUID

import pytest

from metadata.promotion.identities import (
  build_metadata_object_identity,
)
from metadata.promotion.model_contracts import (
  METADATA_TRANSPORT_REGISTRY,
)
from metadata.models import TargetDataset, TargetDatasetReference, TargetSchema


def test_identity_ignores_unclassified_local_ids():
  contract = METADATA_TRANSPORT_REGISTRY.get_model("System").identity

  left = build_metadata_object_identity(
    model_name="System",
    contract=contract,
    values={"short_name": "dwhprod", "id": 1},
  )
  right = build_metadata_object_identity(
    model_name="System",
    contract=contract,
    values={"short_name": "dwhprod", "id": 999},
  )

  assert left.object_key == right.object_key


def test_source_dataset_identity_normalizes_missing_schema_name():
  contract = METADATA_TRANSPORT_REGISTRY.get_model("SourceDataset").identity

  with_none = build_metadata_object_identity(
    model_name="SourceDataset",
    contract=contract,
    values={
      "source_system_key": "crm",
      "schema_name": None,
      "source_dataset_name": "customer",
    },
  )
  with_empty = build_metadata_object_identity(
    model_name="SourceDataset",
    contract=contract,
    values={
      "source_system_key": "crm",
      "schema_name": "",
      "source_dataset_name": "customer",
    },
  )

  assert with_none.object_key == with_empty.object_key


def test_target_dataset_identity_uses_lineage_and_variant_before_name_fallback():
  contract = METADATA_TRANSPORT_REGISTRY.get_model("TargetDataset").identity
  common = {
    "lineage_key": "generated:rawcore:crm:customer",
    "target_schema_key": "TargetSchema:rawcore",
  }

  base = build_metadata_object_identity(
    model_name="TargetDataset",
    contract=contract,
    values={
      **common,
      "dataset_variant": "base",
      "target_dataset_name": "rc_crm_customer",
    },
  )
  renamed_base = build_metadata_object_identity(
    model_name="TargetDataset",
    contract=contract,
    values={
      **common,
      "dataset_variant": "base",
      "target_dataset_name": "rc_crm_customer_v2",
    },
  )
  history = build_metadata_object_identity(
    model_name="TargetDataset",
    contract=contract,
    values={
      **common,
      "dataset_variant": "hist",
      "target_dataset_name": "rc_crm_customer_hist",
    },
  )

  assert base.object_key == renamed_base.object_key
  assert base.object_key != history.object_key


def test_target_column_identity_is_scoped_to_target_dataset():
  contract = METADATA_TRANSPORT_REGISTRY.get_model("TargetColumn").identity
  common = {
    "lineage_key": "source:crm.customer.customer_id",
  }

  base_column = build_metadata_object_identity(
    model_name="TargetColumn",
    contract=contract,
    values={
      **common,
      "target_dataset_key": "TargetDataset:base",
      "target_column_name": "customer_id",
    },
  )
  renamed_base_column = build_metadata_object_identity(
    model_name="TargetColumn",
    contract=contract,
    values={
      **common,
      "target_dataset_key": "TargetDataset:base",
      "target_column_name": "customer_number",
    },
  )
  history_column = build_metadata_object_identity(
    model_name="TargetColumn",
    contract=contract,
    values={
      **common,
      "target_dataset_key": "TargetDataset:hist",
      "target_column_name": "customer_id",
    },
  )
 
  assert base_column.object_key == renamed_base_column.object_key
  assert base_column.object_key != history_column.object_key


def test_query_node_identity_uses_uuid_logical_key():
  contract = METADATA_TRANSPORT_REGISTRY.get_model("QueryNode").identity
  logical_key = UUID("53bdff0a-fcff-44ee-930d-04b874e0417f")

  identity = build_metadata_object_identity(
    model_name="QueryNode",
    contract=contract,
    values={"logical_key": logical_key, "id": 17},
  )

  assert identity.components == (("logical_key", logical_key),)


def test_reference_fk_lineage_key_is_independent_of_local_reference_id():
  schema = TargetSchema(short_name="rawcore", schema_name="rawcore")
  child = TargetDataset(
    target_schema=schema,
    target_dataset_name="rc_crm_order",
    lineage_key="generated:rawcore:crm:order",
  )
  parent = TargetDataset(
    target_schema=schema,
    target_dataset_name="rc_crm_customer",
    lineage_key="generated:rawcore:crm:customer",
  )

  left = TargetDatasetReference(
    id=11,
    referencing_dataset=child,
    referenced_dataset=parent,
    reference_prefix="billing",
  )
  right = TargetDatasetReference(
    id=999,
    referencing_dataset=child,
    referenced_dataset=parent,
    reference_prefix="billing",
  )

  assert left.child_fk_lineage_key == right.child_fk_lineage_key
  assert left.child_fk_lineage_key.startswith("fk:")


def test_identity_error_reports_exact_empty_component():
  contract = METADATA_TRANSPORT_REGISTRY.get_model(
    "TargetDatasetOwnership"
  ).identity

  with pytest.raises(
    ValueError,
    match=r"empty role",
  ):
    build_metadata_object_identity(
      model_name="TargetDatasetOwnership",
      contract=contract,
      values={
        "target_dataset_key": "TargetDataset:abc",
        "person_email": "owner@example.com",
        "role": "",
      },
    )
