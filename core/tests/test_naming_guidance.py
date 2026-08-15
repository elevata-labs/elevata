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

import pytest

from metadata.models import (
  SourceColumn,
  SourceDataset,
  System,
  TargetColumn,
  TargetColumnInput,
  TargetDataset,
  TargetSchema,
)
from metadata.services.naming_guidance import (
  build_naming_guidance_for_source_column_name,
  build_naming_guidance_for_target_column,
)


def _schema(short_name: str) -> TargetSchema:
  """
  Return a target schema for naming guidance tests.
  """
  schema, _ = TargetSchema.objects.get_or_create(
    short_name=short_name,
    defaults={
      "display_name": short_name.title(),
      "schema_name": short_name,
      "default_materialization_type": "table",
      "surrogate_keys_enabled": True,
    },
  )
  return schema


def _source_column(
  source_column_name: str,
  *,
  dataset_name: str,
  ordinal_position: int = 1,
) -> SourceColumn:
  """
  Create a source column with its owning source dataset.
  """
  system, _ = System.objects.get_or_create(
    short_name="sap",
    defaults={
      "name": "SAP",
      "type": "postgresql",
      "target_short_name": "sap",
    },
  )
  dataset = SourceDataset.objects.create(
    source_system=system,
    schema_name="dbo",
    source_dataset_name=dataset_name,
    integrate=True,
    active=True,
  )
  return SourceColumn.objects.create(
    source_dataset=dataset,
    source_column_name=source_column_name,
    ordinal_position=ordinal_position,
    datatype="STRING",
    integrate=True,
  )


def _target_dataset(
  schema_short_name: str,
  target_dataset_name: str,
  *,
  incremental_strategy: str = "full",
) -> TargetDataset:
  """
  Create a target dataset in the requested schema.
  """
  return TargetDataset.objects.create(
    target_schema=_schema(schema_short_name),
    target_dataset_name=target_dataset_name,
    incremental_strategy=incremental_strategy,
    active=True,
  )


def _target_column(
  target_dataset: TargetDataset,
  target_column_name: str,
  *,
  ordinal_position: int = 1,
  system_role: str = "",
) -> TargetColumn:
  """
  Create a target column for naming guidance tests.
  """
  return TargetColumn.objects.create(
    target_dataset=target_dataset,
    target_column_name=target_column_name,
    ordinal_position=ordinal_position,
    datatype="STRING",
    nullable=True,
    system_role=system_role,
    is_system_managed=bool(system_role),
    active=True,
  )


def _direct_input(target_column: TargetColumn, source_column: SourceColumn) -> TargetColumnInput:
  """
  Create a direct source-column input link.
  """
  return TargetColumnInput.objects.create(
    target_column=target_column,
    source_column=source_column,
    ordinal_position=1,
    active=True,
  )


def _upstream_input(target_column: TargetColumn, upstream_column: TargetColumn) -> TargetColumnInput:
  """
  Create an upstream target-column input link.
  """
  return TargetColumnInput.objects.create(
    target_column=target_column,
    upstream_target_column=upstream_column,
    ordinal_position=1,
    active=True,
  )


@pytest.mark.django_db
def test_naming_guidance_returns_new_without_known_usage() -> None:
  """
  Verify unknown source-column names return neutral guidance.
  """
  guidance = build_naming_guidance_for_source_column_name("KUNNR")

  assert guidance.status == "new"
  assert guidance.recommended_target_column_name == ""
  assert guidance.candidates == ()
  assert guidance.usage_count == 0


@pytest.mark.django_db
def test_naming_guidance_suggests_single_known_target_name() -> None:
  """
  Verify one known naming usage becomes a suggestion.
  """
  source_column = _source_column("KUNNR", dataset_name="customer_single")
  target_dataset = _target_dataset("rawcore", "rc_customer_single")
  target_column = _target_column(target_dataset, "customer_no")
  _direct_input(target_column, source_column)

  guidance = build_naming_guidance_for_source_column_name("kunnr")

  assert guidance.status == "suggested"
  assert guidance.recommended_target_column_name == "customer_no"
  assert [candidate.target_column_name for candidate in guidance.candidates] == [
    "customer_no",
  ]
  assert guidance.candidates[0].usage_count == 1
  assert guidance.candidates[0].examples[0].target_dataset_key == "rawcore.rc_customer_single"


@pytest.mark.django_db
def test_naming_guidance_reports_conflict_for_multiple_non_dominant_names() -> None:
  """
  Verify competing known names are reported as a conflict.
  """
  first_source_column = _source_column("KUNNR", dataset_name="customer_conflict_a")
  second_source_column = _source_column("KUNNR", dataset_name="customer_conflict_b")

  first_target_dataset = _target_dataset("rawcore", "rc_customer_conflict_a")
  second_target_dataset = _target_dataset("rawcore", "rc_customer_conflict_b")
  _direct_input(_target_column(first_target_dataset, "customer_no"), first_source_column)
  _direct_input(_target_column(second_target_dataset, "cust_no"), second_source_column)

  guidance = build_naming_guidance_for_source_column_name("KUNNR")

  assert guidance.status == "conflict"
  assert guidance.recommended_target_column_name == ""
  assert [candidate.target_column_name for candidate in guidance.candidates] == [
    "cust_no",
    "customer_no",
  ]


@pytest.mark.django_db
def test_naming_guidance_suggests_dominant_known_target_name() -> None:
  """
  Verify a clearly dominant naming usage becomes a suggestion.
  """
  for index in range(4):
    source_column = _source_column("KUNNR", dataset_name=f"customer_dominant_{index}")
    target_dataset = _target_dataset("rawcore", f"rc_customer_dominant_{index}")
    _direct_input(_target_column(target_dataset, "customer_no"), source_column)

  conflict_source_column = _source_column("KUNNR", dataset_name="customer_dominant_alt")
  conflict_target_dataset = _target_dataset("rawcore", "rc_customer_dominant_alt")
  _direct_input(_target_column(conflict_target_dataset, "cust_no"), conflict_source_column)

  guidance = build_naming_guidance_for_source_column_name("KUNNR")

  assert guidance.status == "suggested"
  assert guidance.recommended_target_column_name == "customer_no"
  assert [candidate.target_column_name for candidate in guidance.candidates] == [
    "customer_no",
    "cust_no",
  ]
  assert guidance.candidates[0].usage_count == 4
  assert guidance.candidates[1].usage_count == 1


@pytest.mark.django_db
def test_naming_guidance_resolves_upstream_column_lineage() -> None:
  """
  Verify guidance can derive the original source-column name through upstream columns.
  """
  source_column = _source_column("KUNNR", dataset_name="customer_lineage")

  raw_dataset = _target_dataset("raw", "raw_sap_customer_lineage")
  raw_column = _target_column(raw_dataset, "KUNNR")
  _direct_input(raw_column, source_column)

  stage_dataset = _target_dataset("stage", "stg_sap_customer_lineage")
  stage_column = _target_column(stage_dataset, "KUNNR")
  _upstream_input(stage_column, raw_column)

  rawcore_dataset = _target_dataset("rawcore", "rc_sap_customer_lineage")
  rawcore_column = _target_column(rawcore_dataset, "customer_no")
  _upstream_input(rawcore_column, stage_column)

  guidance = build_naming_guidance_for_source_column_name("KUNNR")

  assert guidance.status == "suggested"
  assert guidance.recommended_target_column_name == "customer_no"
  assert [candidate.target_column_name for candidate in guidance.candidates] == [
    "customer_no",
  ]
  assert guidance.candidates[0].examples[0].target_dataset_key == "rawcore.rc_sap_customer_lineage"


@pytest.mark.django_db
def test_naming_guidance_for_target_column_excludes_current_column_usage() -> None:
  """
  Verify target-column guidance does not recommend the column from itself.
  """
  source_column = _source_column("KUNNR", dataset_name="customer_self_excluded")
  target_dataset = _target_dataset("rawcore", "rc_customer_self_excluded")
  target_column = _target_column(target_dataset, "customer_no")
  _direct_input(target_column, source_column)

  guidance_items = build_naming_guidance_for_target_column(target_column)

  assert len(guidance_items) == 1
  assert guidance_items[0].source_column_name == "KUNNR"
  assert guidance_items[0].status == "new"
  assert guidance_items[0].recommended_target_column_name == ""


@pytest.mark.django_db
def test_naming_guidance_service_does_not_mutate_metadata() -> None:
  """
  Verify naming guidance only reads existing metadata.
  """
  source_column = _source_column("KUNNR", dataset_name="customer_read_only")
  target_dataset = _target_dataset("rawcore", "rc_customer_read_only")
  target_column = _target_column(target_dataset, "customer_no")
  _direct_input(target_column, source_column)

  before_counts = (
    TargetColumn.objects.count(),
    TargetColumnInput.objects.count(),
    SourceColumn.objects.count(),
  )

  guidance = build_naming_guidance_for_source_column_name("KUNNR")

  after_counts = (
    TargetColumn.objects.count(),
    TargetColumnInput.objects.count(),
    SourceColumn.objects.count(),
  )
  assert guidance.status == "suggested"
  assert after_counts == before_counts


@pytest.mark.django_db
def test_naming_guidance_ignores_hist_dataset_examples() -> None:
  """
  Verify *_hist datasets do not contribute to guidance counts or examples.
  """
  source_column = _source_column("KUNNR", dataset_name="customer_hist_base")

  base_dataset = _target_dataset("rawcore", "rc_customer_hist_base")
  _direct_input(_target_column(base_dataset, "customer_no"), source_column)

  hist_dataset = _target_dataset(
    "rawcore",
    "rc_customer_hist_base_hist",
    incremental_strategy="historize",
  )
  _direct_input(_target_column(hist_dataset, "customer_no"), source_column)

  guidance = build_naming_guidance_for_source_column_name("KUNNR")

  assert guidance.status == "suggested"
  assert guidance.recommended_target_column_name == "customer_no"
  assert guidance.candidates[0].usage_count == 1
  assert [example.target_dataset_key for example in guidance.candidates[0].examples] == [
    "rawcore.rc_customer_hist_base",
  ]


@pytest.mark.django_db
def test_naming_guidance_ignores_ambiguous_multi_source_target_columns() -> None:
  """
  Verify guidance excludes target columns fed by different technical source names.
  """
  source_product_number = _source_column("ProductNumber", dataset_name="product_number_src")
  source_product_name = _source_column("Name", dataset_name="product_name_src")

  clear_dataset = _target_dataset("rawcore", "rc_product_clear")
  clear_target_column = _target_column(clear_dataset, "product_no")
  _direct_input(clear_target_column, source_product_number)

  ambiguous_dataset = _target_dataset("rawcore", "rc_product_ambiguous")
  ambiguous_target_column = _target_column(ambiguous_dataset, "product_desc")
  _direct_input(ambiguous_target_column, source_product_number)
  TargetColumnInput.objects.create(
    target_column=ambiguous_target_column,
    source_column=source_product_name,
    ordinal_position=2,
    active=True,
  )

  guidance = build_naming_guidance_for_source_column_name("ProductNumber")

  assert guidance.status == "suggested"
  assert guidance.recommended_target_column_name == "product_no"
  assert [candidate.target_column_name for candidate in guidance.candidates] == [
    "product_no",
  ]
  assert guidance.candidates[0].usage_count == 1


@pytest.mark.django_db
def test_naming_guidance_ignores_serving_layer_examples_by_default() -> None:
  """
  Verify serving-layer friendly names do not influence technical naming guidance.
  """
  source_column = _source_column("ProductNumber", dataset_name="product_serving_scope")

  rawcore_dataset = _target_dataset("rawcore", "rc_product_serving_scope")
  _direct_input(_target_column(rawcore_dataset, "product_no"), source_column)

  serving_dataset = _target_dataset("serving", "product_serving_scope")
  _direct_input(_target_column(serving_dataset, "Product Number"), source_column)

  guidance = build_naming_guidance_for_source_column_name("ProductNumber")

  assert guidance.status == "suggested"
  assert guidance.recommended_target_column_name == "product_no"
  assert [candidate.target_column_name for candidate in guidance.candidates] == [
    "product_no",
  ]
  assert [example.target_dataset_key for example in guidance.candidates[0].examples] == [
    "rawcore.rc_product_serving_scope",
  ]


@pytest.mark.django_db
def test_naming_guidance_for_target_column_uses_immediate_upstream_name() -> None:
  """
  Verify target-column guidance stops at the immediate upstream target column.
  """
  source_column = _source_column("AccountNumber", dataset_name="account_upstream_source")

  rawcore_dataset = _target_dataset("rawcore", "rc_account_upstream_source")
  rawcore_column = _target_column(rawcore_dataset, "account_no")
  _direct_input(rawcore_column, source_column)

  existing_bizcore_dataset = _target_dataset("bizcore", "bc_account_upstream_existing")
  existing_bizcore_column = _target_column(existing_bizcore_dataset, "account_no")
  _upstream_input(existing_bizcore_column, rawcore_column)

  edited_bizcore_dataset = _target_dataset("bizcore", "bc_account_upstream_edited")
  edited_bizcore_column = _target_column(edited_bizcore_dataset, "customer_no")
  _upstream_input(edited_bizcore_column, rawcore_column)

  guidance_items = build_naming_guidance_for_target_column(edited_bizcore_column)

  assert len(guidance_items) == 1
  assert guidance_items[0].source_column_name == "account_no"
  assert guidance_items[0].status == "suggested"
  assert guidance_items[0].recommended_target_column_name == "account_no"
  assert [candidate.target_column_name for candidate in guidance_items[0].candidates] == [
    "account_no",
  ]
