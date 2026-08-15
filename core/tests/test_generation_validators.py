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
from metadata.generation.validators import validate_or_raise, NAME_REGEX


def _get_or_create_target_schema(
  short_name: str,
  *,
  display_name: str,
  schema_name: str | None = None,
  surrogate_keys_enabled: bool = True,
  default_materialization_type: str = "table",
):
  """
  Return an existing TargetSchema or create it for validator tests.

  Test databases may already contain initialized layer schemas, so tests must
  not create schemas with fixed short_names unconditionally.
  """
  from metadata.models import TargetSchema

  schema, _ = TargetSchema.objects.get_or_create(
    short_name=short_name,
    defaults={
      "display_name": display_name,
      "schema_name": schema_name or short_name,
      "surrogate_keys_enabled": surrogate_keys_enabled,
      "default_materialization_type": default_materialization_type,
    },
  )

  update_fields = []

  if schema.display_name != display_name:
    schema.display_name = display_name
    update_fields.append("display_name")

  expected_schema_name = schema_name or short_name
  if schema.schema_name != expected_schema_name:
    schema.schema_name = expected_schema_name
    update_fields.append("schema_name")

  if schema.surrogate_keys_enabled != surrogate_keys_enabled:
    schema.surrogate_keys_enabled = surrogate_keys_enabled
    update_fields.append("surrogate_keys_enabled")

  if schema.default_materialization_type != default_materialization_type:
    schema.default_materialization_type = default_materialization_type
    update_fields.append("default_materialization_type")

  if update_fields:
    schema.save(update_fields=update_fields)

  return schema


# ---------------------------------------------------------------------
# Basic pattern validation
# ---------------------------------------------------------------------

def test_name_regex_pattern_basic():
  """Ensure NAME_REGEX pattern matches the intended rules."""
  # Must start with lowercase or underscore, then lowercase/digit/underscore
  assert NAME_REGEX.startswith("^")
  assert NAME_REGEX.endswith("$")
  assert "[a-z_]" in NAME_REGEX
  assert "[a-z0-9_]" in NAME_REGEX


# ---------------------------------------------------------------------
# Positive test cases
# ---------------------------------------------------------------------

@pytest.mark.parametrize(
  "name",
  [
    "sap",
    "sap_customer",
    "rc_sap_customer",
    "rc_sap_customer_hist",
    "rc_sap_customer_key",
    "_temp_table",
    "abc123",
  ],
)
def test_validate_or_raise_accepts_valid_identifiers(name):
  """Valid names should pass without raising ValidationError."""
  validate_or_raise(name, context="sanitized_name")  # should not raise


# ---------------------------------------------------------------------
# Negative test cases
# ---------------------------------------------------------------------

@pytest.mark.parametrize(
  "name",
  [
    "",          # empty string
    " ",         # whitespace
    "0sap",      # starts with digit
    "sap-cust",  # dash not allowed
    "SAP",       # uppercase letters not allowed
    "sap cust",  # space inside
    "sap$",      # invalid symbol
  ],
)
def test_validate_or_raise_rejects_invalid_identifiers(name):
  """Invalid names should raise a ValidationError with helpful message."""
  from metadata.generation.validators import ValidationError

  with pytest.raises(ValidationError) as exc_info:
    validate_or_raise(name, context="sanitized_name")

  msg = str(exc_info.value)
  assert "not a valid identifier" in msg
  assert "must not start with a digit" in msg


# ---------------------------------------------------------------------
# Context usage
# ---------------------------------------------------------------------

def test_context_in_error_message():
  """Context should appear in error message to clarify source of failure."""
  from metadata.generation.validators import ValidationError

  bad_name = "123abc"
  context = "target_dataset_name"

  with pytest.raises(ValidationError) as exc_info:
    validate_or_raise(bad_name, context=context)

  msg = str(exc_info.value).lower()
  assert context in msg
  assert bad_name in msg


# ---------------------------------------------------------------------
# Surrogate-key integrity validation
# ---------------------------------------------------------------------

@pytest.mark.django_db
def test_summarize_targetdataset_health_blocks_missing_business_key():
  """Datasets requiring surrogate keys need active business key columns."""
  from metadata.generation.validators import summarize_targetdataset_health
  from metadata.models import TargetDataset, TargetColumn

  schema = _get_or_create_target_schema(
    short_name="rawcore",
    display_name="Rawcore",
    schema_name="rawcore",
    surrogate_keys_enabled=True,
  )
  dataset = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name="rc_missing_business_key",
  )
  TargetColumn.objects.create(
    target_dataset=dataset,
    target_column_name="rc_missing_business_key_key",
    ordinal_position=1,
    datatype="STRING",
    max_length=64,
    nullable=False,
    system_role="surrogate_key",
    lineage_origin="surrogate_key",
    surrogate_expression="",
  )

  level, issues = summarize_targetdataset_health(dataset)

  assert level == "error"
  assert any("no active business key columns" in msg for msg in issues)
  assert not any("surrogate_expression" in msg for msg in issues)


@pytest.mark.django_db
def test_summarize_targetdataset_health_blocks_empty_surrogate_expression_with_business_key():
  """Empty SK expressions are reported when business keys exist."""
  from metadata.generation.validators import summarize_targetdataset_health
  from metadata.models import TargetDataset, TargetColumn

  schema = _get_or_create_target_schema(
    short_name="rawcore",
    display_name="Rawcore",
    schema_name="rawcore",
    surrogate_keys_enabled=True,
  )
  dataset = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name="rc_empty_surrogate_expression",
  )
  TargetColumn.objects.create(
    target_dataset=dataset,
    target_column_name="customer_id",
    ordinal_position=1,
    datatype="STRING",
    max_length=50,
    nullable=False,
    system_role="business_key",
  )
  TargetColumn.objects.create(
    target_dataset=dataset,
    target_column_name="rc_empty_surrogate_expression_key",
    ordinal_position=2,
    datatype="STRING",
    max_length=64,
    nullable=False,
    system_role="surrogate_key",
    lineage_origin="surrogate_key",
    surrogate_expression="",
  )

  level, issues = summarize_targetdataset_health(dataset)

  assert level == "error"
  assert any("surrogate_expression" in msg for msg in issues)


@pytest.mark.django_db
def test_validate_surrogate_key_integrity_accepts_valid_business_key_and_expression():
  """Valid SK metadata should not produce blocking health issues."""
  from metadata.generation.validators import validate_surrogate_key_integrity
  from metadata.models import TargetDataset, TargetColumn

  schema = _get_or_create_target_schema(
    short_name="rawcore",
    display_name="Rawcore",
    schema_name="rawcore",
    surrogate_keys_enabled=True,
  )
  dataset = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name="rc_customer",
  )
  TargetColumn.objects.create(
    target_dataset=dataset,
    target_column_name="customer_id",
    ordinal_position=1,
    datatype="STRING",
    max_length=50,
    nullable=False,
    system_role="business_key",
  )
  TargetColumn.objects.create(
    target_dataset=dataset,
    target_column_name="rc_customer_key",
    ordinal_position=2,
    datatype="STRING",
    max_length=64,
    nullable=False,
    system_role="surrogate_key",
    lineage_origin="surrogate_key",
    surrogate_expression="HASH256(COL(customer_id))",
  )

  assert validate_surrogate_key_integrity(dataset) == []


def test_parse_surrogate_dsl_rejects_empty_expression():
  """Empty DSL input should fail with a targeted message."""
  from metadata.rendering.dsl import parse_surrogate_dsl

  with pytest.raises(ValueError) as exc_info:
    parse_surrogate_dsl("   ")

  assert "Empty surrogate-key DSL expression" in str(exc_info.value)
