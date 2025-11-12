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
from metadata.generation.validators import validate_or_raise, NAME_REGEX


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
