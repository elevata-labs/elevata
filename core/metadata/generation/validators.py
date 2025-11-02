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

from django.core.validators import RegexValidator
from django.core.exceptions import ValidationError
import re

# --- Specific Django field validators (for model fields) ---

SHORT_NAME_VALIDATOR = RegexValidator(
  regex=r"^[a-z][a-z0-9]{0,9}$",
  message=(
    "Must start with a lowercase letter and contain only lowercase letters and digits. "
    "Max length is 10 characters."
  ),
)

TARGET_IDENTIFIER_VALIDATOR = RegexValidator(
  regex=r"^[a-z][a-z0-9_]{0,62}$",
  message=(
    "Must start with a lowercase letter and contain only lowercase letters, "
    "digits, and underscores. Max length is 63 characters."
  ),
)

# --- Generic validator for internal name logic (used by naming.py etc.) ---

NAME_REGEX = r"^[a-z_][a-z0-9_]*$"
NAME_VALIDATOR = re.compile(NAME_REGEX)

def validate_or_raise(name: str, context: str = "name"):
  """Validate a free-form name for internal identifiers."""
  if not NAME_VALIDATOR.match(name or ""):
    raise ValidationError(
      f"{context}: '{name}' is not a valid identifier. "
      "Rules: lowercase letters / digits / underscore, must not start with a digit."
    )
