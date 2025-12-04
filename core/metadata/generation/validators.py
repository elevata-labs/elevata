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
from typing import List, Dict, TYPE_CHECKING
from django.apps import apps

if TYPE_CHECKING:
  # Only for static analysis; NOT executed at runtime → no circular import
  from metadata.models import TargetDataset

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


from typing import List, Dict
from django.apps import apps


def validate_incremental_target_dataset(td: "TargetDataset") -> List[str]:
  """
  Validate the incremental configuration for a single TargetDataset.
  ...
  """
  issues: List[str] = []

  if td.incremental_strategy != "merge":
    return issues

  # 1) Materialization
  if hasattr(td, "effective_materialization_type"):
    # It might be a property or a method, so handle both cases
    eff = getattr(td, "effective_materialization_type")
    mat = eff() if callable(eff) else eff
  else:
    # Fallback: use field value or assume "table"
    mat = getattr(td, "materialization_type", None) or "table"

  if mat != "table":
    issues.append(
      f"incremental_strategy='merge' but effective materialization_type='{mat}' "
      f"(expected 'table')."
    )

  if not td.incremental_source:
    issues.append(
      "incremental_strategy='merge' but incremental_source is not set."
    )

  key_cols = list(td.natural_key_fields or [])
  if not key_cols:
    issues.append(
      "incremental_strategy='merge' but no natural_key_fields are defined."
    )

  return issues


def validate_all_incremental_targets() -> Dict[int, List[str]]:
  """
  Validate incremental configuration for all TargetDatasets that are
  configured with incremental_strategy != 'full'.
  """
  TargetDataset = apps.get_model("metadata", "TargetDataset")

  result: Dict[int, List[str]] = {}

  qs = TargetDataset.objects.exclude(incremental_strategy="full")

  for td in qs.select_related("target_schema", "incremental_source"):
    issues = validate_incremental_target_dataset(td)
    if issues:
      result[td.pk] = issues

  return result


def validate_bizcore_target_dataset(td: "TargetDataset") -> List[str]:
  """
  Validate BizCore-specific semantics for a single TargetDataset.

  Rules (non-strict, but helpful):
  - Only validate if target_schema.short_name == 'bizcore'
  - Check that:
      * biz_entity_role is set
      * biz_grain_note is set
  """
  issues: List[str] = []

  schema = getattr(td, "target_schema", None)
  short_name = getattr(schema, "short_name", None)

  # Only bizcore layer is in scope for this check
  if short_name != "bizcore":
    return issues

  if not td.biz_entity_role:
    issues.append(
      "BizCore dataset has no biz_entity_role set "
      "(expected e.g. 'core_entity', 'fact', 'dimension', 'reference')."
    )

  if not td.biz_grain_note:
    issues.append(
      "BizCore dataset has no biz_grain_note set "
      "(expected a human-readable description of the business grain)."
    )

  return issues


def validate_all_bizcore_targets() -> Dict[int, List[str]]:
  """
  Validate BizCore semantics for all TargetDatasets that belong to
  a target schema with short_name == 'bizcore'.
  """
  TargetDataset = apps.get_model("metadata", "TargetDataset")

  result: Dict[int, List[str]] = {}

  qs = (
    TargetDataset.objects
    .select_related("target_schema")
    .filter(target_schema__short_name="bizcore")
  )

  for td in qs:
    issues = validate_bizcore_target_dataset(td)
    if issues:
      result[td.pk] = issues

  return result


def validate_materialization_for_target(td: "TargetDataset") -> List[str]:
  """
  Validate effective materialization type of a single TargetDataset
  against the TargetSchema.default_materialization_type.

  This is a soft check; issues are hints, not fatal errors.
  """
  issues: List[str] = []

  schema = getattr(td, "target_schema", None)
  if schema is None:
    return issues

  schema_default = getattr(schema, "default_materialization_type", None)
  if not schema_default:
    # No default configured -> nothing to validate
    return issues

  # Compute effective materialization type (schema default + override)
  if hasattr(td, "effective_materialization_type"):
    eff_attr = getattr(td, "effective_materialization_type")
    effective_mat = eff_attr() if callable(eff_attr) else eff_attr
  else:
    effective_mat = getattr(td, "materialization_type", None) or schema_default

  if effective_mat != schema_default:
    issues.append(
      "Effective materialization_type='{}' differs from schema default "
      "'{}' for schema '{}'.".format(
        effective_mat, schema_default, getattr(schema, "short_name", "<?>")
      )
    )

  return issues


def validate_all_materialization() -> Dict[int, List[str]]:
  """
  Validate materialization type for all TargetDatasets against
  their schema's default_materialization_type.

  Returns:
      Dict[TargetDataset.pk, list of human-readable issues]
  """
  TargetDataset = apps.get_model("metadata", "TargetDataset")

  result: Dict[int, List[str]] = {}

  qs = TargetDataset.objects.select_related("target_schema")

  for td in qs:
    issues = validate_materialization_for_target(td)
    if issues:
      result[td.pk] = issues

  return result


def summarize_targetdataset_health(td: "TargetDataset") -> tuple[str, List[str]]:
  """
  Aggregate health information for a single TargetDataset.

  Combines:
    - incremental configuration issues
    - bizcore semantics (if applicable)
    - materialization expectations

  Returns:
      (level, issues)
      level: 'ok' | 'warning' (can be extended to 'error' later)
      issues: list of human-readable messages
  """
  issues: List[str] = []

  # Incremental
  inc_issues = validate_incremental_target_dataset(td)
  issues.extend(f"Incremental: {msg}" for msg in inc_issues)

  # BizCore semantics
  biz_issues = validate_bizcore_target_dataset(td)
  issues.extend(f"BizCore: {msg}" for msg in biz_issues)

  # Materialization
  mat_issues = validate_materialization_for_target(td)
  issues.extend(f"Materialization: {msg}" for msg in mat_issues)

  if not issues:
    level = "ok"
  else:
    # For now we have only 'ok' and 'warning'. We could introduce 'error'
    # later if we classify individual messages.
    level = "warning"

  return level, issues
