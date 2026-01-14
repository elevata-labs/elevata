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

"""
Validator overview (intentional, non-versioned)

This module contains semantic and metadata validations with two categories:

BLOCKING (must be correct for deterministic execution):
- validate_incremental_target_dataset
- validate_semantic_join_integrity

ADVISORY (quality, governance, presentation):
- validate_bizcore_target_dataset
- validate_serving_friendly_names
- validate_materialization_for_target

Aggregation:
- summarize_targetdataset_health determines overall health level
  based on presence of blocking vs advisory findings.
"""

# --- Serving-friendly identifier helpers (Single Source of Truth) ---
#
# Rationale:
# - Model field validators are static and cannot be schema-dependent.
# - Forms + health checks need the same rules (avoid drift).
#
# Philosophy:
# - allow "what most SQL engines allow in quoted identifiers"
# - block only foot-guns that routinely break tools/pipelines
#
def serving_clean_identifier(value: str, *, kind: str) -> str:
  """
  Validate and return a cleaned serving identifier.
  'kind' is used for human-readable error messages (Dataset/Column).
  """
  if value is None:
    raise ValidationError(f"{kind} name is required.")

  v = str(value)

  if not v.strip():
    raise ValidationError(f"{kind} name must not be empty.")

  if v.strip() != v:
    raise ValidationError(f"{kind} name must not have leading/trailing spaces.")

  if any(ch in v for ch in ["\n", "\t", "\r"]):
    raise ValidationError(f"{kind} name must not contain newline/tab characters.")

  # Conservative: block quote characters unless you're sure all dialects escape correctly
  # across all supported backends + BI tools.
  if any(ch in v for ch in ['"', "`"]):
    raise ValidationError(f"{kind} name must not contain quote characters (\") or (`).")

  return v


def serving_normalize_identifier(value: str) -> str:
  """
  Case-insensitive + collapse whitespace to catch near-duplicates.
  Example: 'Customer  Name' == 'customer name'
  """
  return " ".join(str(value).lower().split())

# NOTE:
# Friendly identifier validation for Serving is handled via health/validators,
# because Django model field validators are static and cannot inspect td.target_schema.
# See validate_serving_friendly_names() further below.

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


def validate_semantic_join_integrity(td: "TargetDataset") -> List[str]:
  """
  Validate join metadata integrity for semantic layers (bizcore + serving).

  We treat BOTH bizcore and serving as correctness-critical:
    - if multiple active upstream targets exist, joins must be defined
    - non-cross joins must have predicates
    - join chain must be connected (matches builder MVP constraint)
    - all upstream inputs must be covered by the join tree

  Returns human-readable issues prefixed with ERROR/WARN.
  """
  issues: List[str] = []

  schema = getattr(td, "target_schema", None)
  short_name = getattr(schema, "short_name", None)
  if short_name not in {"bizcore", "serving"}:
    return issues

  severity = "ERROR"  # per your decision: serving must be correct as well

  TargetDatasetInput = apps.get_model("metadata", "TargetDatasetInput")
  TargetDatasetJoin = apps.get_model("metadata", "TargetDatasetJoin")

  upstream_input_ids = list(
    TargetDatasetInput.objects
    .filter(
      target_dataset=td,
      active=True,
      upstream_target_dataset__isnull=False,
    )
    .values_list("id", flat=True)
  )

  # 0 or 1 upstream target -> no joins required
  if len(upstream_input_ids) <= 1:
    return issues

  joins = list(
    TargetDatasetJoin.objects
    .filter(target_dataset=td)
    .select_related("left_input", "right_input")
    .prefetch_related("predicates")
    .order_by("join_order", "id")
  )

  if not joins:
    issues.append(
      f"{severity}: dataset has multiple active upstream targets but no join definitions."
    )
    return issues

  # Predicates required unless CROSS; CROSS must not define predicates
  for j in joins:
    join_type = (j.join_type or "").strip().lower()
    preds = list(j.predicates.all())

    if join_type != "cross" and not preds:
      issues.append(
        f"{severity}: join_order={j.join_order} is '{join_type}' but has no predicates."
      )

    if join_type == "cross" and preds:
      issues.append(
        f"{severity}: join_order={j.join_order} is CROSS but defines predicates (must be empty)."
      )

  # Chain connectivity (MVP model: left side must already be part of join tree)
  used_inputs: set[int] = {joins[0].left_input_id}
  for j in joins:
    if j.left_input_id not in used_inputs:
      issues.append(
        f"{severity}: join chain not connected at join_order={j.join_order} "
        f"(left_input_id={j.left_input_id} not yet part of join tree)."
      )
    used_inputs.add(j.right_input_id)

  # Coverage: every active upstream target input should appear in join tree
  missing = [i for i in upstream_input_ids if i not in used_inputs]
  if missing:
    issues.append(
      f"{severity}: not all active upstream inputs are covered by joins "
      f"(missing TargetDatasetInput ids: {missing})."
    )

  return issues


def validate_serving_friendly_names(td: "TargetDataset") -> List[str]:
  """
  Serving is presentation-facing and may use friendly names (spaces/case/etc.).

  We keep this intentionally pragmatic:
  - block only on obviously broken cases (control whitespace, empty after trim)
  - warn on likely-tooling problems (too long, duplicates after normalization)

  IMPORTANT: This does NOT replace model field validators. See discussion in chat:
  model validators are static and cannot be schema-dependent.
  """
  issues: List[str] = []

  schema = getattr(td, "target_schema", None)
  short_name = getattr(schema, "short_name", None)
  if short_name != "serving":
    return issues

  # Dataset name checks via central helper (Single Source of Truth)
  ds_name = (getattr(td, "target_dataset_name", "") or "")
  try:
    serving_clean_identifier(ds_name, kind="Dataset")
  except ValidationError as e:
    issues.append(f"ERROR: {e}")
  else:
    # Keep portability guardrails as advisory
    if len(ds_name) > 127:
      issues.append("WARN: dataset name is longer than 127 characters (tool compatibility risk).")

  # Column checks
  TargetColumn = apps.get_model("metadata", "TargetColumn")
  cols = list(
    TargetColumn.objects
    .filter(target_dataset=td)
    .order_by("ordinal_position", "id")
    .values_list("target_column_name", flat=True)
  )

  # Normalization: case-insensitive + collapse whitespace
  # This catches confusing near-duplicates in BI tools.
  seen = {}
  for nm in cols:
    name = (nm or "")
    try:
      # Use central helper for hard constraints
      serving_clean_identifier(name, kind="Column")
    except ValidationError as e:
      issues.append(f"ERROR: {e}")
      continue

    norm = serving_normalize_identifier(name)

    if norm in seen:
      issues.append(
        f"ERROR: duplicate serving column names after normalization: '{seen[norm]}' and '{name}'."
      )
    else:
      seen[norm] = name

    # Length: 63 is common, but quoted identifiers can be longer on many platforms.
    # Treat as warning to keep it portable; can be hardened later per dialect.
    if len(name) > 63:
      issues.append(f"WARN: column '{name}' is longer than 63 characters (tool compatibility risk).")

  return issues

def validate_all_semantic_targets() -> Dict[int, List[str]]:
  """
  Validate semantic-layer hints for all TargetDatasets that belong to
  target schemas in {'bizcore', 'serving'}.  
  """
  TargetDataset = apps.get_model("metadata", "TargetDataset")

  result: Dict[int, List[str]] = {}

  qs = (
    TargetDataset.objects
    .select_related("target_schema")
    .filter(target_schema__short_name__in=["bizcore", "serving"])
  )

  for td in qs:
    issues = validate_bizcore_target_dataset(td)
    issues.extend(validate_semantic_join_integrity(td))
    issues.extend(validate_serving_friendly_names(td))
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
      level: 'ok' | 'warning' | 'error'
      issues: list of human-readable messages
  """
  issues: List[str] = []
  has_error = False

  # Incremental
  inc_issues = validate_incremental_target_dataset(td)
  if inc_issues:
    has_error = True
    issues.extend(f"Incremental: {msg}" for msg in inc_issues)

  # BizCore semantics
  biz_issues = validate_bizcore_target_dataset(td)
  issues.extend(f"BizCore: WARN: {msg}" for msg in biz_issues)

  # Join integrity (bizcore + serving) => correctness-critical
  join_issues = validate_semantic_join_integrity(td)
  if join_issues:
    has_error = True
    issues.extend(f"Joins: {msg}" for msg in join_issues)

  # Serving friendly name checks (presentation layer)
  srv_issues = validate_serving_friendly_names(td)
  if srv_issues:
    issues.extend(f"Serving: {msg}" for msg in srv_issues)
    if any("ERROR:" in m for m in srv_issues):
      has_error = True 

  # Materialization
  mat_issues = validate_materialization_for_target(td)
  issues.extend(f"Materialization: WARN: {msg}" for msg in mat_issues)

  # Detect ERROR messages (prefix-based)
  if any("ERROR:" in msg for msg in issues):
    has_error = True

  if not issues:
    level = "ok"
  else:
    level = "error" if has_error else "warning"

  return level, issues
