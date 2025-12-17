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

"""
TargetColumn rename operations for elevata.

Uses the shared rename engine in rename_common.py to provide
consistent validation, collision checks and atomic commit logic.
"""

from metadata.models import TargetColumn
from .rename_common import RenameSpec, dry_run_rename, commit_rename


def _ensure_targetcolumn_rename_allowed(col: TargetColumn) -> list[str]:
  """
  Enforce elevata rules for when a TargetColumn may be renamed.

  Rules:
    - Only columns in schema 'rawcore' are renameable.
    - Derived/technical columns (system_role set) are system-managed and cannot be renamed.
    - Columns in *_hist datasets are system-managed and cannot be renamed.
  """
  errors: list[str] = []

  schema_short = None
  dataset_name = None
  if getattr(col, "target_dataset", None) and getattr(col.target_dataset, "target_schema", None):
    schema_short = getattr(col.target_dataset.target_schema, "short_name", None)
    is_hist = getattr(col.target_dataset, "is_hist", False)

  col_name = getattr(col, "target_column_name", None)

  # Derived / system-managed columns (identified via system_role)
  if getattr(col, "is_protected_name", False):
    errors.append(
      f"Column '{col_name}' is derived (system-managed) and cannot be renamed."
    )

  # History datasets: always locked
  if is_hist:
    errors.append(
      f"Column '{col_name}' belongs to a history dataset and cannot be renamed."
    )

  # Non-rawcore schemas: names are system-managed
  if schema_short is not None and schema_short != "rawcore":
    errors.append(
      f"Column '{col_name}' belongs to schema '{schema_short}', "
      "where names are system-managed. Column names can only be changed in schema 'rawcore'."
    )

  return errors


def _targetcolumn_spec(col: TargetColumn) -> RenameSpec:
  """
  Build a RenameSpec describing how a TargetColumn is renamed.
  The collision scope is all columns within the same dataset.
  """
  return RenameSpec(
    name_attr="target_column_name",
    get_scope_qs=lambda: TargetColumn.objects.filter(target_dataset_id=col.target_dataset_id),
    validator_context="target_column_name",
    collision_label="Target column name",
    collision_scope_label="in this dataset",
  )


def dry_run_targetcolumn_rename(col: TargetColumn, new_name: str) -> dict:
  """
  Validate a TargetColumn rename without persisting changes.
  Applies elevata system-managed rules + syntax and collision checks.
  """
  rule_errors = _ensure_targetcolumn_rename_allowed(col)
  if rule_errors:
    return {"ok": False, "errors": rule_errors}

  return dry_run_rename(col, new_name, _targetcolumn_spec(col))


def commit_targetcolumn_rename(col: TargetColumn, new_name: str, user=None) -> dict:
  """
  Commit a TargetColumn rename atomically.
  Applies elevata system-managed rules before saving.
  Updates audit fields (updated_by, updated_at) if present.
  """
  rule_errors = _ensure_targetcolumn_rename_allowed(col)
  if rule_errors:
    return {"ok": False, "errors": rule_errors}

  return commit_rename(col, new_name, _targetcolumn_spec(col), user=user)
