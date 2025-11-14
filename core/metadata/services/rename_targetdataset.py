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
TargetDataset rename operations for Elevata.

Uses the shared rename engine in rename_common.py to provide
consistent validation, collision checks and atomic commit logic.
"""

from metadata.models import TargetDataset
from .rename_common import RenameSpec, dry_run_rename, commit_rename


def _ensure_targetdataset_rename_allowed(ds: TargetDataset) -> list[str]:
  """
  Enforce Elevata rules for when a TargetDataset may be renamed.

  Rules:
    - Only datasets in schema 'rawcore' are renameable.
  """
  errors: list[str] = []

  schema_short = None
  if getattr(ds, "target_schema", None):
    schema_short = getattr(ds.target_schema, "short_name", None)

  if schema_short is not None and schema_short != "rawcore":
    errors.append(
      f"Dataset '{ds.target_dataset_name}' belongs to schema '{schema_short}', "
      "where names are system-managed. Dataset names can only be changed in schema 'rawcore'."
    )

  return errors


def _targetdataset_spec(ds: TargetDataset) -> RenameSpec:
  """
  Build a RenameSpec describing how a TargetDataset is renamed.
  The collision scope is all datasets in the system (or within a project, if introduced).
  """
  return RenameSpec(
    name_attr="target_dataset_name",
    get_scope_qs=lambda: TargetDataset.objects.all(),
    validator_context="target_dataset_name",
    collision_label="Target dataset name",
    collision_scope_label="in this environment",
  )


def dry_run_targetdataset_rename(ds: TargetDataset, new_name: str) -> dict:
  """
  Validate a TargetDataset rename without persisting changes.
  Applies Elevata system-managed rules + syntax / collision checks.
  """
  rule_errors = _ensure_targetdataset_rename_allowed(ds)
  if rule_errors:
    return {"ok": False, "errors": rule_errors}

  return dry_run_rename(ds, new_name, _targetdataset_spec(ds))


def commit_targetdataset_rename(ds: TargetDataset, new_name: str, user=None) -> dict:
  """
  Commit a TargetDataset rename atomically.
  Applies Elevata system-managed rules before saving.
  """
  rule_errors = _ensure_targetdataset_rename_allowed(ds)
  if rule_errors:
    return {"ok": False, "errors": rule_errors}

  return commit_rename(ds, new_name, _targetdataset_spec(ds), user=user)
