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
TargetDataset rename operations for elevata.

Uses the shared rename engine in rename_common.py to provide
consistent validation, collision checks and atomic commit logic.
"""

from metadata.models import TargetDataset, TargetColumn
from .rename_common import RenameSpec, dry_run_rename, commit_rename
from django.db import transaction
from metadata.services.rename_common import sync_key_former_names_for_rawcore_dataset


def _ensure_targetdataset_rename_allowed(ds: TargetDataset) -> list[str]:
  """
  Enforce elevata rules for when a TargetDataset may be renamed.

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
    extra_update_fields=["former_names"],
  )


def dry_run_targetdataset_rename(ds: TargetDataset, new_name: str) -> dict:
  """
  Validate a TargetDataset rename without persisting changes.
  Applies elevata system-managed rules + syntax / collision checks.
  """
  rule_errors = _ensure_targetdataset_rename_allowed(ds)
  if rule_errors:
    return {"ok": False, "errors": rule_errors}

  return dry_run_rename(ds, new_name, _targetdataset_spec(ds))


def commit_targetdataset_rename(ds: TargetDataset, new_name: str, user=None) -> dict:
  """
  Commit a TargetDataset rename atomically.
  Applies elevata system-managed rules before saving.
  """
  rule_errors = _ensure_targetdataset_rename_allowed(ds)
  if rule_errors:
    return {"ok": False, "errors": rule_errors}
  
  old_name = (ds.target_dataset_name or "").strip()
  new_name = (new_name or "").strip()

  if old_name and new_name and old_name != new_name:
    former = list(getattr(ds, "former_names", None) or [])
    # case-insensitive de-duplication
    if old_name.lower() not in {n.lower() for n in former}:
      former.append(old_name)
    ds.former_names = former


  # Commit rename first (atomic)
  res = commit_rename(ds, new_name, _targetdataset_spec(ds), user=user)
  if not res.get("ok"):
    return res

  # Rawcore-only: deterministically sync hist dataset name + key former_names.
  schema_short = getattr(getattr(ds, "target_schema", None), "short_name", None)
  if schema_short != "rawcore":
    return res
  if ds.target_dataset_name.endswith("_hist"):
    return res
  if not getattr(ds, "historize", False):
    return res

  def _after_commit():
    lineage_key = getattr(ds, "lineage_key", None)
    hist_td = None
    if lineage_key:
      hist_td = (
        TargetDataset.objects
        .filter(
          target_schema=ds.target_schema,
          lineage_key=lineage_key,
          target_dataset_name__endswith="_hist",
        )
        .first()
      )

    # Rename hist dataset to "<base>_hist" and preserve hist former_names
    if hist_td is not None:
      desired_hist_name = f"{ds.target_dataset_name}_hist"
      old_hist_name = (hist_td.target_dataset_name or "").strip()
      if old_hist_name and desired_hist_name and old_hist_name != desired_hist_name:
        hist_former = list(getattr(hist_td, "former_names", None) or [])
        if old_hist_name.lower() not in {n.lower() for n in hist_former}:
          hist_former.append(old_hist_name)
        hist_td.former_names = hist_former
        hist_td.target_dataset_name = desired_hist_name
        hist_td.save(update_fields=["former_names", "target_dataset_name"])

    sync_key_former_names_for_rawcore_dataset(base_td=ds, hist_td=hist_td)

  # Ensure this happens even if caller saves in same transaction
  transaction.on_commit(_after_commit)

  return res