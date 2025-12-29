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

from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Iterable, Any
from django.core.exceptions import ValidationError
from django.db import transaction
from metadata.generation import validators  # shared naming rules
from metadata.models import TargetColumn


@dataclass
class RenameSpec:
  """
  Declarative config describing *how* to rename a given model instance.
  """
  name_attr: str
  get_scope_qs: Callable[[], Iterable]
  validator_context: str
  extra_update_fields: list[str] | None = None

  # New: human readable labels for collision messages
  collision_label: str = "Name"
  collision_scope_label: str | None = None  # e.g. "in this dataset"


def validate_name(new_name: str, context: str) -> list[str]:
  """
  Apply elevata's shared naming rules with a specific context.
  Returns a list of error messages (empty if valid).
  """
  try:
    validators.validate_or_raise(new_name, context=context)
    return []
  except ValidationError as e:
    return [str(e)]


def check_collision(scope_qs, name_attr: str, new_name: str,
                    exclude_pk, label: str, scope_label: str | None) -> list[str]:
  """
  Ensure the new name does not already exist inside the scope.

  scope_qs is expected to be a QuerySet, not a callable.
  """
  filt = {name_attr: new_name}
  qs = scope_qs.filter(**filt)
  if exclude_pk is not None:
    qs = qs.exclude(pk=exclude_pk)

  if not qs.exists():
    return []

  if scope_label:
    return [f"{label} '{new_name}' already exists {scope_label}."]
  return [f"{label} '{new_name}' already exists in this scope."]


def dry_run_rename(instance, new_name: str, spec: RenameSpec) -> dict:
  """
  Non-destructive validation for a rename operation on any instance.

  Returns { ok, errors, impacts? }.
  The caller can enrich 'impacts' with lineage/SQL preview if desired.
  """
  errors = []
  errors += validate_name(new_name, spec.validator_context)
  errors += check_collision(
    spec.get_scope_qs(),
    spec.name_attr,
    new_name,
    exclude_pk=instance.pk,
    label=spec.collision_label,
    scope_label=spec.collision_scope_label,
  )
  if errors:
    return {"ok": False, "errors": errors}

  # Placeholder for lineage/SQL preview (caller may extend)
  impacts = {
    "models": [],
    "sql_diff": {
      "before": f"... {getattr(instance, spec.name_attr)} ...",
      "after":  f"... {new_name} ..."
    }
  }
  return {"ok": True, "impacts": impacts}

def _append_former_name_any(former_names: Any, old: str) -> Any:
  """
  Append `old` to `former_names` in a case-insensitive de-duped way.

  Supports:
    - JSONField list[str] (preferred)
    - legacy comma-separated string
    - None
  Returns the same "shape" as the input (list stays list, str stays str).
  """
  old = (old or "").strip()
  if not old:
    return former_names

  # JSONField list[str]
  if isinstance(former_names, list):
    lowered = {str(x).strip().lower() for x in former_names if str(x).strip()}
    if old.lower() not in lowered:
      former_names.append(old)
    return former_names

  # Unknown type: do nothing, but keep existing value
  return former_names


@transaction.atomic
def commit_rename(instance, new_name: str, spec: RenameSpec, user=None) -> dict:
  """
  Persist the rename atomically, after validating rules & collisions.
  Sets updated_by/updated_at when present on the instance.
  """
  errors = []
  errors += validate_name(new_name, spec.validator_context)
  errors += check_collision(
    spec.get_scope_qs(),
    spec.name_attr,
    new_name,
    exclude_pk=instance.pk,
    label=spec.collision_label,
    scope_label=spec.collision_scope_label,
  )
  if errors:
    return {"ok": False, "errors": errors}
  
  old = (getattr(instance, spec.name_attr) or "").strip()
  new = (new_name or "").strip()

  # Maintain former_names automatically when present on the instance and enabled by spec.
  # We only touch it if the name actually changes.
  if old and new and old != new and hasattr(instance, "former_names"):
    current = getattr(instance, "former_names", None)
    updated = _append_former_name_any(current, old)
    setattr(instance, "former_names", updated)

  setattr(instance, spec.name_attr, new)

  # Optional audit fields
  if hasattr(instance, "updated_by") and getattr(user, "pk", None):
    instance.updated_by = user
  if hasattr(instance, "updated_at"):
    from django.utils import timezone
    instance.updated_at = timezone.now()

  update_fields = [spec.name_attr]
  if hasattr(instance, "updated_by"):
    update_fields.append("updated_by")
  if hasattr(instance, "updated_at"):
    update_fields.append("updated_at")
  if spec.extra_update_fields:
    update_fields.extend(spec.extra_update_fields)

  instance.save(update_fields=update_fields)

  return {"ok": True, "id": instance.pk, "old_name": old, "new_name": new_name}


def _ensure_former_name(former_names: list[str] | None, old_name: str) -> list[str]:
  """
  Add old_name to former_names if missing (case-insensitive), preserving original casing.
  former_names is a JSONField (list[str]).
  """
  old = (old_name or "").strip()
  cur = list(former_names or [])
  if not old:
    return cur
  low = {s.lower() for s in cur if isinstance(s, str)}
  if old.lower() not in low:
    cur.append(old)
  return cur


def sync_key_former_names_for_rawcore_dataset(*, base_td, hist_td=None) -> None:
  """
  Ensure former_names of convention-based key columns are populated so the planner can RENAME_COLUMN.

  Base:
    - <base_dataset>_key should include <old_base_dataset>_key for each former dataset name

  Hist:
    - <hist_dataset>_key should include <old_hist_dataset>_key for each former hist dataset name
    - entity key column name equals base key (<base_dataset>_key), must also include old base key names
  """
  base_key_now = f"{base_td.target_dataset_name}_key"
  base_key_col = TargetColumn.objects.filter(
    target_dataset_id=base_td.id,
    target_column_name=base_key_now,
  ).first()
  if base_key_col:
    former = list(getattr(base_key_col, "former_names", None) or [])
    for old_ds in list(getattr(base_td, "former_names", None) or []):
      former = _ensure_former_name(former, f"{old_ds}_key")
    if former != list(getattr(base_key_col, "former_names", None) or []):
      base_key_col.former_names = former
      base_key_col.save(update_fields=["former_names"])

  if not hist_td:
    return

  hist_key_now = f"{hist_td.target_dataset_name}_key"
  hist_key_col = TargetColumn.objects.filter(
    target_dataset_id=hist_td.id,
    target_column_name=hist_key_now,
  ).first()
  if hist_key_col:
    former = list(getattr(hist_key_col, "former_names", None) or [])
    for old_hist_ds in list(getattr(hist_td, "former_names", None) or []):
      former = _ensure_former_name(former, f"{old_hist_ds}_key")
    if former != list(getattr(hist_key_col, "former_names", None) or []):
      hist_key_col.former_names = former
      hist_key_col.save(update_fields=["former_names"])

  # entity key in hist == base key name
  hist_entity_col = TargetColumn.objects.filter(
    target_dataset_id=hist_td.id,
    system_role="entity_key",
  ).first()

  if hist_entity_col:
    former = list(getattr(hist_entity_col, "former_names", None) or [])
    for old_base_ds in list(getattr(base_td, "former_names", None) or []):
      former = _ensure_former_name(former, f"{old_base_ds}_key")
    if former != list(getattr(hist_entity_col, "former_names", None) or []):
      hist_entity_col.former_names = former
      hist_entity_col.save(update_fields=["former_names"])
