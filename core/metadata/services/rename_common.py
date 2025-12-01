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
from typing import Callable, Iterable
from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils import timezone
from core.metadata.generation import validators  # shared naming rules


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
  
  old = getattr(instance, spec.name_attr)
  setattr(instance, spec.name_attr, new_name)

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
