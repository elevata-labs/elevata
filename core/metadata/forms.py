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

from django import forms
from django.core.exceptions import ValidationError
from metadata.models import TargetDataset, TargetColumn
from metadata.generation import validators  # reuse central naming rules

class TargetDatasetForm(forms.ModelForm):
  """Validation form for TargetDataset with consistent naming rules."""

  class Meta:
    model = TargetDataset
    fields = "__all__"

  def clean_target_dataset_name(self):
    name = self.cleaned_data["target_dataset_name"]
    try:
      validators.validate_or_raise(name, context="target_dataset_name")
    except Exception as e:
      raise forms.ValidationError(str(e))
    return name


class TargetColumnForm(forms.ModelForm):
  """
  ModelForm for TargetColumn ensuring consistent validation for both
  normal CRUD (full-page form) and inline/row edits.

  Responsibilities:
    - Validate technical column name using elevata's shared validators
    - Enforce uniqueness within the parent dataset
    - Avoid duplicate logic between UI paths (scoped + normal CRUD)
  """

  class Meta:
    model = TargetColumn
    fields = "__all__"

  def clean_target_column_name(self):
    """
    Validate 'target_column_name' using shared rules and collision checks.

    Behavior:
      - If unchanged, return as-is (avoid false positives)
      - Use core naming validator (lowercase, underscore, length, etc.)
      - Ensure uniqueness inside the same TargetDataset
    Raises:
      ValidationError if invalid format or name collision occurs.
    """
    new_name = self.cleaned_data["target_column_name"]
    col: TargetColumn = self.instance

    # Skip validation if the value did not change
    if col and col.pk and new_name == col.target_column_name:
      return new_name

    # 1) Syntax & convention via shared validator
    validators.validate_or_raise(new_name, context="target_column_name")

    # 2) Collision inside the same dataset
    #    When creating, instance may not have pk; fallback to submitted dataset id.
    dataset_id = col.target_dataset_id if (col and col.pk) else self.data.get("target_dataset")
    if not dataset_id:
      # Defensive guard; your UI typically provides it
      raise ValidationError("Missing dataset reference for collision check.")

    qs = TargetColumn.objects.filter(
      target_dataset_id=dataset_id,
      target_column_name=new_name
    )
    if col and col.pk:
      qs = qs.exclude(pk=col.pk)

    if qs.exists():
      raise ValidationError("Name already exists in this dataset.")

    return new_name
