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

from django import forms
from django.apps import apps

from django.core.exceptions import ValidationError
from metadata.models import TargetDataset, TargetColumn, TargetSchema
from metadata.generation import validators  # reuse central naming rules

class TargetDatasetForm(forms.ModelForm):
  """Validation form for TargetDataset with consistent naming rules."""

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

    self.fields["target_schema"].widget.attrs["data-elevata-refresh"] = "1"

    # ------------------------------------------------------------
    # Restrict TargetSchema: user may only CREATE bizcore/serving datasets.
    # Keep existing datasets editable even if they already sit in an auto-layer.
    # ------------------------------------------------------------
    ts_field = self.fields.get("target_schema")
    if ts_field is not None:
      allowed = ["bizcore", "serving"]
      allowed_qs = ts_field.queryset.filter(short_name__in=allowed).order_by("short_name")

      # If editing an existing dataset in a non-allowed schema, keep that current
      # choice in the dropdown to avoid "Select a valid choice".
      current_id = getattr(self.instance, "target_schema_id", None)
      if current_id:
        current_short = getattr(getattr(self.instance, "target_schema", None), "short_name", None)
        if current_short and current_short not in allowed:
          current_qs = ts_field.queryset.filter(pk=current_id)
          ts_field.queryset = (allowed_qs | current_qs).distinct()
        else:
          ts_field.queryset = allowed_qs
      else:
        # New object: only allow bizcore/serving
        ts_field.queryset = allowed_qs

    f = self.fields.get("upstream_datasets")
    if f is None:
      return    

    # Resolve selected target_schema id (works for new/edit + HTMX refresh)
    ts_id = None
    if getattr(self, "data", None):
      ts_id = self.data.get("target_schema") or None
    if not ts_id:
      ts_id = self.initial.get("target_schema") or None
    if not ts_id:
      ts_id = getattr(self.instance, "target_schema_id", None)

    # UX bonus: empty list until schema selected
    if not ts_id:
      f.queryset = f.queryset.none()
      f.help_text = "Select a TargetSchema first to see eligible upstream datasets."
      return

    schema_short = (
      TargetSchema.objects
      .filter(pk=ts_id)
      .values_list("short_name", flat=True)
      .first()
    )

    qs = f.queryset
    if schema_short == "bizcore":
      # Bizcore: only rawcore (incl _hist)
      f.queryset = qs.filter(target_schema__short_name="rawcore")
    elif schema_short == "serving":
      # Serving: rawcore + bizcore
      f.queryset = qs.filter(target_schema__short_name__in=["rawcore", "bizcore"])
    else:
      f.queryset = qs

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

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

    self.fields["target_dataset"].widget.attrs["data-elevata-refresh"] = "1"

    f = self.fields.get("upstream_columns")
    if f is None:
      return

    # Resolve selected target_dataset id (new/edit + HTMX refresh)
    td_id = None
    if getattr(self, "data", None):
      td_id = self.data.get("target_dataset") or None
    if not td_id:
      td_id = self.initial.get("target_dataset") or None
    if not td_id:
      td_id = getattr(self.instance, "target_dataset_id", None)

    # UX bonus: empty list until dataset selected
    if not td_id:
      f.queryset = f.queryset.none()
      f.help_text = "Select a TargetDataset first to see eligible upstream columns."
      return

    td = (
      TargetDataset.objects
      .filter(pk=td_id)
      .prefetch_related("upstream_datasets")
      .first()
    )
    if not td:
      f.queryset = f.queryset.none()
      return

    upstreams = list(td.upstream_datasets.all())
    if not upstreams:
      f.queryset = f.queryset.none()
      f.help_text = "No upstream datasets configured on this TargetDataset yet."
      return

    f.queryset = (
      f.queryset
      .filter(target_dataset__in=upstreams)
      .order_by("target_dataset__target_dataset_name", "ordinal_position")
    )

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
    # When creating, instance may not have pk; fallback to submitted dataset id.
    # In scoped create forms, target_dataset field is removed from POST,
    # but the instance is already bound to the parent before validation.
    dataset_id = (
      getattr(col, "target_dataset_id", None)
      or self.data.get("target_dataset")
      or self.initial.get("target_dataset")
    )

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

  def clean(self):
    """
    Safety net for datatype propagation:
    If no manual_expression is set and exactly one upstream column is selected,
    hard-overwrite datatype/length/precision/scale/nullability from upstream.
    """
    cleaned = super().clean()

    manual_expr = (cleaned.get("manual_expression") or "").strip()
    if manual_expr:
      return cleaned

    upstream_cols = cleaned.get("upstream_columns")
    if upstream_cols is None:
      return cleaned

    try:
      upstream_list = list(upstream_cols.all())
    except Exception:
      upstream_list = list(upstream_cols) if upstream_cols else []

    if len(upstream_list) != 1:
      return cleaned

    src = upstream_list[0]
    cleaned["datatype"] = src.datatype or ""
    cleaned["max_length"] = src.max_length
    cleaned["decimal_precision"] = src.decimal_precision
    cleaned["decimal_scale"] = src.decimal_scale
    cleaned["nullable"] = src.nullable

    return cleaned
