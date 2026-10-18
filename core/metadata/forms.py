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
from metadata.models import (
  SourceDataset,
  TargetDataset,
  TargetColumn,
  TargetColumnInput,
  TargetSchema,
  target_dataset_downstream_ids,
)
from metadata.generation import validators  # reuse central naming rules
from metadata.rendering.portable_expression import (
  PortableExpressionError,
  normalize_expression_contract,
  normalize_predicate_contract,
)
from metadata.widgets.portable_expression_widget import PortableExpressionWidget

# Single source of truth for serving-friendly naming
from metadata.generation.validators import (
  serving_clean_identifier,
  serving_normalize_identifier,
)



class PortableExpressionFormField(forms.CharField):
  """Form field that validates a hidden portable expression or filter contract."""

  def __init__(
    self,
    *args,
    predicate_only=False,
    allowed_runtime_values=(),
    **kwargs,
  ):
    kwargs.setdefault("required", False)
    kwargs.setdefault("widget", PortableExpressionWidget())
    self.predicate_only = bool(predicate_only)
    self.allowed_runtime_values = tuple(allowed_runtime_values)
    super().__init__(*args, **kwargs)

  def clean(self, value):
    raw = super().clean(value)
    if not raw:
      return None

    import json

    try:
      contract = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError) as exc:
      raise ValidationError("Portable contract contains invalid JSON.") from exc

    try:
      if self.predicate_only:
        return normalize_predicate_contract(
          contract,
          allowed_runtime_values=self.allowed_runtime_values,
        )
      return normalize_expression_contract(
        contract,
        allowed_runtime_values=self.allowed_runtime_values,
      )
    except PortableExpressionError as exc:
      raise ValidationError(str(exc)) from exc


def _portable_contract_initial(instance, field_name) -> str:
  """Return stable JSON for a structured editor without exposing it directly."""
  import json

  contract = getattr(instance, field_name, None)
  if not contract:
    return ""
  return json.dumps(contract, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _portable_expression_initial(instance) -> str:
  """Return the persisted TargetColumn portable expression as stable JSON."""
  return _portable_contract_initial(instance, "portable_expression")


def _expression_column_names(contract) -> set[str]:
  """Collect logical column references from a normalized expression contract."""
  names: set[str] = set()

  def walk(node):
    if not isinstance(node, dict):
      return
    if node.get("type") == "column":
      name = str(node.get("name") or "").strip()
      if name:
        names.add(name)
    for value in node.values():
      if isinstance(value, dict):
        walk(value)
      elif isinstance(value, list):
        for item in value:
          if isinstance(item, dict):
            walk(item)

  if isinstance(contract, dict):
    walk(contract.get("expression"))
  return names


class PortableExpressionFormMixin:
  """Persist the structured editor value into the non-editable model field."""

  system_managed_field_aliases = {
    "portable_expression": "portable_expression_editor",
  }

  def _configure_portable_expression_editor(self, column_names):
    field = self.fields.get("portable_expression_editor")
    if field is None:
      return
    field.initial = _portable_expression_initial(self.instance)
    field.widget.column_names = sorted({str(name) for name in column_names if str(name).strip()})
    posted_manual = ""
    if getattr(self, "data", None):
      posted_manual = str(self.data.get("manual_expression") or "").strip()
    field.widget.manual_expression_present = bool(
      posted_manual
      or str(getattr(self.instance, "manual_expression", "") or "").strip()
    )

    manual_field = self.fields.get("manual_expression")
    if manual_field is not None:
      manual_field.label = "Target-specific SQL fallback"
      manual_field.help_text = (
        "Use only when the transformation cannot be expressed as a portable expression. "
        "A portable expression takes precedence when both are defined."
      )

  def _order_portable_expression_fields(self, anchor_names):
    """Place portable authoring directly after the relevant lineage fields."""
    portable_name = "portable_expression_editor"
    manual_name = "manual_expression"
    if portable_name not in self.fields:
      return

    ordered_names = [
      name for name in self.fields
      if name not in (portable_name, manual_name)
    ]
    anchor_positions = [
      ordered_names.index(name)
      for name in anchor_names
      if name in ordered_names
    ]
    insert_at = max(anchor_positions) + 1 if anchor_positions else len(ordered_names)
    ordered_names.insert(insert_at, portable_name)
    if manual_name in self.fields:
      ordered_names.insert(insert_at + 1, manual_name)
    self.order_fields(ordered_names)

  def _validate_expression_columns(self, cleaned, allowed_names):
    contract = cleaned.get("portable_expression_editor")
    if not contract:
      return cleaned
    referenced = _expression_column_names(contract)
    allowed = {str(name) for name in allowed_names if str(name).strip()}
    unknown = sorted(referenced - allowed)
    if unknown:
      self.add_error(
        "portable_expression_editor",
        "Portable expression references columns outside the selected input lineage: "
        + ", ".join(unknown)
        + ".",
      )
    return cleaned

  def save(self, commit=True):
    instance = super().save(commit=False)
    field = self.fields.get("portable_expression_editor")
    if field is not None and not field.disabled:
      instance.portable_expression = self.cleaned_data.get("portable_expression_editor")
    if commit:
      instance.save()
      if hasattr(self, "save_m2m"):
        self.save_m2m()
    return instance


class QueryFilterAuthoringForm(forms.Form):
  """Portable predicate editor used to create and edit Query FILTER nodes."""

  predicate = PortableExpressionFormField(
    label="Filter",
    predicate_only=True,
    required=True,
    widget=PortableExpressionWidget(
      editor_kind="filter",
      column_label="Input column",
      intro_text="Build a platform-neutral row filter from columns exposed by the input query node.",
      empty_text="No filter defined.",
    ),
  )

  def __init__(self, *args, column_names=(), initial_contract=None, **kwargs):
    super().__init__(*args, **kwargs)
    ordered_column_names = []
    seen = set()
    for name in column_names:
      value = str(name).strip()
      if not value:
        continue
      key = value.lower()
      if key in seen:
        continue
      seen.add(key)
      ordered_column_names.append(value)

    self.column_names = set(ordered_column_names)
    field = self.fields["predicate"]
    field.widget.column_names = ordered_column_names
    if initial_contract:
      import json
      field.initial = json.dumps(
        initial_contract,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
      )

  def clean_predicate(self):
    contract = self.cleaned_data.get("predicate")
    if not contract:
      raise ValidationError("Define a filter before saving the FILTER node.")

    unknown = sorted(_expression_column_names(contract) - self.column_names)
    if unknown:
      raise ValidationError(
        "Portable filter references columns outside the input query contract: "
        + ", ".join(unknown)
        + "."
      )
    return contract


class SourceDatasetForm(forms.ModelForm):
  """SourceDataset form with structured static and incremental filter editors."""

  portable_static_filter_editor = PortableExpressionFormField(
    label="Portable static filter",
    predicate_only=True,
    widget=PortableExpressionWidget(
      editor_kind="filter",
      column_label="Source column",
      intro_text="Build a platform-neutral filter from columns of this source dataset.",
      empty_text="No portable static filter defined.",
    ),
  )
  portable_increment_filter_editor = PortableExpressionFormField(
    label="Portable increment filter",
    predicate_only=True,
    allowed_runtime_values=("delta_cutoff",),
    widget=PortableExpressionWidget(
      editor_kind="filter",
      column_label="Source column",
      intro_text=(
        "Build a platform-neutral incremental filter from source columns and the Delta cutoff runtime value."
      ),
      empty_text="No portable increment filter defined.",
      runtime_values=(("delta_cutoff", "Delta cutoff"),),
    ),
  )

  class Meta:
    model = SourceDataset
    fields = "__all__"

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

    source_names = []
    if getattr(self.instance, "pk", None):
      source_names = list(
        self.instance.source_columns.order_by("ordinal_position", "id").values_list(
          "source_column_name",
          flat=True,
        )
      )

    self._configure_source_filter_editor(
      "portable_static_filter_editor",
      "portable_static_filter",
      "static_filter",
      source_names,
      (
        "A deprecated source-specific SQL static filter is also stored. "
        "The portable filter takes precedence when present; clearing it restores the SQL fallback."
      ),
    )
    self._configure_source_filter_editor(
      "portable_increment_filter_editor",
      "portable_increment_filter",
      "increment_filter",
      source_names,
      (
        "A deprecated source-specific SQL increment filter is also stored. "
        "The portable filter takes precedence when present; clearing it restores the SQL fallback."
      ),
    )

    static_sql = self.fields.get("static_filter")
    if static_sql is not None:
      static_sql.label = "Deprecated static SQL fallback"
    increment_sql = self.fields.get("increment_filter")
    if increment_sql is not None:
      increment_sql.label = "Deprecated increment SQL fallback"

    self._order_source_filter_fields()

  def _configure_source_filter_editor(
    self,
    editor_name,
    model_field_name,
    fallback_field_name,
    source_names,
    fallback_note,
  ):
    field = self.fields.get(editor_name)
    if field is None:
      return
    field.initial = _portable_contract_initial(self.instance, model_field_name)
    field.widget.column_names = sorted({str(name) for name in source_names if str(name).strip()})

    posted_fallback = ""
    if getattr(self, "data", None):
      posted_fallback = str(self.data.get(fallback_field_name) or "").strip()
    field.widget.fallback_present = bool(
      posted_fallback
      or str(getattr(self.instance, fallback_field_name, "") or "").strip()
    )
    field.widget.fallback_note = fallback_note

  def _order_source_filter_fields(self):
    """Keep each portable filter immediately before its deprecated SQL fallback."""
    preferred_order = [
      "source_system",
      "schema_name",
      "source_dataset_name",
      "description",
      "integrate",
      "portable_static_filter_editor",
      "static_filter",
      "incremental",
      "portable_increment_filter_editor",
      "increment_filter",
      "manual_model",
      "distinct_select",
      "owner",
      "generate_raw_table",
      "ingestion_config",
      "active",
    ]
    ordered = [name for name in preferred_order if name in self.fields]
    ordered.extend(name for name in self.fields if name not in ordered)
    self.order_fields(ordered)

  def _validate_filter_columns(self, editor_name, cleaned, source_names):
    contract = cleaned.get(editor_name)
    if not contract:
      return
    referenced = _expression_column_names(contract)
    allowed = {str(name) for name in source_names if str(name).strip()}
    unknown = sorted(referenced - allowed)
    if unknown:
      self.add_error(
        editor_name,
        "Portable filter references columns outside this SourceDataset: "
        + ", ".join(unknown)
        + ".",
      )

  def clean(self):
    cleaned = super().clean()
    source_names = []
    if getattr(self.instance, "pk", None):
      source_names = list(
        self.instance.source_columns.values_list("source_column_name", flat=True)
      )
    self._validate_filter_columns("portable_static_filter_editor", cleaned, source_names)
    self._validate_filter_columns("portable_increment_filter_editor", cleaned, source_names)
    return cleaned

  def save(self, commit=True):
    instance = super().save(commit=False)
    instance.portable_static_filter = self.cleaned_data.get("portable_static_filter_editor")
    instance.portable_increment_filter = self.cleaned_data.get("portable_increment_filter_editor")
    if commit:
      instance.save()
      if hasattr(self, "save_m2m"):
        self.save_m2m()
    return instance


class TargetDatasetForm(forms.ModelForm):
  """Validation form for TargetDataset with consistent naming rules."""

  def _is_serving_schema(self) -> bool:
    schema = (
      self.cleaned_data.get("target_schema")
      or getattr(self.instance, "target_schema", None)
    )
    return getattr(schema, "short_name", None) == "serving"

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

    allowed_upstream_layers = {
      "bizcore": ["rawcore", "bizcore"],
      "serving": ["rawcore", "bizcore", "serving"],
    }
    allowed_layers = allowed_upstream_layers.get(schema_short)
    if allowed_layers is None:
      f.queryset = f.queryset.order_by("target_dataset_name")
      return

    qs = (
      f.queryset
      .filter(target_schema__short_name__in=allowed_layers)
      .order_by("target_schema__short_name", "target_dataset_name")
    )
    current_id = getattr(self.instance, "pk", None)
    if current_id:
      forbidden_ids = target_dataset_downstream_ids(current_id)
      forbidden_ids.add(current_id)
      qs = qs.exclude(pk__in=forbidden_ids)
      if forbidden_ids:
        f.help_text = (
          "Datasets that already depend on this dataset are excluded because "
          "selecting them as upstream would create a dependency cycle."
        )
    f.queryset = qs

  class Meta:
    model = TargetDataset
    fields = "__all__"

  def clean_target_dataset_name(self):
    name = self.cleaned_data["target_dataset_name"]

    # Serving: friendly names allowed
    if self._is_serving_schema():
      name = serving_clean_identifier(name, kind="Dataset")

      # Optional: prevent duplicates (normalized) inside serving schema
      schema = self.cleaned_data.get("target_schema") or getattr(self.instance, "target_schema", None)
      if schema and getattr(schema, "pk", None):
        norm = serving_normalize_identifier(name)
        qs = TargetDataset.objects.filter(target_schema=schema)
        if self.instance and getattr(self.instance, "pk", None):
          qs = qs.exclude(pk=self.instance.pk)
        # Compare in Python to avoid collation differences between DBs
        for other in qs.values_list("target_dataset_name", flat=True):
          if serving_normalize_identifier(other or "") == norm:
            raise ValidationError(
              f"Dataset name duplicates another dataset after normalization: '{other}' vs '{name}'."
            )
      return name

    # Non-serving: strict technical naming via shared validator
    try:
      validators.validate_or_raise(name, context="target_dataset_name")
    except Exception as e:
      raise forms.ValidationError(str(e))
    return name

class TargetColumnForm(PortableExpressionFormMixin, forms.ModelForm):
  """Validation and portable-expression authoring form for TargetColumn."""

  portable_expression_editor = PortableExpressionFormField(
    label="Portable expression",
  )

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

    self.fields["target_dataset"].widget.attrs["data-elevata-refresh"] = "1"

    editor_names = []
    try:
      editor_names = list(
        self.instance.input_links.filter(active=True).values_list(
          "upstream_target_column__target_column_name",
          flat=True,
        )
      )
      editor_names += list(
        self.instance.input_links.filter(active=True).values_list(
          "source_column__source_column_name",
          flat=True,
        )
      )
    except Exception:
      editor_names = []
    self._configure_portable_expression_editor(editor_names)
    self._order_portable_expression_fields(("upstream_columns",))

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

    selected_ids = []
    if getattr(self, "data", None):
      selected_ids = self.data.getlist("upstream_columns")
    if not selected_ids:
      initial_upstream = self.initial.get("upstream_columns")
      if initial_upstream is not None:
        try:
          selected_ids = [str(obj.pk) for obj in initial_upstream]
        except TypeError:
          selected_ids = [str(initial_upstream)]
    if selected_ids:
      editor_names = list(
        f.queryset.filter(pk__in=selected_ids).values_list("target_column_name", flat=True)
      )
      self._configure_portable_expression_editor(editor_names)

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

    # Collision inside the same dataset
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

    td = TargetDataset.objects.select_related("target_schema").filter(pk=dataset_id).first()

    # Serving: friendly names allowed
    if td and getattr(getattr(td, "target_schema", None), "short_name", None) == "serving":
      new_name = serving_clean_identifier(new_name, kind="Column")

      # Normalized collision (case-insensitive + collapse spaces)
      norm = serving_normalize_identifier(new_name)
      existing = (
        TargetColumn.objects
        .filter(target_dataset_id=dataset_id)
        .exclude(pk=getattr(col, "pk", None) or None)
        .values_list("target_column_name", flat=True)
      )
      for other in existing:
        if serving_normalize_identifier(other or "") == norm:
          raise ValidationError("Name already exists in this dataset (after normalization).")

      return new_name

    # Non-serving: strict technical naming via shared validator
    validators.validate_or_raise(new_name, context="target_column_name")

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
    """Apply datatype propagation and validate expression lineage."""
    cleaned = super().clean()

    upstream_cols = cleaned.get("upstream_columns")
    try:
      upstream_list = list(upstream_cols.all()) if upstream_cols is not None else []
    except Exception:
      upstream_list = list(upstream_cols) if upstream_cols else []

    cleaned = self._validate_expression_columns(
      cleaned,
      [getattr(col, "target_column_name", "") for col in upstream_list],
    )

    portable_expr = cleaned.get("portable_expression_editor")
    manual_expr = (cleaned.get("manual_expression") or "").strip()
    if portable_expr is not None or manual_expr or len(upstream_list) != 1:
      return cleaned

    src = upstream_list[0]

    if not cleaned.get("datatype"):
      cleaned["datatype"] = src.datatype or ""
    if cleaned.get("max_length") in (None, "", 0):
      cleaned["max_length"] = src.max_length
    if cleaned.get("decimal_precision") in (None, "", 0):
      cleaned["decimal_precision"] = src.decimal_precision
    if cleaned.get("decimal_scale") in (None, "", 0):
      cleaned["decimal_scale"] = src.decimal_scale
    if cleaned.get("nullable") is None:
      cleaned["nullable"] = src.nullable

    return cleaned



class TargetColumnInputForm(PortableExpressionFormMixin, forms.ModelForm):
  """Form for source-specific column lineage with portable-expression editing."""

  portable_expression_editor = PortableExpressionFormField(
    label="Portable expression",
  )

  class Meta:
    model = TargetColumnInput
    fields = "__all__"

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    names = []
    upstream = getattr(self.instance, "upstream_target_column", None)
    source = getattr(self.instance, "source_column", None)
    if upstream is not None:
      names.append(getattr(upstream, "target_column_name", ""))
    if source is not None:
      names.append(getattr(source, "source_column_name", ""))

    if getattr(self, "data", None):
      upstream_id = self.data.get("upstream_target_column")
      source_id = self.data.get("source_column")
      upstream_field = self.fields.get("upstream_target_column")
      source_field = self.fields.get("source_column")
      if upstream_id and upstream_field is not None:
        selected = upstream_field.queryset.filter(pk=upstream_id).first()
        if selected is not None:
          names.append(getattr(selected, "target_column_name", ""))
      if source_id and source_field is not None:
        selected = source_field.queryset.filter(pk=source_id).first()
        if selected is not None:
          names.append(getattr(selected, "source_column_name", ""))

    self._configure_portable_expression_editor(names)
    self._order_portable_expression_fields(("upstream_target_column", "source_column"))

  def clean(self):
    cleaned = super().clean()
    names = []
    upstream = cleaned.get("upstream_target_column")
    source = cleaned.get("source_column")
    if upstream is not None:
      names.append(getattr(upstream, "target_column_name", ""))
    if source is not None:
      names.append(getattr(source, "source_column_name", ""))
    return self._validate_expression_columns(cleaned, names)
