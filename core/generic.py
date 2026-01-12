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

from django.shortcuts import render, get_object_or_404, redirect
from django.views import View
from django.urls import reverse_lazy
from django.contrib import messages
from django.utils.translation import gettext_lazy as _
from django.forms import modelform_factory
from django.http import Http404, HttpResponse, HttpResponseNotFound
from crum import get_current_user
from django.contrib.auth.mixins import LoginRequiredMixin
from django.conf import settings
from django.db import models
from django import forms as djforms
from django.apps import apps
from django.core.exceptions import FieldDoesNotExist
from django.db.models import ManyToManyField, Q

# ------------------------------------------------------------
# Utility helper
# ------------------------------------------------------------
def display_key(*parts):
  """Build a human-friendly composite key; ignores empty parts and casts to str."""
  cleaned = []
  for p in parts:
    if p is None:
      continue
    s = str(p).strip()
    if s:
      cleaned.append(s)
  return " · ".join(cleaned)

# ------------------------------------------------------------
# Generic CRUD base view
# ------------------------------------------------------------
class GenericCRUDView(LoginRequiredMixin, View):
  """Generic CRUD base view used for all metadata models."""
  login_url = "/accounts/login/"
  redirect_field_name = "next"

  model = None
  template_list = None
  template_form = None
  template_confirm_delete = None
  success_url = None
  action = "list"

  # --------------------------------------------------
  # Dispatch routing
  # --------------------------------------------------
  def dispatch(self, request, *args, **kwargs):
    """Dispatch request by 'action' name."""
    self.action = kwargs.pop("action", self.action)
    pk = kwargs.get("pk")

    # Handle GET actions
    if request.method == "GET":
      if self.action == "list":
        return self.list(request)
      if self.action == "edit":
        return self.edit(request, pk)
      if self.action == "detail": 
        return self.detail(request, pk)
      if self.action == "row":
        return self.row(request, pk)
      if self.action == "row_edit":
        return self.row_edit(request, pk)
      if self.action == "row_new":
        return self.row_new(request)

    # Handle POST actions
    if request.method == "POST":
      if self.action == "edit":
        return self.edit(request, pk)
      # accept both names: row_edit (URL) and row_update (older templates)
      if self.action in ("row_edit", "row_update"):
        return self.row_edit(request, pk)
      if self.action == "row_create":
        return self.row_create(request)
      if self.action == "row_delete":
        return self.row_delete(request, pk)
      # toggle a boolean field inline (HTMX)
      if self.action == "row_toggle":
        return self.row_toggle(request, pk)

    # Handle DELETE (for HTMX)
    if request.method == "DELETE" and self.action == "row_delete":
      return self.row_delete(request, pk)

    return HttpResponseNotFound(f"Invalid action '{self.action}' for {self.__class__.__name__}")

  # --------------------------------------------------
  # Utility helpers
  # --------------------------------------------------
  def _initial_from_querydict(self, qd):
    """
    Convert a QueryDict (request.GET) to a plain dict usable as ModelForm(initial=...).
    Keeps multi-value keys (multi-select) via getlist().
    """
    if not qd:
      return None
    initial = {}
    for key in qd.keys():
      vals = qd.getlist(key)
      initial[key] = vals[0] if len(vals) == 1 else vals
    return initial


  def get_system_managed_locked_fields(self, instance=None):
    """
    Returns a set of field names that should be read-only if this row is system-managed.

    Base configuration comes from settings.ELEVATA_CRUD["metadata"]["system_managed"].
    Optional schema-specific overrides can unlock or add locks based on
    TargetSchema.short_name.

    Special case:
    - For TargetColumn rows that are marked as system_role='surrogate_key',
      we always use the global lock list without schema-level unlocks.
      This ensures that surrogate key column names (and related attributes)
      follow strict system conventions even in rawcore.
    """
    meta_cfg = getattr(settings, "ELEVATA_CRUD", {}).get("metadata", {})
    sysman_cfg_all = meta_cfg.get("system_managed", {})

    model_name = self.model.__name__

    # Start with the global list of locked fields for this model
    base_locked = set(sysman_cfg_all.get(model_name, []))

    # If we have no instance or no schema context, return the base locks
    if instance is None:
      return base_locked

    # Determine schema short name for this instance (if any)
    schema_short_name = self._get_schema_short_name_for_instance(instance)

    # ------------------------------------------------------------------
    # Special case: surrogate key / foreign-key columns are always fully locked
    # ------------------------------------------------------------------
    if model_name == "TargetColumn" and getattr(instance, "system_role", "") != "":
      return base_locked

    # ------------------------------------------------------------------
    # History datasets and their columns are always fully locked.
    # Schema overrides must not unlock any fields here.
    # ------------------------------------------------------------------
    dataset_name = None
    if model_name == "TargetDataset":
      dataset_name = getattr(instance, "target_dataset_name", None)
    elif model_name == "TargetColumn":
      td = getattr(instance, "target_dataset", None)
      if td is not None:
        dataset_name = getattr(td, "target_dataset_name", None)
        if schema_short_name is None:
          schema = getattr(td, "target_schema", None)
          schema_short_name = getattr(schema, "short_name", None)

    if (
      schema_short_name == "rawcore"
      and isinstance(dataset_name, str)
      and dataset_name.endswith("_hist")
    ):
      # fully locked, no schema-based unlocks
      return base_locked

    # ------------------------------------------------------------------
    # Schema-specific overrides (for non-history, non-SK columns)
    # ------------------------------------------------------------------
    if not schema_short_name:
      return base_locked

    schema_overrides = sysman_cfg_all.get("schema_overrides", {})
    model_override = schema_overrides.get(schema_short_name, {}).get(model_name, {})

    unlock_fields = model_override.get("unlock", [])
    base_locked.difference_update(unlock_fields)

    extra_fields = model_override.get("lock_extra", [])
    base_locked.update(extra_fields)

    return base_locked


  def get_schema_readonly_fields(self, instance):
    """
    Return a set of field names that should be read-only for this instance
    based solely on its TargetSchema, regardless of is_system_managed.
    """
    if not instance:
      return set()

    meta_cfg = getattr(settings, "ELEVATA_CRUD", {}).get("metadata", {})
    sysman_cfg_all = meta_cfg.get("system_managed", {})

    schema_short_name = self._get_schema_short_name_for_instance(instance)
    if not schema_short_name:
      return set()

    schema_readonly_cfg = sysman_cfg_all.get("schema_readonly", {})
    schema_cfg = schema_readonly_cfg.get(schema_short_name, {})

    model_cfg = schema_cfg.get(self.model.__name__, [])
    return set(model_cfg)


  def _get_schema_short_name_for_instance(self, instance):
    """
    Try to determine the TargetSchema.short_name for a given instance.

    - For TargetSchema -> use instance.short_name
    - For TargetDataset -> use instance.target_schema.short_name
    - For TargetColumn -> use instance.target_dataset.target_schema.short_name
    """
    if not instance:
      return None

    model_name = instance.__class__.__name__

    # TargetSchema: instance itself is the schema
    if model_name == "TargetSchema":
      return getattr(instance, "short_name", None)

    # TargetDataset: schema is instance.target_schema
    if model_name == "TargetDataset":
      schema = getattr(instance, "target_schema", None)
      if schema is not None:
        return getattr(schema, "short_name", None)
      return None

    # TargetColumn: schema is instance.target_dataset.target_schema
    if model_name == "TargetColumn":
      dataset = getattr(instance, "target_dataset", None)
      if dataset is not None:
        schema = getattr(dataset, "target_schema", None)
        if schema is not None:
          return getattr(schema, "short_name", None)
      return None

    # All other models either do not belong to a TargetSchema
    # or share the same lock behavior regardless of schema.
    return None

  def _get_parent_relation_field_name(self, child_model=None, parent_model=None):
    """
    Return the name of the child-model field that relates to the scoped parent model.

    This is the *child-side relation field name* (usually a ForeignKey),
    e.g. TargetColumn.target_dataset, TargetDatasetJoinPredicate.join, etc.

    Centralized here so all scoped views share a single source of truth.
    """
    child_model = child_model or getattr(self, "model", None)
    parent_model = parent_model or getattr(self, "parent_model", None)
    if not child_model or not parent_model:
      return None

    # small cache per view instance (safe, scoped views are request-scoped)
    cache_key = (getattr(child_model, "__name__", str(child_model)), getattr(parent_model, "__name__", str(parent_model)))
    cache = getattr(self, "_parent_relation_field_cache", None)
    if cache is None:
      cache = {}
      setattr(self, "_parent_relation_field_cache", cache)
    if cache_key in cache:
      return cache[cache_key]

    for f in child_model._meta.fields:
      if getattr(f, "is_relation", False) and getattr(f, "remote_field", None):
        if f.remote_field.model is parent_model:
          cache[cache_key] = f.name
          return f.name

    cache[cache_key] = None
    return None

  # --------------------------------------------------
  # UI field visibility (settings-driven)
  # --------------------------------------------------
  def _get_ui_cfg(self) -> dict:
    meta_cfg = getattr(settings, "ELEVATA_CRUD", {}).get("metadata", {})
    return meta_cfg.get("ui", {}) or {}

  def _get_ui_view_cfg(self, view_kind: str) -> dict:
    """
    view_kind: "list" | "form" | "detail"

    Merging rules (SSOT, non-contradicting):
    - If model_cfg defines include_fields => include-mode (ignore any excludes, incl. defaults)
    - Else => exclude-mode where exclude_fields = defaults.exclude_fields ∪ model_cfg.exclude_fields
    """
    ui_cfg = self._get_ui_cfg()
    defaults = (ui_cfg.get("defaults", {}) or {}).get(view_kind, {}) or {}
    model_cfg = ((ui_cfg.get("models", {}) or {}).get(self.model.__name__, {}) or {}).get(view_kind, {}) or {}

    def _as_list(val):
      if isinstance(val, (list, tuple, set)):
        return [str(x) for x in val]
      return []

    default_include = defaults.get("include_fields", None)
    default_exclude = _as_list(defaults.get("exclude_fields", []))

    model_include = model_cfg.get("include_fields", None)
    model_exclude = _as_list(model_cfg.get("exclude_fields", []))

    # -----------------------------
    # include-mode wins if present
    # -----------------------------
    if model_include is not None:
      if not isinstance(model_include, (list, tuple)):
        # treat invalid include as "not set"
        model_include = None
      else:
        return {
          "include_fields": [str(x) for x in model_include],
        }

    # If defaults define include_fields and model does not override it,
    # we allow defaults include-mode (rare, but consistent).
    if default_include is not None and isinstance(default_include, (list, tuple)):
      return {
        "include_fields": [str(x) for x in default_include],
      }

    # -----------------------------
    # exclude-mode (union)
    # -----------------------------
    exclude_union = []
    seen = set()

    for name in default_exclude + model_exclude:
      if name not in seen:
        seen.add(name)
        exclude_union.append(name)

    return {
      "exclude_fields": exclude_union,
    }

  def _ui_include_fields(self, view_kind: str) -> list[str] | None:
    cfg = self._get_ui_view_cfg(view_kind)
    include = cfg.get("include_fields", None)
    if include is None:
      return None
    if not isinstance(include, (list, tuple)):
      return None
    return [str(x) for x in include]

  def _ui_exclude_fields(self, view_kind: str) -> set[str]:
    """
    Exclude-mode only. In include-mode, excludes are ignored by design.
    """
    cfg = self._get_ui_view_cfg(view_kind)
    exclude = cfg.get("exclude_fields", []) or []
    if not isinstance(exclude, (list, tuple, set)):
      exclude = []
    return {str(x) for x in exclude}

  def _get_form_exclude_names(self) -> list[str]:
    """
    Django ModelForm supports 'exclude' only.
    We implement include-mode by translating include_fields -> exclude=all-minus-include.
    """
    include = self._ui_include_fields("form")

    # Gather "form-capable" field names (normal fields + m2m), skip auto_created.
    # This mirrors typical Django admin behavior and keeps your current UX.
    field_names = []
    for f in list(self.model._meta.fields) + list(self.model._meta.many_to_many):
      if getattr(f, "auto_created", False):
        continue
      field_names.append(f.name)

    if include is not None:
      include_set = set(include)
      return [name for name in field_names if name not in include_set]

    # exclude-mode: only settings-driven excludes
    exclude = set(self._ui_exclude_fields("form") or [])
    return sorted(exclude)

  def is_instance_system_managed(self, instance):
    """
    Returns True if this instance is marked as system-managed,
    i.e. instance.is_system_managed == True.
    If the field doesn't exist, returns False.
    """
    if not instance:
      return False
    return getattr(instance, "is_system_managed", False) is True


  def apply_system_managed_locking(self, form, instance):
    """
    Apply read-only rules to the form:

    - schema-level readonly fields (irrelevant in a given layer),
      configured via settings.ELEVATA_CRUD["metadata"]["system_managed"]["schema_readonly"]
    - system-managed locked fields for rows where is_system_managed == True
    - hide is_system_managed itself from the form
    """
    # 1) Always-readonly fields for this schema/model
    readonly_fields = self.get_schema_readonly_fields(instance)
    for fname in readonly_fields:
      if fname in form.fields:
        form.fields[fname].disabled = True
        form.fields[fname].widget.attrs["readonly"] = True

    # 2) System-managed locked fields (existing behavior)
    locked_fields = self.get_system_managed_locked_fields(instance)
    if self.is_instance_system_managed(instance):
      for fname in locked_fields:
        if fname in form.fields:
          form.fields[fname].disabled = True
          form.fields[fname].widget.attrs["readonly"] = True

    # 3) Hide is_system_managed flag itself
    if "is_system_managed" in form.fields:
      form.fields["is_system_managed"].widget = djforms.HiddenInput()


  def is_creation_blocked_for_model(self):
    """Evaluates if a model does not allow creation of new rows."""
    meta_cfg = getattr(settings, "ELEVATA_CRUD", {}).get("metadata", {})
    blocked = set(meta_cfg.get("no_create", []))
    return self.model.__name__ in blocked

  def enforce_system_managed_integrity(self, instance):
    """
    If instance is system-managed, restore locked fields to their original DB values
    to prevent tampering via POST.

    Many-to-many fields are skipped here, because direct assignment is not allowed
    and they are typically managed via separate relation updates.
    """
    if not self.is_instance_system_managed(instance):
      return

    locked_fields = self.get_system_managed_locked_fields(instance)

    if instance.pk:
      original = self.model.objects.get(pk=instance.pk)
      model_meta = self.model._meta

      for fname in locked_fields:
        try:
          field = model_meta.get_field(fname)
        except FieldDoesNotExist:
          # Unknown field name in configuration: ignore safely
          continue

        # Skip many-to-many fields (and optionally reverse relations)
        if isinstance(field, ManyToManyField) or field.many_to_many:
          continue

        setattr(instance, fname, getattr(original, fname))

  def get_toggle_field_names(self):
    """
    Return a set of field names that are controlled by toggle buttons
    for this model, based on settings.ELEVATA_CRUD["metadata"]["list_toggle_fields"].
    """
    cfg = getattr(settings, "ELEVATA_CRUD", {}).get("metadata", {})
    toggle_cfg_all = cfg.get("list_toggle_fields", {})

    toggle_field_names = set()

    model_toggle_cfg = toggle_cfg_all.get(self.model.__name__)
    if model_toggle_cfg:
      for entry in model_toggle_cfg:
        field_name = entry.get("field")
        if field_name:
          toggle_field_names.add(field_name)

    return toggle_field_names

  def get_list_fields(self):
    """
    Return the model fields that should appear as columns in the grid.
    Visibility rules are settings-driven via ELEVATA_CRUD["metadata"]["ui"].
    - include_fields (whitelist) OR exclude_fields (blacklist)
    - toggle fields and badge fields are always excluded here (rendered elsewhere)
    """
    meta_cfg = getattr(settings, "ELEVATA_CRUD", {}).get("metadata", {})

    # toggle fields are not shown as normal columns
    toggle_field_names = set()
    model_toggle_cfg = (meta_cfg.get("list_toggle_fields", {}) or {}).get(self.model.__name__)
    if model_toggle_cfg:
      for entry in model_toggle_cfg:
        fn = entry.get("field")
        if fn:
          toggle_field_names.add(fn)

    # badge fields are not shown as normal columns (rendered in badge column)
    badge_field_names = set()
    model_badge_cfg = (meta_cfg.get("badges", {}) or {}).get(self.model.__name__)
    if model_badge_cfg:
      for entry in model_badge_cfg:
        fn = entry.get("field")
        if fn:
          badge_field_names.add(fn)

    # Build eligible fields
    eligible = []
    for f in self.model._meta.get_fields():
      if getattr(f, "many_to_many", False):
        continue
      if getattr(f, "auto_created", False):
        continue
      if not getattr(f, "concrete", True):
        continue
      if f.name in toggle_field_names:
        continue
      if f.name in badge_field_names:
        continue
      eligible.append(f)

    include = self._ui_include_fields("list")
    if include is not None:
      by_name = {f.name: f for f in eligible}
      return [by_name[name] for name in include if name in by_name]

    exclude = set(self._ui_exclude_fields("list") or [])
    return [f for f in eligible if f.name not in exclude]

  def build_auto_filter_config(self):
    """
    Build filter definitions for:
    - visible list fields
    - toggle-only fields
    - ForeignKeys (dropdown of related model instances)
    """
    cfgs = []
    seen = set()

    toggle_names = self.get_toggle_field_names()

    # Collect relevant model fields (list + toggles)
    base_fields = list(self.get_list_fields())
    model_fields_by_name = {
      f.name: f for f in self.model._meta.get_fields()
      if getattr(f, "concrete", True)
      and not getattr(f, "auto_created", False)
      and not getattr(f, "many_to_many", False)
    }
    for name in toggle_names:
      f = model_fields_by_name.get(name)
      if f:
        base_fields.append(f)

    for f in base_fields:
      if f.name in seen:
        continue
      seen.add(f.name)

      internal_type = f.get_internal_type()

      # --- Choice-like fields ---
      if getattr(f, "choices", None):
        choices_list = [(c[0], c[1]) for c in f.choices]
        cfgs.append({
          "field_path": f.name,
          "label": f.verbose_name.title(),
          "input_type": "choice",
          "choices": choices_list,
          "lookup": "exact",
        })
        continue

      # --- ForeignKey fields ---
      if isinstance(f, models.ForeignKey):
        # Try to load all related objects (ordered by str)
        related_model = f.related_model

        try:
          qs = related_model.objects.all()

          # Use model's default ordering if defined, otherwise fallback to pk
          if related_model._meta.ordering:
            qs = qs.order_by(*related_model._meta.ordering)
          else:
            qs = qs.order_by("pk")

          choices_list = [(str(o.pk), str(o)) for o in qs]

        except Exception:
          choices_list = []

        cfgs.append({
          "field_path": f.name,
          "label": f.verbose_name.title(),
          "input_type": "foreignkey",
          "choices": choices_list,
          "lookup": "exact",
        })
        continue

      # --- Text-like fields ---
      if internal_type in ("CharField", "TextField"):
        cfgs.append({
          "field_path": f.name,
          "label": f.verbose_name.title(),
          "input_type": "text",
          "lookup": "icontains",
        })
        continue

      # --- Boolean fields ---
      if internal_type in ("BooleanField", "NullBooleanField"):
        cfgs.append({
          "field_path": f.name,
          "label": f.verbose_name.title(),
          "input_type": "boolean",
          "lookup": "exact",
        })
        continue

      # --- Numeric fields ---
      if internal_type in (
        "IntegerField", "BigIntegerField", "SmallIntegerField",
        "PositiveIntegerField", "PositiveSmallIntegerField",
        "AutoField", "BigAutoField", "DecimalField", "FloatField"
      ):
        cfgs.append({
          "field_path": f.name,
          "label": f.verbose_name.title(),
          "input_type": "number",
          "lookup": "exact",
        })
        continue

    return cfgs

  def apply_auto_filters(self, request, qs, auto_filter_cfgs):
    """
    Apply filters to queryset `qs` based on GET params from the filter form.
    Returns (qs, active_filters) so the template can re-fill inputs.
    """
    active = {}

    for fcfg in auto_filter_cfgs:
      field_path = fcfg["field_path"]       # e.g. "name", "integrate"
      lookup = fcfg["lookup"]               # e.g. "icontains", "exact"
      input_type = fcfg["input_type"]       # "text", "boolean", ...
      param_name = f"filter__{field_path}"  # GET param name

      raw_val = request.GET.get(param_name)
      if raw_val in (None, ""):
        continue

      # Store chosen value so we can show it back in the form
      active[param_name] = raw_val

      # Normalize booleans
      if input_type == "boolean":
        truthy = {"1","true","True","on","yes"}
        falsy  = {"0","false","False","off","no"}
        if raw_val in truthy:
          value = True
        elif raw_val in falsy:
          value = False
        else:
          # invalid input, skip filter
          continue
        filter_expr = {f"{field_path}__{lookup}": value}
      else:
        # numeric, text, choice
        filter_expr = {f"{field_path}__{lookup}": raw_val}

      qs = qs.filter(**filter_expr)

    return qs, active
  
  def list(self, request):
    """
    Render the list view with:
    - dynamic filter config (visible fields + toggle fields)
    - applied filters
    - final queryset
    """
    # Build filter definition (fields in the grid + toggle fields)
    auto_filter_cfgs = self.build_auto_filter_config()

    # Base queryset
    qs = self.get_queryset()

    # Apply user-submitted filters from GET
    qs, active_filters = self.apply_auto_filters(request, qs, auto_filter_cfgs)

    # Default ordering
    if self.model.__name__ == "TargetSchema":
      qs = qs.order_by("id")
    else:
      qs = qs.order_by(*self.model._meta.ordering or ["id"])

    context = {
      "model": self.model,
      "objects": qs,
      "fields": self.get_list_fields(),
      "model_name": self.model._meta.model_name,
      "model_class_name": self.model.__name__,
      "meta": self.model._meta,
      "title": self.model._meta.verbose_name_plural.title(),

      # NEW: data for the filter toolbar
      "auto_filter_cfgs": auto_filter_cfgs,
      "active_filters": active_filters,
    }
    return render(request, self.template_list, context)

  def get_form_class(self):
    """Return a model form with improved widget defaults (for all models)."""
    from django import forms
    from django.forms import widgets as w
    from django.forms import ModelForm

    exclude = self._get_form_exclude_names()

    # ------------------------------------------------------------
    # Auto-pick <ModelName>Form from metadata.forms if present.
    # IMPORTANT: we still apply exclude via modelform_factory(..., exclude=exclude)
    # so ELEVATA_CRUD visibility rules remain enforced.
    # ------------------------------------------------------------
    effective_form_class = None
    try:
      import metadata.forms as meta_forms
      candidate_name = f"{self.model.__name__}Form"
      Candidate = getattr(meta_forms, candidate_name, None)
      if Candidate and isinstance(Candidate, type) and issubclass(Candidate, ModelForm):
        effective_form_class = Candidate
    except Exception:
      effective_form_class = None

    # If a custom form_class is defined on the view, it wins over auto-pick
    if hasattr(self, "form_class") and self.form_class:
      effective_form_class = self.form_class    

    # If a custom form_class is defined on the view, we MUST still apply exclude.
    # Otherwise Meta.fields="__all__" will expose everything (lineage_key, is_system_managed, etc.).
    if effective_form_class:
      FormClass = modelform_factory(
        self.model,
        form=effective_form_class,
        exclude=exclude,
      )
    else:
      FormClass = modelform_factory(self.model, exclude=exclude)

    # Tweak widgets for all base fields
    for name, bf in FormClass.base_fields.items():
      model_field = self.model._meta.get_field(name)
      widget = bf.widget

      # ManyToMany -> nice multi-select
      if getattr(model_field, "many_to_many", False):
        existing = widget.attrs.get("class", "")
        widget.attrs["class"] = f"{existing} form-select form-select-sm".strip()
        widget.attrs.setdefault("size", "4")
        continue

      # ForeignKey / Select-like widgets -> use form-select
      if isinstance(widget, (w.Select, w.SelectMultiple)) and not getattr(model_field, "many_to_many", False):
        existing = widget.attrs.get("class", "")
        widget.attrs["class"] = f"{existing} form-select".strip()
        continue

      # Boolean -> checkbox styling
      if isinstance(widget, w.CheckboxInput):
        existing = widget.attrs.get("class", "")
        widget.attrs["class"] = f"{existing} form-check-input".strip()
        continue

      # Default: inputs as form-control
      existing = widget.attrs.get("class", "")
      widget.attrs["class"] = f"{existing} form-control".strip()

    # IMPORTANT:
    # Do not set autofocus here – focus is handled globally
    # in base.html after system-managed locking and dynamic
    # field enhancements have been applied.
    return FormClass
   
  def get_queryset(self):
    return self.model.objects.all()

  def get_success_url(self):
    if self.success_url:
      return self.success_url
    return reverse_lazy(f"{self.model._meta.model_name}_list")

  def _set_audit_fields(self, instance, user, is_new):
    """Auto-fill audit fields if present."""
    if hasattr(instance, "updated_by"):
      instance.updated_by = user
    if is_new and hasattr(instance, "created_by"):
      instance.created_by = user

  def enhance_dynamic_fields(self, form):
    """
    Turn configured CharFields into <select> dropdowns whose choices are
    dynamically populated from another model's distinct values.

    Driven by settings.ELEVATA_CRUD["metadata"]["dynamic_choices"]:

    Example config:
    "dynamic_choices": {
      "SourceDatasetGroup": {
        "target_short_name": {
          "model": "System",
          "field": "target_short_name",
          "placeholder": "— choose target short name —"
        }
      }
    }
    """

    crud_cfg = getattr(settings, "ELEVATA_CRUD", {}).get("metadata", {})
    dyn_cfg_all = crud_cfg.get("dynamic_choices", {})

    model_name = self.model.__name__
    field_cfg_map = dyn_cfg_all.get(model_name, {})

    # nothing to enhance for this model
    if not field_cfg_map:
      return form

    for field_name, cfg in field_cfg_map.items():
      # only enhance if the field actually exists on the form
      if field_name not in form.fields:
        continue

      source_model_name = cfg.get("model")
      source_field_name = cfg.get("field")
      placeholder = cfg.get("placeholder", None)

      if not source_model_name or not source_field_name:
        continue  # misconfigured, fail gracefully

      # We assume all metadata models are in app 'metadata'.
      SourceModel = apps.get_model("metadata", source_model_name)

      # Query all distinct non-empty values
      qs = (
        SourceModel.objects
        .exclude(**{f"{source_field_name}__isnull": True})
        .exclude(**{f"{source_field_name}__exact": ""})
        .values_list(source_field_name, flat=True)
        .distinct()
        .order_by(source_field_name)
      )

      values = list(qs)

      # build (value, label) tuples
      choices = [(v, v) for v in values]

      # optional placeholder at top
      if placeholder:
        choices = [("", placeholder)] + choices

      # replace widget with a Select
      form.fields[field_name].widget = djforms.Select(choices=choices)

    return form

  def enhance_domain_fields(self, form):
    """
    Domain-specific form enhancements (not generic 'dynamic choices').

    - TargetDataset.upstream_datasets filtered by target_schema
    - TargetColumn.upstream_columns filtered by TargetDataset.upstream_datasets
    """
    try:
      from metadata.models import TargetDataset, TargetSchema
    except Exception:
      return form

    model_name = self.model.__name__

    # ---- TargetDataset.upstream_datasets filtering by layer ----
    if model_name == "TargetDataset" and "upstream_datasets" in form.fields:
      upstream_field = form.fields.get("upstream_datasets")
      if upstream_field is not None:
        schema_short = None

        ts_id = (getattr(form, "data", {}) or {}).get("target_schema") or getattr(form.instance, "target_schema_id", None)
        if ts_id:
          schema_short = (
            TargetSchema.objects
              .filter(pk=ts_id)
              .values_list("short_name", flat=True)
              .first()
          )
        else:
          schema = getattr(form.instance, "target_schema", None)
          schema_short = getattr(schema, "short_name", None)

        qs = upstream_field.queryset
        if schema_short == "bizcore":
          upstream_field.queryset = (
            qs.filter(target_schema__short_name="rawcore")
              .order_by("target_dataset_name")
          )
        elif schema_short == "serving":
          upstream_field.queryset = (
            qs.filter(target_schema__short_name__in=["rawcore", "bizcore"])
              .order_by("target_dataset_name")
          )

    # ---- TargetColumn.upstream_columns filtering by selected upstream_datasets ----
    if model_name == "TargetColumn" and "upstream_columns" in form.fields:
      uc_field = form.fields.get("upstream_columns")
      if uc_field is not None:
        td = getattr(form.instance, "target_dataset", None)
        td_id = (getattr(form, "data", {}) or {}).get("target_dataset") or getattr(form.instance, "target_dataset_id", None)
        if td is None and td_id:
          td = (
            TargetDataset.objects
              .filter(pk=td_id)
              .prefetch_related("upstream_datasets")
              .first()
          )

        if td is not None:
          upstream_ds_ids = list(td.upstream_datasets.values_list("id", flat=True))
          if upstream_ds_ids:
            uc_field.queryset = (
              uc_field.queryset
                .filter(target_dataset_id__in=upstream_ds_ids)
                .order_by("target_dataset__target_dataset_name", "ordinal_position")
            )
          else:
            uc_field.queryset = uc_field.queryset.none()

    return form

  def enhance_form(self, form):
    form = self.enhance_dynamic_fields(form)
    form = self.enhance_domain_fields(form)
    return form

  def _apply_autofocus(self, form):
    """
    Set 'autofocus' on the first truly editable field in a full-page form.

    Regeln:
    - Felder, die vom Field selbst als disabled markiert sind, werden übersprungen
    - Hidden- und Checkbox-Felder werden übersprungen
    - Widgets mit readonly/disabled-Attribut werden übersprungen
    """
    from django.forms import widgets as wdg

    for name, field in form.fields.items():
      # Field ist von Django disabled
      if getattr(field, "disabled", False):
        continue

      widget = field.widget

      # Hidden oder Checkbox überspringen
      if isinstance(widget, (wdg.HiddenInput, wdg.CheckboxInput)):
        continue

      # Widgets, die explizit readonly/disabled sind, überspringen
      if widget.attrs.get("readonly") or widget.attrs.get("disabled"):
        continue

      # Dieses Feld bekommt den Autofokus
      widget.attrs["autofocus"] = True
      break

    return form
    
  # --------------------------------------------------
  # CRUD operations (standard)
  # --------------------------------------------------
  def edit(self, request, pk=None):
    obj = get_object_or_404(self.model, pk=pk) if pk else None
    FormClass = self.get_form_class()
    if request.method == "POST":
      form = FormClass(request.POST, instance=obj)
      # lock down if system-managed
      self.apply_system_managed_locking(form, obj)
      form = self.enhance_form(form)
      if form.is_valid():
        instance = form.save(commit=False)
        user = get_current_user() or request.user
        self._set_audit_fields(instance, user, pk is None)

        # prevent tampering of locked fields
        self.enforce_system_managed_integrity(instance)

        instance.save()
        messages.success(request, _("Saved successfully."))
        return redirect(self.get_success_url())
    else:
      data = request.GET if request.GET else None
      form = FormClass(data=data, instance=obj)

      self.apply_system_managed_locking(form, obj)
      form = self.enhance_form(form)

    form = self._apply_autofocus(form)

    context = {
      "form": form,
      "object": obj,
      "model": self.model,
      "title": _("Edit") if pk else _("Create"),
      "cancel_url": reverse_lazy(f"{self.model._meta.model_name}_list"),
    }
    return render(request, self.template_form, context)
  
  def delete(self, request, pk):
    obj = get_object_or_404(self.model, pk=pk)
    if request.method == "POST":
      obj.delete()
      messages.success(request, _("Deleted successfully."))
      return redirect(self.get_success_url())
    context = {
      "object": obj,
      "model": self.model,
      "title": _("Confirm Deletion"),
      "cancel_url": reverse_lazy(f"{self.model._meta.model_name}_list"),
    }
    return render(request, self.template_confirm_delete, context)

  # --------------------------------------------------
  # Inline (HTMX) operations
  # --------------------------------------------------
  def row(self, request, pk):
    obj = get_object_or_404(self.model, pk=pk)
    context = {
      "model": self.model,
      "meta": self.model._meta,
      "object": obj,
      "fields": self.get_list_fields(),
      "model_name": self.model._meta.model_name,
      "model_class_name": self.model.__name__,
    }
    return render(request, "generic/row.html", context)
   
  def row_edit(self, request, pk):
    obj = get_object_or_404(self.model, pk=pk)
    FormClass = self.get_form_class()

    if request.method == "POST":
      form = FormClass(request.POST, instance=obj)
      self.apply_system_managed_locking(form, obj)
      form = self.enhance_form(form)
      if form.is_valid():
        instance = form.save(commit=False)
        user = get_current_user() or request.user
        self._set_audit_fields(instance, user, is_new=False)

        # prevent tampering of locked fields
        self.enforce_system_managed_integrity(instance)

        instance.save()

        # Save ManyToMany relationships explicitly
        if hasattr(form, "save_m2m"):
          form.save_m2m()

        context = {
          "model": self.model,
          "meta": self.model._meta,
          "object": instance,
          "fields": self.get_list_fields(),
          "model_name": self.model._meta.model_name,
          "model_class_name": self.model.__name__,
          "highlight": True,
        }
        return render(request, "generic/row.html", context, status=200)

      # if form is NOT valid, we fall through to render the form with errors
    else:
      # Refresh GET should be unbound; keep values via initial.
      initial = {}
      if request.GET:
        for k in request.GET.keys():
          vals = request.GET.getlist(k)
          initial[k] = vals[0] if len(vals) == 1 else vals

      form = FormClass(instance=obj, initial=initial)
      self.apply_system_managed_locking(form, obj)
      form = self.enhance_form(form)

    # both GET and invalid POST end up here:
    context = {
      "model": self.model,
      "meta": self.model._meta,
      "form": form, # bound form (with errors on invalid POST)
      "object": obj,
      "fields": self.get_list_fields(), # needed for colspan calc
      "model_name": self.model._meta.model_name,
      "model_class_name": self.model.__name__,
      "is_new": False,
      "refresh_url": request.path,
    }
    return render(request, "generic/row_form.html", context, status=200)

  def row_new(self, request):
    # block creation for certain system-managed models
    if self.is_creation_blocked_for_model():
      context = {
        "fields": self.get_list_fields(),
      }
      # Render small info row instead of empty response
      return render(request, "generic/_row_form_blocked.html", context, status=200)

    FormClass = self.get_form_class()
    # Refresh GET should not be "bound" (prevents required errors).
    initial = {}
    if request.GET:
      for k in request.GET.keys():
        vals = request.GET.getlist(k)
        initial[k] = vals[0] if len(vals) == 1 else vals

    form = FormClass(initial=initial)
    self.apply_system_managed_locking(form, instance=None)
    form = self.enhance_form(form)

    context = {
      "model": self.model,
      "meta": self.model._meta,
      "form": form,
      "object": None,
      "fields": self.get_list_fields(), # for colspan
      "model_name": self.model._meta.model_name,
      "model_class_name": self.model.__name__,
      "is_new": True,
      "refresh_url": request.path,
    }
    return render(request, "generic/row_form.html", context, status=200)
      
  def row_create(self, request):
    # Allow GET for "refresh" (HTMX) so dependent selects can be re-filtered
    # without saving. POST still performs the actual create.
    if request.method == "GET":
      if self.is_creation_blocked_for_model():
        return HttpResponse(status=204)

      FormClass = self.get_form_class()
      data = request.GET if request.GET else None
      form = FormClass(data=data)

      # apply same hiding/locking so that is_system_managed doesn't get user-supplied
      self.apply_system_managed_locking(form, instance=None)
      form = self.enhance_form(form)

      context = {
        "model": self.model,
        "meta": self.model._meta,
        "form": form,
        "object": None,
        "fields": self.get_list_fields(),
        "model_name": self.model._meta.model_name,
        "model_class_name": self.model.__name__,
        "is_new": True,
      }
      return render(request, "generic/row_form.html", context, status=200)

    if request.method != "POST":
      raise Http404("GET/POST required")

    if self.is_creation_blocked_for_model():
      return HttpResponse(status=204)

    FormClass = self.get_form_class()
    form = FormClass(request.POST)

    # apply same hiding/locking so that is_system_managed doesn't get user-supplied
    self.apply_system_managed_locking(form, instance=None)
    form = self.enhance_form(form)

    if form.is_valid():
      instance = form.save(commit=False)
      user = get_current_user() or request.user
      self._set_audit_fields(instance, user, is_new=True)

      # Security: if is_system_managed came somewhere from POST (DevTools hack),
      # we set it to false again. New objects are NEVER automatically "managed".
      if hasattr(instance, "is_system_managed"):
        instance.is_system_managed = False

      # prevent tampering of locked fields
      self.enforce_system_managed_integrity(instance)

      instance.save()

      # Save ManyToMany explicitly
      if hasattr(form, "save_m2m"):
        form.save_m2m()

      context = {
        "model": self.model,
        "meta": self.model._meta,
        "object": instance,
        "fields": self.get_list_fields(),
        "model_name": self.model._meta.model_name,
        "model_class_name": self.model.__name__,
        "highlight": True,
      }
      # 201 Created is fine, HTMX will swap just like 200
      return render(request, "generic/row.html", context, status=201)

    # invalid form -> re-render the form WITH ERRORS in create mode
    context = {
      "model": self.model,
      "meta": self.model._meta,
      "form": form,
      "object": None,
      "fields": self.get_list_fields(),
      "model_name": self.model._meta.model_name,
      "model_class_name": self.model.__name__,
      "is_new": True,
    }
    return render(request, "generic/row_form.html", context, status=200)
  
  def row_delete(self, request, pk):
    obj = get_object_or_404(self.model, pk=pk)
    if request.method not in ("DELETE", "POST"):
      return HttpResponse(status=405)
    obj_id = obj.pk
    obj.delete()
    html = f'<tr id="row-{obj_id}" hx-swap-oob="delete"></tr>'
    return HttpResponse(html, status=200)

  def row_toggle(self, request, pk):
    """
    Toggle a boolean field on this row, e.g. 'integrate'.
    Expects POST with 'field' parameter.
    Returns updated <tr> HTML fragment via the existing row() template.
    """
    if request.method != "POST":
      return HttpResponse(status=405)

    field_name = request.POST.get("field")
    if not field_name:
      return HttpResponse("Missing field", status=400)

    obj = get_object_or_404(self.model, pk=pk)

    # Check that the field exists and is boolean-like
    if not hasattr(obj, field_name):
      return HttpResponse("Invalid field", status=400)

    current_val = getattr(obj, field_name)
    if not isinstance(current_val, bool):
      return HttpResponse("Field not toggleable", status=400)

    # Flip it
    setattr(obj, field_name, not current_val)

    # audit fields
    user = get_current_user() or request.user
    self._set_audit_fields(obj, user, is_new=False)

    obj.save()

    # Re-render just the row so HTMX can swap it
    context = {
      "model": self.model,
      "meta": self.model._meta,
      "object": obj,
      "fields": self.get_list_fields(),
      "model_name": self.model._meta.model_name,
      "model_class_name": self.model.__name__,
    }
    return render(request, "generic/row.html", context, status=200)

  # ------------------------------------------------------------
  # Read-only detail view
  # ------------------------------------------------------------
  def get_related_objects(self, instance):
    """Collect forward/reverse relations for display."""
    related = []
    seen_labels = set()

    for f in instance._meta.get_fields():
      if f.many_to_many and not f.auto_created:
        label = getattr(f, "verbose_name", f.name).title()
        if label in seen_labels:
          continue
        seen_labels.add(label)
        qs = getattr(instance, f.name).all()
        related.append((label, qs))

      elif f.one_to_many and f.auto_created:
        accessor = f.get_accessor_name()
        label = f.related_model._meta.verbose_name_plural.title()
        if label in seen_labels:
          continue
        seen_labels.add(label)
        qs = getattr(instance, accessor).all()
        related.append((label, qs))

      elif f.many_to_many and f.auto_created:
        accessor = f.get_accessor_name()
        label = f.related_model._meta.verbose_name_plural.title()
        if label in seen_labels:
          continue
        seen_labels.add(label)
        qs = getattr(instance, accessor).all()
        related.append((label, qs))

    return related

  def detail(self, request, pk):
    """Display a read-only detail view for one record."""
    obj = get_object_or_404(self.model, pk=pk)
    excluded = {"id", "created_at", "created_by", "updated_at", "updated_by", "lineage_key", "former_names"}

    # build cleaned field/value pairs for display
    clean_rows = []
    for f in self.model._meta.fields:
      if f.name not in excluded:
        raw_value = getattr(obj, f.name, "")
        display_value = "" if raw_value is None else raw_value
        clean_rows.append((f, display_value))

    context = {
      "object": obj,
      "model": self.model,
      "model_name": self.model._meta.model_name,
      "title": f"{self.model._meta.verbose_name.title()} Details",
      "fields": [f for f in self.model._meta.fields if f.name not in excluded],
      "rows": clean_rows, 
      "many_to_many": [f for f in self.model._meta.many_to_many if f.name not in excluded],
      "related_objects": self.get_related_objects(obj),
    }
    return render(request, "generic/detail.html", context)
  