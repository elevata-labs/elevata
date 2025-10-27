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

from django.shortcuts import render, get_object_or_404, redirect
from django.views import View
from django.urls import reverse_lazy
from django.contrib import messages
from django.utils.translation import gettext_lazy as _
from django.forms import modelform_factory
from django.http import Http404, HttpResponse, HttpResponseNotFound
from django.template.loader import render_to_string
from crum import get_current_user
from django.contrib.auth.mixins import LoginRequiredMixin
from django.conf import settings
from django.db import models

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

  list_exclude = {"id", "created_at", "created_by", "updated_at", "updated_by"}
  form_exclude = {"id", "created_at", "created_by", "updated_at", "updated_by"}

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

    Rules:
    - skip auto_created, m2m, non-concrete
    - skip anything in self.list_exclude
    - skip toggle fields (they render as buttons in the Actions column)
    - skip badge fields (they render as badges in the Actions column)
    """

    meta_cfg = getattr(settings, "ELEVATA_CRUD", {}).get("metadata", {})

    # 1. collect toggle fields for this model
    toggle_cfg_all = meta_cfg.get("list_toggle_fields", {})
    toggle_field_names = set()
    model_toggle_cfg = toggle_cfg_all.get(self.model.__name__)
    if model_toggle_cfg:
      for entry in model_toggle_cfg:
        field_name = entry.get("field")
        if field_name:
          toggle_field_names.add(field_name)

    # 2. collect badge fields for this model
    badge_cfg_all = meta_cfg.get("badges", {})
    badge_field_names = set()
    model_badge_cfg = badge_cfg_all.get(self.model.__name__)
    if model_badge_cfg:
      # new structure: list of dicts, each has "field": "<fieldname>"
      for entry in model_badge_cfg:
        field_name = entry.get("field")
        if field_name:
          badge_field_names.add(field_name)

    fields = []
    for f in self.model._meta.get_fields():
      # skip django internals
      if getattr(f, "many_to_many", False):
        continue
      if getattr(f, "auto_created", False):
        continue
      if not getattr(f, "concrete", True):
        continue

      # explicit per-view excludes, if you use that
      if f.name in getattr(self, "list_exclude", []):
        continue

      # skip toggle fields (we render buttons instead)
      if f.name in toggle_field_names:
        continue

      # skip badge fields (we render badges instead)
      if f.name in badge_field_names:
        continue

      fields.append(f)

    return fields
  
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
          choices_list = [(str(o.pk), str(o)) for o in related_model.objects.all().order_by("pk")]
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

    # Default ordering (keep what you had)
    qs = qs.order_by("id")

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

    # If a custom form_class is defined on the view, use it
    if hasattr(self, "form_class") and self.form_class:
      return self.form_class

    FormClass = modelform_factory(self.model, exclude=list(self.form_exclude))

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

    # Set autofocus on first usable field
    for name, bf in FormClass.base_fields.items():
      widget = bf.widget
      # skip checkboxes/hidden fields
      if isinstance(widget, (w.CheckboxInput, w.HiddenInput)):
        continue
      widget.attrs.setdefault("autofocus", True)
      break

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

  # --------------------------------------------------
  # CRUD operations (standard)
  # --------------------------------------------------
  def edit(self, request, pk=None):
    obj = get_object_or_404(self.model, pk=pk) if pk else None
    FormClass = self.get_form_class()
    if request.method == "POST":
      form = FormClass(request.POST, instance=obj)
      if form.is_valid():
        instance = form.save(commit=False)
        user = get_current_user() or request.user
        self._set_audit_fields(instance, user, pk is None)
        instance.save()
        messages.success(request, _("Saved successfully."))
        return redirect(self.get_success_url())
    else:
      form = FormClass(instance=obj)
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
      if form.is_valid():
        instance = form.save(commit=False)
        user = get_current_user() or request.user
        self._set_audit_fields(instance, user, is_new=False)
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
      form = FormClass(instance=obj)

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
    }
    return render(request, "generic/row_form.html", context, status=200)

  def row_new(self, request):
    FormClass = self.get_form_class()
    form = FormClass()

    context = {
      "model": self.model,
      "meta": self.model._meta,
      "form": form,
      "object": None,
      "fields": self.get_list_fields(), # for colspan
      "model_name": self.model._meta.model_name,
      "model_class_name": self.model.__name__,
      "is_new": True,
    }
    return render(request, "generic/row_form.html", context, status=200)
  
  def row_create(self, request):
    if request.method != "POST":
      raise Http404("POST required")

    FormClass = self.get_form_class()
    form = FormClass(request.POST)

    if form.is_valid():
      instance = form.save(commit=False)
      user = get_current_user() or request.user
      self._set_audit_fields(instance, user, is_new=True)
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
    for f in instance._meta.get_fields():
      if f.many_to_many and not f.auto_created:
        label = getattr(f, "verbose_name", f.name).title()
        qs = getattr(instance, f.name).all()
        related.append((label, qs))
      elif f.one_to_many and f.auto_created:
        accessor = f.get_accessor_name()
        label = f.related_model._meta.verbose_name_plural.title()
        qs = getattr(instance, accessor).all()
        related.append((label, qs))
      elif f.many_to_many and f.auto_created:
        accessor = f.get_accessor_name()
        label = f.related_model._meta.verbose_name_plural.title()
        qs = getattr(instance, accessor).all()
        related.append((label, qs))
    return related

  def detail(self, request, pk):
    """Display a read-only detail view for one record."""
    obj = get_object_or_404(self.model, pk=pk)
    excluded = {"id", "created_at", "created_by", "updated_at", "updated_by"}
    context = {
      "object": obj,
      "model": self.model,
      "model_name": self.model._meta.model_name,
      "title": f"{self.model._meta.verbose_name.title()} Details",
      "fields": [f for f in self.model._meta.fields if f.name not in excluded],
      "many_to_many": [f for f in self.model._meta.many_to_many if f.name not in excluded],
      "related_objects": self.get_related_objects(obj),
    }
    return render(request, "generic/detail.html", context)
