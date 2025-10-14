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


# ------------------------------------------------------------
# Utility helper
# ------------------------------------------------------------
def display_key(*parts):
  """Join parts with dot, skipping None or empty strings."""
  return ".".join(p for p in parts if p)


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

    # Handle DELETE (for HTMX)
    if request.method == "DELETE" and self.action == "row_delete":
      return self.row_delete(request, pk)

    return HttpResponseNotFound(f"Invalid action '{self.action}' for {self.__class__.__name__}")

  # --------------------------------------------------
  # Utility helpers
  # --------------------------------------------------
  def get_list_fields(self):
    """Return model fields for the grid (excluding PK and audit)."""
    fields = []
    for f in self.model._meta.get_fields():
      if getattr(f, "many_to_many", False):
        continue
      if getattr(f, "auto_created", False):
        continue
      if not getattr(f, "concrete", True):
        continue
      if f.name in self.list_exclude:
        continue
      fields.append(f)
    return fields

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
  def list(self, request):
    objects = self.get_queryset().order_by("id")
    context = {
      "model": self.model,
      "objects": objects,
      "fields": self.get_list_fields(),
      "model_name": self.model._meta.model_name,
      "meta": self.model._meta,
      "title": self.model._meta.verbose_name_plural.title(),
    }
    return render(request, self.template_list, context)

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
        }
        return render(request, "generic/row.html", context, status=200)
    else:
      form = FormClass(instance=obj)
    context = {
      "model": self.model,
      "meta": self.model._meta,
      "form": form,
      "object": obj,
      "model_name": self.model._meta.model_name,
    }
    return render(request, "generic/row_form.html", context)

  def row_new(self, request):
    FormClass = self.get_form_class()
    form = FormClass()
    context = {
      "model": self.model,
      "meta": self.model._meta,
      "form": form,
      "object": None,
      "model_name": self.model._meta.model_name,
    }
    return render(request, "generic/row_form_new.html", context)

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
      context = {
        "model": self.model,
        "meta": self.model._meta,
        "object": instance,
        "fields": self.get_list_fields(),
        "model_name": self.model._meta.model_name,
        "highlight": True,
      }
      return render(request, "generic/row.html", context, status=201)
    context = {
      "model": self.model,
      "meta": self.model._meta,
      "form": form,
      "object": None,
      "model_name": self.model._meta.model_name,
    }
    return render(request, "generic/row_form_new.html", context, status=400)

  def row_delete(self, request, pk):
    obj = get_object_or_404(self.model, pk=pk)
    if request.method not in ("DELETE", "POST"):
      return HttpResponse(status=405)
    obj_id = obj.pk
    obj.delete()
    html = f'<tr id="row-{obj_id}" hx-swap-oob="delete"></tr>'
    return HttpResponse(html, status=200)

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
