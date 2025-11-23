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
from django.http import HttpResponse, HttpResponseNotFound, Http404
from django.shortcuts import render
from django.urls import reverse_lazy
from generic import GenericCRUDView
from metadata.forms import TargetColumnForm
from metadata.models import (
  # Target-side
  TargetDataset,
  TargetDatasetInput,
  TargetColumn,
  TargetDatasetOwnership,
  TargetDatasetReference,
  TargetColumnInput,
  TargetDatasetReferenceComponent,
  # Source-side
  System,
  SourceDataset,
  SourceColumn,
  SourceDatasetGroup,
  SourceDatasetGroupMembership,
  SourceDatasetOwnership,
  SourceDatasetIncrementPolicy,
)


class _ScopedChildView(GenericCRUDView):
  """
  Base class for all 'child-of-parent' CRUD views.

  Key conventions:
  - parent_pk comes from URL (<int:parent_pk>)
  - pk (if present) refers to the child record
  - route_name must point to the list-view route name, e.g. 'sourcedatasetgroupmembership_list'
  - parent_model defines the parent model class (e.g. SourceDatasetGroup)
  """

  template_list = "generic/list.html"
  template_form = "generic/form.html"
  template_confirm_delete = "generic/confirm_delete.html"

  route_name = None
  parent_model = None

  def get_parent_pk(self):
    """Return the parent object's primary key from URL kwargs."""
    return self.kwargs.get("parent_pk")

  def get_success_url(self):
    """After save/delete, redirect back to the scoped list of the same parent."""
    return reverse_lazy(self.route_name, kwargs={"parent_pk": self.get_parent_pk()})

  def get_parent_object(self):
    """Load and return the parent model instance, if defined."""
    if self.parent_model:
      return self.parent_model.objects.get(pk=self.get_parent_pk())
    return None

  def get_context_base(self, request):
    """
    Build the same base context as GenericCRUDView.list(),
    but include parent_pk and parent for scoped templates.
    """
    auto_filter_cfgs = self.build_auto_filter_config()
    qs = self.get_queryset()
    qs, active_filters = self.apply_auto_filters(request, qs, auto_filter_cfgs)
    qs = qs.order_by("id")

    ctx = {
      "model": self.model,
      "objects": qs,
      "fields": self.get_list_fields(),
      "model_name": self.model._meta.model_name,
      "model_class_name": self.model.__name__,
      "meta": self.model._meta,
      "title": self.model._meta.verbose_name_plural.title(),
      "auto_filter_cfgs": auto_filter_cfgs,
      "active_filters": active_filters,
    }

    # Scoped additions
    ctx["parent_pk"] = self.get_parent_pk()
    parent_obj = self.get_parent_object()
    if parent_obj is not None:
      ctx["parent"] = parent_obj

    # expose detail_route_name so row.html can build a scoped detail_url
    detail_route_name = getattr(self, "detail_route_name", None)
    if detail_route_name:
      ctx["detail_route_name"] = detail_route_name
      
    return ctx
  
  
  def _remove_parent_fk_from_form(self, form):
    """
    For scoped views: remove the FK field that points to the parent model
    from the visible form fields so the user can't re-parent or choose
    the wrong parent when creating.
    """
    if not self.parent_model:
      return form

    # find FK field(s) that point to parent_model
    for f in self.model._meta.fields:
      if getattr(f, "is_relation", False) and getattr(f, "remote_field", None):
        if f.remote_field.model is self.parent_model:
          parent_fk_name = f.name
          # If that field is in the form, drop it from user input
          if parent_fk_name in form.fields:
            form.fields.pop(parent_fk_name, None)
          break

    return form

  def _apply_autofocus(self, form):
    # pick first usable field after all locking/removals
    for name, field in form.fields.items():
      if getattr(field, "disabled", False):
        continue
      w = field.widget
      # skip hidden/checkbox
      from django.forms import widgets as wdg
      if isinstance(w, (wdg.HiddenInput, wdg.CheckboxInput)):
        continue
      if w.attrs.get("readonly") or w.attrs.get("disabled"):
        continue
      w.attrs["autofocus"] = True
      break
    return form

  def list(self, request):
    """
    Override GenericCRUDView.list() to inject parent_pk/parent
    into the context so the Add button in list.html can build
    the correct scoped 'row-new' URL.
    """
    context = self.get_context_base(request)
    return render(request, self.template_list, context)

  def row_new(self, request):
    """
    Override row_new() to ensure parent_pk/parent are included
    when rendering the inline form (row_form.html). This is critical
    for child tables that must maintain a parent reference.
    """
    if self.is_creation_blocked_for_model():
      blocked_ctx = {
        "fields": self.get_list_fields(),
        "parent_pk": self.get_parent_pk(),
      }
      parent_obj = self.get_parent_object()
      if parent_obj is not None:
        blocked_ctx["parent"] = parent_obj
      return render(request, "generic/_row_form_blocked.html", blocked_ctx, status=200)

    FormClass = self.get_form_class()
    form = FormClass()

    # Apply system-managed field locking
    self.apply_system_managed_locking(form, instance=None)
    form = self.enhance_dynamic_fields(form)

    # Hide/remove parent FK field from the form
    form = self._remove_parent_fk_from_form(form)

    ctx = {
      "model": self.model,
      "meta": self.model._meta,
      "form": form,
      "object": None,
      "fields": self.get_list_fields(),
      "model_name": self.model._meta.model_name,
      "model_class_name": self.model.__name__,
      "is_new": True,
      "parent_pk": self.get_parent_pk(),
    }
    parent_obj = self.get_parent_object()
    if parent_obj is not None:
      ctx["parent"] = parent_obj

    form = self._apply_autofocus(form)

    return render(request, "generic/row_form.html", ctx, status=200)

  def row_create(self, request):
    """
    Handle POST from inline 'Add new row' form in a scoped list.
    Returns a single <tr> (row.html) on success so HTMX can insert it.
    We override GenericCRUDView.row_create() only to:
      - bind the new row to its parent_pk
      - set audit fields
      - return row.html with all required context keys
    """
    if request.method != "POST":
      return HttpResponseNotFound("POST required")

    if self.is_creation_blocked_for_model():
      return HttpResponse(status=204)

    FormClass = self.get_form_class()
    form = FormClass(request.POST)

    # lock system-managed fields before validation
    self.apply_system_managed_locking(form, instance=None)
    form = self.enhance_dynamic_fields(form)

    # Hide/remove parent FK field from the form
    form = self._remove_parent_fk_from_form(form)

    if form.is_valid():
      instance = form.save(commit=False)

      # audit fields (created_by / updated_by etc.)
      user = getattr(request, "user", None)
      self._set_audit_fields(instance, user, is_new=True)

      # safety: never allow user to create rows marked as system-managed
      if hasattr(instance, "is_system_managed"):
        instance.is_system_managed = False

      # enforce integrity of locked/system managed fields
      self.enforce_system_managed_integrity(instance)

      # make sure this row is attached to the correct parent object
      parent_obj = self.get_parent_object()
      if parent_obj and self.parent_model:
        for f in self.model._meta.fields:
          if getattr(f, "is_relation", False) and getattr(f, "remote_field", None):
            if f.remote_field.model is self.parent_model:
              setattr(instance, f.name, parent_obj)
              break

      instance.save()

      # return a fully-populated row context so row.html won't crash
      ctx = {
        "object": instance,
        "fields": self.get_list_fields(),
        "model_name": self.model._meta.model_name,
        "model_class_name": self.model.__name__,
        "meta": self.model._meta,
        "parent_pk": self.get_parent_pk(),
      }
      return render(request, "generic/row.html", ctx, status=200)

    # form invalid -> show row_form again with errors, keep inline editor open
    ctx = {
      "model": self.model,
      "meta": self.model._meta,
      "form": form,
      "object": None,  # new record that failed validation
      "fields": self.get_list_fields(),
      "model_name": self.model._meta.model_name,
      "model_class_name": self.model.__name__,
      "is_new": True,
      "parent_pk": self.get_parent_pk(),
    }
    parent_obj = self.get_parent_object()
    if parent_obj is not None:
      ctx["parent"] = parent_obj

    return render(request, "generic/row_form.html", ctx, status=400)

  def row_edit(self, request, pk):
    """
    Scoped inline edit for an existing row.
    - GET  -> return row_form.html so the row becomes editable inline
    - POST -> validate & save, then return row.html so that the row collapses back
    We override GenericCRUDView.row_edit() for:
      - parent_pk awareness
      - audit fields
      - returning full row.html context
    """
    try:
      instance = self.model.objects.get(pk=pk)
    except self.model.DoesNotExist:
      return HttpResponseNotFound("Row not found")

    FormClass = self.get_form_class()

    if request.method == "GET":
      # render inline edit form
      form = FormClass(instance=instance)

      self.apply_system_managed_locking(form, instance=instance)
      form = self.enhance_dynamic_fields(form)

      # Hide/remove parent FK field from the form
      form = self._remove_parent_fk_from_form(form)

      ctx = {
        "model": self.model,
        "meta": self.model._meta,
        "form": form,
        "object": instance,
        "fields": self.get_list_fields(),
        "model_name": self.model._meta.model_name,
        "model_class_name": self.model.__name__,
        "is_new": False,                # editing existing row
        "parent_pk": self.get_parent_pk(),  # critical so row_form.html generates scoped hx-post
      }
      parent_obj = self.get_parent_object()
      if parent_obj is not None:
        ctx["parent"] = parent_obj

      form = self._apply_autofocus(form)

      return render(request, "generic/row_form.html", ctx, status=200)

    # POST -> save update
    if request.method == "POST":
      form = FormClass(request.POST, instance=instance)

      self.apply_system_managed_locking(form, instance=instance)
      form = self.enhance_dynamic_fields(form)

      # Hide/remove parent FK field from the form
      form = self._remove_parent_fk_from_form(form)

      if form.is_valid():
        updated = form.save(commit=False)

        # audit on update
        user = getattr(request, "user", None)
        self._set_audit_fields(updated, user, is_new=False)

        # keep readonly/system-managed guarantees
        self.enforce_system_managed_integrity(updated)

        # keep row assigned to the same parent (don't allow re-parenting)
        parent_obj = self.get_parent_object()
        if parent_obj and self.parent_model:
          for f in self.model._meta.fields:
            if getattr(f, "is_relation", False) and getattr(f, "remote_field", None):
              if f.remote_field.model is self.parent_model:
                setattr(updated, f.name, parent_obj)
                break

        updated.save()

        # return the final row view (static mode again) so HTMX swaps it back
        ctx = {
          "object": updated,
          "fields": self.get_list_fields(),
          "model_name": self.model._meta.model_name,
          "model_class_name": self.model.__name__,
          "meta": self.model._meta,
          "parent_pk": self.get_parent_pk(),
        }
        return render(request, "generic/row.html", ctx, status=200)

      # invalid -> send edit form with errors
      ctx = {
        "model": self.model,
        "meta": self.model._meta,
        "form": form,
        "object": instance,
        "fields": self.get_list_fields(),
        "model_name": self.model._meta.model_name,
        "model_class_name": self.model.__name__,
        "is_new": False,
        "parent_pk": self.get_parent_pk(),
      }
      parent_obj = self.get_parent_object()
      if parent_obj is not None:
        ctx["parent"] = parent_obj

      return render(request, "generic/row_form.html", ctx, status=400)

    return HttpResponseNotFound("Unsupported method for row_edit")

  def row_toggle(self, request, pk):
    """
    Inline toggle of a boolean field (e.g. 'integrate') in a scoped list.
    Expects POST with {"field": "<fieldname>"}.
    Returns the updated <tr> (row.html).
    """
    if request.method != "POST":
      return HttpResponseNotFound("POST required")

    field_name = request.POST.get("field")
    if not field_name:
      return HttpResponseNotFound("Missing field")

    try:
      instance = self.model.objects.get(pk=pk)
    except self.model.DoesNotExist:
      return HttpResponseNotFound("Row not found")

    # Flip the boolean field
    current_val = getattr(instance, field_name, None)
    # We only allow toggling real booleans
    if not isinstance(current_val, bool):
      return HttpResponseNotFound("Not a toggleable boolean field")

    new_val = not current_val
    setattr(instance, field_name, new_val)

    # Audit (updated_by, updated_at, etc.)
    user = getattr(request, "user", None)
    self._set_audit_fields(instance, user, is_new=False)

    # Protect system-managed fields (don't allow illegal changes)
    self.enforce_system_managed_integrity(instance)

    # Still enforce correct parent (no re-parenting via toggle)
    parent_obj = self.get_parent_object()
    if parent_obj and self.parent_model:
      for f in self.model._meta.fields:
        if getattr(f, "is_relation", False) and getattr(f, "remote_field", None):
          if f.remote_field.model is self.parent_model:
            setattr(instance, f.name, parent_obj)
            break

    instance.save()

    # Return updated row so HTMX can swap it back in-place
    ctx = {
      "object": instance,
      "fields": self.get_list_fields(),
      "model_name": self.model._meta.model_name,
      "model_class_name": self.model.__name__,
      "meta": self.model._meta,
      "parent_pk": self.get_parent_pk(),
    }
    return render(request, "generic/row.html", ctx, status=200)
  
  def row(self, request, pk):
    """
    Return a single <tr> (row.html) for this object in a scoped list.
    This is used e.g. by 'Cancel' in inline edit, to restore the read-only row
    without saving changes.
    """
    try:
      instance = self.model.objects.get(pk=pk)
    except self.model.DoesNotExist:
      return HttpResponseNotFound("Row not found")

    ctx = {
      "object": instance,
      "fields": self.get_list_fields(),
      "model_name": self.model._meta.model_name,
      "model_class_name": self.model.__name__,
      "meta": self.model._meta,
      "parent_pk": self.get_parent_pk(),
    }

    # pass detail_route_name into row context, if defined
    detail_route_name = getattr(self, "detail_route_name", None)
    if detail_route_name:
      ctx["detail_route_name"] = detail_route_name

    return render(request, "generic/row.html", ctx, status=200)

# ---------------- Target side ----------------

class TargetDatasetInputScopedView(_ScopedChildView):
  model = TargetDatasetInput
  parent_model = TargetDataset
  route_name = "targetdatasetinput_list"

  def get_queryset(self):
    return (
      self.model.objects
      .filter(target_dataset_id=self.get_parent_pk())
      .select_related("source_dataset", "target_dataset")
      .order_by(
        "role",
        "source_dataset__source_system__short_name",
        "source_dataset__source_dataset_name",
      )
    )

  def get_context_data(self, **kwargs):
    ctx = super().get_context_data(**kwargs)
    ctx["dataset"] = self.get_parent_object()
    return ctx


class TargetDatasetColumnScopedView(_ScopedChildView):
  model = TargetColumn
  parent_model = TargetDataset
  route_name = "targetdatasetcolumn_list"
  form_class = TargetColumnForm

  def get_queryset(self):
    return (
      self.model.objects
      .filter(target_dataset_id=self.get_parent_pk())
      .order_by("ordinal_position")
    )

  def get_context_data(self, **kwargs):
    ctx = super().get_context_data(**kwargs)
    ctx["dataset"] = self.get_parent_object()
    return ctx
  

class TargetDatasetReferenceScopedView(_ScopedChildView):
  model = TargetDatasetReference
  parent_model = TargetDataset
  route_name = "targetdatasetreference_list"
  detail_route_name = "targetdatasetreference_detail_scoped"

  def get_queryset(self):
    return (
      self.model.objects
      .filter(referencing_dataset_id=self.get_parent_pk())
      .select_related("referenced_dataset")
      .order_by("referenced_dataset__target_dataset_name")
    )

  def enhance_dynamic_fields(self, form):
    """
    Restrict referencing_dataset / referenced_dataset dropdowns
    to RawCore target datasets.
    """
    form = super().enhance_dynamic_fields(form)

    rawcore_qs = TargetDataset.objects.filter(
      target_schema__short_name="rawcore"
    ).order_by("target_dataset_name")

    ref_child = form.fields.get("referencing_dataset")
    if ref_child is not None:
      ref_child.queryset = rawcore_qs

    ref_parent = form.fields.get("referenced_dataset")
    if ref_parent is not None:
      ref_parent.queryset = rawcore_qs

    return form

  def get_context_data(self, **kwargs):
    ctx = super().get_context_data(**kwargs)
    ctx["dataset"] = self.get_parent_object()
    return ctx
    

class TargetDatasetReferenceComponentScopedView(_ScopedChildView):
  model = TargetDatasetReferenceComponent
  parent_model = TargetDatasetReference
  route_name = "targetdatasetreferencecomponent_list"

  def get_queryset(self):
    return (
      self.model.objects
      .filter(reference_id=self.get_parent_pk())
      .select_related("reference", "from_column", "to_column")
      .order_by("ordinal_position")
    )

  def get_context_data(self, **kwargs):
    ctx = super().get_context_data(**kwargs)
    ctx["reference"] = self.get_parent_object()
    return ctx

  def enhance_dynamic_fields(self, form):
    """
    Restrict dropdowns for TargetDatasetReferenceComponent:

    - from_column: only columns of the referencing (child) dataset
    - to_column: only business key columns of the referenced (parent) dataset
    """
    form = super().enhance_dynamic_fields(form)

    parent = self.get_parent_object()
    if not parent:
      return form

    # Child: referencing_dataset
    from_field = form.fields.get("from_column")
    if from_field is not None:
      from_field.queryset = (
        from_field.queryset
        .filter(target_dataset=parent.referencing_dataset)
        .order_by("target_dataset__target_dataset_name", "ordinal_position")
      )

    # Parent: referenced_dataset → only business key columns
    to_field = form.fields.get("to_column")
    if to_field is not None:
      to_field.queryset = (
        to_field.queryset
        .filter(
          target_dataset=parent.referenced_dataset,
          business_key_column=True,
        )
        .order_by("target_dataset__target_dataset_name", "ordinal_position")
      )

    return form


class TargetColumnInputScopedView(_ScopedChildView):
  model = TargetColumnInput
  parent_model = TargetColumn
  route_name = "targetcolumninput_list"

  def get_queryset(self):
    return (
      self.model.objects
      .filter(target_column_id=self.get_parent_pk())
      .select_related("source_column", "target_column")
      .order_by("ordinal_position")
    )

  def get_context_data(self, **kwargs):
    ctx = super().get_context_data(**kwargs)
    ctx["column"] = self.get_parent_object()
    return ctx


# ---------------- Source side ----------------

class SourceSystemDatasetScopedView(_ScopedChildView):
  model = SourceDataset
  parent_model = System
  route_name = "sourcesystemdataset_list"

  def get_parent_object(self):
    parent = super().get_parent_object()
    if parent and not parent.is_source:
      # If someone tries to access datasets on a non-source system, show 404
      raise Http404("This system is not marked as a source system.")
    return parent

  def get_queryset(self):
    return (
      self.model.objects
      .filter(source_system_id=self.get_parent_pk())
      .order_by("source_dataset_name")
    )

  def get_context_data(self, **kwargs):
    ctx = super().get_context_data(**kwargs)
    ctx["system"] = self.get_parent_object()
    return ctx
  

class SourceDatasetColumnScopedView(_ScopedChildView):
  model = SourceColumn
  parent_model = SourceDataset
  route_name = "sourcedatasetcolumn_list"

  def get_queryset(self):
    return (
      self.model.objects
      .filter(source_dataset_id=self.get_parent_pk())
      .order_by("ordinal_position")
    )

  def get_context_data(self, **kwargs):
    ctx = super().get_context_data(**kwargs)
    ctx["dataset"] = self.get_parent_object()
    return ctx


class SourceDatasetGroupMembershipScopedView(_ScopedChildView):
  model = SourceDatasetGroupMembership
  parent_model = SourceDatasetGroup
  route_name = "sourcedatasetgroupmembership_list"

  def get_queryset(self):
    return (
      self.model.objects
      .filter(group_id=self.get_parent_pk())
      .select_related("source_dataset")
      .order_by(
        "-is_primary_system",
        "source_dataset__source_dataset_name",
      )
    )

  def get_context_data(self, **kwargs):
    ctx = super().get_context_data(**kwargs)
    ctx["group"] = self.get_parent_object()
    return ctx

class SourceDatasetOwnershipScopedView(_ScopedChildView):
  model = SourceDatasetOwnership
  parent_model = SourceDataset
  route_name = "sourcedatasetownership_list"

  def get_queryset(self):
    return (
      self.model.objects
      .filter(source_dataset_id=self.get_parent_pk())
      .select_related("source_dataset", "person")
      .order_by("-is_primary_owner", "person__name")
    )

  def get_context_data(self, **kwargs):
    ctx = super().get_context_base(self.request) if hasattr(self, "request") else {}
    ctx["dataset"] = self.get_parent_object()
    return ctx

class TargetDatasetOwnershipScopedView(_ScopedChildView):
  model = TargetDatasetOwnership
  parent_model = TargetDataset
  route_name = "targetdatasetownership_list"

  def get_queryset(self):
    return (
      self.model.objects
      .filter(target_dataset_id=self.get_parent_pk())
      .select_related("target_dataset", "person")
      .order_by("-is_primary_owner", "person__name")
    )

  def get_context_data(self, **kwargs):
    ctx = super().get_context_base(self.request) if hasattr(self, "request") else {}
    ctx["dataset"] = self.get_parent_object()
    return ctx
  

class SourceDatasetIncrementPolicyScopedView(_ScopedChildView):
  """
  Scoped CRUD for SourceDatasetIncrementPolicy, filtered by parent SourceDataset.
  """
  model = SourceDatasetIncrementPolicy
  parent_model = SourceDataset
  route_name = "sourcedatasetincrementpolicy_list"

  def get_queryset(self):
    return (
      self.model.objects
      .filter(source_dataset_id=self.get_parent_pk())
      .select_related("source_dataset")
      .order_by("environment")
    )

  def get_context_data(self, **kwargs):
    ctx = super().get_context_data(**kwargs)
    ctx["dataset"] = self.get_parent_object()
    ctx["title"] = f"Increment policies for source dataset {ctx['dataset']}"
    return ctx
