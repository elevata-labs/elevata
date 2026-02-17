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
from django.contrib import messages
from django.db import transaction
from django.db.models import Count
from django.forms import widgets as wdg
from django.http import HttpResponse, HttpResponseNotFound, Http404
from django.shortcuts import render, get_object_or_404, redirect
from django.urls import reverse, reverse_lazy
from django.utils.html import escape
from django.utils.translation import gettext_lazy as _
from crum import get_current_user
from generic import GenericCRUDView, htmx_oob_warning
from metadata.generation.policies import (
  query_tree_allowed_for_dataset,
  query_tree_mutations_allowed_for_dataset,
  query_tree_mutation_block_reason,
)
from metadata.generation.query_contract import infer_query_node_contract
from metadata.services.query_contract_sync_trigger import trigger_query_contract_column_sync
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
  TargetDatasetJoin, 
  TargetDatasetJoinPredicate,
  # Query-side (bizcore/serving)
  QueryNode,
  QuerySelectNode,
  QueryAggregateNode,
  QueryAggregateGroupKey,
  QueryAggregateMeasure,
  OrderByExpression,
  OrderByItem,
  QueryUnionNode,
  QueryUnionOutputColumn,
  QueryUnionBranch,
  QueryUnionBranchMapping,
  QueryWindowNode,
  QueryWindowColumn,
  QueryWindowColumnArg,
  PartitionByExpression,
  PartitionByItem,
  # Source-side
  System,
  SourceDataset,
  SourceColumn,
  SourceDatasetGroup,
  SourceDatasetGroupMembership,
  SourceDatasetOwnership,
  SourceDatasetIncrementPolicy,
)


QUERY_SCOPED_MODEL_NAMES = {
  "QueryNode",
  "QuerySelectNode",
  "QueryAggregateNode",
  "QueryAggregateGroupKey",
  "QueryAggregateMeasure",
  "OrderByExpression",
  "OrderByItem",
  "QueryUnionNode",
  "QueryUnionOutputColumn",
  "QueryUnionBranch",
  "QueryUnionBranchMapping",
  "QueryWindowNode",
  "QueryWindowColumn",
  "QueryWindowColumnArg",
  "PartitionByExpression",
  "PartitionByItem",
}


def _is_query_scoped_model(model) -> bool:
  try:
    return model.__name__ in QUERY_SCOPED_MODEL_NAMES
  except Exception:
    return False


def _is_htmx(request) -> bool:
  try:
    return (request.headers.get("HX-Request") or "").lower() == "true"
  except Exception:
    return False


def _get_query_builder_dataset(parent_obj):
  """
  Return the owning TargetDataset for query-scoped objects so scoped views can
  reliably link back to the Query Builder (instead of an unrelated detail page).
  """
  if parent_obj is None:
    return None

  if isinstance(parent_obj, TargetDataset):
    return parent_obj

  if isinstance(parent_obj, QueryNode):
    return getattr(parent_obj, "target_dataset", None)

  if isinstance(parent_obj, (QuerySelectNode, QueryAggregateNode, QueryUnionNode, QueryWindowNode)):
    node = getattr(parent_obj, "node", None)
    return getattr(node, "target_dataset", None) if node is not None else None

  if isinstance(parent_obj, (QueryAggregateGroupKey, QueryAggregateMeasure)):
    agg = getattr(parent_obj, "aggregate_node", None)
    input_node = getattr(agg, "input_node", None) if agg is not None else None
    return getattr(input_node, "target_dataset", None) if input_node is not None else None  

  if isinstance(parent_obj, (QueryUnionOutputColumn, QueryUnionBranch)):
    un = getattr(parent_obj, "union_node", None)
    node = getattr(un, "node", None) if un is not None else None
    return getattr(node, "target_dataset", None) if node is not None else None

  if isinstance(parent_obj, QueryUnionBranchMapping):
    br = getattr(parent_obj, "branch", None)
    un = getattr(br, "union_node", None) if br is not None else None
    node = getattr(un, "node", None) if un is not None else None
    return getattr(node, "target_dataset", None) if node is not None else None

  if isinstance(parent_obj, QueryWindowColumn):
    wn = getattr(parent_obj, "window_node", None)
    node = getattr(wn, "node", None) if wn is not None else None
    return getattr(node, "target_dataset", None) if node is not None else None

  if isinstance(parent_obj, QueryWindowColumnArg):
    wc = getattr(parent_obj, "window_column", None)
    wn = getattr(wc, "window_node", None) if wc is not None else None
    node = getattr(wn, "node", None) if wn is not None else None
    return getattr(node, "target_dataset", None) if node is not None else None

  if isinstance(parent_obj, (OrderByExpression, PartitionByExpression)):
    return getattr(parent_obj, "target_dataset", None)

  if isinstance(parent_obj, OrderByItem):
    ob = getattr(parent_obj, "order_by", None)
    return getattr(ob, "target_dataset", None) if ob is not None else None

  if isinstance(parent_obj, PartitionByItem):
    pb = getattr(parent_obj, "partition_by", None)
    return getattr(pb, "target_dataset", None) if pb is not None else None

  return None


def _downstream_target_datasets(td) -> list[str]:
  """
  Return a stable list of downstream datasets (schema.dataset) that depend on td.
  Best-effort: used for user-facing error messages.
  """
  try:
    TargetDatasetInput = apps.get_model("metadata", "TargetDatasetInput")
    qs = TargetDatasetInput.objects.filter(upstream_target_dataset=td)
    # respect active flag if present
    try:
      if any(f.name == "active" for f in TargetDatasetInput._meta.get_fields()):
        qs = qs.filter(active=True)
    except Exception:
      pass

    ds = []
    # Prefer select_related to avoid extra queries
    qs = qs.select_related("target_dataset", "target_dataset__target_schema")
    for r in qs:
      d = getattr(r, "target_dataset", None)
      if d is None:
        continue
      schema = getattr(getattr(d, "target_schema", None), "short_name", None) or ""
      name = getattr(d, "target_dataset_name", None) or ""
      key = f"{schema}.{name}".strip(".")
      if key and key not in ds:
        ds.append(key)
    ds.sort()
    return ds
  except Exception:
    return []


def _blocked_by_downstream_message(td) -> str:
  ds = _downstream_target_datasets(td)
  if ds:
    # escape for safety; this ends up in HTML
    safe = ", ".join(escape(x) for x in ds)
    return (
      "Rename not allowed: this column is already referenced by downstream datasets: "
      + safe
    )
  return (
    "Rename not allowed: this column is already referenced by downstream datasets. "
    "Please update dependent datasets (inputs/mappings) first or remove the dependency."
  )


def _first_attr(obj, names):
  """
  Return the first non-None attribute found on obj from the given list of names.
  Helps when related_name differs (e.g. 'aggregate' vs 'aggregate_node').
  """
  for n in names:
    try:
      v = getattr(obj, n, None)
      if v is not None:
        return v
    except Exception:
      continue
  return None


def _candidate_column_names_for_targetdataset(td, query_node=None):
  """
  Best-effort list of column names to help users pick input_column_name fields.
  Prefers inferred query contract if available; falls back to dataset columns.
  If query_node is provided, infer columns from that node's *input* contract (preferred).
  """
  # 1) Prefer inferred contract for the relevant node (best UX for input_column_name fields)
  try:
    import metadata.generation.query_contract as query_contract
    # ------------------------------------------------------------
    # Determine which node defines the INPUT contract
    # ------------------------------------------------------------
    root = None

    if query_node is not None:

      # AGGREGATE nodes must use their input node contract
      agg = getattr(query_node, "aggregate", None)
      if agg and getattr(agg, "input_node", None):
        root = agg.input_node

      else:
        # WINDOW nodes also operate on input node output
        wnd = getattr(query_node, "window", None)
        if wnd and getattr(wnd, "input_node", None):
          root = wnd.input_node
        else:
          root = query_node

    else:
      root = getattr(td, "query_root", None)

    if root is not None:
      contract = query_contract.infer_query_node_contract(root)
      
      raw = (
        getattr(contract, "available_input_columns", None)
        or getattr(contract, "output_columns", None)
        or getattr(contract, "columns", None)
        or []
      )

      out = []
      for c in raw:
        if isinstance(c, str):
          v = c.strip()
        else:
          v = (getattr(c, "name", None) or getattr(c, "output_name", None) or str(c) or "").strip()
        if v:
          out.append(v)
      if out:
        return out
  except Exception:
    pass

  # 2) Fallback: real TargetColumns (materialization truth / legacy screens)
  cols = []
  try:
    qs = getattr(td, "columns", None) or getattr(td, "target_columns", None)
    if qs is not None:
      items = qs
      # qs may be a related manager or already a list-like (tests)
      if hasattr(items, "all"):
        items = items.all()
      # items may be a QuerySet OR a plain list (FakeQuerySet/list)
      if hasattr(items, "order_by"):
        items = items.order_by("ordinal_position", "id")
      for c in list(items):
        name = (getattr(c, "target_column_name", "") or getattr(c, "name", "") or "").strip()
        if name:
          cols.append(name)
  except Exception:
    pass

  return cols


def _select_widget_from_columns(cols):
  cols = cols or []
  # de-dup but keep order
  seen = set()
  out = []
  for c in cols:
    c = str(c)
    if c and c not in seen:
      seen.add(c)
      out.append(c)
  choices = [("", "---------")] + [(c, c) for c in out]
  return forms.Select(choices=choices)


def _td_from_query_instance(obj):
  """
  Best-effort TargetDataset resolver for Query-scoped models.
  Keep it intentionally small + defensive.
  """
  if obj is None:
    return None

  td = getattr(obj, "target_dataset", None)
  if td is not None:
    return td

  node = getattr(obj, "node", None)
  if node is not None:
    td = getattr(node, "target_dataset", None)
    if td is not None:
      return td

  # Most children have node via their parent (window/aggregate/union)
  for attr in ("window_node", "aggregate_node", "union_node"):
    parent = getattr(obj, attr, None)
    if parent is None:
      continue
    node = getattr(parent, "node", None)
    if node is None:
      continue
    td = getattr(node, "target_dataset", None)
    if td is not None:
      return td

  # WindowColumnArg -> column -> window_node -> node -> td
  col = getattr(obj, "column", None)
  if col is not None:
    wn = getattr(col, "window_node", None)
    node = getattr(wn, "node", None) if wn is not None else None
    td = getattr(node, "target_dataset", None) if node is not None else None
    if td is not None:
      return td

  return None


def _maybe_trigger_query_contract_sync(instance):
  """
  Hard safety net: Query UI mutations must keep TargetColumn in sync.
  Signals are nice, but UI flows can bypass them (bulk paths / edge deletes).
  """
  try:
    if not _is_query_scoped_model(instance.__class__):
      return
  except Exception:
    return

  td = _td_from_query_instance(instance)
  if td is None:
    return

  # Run after commit so contract inference sees the final state.
  transaction.on_commit(lambda: trigger_query_contract_column_sync(td))


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
  page_title = None
  page_title_singular = None


  def _guard_query_mutation_or_redirect(self, request):
    """
    Block create/edit/delete/toggle for query-scoped models if:
    - dataset is not allowed for query logic (raw/stage/rawcore), OR
    - downstream datasets depend on it (contract-changing).
    Returns an HttpResponse redirect if blocked, else None.
    """
    if not _is_query_scoped_model(self.model):
      return None

    parent_obj = None
    try:
      parent_obj = self.get_parent_object()
    except Exception:
      parent_obj = None

    td = _get_query_builder_dataset(parent_obj)
    if td is None:
      return None

    # Layer restriction (hard rule)
    if not query_tree_allowed_for_dataset(td):
      # Give a more actionable error than the generic policy text.
      # (Especially for inline edits where the user otherwise only sees "409 Conflict".)
      reason = _blocked_by_downstream_message(td)
      if _is_htmx(request):
        return htmx_oob_warning(reason)
      messages.error(request, reason)
      return redirect("targetdataset_query_builder", td.pk)

    # Downstream restriction (contract safety)
    if not query_tree_mutations_allowed_for_dataset(td):
      reason = query_tree_mutation_block_reason(td)
      if _is_htmx(request):
        return htmx_oob_warning(reason)
      messages.error(request, reason)
      return redirect("targetdataset_query_builder", td.pk)

    return None


  def _apply_scoped_nav_context(self, ctx):
    """
    Ensure scoped list/edit/detail pages can always navigate back to the owning Query Builder
    (instead of unrelated/unscoped pages).
    """
    parent_obj = self.get_parent_object()

    ctx["parent_pk"] = self.get_parent_pk()
    ctx["parent"] = parent_obj if parent_obj is not None else None

    # Defaults: show breadcrumb to *parent* unless we can point higher (Query Builder)
    ctx["scoped_parent_label"] = str(parent_obj) if parent_obj is not None else ""
    ctx["scoped_parent_url"] = None
    ctx["scoped_parent_is_query_builder"] = False
    ctx["scoped_query_dataset_pk"] = None

    if parent_obj is not None:
      if isinstance(parent_obj, TargetDatasetReference):
        # References are scoped under the referencing dataset
        try:
          ctx["scoped_parent_url"] = reverse(
            "targetdatasetreference_list",
            args=[parent_obj.referencing_dataset_id],
          )
        except Exception:
          pass

      elif isinstance(parent_obj, TargetDatasetJoin):
        # Joins are scoped under the target dataset
        try:
          ctx["scoped_parent_url"] = reverse(
            "targetdatasetjoin_list",
            args=[parent_obj.target_dataset_id],
          )
        except Exception:
          pass

      # Fallback: parent's detail route (only if still unresolved)
      if ctx["scoped_parent_url"] is None:
        parent_model_name = parent_obj.__class__.__name__.lower()
        for route in (f"{parent_model_name}_detail", f"{parent_model_name}_detail_scoped"):
          try:
            ctx["scoped_parent_url"] = reverse(route, args=[parent_obj.pk])
            break
          except Exception:
            # IMPORTANT: don't overwrite a previously valid URL
            continue

    # Query-scoped navigation: prefer owning dataset's Query Builder.
    query_ds = _get_query_builder_dataset(parent_obj)
    if query_ds is not None:
      ctx["scoped_parent_label"] = str(query_ds)
      ctx["scoped_parent_url"] = reverse("targetdataset_query_builder", args=[query_ds.pk])
      ctx["scoped_parent_is_query_builder"] = True
      ctx["scoped_query_dataset_pk"] = query_ds.pk

    return ctx


  def get_parent_pk(self):
    """Return the parent object's primary key from URL kwargs."""
    return self.kwargs.get("parent_pk")
  
  def get_success_url(self):
    """After save/delete, redirect back to the scoped list of the same parent."""
    return reverse_lazy(self.route_name, kwargs={"parent_pk": self.get_parent_pk()})

  def get_parent_object(self):
    """Load and return the parent model instance, if defined."""
    pk = self.get_parent_pk()
    if not pk:
      return None
    return self.parent_model.objects.get(pk=pk)

  def get_context_base(self, request):
    """
    Build the same base context as GenericCRUDView.list(),
    but include parent_pk and parent for scoped templates.
    """
    auto_filter_cfgs = self.build_auto_filter_config()
    qs = self.get_queryset()
    qs, active_filters = self.apply_auto_filters(request, qs, auto_filter_cfgs)
    if not qs.query.order_by:
      if self.model._meta.ordering:
        qs = qs.order_by(*self.model._meta.ordering)
      else:
        qs = qs.order_by("id")

    title = self.model._meta.verbose_name_plural.title()
    if getattr(self, "page_title", None):
      title = self.page_title

    ctx = {
      "model": self.model,
      "objects": qs,
      "fields": self.get_list_fields(),
      "model_name": self.model._meta.model_name,
      "model_class_name": self.model.__name__,
      "meta": self.model._meta,
      "title": title,      
      "auto_filter_cfgs": auto_filter_cfgs,
      "active_filters": active_filters,
    }

    # Scoped breadcrumb/query-builder nav for LIST pages
    self._apply_scoped_nav_context(ctx)

    # expose detail_route_name so row.html can build a scoped detail_url
    detail_route_name = getattr(self, "detail_route_name", None)
    if detail_route_name:
      ctx["detail_route_name"] = detail_route_name
      
    return ctx
  

  def edit(self, request, pk=None):
    """
    Same as GenericCRUDView.edit(), but:
    - Cancel returns to the scoped list (same parent)
    - Context includes scoped breadcrumb + Query Builder navigation
    """
    obj = get_object_or_404(self.model, pk=pk) if pk else None
    FormClass = self.get_form_class()
    if request.method == "POST":
      blocked = self._guard_query_mutation_or_redirect(request)
      if blocked is not None:
        return blocked
      form = FormClass(request.POST, instance=obj)
      self.apply_system_managed_locking(form, obj)
      form = self.enhance_form(form)
      if form.is_valid():
        instance = form.save(commit=False)
        user = get_current_user() or request.user
        self._set_audit_fields(instance, user, pk is None)
        self.enforce_system_managed_integrity(instance)

        try:
          model_name = instance.__class__.__name__
          is_query_relevant = model_name.startswith("Query") or model_name.startswith("OrderBy") or model_name.startswith("PartitionBy")
          if is_query_relevant:
            # resolve owning dataset
            td = getattr(instance, "target_dataset", None)
            if td is None:
              node = getattr(instance, "node", None)
              td = getattr(node, "target_dataset", None) if node is not None else None
            if td is None:
              parent_obj = self.get_parent_object()
              td = _get_query_builder_dataset(parent_obj)

            if td is not None:
              # Only bizcore/serving may define custom query logic
              if query_tree_allowed_for_dataset(td):
                TargetDatasetInput = apps.get_model("metadata", "TargetDatasetInput")
                qs = TargetDatasetInput.objects.filter(upstream_target_dataset=td)
                if hasattr(TargetDatasetInput, "active"):
                  qs = qs.filter(active=True)
                if qs.exists():
                  messages.error(request, _("Blocked: downstream datasets depend on this dataset (see Lineage)."))
                  return redirect(self.get_success_url())
        except Exception:
          pass

        instance.save()
        messages.success(request, _("Saved successfully."))
        return redirect(self.get_success_url())
    else:
      data = request.GET if request.GET else None
      form = FormClass(data=data, instance=obj)
      self.apply_system_managed_locking(form, obj)
      form = self.enhance_form(form)

    form = self._apply_autofocus(form)

    title = _("Edit") if pk else _("Create")
    if getattr(self, "page_title_singular", None):
      title = f"{title} {self.page_title_singular}"

    context = {
      "form": form,
      "object": obj,
      "model": self.model,
      "title": title,
      "cancel_url": self.get_success_url(),
    }
    self._apply_scoped_nav_context(context)
    return render(request, self.template_form, context)

  def detail(self, request, pk):
    """
    Same as GenericCRUDView.detail(), but includes scoped breadcrumb + Query Builder navigation.
    """
    obj = get_object_or_404(self.model, pk=pk)
    excluded = {"id", "created_at", "created_by", "updated_at", "updated_by", "lineage_key", "former_names"}

    clean_rows = []
    for f in self.model._meta.fields:
      if f.name not in excluded:
        raw_value = getattr(obj, f.name, "")
        display_value = "" if raw_value is None else raw_value
        clean_rows.append((f, display_value))

    title = f"{self.model._meta.verbose_name.title()} Details"
    if getattr(self, "page_title_singular", None):
      title = f"{self.page_title_singular} details"

    context = {
      "object": obj,
      "model": self.model,
      "model_name": self.model._meta.model_name,
      "title": title,
      "fields": [f for f in self.model._meta.fields if f.name not in excluded],
      "rows": clean_rows,
      "many_to_many": [f for f in self.model._meta.many_to_many if f.name not in excluded],
      "related_objects": self.get_related_objects(obj),
    }
    self._apply_scoped_nav_context(context)
    return render(request, "generic/detail.html", context)


  def _remove_parent_fk_from_form(self, form):
    """
    For scoped views: remove the FK field that points to the parent model
    from the visible form fields so the user can't re-parent or choose
    the wrong parent when creating.
    """
    parent_field = self._get_parent_relation_field_name()
    if parent_field and parent_field in form.fields:
      form.fields.pop(parent_field, None)

    return form

  def _apply_autofocus(self, form):
    # pick first usable field after all locking/removals
    for name, field in form.fields.items():
      if getattr(field, "disabled", False):
        continue
      w = field.widget
      # skip hidden/checkbox
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
    blocked = self._guard_query_mutation_or_redirect(request)
    if blocked is not None:
      return blocked

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

    parent_fk = self._get_parent_relation_field_name()
    initial = {}

    # UX refresh: copy current selections into initial, but DO NOT bind (data=None)
    if request.GET:
      # QueryDict: values are lists; keep lists for multi-selects, collapse singletons
      for k in request.GET.keys():
        vals = request.GET.getlist(k)
        if len(vals) == 1:
          initial[k] = vals[0]
        else:
          initial[k] = vals

    # keep parent binding in initial (scoped create)
    # do this AFTER copying request.GET so the parent always wins
    if parent_fk:
      initial[parent_fk] = self.get_parent_pk()

    form = FormClass(initial=initial)

    # Apply system-managed field locking + full enhancement pipeline
    self.apply_system_managed_locking(form, instance=None)
    form = self.enhance_form(form)

    # Hide parent FK field from user input in scoped views.
    # Note: parent FK is injected into POST in row_create(), so it doesn't need to be in the form.
    form = self._remove_parent_fk_from_form(form)

    # Apply autofocus BEFORE building ctx so templates receive the final widget attrs
    form = self._apply_autofocus(form)    

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
      "refresh_url": request.path,
    }
    parent_obj = self.get_parent_object()
    if parent_obj is not None:
      ctx["parent"] = parent_obj

    return render(request, "generic/row_form.html", ctx, status=200)

  def _bind_parent_on_instance(self, instance, parent_obj):
    """
    Ensure the FK to parent_model is already set on the instance BEFORE validation.
    This avoids model.clean() / full_clean() errors when the parent FK field is removed from the form.
    """
    if not self.parent_model or parent_obj is None:
      return

    parent_field = self._get_parent_relation_field_name()
    if parent_field:
      setattr(instance, parent_field, parent_obj)
    return

  def row_create(self, request):
    """
    Handle POST from inline 'Add new row' form in a scoped list.
    Returns a single <tr> (row.html) on success so HTMX can insert it.
    We override GenericCRUDView.row_create() only to:
      - bind the new row to its parent_pk
      - set audit fields
      - return row.html with all required context keys
    """
    blocked = self._guard_query_mutation_or_redirect(request)
    if blocked is not None:
      return blocked

    if request.method != "POST":
      return HttpResponseNotFound("POST required")

    if self.is_creation_blocked_for_model():
      return HttpResponse(status=204)

    FormClass = self.get_form_class()

    # Inject parent FK into POST so form __init__ can filter querysets correctly.
    post = request.POST.copy()
    parent_fk = self._get_parent_relation_field_name()
    if parent_fk and not post.get(parent_fk):
      post[parent_fk] = str(self.get_parent_pk())

    form = FormClass(post)

    # lock system-managed fields before validation
    self.apply_system_managed_locking(form, instance=None)
    form = self.enhance_form(form)

    # Remove parent FK from form (user must not choose parent)
    form = self._remove_parent_fk_from_form(form)

    # IMPORTANT: bind parent BEFORE validation so model.clean() sees it
    parent_obj = self.get_parent_object()
    self._bind_parent_on_instance(form.instance, parent_obj)

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

      # parent already bound on form.instance before validation

      instance.save()

      # Save ManyToMany relationships explicitly (e.g. upstream_columns)
      if hasattr(form, "save_m2m"):
        form.save_m2m()

      # Hard safety net: keep TargetColumn in sync for query-scoped mutations
      _maybe_trigger_query_contract_sync(instance)        

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

    return render(request, "generic/row_form.html", ctx, status=200)

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

    # IMPORTANT: GET is not a mutation. We only block actual mutations.
    if request.method in ("POST", "DELETE"):
      blocked = self._guard_query_mutation_or_redirect(request)
      if blocked is not None:
        return blocked

    try:
      instance = self.model.objects.get(pk=pk)
    except self.model.DoesNotExist:
      return HttpResponseNotFound("Row not found")

    FormClass = self.get_form_class()

    if request.method == "GET":
      # render inline edit form
      # UX: refresh should not bind the form; use initial overlay instead
      if request.GET:
        initial = dict(request.GET)
        initial = {k: (v[0] if isinstance(v, list) and len(v) == 1 else v) for k, v in initial.items()}
        form = FormClass(instance=instance, initial=initial)
      else:
        form = FormClass(instance=instance)      

      self.apply_system_managed_locking(form, instance=instance)
      form = self.enhance_form(form)

      # Remove parent FK and bind parent BEFORE validation
      form = self._remove_parent_fk_from_form(form)
      parent_obj = self.get_parent_object()
      self._bind_parent_on_instance(form.instance, parent_obj)

      # Apply autofocus BEFORE ctx so the template sees the final attrs
      form = self._apply_autofocus(form)      

      ctx = {
        "model": self.model,
        "meta": self.model._meta,
        "form": form,
        "object": instance,
        "fields": self.get_list_fields(),
        "model_name": self.model._meta.model_name,
        "model_class_name": self.model.__name__,
        "is_new": False,
        "parent_pk": self.get_parent_pk(),  # critical so row_form.html generates scoped hx-post
        "refresh_url": request.path,
      }
      parent_obj = self.get_parent_object()
      if parent_obj is not None:
        ctx["parent"] = parent_obj

      return render(request, "generic/row_form.html", ctx, status=200)

    # POST -> save update
    if request.method == "POST":
      form = FormClass(request.POST, instance=instance)

      self.apply_system_managed_locking(form, instance=instance)
      form = self.enhance_form(form)

      # Hide/remove parent FK field from the form
      form = self._remove_parent_fk_from_form(form)

      # IMPORTANT: enforce correct parent BEFORE validation (anti-tamper + clean())
      parent_obj = self.get_parent_object()
      self._bind_parent_on_instance(form.instance, parent_obj)      

      if form.is_valid():
        updated = form.save(commit=False)

        # audit on update
        user = getattr(request, "user", None)
        self._set_audit_fields(updated, user, is_new=False)

        # keep readonly/system-managed guarantees
        self.enforce_system_managed_integrity(updated)

        # parent already enforced on form.instance before validation

        updated.save()

        # Save ManyToMany relationships explicitly (e.g. upstream_columns)
        if hasattr(form, "save_m2m"):
          form.save_m2m()

        # Hard safety net: keep TargetColumn in sync for query-scoped mutations
        _maybe_trigger_query_contract_sync(updated)

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
      form = self._apply_autofocus(form)

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

      return render(request, "generic/row_form.html", ctx, status=200)

    return HttpResponseNotFound("Unsupported method for row_edit")


  def row_delete(self, request, pk):
    blocked = self._guard_query_mutation_or_redirect(request)
    if blocked is not None:
      return blocked

    # If we delete a QueryNode that is currently used as td.query_root,
    # we must "rewind" td.query_root to the node's input to avoid silently
    # disabling custom query logic.
    try:
      obj = self.model.objects.get(pk=pk)
    except self.model.DoesNotExist:
      return HttpResponseNotFound("Row not found")

    if obj.__class__.__name__ == "QueryNode":
      td = getattr(obj, "target_dataset", None)
      if td is not None:
        obj_id = getattr(obj, "id", None)
        is_root = (getattr(td, "query_root_id", None) == obj_id)
        is_head = (getattr(td, "query_head_id", None) == obj_id)

        if is_root or is_head:
          next_root = None
          ntype = (getattr(obj, "node_type", "") or "").strip().lower()

          if ntype == "window":
            w = getattr(obj, "window", None)
            next_root = getattr(w, "input_node", None) if w is not None else None
          elif ntype == "aggregate":
            a = getattr(obj, "aggregate", None)
            next_root = getattr(a, "input_node", None) if a is not None else None
          elif ntype == "union":
            un = getattr(obj, "union", None)
            if un is not None:
              b = un.branches.all().order_by("ordinal_position", "id").first()
              next_root = getattr(b, "input_node", None) if b is not None else None
          else:
            next_root = None

          with transaction.atomic():
            update_fields = []
            # query_head drives SQL generation
            if is_head:
              td.query_head = next_root
              update_fields.append("query_head")

            # query_root is the base-select anchor. Only update it if we delete the root itself.
            if is_root:
              td.query_root = next_root
              update_fields.append("query_root")

            if update_fields:
              td.save(update_fields=update_fields)

    return super().row_delete(request, pk)


  def row_toggle(self, request, pk):
    """
    Inline toggle of a boolean field (e.g. 'integrate') in a scoped list.
    Expects POST with {"field": "<fieldname>"}.
    Returns the updated <tr> (row.html).
    """
    blocked = self._guard_query_mutation_or_redirect(request)
    if blocked is not None:
      return blocked

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
    self._bind_parent_on_instance(instance, parent_obj)

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


class TargetColumnScopedView(_ScopedChildView):
  model = TargetColumn
  parent_model = TargetDataset
  route_name = "targetcolumn_list"
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

    rawcore_qs = (
      TargetDataset.objects
      .filter(target_schema__short_name="rawcore")
      .exclude(target_dataset_name__endswith="_hist")
      .order_by("target_dataset_name")
    )

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

  def get_parent_object(self):
    parent = super().get_parent_object()
    if parent is None:
      return None
    # References only in rawcore allowed
    if parent.target_schema.short_name != "rawcore":
      raise Http404("References are only available for rawcore datasets.")
    return parent


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
        .filter(
          target_dataset=parent.referencing_dataset,
          system_role="",
        )
        .order_by("target_dataset__target_dataset_name", "ordinal_position")
      )

    # Parent: referenced_dataset → only business key columns
    to_field = form.fields.get("to_column")
    if to_field is not None:
      to_field.queryset = (
        to_field.queryset
        .filter(
          target_dataset=parent.referenced_dataset,
          system_role="business_key",
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


class TargetDatasetJoinScopedView(_ScopedChildView):
  """
  Scoped CRUD for TargetDatasetJoin, filtered by parent TargetDataset
  """
  model = TargetDatasetJoin
  parent_model = TargetDataset
  route_name = "targetdatasetjoin_list"
  detail_route_name = "targetdatasetjoin_detail_scoped"

  def get_queryset(self):
    return (
      self.model.objects
      .filter(target_dataset_id=self.get_parent_pk())
      .select_related("left_input", "right_input", "target_dataset")
      .order_by("join_order", "id")
    )

  def get_context_data(self, **kwargs):
    ctx = super().get_context_data(**kwargs)
    ctx["dataset"] = self.get_parent_object()
    ctx["title"] = f"Joins for target dataset {ctx['dataset']}"
    return ctx

  def enhance_dynamic_fields(self, form):
    form = super().enhance_dynamic_fields(form)

    parent = self.get_parent_object()
    if not parent:
      return form

    # Only inputs of this dataset are valid join endpoints
    inputs_qs = (
      TargetDatasetInput.objects
      .filter(target_dataset=parent, active=True)
      .select_related("source_dataset", "upstream_target_dataset", "target_dataset")
      .order_by("role", "id")
    )

    lf = form.fields.get("left_input")
    rf = form.fields.get("right_input")
    if lf is not None:
      lf.queryset = inputs_qs
    if rf is not None:
      rf.queryset = inputs_qs

    return form

  def get_parent_object(self):
    parent = super().get_parent_object()
    if parent and parent.target_schema.short_name not in ("bizcore", "serving"):
      raise Http404("Joins are only available for Bizcore/Serving datasets.")

    return parent


class TargetDatasetJoinPredicateScopedView(_ScopedChildView):
  """
  Scoped CRUD for TargetDatasetJoinPredicate, filtered by parent TargetDatasetJoin
  """
  model = TargetDatasetJoinPredicate
  parent_model = TargetDatasetJoin
  route_name = "targetdatasetjoinpredicate_list"

  def get_queryset(self):
    return (
      self.model.objects
      .filter(join_id=self.get_parent_pk())
      .select_related("join", "join__left_input", "join__right_input", "join__target_dataset")
      .order_by("ordinal_position", "id")
    )

  def get_context_data(self, **kwargs):
    ctx = super().get_context_data(**kwargs)
    parent_join = self.get_parent_object()  # parent = TargetDatasetJoin
    ctx["scoped_parent_label"] = str(parent_join)
    ctx["scoped_parent_url"] = reverse("targetdatasetjoin_list", args=[parent_join.target_dataset_id])
    return ctx
  
  def get_context_base(self, request):
    """
    IMPORTANT:
    list() renders get_context_base() directly (not get_context_data()).
    So breadcrumbs for the LIST must be set here.
    """
    ctx = super().get_context_base(request)
    parent_join = self.get_parent_object()  # parent = TargetDatasetJoin

    # Breadcrumb should bring you back to the Joins list of the dataset
    # (not to a non-existing TargetDatasetJoin "detail" view).
    ctx["scoped_parent_label"] = str(parent_join)
    ctx["scoped_parent_url"] = reverse("targetdatasetjoin_list", args=[parent_join.target_dataset_id]) 

    return ctx


# -------------------------------------------------------------------
# Query scoped views (UI/CRUD support, excluded from main menu)
# -------------------------------------------------------------------
class QueryNodeScopedView(_ScopedChildView):
  model = QueryNode
  parent_model = TargetDataset
  route_name = "querynode_list"
  detail_route_name = "querynode_detail_scoped"
  page_title = "Query nodes"
  page_title_singular = "Query node"

  def get_queryset(self):
    # select_related to avoid N+1 when building summaries
    return (
      self.model.objects
      .filter(target_dataset_id=self.get_parent_pk())
      .select_related("window", "aggregate", "union", "select")
      .order_by("id")
    )

  def get_context_base(self, request):
    ctx = super().get_context_base(request)
    # Build lightweight summaries for UX ("what did I configure here?")
    summaries = {}

    def _count_active(qs):
      """
      Count active rows if the model supports an `active` field, otherwise count all.
      """
      if qs is None:
        return 0
      try:
        # Only filter if the field exists on the model
        if any(f.name == "active" for f in qs.model._meta.get_fields()):
          return qs.filter(active=True).count()
      except Exception:
        pass
      try:
        return qs.count()
      except Exception:
        return 0

    for n in ctx.get("objects", []):
      parts = []
      try:
        nt = getattr(n, "node_type", "") or ""
        if nt == "select":
          parts.append("base select")

        elif nt == "window":
          w = _first_attr(n, ["window", "window_node", "windownode", "querywindownode"])

          if w is not None:
            cols_qs = getattr(w, "columns", None)
            c_cnt = _count_active(cols_qs)

            parts.append(f"columns: {c_cnt}")
            if c_cnt:
              try:
                if any(f.name == "active" for f in cols_qs.model._meta.get_fields()):
                  first = cols_qs.filter(active=True).order_by("ordinal_position").first()
                else:
                  first = cols_qs.order_by("ordinal_position").first()
              except Exception:
                first = cols_qs.order_by("ordinal_position").first() if cols_qs is not None else None

              if first is not None:
                out = (getattr(first, "output_name", "") or "").strip()
                if out:
                  parts.append(f"e.g. {out}")
          if not parts:
            parts.append("window")

        elif nt == "aggregate":
          a = _first_attr(n, ["aggregate", "aggregate_node", "aggregatenode", "queryaggregatenode"])

          if a is not None:
            gk_qs = getattr(a, "group_keys", None)
            m_qs = getattr(a, "measures", None)
            gk_cnt = _count_active(gk_qs)
            m_cnt = _count_active(m_qs)

            parts.append(f"group keys: {gk_cnt}")
            parts.append(f"measures: {m_cnt}")
          if not parts:
            parts.append("aggregate")

        elif nt == "union":
          u = _first_attr(n, ["union", "union_node", "unionnode", "queryunionnode"])

          if u is not None:
            b_qs = getattr(u, "branches", None)
            o_qs = getattr(u, "output_columns", None)
            b_cnt = b_qs.count() if b_qs is not None else 0
            o_cnt = o_qs.count() if o_qs is not None else 0

            parts.append(f"branches: {b_cnt}")
            parts.append(f"outputs: {o_cnt}")
          if not parts:
            parts.append("union")

      except Exception:
        pass

      if parts:
        summaries[n.pk] = " · ".join([p for p in parts if p])

    ctx["node_summaries"] = summaries
    return ctx
  

class QuerySelectNodeScopedView(_ScopedChildView):
  model = QuerySelectNode
  parent_model = QueryNode
  route_name = "queryselectnode_list"
  detail_route_name = "queryselectnode_detail_scoped"
  page_title = "Select operator"
  page_title_singular = "Select operator"

  def get_queryset(self):
    return self.model.objects.filter(node_id=self.get_parent_pk()).select_related("node").order_by("id")


class QueryAggregateNodeScopedView(_ScopedChildView):
  model = QueryAggregateNode
  parent_model = QueryNode
  route_name = "queryaggregatenode_list"
  detail_route_name = "queryaggregatenode_detail_scoped"
  page_title = "Aggregate operator"
  page_title_singular = "Aggregate operator"

  def get_queryset(self):
    return self.model.objects.filter(node_id=self.get_parent_pk()).select_related("node", "input_node").order_by("id")


class QueryAggregateGroupKeyScopedView(_ScopedChildView):
  model = QueryAggregateGroupKey
  parent_model = QueryAggregateNode
  route_name = "queryaggregategroupkey_list"
  detail_route_name = "queryaggregategroupkey_detail_scoped"
  page_title = "Aggregate group keys"
  page_title_singular = "Aggregate group key"

  def get_queryset(self):
    return self.model.objects.filter(aggregate_node_id=self.get_parent_pk()).order_by("ordinal_position", "id")
  
  def enhance_form(self, form):
    form = super().enhance_form(form)
    try:
      parent = self.get_parent_object()  # QueryAggregateNode
      td = _get_query_builder_dataset(parent)
      input_node = getattr(parent, "input_node", None)
      if td and "input_column_name" in form.fields:
        cols = _candidate_column_names_for_targetdataset(td, query_node=input_node)
        form.fields["input_column_name"].widget = _select_widget_from_columns(cols)
    except Exception:
      pass
    return form
  

class QueryAggregateMeasureScopedView(_ScopedChildView):
  model = QueryAggregateMeasure
  parent_model = QueryAggregateNode
  route_name = "queryaggregatemeasure_list"
  detail_route_name = "queryaggregatemeasure_detail_scoped"
  page_title = "Aggregate measures"
  page_title_singular = "Aggregate measure"

  def get_queryset(self):
    return self.model.objects.filter(aggregate_node_id=self.get_parent_pk()).select_related("order_by").order_by("ordinal_position", "id")
  
  def enhance_form(self, form):
    form = super().enhance_form(form)
    try:
      parent = self.get_parent_object()  # QueryAggregateNode
      td = _get_query_builder_dataset(parent)
      input_node = getattr(parent, "input_node", None)
      # Some implementations call it input_column_name, others source_column_name etc.
      for fname in ("input_column_name", "source_column_name"):
        if td and fname in form.fields:
          cols = _candidate_column_names_for_targetdataset(td, query_node=input_node)
          form.fields[fname].widget = _select_widget_from_columns(cols)
    except Exception:
      pass
    return form
  

class OrderByExpressionScopedView(_ScopedChildView):
  model = OrderByExpression
  parent_model = TargetDataset
  route_name = "orderbyexpression_list"
  detail_route_name = "orderbyexpression_detail_scoped"
  page_title = "Order by definitions"
  page_title_singular = "Order by definition"

  def get_queryset(self):
    return self.model.objects.filter(target_dataset_id=self.get_parent_pk()).order_by("name", "id")

  def row_create(self, request):
    """
    HTMX inline-create: after creating an OrderByExpression, immediately jump
    into the items editor (OrderByItem scoped list).
    """
    if request.method != "POST":
      return HttpResponseNotFound("POST required")

    if self.is_creation_blocked_for_model():
      return HttpResponse(status=204)

    FormClass = self.get_form_class()
    post = request.POST.copy()
    # parent = TargetDataset
    parent_fk = self._get_parent_relation_field_name()
    if parent_fk and not post.get(parent_fk):
      post[parent_fk] = str(self.get_parent_pk())

    form = FormClass(post)
    self.apply_system_managed_locking(form, instance=None)
    form = self.enhance_form(form)
    form = self._remove_parent_fk_from_form(form)

    parent_obj = self.get_parent_object()
    self._bind_parent_on_instance(form.instance, parent_obj)

    if form.is_valid():
      obj = form.save(commit=False)
      user = getattr(request, "user", None)
      self._set_audit_fields(obj, user, is_new=True)
      if hasattr(obj, "is_system_managed"):
        obj.is_system_managed = False
      self.enforce_system_managed_integrity(obj)
      obj.save()

      redirect_url = reverse("orderbyitem_list", kwargs={"parent_pk": obj.pk})
      resp = HttpResponse(status=204)
      resp["HX-Redirect"] = redirect_url
      return resp

    # invalid -> keep inline editor open
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
    return render(request, "generic/row_form.html", ctx, status=200)


class OrderByItemScopedView(_ScopedChildView):
  model = OrderByItem
  parent_model = OrderByExpression
  route_name = "orderbyitem_list"
  detail_route_name = "orderbyitem_detail_scoped"
  page_title = "Order by items"
  page_title_singular = "Order by item"

  def get_queryset(self):
    return self.model.objects.filter(order_by_id=self.get_parent_pk()).order_by("ordinal_position", "id")
  
  def enhance_form(self, form):
    form = super().enhance_form(form)
    try:
      parent = self.get_parent_object()  # OrderByExpression
      td = getattr(parent, "target_dataset", None)
      if td and "input_column_name" in form.fields:
        cols = _candidate_column_names_for_targetdataset(td)
        form.fields["input_column_name"].widget = _select_widget_from_columns(cols)
    except Exception:
      pass
    return form


class QueryUnionNodeScopedView(_ScopedChildView):
  model = QueryUnionNode
  parent_model = QueryNode
  route_name = "queryunionnode_list"
  detail_route_name = "queryunionnode_detail_scoped"
  page_title = "UNION operator"
  page_title_singular = "UNION operator"

  def get_queryset(self):
    return self.model.objects.filter(node_id=self.get_parent_pk()).select_related("node").order_by("id")


class QueryUnionOutputColumnScopedView(_ScopedChildView):
  model = QueryUnionOutputColumn
  parent_model = QueryUnionNode
  route_name = "queryunionoutputcolumn_list"
  detail_route_name = "queryunionoutputcolumn_detail_scoped"
  page_title = "UNION output columns"
  page_title_singular = "UNION output column"

  def get_queryset(self):
    return self.model.objects.filter(union_node_id=self.get_parent_pk()).order_by("ordinal_position", "id")
  
  def get_context_base(self, request):
    ctx = super().get_context_base(request)
    try:
      union_node = self.get_parent_object()
      ctx["union_parent_pk"] = getattr(union_node, "pk", None)
      ctx["union_branches"] = list(
        union_node.branches.all().order_by("ordinal_position", "id")
      ) if union_node is not None else []
      ctx["union_output_count"] = union_node.output_columns.count() if union_node else 0

    except Exception:
      ctx["union_parent_pk"] = None
      ctx["union_branches"] = []
      ctx["union_output_count"] = 0
    return ctx


class QueryUnionBranchScopedView(_ScopedChildView):
  model = QueryUnionBranch
  parent_model = QueryUnionNode
  route_name = "queryunionbranch_list"
  detail_route_name = "queryunionbranch_detail_scoped"
  page_title = "UNION branches"
  page_title_singular = "UNION branch"

  def get_queryset(self):
    return (
      self.model.objects
      .filter(union_node_id=self.get_parent_pk())
      .select_related("input_node", "input_node__target_dataset")
      .order_by("ordinal_position", "id")
    )

  def get_context_base(self, request):
    ctx = super().get_context_base(request)
    try:
      un = self.get_parent_object()
      ctx["union_parent_pk"] = getattr(un, "pk", None)
      branches = list(un.branches.all().order_by("ordinal_position", "id")) if un else []
      ctx["union_branches"] = branches

      # Guardrail counts: mappings completeness per branch
      total_out = 0
      try:
        total_out = un.output_columns.count() if un else 0        
      except Exception:
        total_out = 0
      ctx["union_output_count"] = total_out

      # mapped count per branch (distinct output_column)
      mapped_rows = (
        QueryUnionBranchMapping.objects
        .filter(branch_id__in=[b.pk for b in branches])
        .values("branch_id")
        .annotate(mapped=Count("output_column", distinct=True))
      ) if branches else []
      mapped_by_branch = {r["branch_id"]: r["mapped"] for r in mapped_rows}

      counts = {}
      for b in branches:
        mapped = int(mapped_by_branch.get(b.pk, 0) or 0)
        missing = total_out - mapped
        if missing < 0:
          missing = 0
        counts[b.pk] = {"mapped": mapped, "missing": missing, "total": total_out}
      ctx["branch_mapping_counts"] = counts

    except Exception:
      ctx["union_parent_pk"] = None
      ctx["union_branches"] = []
      ctx["union_output_count"] = 0
      ctx["branch_mapping_counts"] = {}
    return ctx


class QueryUnionBranchMappingScopedView(_ScopedChildView):
  model = QueryUnionBranchMapping
  parent_model = QueryUnionBranch
  route_name = "queryunionbranchmapping_list"
  detail_route_name = "queryunionbranchmapping_detail_scoped"
  page_title = "UNION branch mappings"
  page_title_singular = "UNION branch mapping"

  def get_queryset(self):
    return (
      self.model.objects
      .filter(branch_id=self.get_parent_pk())
      .select_related("output_column", "branch", "branch__input_node", "branch__input_node__target_dataset")
      .order_by("output_column__ordinal_position", "id")
    )
  
  def get_context_base(self, request):
    ctx = super().get_context_base(request)
    # Provide union_node id for navigation (Mappings -> Branches / Output columns)
    try:
      br = self.get_parent_object()
      un = getattr(br, "union_node", None)
      ctx["union_parent_pk"] = getattr(un, "pk", None)
    except Exception:
      ctx["union_parent_pk"] = None
    return ctx
  
  def enhance_form(self, form):
    form = super().enhance_form(form)
    try:
      parent = self.get_parent_object()  # QueryUnionBranch
      td = _get_query_builder_dataset(parent)
      input_node = getattr(parent, "input_node", None)
      if td:
        for fname in ("input_column_name", "source_column_name"):
          if fname in form.fields:
            cols = _candidate_column_names_for_targetdataset(td, query_node=input_node)
            form.fields[fname].widget = _select_widget_from_columns(cols)
    except Exception:
      pass
    return form


class QueryWindowNodeScopedView(_ScopedChildView):
  model = QueryWindowNode
  parent_model = QueryNode
  route_name = "querywindownode_list"
  detail_route_name = "querywindownode_detail_scoped"
  page_title = "Window operator"
  page_title_singular = "Window operator"

  def get_queryset(self):
    return self.model.objects.filter(node_id=self.get_parent_pk()).select_related("node", "input_node").order_by("id")


class PartitionByExpressionScopedView(_ScopedChildView):
  model = PartitionByExpression
  parent_model = TargetDataset
  route_name = "partitionbyexpression_list"
  detail_route_name = "partitionbyexpression_detail_scoped"
  page_title = "Partition by definitions"
  page_title_singular = "Partition by definition"

  def get_queryset(self):
    return self.model.objects.filter(target_dataset_id=self.get_parent_pk()).order_by("name", "id")

  def row_create(self, request):
    """
    HTMX inline-create: after creating a PartitionByExpression, immediately jump
    into the items editor (PartitionByItem scoped list).
    """
    if request.method != "POST":
      return HttpResponseNotFound("POST required")

    if self.is_creation_blocked_for_model():
      return HttpResponse(status=204)

    FormClass = self.get_form_class()
    post = request.POST.copy()
    parent_fk = self._get_parent_relation_field_name()
    if parent_fk and not post.get(parent_fk):
      post[parent_fk] = str(self.get_parent_pk())

    form = FormClass(post)
    self.apply_system_managed_locking(form, instance=None)
    form = self.enhance_form(form)
    form = self._remove_parent_fk_from_form(form)

    parent_obj = self.get_parent_object()
    self._bind_parent_on_instance(form.instance, parent_obj)

    if form.is_valid():
      obj = form.save(commit=False)
      user = getattr(request, "user", None)
      self._set_audit_fields(obj, user, is_new=True)
      if hasattr(obj, "is_system_managed"):
        obj.is_system_managed = False
      self.enforce_system_managed_integrity(obj)
      obj.save()

      redirect_url = reverse("partitionbyitem_list", kwargs={"parent_pk": obj.pk})
      resp = HttpResponse(status=204)
      resp["HX-Redirect"] = redirect_url
      return resp

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
    return render(request, "generic/row_form.html", ctx, status=200)


class PartitionByItemScopedView(_ScopedChildView):
  model = PartitionByItem
  parent_model = PartitionByExpression
  route_name = "partitionbyitem_list"
  detail_route_name = "partitionbyitem_detail_scoped"
  page_title = "Partition by items"
  page_title_singular = "Partition by item"

  def get_queryset(self):
    return self.model.objects.filter(partition_by_id=self.get_parent_pk()).order_by("ordinal_position", "id")
  
  def enhance_form(self, form):
    form = super().enhance_form(form)
    try:
      parent = self.get_parent_object()  # PartitionByExpression
      td = getattr(parent, "target_dataset", None)
      if td and "input_column_name" in form.fields:
        cols = _candidate_column_names_for_targetdataset(td)
        form.fields["input_column_name"].widget = _select_widget_from_columns(cols)
    except Exception:
      pass
    return form


class QueryWindowColumnScopedView(_ScopedChildView):
  model = QueryWindowColumn
  parent_model = QueryWindowNode
  route_name = "querywindowcolumn_list"
  detail_route_name = "querywindowcolumn_detail_scoped"
  page_title = "Window columns"
  page_title_singular = "Window column"

  def get_queryset(self):
    return self.model.objects.filter(window_node_id=self.get_parent_pk()).select_related("order_by", "partition_by").order_by("ordinal_position", "id")


class QueryWindowColumnArgScopedView(_ScopedChildView):
  model = QueryWindowColumnArg
  parent_model = QueryWindowColumn
  route_name = "querywindowcolumnarg_list"
  detail_route_name = "querywindowcolumnarg_detail_scoped"
  page_title = "Window function arguments"
  page_title_singular = "Window function argument"

  def get_queryset(self):
    return self.model.objects.filter(window_column_id=self.get_parent_pk()).order_by("ordinal_position", "id")
