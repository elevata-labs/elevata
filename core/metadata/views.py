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

import re
import traceback
from io import StringIO

from collections import deque
from django.core.management import call_command
from django.apps import apps
from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required, permission_required
from django.core.management import call_command
from django.db import transaction
from django.http import JsonResponse, HttpResponse, HttpResponseBadRequest, Http404
from django.shortcuts import get_object_or_404, render, redirect
from django.template.loader import render_to_string
from django.views.decorators.http import require_POST, require_GET
from sqlalchemy.exc import SQLAlchemyError

from generic import GenericCRUDView

from metadata.constants import DIALECT_HINTS
from metadata.forms import TargetColumnForm, TargetDatasetForm
from metadata.generation.policies import (
  query_tree_allowed_for_dataset,
  query_tree_mutations_allowed_for_dataset,
  query_tree_mutation_block_reason,
)
from metadata.generation.query_contract import infer_query_node_contract
from metadata.generation.query_contract_diff import compute_contract_diff
from metadata.generation.query_governance import analyze_query_governance
from metadata.generation.validators import summarize_targetdataset_health
from metadata.ingestion.import_service import import_metadata_for_datasets
from metadata.models import (
  SourceDataset, System, TargetDataset, TargetDatasetInput, TargetColumn,)
from metadata.rendering.dialects import get_active_dialect
from metadata.rendering.sql_service import (
  render_preview_sql,
  render_merge_sql,
  render_delete_detection_sql,
)
from metadata.services.lineage_analysis import collect_upstream_targets_extra, collect_downstream_targets_extra

import logging
logger = logging.getLogger(__name__)


def _render_sql_ok(sql: str) -> HttpResponse:
  html = render_to_string(
    "metadata/partials/_sql_preview_block.html",
    {"ok": True, "sql": sql},
  )
  return HttpResponse(html)


def _render_sql_error(prefix: str, exc: Exception) -> HttpResponse:
  logger.exception("%s: %s", prefix, exc)
  html = render_to_string(
    "metadata/partials/_sql_preview_block.html",
    {"ok": False, "prefix": prefix, "message": str(exc)},
  )
  # SQL preview is a best-effort panel; errors are shown inline without failing the page.
  return HttpResponse(html, status=200)


def make_crud_view(model):
  """Dynamically create a CRUD view for a given model."""
  return type(
    f"{model.__name__}CRUDView",
    (GenericCRUDView,),
    {
      "model": model,
      "template_list": "generic/list.html",
      "template_form": "generic/form.html",
      "template_confirm_delete": "generic/confirm_delete.html",
    },
  )


# Dynamically make all models in metadata App CRUD views
metadata_models = apps.get_app_config("metadata").get_models()
globals().update({
  f"{model.__name__}CRUDView": make_crud_view(model)
  for model in metadata_models
})

# ensure CRUD views use the validated forms
TargetColumnCRUDView.form_class = TargetColumnForm  # type: ignore[attr-defined]
TargetDatasetCRUDView.form_class = TargetDatasetForm  # type: ignore[attr-defined]

# helper
def _is_htmx(request):
  return request.headers.get("HX-Request") == "true"

@login_required
@permission_required("metadata.change_sourcedataset", raise_exception=True)
@require_POST
def import_dataset_metadata(request, pk: int):
  ds = get_object_or_404(SourceDataset.objects.select_related("source_system"), pk=pk)
  if not ds.source_system.is_source:
    raise Http404("Metadata import is only allowed for source systems.")
  autointegrate_pk = request.POST.get("autointegrate_pk", "on") == "on"
  reset_flags = request.POST.get("reset_flags") == "on"

  qs = SourceDataset.objects.filter(pk=ds.pk)
  if not qs.exists():
    ctx = {"scope": "dataset", "dataset": ds, "empty": True, "result": {"datasets": 0, "columns_imported": 0}}
    return render(request, "metadata/partials/_import_result.html", ctx) if _is_htmx(request) \
      else HttpResponseBadRequest("Dataset not marked for metadata import.")

  try:
    result = import_metadata_for_datasets(qs, autointegrate_pk=autointegrate_pk, reset_flags=reset_flags)
    ctx = {"scope": "dataset", "dataset": ds, "result": result}
    return render(request, "metadata/partials/_import_result.html", ctx) if _is_htmx(request) \
      else JsonResponse({"ok": True, "result": result})

  except (SQLAlchemyError, NotImplementedError, ValueError, RuntimeError) as e:
    # typical connection/dialect/unsupported-type issues
    ctx = {
      "scope": "dataset", "dataset": ds,
      "error": str(e),
      "debug": traceback.format_exc(limit=2),
    }
    return render(request, "metadata/partials/_import_result_error.html", ctx) if _is_htmx(request) \
      else JsonResponse({"ok": False, "error": str(e)}, status=502)

  except Exception as e:
    ctx = {
      "scope": "dataset", "dataset": ds,
      "error": f"Unexpected error: {e}",
      "debug": traceback.format_exc(limit=2),
    }
    return render(request, "metadata/partials/_import_result_error.html", ctx) if _is_htmx(request) \
      else JsonResponse({"ok": False, "error": str(e)}, status=500)


@login_required
@permission_required("metadata.change_sourcedataset", raise_exception=True)
@require_POST
def import_system_metadata(request, pk: int):
  system = get_object_or_404(System, pk=pk, is_source=True)
  autointegrate_pk = request.POST.get("autointegrate_pk", "on") == "on"
  reset_flags = request.POST.get("reset_flags") == "on"

  qs = SourceDataset.objects.filter(source_system=system)
  if not qs.exists():
    ctx = {"scope": "system", "system": system, "empty": True, "result": {"datasets": 0, "columns_imported": 0}}
    return render(request, "metadata/partials/_import_result.html", ctx) if _is_htmx(request) \
      else HttpResponseBadRequest("No datasets on this system are stored.")

  try:
    result = import_metadata_for_datasets(qs, autointegrate_pk=autointegrate_pk, reset_flags=reset_flags)
    ctx = {"scope": "system", "system": system, "result": result}
    return render(request, "metadata/partials/_import_result.html", ctx) if _is_htmx(request) \
      else JsonResponse({"ok": True, "result": result})

  except (SQLAlchemyError, NotImplementedError, ValueError, RuntimeError) as e:
    ctx = {
      "scope": "system", "system": system,
      "error": str(e),
      "debug": traceback.format_exc(limit=2),
    }
    return render(request, "metadata/partials/_import_result_error.html", ctx) if _is_htmx(request) \
      else JsonResponse({"ok": False, "error": str(e)}, status=502)

  except Exception as e:
    ctx = {
      "scope": "system", "system": system,
      "error": f"Unexpected error: {e}",
      "debug": traceback.format_exc(limit=2),
    }
    return render(request, "metadata/partials/_import_result_error.html", ctx) if _is_htmx(request) \
      else JsonResponse({"ok": False, "error": str(e)}, status=500)


@login_required
def source_type_hint(request):
  """
  Returns a tiny HTML snippet as a hint for the selected source-system type.
  If no type is selected yet, returns an empty string.
  """
  code = (request.GET.get("type") or "").strip().lower()
  if not code:
    return HttpResponse("")  # nothing yet

  text = DIALECT_HINTS.get(code) or DIALECT_HINTS.get("default", "No specific notes.")
  docs_url = getattr(settings, "DOCS_BACKENDS_URL", "https://github.com/elevata-labs/elevata/blob/main/docs/source_backends.md")
  html = (
    f'<div class="small text-muted">'
    f'{text} '
    f'<a href="{docs_url}" target="_blank" rel="noopener">Docs</a>'
    f'</div>'
  )

  return HttpResponse(html, content_type="text/html")


@login_required
@permission_required("metadata.change_targetdataset", raise_exception=True)
@require_POST
def generate_targets(request):
  """
  Trigger target generation via the management command.

  Returns a small HTML alert snippet (for HTMX) with a short summary.
  """
  buffer = StringIO()

  try:
    # Run the management command; output is captured in buffer
    call_command("generate_targets", stdout=buffer)

    raw_output = buffer.getvalue().strip()

    # Strip ANSI color codes (e.g. \x1b[32;1m ... \x1b[0m)
    ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
    output = ansi_escape.sub("", raw_output)

    total_datasets = None

    # Try to find the "Done. Total: X target datasets ..." line from the command
    if output:
      for line in output.splitlines():
        line = line.strip()
        if line.startswith("Done. Total:"):
          # Expected format:
          # "Done. Total: X target datasets and Y target columns generated/updated."
          parts = line.split()
          # parts[2] should be X (number of datasets)
          if len(parts) >= 3:
            try:
              total_datasets = int(parts[2])
            except ValueError:
              pass

    if total_datasets is not None:
      msg = f"Generated {total_datasets} target datasets."
    else:
      # Fallback: show last (clean) line of command output or a generic message
      if output:
        msg = output.splitlines()[-1]
      else:
        msg = "Target generation completed."

    return HttpResponse(
      '<div class="alert alert-success py-1 px-2 mb-0 small">'
      f'{msg}'
      '</div>'
    )

  except Exception as e:
    return HttpResponse(
      '<div class="alert alert-danger py-1 px-2 mb-0 small">'
      f'Generation failed: {e}'
      '</div>',
      status=500,
    )
  
@login_required
@permission_required("metadata.view_targetdataset", raise_exception=True)
def targetdataset_sql_preview(request, pk: int):
  dataset = get_object_or_404(TargetDataset, pk=pk)

  dialect_name = request.GET.get("dialect") or None
  dialect = get_active_dialect(dialect_name)

  try:
    sql = render_preview_sql(dataset, dialect)
    return _render_sql_ok(sql)
  except Exception as e:
    return _render_sql_error("SQL preview failed", e)
  

@login_required
@permission_required("metadata.view_targetdataset", raise_exception=True)
def targetdataset_merge_sql_preview(request, pk):
  dataset = get_object_or_404(TargetDataset, pk=pk)

  dialect_name = request.GET.get("dialect") or None
  dialect = get_active_dialect(dialect_name)

  try:
    sql = render_merge_sql(dataset, dialect, presentation=True)
    return _render_sql_ok(sql)
  except Exception as e:
    return _render_sql_error("SQL preview failed", e)


@login_required
@permission_required("metadata.view_targetdataset", raise_exception=True)
def targetdataset_delete_sql_preview(request, pk):
  dataset = get_object_or_404(TargetDataset, pk=pk)

  dialect_name = request.GET.get("dialect") or None
  dialect = get_active_dialect(dialect_name)

  try:
    sql = render_delete_detection_sql(dataset, dialect, presentation=True)
    return _render_sql_ok(sql)
  except Exception as e:
    return _render_sql_error("SQL preview failed", e)


@login_required
@permission_required("metadata.view_targetdataset", raise_exception=True)
def targetdataset_lineage(request, pk):
  """
  Read-only lineage overview for a single TargetDataset.

  Shows:
    - Upstream inputs (source datasets + upstream target datasets)
    - Downstream datasets that use this dataset as upstream
    - Incoming / outgoing semantic references (FK-style relationships)
  """
  dataset = get_object_or_404(
    TargetDataset.objects.select_related("target_schema"),
    pk=pk,
  )

  depth = int(request.GET.get("depth") or 0)
  depth = max(0, min(depth, 6))  # depth here = extra depth beyond direct

  show_inactive = request.GET.get("show_inactive") == "1"

  upstream_qs = (
    dataset.input_links
    .select_related("source_dataset", "upstream_target_dataset")
    .order_by("role", "id")
  )
  if not show_inactive:
    upstream_qs = upstream_qs.filter(active=True)

  upstream_inputs = upstream_qs

  # Downstream: other targets that use this dataset as input (via upstream_target_dataset)
  downstream_inputs = (
    TargetDatasetInput.objects
    .select_related("target_dataset")
    .filter(upstream_target_dataset=dataset)
    .order_by("target_dataset__target_dataset_name")
  )

  upstream_transitive = {}
  downstream_transitive = {}
  upstream_levels_desc = []
  downstream_levels_desc = []
  reverse_count = 0
  impact_count = 0

  if depth > 0:
    upstream_transitive = collect_upstream_targets_extra(dataset, depth)
    downstream_transitive = collect_downstream_targets_extra(dataset, depth)

    upstream_levels_desc = sorted(upstream_transitive.items(), key=lambda x: x[0], reverse=True)
    downstream_levels_desc = sorted(downstream_transitive.items(), key=lambda x: x[0], reverse=True)

    reverse_count = sum(len(v) for v in upstream_transitive.values())
    impact_count = sum(len(v) for v in downstream_transitive.values())

  # Semantic references (FK-style)
  incoming_refs = (
    dataset.incoming_references
    .select_related("referencing_dataset")
    .order_by("referencing_dataset__target_dataset_name")
  )
  outgoing_refs = (
    dataset.outgoing_references
    .select_related("referenced_dataset")
    .order_by("referenced_dataset__target_dataset_name")
  )

  # Effective materialization (schema default + override)
  if hasattr(dataset, "effective_materialization_type"):
    eff_attr = getattr(dataset, "effective_materialization_type")
    effective_mat = eff_attr() if callable(eff_attr) else eff_attr
  else:
    effective_mat = getattr(dataset, "materialization_type", None) or getattr(
      dataset.target_schema, "default_materialization_type", "table"
    )

  health_level, health_messages = summarize_targetdataset_health(dataset)

  # --- Impact counts (for header badge) ---
  reverse_count = 0
  impact_count = 0
  if depth > 0:
    reverse_count = sum(len(v) for v in upstream_transitive.values())
    impact_count = sum(len(v) for v in downstream_transitive.values())

  context = {
    "object": dataset,
    "title": f"Lineage for {dataset.target_dataset_name}",
    "upstream_inputs": upstream_inputs,
    "downstream_inputs": downstream_inputs,
    "upstream_levels_desc": upstream_levels_desc,
    "downstream_levels_desc": downstream_levels_desc,
    "incoming_refs": incoming_refs,
    "outgoing_refs": outgoing_refs,
    "effective_materialization": effective_mat,
    "health_level": health_level,
    "health_messages": health_messages,
    "depth_options": [0, 1, 2, 3],
    "depth": depth,
    "reverse_count": reverse_count,
    "impact_count": impact_count,
  }

  return render(request, "metadata/lineage/targetdataset_lineage.html", context)

@require_GET
def targetcolumn_upstream_meta(request):
  """
  Returns datatype metadata for a given upstream TargetColumn.
  GET params:
    - upstream_column_id
  """
  col_id = request.GET.get("upstream_column_id")
  if not col_id:
    return JsonResponse({"error": "missing upstream_column_id"}, status=400)

  try:
    col = TargetColumn.objects.get(pk=col_id)
  except TargetColumn.DoesNotExist:
    return JsonResponse({"error": "not found"}, status=404)

  return JsonResponse({
    "datatype": col.datatype or "",
    "max_length": col.max_length,
    "decimal_precision": col.decimal_precision,
    "decimal_scale": col.decimal_scale,
    "nullable": col.nullable,
  })


@login_required
@permission_required("metadata.view_targetdataset", raise_exception=True)
def targetdataset_query_contract_view(request, pk: int):
  td = get_object_or_404(TargetDataset, pk=pk)
  query_root = getattr(td, "query_root", None)
  query_head = getattr(td, "query_head", None) or query_root
  cr = infer_query_node_contract(query_head) if query_head else None
  gov = analyze_query_governance(query_head) if query_head else None
  diff = compute_contract_diff(query_head) if query_head else None

  ctx = {
    "title": "Query contract",
    "object": td,
    "has_query_root": bool(query_root),
    "query_root": query_root,
    "query_head": query_head,
    "contract_columns": (cr.columns if cr else []),
    "contract_issues": (cr.issues if cr else []),
    "governance": gov,
    "contract_diff": diff,
  }
  return render(request, "metadata/query/targetdataset_query_contract.html", ctx)


@login_required
@permission_required("metadata.view_targetdataset", raise_exception=True)
def targetdataset_query_contract_json(request, pk: int):
  td = get_object_or_404(TargetDataset, pk=pk)
  query_root = getattr(td, "query_root", None)
  query_head = getattr(td, "query_head", None) or query_root
  if not query_head:
    return JsonResponse({
      "ok": True,
      "has_query_root": False,
      "columns": [],
      "issues": [],
      "governance": analyze_query_governance(None),
    })  

  cr = infer_query_node_contract(query_head)
  return JsonResponse({
    "ok": True,
    "has_query_root": True,
    "query_root_id": query_root.id if query_root else None,
    "query_head_id": query_head.id if query_head else None,
    "columns": cr.columns,
    "issues": cr.issues,
    "governance": analyze_query_governance(query_head),    
  })


def targetdataset_query_tree_view(request, pk: int):
  td = get_object_or_404(TargetDataset, pk=pk)
  query_root = getattr(td, "query_root", None)
  query_head = getattr(td, "query_head", None) or query_root
  gov = analyze_query_governance(query_head)
  cr = infer_query_node_contract(query_head) if query_head else None  

  nodes = []
  edges = []

  if query_head:
    q = deque([(query_head, 0)])
    seen = set()

    while q:
      n, lvl = q.popleft()
      nid = int(getattr(n, "id", 0) or 0)
      if not nid or nid in seen:
        continue
      seen.add(nid)

      ntype = (getattr(n, "node_type", "") or "").strip().lower()
      name = (getattr(n, "name", "") or "").strip()
      label = name or f"node:{nid}"

      child_ids = []
      summary = ""

      if ntype == "select":
        summary = "Base select"

      elif ntype == "aggregate":
        agg = getattr(n, "aggregate", None)
        if agg and getattr(agg, "input_node", None):
          inp = agg.input_node
          cid = int(getattr(inp, "id", 0) or 0)
          if cid:
            child_ids.append(cid)
            edges.append((nid, cid))
            q.append((inp, lvl + 1))
        gk = agg.group_keys.count() if agg else 0
        ms = agg.measures.count() if agg else 0
        summary = f"group_keys={gk}, measures={ms}"

      elif ntype == "union":
        un = getattr(n, "union", None)
        branch_cnt = un.branches.count() if un else 0
        out_cnt = un.output_columns.count() if un else 0
        summary = f"branches={branch_cnt}, output_cols={out_cnt}"
        if un:
          for b in un.branches.all().order_by("ordinal_position", "id"):
            inp = getattr(b, "input_node", None)
            cid = int(getattr(inp, "id", 0) or 0) if inp else 0
            if cid:
              child_ids.append(cid)
              edges.append((nid, cid))
              q.append((inp, lvl + 1))

      elif ntype == "window":
        w = getattr(n, "window", None)
        col_cnt = w.columns.count() if w else 0
        summary = f"window_cols={col_cnt}"
        if w and getattr(w, "input_node", None):
          inp = w.input_node
          cid = int(getattr(inp, "id", 0) or 0)
          if cid:
            child_ids.append(cid)
            edges.append((nid, cid))
            q.append((inp, lvl + 1))

      nodes.append({
        "id": nid,
        "level": lvl,
        "type": ntype,
        "label": label,
        "summary": summary,
        "children": child_ids,
      })

  ctx = {
    "title": "Query tree",
    "object": td,
    "has_query_root": bool(query_root),
    "query_root": query_root,
    "query_head": query_head,
    "has_query_head": bool(query_head),
    "nodes": sorted(nodes, key=lambda x: (x["level"], x["id"])),
    "edges": edges,
    "contract_columns": (cr.columns if cr else []),
    "contract_issues": (cr.issues if cr else []),
    "governance": gov,
  }
  return render(request, "metadata/query/targetdataset_query_tree.html", ctx)

@login_required
@permission_required("metadata.view_targetdataset", raise_exception=True)
def targetdataset_query_builder(request, pk: int):
  """
  Guided hub for query-related configuration.
  Read-only for now: shows status, governance, contract, and next steps.
  """
  td = get_object_or_404(TargetDataset, pk=pk)

  schema_short = getattr(getattr(td, "target_schema", None), "short_name", "") or ""
  is_query_schema = schema_short in ("bizcore", "serving")

  query_root = getattr(td, "query_root", None)
  query_head = getattr(td, "query_head", None) or query_root
  gov = analyze_query_governance(query_head) if query_head else None
  cr = infer_query_node_contract(query_head) if query_head else None
  diff = compute_contract_diff(query_head) if query_head else None

  OrderByExpression = apps.get_model("metadata", "OrderByExpression")
  PartitionByExpression = apps.get_model("metadata", "PartitionByExpression")

  ctx = {
    "title": "Query builder",
    "object": td,
    "schema_short": schema_short,
    "is_query_schema": is_query_schema,
    "has_query_root": bool(query_root),
    "query_root": query_root,
    "query_head": query_head,
    "has_query_head": bool(query_head),
    "governance": gov,
    "contract_columns": (cr.columns if cr else []),
    "contract_issues": (cr.issues if cr else []),
    "contract_diff": diff,
    "order_by_def_count": OrderByExpression.objects.filter(target_dataset=td).count(),
    "partition_by_def_count": PartitionByExpression.objects.filter(target_dataset=td).count(),
  }
  return render(request, "metadata/query/targetdataset_query_builder.html", ctx)

@login_required
@permission_required("metadata.change_targetdataset", raise_exception=True)
@require_POST
def targetdataset_create_query_root(request, pk: int):
  td = get_object_or_404(TargetDataset, pk=pk)

  # Only bizcore/serving may define custom query logic
  if not query_tree_allowed_for_dataset(td):
    messages.error(request, "Custom query logic is only allowed in bizcore/serving.")
    return redirect("targetdataset_query_builder", td.pk)

  # Block contract-changing actions if downstream depends on this dataset
  if not query_tree_mutations_allowed_for_dataset(td):
    messages.error(request, query_tree_mutation_block_reason(td))
    return redirect("targetdataset_query_builder", pk=td.pk)

  if getattr(td, "query_root", None):
    return redirect("targetdataset_query_builder", pk=td.pk)

  QueryNode = apps.get_model("metadata", "QueryNode")
  QuerySelectNode = apps.get_model("metadata", "QuerySelectNode")

  with transaction.atomic():
    root = QueryNode.objects.create(
      target_dataset=td,
      node_type="select",
      name="Base select",
      active=True,
    )
    QuerySelectNode.objects.create(
      node=root,
      use_dataset_definition=True,
    )
    td.query_root = root
    td.query_head = root
    td.save(update_fields=["query_root", "query_head"])

  return redirect("targetdataset_query_builder", pk=td.pk)

@login_required
@require_POST
def targetdataset_reset_query_root(request, pk: int):
  """
  Disable custom query logic for a dataset:
  - sets query_root to NULL
  - deletes all query nodes/operators owned by this dataset
  """
  td = get_object_or_404(TargetDataset, pk=pk)

  if not query_tree_allowed_for_dataset(td):
    messages.error(request, "Custom query logic is only allowed in bizcore/serving.")
    return redirect("targetdataset_query_builder", td.pk)
  
  if not query_tree_mutations_allowed_for_dataset(td):
    messages.error(request, query_tree_mutation_block_reason(td))
    return redirect("targetdataset_query_builder", pk=td.pk)

  root = getattr(td, "query_root", None)
  if not root:
    messages.info(request, "No custom query logic configured.")
    return redirect("targetdataset_query_builder", pk=td.pk)

  QueryNode = apps.get_model("metadata", "QueryNode")
  QueryAggregateNode = apps.get_model("metadata", "QueryAggregateNode")
  QueryUnionBranch = apps.get_model("metadata", "QueryUnionBranch")
  QueryWindowNode = apps.get_model("metadata", "QueryWindowNode")

  with transaction.atomic():
    # 1) detach root first (so builder switches back immediately)
    td.query_root = None
    td.query_head = None
    td.save(update_fields=["query_root", "query_head"])

    # 2) delete query graph owned by this dataset
    # PROTECT FKs between nodes mean we must delete "consumers" first.
    # We do a simple leaf-stripping loop: delete nodes that are not referenced as an input.
    remaining = QueryNode.objects.filter(target_dataset=td)

    # safety valve to avoid infinite loops
    max_iters = 1000
    iters = 0
    while remaining.exists():
      iters += 1
      if iters > max_iters:
        raise RuntimeError("Reset failed: exceeded iteration limit while deleting query graph.")

      referenced_ids = set()
      referenced_ids.update(
        QueryAggregateNode.objects.filter(node__target_dataset=td)
        .exclude(input_node_id__isnull=True)
        .values_list("input_node_id", flat=True)
      )
      referenced_ids.update(
        QueryUnionBranch.objects.filter(union_node__node__target_dataset=td)
        .exclude(input_node_id__isnull=True)
        .values_list("input_node_id", flat=True)
      )
      referenced_ids.update(
        QueryWindowNode.objects.filter(node__target_dataset=td)
        .exclude(input_node_id__isnull=True)
        .values_list("input_node_id", flat=True)
      )

      deletable = remaining.exclude(id__in=referenced_ids)
      if not deletable.exists():
        # This should not happen if validators prevent cycles/shared refs.
        raise RuntimeError(
          "Reset failed: query graph contains a cycle or shared references that prevent safe deletion."
        )
      deletable.delete()
      remaining = QueryNode.objects.filter(target_dataset=td)

  messages.success(request, "Custom query logic has been disabled and reset.")
  return redirect("targetdataset_query_builder", pk=td.pk)

@login_required
@permission_required("metadata.change_targetdataset", raise_exception=True)
@require_POST
def targetdataset_add_window_node(request, pk: int) -> HttpResponse:
  td = get_object_or_404(TargetDataset, pk=pk)

  # Only bizcore/serving may define custom query logic
  if not query_tree_allowed_for_dataset(td):
    messages.error(request, "Custom query logic is only allowed in bizcore/serving.")
    return redirect("targetdataset_query_builder", td.pk)

  # Block contract-changing actions if downstream depends on this dataset
  if not query_tree_mutations_allowed_for_dataset(td):
    messages.error(request, query_tree_mutation_block_reason(td))
    return redirect("targetdataset_query_builder", pk=td.pk)

  query_root = getattr(td, "query_root", None)
  query_head = getattr(td, "query_head", None) or query_root
  if not query_root:
    return HttpResponseBadRequest("No query root configured. Enable custom query logic first.")

  # Resolve models lazily (avoids import tangles)
  QueryNode = apps.get_model("metadata", "QueryNode")
  QueryWindowNode = apps.get_model("metadata", "QueryWindowNode")
  QueryWindowColumn = apps.get_model("metadata", "QueryWindowColumn")

  # Wrap existing root: new window node becomes the new root
  new_node = QueryNode.objects.create(
    target_dataset=td,
    node_type="window",
    name="Window",
    active=True,
  )

  w = QueryWindowNode.objects.create(
    node=new_node,
    input_node=query_head,
  )

  # Create one default output column so the tree is immediately "valid-ish"
  # (validator will only WARN about missing ORDER BY, not ERROR about missing columns)
  QueryWindowColumn.objects.create(
    window_node=w,
    output_name="row_number",
    function="ROW_NUMBER",
    ordinal_position=1,
    active=True,
  )

  td.query_head = new_node
  td.save(update_fields=["query_head"])

  # After creating the node, jump straight into the operator-specific editor.
  # For window nodes, the best next step is configuring window columns.
  try:
    messages.success(
      request,
      "Window node created with a default row_number column. Next, configure window columns and ordering "
      "(or delete row_number if you don’t need it)."
    )
  except Exception:
    pass

  return redirect("querywindowcolumn_list", parent_pk=w.pk)


@login_required
@permission_required("metadata.change_targetdataset", raise_exception=True)
@require_POST
def targetdataset_add_aggregate_node(request, pk: int) -> HttpResponse:
  td = get_object_or_404(TargetDataset, pk=pk)

  # Only bizcore/serving may define custom query logic
  if not query_tree_allowed_for_dataset(td):
    messages.error(request, "Custom query logic is only allowed in bizcore/serving.")
    return redirect("targetdataset_query_builder", td.pk)

  if not query_tree_mutations_allowed_for_dataset(td):
    messages.error(request, query_tree_mutation_block_reason(td))
    return redirect("targetdataset_query_builder", pk=td.pk)

  query_root = getattr(td, "query_root", None)
  query_head = getattr(td, "query_head", None) or query_root
  if not query_root:
    return HttpResponseBadRequest("No query root configured. Enable custom query logic first.")
  
  # Resolve models lazily (avoids import tangles)
  QueryNode = apps.get_model("metadata", "QueryNode")
  QueryAggregateNode = apps.get_model("metadata", "QueryAggregateNode")

  # Wrap existing root: new aggregate node becomes the new root
  new_node = QueryNode.objects.create(
    target_dataset=td,
    node_type="aggregate",
    name="Aggregate",
    active=True,
  )

  agg = QueryAggregateNode.objects.create(node=new_node, input_node=query_head)

  # IMPORTANT: root stays stable; head moves.
  td.query_head = new_node
  td.save(update_fields=["query_head"])

  try:
    messages.success(
      request,
      "Aggregate node created. Next, add measures (and optional group keys) in the scoped editor."
    )
  except Exception:
    pass

  # Jump to measures (most common next step)
  return redirect("queryaggregatemeasure_list", parent_pk=agg.pk)


@login_required
@permission_required("metadata.change_targetdataset", raise_exception=True)
@require_POST
def targetdataset_add_union_node(request, pk: int) -> HttpResponse:
  td = get_object_or_404(TargetDataset, pk=pk)

  # Only bizcore/serving may define custom query logic
  if not query_tree_allowed_for_dataset(td):
    messages.error(request, "Custom query logic is only allowed in bizcore/serving.")
    return redirect("targetdataset_query_builder", td.pk)

  if not query_tree_mutations_allowed_for_dataset(td):
    messages.error(request, query_tree_mutation_block_reason(td))
    return redirect("targetdataset_query_builder", pk=td.pk)

  query_root = getattr(td, "query_root", None)
  query_head = getattr(td, "query_head", None) or query_root
  if not query_root:
    return HttpResponseBadRequest("No query root configured. Enable custom query logic first.")

  # Resolve models lazily (avoids import tangles)
  QueryNode = apps.get_model("metadata", "QueryNode")
  QueryUnionNode = apps.get_model("metadata", "QueryUnionNode")
  QueryUnionBranch = apps.get_model("metadata", "QueryUnionBranch")

  # Wrap existing root: new union node becomes the new root
  new_node = QueryNode.objects.create(
    target_dataset=td,
    node_type="union",
    name="UNION",
    active=True,
  )

  # Default to UNION ALL (explicitly)
  un = QueryUnionNode.objects.create(
    node=new_node,
    mode="union_all",
  )

  # Create a first branch pointing to the previous root so the union is not empty.
  QueryUnionBranch.objects.create(
    union_node=un,
    input_node=query_head,
    ordinal_position=1,
  )

  td.query_head = new_node
  td.save(update_fields=["query_head"])

  try:
    messages.success(
      request,
      "UNION node created (UNION ALL). Next, define output columns and add additional branches."
    )
  except Exception:
    pass

  # Jump to output columns (users need this early)
  return redirect("queryunionoutputcolumn_list", parent_pk=un.pk)
