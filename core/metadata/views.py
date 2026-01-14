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

from django.core.management import call_command
from django.apps import apps
from django.conf import settings
from django.http import JsonResponse, HttpResponse, HttpResponseBadRequest, Http404
from django.contrib.auth.decorators import login_required, permission_required
from django.core.management import call_command
from django.utils.html import escape
from django.views.decorators.http import require_POST, require_GET
from django.shortcuts import get_object_or_404, render
from sqlalchemy.exc import SQLAlchemyError

from generic import GenericCRUDView

from metadata.constants import DIALECT_HINTS
from metadata.forms import TargetColumnForm, TargetDatasetForm
from metadata.generation.validators import summarize_targetdataset_health
from metadata.ingestion.import_service import import_metadata_for_datasets
from metadata.models import (
  SourceDataset, System, TargetDataset, TargetDatasetInput, TargetColumn,
  TargetDatasetJoin, TargetDatasetJoinPredicate,
)
from metadata.rendering.dialects import get_active_dialect
from metadata.rendering.sql_service import (
  render_preview_sql,
  render_merge_sql,
  render_delete_detection_sql,
)
from metadata.services.lineage_analysis import collect_upstream_targets_extra, collect_downstream_targets_extra


def _render_sql_ok(sql: str) -> HttpResponse:
  return HttpResponse(
    '<div class="alert alert-sql py-1 px-2 mb-0 small">'
    '<pre class="mb-0" style="white-space: pre-wrap;">'
    f'{sql}'
    '</pre>'
    '</div>'
  )

import logging
logger = logging.getLogger(__name__)

def _render_sql_error(prefix: str, exc: Exception) -> HttpResponse:
  logger.exception("%s: %s", prefix, exc)
  return HttpResponse(
    f'<div class="alert alert-danger py-1 px-2 mb-0 small">'
    f'{prefix}: {escape(str(exc))}'
    f'</div>',
    status=500,
  )


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
