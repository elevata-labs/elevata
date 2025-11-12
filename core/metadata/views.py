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

import re
from django.core.management import call_command
from django.apps import apps
from django.conf import settings
from generic import GenericCRUDView
from django.http import HttpResponse
from django.contrib.auth.decorators import login_required, permission_required
from django.core.management import call_command
from io import StringIO
from metadata.constants import DIALECT_HINTS

# --- custom import views (HTMX-friendly) ---

from django.contrib.auth.decorators import login_required, permission_required
from django.views.decorators.http import require_POST
from django.shortcuts import get_object_or_404, render
from django.http import JsonResponse, HttpResponseBadRequest
from metadata.models import SourceDataset, SourceSystem, TargetDataset
from metadata.ingestion.import_service import import_metadata_for_datasets
from sqlalchemy.exc import SQLAlchemyError
import traceback

from django.contrib.auth.decorators import permission_required
from django.views.decorators.http import require_POST
from django.shortcuts import get_object_or_404, render
from django.http import JsonResponse, HttpResponseBadRequest
from django.utils.html import escape, conditional_escape
from sqlalchemy.exc import SQLAlchemyError
import traceback

from metadata.generation.target_generation_service import TargetGenerationService
from metadata.generation.security import get_runtime_pepper
from metadata.rendering.preview import build_sql_preview_for_target


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

# helper
def _is_htmx(request):
  return request.headers.get("HX-Request") == "true"

@login_required
@permission_required("metadata.change_sourcedataset", raise_exception=True)
@require_POST
def import_dataset_metadata(request, pk: int):
  ds = get_object_or_404(SourceDataset, pk=pk)
  autointegrate_pk = request.POST.get("autointegrate_pk", "on") == "on"
  reset_flags = request.POST.get("reset_flags") == "on"

  qs = SourceDataset.objects.filter(pk=ds.pk)
  if not qs.exists():
    ctx = {"scope": "dataset", "dataset": ds, "empty": True, "result": {"datasets": 0, "columns_imported": 0}}
    return render(request, "metadata/partials/import_result.html", ctx) if _is_htmx(request) \
      else HttpResponseBadRequest("Dataset not marked for metadata import.")

  try:
    result = import_metadata_for_datasets(qs, autointegrate_pk=autointegrate_pk, reset_flags=reset_flags)
    ctx = {"scope": "dataset", "dataset": ds, "result": result}
    return render(request, "metadata/partials/import_result.html", ctx) if _is_htmx(request) \
      else JsonResponse({"ok": True, "result": result})

  except (SQLAlchemyError, NotImplementedError, ValueError, RuntimeError) as e:
    # typical connection/dialect/unsupported-type issues
    ctx = {
      "scope": "dataset", "dataset": ds,
      "error": str(e),
      "debug": traceback.format_exc(limit=2),
    }
    return render(request, "metadata/partials/import_result_error.html", ctx) if _is_htmx(request) \
      else JsonResponse({"ok": False, "error": str(e)}, status=502)

  except Exception as e:
    ctx = {
      "scope": "dataset", "dataset": ds,
      "error": f"Unexpected error: {e}",
      "debug": traceback.format_exc(limit=2),
    }
    return render(request, "metadata/partials/import_result_error.html", ctx) if _is_htmx(request) \
      else JsonResponse({"ok": False, "error": str(e)}, status=500)


@login_required
@permission_required("metadata.change_sourcedataset", raise_exception=True)
@require_POST
def import_system_metadata(request, pk: int):
  system = get_object_or_404(SourceSystem, pk=pk)
  autointegrate_pk = request.POST.get("autointegrate_pk", "on") == "on"
  reset_flags = request.POST.get("reset_flags") == "on"

  qs = SourceDataset.objects.filter(source_system=system)
  if not qs.exists():
    ctx = {"scope": "system", "system": system, "empty": True, "result": {"datasets": 0, "columns_imported": 0}}
    return render(request, "metadata/partials/import_result.html", ctx) if _is_htmx(request) \
      else HttpResponseBadRequest("No datasets on this system are stored.")

  try:
    result = import_metadata_for_datasets(qs, autointegrate_pk=autointegrate_pk, reset_flags=reset_flags)
    ctx = {"scope": "system", "system": system, "result": result}
    return render(request, "metadata/partials/import_result.html", ctx) if _is_htmx(request) \
      else JsonResponse({"ok": True, "result": result})

  except (SQLAlchemyError, NotImplementedError, ValueError, RuntimeError) as e:
    ctx = {
      "scope": "system", "system": system,
      "error": str(e),
      "debug": traceback.format_exc(limit=2),
    }
    return render(request, "metadata/partials/import_result_error.html", ctx) if _is_htmx(request) \
      else JsonResponse({"ok": False, "error": str(e)}, status=502)

  except Exception as e:
    ctx = {
      "scope": "system", "system": system,
      "error": f"Unexpected error: {e}",
      "debug": traceback.format_exc(limit=2),
    }
    return render(request, "metadata/partials/import_result_error.html", ctx) if _is_htmx(request) \
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
  """
  Build and return a SQL preview snippet for a single TargetDataset.

  Intended to be called via HTMX from the UI.
  """
  dataset = get_object_or_404(TargetDataset, pk=pk)

  try:
    sql = build_sql_preview_for_target(dataset)
    # Basic HTML-escaped <pre><code> block for readability
    return HttpResponse(
      '<div class="alert alert-success py-1 px-2 mb-0 small">'
      '<pre class="mb-0" style="white-space: pre-wrap;">'
      f'{sql}'
      '</pre>'
      '</div>'
    )
  except Exception as e:
    return HttpResponse(
      f'<div class="alert alert-danger py-1 px-2 mb-0 small">'
      f'SQL preview failed: {escape(str(e))}'
      f'</div>',
      status=500,
    )
