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

import json
from django.shortcuts import get_object_or_404, render
from django.http import JsonResponse
from django.views.decorators.http import require_POST
from django.contrib.auth.decorators import login_required

from metadata.models import TargetColumn, TargetDataset
from metadata.services.rename_targetdataset import dry_run_targetdataset_rename, commit_targetdataset_rename
from metadata.services.rename_targetcolumn import dry_run_targetcolumn_rename, commit_targetcolumn_rename

def _is_htmx(request) -> bool:
  """
  Detect HTMX requests to decide whether to return HTML partials or JSON.

  HTMX sets the 'HX-Request' header to 'true' on asynchronous requests.
  """
  return request.headers.get("HX-Request", "").lower() == "true"


@login_required
@require_POST
def targetcolumn_rename(request, pk: int):
  col = get_object_or_404(TargetColumn, pk=pk)

  new_name = (request.POST.get("target_column_name") or "").strip()
  dry_run_flag = (request.POST.get("dry_run", "false").lower() in ("1", "true"))

  if dry_run_flag:
    result = dry_run_targetcolumn_rename(col, new_name)
    return render(
      request,
      "metadata/partials/_targetcolumn_inline_preview.html",
      {
        "ok": result.get("ok", False),
        "errors": result.get("errors", []),
        "impacts": result.get("impacts", {}),
        "col": col,
        "new_name": new_name,
      },
    )

  result = commit_targetcolumn_rename(col, new_name, user=request.user)

  if not result.get("ok"):
    return render(
      request,
      "metadata/partials/_targetcolumn_inline_preview.html",
      {
        "ok": False,
        "errors": result.get("errors", []),
        "impacts": {},
        "col": col,
        "new_name": new_name,
      },
    )

  col.refresh_from_db()
  return render(
    request,
    "metadata/partials/_targetcolumn_inline_cell.html",
    {"col": col},
  )


@login_required
@require_POST
def targetdataset_rename(request, pk: int):
  ds = get_object_or_404(TargetDataset, pk=pk)

  new_name = (request.POST.get("target_dataset_name") or "").strip()
  dry_run_flag = (request.POST.get("dry_run", "false").lower() in ("1", "true"))

  if dry_run_flag:
    result = dry_run_targetdataset_rename(ds, new_name)
    return render(
      request,
      "metadata/partials/_targetdataset_inline_preview.html",
      {
        "ok": result.get("ok", False),
        "errors": result.get("errors", []),
        "impacts": result.get("impacts", {}),
        "ds": ds,
        "new_name": new_name,
      },
    )

  result = commit_targetdataset_rename(ds, new_name, user=request.user)

  if not result.get("ok"):
    return render(
      request,
      "metadata/partials/_targetdataset_inline_preview.html",
      {
        "ok": False,
        "errors": result.get("errors", []),
        "impacts": {},
        "ds": ds,
        "new_name": new_name,
      },
    )

  ds.refresh_from_db()
  return render(
    request,
    "metadata/partials/_targetdataset_inline_cell.html",
    {"ds": ds},
  )
