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

from django.contrib.auth.decorators import login_required, permission_required
from django.shortcuts import get_object_or_404, render

from metadata.architecture.catalog import (
  build_architecture_catalog_context,
  build_architecture_catalog_detail_context,
)
from metadata.architecture.catalog_insights import (
  build_architecture_catalog_insights_context,
)
from metadata.models import TargetDataset


@login_required
@permission_required("metadata.view_targetdataset", raise_exception=True)
def architecture_catalog(request):
  """
  Render the read-only Architecture Catalog workspace.
  """
  context = build_architecture_catalog_context(request.GET)
  return render(
    request,
    "metadata/architecture/architecture_catalog.html",
    context,
  )


@login_required
@permission_required("metadata.view_targetdataset", raise_exception=True)
def architecture_catalog_insights(request):
  """
  Render the read-only Architecture Catalog Insights page.
  """
  context = build_architecture_catalog_insights_context()
  return render(
    request,
    "metadata/architecture/architecture_catalog_insights.html",
    context,
  )


@login_required
@permission_required("metadata.view_targetdataset", raise_exception=True)
def architecture_catalog_detail(request, pk: int):
  """
  Render the read-only Architecture Catalog detail page for one TargetDataset.
  """
  target_dataset = get_object_or_404(TargetDataset, pk=pk)
  context = build_architecture_catalog_detail_context(target_dataset)
  return render(
    request,
    "metadata/architecture/architecture_catalog_detail.html",
    context,
  )