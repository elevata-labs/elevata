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
from metadata.architecture.catalog_data_products import (
  build_architecture_catalog_data_products_context,
)
from metadata.architecture.catalog_insights import (
  build_architecture_catalog_insights_context,
)
from metadata.architecture.catalog_map import (
  build_architecture_catalog_map_context,
)
from metadata.architecture.catalog_portfolio import (
  build_architecture_catalog_portfolio_context,
)
from metadata.models import TargetDataset
from metadata.services.quality_review import (
  build_quality_review,
)
from metadata.services.reference_integrity_review import (
  build_reference_integrity_review,
)


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
def architecture_catalog_portfolio(request):
  """
  Render the read-only Architecture Catalog Portfolio page.
  """
  context = build_architecture_catalog_portfolio_context()
  return render(
    request,
    "metadata/architecture/architecture_catalog_portfolio.html",
    context,
  )


@login_required
@permission_required("metadata.view_targetdataset", raise_exception=True)
def architecture_catalog_data_products(request):
  """
  Render the read-only Architecture Catalog Data Products page.
  """
  context = build_architecture_catalog_data_products_context(request.GET)
  return render(
    request,
    "metadata/architecture/architecture_catalog_data_products.html",
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
def architecture_catalog_map(request):
  """
  Render the read-only Architecture Catalog Map page.
  """
  context = build_architecture_catalog_map_context()
  return render(
    request,
    "metadata/architecture/architecture_catalog_map.html",
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


def _quality_review_example_limit(request) -> int:
  """Return a bounded example limit for UI-triggered quality reviews."""
  raw_value = (request.GET.get("limit") or "").strip()
  try:
    value = int(raw_value)
  except (TypeError, ValueError):
    value = 20

  return min(max(value, 1), 100)


@login_required
@permission_required("metadata.view_targetdataset", raise_exception=True)
def architecture_catalog_quality_review(request, pk: int):
  """
  Render the on-demand Architecture Quality Review panel for one TargetDataset.
  """
  target_dataset = get_object_or_404(TargetDataset, pk=pk)
  example_limit = _quality_review_example_limit(request)
  include_sql = (request.GET.get("include_sql") or "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
  }

  review = None
  review_error = ""

  try:
    review = build_quality_review(
      target_dataset,
      example_limit=example_limit,
      include_sql=include_sql,
    )
  except Exception as exc:
    review_error = str(exc)

  return render(
    request,
    "metadata/partials/_quality_review.html",
    {
      "target_dataset": target_dataset,
      "review": review,
      "review_error": review_error,
      "example_limit": example_limit,
    },
  )


def _reference_integrity_example_limit(request) -> int:
  """Return a bounded missing-parent example limit for UI-triggered reviews."""
  raw_value = (request.GET.get("limit") or "").strip()
  try:
    value = int(raw_value)
  except (TypeError, ValueError):
    value = 20

  return min(max(value, 1), 100)


@login_required
@permission_required("metadata.view_targetdataset", raise_exception=True)
def architecture_catalog_reference_integrity(request, pk: int):
  """
  Render the on-demand Reference Integrity Review panel for one TargetDataset.
  """
  target_dataset = get_object_or_404(TargetDataset, pk=pk)
  example_limit = _reference_integrity_example_limit(request)
  include_sql = (request.GET.get("include_sql") or "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
  }

  review = None
  review_error = ""

  try:
    review = build_reference_integrity_review(
      target_dataset,
      example_limit=example_limit,
      include_sql=include_sql,
    )
  except Exception as exc:
    review_error = str(exc)

  return render(
    request,
    "metadata/partials/_reference_integrity_review.html",
    {
      "target_dataset": target_dataset,
      "review": review,
      "review_error": review_error,
      "example_limit": example_limit,
    },
  )
