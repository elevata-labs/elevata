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

from __future__ import annotations

from types import SimpleNamespace

from django.http import HttpResponse
from django.template.loader import render_to_string
from django.test import RequestFactory

import metadata.architecture.catalog_sources as catalog_sources
import metadata.views_catalog as views_catalog
from metadata.services.source_ingestion_readiness import (
  SourceIngestionReadiness,
  SourceIngestionReadinessSignal,
  build_source_ingestion_readiness,
)


class _Manager:
  def __init__(self, items):
    self._items = list(items)

  def all(self):
    return list(self._items)


class _FailManager:
  def all(self):
    raise AssertionError("Prefetched relation should be used")

  def filter(self, **kwargs):
    raise AssertionError("Prefetched relation should be used")


class _SourceSystem:
  def __init__(
    self,
    pk,
    short_name,
    name,
    source_type,
    include_ingest,
    datasets=(),
    *,
    description="",
    active=True,
  ):
    self.pk = pk
    self.short_name = short_name
    self.name = name
    self.description = description
    self.type = source_type
    self.include_ingest = include_ingest
    self.active = active
    self.is_source = True
    self.generate_raw_tables = True
    self.source_datasets = _Manager(datasets)
    for dataset in datasets:
      dataset.source_system = self


class _SourceDataset:
  def __init__(
    self,
    pk,
    name,
    *,
    schema_name="",
    description="",
    active=True,
    integrate=True,
  ):
    self.pk = pk
    self.schema_name = schema_name
    self.source_dataset_name = name
    self.description = description
    self.active = active
    self.integrate = integrate
    self.source_system = None


def _signal(code, severity, label=None):
  return SourceIngestionReadinessSignal(
    code=code,
    severity=severity,
    label=label or code.replace("_", " ").title(),
    message=f"Message for {code}.",
  )


def _readiness(
  dataset,
  *,
  status,
  ingest_mode,
  signals=(),
  landing_required=True,
  raw_target_keys=(),
  integrated_column_count=2,
):
  return SourceIngestionReadiness(
    dataset_key=(
      f"{dataset.schema_name}.{dataset.source_dataset_name}".strip(".")
    ),
    source_system=dataset.source_system.short_name,
    source_type=dataset.source_system.type,
    source_kind=(
      "rest"
      if dataset.source_system.type == "rest"
      else "relational"
    ),
    metadata_import_mode="automatic",
    landing_required=landing_required,
    ingest_mode=ingest_mode,
    raw_target_keys=tuple(raw_target_keys),
    integrated_column_count=integrated_column_count,
    status=status,
    signals=tuple(signals),
  )


def _fixture_systems():
  customer = _SourceDataset(
    11,
    "customer",
    schema_name="dbo",
    description="Customer master data",
  )
  orders = _SourceDataset(
    12,
    "orders",
    schema_name="sales",
    description="Sales orders",
  )
  products = _SourceDataset(
    21,
    "products",
    description="Product API",
  )
  archive = _SourceDataset(
    22,
    "archive",
    active=False,
  )

  alpha = _SourceSystem(
    1,
    "alpha",
    "Alpha ERP",
    "mssql",
    "native",
    (customer, orders),
  )
  beta = _SourceSystem(
    2,
    "beta",
    "Beta API",
    "rest",
    "external",
    (products, archive),
  )
  empty = _SourceSystem(
    3,
    "empty",
    "Empty Source",
    "csv",
    "native",
    (),
  )

  readiness_by_pk = {
    11: _readiness(
      customer,
      status="ready",
      ingest_mode="native",
      raw_target_keys=("raw.raw_alpha_customer",),
    ),
    12: _readiness(
      orders,
      status="attention",
      ingest_mode="native",
      signals=(
        _signal("raw_target_missing", "blocking", "RAW target missing"),
        _signal("multiple_raw_targets", "warning", "Multiple RAW targets"),
      ),
    ),
    21: _readiness(
      products,
      status="unavailable",
      ingest_mode="external",
      signals=(
        _signal("source_system_missing", "blocking", "Source unavailable"),
      ),
    ),
    22: _readiness(
      archive,
      status="not_applicable",
      ingest_mode="none",
      landing_required=False,
      signals=(
        _signal("dataset_inactive", "info", "Dataset inactive"),
      ),
    ),
  }

  return (alpha, beta, empty), readiness_by_pk


def _patch_reverse(monkeypatch):
  monkeypatch.setattr(
    catalog_sources,
    "reverse",
    lambda name, args=None: f"/{name}/{args[0]}/",
  )


def test_architecture_catalog_sources_groups_readiness_by_source_system(
  monkeypatch,
):
  _patch_reverse(monkeypatch)
  systems, readiness_by_pk = _fixture_systems()

  context = catalog_sources.build_architecture_catalog_sources_context(
    {},
    source_systems=systems,
    readiness_builder=lambda dataset: readiness_by_pk[dataset.pk],
  )

  assert context["total_source_system_count"] == 3
  assert context["filtered_source_system_count"] == 3
  assert context["total_dataset_count"] == 4
  assert context["filtered_dataset_count"] == 4
  assert context["blocking_signal_count"] == 2
  assert context["warning_signal_count"] == 1

  status_counts = {
    item.key: item.count
    for item in context["status_counts"]
  }
  assert status_counts == {
    "ready": 1,
    "attention": 1,
    "not_applicable": 1,
    "unavailable": 1,
  }

  groups = context["source_systems"]
  assert [group.short_name for group in groups] == [
    "beta",
    "alpha",
    "empty",
  ]

  beta = groups[0]
  assert beta.status == "unavailable"
  assert beta.unavailable_count == 1
  assert beta.not_applicable_count == 1
  assert beta.blocking_signal_count == 1
  assert beta.detail_url == "/system_detail/2/"
  assert [dataset.source_dataset_name for dataset in beta.datasets] == [
    "products",
    "archive",
  ]

  alpha = groups[1]
  assert alpha.status == "attention"
  assert alpha.attention_count == 1
  assert alpha.ready_count == 1
  assert alpha.warning_signal_count == 1
  assert [dataset.source_dataset_name for dataset in alpha.datasets] == [
    "orders",
    "customer",
  ]
  assert alpha.datasets[0].detail_url == "/sourcedataset_detail/12/"
  assert alpha.datasets[0].primary_finding.label == "RAW target missing"
  assert alpha.remaining_dataset_count == 1

  assert groups[2].status == "not_applicable"
  assert groups[2].dataset_count == 0
  assert groups[2].total_dataset_count == 0


def test_architecture_catalog_sources_filters_dataset_and_system_dimensions(
  monkeypatch,
):
  _patch_reverse(monkeypatch)
  systems, readiness_by_pk = _fixture_systems()
  builder = lambda dataset: readiness_by_pk[dataset.pk]

  attention = catalog_sources.build_architecture_catalog_sources_context(
    {"status": "attention"},
    source_systems=systems,
    readiness_builder=builder,
  )
  assert attention["filtered_source_system_count"] == 1
  assert attention["filtered_dataset_count"] == 1
  assert attention["source_systems"][0].short_name == "alpha"
  assert attention["source_systems"][0].datasets[0].dataset_key == "sales.orders"

  external = catalog_sources.build_architecture_catalog_sources_context(
    {"ingest_mode": "external"},
    source_systems=systems,
    readiness_builder=builder,
  )
  assert external["filtered_dataset_count"] == 2
  assert external["source_systems"][0].short_name == "beta"
  assert [
    dataset.dataset_key
    for dataset in external["source_systems"][0].datasets
  ] == ["products", "archive"]

  rest = catalog_sources.build_architecture_catalog_sources_context(
    {"source_type": "REST"},
    source_systems=systems,
    readiness_builder=builder,
  )
  assert rest["filtered_source_system_count"] == 1
  assert rest["filtered_dataset_count"] == 2
  assert rest["source_systems"][0].short_name == "beta"

  alpha = catalog_sources.build_architecture_catalog_sources_context(
    {"source_system": "ALPHA"},
    source_systems=systems,
    readiness_builder=builder,
  )
  assert alpha["filtered_source_system_count"] == 1
  assert alpha["filtered_dataset_count"] == 2
  assert alpha["source_systems"][0].short_name == "alpha"


def test_architecture_catalog_sources_search_matches_system_or_dataset(
  monkeypatch,
):
  _patch_reverse(monkeypatch)
  systems, readiness_by_pk = _fixture_systems()
  builder = lambda dataset: readiness_by_pk[dataset.pk]

  system_match = catalog_sources.build_architecture_catalog_sources_context(
    {"q": "Alpha ERP"},
    source_systems=systems,
    readiness_builder=builder,
  )
  assert system_match["filtered_source_system_count"] == 1
  assert system_match["filtered_dataset_count"] == 2
  assert system_match["source_systems"][0].short_name == "alpha"

  dataset_match = catalog_sources.build_architecture_catalog_sources_context(
    {"q": "sales orders"},
    source_systems=systems,
    readiness_builder=builder,
  )
  assert dataset_match["filtered_source_system_count"] == 1
  assert dataset_match["filtered_dataset_count"] == 1
  assert dataset_match["source_systems"][0].datasets[0].dataset_key == "sales.orders"


def test_architecture_catalog_sources_keeps_empty_systems_only_without_dataset_filters(
  monkeypatch,
):
  _patch_reverse(monkeypatch)
  systems, readiness_by_pk = _fixture_systems()
  builder = lambda dataset: readiness_by_pk[dataset.pk]

  complete = catalog_sources.build_architecture_catalog_sources_context(
    {},
    source_systems=systems,
    readiness_builder=builder,
  )
  assert "empty" in {
    group.short_name
    for group in complete["source_systems"]
  }

  ready_only = catalog_sources.build_architecture_catalog_sources_context(
    {"status": "ready"},
    source_systems=systems,
    readiness_builder=builder,
  )
  assert "empty" not in {
    group.short_name
    for group in ready_only["source_systems"]
  }


def test_architecture_catalog_source_filters_normalize_invalid_values():
  filters = catalog_sources.ArchitectureCatalogSourceFilters.from_values({
    "q": "  customer  ",
    "status": "unknown",
    "source_system": " SAP ",
    "source_type": " MSSQL ",
    "ingest_mode": "unsupported",
  })

  assert filters.q == "customer"
  assert filters.status == "all"
  assert filters.source_system == "sap"
  assert filters.source_type == "mssql"
  assert filters.ingest_mode == ""


def test_source_ingestion_readiness_reuses_prefetched_relations():
  source_system = SimpleNamespace(
    short_name="erp",
    type="mssql",
    is_source=True,
    include_ingest="native",
    generate_raw_tables=True,
  )
  source_dataset = SimpleNamespace(
    schema_name="dbo",
    source_dataset_name="customer",
    source_system=source_system,
    active=True,
    integrate=True,
    generate_raw_table=None,
    incremental=False,
    source_columns=_FailManager(),
    output_links=_FailManager(),
    increment_policies=_FailManager(),
  )
  raw_target = SimpleNamespace(
    target_schema=SimpleNamespace(short_name="raw"),
    target_dataset_name="raw_erp_customer",
  )
  source_dataset._prefetched_objects_cache = {
    "source_columns": [
      SimpleNamespace(
        integrate=True,
        source_column_name="customer_id",
        json_path=None,
      ),
    ],
    "output_links": [
      SimpleNamespace(
        active=True,
        target_dataset=raw_target,
      ),
    ],
    "increment_policies": [],
  }

  readiness = build_source_ingestion_readiness(source_dataset)

  assert readiness.status == "ready"
  assert readiness.raw_target_keys == ("raw.raw_erp_customer",)
  assert readiness.integrated_column_count == 1


def _unwrap_view(view_func):
  """Return the undecorated view function."""
  current = view_func
  while hasattr(current, "__wrapped__"):
    current = current.__wrapped__
  return current


def test_architecture_catalog_source_systems_view_renders_catalog_template(
  monkeypatch,
):
  """Verify the Source Systems Catalog view delegates filters to the service."""
  rendered = {}
  expected_context = {
    "source_systems": (),
    "filtered_source_system_count": 0,
    "total_source_system_count": 0,
  }

  def fake_render(request, template_name, context):
    rendered["template_name"] = template_name
    rendered["context"] = context
    return HttpResponse("ok")

  captured = {}

  def fake_build_context(values):
    captured["q"] = values.get("q")
    captured["status"] = values.get("status")
    return expected_context

  monkeypatch.setattr(views_catalog, "render", fake_render)
  monkeypatch.setattr(
    views_catalog,
    "build_architecture_catalog_sources_context",
    fake_build_context,
  )

  request = RequestFactory().get(
    "/architecture-catalog/source-systems/?q=orders&status=attention"
  )
  response = _unwrap_view(
    views_catalog.architecture_catalog_source_systems
  )(request)

  assert response.status_code == 200
  assert rendered["template_name"] == (
    "metadata/architecture/architecture_catalog_source_systems.html"
  )
  assert rendered["context"] is expected_context
  assert captured == {
    "q": "orders",
    "status": "attention",
  }


def test_architecture_catalog_source_systems_template_prioritizes_attention(
  monkeypatch,
):
  """Verify actionable datasets remain visible and remaining rows are collapsed."""
  _patch_reverse(monkeypatch)
  systems, readiness_by_pk = _fixture_systems()
  context = catalog_sources.build_architecture_catalog_sources_context(
    {},
    source_systems=systems,
    readiness_builder=lambda dataset: readiness_by_pk[dataset.pk],
  )

  html = render_to_string(
    "metadata/architecture/architecture_catalog_source_systems.html",
    context,
  )

  assert "Architecture Catalog Source Systems" in html
  assert "Portfolio" in html
  assert "Target Datasets" in html
  assert "Data Products" in html
  assert "Insights" in html
  assert "Map" in html
  assert "sales.orders" in html
  assert "RAW target missing" in html
  assert "Show remaining datasets (1)" in html
  assert html.count("sales.orders") == 1
  assert html.count("dbo.customer") == 1


def test_architecture_catalog_source_systems_template_shows_filtered_rows(
  monkeypatch,
):
  """Verify filtered ready rows are shown without a remaining-dataset expander."""
  _patch_reverse(monkeypatch)
  systems, readiness_by_pk = _fixture_systems()
  context = catalog_sources.build_architecture_catalog_sources_context(
    {"status": "ready"},
    source_systems=systems,
    readiness_builder=lambda dataset: readiness_by_pk[dataset.pk],
  )

  html = render_to_string(
    "metadata/architecture/architecture_catalog_source_systems.html",
    context,
  )

  assert "dbo.customer" in html
  assert "Show remaining datasets" not in html
  assert "No attention signals." in html


def test_architecture_catalog_source_systems_uses_plain_dataset_label(
  monkeypatch,
):
  """Verify a fully collapsed group does not describe its rows as remaining."""
  _patch_reverse(monkeypatch)
  customer = _SourceDataset(
    31,
    "customer",
    schema_name="dbo",
  )
  ready_system = _SourceSystem(
    4,
    "ready",
    "Ready Source",
    "mssql",
    "native",
    (customer,),
  )
  readiness = _readiness(
    customer,
    status="ready",
    ingest_mode="native",
    raw_target_keys=("raw.raw_ready_customer",),
  )
  context = catalog_sources.build_architecture_catalog_sources_context(
    {},
    source_systems=(ready_system,),
    readiness_builder=lambda _dataset: readiness,
  )

  html = render_to_string(
    "metadata/architecture/architecture_catalog_source_systems.html",
    context,
  )

  assert "Show datasets (1)" in html
  assert "Hide datasets" in html
  assert "Show remaining datasets" not in html
  assert "Hide remaining datasets" not in html
