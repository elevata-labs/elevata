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
from typing import Any

import pytest
from django.http import HttpResponse
from django.test import RequestFactory

import metadata.architecture.catalog as catalog
import metadata.architecture.catalog_insights as catalog_insights
import metadata.views_catalog as views_catalog
from metadata.models import (
  QueryNode,
  QuerySelectNode,
  TargetColumn,
  TargetDataset,
  TargetDatasetInput,
  TargetDatasetOwnership,
  TargetSchema,
  Person,
)


def _unwrap_view(view_func):
  """
  Return the undecorated view function.
  """
  current = view_func
  while hasattr(current, "__wrapped__"):
    current = current.__wrapped__
  return current


def _patch_reverse(monkeypatch) -> None:
  """
  Patch URL reversing for catalog service tests.
  """
  def fake_reverse(name: str, args: list[Any] | None = None) -> str:
    """
    Return deterministic test URLs.
    """
    if name == "architecture_catalog":
      return "/architecture-catalog/"

    if name == "architecture_catalog_insights":
      return "/architecture-catalog/insights/"

    if name == "architecture_control":
      return "/architecture-control/"

    if name == "architecture_catalog_detail":
      pk = args[0] if args else "0"
      return f"/architecture-catalog/{pk}/"

    pk = args[0] if args else "0"
    return f"/{name}/{pk}/"

  monkeypatch.setattr(catalog, "reverse", fake_reverse)
  monkeypatch.setattr(catalog_insights, "reverse", fake_reverse)


def _get_or_create_target_schema(
  short_name: str,
  *,
  display_name: str,
  database_name: str = "dw",
  schema_name: str | None = None,
  default_materialization_type: str = "table",
) -> TargetSchema:
  """
  Return an existing TargetSchema or create it for Architecture Catalog tests.
  """
  schema, _ = TargetSchema.objects.get_or_create(
    short_name=short_name,
    defaults={
      "display_name": display_name,
      "database_name": database_name,
      "schema_name": schema_name or short_name,
      "default_materialization_type": default_materialization_type,
      "surrogate_keys_enabled": True,
    },
  )

  return schema


@pytest.mark.django_db
def test_architecture_catalog_context_filters_by_search_and_schema(
  monkeypatch,
) -> None:
  """
  Verify Catalog context construction with search and schema filters.
  """
  _patch_reverse(monkeypatch)

  rawcore = _get_or_create_target_schema(
    "rawcore",
    display_name="Rawcore",
  )
  serving = _get_or_create_target_schema(
    "serving",
    display_name="Serving",
    default_materialization_type="view",
  )

  TargetDataset.objects.create(
    target_schema=rawcore,
    target_dataset_name="catalog_test_order",
    description="Order integration core",
  )
  customer = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_test_customer_overview",
    description="Customer serving dataset",
    materialization_type="view",
  )

  owner, _ = Person.objects.get_or_create(
    email="owner@example.com",
    defaults={
      "name": "Catalog Owner",
    },
  )
  TargetDatasetOwnership.objects.create(
    target_dataset=customer,
    person=owner,
    role="owner",
    is_primary_owner=True,
  )

  context = catalog.build_architecture_catalog_context({
    "q": "catalog_test_customer",
    "schema_short": "serving",
  })

  assert context["filtered_count"] == 1
  assert context["total_count"] >= 2

  summary = context["datasets"][0]
  assert summary.target_dataset_id == customer.pk
  assert summary.dataset_key == "serving.catalog_test_customer_overview"
  assert summary.effective_materialization == "view"
  assert summary.owner_labels == ("Catalog Owner (owner)",)
  assert summary.catalog_detail_url == f"/architecture-catalog/{customer.pk}/"
  assert summary.detail_url == f"/targetdataset_detail/{customer.pk}/"
  assert summary.lineage_url == f"/targetdataset_lineage/{customer.pk}/"
  assert summary.query_contract_url == (
    f"/targetdataset_query_contract/{customer.pk}/"
  )
  assert summary.architecture_control_url == (
    f"/architecture-control/?scope_mode=target_dataset"
    f"&target_dataset_id={customer.pk}"
  )


@pytest.mark.django_db
def test_architecture_catalog_context_filters_custom_query_logic(
  monkeypatch,
) -> None:
  """
  Verify custom query logic filtering in the Catalog context.
  """
  _patch_reverse(monkeypatch)

  serving = _get_or_create_target_schema(
    "serving",
    display_name="Serving",
    default_materialization_type="view",
  )
  standard_dataset = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_test_standard_dataset",
  )
  custom_dataset = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_test_custom_dataset",
  )
  root = QueryNode.objects.create(
    target_dataset=custom_dataset,
    node_type="select",
    name="Base select",
    active=True,
  )
  QuerySelectNode.objects.create(
    node=root,
    use_dataset_definition=True,
  )
  custom_dataset.query_root = root
  custom_dataset.query_head = root
  custom_dataset.save(update_fields=["query_root", "query_head"])

  context = catalog.build_architecture_catalog_context({
    "q": "catalog_test",
    "schema_short": "serving",
    "query_logic": "custom",
  })

  dataset_keys = [item.dataset_key for item in context["datasets"]]

  assert f"serving.{custom_dataset.target_dataset_name}" in dataset_keys
  assert f"serving.{standard_dataset.target_dataset_name}" not in dataset_keys


@pytest.mark.django_db
def test_architecture_catalog_detail_context_contains_dataset_evidence(
  monkeypatch,
) -> None:
  """
  Verify Catalog detail context construction for dataset evidence.
  """
  _patch_reverse(monkeypatch)
  calls: dict[str, Any] = {}

  serving = _get_or_create_target_schema(
    "serving",
    display_name="Serving",
    default_materialization_type="view",
  )
  upstream_dataset = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_test_detail_upstream",
  )
  target_dataset = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_test_detail_target",
    materialization_type="view",
  )
  downstream_dataset = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_test_detail_downstream",
  )

  TargetDatasetInput.objects.create(
    target_dataset=target_dataset,
    upstream_target_dataset=upstream_dataset,
    role="primary",
  )
  TargetDatasetInput.objects.create(
    target_dataset=downstream_dataset,
    upstream_target_dataset=target_dataset,
    role="primary",
  )
  TargetColumn.objects.create(
    target_dataset=target_dataset,
    target_column_name="customer_name",
    ordinal_position=1,
    datatype="string",
    nullable=True,
    description="Customer display name",
  )

  class FakeExecutionRecordStore:
    """
    Execution record store test double for Catalog detail evidence.
    """

    def list_records(self, filters, *, limit: int | None = 50):
      """
      Capture filters and return one latest execution record.
      """
      calls["filters"] = filters
      calls["limit"] = limit
      return (
        SimpleNamespace(
          execution_id="exec_123",
          started_at="2026-05-27T10:00:00+00:00",
          status="success",
          duration_label="1.234 s",
          dependency_mode="with_dependencies",
          record_fingerprint="abcdef1234567890",
        ),
      )

  monkeypatch.setattr(catalog, "ArchitectureExecutionRecordStore", FakeExecutionRecordStore)
  monkeypatch.setattr(
    catalog,
    "build_target_dataset_architecture_review_status",
    lambda target_dataset: SimpleNamespace(
      status="pending",
      label="Pending",
      message="Architecture changes are present and have no matching approval.",
      badge_class="text-bg-warning",
      icon="bi-hourglass-split",
      report_fingerprint="reviewreport1234567890",
      approval_id=None,
      has_changes=True,
      is_blocked=False,
    ),
  )

  context = catalog.build_architecture_catalog_detail_context(target_dataset)

  assert context["dataset"].dataset_key == "serving.catalog_test_detail_target"
  assert context["dataset"].catalog_detail_url == (
    f"/architecture-catalog/{target_dataset.pk}/"
  )
  assert context["catalog_url"] == "/architecture-catalog/"
  assert context["columns"][0].target_column_name == "customer_name"
  assert context["columns"][0].datatype_label.lower() == "string"
  assert calls["filters"].scope_key == "serving.catalog_test_detail_target"
  assert calls["limit"] == 1
  assert context["latest_execution_record"].execution_id == "exec_123"
  assert context["latest_execution_record"].fingerprint_short == "abcdef123456"
  assert context["review_status"].label == "Pending"
  assert context["review_status"].fingerprint_short == "reviewreport"
  assert context["review_status_error"] == ""
  assert context["insights_url"] == "/architecture-catalog/insights/"
  assert context["upstream_inputs"][0].label == "serving.catalog_test_detail_upstream"
  assert context["downstream_consumers"][0].label == (
    "serving.catalog_test_detail_downstream"
  )
  insight_keys = {
    insight.key
    for insight in context["detail_insights"]
  }
  assert "missing_owner" in insight_keys
  assert "review_pending" in insight_keys
  assert "missing_execution_evidence" not in insight_keys


@pytest.mark.django_db
def test_architecture_catalog_insights_context_groups_catalog_signals(
  monkeypatch,
) -> None:
  """
  Verify Architecture Catalog Insights context construction.
  """
  _patch_reverse(monkeypatch)

  monkeypatch.setattr(
    catalog_insights,
    "INSIGHT_ITEM_LIMIT",
    1,
  )

  serving = _get_or_create_target_schema(
    "serving",
    display_name="Serving",
    default_materialization_type="view",
  )
  ownerless = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_test_ownerless",
  )
  health_error = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_test_health_error",
  )
  health_warning = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_test_health_warning",
  )
  custom_dataset = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_test_custom_query",
  )
  evidence_dataset = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_test_evidence",
  )
  inactive_dataset = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_test_inactive_input",
    active=False,
  )
  downstream_consumer = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_test_downstream_consumer",
  )

  owner, _ = Person.objects.get_or_create(
    email="catalog-insights-owner@example.com",
    defaults={
      "name": "Catalog Insights Owner",
    },
  )
  for dataset in (
    health_error,
    health_warning,
    custom_dataset,
    evidence_dataset,
    downstream_consumer,
  ):
    TargetDatasetOwnership.objects.create(
      target_dataset=dataset,
      person=owner,
      role="owner",
      is_primary_owner=True,
    )

  root = QueryNode.objects.create(
    target_dataset=custom_dataset,
    node_type="select",
    name="Catalog insights select",
    active=True,
  )
  QuerySelectNode.objects.create(
    node=root,
    use_dataset_definition=True,
  )
  custom_dataset.query_root = root
  custom_dataset.query_head = root
  custom_dataset.save(update_fields=["query_root", "query_head"])

  TargetDatasetInput.objects.create(
    target_dataset=downstream_consumer,
    upstream_target_dataset=inactive_dataset,
    role="primary",
  )

  def fake_health(target_dataset):
    """
    Return deterministic health states for Catalog Insights tests.
    """
    if target_dataset.target_dataset_name == "catalog_test_health_error":
      return "error", ("Blocking metadata issue.",)

    if target_dataset.target_dataset_name == "catalog_test_health_warning":
      return "warning", ("Advisory metadata issue.",)

    return "ok", ()

  class FakeExecutionRecordStore:
    """
    Execution record store test double for Catalog Insights.
    """

    def list_records(self, filters=None, *, limit: int | None = 50):
      """
      Return one stored execution record for one dataset scope.
      """
      return (
        SimpleNamespace(
          scope_key=(
            f"serving.{evidence_dataset.target_dataset_name}"
          ),
        ),
      )

  monkeypatch.setattr(
    catalog_insights,
    "summarize_targetdataset_health",
    fake_health,
  )
  monkeypatch.setattr(
    catalog_insights,
    "ArchitectureExecutionRecordStore",
    FakeExecutionRecordStore,
  )

  context = catalog_insights.build_architecture_catalog_insights_context()
  cards = {
    card.key: card
    for card in context["insight_cards"]
  }

  assert context["active_dataset_count"] == 6
  assert context["total_dataset_count"] == 7
  assert cards["missing_owner"].count == 1
  assert cards["missing_owner"].items[0].dataset_key == (
    f"serving.{ownerless.target_dataset_name}"
  )
  assert cards["health_errors"].count == 1
  assert cards["health_warnings"].count == 1
  assert cards["custom_query_logic"].count == 1
  assert cards["without_downstream_consumers"].count >= 1
  assert cards["inactive_with_downstream"].count == 1
  assert cards["missing_execution_evidence"].count == 5
  assert cards["missing_execution_evidence"].has_more is True
  assert len(cards["missing_execution_evidence"].items) == 1
  assert len(cards["missing_execution_evidence"].remaining_items) == 4
  assert all(
    item.dataset_key != f"serving.{evidence_dataset.target_dataset_name}"
    for item in cards["missing_execution_evidence"].items
  )


def test_architecture_catalog_view_renders_catalog_template(
  monkeypatch,
) -> None:
  """
  Verify Architecture Catalog view rendering.
  """
  rendered: dict[str, Any] = {}

  def fake_render(request, template_name: str, context: dict[str, Any]):
    """
    Store render arguments and return a simple response.
    """
    rendered["template_name"] = template_name
    rendered["context"] = context
    return HttpResponse("ok")

  monkeypatch.setattr(views_catalog, "render", fake_render)
  monkeypatch.setattr(
    views_catalog,
    "build_architecture_catalog_context",
    lambda values: {
      "datasets": (),
      "filtered_count": 0,
      "total_count": 0,
    },
  )

  request = RequestFactory().get("/architecture-catalog/?q=customer")
  response = _unwrap_view(views_catalog.architecture_catalog)(request)

  assert response.status_code == 200
  assert rendered["template_name"] == (
    "metadata/architecture/architecture_catalog.html"
  )
  assert rendered["context"]["filtered_count"] == 0


def test_architecture_catalog_insights_view_renders_insights_template(
  monkeypatch,
) -> None:
  """
  Verify Architecture Catalog Insights view rendering.
  """
  rendered: dict[str, Any] = {}

  def fake_render(request, template_name: str, context: dict[str, Any]):
    """
    Store render arguments and return a simple response.
    """
    rendered["template_name"] = template_name
    rendered["context"] = context
    return HttpResponse("ok")

  monkeypatch.setattr(views_catalog, "render", fake_render)
  monkeypatch.setattr(
    views_catalog,
    "build_architecture_catalog_insights_context",
    lambda: {
      "insight_cards": (),
      "active_dataset_count": 0,
      "total_dataset_count": 0,
    },
  )

  request = RequestFactory().get("/architecture-catalog/insights/")
  response = _unwrap_view(views_catalog.architecture_catalog_insights)(request)

  assert response.status_code == 200
  assert rendered["template_name"] == (
    "metadata/architecture/architecture_catalog_insights.html"
  )
  assert rendered["context"]["active_dataset_count"] == 0
  assert rendered["context"]["total_dataset_count"] == 0


def test_architecture_catalog_detail_view_renders_detail_template(
  monkeypatch,
) -> None:
  """
  Verify Architecture Catalog detail view rendering.
  """
  rendered: dict[str, Any] = {}

  def fake_render(request, template_name: str, context: dict[str, Any]):
    """
    Store render arguments and return a simple response.
    """
    rendered["template_name"] = template_name
    rendered["context"] = context
    return HttpResponse("ok")

  monkeypatch.setattr(views_catalog, "render", fake_render)
  monkeypatch.setattr(
    views_catalog,
    "get_object_or_404",
    lambda model, pk: TargetDataset(pk=pk),
  )
  monkeypatch.setattr(
    views_catalog,
    "build_architecture_catalog_detail_context",
    lambda target_dataset: {"object": target_dataset},
  )

  request = RequestFactory().get("/architecture-catalog/42/")
  response = _unwrap_view(views_catalog.architecture_catalog_detail)(request, pk=42)

  assert response.status_code == 200
  assert rendered["template_name"] == (
    "metadata/architecture/architecture_catalog_detail.html"
  )
  assert rendered["context"]["object"].pk == 42