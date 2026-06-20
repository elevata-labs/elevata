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
import metadata.architecture.catalog_data_products as catalog_data_products
import metadata.architecture.catalog_insights as catalog_insights
import metadata.architecture.catalog_map as catalog_map
import metadata.architecture.catalog_portfolio as catalog_portfolio
import metadata.views_catalog as views_catalog
from metadata.models import (
  QueryNode,
  QuerySelectNode,
  TargetColumn,
  TargetDataset,
  TargetDatasetInput,
  TargetDatasetOwnership,
  TargetDatasetReference,
  TargetSchema,
  Person,
)


def test_architecture_catalog_portfolio_metric_action_requires_gap() -> None:
  """
  Verify Portfolio metric actions only appear for non-empty worklists.
  """
  complete_metric = catalog_portfolio.ArchitectureCatalogPortfolioMetric(
    key="contract_coverage",
    label="Contract coverage",
    value=10,
    total=10,
    badge_class="text-bg-success",
    description="All active datasets have contract columns.",
    url="/architecture-catalog/?status=active&catalog_signal=missing_contract",
    action_label="Review missing",
  )
  attention_metric = catalog_portfolio.ArchitectureCatalogPortfolioMetric(
    key="ownership_coverage",
    label="Ownership coverage",
    value=8,
    total=10,
    badge_class="text-bg-success",
    description="Some active datasets have ownership gaps.",
    url="/architecture-catalog/?status=active&catalog_signal=missing_ownership",
    action_label="Review missing",
  )

  assert complete_metric.action_count == 0
  assert complete_metric.has_action is False
  assert attention_metric.action_count == 2
  assert attention_metric.has_action is True


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

    if name == "architecture_catalog_portfolio":
      return "/architecture-catalog/portfolio/"

    if name == "architecture_catalog_data_products":
      return "/architecture-catalog/data-products/"

    if name == "architecture_catalog_insights":
      return "/architecture-catalog/insights/"

    if name == "architecture_catalog_map":
      return "/architecture-catalog/map/"

    if name == "architecture_control":
      return "/architecture-control/"

    if name == "architecture_catalog_detail":
      pk = args[0] if args else "0"
      return f"/architecture-catalog/{pk}/"

    pk = args[0] if args else "0"
    return f"/{name}/{pk}/"

  monkeypatch.setattr(catalog, "reverse", fake_reverse)
  monkeypatch.setattr(catalog_data_products, "reverse", fake_reverse)
  monkeypatch.setattr(catalog_insights, "reverse", fake_reverse)
  monkeypatch.setattr(catalog_map, "reverse", fake_reverse)
  monkeypatch.setattr(catalog_portfolio, "reverse", fake_reverse)


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
def test_architecture_catalog_context_filters_portfolio_signals(
  monkeypatch,
) -> None:
  """
  Verify Catalog worklist filtering for Portfolio signals.
  """
  _patch_reverse(monkeypatch)

  serving = _get_or_create_target_schema(
    "serving",
    display_name="Serving",
    default_materialization_type="view",
  )
  ready = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_signal_ready",
  )
  ownerless = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_signal_ownerless",
  )
  contractless = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_signal_contractless",
  )
  health_attention = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_signal_health_attention",
  )
  review_attention = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_signal_review_attention",
  )
  missing_execution = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_signal_missing_execution",
  )
  inactive_with_consumers = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_signal_inactive_with_consumers",
    active=False,
  )
  inactive_consumer = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_signal_inactive_consumer",
  )
  TargetDatasetInput.objects.create(
    target_dataset=inactive_consumer,
    upstream_target_dataset=inactive_with_consumers,
    role="primary",
  )

  owner, _ = Person.objects.get_or_create(
    email="catalog-signal-owner@example.com",
    defaults={
      "name": "Catalog Signal Owner",
    },
  )
  for dataset in (
    ready,
    contractless,
    health_attention,
    review_attention,
    missing_execution,
    inactive_consumer,
  ):
    TargetDatasetOwnership.objects.create(
      target_dataset=dataset,
      person=owner,
      role="owner",
      is_primary_owner=True,
    )

  for dataset in (
    ready,
    ownerless,
    health_attention,
    review_attention,
    missing_execution,
    inactive_consumer,
  ):
    TargetColumn.objects.create(
      target_dataset=dataset,
      target_column_name="dataset_key",
      ordinal_position=1,
      datatype="string",
    )

  def fake_health(target_dataset):
    """
    Return deterministic health states for Catalog signal tests.
    """
    if target_dataset.target_dataset_name == "catalog_signal_health_attention":
      return "warning", ("Review metadata completeness.",)
    return "ok", ()

  def fake_review_status(target_dataset, *, build_context=None):
    """
    Return deterministic review states for Catalog signal tests.
    """
    if target_dataset.target_dataset_name == "catalog_signal_review_attention":
      return SimpleNamespace(status="pending")
    return SimpleNamespace(status="no_changes")

  class FakeExecutionRecordStore:
    """
    Provide deterministic Architecture Execution Records for Catalog signals.
    """

    def list_records(self, filters=None, *, limit=50):
      """
      Return stored execution record summaries for all but one dataset.
      """
      return tuple(
        SimpleNamespace(scope_key=f"serving.{dataset.target_dataset_name}")
        for dataset in (
          ready,
          ownerless,
          contractless,
          health_attention,
          review_attention,
          inactive_consumer,
        )
      )

  monkeypatch.setattr(catalog, "summarize_targetdataset_health", fake_health)
  monkeypatch.setattr(
    catalog,
    "build_architecture_review_status_context",
    lambda: object(),
  )
  monkeypatch.setattr(
    catalog,
    "build_target_dataset_architecture_review_status",
    fake_review_status,
  )
  monkeypatch.setattr(
    catalog,
    "ArchitectureExecutionRecordStore",
    FakeExecutionRecordStore,
  )

  expected_keys = {
    "missing_ownership": f"serving.{ownerless.target_dataset_name}",
    "missing_contract": f"serving.{contractless.target_dataset_name}",
    "health_attention": f"serving.{health_attention.target_dataset_name}",
    "review_attention": f"serving.{review_attention.target_dataset_name}",
    "missing_execution_evidence": f"serving.{missing_execution.target_dataset_name}",
    "inactive_with_consumers": (
      f"serving.{inactive_with_consumers.target_dataset_name}"
    ),
  }

  for signal, expected_key in expected_keys.items():
    request_values = {"catalog_signal": signal}
    if signal == "inactive_with_consumers":
      request_values["status"] = "inactive"

    context = catalog.build_architecture_catalog_context(request_values)
    dataset_keys = [item.dataset_key for item in context["datasets"]]

    assert dataset_keys == [expected_key]
    assert context["active_signal"]["key"] == signal
    assert context["filters"].catalog_signal == signal


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
  referenced_dataset = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_test_detail_parent",
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
  TargetDatasetReference.objects.create(
    referencing_dataset=target_dataset,
    referenced_dataset=referenced_dataset,
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
  monkeypatch.setattr(
    catalog,
    "build_architecture_catalog_consumer_readiness_for_dataset",
    lambda target_dataset: SimpleNamespace(
      readiness_key="review",
      readiness_label="Review recommended",
      readiness_badge_class="text-bg-warning",
      readiness_message="1 readiness signal needs review.",
      owner_labels=(),
      contract_column_count=1,
      upstream_count=1,
      downstream_count=1,
      warning_signal_count=1,
      blocking_signal_count=0,
      signals=(),
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
  assert context["consumer_readiness"].readiness_label == "Review recommended"
  assert context["outgoing_reference_count"] == 1
  assert context["has_outgoing_references"] is True
  assert context["data_products_url"] == "/architecture-catalog/data-products/"
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
def test_architecture_catalog_data_products_context_groups_readiness(
  monkeypatch,
) -> None:
  """
  Verify Catalog Data Products context construction.
  """
  _patch_reverse(monkeypatch)

  rawcore = _get_or_create_target_schema(
    "rawcore",
    display_name="Rawcore",
  )
  bizcore = _get_or_create_target_schema(
    "bizcore",
    display_name="Bizcore",
  )
  serving = _get_or_create_target_schema(
    "serving",
    display_name="Serving",
    default_materialization_type="view",
  )

  upstream_dataset = TargetDataset.objects.create(
    target_schema=rawcore,
    target_dataset_name="catalog_data_product_upstream",
  )
  downstream_dataset = TargetDataset.objects.create(
    target_schema=rawcore,
    target_dataset_name="catalog_data_product_downstream",
  )
  ready_dataset = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_data_product_ready",
    description="Trusted customer overview for consumption.",
  )
  review_dataset = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_data_product_review",
    description="Customer overview needing review.",
  )
  not_ready_dataset = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_data_product_not_ready",
    description="Serving dataset without contract columns.",
  )
  business_logic_dataset = TargetDataset.objects.create(
    target_schema=bizcore,
    target_dataset_name="catalog_data_product_business_logic",
    description="Business core implementation dataset.",
  )

  owner, _ = Person.objects.get_or_create(
    email="catalog-data-products-owner@example.com",
    defaults={
      "name": "Catalog Data Products Owner",
    },
  )
  for dataset in (ready_dataset, not_ready_dataset):
    TargetDatasetOwnership.objects.create(
      target_dataset=dataset,
      person=owner,
      role="owner",
      is_primary_owner=True,
    )

  TargetColumn.objects.create(
    target_dataset=ready_dataset,
    target_column_name="customer_key",
    ordinal_position=1,
    datatype="string",
  )
  TargetColumn.objects.create(
    target_dataset=review_dataset,
    target_column_name="customer_key",
    ordinal_position=1,
    datatype="string",
  )

  TargetDatasetInput.objects.create(
    target_dataset=ready_dataset,
    upstream_target_dataset=upstream_dataset,
    role="primary",
  )
  TargetDatasetInput.objects.create(
    target_dataset=downstream_dataset,
    upstream_target_dataset=ready_dataset,
    role="primary",
  )

  def fake_review_status(target_dataset, *, build_context=None):
    """
    Return deterministic review statuses for Data Product tests.
    """
    if target_dataset.target_dataset_name == "catalog_data_product_ready":
      return SimpleNamespace(
        status="no_changes",
        label="No architecture changes",
        message="No architecture changes are present for this dataset scope.",
        badge_class="badge-lineage-inactive",
        icon="bi-check2-circle",
        has_changes=False,
        is_blocked=False,
      )

    if target_dataset.target_dataset_name == "catalog_data_product_not_ready":
      return SimpleNamespace(
        status="blocked",
        label="Blocked by policy",
        message="The architecture report contains blocking policy decisions.",
        badge_class="badge-health-error",
        icon="bi-shield-exclamation",
        has_changes=True,
        is_blocked=True,
      )

    return SimpleNamespace(
      status="pending",
      label="Pending review",
      message="Architecture changes are present and have no matching approval.",
      badge_class="badge-health-warning",
      icon="bi-hourglass-split",
      has_changes=True,
      is_blocked=False,
    )

  class FakeExecutionRecordStore:
    """
    Execution record store test double for Catalog Data Products.
    """

    def list_records(self, filters=None, *, limit: int | None = 50):
      """
      Return one stored execution record for the ready dataset scope.
      """
      return (
        SimpleNamespace(
          scope_key="serving.catalog_data_product_ready",
          execution_id="exec_ready",
          started_at="2026-05-31T10:00:00+00:00",
          status="success",
          duration_label="1.000 s",
          dependency_mode="with_dependencies",
          record_fingerprint="readyfingerprint123",
        ),
      )

  monkeypatch.setattr(
    catalog_data_products,
    "build_target_dataset_architecture_review_status",
    fake_review_status,
  )
  monkeypatch.setattr(
    catalog_data_products,
    "summarize_targetdataset_health",
    lambda target_dataset: ("ok", ()),
  )
  monkeypatch.setattr(
    catalog_data_products,
    "ArchitectureExecutionRecordStore",
    FakeExecutionRecordStore,
  )

  context = catalog_data_products.build_architecture_catalog_data_products_context({
    "q": "catalog_data_product",
  })
  data_products = {
    data_product.dataset_key: data_product
    for data_product in context["data_products"]
  }
  readiness_counts = {
    group["key"]: group["count"]
    for group in context["readiness_counts"]
  }

  assert context["total_candidate_count"] == 3
  assert context["filtered_count"] == 3
  assert context["catalog_url"] == "/architecture-catalog/"
  assert context["insights_url"] == "/architecture-catalog/insights/"
  assert context["map_url"] == "/architecture-catalog/map/"
  assert readiness_counts == {
    "ready": 1,
    "review": 1,
    "not_ready": 1,
  }
  assert data_products["serving.catalog_data_product_ready"].readiness_key == "ready"
  assert data_products["serving.catalog_data_product_ready"].is_consumption_ready is True
  assert data_products["serving.catalog_data_product_ready"].contract_column_count == 1
  assert data_products["serving.catalog_data_product_ready"].latest_execution_record.execution_id == "exec_ready"
  assert data_products["serving.catalog_data_product_review"].readiness_key == "review"
  assert data_products["serving.catalog_data_product_not_ready"].readiness_key == "not_ready"
  assert "bizcore.catalog_data_product_business_logic" not in data_products
  assert [option["value"] for option in context["schema_options"]] == [
    "serving",
  ]

  ready_context = catalog_data_products.build_architecture_catalog_data_products_context({
    "q": "catalog_data_product",
    "readiness": "ready",
  })

  assert ready_context["filtered_count"] == 1
  assert ready_context["data_products"][0].dataset_key == (
    "serving.catalog_data_product_ready"
  )


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


@pytest.mark.django_db
def test_architecture_catalog_map_context_groups_layers_and_transitions(
  monkeypatch,
) -> None:
  """
  Verify Architecture Catalog Map context construction.
  """
  _patch_reverse(monkeypatch)

  raw = _get_or_create_target_schema(
    "raw",
    display_name="Raw",
  )
  stage = _get_or_create_target_schema(
    "stage",
    display_name="Stage",
  )
  rawcore = _get_or_create_target_schema(
    "rawcore",
    display_name="Rawcore",
  )
  serving = _get_or_create_target_schema(
    "serving",
    display_name="Serving",
    default_materialization_type="view",
  )

  raw_customer = TargetDataset.objects.create(
    target_schema=raw,
    target_dataset_name="catalog_map_raw_customer",
    description="Raw customer landing dataset",
  )
  stage_customer = TargetDataset.objects.create(
    target_schema=stage,
    target_dataset_name="catalog_map_stage_customer",
  )
  rawcore_customer = TargetDataset.objects.create(
    target_schema=rawcore,
    target_dataset_name="catalog_map_rawcore_customer",
  )
  serving_customer = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_map_serving_customer",
    active=False,
  )

  TargetDatasetInput.objects.create(
    target_dataset=stage_customer,
    upstream_target_dataset=raw_customer,
    role="primary",
  )
  TargetDatasetInput.objects.create(
    target_dataset=rawcore_customer,
    upstream_target_dataset=stage_customer,
    role="primary",
  )
  TargetDatasetInput.objects.create(
    target_dataset=serving_customer,
    upstream_target_dataset=rawcore_customer,
    role="primary",
  )

  context = catalog_map.build_architecture_catalog_map_context()
  layers = {
    layer.schema_short: layer
    for layer in context["layer_summaries"]
  }
  transitions = {
    (transition.source_schema_short, transition.target_schema_short): transition
    for transition in context["transitions"]
  }

  assert context["total_dataset_count"] == 4
  assert context["active_dataset_count"] == 3
  assert context["catalog_url"] == "/architecture-catalog/"
  assert context["insights_url"] == "/architecture-catalog/insights/"
  assert [
    step.schema_short
    for step in context["layer_flow_steps"]
  ] == ["raw", "stage", "rawcore", "serving"]
  assert context["layer_flow_steps"][0].next_schema_short == "stage"
  assert context["layer_flow_steps"][0].next_transition_count == 1
  assert context["layer_flow_steps"][1].next_schema_short == "rawcore"
  assert context["layer_flow_steps"][1].next_transition_count == 1
  assert context["layer_flow_steps"][2].next_schema_short == "serving"
  assert context["layer_flow_steps"][2].next_transition_count == 1
  assert context["layer_flow_steps"][3].has_next_layer is False
  matrix_rows = {
    row.source_schema_short: row
    for row in context["layer_matrix_rows"]
  }
  raw_matrix_counts = {
    cell.target_schema_short: cell.count
    for cell in matrix_rows["raw"].cells
  }
  stage_matrix_counts = {
    cell.target_schema_short: cell.count
    for cell in matrix_rows["stage"].cells
  }
  rawcore_matrix_counts = {
    cell.target_schema_short: cell.count
    for cell in matrix_rows["rawcore"].cells
  }
  assert context["matrix_total_dependency_count"] == 3
  assert matrix_rows["raw"].total_count == 1
  assert raw_matrix_counts["stage"] == 1
  assert raw_matrix_counts["rawcore"] == 0
  assert stage_matrix_counts["rawcore"] == 1
  assert rawcore_matrix_counts["serving"] == 1
  assert layers["raw"].dataset_count == 1
  assert layers["raw"].outgoing_transition_count == 1
  assert layers["stage"].incoming_transition_count == 1
  assert layers["stage"].outgoing_transition_count == 1
  assert layers["serving"].inactive_dataset_count == 1
  assert layers["raw"].dataset_examples[0].dataset_key == (
    "raw.catalog_map_raw_customer"
  )
  assert transitions[("raw", "stage")].count == 1
  assert transitions[("stage", "rawcore")].count == 1
  assert transitions[("rawcore", "serving")].examples[0].target_dataset_key == (
    "serving.catalog_map_serving_customer"
  )
  assert transitions[("rawcore", "serving")].examples[0].target_lineage_url == (
    f"/targetdataset_lineage/{serving_customer.pk}/"
  )


@pytest.mark.django_db
def test_architecture_catalog_map_context_keeps_collapsed_items(
  monkeypatch,
) -> None:
  """
  Verify Catalog Map collapsed layer and transition items are available.
  """
  _patch_reverse(monkeypatch)

  raw = _get_or_create_target_schema(
    "raw",
    display_name="Raw",
  )
  stage = _get_or_create_target_schema(
    "stage",
    display_name="Stage",
  )

  dataset_count = catalog_map.LAYER_DATASET_LIMIT + 2
  for index in range(dataset_count):
    raw_dataset = TargetDataset.objects.create(
      target_schema=raw,
      target_dataset_name=f"catalog_map_expand_raw_{index:02d}",
    )
    stage_dataset = TargetDataset.objects.create(
      target_schema=stage,
      target_dataset_name=f"catalog_map_expand_stage_{index:02d}",
    )
    TargetDatasetInput.objects.create(
      target_dataset=stage_dataset,
      upstream_target_dataset=raw_dataset,
      role="primary",
    )

  context = catalog_map.build_architecture_catalog_map_context()
  layers = {
    layer.schema_short: layer
    for layer in context["layer_summaries"]
  }
  transitions = {
    (transition.source_schema_short, transition.target_schema_short): transition
    for transition in context["transitions"]
  }

  assert layers["raw"].dataset_count == dataset_count
  assert len(layers["raw"].dataset_examples) == catalog_map.LAYER_DATASET_LIMIT
  assert layers["raw"].remaining_dataset_count == 2
  assert len(layers["raw"].remaining_datasets) == 2
  assert layers["raw"].remaining_datasets[0].dataset_key == (
    "raw.catalog_map_expand_raw_05"
  )
  assert transitions[("raw", "stage")].count == dataset_count
  assert len(transitions[("raw", "stage")].examples) == (
    catalog_map.TRANSITION_EXAMPLE_LIMIT
  )
  assert transitions[("raw", "stage")].remaining_example_count == 2
  assert len(transitions[("raw", "stage")].remaining_examples) == 2
  assert transitions[("raw", "stage")].remaining_examples[0].target_dataset_key == (
    "stage.catalog_map_expand_stage_05"
  )


@pytest.mark.django_db
def test_architecture_catalog_portfolio_context_summarizes_metrics(
  monkeypatch,
) -> None:
  """
  Verify Architecture Catalog Portfolio metric aggregation.
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

  ready = TargetDataset.objects.create(
    target_schema=serving,
    target_dataset_name="catalog_portfolio_ready",
    description="Ready data product",
    materialization_type="view",
  )
  attention = TargetDataset.objects.create(
    target_schema=rawcore,
    target_dataset_name="catalog_portfolio_attention",
    description="Dataset with attention signals",
  )
  inactive = TargetDataset.objects.create(
    target_schema=rawcore,
    target_dataset_name="catalog_portfolio_inactive",
    active=False,
  )
  TargetDatasetInput.objects.create(
    target_dataset=ready,
    upstream_target_dataset=inactive,
    role="primary",
  )

  TargetColumn.objects.create(
    target_dataset=ready,
    target_column_name="customer_key",
    ordinal_position=1,
    datatype="string",
  )

  owner, _ = Person.objects.get_or_create(
    email="portfolio-owner@example.com",
    defaults={
      "name": "Portfolio Owner",
    },
  )
  TargetDatasetOwnership.objects.create(
    target_dataset=ready,
    person=owner,
    role="owner",
    is_primary_owner=True,
  )

  def fake_health(target_dataset):
    """
    Return deterministic health findings for Portfolio tests.
    """
    if target_dataset.target_dataset_name.endswith("attention"):
      return "warning", ("Review metadata completeness.",)
    return "ok", ()

  def fake_review_status(target_dataset, *, build_context=None):
    """
    Return deterministic review status for Portfolio tests.
    """
    if target_dataset.target_dataset_name.endswith("ready"):
      return SimpleNamespace(status="no_changes")
    return SimpleNamespace(status="blocked")

  class FakeExecutionRecordStore:
    """
    Provide deterministic Architecture Execution Records for Portfolio tests.
    """
    def list_records(self, filters=None, *, limit=50):
      """
      Return stored execution record summaries.
      """
      return (
        SimpleNamespace(scope_key="serving.catalog_portfolio_ready"),
      )

  monkeypatch.setattr(
    catalog_portfolio,
    "summarize_targetdataset_health",
    fake_health,
  )
  monkeypatch.setattr(
    catalog_portfolio,
    "build_architecture_review_status_context",
    lambda: object(),
  )
  monkeypatch.setattr(
    catalog_portfolio,
    "build_target_dataset_architecture_review_status",
    fake_review_status,
  )
  monkeypatch.setattr(
    catalog_portfolio,
    "ArchitectureExecutionRecordStore",
    FakeExecutionRecordStore,
  )
  monkeypatch.setattr(
    catalog_portfolio,
    "_data_product_readiness_groups",
    lambda: (
      (
        catalog_portfolio.ArchitectureCatalogPortfolioReadinessGroup(
          key="ready",
          label="Consumption-ready",
          count=1,
          total=1,
          badge_class="text-bg-success",
          url="/architecture-catalog/data-products/?readiness=ready&status=active",
        ),
      ),
      1,
    ),
  )

  context = catalog_portfolio.build_architecture_catalog_portfolio_context()
  metrics = {metric.key: metric for metric in context["metrics"]}
  hotspots = {hotspot.key: hotspot for hotspot in context["hotspots"]}
  layers = {layer.schema_short: layer for layer in context["layer_summaries"]}

  assert context["total_dataset_count"] == 3
  assert context["active_dataset_count"] == 2
  assert context["data_product_count"] == 1
  assert metrics["ownership_coverage"].value == 1
  assert metrics["contract_coverage"].value == 1
  assert metrics["health_clearance"].value == 1
  assert metrics["review_clearance"].value == 1
  assert metrics["execution_evidence"].value == 1
  assert metrics["active_datasets"].has_action is False
  assert metrics["ownership_coverage"].url == (
    "/architecture-catalog/?status=active&catalog_signal=missing_ownership"
  )
  assert metrics["contract_coverage"].url == (
    "/architecture-catalog/?status=active&catalog_signal=missing_contract"
  )
  assert metrics["health_clearance"].url == (
    "/architecture-catalog/?status=active&catalog_signal=health_attention"
  )
  assert metrics["review_clearance"].url == (
    "/architecture-catalog/?status=active&catalog_signal=review_attention"
  )
  assert metrics["execution_evidence"].url == (
    "/architecture-catalog/?status=active&catalog_signal=missing_execution_evidence"
  )
  assert metrics["ownership_coverage"].action_label == "Review missing"
  assert metrics["health_clearance"].action_label == "Review attention"
  assert hotspots["missing_ownership"].count == 1
  assert hotspots["missing_contract"].count == 1
  assert hotspots["health_attention"].count == 1
  assert hotspots["review_attention"].count == 1
  assert hotspots["missing_execution_evidence"].count == 1
  assert hotspots["inactive_with_consumers"].count == 1
  assert hotspots["inactive_with_consumers"].url == (
    "/architecture-catalog/?status=inactive&catalog_signal=inactive_with_consumers"
  )
  assert layers["serving"].owner_coverage_count == 1
  assert layers["serving"].contract_coverage_count == 1
  assert layers["serving"].execution_coverage_count == 1
  assert layers["serving"].catalog_url == (
    "/architecture-catalog/?status=all&schema_short=serving"
  )
  assert layers["rawcore"].contract_coverage_count == 0
  assert layers["rawcore"].health_attention_count == 1
  assert layers["rawcore"].catalog_url == (
    "/architecture-catalog/?status=all&schema_short=rawcore"
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


def test_architecture_catalog_portfolio_view_renders_portfolio_template(
  monkeypatch,
) -> None:
  """
  Verify Architecture Catalog Portfolio view rendering.
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
    "build_architecture_catalog_portfolio_context",
    lambda: {
      "metrics": (),
      "hotspots": (),
      "layer_summaries": (),
      "active_dataset_count": 0,
      "total_dataset_count": 0,
    },
  )

  request = RequestFactory().get("/architecture-catalog/portfolio/")
  response = _unwrap_view(views_catalog.architecture_catalog_portfolio)(request)

  assert response.status_code == 200
  assert rendered["template_name"] == (
    "metadata/architecture/architecture_catalog_portfolio.html"
  )
  assert rendered["context"]["active_dataset_count"] == 0
  assert rendered["context"]["total_dataset_count"] == 0


def test_architecture_catalog_data_products_view_renders_template(
  monkeypatch,
) -> None:
  """
  Verify Architecture Catalog Data Products view rendering.
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
    "build_architecture_catalog_data_products_context",
    lambda values: {
      "data_products": (),
      "filtered_count": 0,
      "total_candidate_count": 0,
    },
  )

  request = RequestFactory().get("/architecture-catalog/data-products/")
  response = _unwrap_view(views_catalog.architecture_catalog_data_products)(request)

  assert response.status_code == 200
  assert rendered["template_name"] == (
    "metadata/architecture/architecture_catalog_data_products.html"
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


def test_architecture_catalog_map_view_renders_map_template(
  monkeypatch,
) -> None:
  """
  Verify Architecture Catalog Map view rendering.
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
    "build_architecture_catalog_map_context",
    lambda: {
      "layer_summaries": (),
      "layer_flow_steps": (),
      "layer_matrix_columns": (),
      "layer_matrix_rows": (),
      "matrix_total_dependency_count": 0,
      "transitions": (),
      "active_dataset_count": 0,
      "total_dataset_count": 0,
    },
  )

  request = RequestFactory().get("/architecture-catalog/map/")
  response = _unwrap_view(views_catalog.architecture_catalog_map)(request)

  assert response.status_code == 200
  assert rendered["template_name"] == (
    "metadata/architecture/architecture_catalog_map.html"
  )
  assert rendered["context"]["layer_flow_steps"] == ()
  assert rendered["context"]["layer_matrix_rows"] == ()
  assert rendered["context"]["transitions"] == ()


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


def test_architecture_catalog_reference_integrity_view_renders_partial(
  monkeypatch,
) -> None:
  """
  Verify Reference Integrity Review partial rendering from Catalog Detail.
  """
  rendered: dict[str, Any] = {}
  calls: dict[str, Any] = {}

  def fake_render(request, template_name: str, context: dict[str, Any]):
    """
    Store render arguments and return a simple response.
    """
    rendered["template_name"] = template_name
    rendered["context"] = context
    return HttpResponse("ok")

  def fake_build_review(target_dataset, **kwargs):
    """
    Return a compact review test double.
    """
    calls["target_dataset"] = target_dataset
    calls["kwargs"] = kwargs
    return SimpleNamespace(
      status="complete",
      dataset_key="bizcore.bc_order",
      checked_reference_count=1,
      reference_count=1,
      complete_reference_count=1,
      attention_reference_count=0,
      not_checked_reference_count=0,
      example_limit=5,
      notes=(),
      results=(),
    )

  monkeypatch.setattr(views_catalog, "render", fake_render)
  monkeypatch.setattr(
    views_catalog,
    "get_object_or_404",
    lambda model, pk: TargetDataset(pk=pk),
  )
  monkeypatch.setattr(
    views_catalog,
    "build_reference_integrity_review",
    fake_build_review,
  )

  request = RequestFactory().get(
    "/architecture-catalog/42/reference-integrity/?limit=5"
  )
  response = _unwrap_view(
    views_catalog.architecture_catalog_reference_integrity
  )(request, pk=42)

  assert response.status_code == 200
  assert rendered["template_name"] == (
    "metadata/partials/_reference_integrity_review.html"
  )
  assert rendered["context"]["review"].status == "complete"
  assert rendered["context"]["review_error"] == ""
  assert calls["target_dataset"].pk == 42
  assert calls["kwargs"]["example_limit"] == 5
  assert calls["kwargs"]["include_sql"] is False


def test_architecture_catalog_reference_integrity_view_renders_error(
  monkeypatch,
) -> None:
  """
  Verify Reference Integrity Review errors are rendered inline.
  """
  rendered: dict[str, Any] = {}

  def fake_render(request, template_name: str, context: dict[str, Any]):
    """
    Store render arguments and return a simple response.
    """
    rendered["template_name"] = template_name
    rendered["context"] = context
    return HttpResponse("ok")

  def fake_build_review(*_args, **_kwargs):
    """
    Simulate an unavailable runtime review.
    """
    raise RuntimeError("target connection unavailable")

  monkeypatch.setattr(views_catalog, "render", fake_render)
  monkeypatch.setattr(
    views_catalog,
    "get_object_or_404",
    lambda model, pk: TargetDataset(pk=pk),
  )
  monkeypatch.setattr(
    views_catalog,
    "build_reference_integrity_review",
    fake_build_review,
  )

  request = RequestFactory().get(
    "/architecture-catalog/42/reference-integrity/?include_sql=1"
  )
  response = _unwrap_view(
    views_catalog.architecture_catalog_reference_integrity
  )(request, pk=42)

  assert response.status_code == 200
  assert rendered["template_name"] == (
    "metadata/partials/_reference_integrity_review.html"
  )
  assert rendered["context"]["review"] is None
  assert rendered["context"]["review_error"] == "target connection unavailable"
  assert rendered["context"]["example_limit"] == 20
