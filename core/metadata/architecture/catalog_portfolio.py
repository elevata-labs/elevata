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

from collections import defaultdict
from dataclasses import dataclass
import logging
from typing import Any
from urllib.parse import urlencode

from django.db.models import Count, Q
from django.urls import reverse

from metadata.architecture.catalog_data_products import (
  build_architecture_catalog_data_products_context,
)
from metadata.architecture.catalog_map import CANONICAL_LAYER_ORDER
from metadata.architecture.execution_record import ArchitectureExecutionRecordStore
from metadata.architecture.review_status import (
  ArchitectureReviewStatusError,
  build_architecture_review_status_context,
  build_target_dataset_architecture_review_status,
)
from metadata.generation.validators import summarize_targetdataset_health
from metadata.models import TargetDataset


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ArchitectureCatalogPortfolioMetric:
  """
  Read-only metric displayed on the Architecture Catalog Portfolio page.
  """
  key: str
  label: str
  value: int
  total: int
  badge_class: str
  description: str
  url: str
  action_label: str = ""

  @property
  def action_count(self) -> int:
    """
    Return the number of datasets behind the focused worklist action.
    """
    return max(self.total - self.value, 0)

  @property
  def has_action(self) -> bool:
    """
    Return whether this metric exposes a non-empty focused worklist action.
    """
    return bool(self.action_label and self.url and self.action_count > 0)

  @property
  def value_label(self) -> str:
    """
    Return the compact metric value label.
    """
    return f"{self.value} / {self.total}" if self.total else str(self.value)

  @property
  def percentage_value(self) -> int:
    """
    Return the rounded metric percentage value.
    """
    if self.total <= 0:
      return 0
    return round((self.value / self.total) * 100)

  @property
  def percentage_label(self) -> str:
    """
    Return the compact metric percentage label.
    """
    if self.total <= 0:
      return "—"
    return f"{self.percentage_value}%"


@dataclass(frozen=True)
class ArchitectureCatalogPortfolioReadinessGroup:
  """
  Read-only Data Product readiness group for the Portfolio page.
  """
  key: str
  label: str
  count: int
  total: int
  badge_class: str
  url: str

  @property
  def share_label(self) -> str:
    """
    Return the rounded readiness share label.
    """
    if self.total <= 0:
      return "—"
    return f"{round((self.count / self.total) * 100)}%"


@dataclass(frozen=True)
class ArchitectureCatalogPortfolioHotspot:
  """
  Read-only portfolio attention area derived from Catalog signals.
  """
  key: str
  label: str
  count: int
  badge_class: str
  message: str
  url: str

  @property
  def has_attention(self) -> bool:
    """
    Return whether this hotspot contains datasets needing attention.
    """
    return self.count > 0


@dataclass(frozen=True)
class ArchitectureCatalogPortfolioLayerSummary:
  """
  Read-only layer summary for the Architecture Catalog Portfolio page.
  """
  schema_short: str
  display_name: str
  dataset_count: int
  active_dataset_count: int
  custom_query_count: int
  owner_coverage_count: int
  contract_coverage_count: int
  health_attention_count: int
  execution_coverage_count: int
  catalog_url: str

  @property
  def owner_coverage_label(self) -> str:
    """
    Return the compact ownership coverage label for this layer.
    """
    return f"{self.owner_coverage_count} / {self.active_dataset_count}"

  @property
  def contract_coverage_label(self) -> str:
    """
    Return the compact contract coverage label for this layer.
    """
    return f"{self.contract_coverage_count} / {self.active_dataset_count}"

  @property
  def execution_coverage_label(self) -> str:
    """
    Return the compact execution evidence coverage label for this layer.
    """
    return f"{self.execution_coverage_count} / {self.active_dataset_count}"


@dataclass(frozen=True)
class ArchitectureCatalogPortfolioContext:
  """
  Template context for the Architecture Catalog Portfolio page.
  """
  metrics: tuple[ArchitectureCatalogPortfolioMetric, ...]
  readiness_groups: tuple[ArchitectureCatalogPortfolioReadinessGroup, ...]
  hotspots: tuple[ArchitectureCatalogPortfolioHotspot, ...]
  layer_summaries: tuple[ArchitectureCatalogPortfolioLayerSummary, ...]
  total_dataset_count: int
  active_dataset_count: int
  data_product_count: int
  catalog_url: str
  data_products_url: str
  insights_url: str
  map_url: str
  architecture_control_url: str


def build_architecture_catalog_portfolio_context() -> dict[str, Any]:
  """
  Build read-only portfolio-level Architecture Catalog signals.
  """
  datasets = tuple(_portfolio_dataset_queryset())
  active_datasets = tuple(dataset for dataset in datasets if dataset.active)
  inactive_datasets = tuple(dataset for dataset in datasets if not dataset.active)
  health_by_dataset_id = _health_by_dataset_id(active_datasets)
  review_counts = _review_status_counts(active_datasets)
  execution_scope_keys = _execution_scope_keys()
  readiness_groups, data_product_count = _data_product_readiness_groups()

  context = ArchitectureCatalogPortfolioContext(
    metrics=_portfolio_metrics(
      datasets=datasets,
      active_datasets=active_datasets,
      health_by_dataset_id=health_by_dataset_id,
      review_counts=review_counts,
      execution_scope_keys=execution_scope_keys,
    ),
    readiness_groups=readiness_groups,
    hotspots=_portfolio_hotspots(
      active_datasets=active_datasets,
      inactive_datasets=inactive_datasets,
      health_by_dataset_id=health_by_dataset_id,
      review_counts=review_counts,
      execution_scope_keys=execution_scope_keys,
    ),
    layer_summaries=_layer_summaries(
      datasets,
      health_by_dataset_id=health_by_dataset_id,
      execution_scope_keys=execution_scope_keys,
    ),
    total_dataset_count=len(datasets),
    active_dataset_count=len(active_datasets),
    data_product_count=data_product_count,
    catalog_url=reverse("architecture_catalog"),
    data_products_url=reverse("architecture_catalog_data_products"),
    insights_url=reverse("architecture_catalog_insights"),
    map_url=reverse("architecture_catalog_map"),
    architecture_control_url=reverse("architecture_control"),
  )

  return context.__dict__


def _portfolio_dataset_queryset():
  """
  Return TargetDatasets with the metadata needed for Portfolio signals.
  """
  return (
    TargetDataset.objects
    .select_related("target_schema", "query_root")
    .annotate(
      owner_count=Count("target_dataset_ownerships", distinct=True),
      contract_column_count=Count("target_columns", distinct=True),
      upstream_target_count=Count(
        "input_links",
        filter=Q(
          input_links__active=True,
          input_links__upstream_target_dataset__isnull=False,
        ),
        distinct=True,
      ),
      downstream_target_count=Count(
        "downstream_input_links",
        filter=Q(downstream_input_links__active=True),
        distinct=True,
      ),
    )
    .order_by("target_schema__short_name", "target_dataset_name")
  )


def _portfolio_metrics(
  *,
  datasets: tuple[TargetDataset, ...],
  active_datasets: tuple[TargetDataset, ...],
  health_by_dataset_id: dict[int, tuple[str, tuple[str, ...]]],
  review_counts: dict[str, int],
  execution_scope_keys: set[str],
) -> tuple[ArchitectureCatalogPortfolioMetric, ...]:
  """
  Return portfolio-level metric cards.
  """
  active_count = len(active_datasets)

  return (
    ArchitectureCatalogPortfolioMetric(
      key="active_datasets",
      label="Active datasets",
      value=active_count,
      total=len(datasets),
      badge_class="text-bg-primary",
      description="Datasets participating in the active architecture portfolio.",
      url=_catalog_filter_url(status="active"),
    ),
    ArchitectureCatalogPortfolioMetric(
      key="ownership_coverage",
      label="Ownership coverage",
      value=sum(1 for dataset in active_datasets if _owner_count(dataset) > 0),
      total=active_count,
      badge_class="text-bg-success",
      description="Active datasets with at least one assigned owner.",
      url=_catalog_signal_url("missing_ownership"),
      action_label="Review missing",
    ),
    ArchitectureCatalogPortfolioMetric(
      key="contract_coverage",
      label="Contract coverage",
      value=sum(
        1
        for dataset in active_datasets
        if _contract_column_count(dataset) > 0
      ),
      total=active_count,
      badge_class="text-bg-success",
      description="Active datasets with at least one defined contract column.",
      url=_catalog_signal_url("missing_contract"),
      action_label="Review missing",
    ),
    ArchitectureCatalogPortfolioMetric(
      key="health_clearance",
      label="Health clearance",
      value=sum(
        1
        for dataset in active_datasets
        if health_by_dataset_id.get(int(dataset.pk), ("ok", ()))[0] == "ok"
      ),
      total=active_count,
      badge_class="text-bg-success",
      description="Active datasets without metadata health warnings or issues.",
      url=_catalog_signal_url("health_attention"),
      action_label="Review attention",
    ),
    ArchitectureCatalogPortfolioMetric(
      key="review_clearance",
      label="Review clearance",
      value=review_counts["clear"],
      total=active_count,
      badge_class="text-bg-info",
      description="Active dataset scopes with approved or unchanged review state.",
      url=_catalog_signal_url("review_attention"),
      action_label="Review attention",
    ),
    ArchitectureCatalogPortfolioMetric(
      key="execution_evidence",
      label="Execution evidence",
      value=sum(
        1
        for dataset in active_datasets
        if _dataset_key(dataset) in execution_scope_keys
      ),
      total=active_count,
      badge_class="text-bg-secondary",
      description="Active datasets with Architecture Execution Record evidence.",
      url=_catalog_signal_url("missing_execution_evidence"),
      action_label="Review missing",
    ),
  )


def _portfolio_hotspots(
  *,
  active_datasets: tuple[TargetDataset, ...],
  inactive_datasets: tuple[TargetDataset, ...],
  health_by_dataset_id: dict[int, tuple[str, tuple[str, ...]]],
  review_counts: dict[str, int],
  execution_scope_keys: set[str],
) -> tuple[ArchitectureCatalogPortfolioHotspot, ...]:
  """
  Return aggregated portfolio attention areas.
  """
  missing_owner_count = sum(
    1 for dataset in active_datasets if _owner_count(dataset) == 0
  )
  missing_contract_count = sum(
    1 for dataset in active_datasets if _contract_column_count(dataset) == 0
  )
  health_attention_count = sum(
    1
    for dataset in active_datasets
    if health_by_dataset_id.get(int(dataset.pk), ("ok", ()))[0]
    in {"warning", "error"}
  )
  review_attention_count = (
    review_counts["attention"]
    + review_counts["blocked"]
    + review_counts["unavailable"]
  )
  missing_execution_count = sum(
    1
    for dataset in active_datasets
    if _dataset_key(dataset) not in execution_scope_keys
  )
  inactive_consumed_count = sum(
    1 for dataset in inactive_datasets if _downstream_target_count(dataset) > 0
  )

  return (
    ArchitectureCatalogPortfolioHotspot(
      key="missing_ownership",
      label="Missing ownership",
      count=missing_owner_count,
      badge_class="text-bg-warning",
      message="Active datasets without assigned ownership.",
      url=_catalog_signal_url("missing_ownership"),
    ),
    ArchitectureCatalogPortfolioHotspot(
      key="missing_contract",
      label="Missing contract",
      count=missing_contract_count,
      badge_class="text-bg-warning" if missing_contract_count else "text-bg-success",
      message="Active datasets without defined contract columns.",
      url=_catalog_signal_url("missing_contract"),
    ),
    ArchitectureCatalogPortfolioHotspot(
      key="health_attention",
      label="Health attention",
      count=health_attention_count,
      badge_class="text-bg-danger" if health_attention_count else "text-bg-success",
      message="Active datasets with metadata health warnings or issues.",
      url=_catalog_signal_url("health_attention"),
    ),
    ArchitectureCatalogPortfolioHotspot(
      key="review_attention",
      label="Review attention",
      count=review_attention_count,
      badge_class="text-bg-warning" if review_attention_count else "text-bg-success",
      message="Active dataset scopes needing Architecture Control attention.",
      url=_catalog_signal_url("review_attention"),
    ),
    ArchitectureCatalogPortfolioHotspot(
      key="missing_execution_evidence",
      label="Missing execution evidence",
      count=missing_execution_count,
      badge_class="text-bg-secondary",
      message="Active datasets without Architecture Execution Record evidence.",
      url=_catalog_signal_url("missing_execution_evidence"),
    ),
    ArchitectureCatalogPortfolioHotspot(
      key="inactive_with_consumers",
      label="Inactive with consumers",
      count=inactive_consumed_count,
      badge_class="text-bg-warning" if inactive_consumed_count else "text-bg-success",
      message="Inactive datasets still referenced by active downstream links.",
      url=_catalog_filter_url(
        status="inactive",
        catalog_signal="inactive_with_consumers",
      ),
    ),
  )


def _layer_summaries(
  datasets: tuple[TargetDataset, ...],
  *,
  health_by_dataset_id: dict[int, tuple[str, tuple[str, ...]]],
  execution_scope_keys: set[str],
) -> tuple[ArchitectureCatalogPortfolioLayerSummary, ...]:
  """
  Return layer-level portfolio summaries.
  """
  by_layer: dict[str, list[TargetDataset]] = defaultdict(list)
  for dataset in datasets:
    by_layer[dataset.target_schema.short_name].append(dataset)

  summaries = tuple(
    _layer_summary(
      schema_short,
      tuple(layer_datasets),
      health_by_dataset_id=health_by_dataset_id,
      execution_scope_keys=execution_scope_keys,
    )
    for schema_short, layer_datasets in by_layer.items()
  )

  return tuple(sorted(summaries, key=lambda summary: _layer_sort_key(summary.schema_short)))


def _layer_summary(
  schema_short: str,
  datasets: tuple[TargetDataset, ...],
  *,
  health_by_dataset_id: dict[int, tuple[str, tuple[str, ...]]],
  execution_scope_keys: set[str],
) -> ArchitectureCatalogPortfolioLayerSummary:
  """
  Return one layer-level portfolio summary.
  """
  active_datasets = tuple(dataset for dataset in datasets if dataset.active)
  layer = datasets[0].target_schema

  return ArchitectureCatalogPortfolioLayerSummary(
    schema_short=schema_short,
    display_name=layer.display_name or schema_short,
    dataset_count=len(datasets),
    active_dataset_count=len(active_datasets),
    custom_query_count=sum(
      1 for dataset in datasets if getattr(dataset, "query_root", None) is not None
    ),
    owner_coverage_count=sum(
      1 for dataset in active_datasets if _owner_count(dataset) > 0
    ),
    contract_coverage_count=sum(
      1 for dataset in active_datasets if _contract_column_count(dataset) > 0
    ),
    health_attention_count=sum(
      1
      for dataset in active_datasets
      if health_by_dataset_id.get(int(dataset.pk), ("ok", ()))[0]
      in {"warning", "error"}
    ),
    execution_coverage_count=sum(
      1
      for dataset in active_datasets
      if _dataset_key(dataset) in execution_scope_keys
    ),
    catalog_url=_catalog_filter_url(
      status="all",
      schema_short=schema_short,
    ),
  )


def _data_product_readiness_groups(
) -> tuple[tuple[ArchitectureCatalogPortfolioReadinessGroup, ...], int]:
  """
  Return Data Product readiness groups for the Portfolio page.
  """
  context = build_architecture_catalog_data_products_context({"status": "active"})
  total = int(context.get("total_candidate_count", 0) or 0)
  groups = tuple(
    ArchitectureCatalogPortfolioReadinessGroup(
      key=str(group["key"]),
      label=str(group["label"]),
      count=int(group["count"] or 0),
      total=total,
      badge_class=str(group["badge_class"]),
      url=_data_products_filter_url(str(group["key"])),
    )
    for group in context.get("readiness_counts", ())
  )
  return groups, total


def _review_status_counts(
  datasets: tuple[TargetDataset, ...],
) -> dict[str, int]:
  """
  Return review status counts for active dataset scopes.
  """
  build_context = build_architecture_review_status_context()
  counts = {
    "clear": 0,
    "attention": 0,
    "blocked": 0,
    "unavailable": 0,
  }

  for dataset in datasets:
    try:
      review_status = build_target_dataset_architecture_review_status(
        dataset,
        build_context=build_context,
      )
    except ArchitectureReviewStatusError as exc:
      logger.info("Catalog Portfolio review status unavailable: %s", exc)
      counts["unavailable"] += 1
      continue
    except Exception as exc:
      logger.exception("Catalog Portfolio review status failed: %s", exc)
      counts["unavailable"] += 1
      continue

    status = str(getattr(review_status, "status", "") or "")
    if status in {"approved", "no_changes"}:
      counts["clear"] += 1
    elif status in {"blocked", "invalid"}:
      counts["blocked"] += 1
    else:
      counts["attention"] += 1

  return counts


def _health_by_dataset_id(
  datasets: tuple[TargetDataset, ...],
) -> dict[int, tuple[str, tuple[str, ...]]]:
  """
  Return metadata health findings keyed by TargetDataset identifier.
  """
  return {
    int(dataset.pk): _health_summary(dataset)
    for dataset in datasets
    if dataset.pk is not None
  }


def _health_summary(target_dataset: TargetDataset) -> tuple[str, tuple[str, ...]]:
  """
  Return normalized metadata health findings for one TargetDataset.
  """
  health_level, messages = summarize_targetdataset_health(target_dataset)
  return str(health_level or "ok"), tuple(messages or ())


def _execution_scope_keys() -> set[str]:
  """
  Return scope keys with stored Architecture Execution Records.
  """
  return {
    str(getattr(record, "scope_key", "") or "")
    for record in ArchitectureExecutionRecordStore().list_records(limit=None)
    if getattr(record, "scope_key", "")
  }


def _dataset_key(target_dataset: TargetDataset) -> str:
  """
  Return the stable Catalog key for a TargetDataset.
  """
  return (
    f"{target_dataset.target_schema.short_name}."
    f"{target_dataset.target_dataset_name}"
  )


def _owner_count(target_dataset: TargetDataset) -> int:
  """
  Return the assigned TargetDataset owner count.
  """
  annotated = getattr(target_dataset, "owner_count", None)
  if annotated is not None:
    return int(annotated or 0)

  return target_dataset.target_dataset_ownerships.count()


def _contract_column_count(target_dataset: TargetDataset) -> int:
  """
  Return the assigned TargetDataset contract column count.
  """
  annotated = getattr(target_dataset, "contract_column_count", None)
  if annotated is not None:
    return int(annotated or 0)

  return target_dataset.target_columns.count()


def _downstream_target_count(target_dataset: TargetDataset) -> int:
  """
  Return the direct downstream TargetDataset consumer count.
  """
  annotated = getattr(target_dataset, "downstream_target_count", None)
  if annotated is not None:
    return int(annotated or 0)

  return target_dataset.downstream_input_links.filter(active=True).count()


def _catalog_signal_url(catalog_signal: str) -> str:
  """
  Return an active Architecture Catalog worklist URL for a Portfolio signal.
  """
  return _catalog_filter_url(
    status="active",
    catalog_signal=catalog_signal,
  )


def _catalog_filter_url(**params: str) -> str:
  """
  Return an Architecture Catalog URL with filter parameters.
  """
  query = urlencode(params)
  return f"{reverse('architecture_catalog')}?{query}" if query else reverse(
    "architecture_catalog",
  )


def _data_products_filter_url(readiness: str) -> str:
  """
  Return a Data Products URL filtered by readiness group.
  """
  query = urlencode({
    "readiness": readiness,
    "status": "active",
  })
  return f"{reverse('architecture_catalog_data_products')}?{query}"


def _layer_sort_key(schema_short: str) -> tuple[int, str]:
  """
  Return the stable Portfolio layer ordering key.
  """
  try:
    return CANONICAL_LAYER_ORDER.index(schema_short), schema_short
  except ValueError:
    return len(CANONICAL_LAYER_ORDER), schema_short
