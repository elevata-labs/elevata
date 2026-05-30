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

from dataclasses import dataclass
from typing import Any, Callable, Iterable

from django.db.models import Count, Prefetch, Q
from django.urls import reverse

from metadata.architecture.execution_record import ArchitectureExecutionRecordStore
from metadata.generation.validators import summarize_targetdataset_health
from metadata.models import TargetDataset, TargetDatasetOwnership


INSIGHT_ITEM_LIMIT = 8


@dataclass(frozen=True)
class ArchitectureCatalogInsightItem:
  """
  Read-only dataset reference displayed inside one Catalog Insight card.
  """
  target_dataset_id: int
  dataset_key: str
  schema_short: str
  target_dataset_name: str
  reason: str
  catalog_detail_url: str


@dataclass(frozen=True)
class ArchitectureCatalogInsightCard:
  """
  Read-only Architecture Catalog signal card.
  """
  key: str
  title: str
  description: str
  count: int
  badge_class: str
  items: tuple[ArchitectureCatalogInsightItem, ...]
  remaining_items: tuple[ArchitectureCatalogInsightItem, ...]
  empty_label: str

  @property
  def has_more(self) -> bool:
    """
    Return whether the card contains additional collapsed items.
    """
    return bool(self.remaining_items)


@dataclass(frozen=True)
class ArchitectureCatalogInsightsContext:
  """
  Template context for the Architecture Catalog Insights page.
  """
  insight_cards: tuple[ArchitectureCatalogInsightCard, ...]
  total_dataset_count: int
  active_dataset_count: int
  catalog_url: str


def build_architecture_catalog_insights_context() -> dict[str, Any]:
  """
  Build read-only quality and governance signals for the Architecture Catalog.
  """
  datasets = tuple(_insight_dataset_queryset())
  active_datasets = tuple(dataset for dataset in datasets if dataset.active)
  inactive_datasets = tuple(dataset for dataset in datasets if not dataset.active)
  health_by_dataset_id = _health_by_dataset_id(active_datasets)
  execution_scope_keys = _execution_scope_keys()

  context = ArchitectureCatalogInsightsContext(
    insight_cards=(
      _missing_owner_card(active_datasets),
      _health_card(
        active_datasets,
        health_by_dataset_id,
        level="error",
        key="health_errors",
        title="Datasets with health issues",
        badge_class="text-bg-danger",
        empty_label="No active datasets have health issues.",
      ),
      _health_card(
        active_datasets,
        health_by_dataset_id,
        level="warning",
        key="health_warnings",
        title="Datasets with health warnings",
        badge_class="text-bg-warning",
        empty_label="No active datasets have health warnings.",
      ),
      _custom_query_logic_card(active_datasets),
      _without_downstream_consumers_card(active_datasets),
      _inactive_with_downstream_card(inactive_datasets),
      _missing_execution_evidence_card(
        active_datasets,
        execution_scope_keys,
      ),
    ),
    total_dataset_count=len(datasets),
    active_dataset_count=len(active_datasets),
    catalog_url=reverse("architecture_catalog"),
  )

  return context.__dict__


def _insight_dataset_queryset():
  """
  Return TargetDatasets with the metadata needed for Catalog Insights.
  """
  ownership_queryset = (
    TargetDatasetOwnership.objects
    .select_related("person")
    .order_by("-is_primary_owner", "role", "person__name", "person__email")
  )

  return (
    TargetDataset.objects
    .select_related("target_schema", "query_root")
    .prefetch_related(
      Prefetch("target_dataset_ownerships", queryset=ownership_queryset),
    )
    .annotate(
      owner_count=Count("target_dataset_ownerships", distinct=True),
      downstream_target_count=Count(
        "downstream_input_links",
        filter=Q(downstream_input_links__active=True),
        distinct=True,
      ),
    )
    .order_by("target_schema__short_name", "target_dataset_name")
  )


def _missing_owner_card(
  datasets: tuple[TargetDataset, ...],
) -> ArchitectureCatalogInsightCard:
  """
  Return the Catalog Insight card for active datasets without ownership.
  """
  return _build_card(
    key="missing_owner",
    title="Datasets without owner",
    description="Active datasets without assigned ownership.",
    datasets=(
      dataset
      for dataset in datasets
      if int(getattr(dataset, "owner_count", 0) or 0) == 0
    ),
    reason_builder=lambda dataset: "No owner assigned.",
    badge_class="text-bg-warning",
    empty_label="All active datasets have ownership assigned.",
  )


def _health_card(
  datasets: tuple[TargetDataset, ...],
  health_by_dataset_id: dict[int, tuple[str, tuple[str, ...]]],
  *,
  level: str,
  key: str,
  title: str,
  badge_class: str,
  empty_label: str,
) -> ArchitectureCatalogInsightCard:
  """
  Return a Catalog Insight card for one metadata health level.
  """
  return _build_card(
    key=key,
    title=title,
    description="Active datasets grouped by metadata health findings.",
    datasets=(
      dataset
      for dataset in datasets
      if health_by_dataset_id.get(int(dataset.pk), ("ok", ()))[0] == level
    ),
    reason_builder=(
      lambda dataset: _health_reason(dataset, health_by_dataset_id)
    ),
    badge_class=badge_class,
    empty_label=empty_label,
  )


def _custom_query_logic_card(
  datasets: tuple[TargetDataset, ...],
) -> ArchitectureCatalogInsightCard:
  """
  Return the Catalog Insight card for custom query logic usage.
  """
  return _build_card(
    key="custom_query_logic",
    title="Datasets with custom query logic",
    description="Active datasets whose output is defined by a Query Tree.",
    datasets=(
      dataset
      for dataset in datasets
      if getattr(dataset, "query_root", None) is not None
    ),
    reason_builder=lambda dataset: "Custom query logic enabled.",
    badge_class="text-bg-info",
    empty_label="No active datasets use custom query logic.",
  )


def _without_downstream_consumers_card(
  datasets: tuple[TargetDataset, ...],
) -> ArchitectureCatalogInsightCard:
  """
  Return the Catalog Insight card for active datasets without consumers.
  """
  return _build_card(
    key="without_downstream_consumers",
    title="Datasets without downstream consumers",
    description="Active datasets without active TargetDataset consumers.",
    datasets=(
      dataset
      for dataset in datasets
      if _downstream_target_count(dataset) == 0
    ),
    reason_builder=(
      lambda dataset: "No active downstream TargetDataset consumers."
    ),
    badge_class="text-bg-secondary",
    empty_label="All active datasets have downstream consumers.",
  )


def _inactive_with_downstream_card(
  datasets: tuple[TargetDataset, ...],
) -> ArchitectureCatalogInsightCard:
  """
  Return the Catalog Insight card for inactive datasets still consumed downstream.
  """
  return _build_card(
    key="inactive_with_downstream",
    title="Inactive datasets with downstream consumers",
    description="Inactive datasets that still feed active downstream links.",
    datasets=(
      dataset
      for dataset in datasets
      if _downstream_target_count(dataset) > 0
    ),
    reason_builder=(
      lambda dataset: (
        f"Inactive dataset has {_downstream_target_count(dataset)} "
        "active downstream consumer(s)."
      )
    ),
    badge_class="text-bg-warning",
    empty_label="No inactive datasets have active downstream consumers.",
  )


def _missing_execution_evidence_card(
  datasets: tuple[TargetDataset, ...],
  execution_scope_keys: set[str],
) -> ArchitectureCatalogInsightCard:
  """
  Return the Catalog Insight card for active datasets without execution evidence.
  """
  return _build_card(
    key="missing_execution_evidence",
    title="Datasets without execution evidence",
    description="Active datasets without stored Architecture Execution Records.",
    datasets=(
      dataset
      for dataset in datasets
      if _dataset_key(dataset) not in execution_scope_keys
    ),
    reason_builder=(
      lambda dataset: (
        "No Architecture Execution Record found for this dataset scope."
      )
    ),
    badge_class="text-bg-secondary",
    empty_label="All active datasets have execution evidence.",
  )


def _build_card(
  *,
  key: str,
  title: str,
  description: str,
  datasets: Iterable[TargetDataset],
  reason_builder: Callable[[TargetDataset], str],
  badge_class: str,
  empty_label: str,
) -> ArchitectureCatalogInsightCard:
  """
  Build one Catalog Insight card from a dataset collection.
  """
  dataset_tuple = tuple(datasets)
  insight_items = tuple(
    _item_for_dataset(dataset, reason_builder(dataset))
    for dataset in dataset_tuple
  )

  return ArchitectureCatalogInsightCard(
    key=key,
    title=title,
    description=description,
    count=len(dataset_tuple),
    badge_class=badge_class,
    items=insight_items[:INSIGHT_ITEM_LIMIT],
    remaining_items=insight_items[INSIGHT_ITEM_LIMIT:],
    empty_label=empty_label,
  )


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


def _health_reason(
  target_dataset: TargetDataset,
  health_by_dataset_id: dict[int, tuple[str, tuple[str, ...]]],
) -> str:
  """
  Return the display reason for a TargetDataset health insight.
  """
  health_level, messages = health_by_dataset_id.get(
    int(target_dataset.pk),
    ("ok", ()),
  )
  label = {
    "warning": "Needs review",
    "error": "Issues",
  }.get(health_level, health_level or "Unknown")

  if messages:
    return f"Health: {label} ({len(messages)} finding(s))."

  return f"Health: {label}."


def _execution_scope_keys() -> set[str]:
  """
  Return scope keys with stored Architecture Execution Records.
  """
  return {
    str(getattr(record, "scope_key", "") or "")
    for record in ArchitectureExecutionRecordStore().list_records(limit=None)
    if getattr(record, "scope_key", None)
  }


def _item_for_dataset(
  target_dataset: TargetDataset,
  reason: str,
) -> ArchitectureCatalogInsightItem:
  """
  Return one Catalog Insight item for a TargetDataset.
  """
  schema_short = target_dataset.target_schema.short_name
  target_name = target_dataset.target_dataset_name
  return ArchitectureCatalogInsightItem(
    target_dataset_id=target_dataset.pk,
    dataset_key=f"{schema_short}.{target_name}",
    schema_short=schema_short,
    target_dataset_name=target_name,
    reason=reason,
    catalog_detail_url=reverse(
      "architecture_catalog_detail",
      args=[target_dataset.pk],
    ),
  )


def _dataset_key(target_dataset: TargetDataset) -> str:
  """
  Return the stable Catalog dataset key for a TargetDataset.
  """
  return (
    f"{target_dataset.target_schema.short_name}."
    f"{target_dataset.target_dataset_name}"
  )


def _downstream_target_count(target_dataset: TargetDataset) -> int:
  """
  Return the active downstream TargetDataset consumer count.
  """
  return int(getattr(target_dataset, "downstream_target_count", 0) or 0)