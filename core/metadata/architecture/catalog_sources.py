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

from django.db.models import Prefetch
from django.urls import reverse

from metadata.models import SourceDataset, System, TargetDatasetInput
from metadata.services.source_ingestion_readiness import (
  SourceIngestionReadiness,
  SourceIngestionReadinessSignal,
  build_source_ingestion_readiness,
)


_STATUS_ORDER = {
  "unavailable": 0,
  "attention": 1,
  "ready": 2,
  "not_applicable": 3,
}
_STATUS_META = {
  "ready": ("Ready", "text-bg-success"),
  "attention": ("Attention", "text-bg-warning"),
  "not_applicable": ("Not applicable", "text-bg-secondary"),
  "unavailable": ("Unavailable", "text-bg-dark"),
}
_VALID_STATUSES = set(_STATUS_META)
_VALID_INGEST_MODES = {"native", "external", "none"}


@dataclass(frozen=True)
class ArchitectureCatalogSourceFilters:
  """Normalized filters for the Architecture Catalog Source Systems view."""
  q: str = ""
  status: str = "all"
  source_system: str = ""
  source_type: str = ""
  ingest_mode: str = ""

  @classmethod
  def from_values(cls, values) -> "ArchitectureCatalogSourceFilters":
    """Build deterministic filters from a request-like mapping."""
    values = values or {}
    q = str(values.get("q") or "").strip()
    status = str(values.get("status") or "all").strip().lower()
    source_system = str(
      values.get("source_system") or ""
    ).strip().lower()
    source_type = str(values.get("source_type") or "").strip().lower()
    ingest_mode = str(values.get("ingest_mode") or "").strip().lower()

    if status not in _VALID_STATUSES | {"all"}:
      status = "all"
    if ingest_mode not in _VALID_INGEST_MODES:
      ingest_mode = ""

    return cls(
      q=q,
      status=status,
      source_system=source_system,
      source_type=source_type,
      ingest_mode=ingest_mode,
    )


@dataclass(frozen=True)
class ArchitectureCatalogSourceStatusCount:
  """One readiness status counter for Catalog Sources."""
  key: str
  label: str
  count: int
  badge_class: str


@dataclass(frozen=True)
class ArchitectureCatalogSourceDatasetSummary:
  """Catalog-facing summary for one SourceDataset."""
  source_dataset_id: int | None
  dataset_key: str
  source_dataset_name: str
  schema_name: str
  description: str
  active: bool
  integrate: bool
  readiness: SourceIngestionReadiness
  detail_url: str

  @property
  def status(self) -> str:
    return self.readiness.status

  @property
  def status_label(self) -> str:
    return _STATUS_META.get(
      self.status,
      (self.status.replace("_", " ").title(), "text-bg-secondary"),
    )[0]

  @property
  def badge_class(self) -> str:
    return _STATUS_META.get(
      self.status,
      ("", "text-bg-secondary"),
    )[1]

  @property
  def landing_required(self) -> bool:
    return self.readiness.landing_required

  @property
  def ingest_mode(self) -> str:
    return self.readiness.ingest_mode

  @property
  def raw_target_keys(self) -> tuple[str, ...]:
    return self.readiness.raw_target_keys

  @property
  def integrated_column_count(self) -> int:
    return self.readiness.integrated_column_count

  @property
  def blocking_signal_count(self) -> int:
    return self.readiness.blocking_signal_count

  @property
  def warning_signal_count(self) -> int:
    return self.readiness.warning_signal_count

  @property
  def is_actionable(self) -> bool:
    return self.status in {"attention", "unavailable"}

  @property
  def finding_signals(self) -> tuple[SourceIngestionReadinessSignal, ...]:
    """Return signals suitable for a compact Portfolio worklist."""
    findings = tuple(
      signal
      for signal in self.readiness.signals
      if signal.severity in {"blocking", "warning"}
    )
    if findings:
      return findings

    if self.status == "not_applicable":
      return tuple(
        signal
        for signal in self.readiness.signals
        if signal.code in {
          "dataset_inactive",
          "dataset_not_integrated",
          "landing_not_required",
        }
      )

    return ()

  @property
  def primary_finding(self) -> SourceIngestionReadinessSignal | None:
    findings = self.finding_signals
    return findings[0] if findings else None


@dataclass(frozen=True)
class ArchitectureCatalogSourceSystemSummary:
  """Grouped Source System summary for the Architecture Catalog."""
  source_system_id: int | None
  short_name: str
  name: str
  description: str
  source_type: str
  ingest_mode: str
  active: bool
  total_dataset_count: int
  datasets: tuple[ArchitectureCatalogSourceDatasetSummary, ...]
  detail_url: str

  @property
  def dataset_count(self) -> int:
    return len(self.datasets)

  @property
  def ready_count(self) -> int:
    return sum(1 for dataset in self.datasets if dataset.status == "ready")

  @property
  def attention_count(self) -> int:
    return sum(
      1
      for dataset in self.datasets
      if dataset.status == "attention"
    )

  @property
  def not_applicable_count(self) -> int:
    return sum(
      1
      for dataset in self.datasets
      if dataset.status == "not_applicable"
    )

  @property
  def unavailable_count(self) -> int:
    return sum(
      1
      for dataset in self.datasets
      if dataset.status == "unavailable"
    )

  @property
  def blocking_signal_count(self) -> int:
    return sum(dataset.blocking_signal_count for dataset in self.datasets)

  @property
  def warning_signal_count(self) -> int:
    return sum(dataset.warning_signal_count for dataset in self.datasets)

  @property
  def status(self) -> str:
    if self.unavailable_count:
      return "unavailable"
    if self.attention_count:
      return "attention"
    if self.ready_count:
      return "ready"
    return "not_applicable"

  @property
  def status_label(self) -> str:
    return _STATUS_META[self.status][0]

  @property
  def badge_class(self) -> str:
    return _STATUS_META[self.status][1]

  @property
  def has_attention(self) -> bool:
    return self.status in {"attention", "unavailable"}

  @property
  def actionable_datasets(
    self,
  ) -> tuple[ArchitectureCatalogSourceDatasetSummary, ...]:
    return tuple(dataset for dataset in self.datasets if dataset.is_actionable)

  @property
  def remaining_datasets(
    self,
  ) -> tuple[ArchitectureCatalogSourceDatasetSummary, ...]:
    return tuple(dataset for dataset in self.datasets if not dataset.is_actionable)

  @property
  def remaining_dataset_count(self) -> int:
    return len(self.remaining_datasets)


ReadinessBuilder = Callable[[Any], SourceIngestionReadiness]


def _relation_values(instance, relation_name: str) -> list[Any]:
  relation = getattr(instance, relation_name, None)
  if relation is None:
    return []
  values = relation.all() if hasattr(relation, "all") else relation
  return list(values)


def _source_systems_with_readiness_relations() -> Iterable[System]:
  """Load Source Systems with all relations required by readiness evaluation."""
  source_dataset_queryset = (
    SourceDataset.objects
      .select_related("source_system")
      .prefetch_related(
        "source_columns",
        "increment_policies",
        Prefetch(
          "output_links",
          queryset=(
            TargetDatasetInput.objects
              .select_related(
                "target_dataset",
                "target_dataset__target_schema",
              )
              .order_by("pk")
          ),
        ),
      )
      .order_by("schema_name", "source_dataset_name", "pk")
  )

  return (
    System.objects
      .filter(is_source=True)
      .prefetch_related(Prefetch(
        "source_datasets",
        queryset=source_dataset_queryset,
      ))
      .order_by("short_name", "pk")
  )


def _dataset_summary(
  source_dataset,
  readiness_builder: ReadinessBuilder,
) -> ArchitectureCatalogSourceDatasetSummary:
  readiness = readiness_builder(source_dataset)
  source_dataset_id = getattr(source_dataset, "pk", None)
  schema_name = str(getattr(source_dataset, "schema_name", "") or "").strip()
  source_dataset_name = str(
    getattr(source_dataset, "source_dataset_name", "") or ""
  ).strip()

  return ArchitectureCatalogSourceDatasetSummary(
    source_dataset_id=source_dataset_id,
    dataset_key=readiness.dataset_key,
    source_dataset_name=source_dataset_name,
    schema_name=schema_name,
    description=str(
      getattr(source_dataset, "description", "") or ""
    ).strip(),
    active=getattr(source_dataset, "active", True) is True,
    integrate=getattr(source_dataset, "integrate", False) is True,
    readiness=readiness,
    detail_url=(
      reverse("sourcedataset_detail", args=[source_dataset_id])
      if source_dataset_id is not None
      else ""
    ),
  )


def _dataset_sort_key(
  dataset: ArchitectureCatalogSourceDatasetSummary,
) -> tuple[int, str]:
  return (
    _STATUS_ORDER.get(dataset.status, 99),
    dataset.dataset_key.lower(),
  )


def _system_sort_key(
  source_system: ArchitectureCatalogSourceSystemSummary,
) -> tuple[int, str]:
  return (
    _STATUS_ORDER.get(source_system.status, 99),
    source_system.short_name.lower(),
  )


def _source_system_summary(
  source_system,
  datasets: tuple[ArchitectureCatalogSourceDatasetSummary, ...],
  *,
  total_dataset_count: int,
) -> ArchitectureCatalogSourceSystemSummary:
  source_system_id = getattr(source_system, "pk", None)
  return ArchitectureCatalogSourceSystemSummary(
    source_system_id=source_system_id,
    short_name=str(getattr(source_system, "short_name", "") or "").strip(),
    name=str(getattr(source_system, "name", "") or "").strip(),
    description=str(
      getattr(source_system, "description", "") or ""
    ).strip(),
    source_type=str(getattr(source_system, "type", "") or "").strip().lower(),
    ingest_mode=str(
      getattr(source_system, "include_ingest", "none") or "none"
    ).strip().lower(),
    active=getattr(source_system, "active", True) is True,
    total_dataset_count=total_dataset_count,
    datasets=tuple(sorted(datasets, key=_dataset_sort_key)),
    detail_url=(
      reverse("system_detail", args=[source_system_id])
      if source_system_id is not None
      else ""
    ),
  )


def _matches_query(value: str, query: str) -> bool:
  return query in str(value or "").lower()


def _system_matches_query(
  source_system: ArchitectureCatalogSourceSystemSummary,
  query: str,
) -> bool:
  return any(
    _matches_query(value, query)
    for value in (
      source_system.short_name,
      source_system.name,
      source_system.description,
      source_system.source_type,
      source_system.ingest_mode,
    )
  )


def _dataset_matches_query(
  dataset: ArchitectureCatalogSourceDatasetSummary,
  query: str,
) -> bool:
  return any(
    _matches_query(value, query)
    for value in (
      dataset.dataset_key,
      dataset.source_dataset_name,
      dataset.schema_name,
      dataset.description,
      " ".join(dataset.raw_target_keys),
    )
  )


def _dataset_matches_filters(
  dataset: ArchitectureCatalogSourceDatasetSummary,
  filters: ArchitectureCatalogSourceFilters,
) -> bool:
  if filters.status != "all" and dataset.status != filters.status:
    return False
  return True


def _status_counts(
  datasets: Iterable[ArchitectureCatalogSourceDatasetSummary],
) -> tuple[ArchitectureCatalogSourceStatusCount, ...]:
  datasets = tuple(datasets)
  return tuple(
    ArchitectureCatalogSourceStatusCount(
      key=key,
      label=label,
      count=sum(1 for dataset in datasets if dataset.status == key),
      badge_class=badge_class,
    )
    for key, (label, badge_class) in _STATUS_META.items()
  )


def build_architecture_catalog_sources_context(
  values=None,
  *,
  source_systems: Iterable[Any] | None = None,
  readiness_builder: ReadinessBuilder = build_source_ingestion_readiness,
) -> dict[str, Any]:
  """
  Build the Architecture Catalog Source Systems context.

  The context groups SourceDataset readiness by Source System. Readiness rules
  remain owned by build_source_ingestion_readiness(); this service only
  aggregates, filters and prepares Catalog-facing summaries.
  """
  filters = ArchitectureCatalogSourceFilters.from_values(values)
  if source_systems is None:
    source_systems = _source_systems_with_readiness_relations()

  all_systems: list[ArchitectureCatalogSourceSystemSummary] = []
  for source_system in source_systems:
    source_datasets = _relation_values(source_system, "source_datasets")
    dataset_summaries = tuple(
      _dataset_summary(source_dataset, readiness_builder)
      for source_dataset in source_datasets
    )
    all_systems.append(_source_system_summary(
      source_system,
      dataset_summaries,
      total_dataset_count=len(dataset_summaries),
    ))

  all_systems = sorted(all_systems, key=lambda item: item.short_name.lower())
  all_datasets = tuple(
    dataset
    for source_system in all_systems
    for dataset in source_system.datasets
  )

  query = filters.q.lower()
  filtered_systems: list[ArchitectureCatalogSourceSystemSummary] = []
  for source_system in all_systems:
    if (
      filters.source_system
      and source_system.short_name.lower() != filters.source_system
    ):
      continue
    if filters.source_type and source_system.source_type != filters.source_type:
      continue
    if filters.ingest_mode and source_system.ingest_mode != filters.ingest_mode:
      continue

    system_query_match = not query or _system_matches_query(
      source_system,
      query,
    )
    visible_datasets = tuple(
      dataset
      for dataset in source_system.datasets
      if _dataset_matches_filters(dataset, filters)
      and (
        system_query_match
        or _dataset_matches_query(dataset, query)
      )
    )

    has_dataset_filter = filters.status != "all"
    include_empty_system = (
      not has_dataset_filter
      and (not query or system_query_match)
      and not source_system.datasets
    )

    if not visible_datasets and not include_empty_system:
      continue

    filtered_systems.append(ArchitectureCatalogSourceSystemSummary(
      source_system_id=source_system.source_system_id,
      short_name=source_system.short_name,
      name=source_system.name,
      description=source_system.description,
      source_type=source_system.source_type,
      ingest_mode=source_system.ingest_mode,
      active=source_system.active,
      total_dataset_count=source_system.total_dataset_count,
      datasets=tuple(sorted(visible_datasets, key=_dataset_sort_key)),
      detail_url=source_system.detail_url,
    ))

  filtered_systems = sorted(filtered_systems, key=_system_sort_key)
  filtered_datasets = tuple(
    dataset
    for source_system in filtered_systems
    for dataset in source_system.datasets
  )

  source_system_options = tuple(
    {
      "value": source_system.short_name,
      "label": (
        f"{source_system.short_name} · {source_system.name}"
        if source_system.name
        else source_system.short_name
      ),
    }
    for source_system in all_systems
  )
  source_type_options = tuple(
    sorted({
      source_system.source_type
      for source_system in all_systems
      if source_system.source_type
    })
  )
  present_ingest_modes = {
    source_system.ingest_mode
    for source_system in all_systems
    if source_system.ingest_mode
  }
  ingest_mode_options = tuple(
    mode
    for mode in ("native", "external", "none")
    if mode in present_ingest_modes
  )

  return {
    "source_systems": tuple(filtered_systems),
    "filters": filters,
    "status_counts": _status_counts(all_datasets),
    "filtered_status_counts": _status_counts(filtered_datasets),
    "source_system_options": source_system_options,
    "source_type_options": source_type_options,
    "ingest_mode_options": ingest_mode_options,
    "total_source_system_count": len(all_systems),
    "filtered_source_system_count": len(filtered_systems),
    "total_dataset_count": len(all_datasets),
    "filtered_dataset_count": len(filtered_datasets),
    "blocking_signal_count": sum(
      dataset.blocking_signal_count
      for dataset in all_datasets
    ),
    "warning_signal_count": sum(
      dataset.warning_signal_count
      for dataset in all_datasets
    ),
    "filtered_blocking_signal_count": sum(
      dataset.blocking_signal_count
      for dataset in filtered_datasets
    ),
    "filtered_warning_signal_count": sum(
      dataset.warning_signal_count
      for dataset in filtered_datasets
    ),
  }
