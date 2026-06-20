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
import logging
from typing import Any, Mapping
from urllib.parse import urlencode

from django.db.models import Count, Prefetch, Q
from django.urls import reverse

from metadata.architecture.catalog_data_products import (
  build_architecture_catalog_consumer_readiness_for_dataset,
)
from metadata.architecture.execution_record import (
  ArchitectureExecutionRecordFilters,
  ArchitectureExecutionRecordStore,
)
from metadata.architecture.review_status import (
  ArchitectureReviewStatusError,
  build_architecture_review_status_context,
  build_target_dataset_architecture_review_status,
)
from metadata.models import (
  Person,
  TargetColumn,
  TargetDataset,
  TargetDatasetInput,
  TargetDatasetOwnership,
  TargetSchema,
)
from metadata.generation.validators import summarize_targetdataset_health


logger = logging.getLogger(__name__)

CATALOG_SIGNAL_DEFINITIONS = {
  "missing_ownership": {
    "label": "Missing ownership",
    "description": "Active datasets without assigned ownership.",
  },
  "missing_contract": {
    "label": "Missing contract",
    "description": "Active datasets without defined contract columns.",
  },
  "health_attention": {
    "label": "Health attention",
    "description": "Active datasets with metadata health warnings or issues.",
  },
  "review_attention": {
    "label": "Review attention",
    "description": "Active dataset scopes needing Architecture Control attention.",
  },
  "missing_execution_evidence": {
    "label": "Missing execution evidence",
    "description": "Active datasets without Architecture Execution Record evidence.",
  },
  "inactive_with_consumers": {
    "label": "Inactive with consumers",
    "description": "Inactive datasets still referenced by active downstream links.",
  },
}


@dataclass(frozen=True)
class ArchitectureCatalogFilters:
  """
  User-facing filter values for the Architecture Catalog workspace.
  """
  search: str = ""
  schema_short: str = ""
  owner_id: str = ""
  status: str = "active"
  system_managed: str = "all"
  materialization_type: str = ""
  incremental_strategy: str = ""
  query_logic: str = "all"
  catalog_signal: str = ""

  @classmethod
  def from_values(cls, values: Mapping[str, Any]) -> "ArchitectureCatalogFilters":
    """
    Build normalized Architecture Catalog filters from request values.
    """
    status = _clean_choice(
      values.get("status"),
      allowed={"active", "inactive", "all"},
      fallback="active",
    )
    system_managed = _clean_choice(
      values.get("system_managed"),
      allowed={"all", "yes", "no"},
      fallback="all",
    )
    query_logic = _clean_choice(
      values.get("query_logic"),
      allowed={"all", "custom", "standard"},
      fallback="all",
    )
    catalog_signal = _clean_choice(
      values.get("catalog_signal"),
      allowed=set(CATALOG_SIGNAL_DEFINITIONS),
      fallback="",
    )

    return cls(
      search=str(values.get("q") or "").strip(),
      schema_short=str(values.get("schema_short") or "").strip(),
      owner_id=str(values.get("owner_id") or "").strip(),
      status=status,
      system_managed=system_managed,
      materialization_type=str(values.get("materialization_type") or "").strip(),
      incremental_strategy=str(values.get("incremental_strategy") or "").strip(),
      query_logic=query_logic,
      catalog_signal=catalog_signal,
    )


@dataclass(frozen=True)
class ArchitectureCatalogDatasetSummary:
  """
  Read-only Architecture Catalog summary for one TargetDataset.
  """
  target_dataset_id: int
  dataset_key: str
  schema_short: str
  target_dataset_name: str
  description: str
  effective_materialization: str
  incremental_strategy: str
  active: bool
  is_system_managed: bool
  owner_labels: tuple[str, ...]
  upstream_count: int
  downstream_count: int
  health_level: str
  health_issue_count: int
  has_query_root: bool
  query_head_type: str
  catalog_detail_url: str
  detail_url: str
  lineage_url: str
  query_contract_url: str
  architecture_control_url: str

  @property
  def health_label(self) -> str:
    """
    Return the compact health label used by the Catalog table.
    """
    return {
      "ok": "OK",
      "warning": "Needs review",
      "error": "Issues",
    }.get(self.health_level, self.health_level or "Unknown")

  @property
  def query_logic_label(self) -> str:
    """
    Return the compact query logic label used by the Catalog table.
    """
    if self.has_query_root:
      return self.query_head_type or "Custom"
    return "Standard"

  @property
  def status_label(self) -> str:
    """
    Return the compact lifecycle status label used by the Catalog table.
    """
    return "Active" if self.active else "Inactive"

  @property
  def management_label(self) -> str:
    """
    Return the compact management ownership label used by the Catalog table.
    """
    return "System-managed" if self.is_system_managed else "User-managed"


@dataclass(frozen=True)
class ArchitectureCatalogColumnSummary:
  """
  Read-only Architecture Catalog summary for one TargetColumn.
  """
  target_column_name: str
  ordinal_position: int
  datatype_label: str
  nullable_label: str
  system_role_label: str
  lineage_origin_label: str
  active: bool
  description: str


@dataclass(frozen=True)
class ArchitectureCatalogInputSummary:
  """
  Read-only Architecture Catalog summary for one dataset relationship.
  """
  label: str
  role_label: str
  active: bool
  kind: str
  catalog_detail_url: str
  detail_url: str

  @property
  def status_label(self) -> str:
    """
    Return the compact relationship status label.
    """
    return "Active" if self.active else "Inactive"


@dataclass(frozen=True)
class ArchitectureCatalogExecutionSummary:
  """
  Read-only Architecture Catalog summary for the latest execution record.
  """
  execution_id: str
  started_at: str
  status: str
  duration_label: str
  dependency_mode: str
  record_fingerprint: str

  @property
  def fingerprint_short(self) -> str:
    """
    Return a compact fingerprint label.
    """
    return self.record_fingerprint[:12] if self.record_fingerprint else ""


@dataclass(frozen=True)
class ArchitectureCatalogReviewStatusSummary:
  """
  Read-only Architecture Catalog summary for Architecture Control review status.
  """
  status: str
  label: str
  message: str
  badge_class: str
  icon: str
  report_fingerprint: str
  approval_id: str | None
  has_changes: bool
  is_blocked: bool
  architecture_control_url: str

  @property
  def fingerprint_short(self) -> str:
    """
    Return a compact report fingerprint label.
    """
    return self.report_fingerprint[:12] if self.report_fingerprint else ""


@dataclass(frozen=True)
class ArchitectureCatalogDetailInsight:
  """
  Read-only dataset-specific Catalog Insight signal.
  """
  key: str
  label: str
  message: str
  badge_class: str


@dataclass(frozen=True)
class ArchitectureCatalogDetailContext:
  """
  Template context for one Architecture Catalog dataset detail view.
  """
  object: TargetDataset
  dataset: ArchitectureCatalogDatasetSummary
  columns: tuple[ArchitectureCatalogColumnSummary, ...]
  upstream_inputs: tuple[ArchitectureCatalogInputSummary, ...]
  downstream_consumers: tuple[ArchitectureCatalogInputSummary, ...]
  latest_execution_record: ArchitectureCatalogExecutionSummary | None
  detail_insights: tuple[ArchitectureCatalogDetailInsight, ...]
  review_status: ArchitectureCatalogReviewStatusSummary | None
  review_status_error: str
  consumer_readiness: Any
  health_messages: tuple[str, ...]
  outgoing_reference_count: int
  has_outgoing_references: bool
  data_products_url: str
  insights_url: str
  catalog_url: str


@dataclass(frozen=True)
class ArchitectureCatalogContext:
  """
  Template context for the Architecture Catalog workspace.
  """
  filters: ArchitectureCatalogFilters
  datasets: tuple[ArchitectureCatalogDatasetSummary, ...]
  schema_options: tuple[dict[str, Any], ...]
  owner_options: tuple[dict[str, Any], ...]
  materialization_options: tuple[dict[str, Any], ...]
  incremental_strategy_options: tuple[dict[str, Any], ...]
  active_signal: dict[str, str] | None
  active_filter: dict[str, str] | None
  total_count: int
  filtered_count: int
  clear_url: str


def build_architecture_catalog_context(
  values: Mapping[str, Any],
) -> dict[str, Any]:
  """
  Build the read-only Architecture Catalog context from metadata.
  """
  filters = ArchitectureCatalogFilters.from_values(values)
  queryset = _build_dataset_queryset(filters)
  target_datasets = _apply_runtime_catalog_signal_filter(
    tuple(queryset),
    filters.catalog_signal,
  )
  datasets = tuple(_summarize_dataset(dataset) for dataset in target_datasets)

  context = ArchitectureCatalogContext(
    filters=filters,
    datasets=datasets,
    schema_options=_schema_options(filters.schema_short),
    owner_options=_owner_options(filters.owner_id),
    materialization_options=_field_choice_options(
      TargetDataset,
      "materialization_type",
      filters.materialization_type,
    ),
    incremental_strategy_options=_field_choice_options(
      TargetDataset,
      "incremental_strategy",
      filters.incremental_strategy,
    ),
    active_signal=_catalog_signal_context(filters.catalog_signal),
    active_filter=_catalog_filter_context(filters),
    total_count=TargetDataset.objects.count(),
    filtered_count=len(datasets),
    clear_url=reverse("architecture_catalog"),
  )

  return {
    "filters": context.filters,
    "datasets": context.datasets,
    "schema_options": context.schema_options,
    "owner_options": context.owner_options,
    "materialization_options": context.materialization_options,
    "incremental_strategy_options": context.incremental_strategy_options,
    "active_signal": context.active_signal,
    "active_filter": context.active_filter,
    "total_count": context.total_count,
    "filtered_count": context.filtered_count,
    "clear_url": context.clear_url,
  }


def build_architecture_catalog_detail_context(
  target_dataset: TargetDataset,
) -> dict[str, Any]:
  """
  Build the read-only Architecture Catalog detail context for one TargetDataset.
  """
  dataset = (
    TargetDataset.objects
    .select_related("target_schema", "query_root", "query_head")
    .prefetch_related(
      Prefetch(
        "target_dataset_ownerships",
        queryset=(
          TargetDatasetOwnership.objects
          .select_related("person")
          .order_by("-is_primary_owner", "role", "person__name", "person__email")
        ),
      ),
    )
    .get(pk=target_dataset.pk)
  )

  _health_level, health_messages = summarize_targetdataset_health(dataset)
  summary = _summarize_dataset(dataset)
  latest_execution_record = _latest_execution_record_summary(
    summary.dataset_key,
  )
  review_status, review_status_error = _review_status_summary(
    dataset,
    architecture_control_url=summary.architecture_control_url,
  )
  health_message_tuple = tuple(health_messages)
  outgoing_reference_count = dataset.outgoing_references.count()

  context = ArchitectureCatalogDetailContext(
    object=dataset,
    dataset=summary,
    columns=_column_summaries(dataset),
    upstream_inputs=_upstream_input_summaries(dataset),
    downstream_consumers=_downstream_consumer_summaries(dataset),
    latest_execution_record=latest_execution_record,
    detail_insights=_detail_insights(
      summary,
      latest_execution_record,
      review_status,
      health_message_tuple,
    ),
    review_status=review_status,
    review_status_error=review_status_error,
    consumer_readiness=build_architecture_catalog_consumer_readiness_for_dataset(
      dataset,
    ),
    health_messages=health_message_tuple,
    outgoing_reference_count=outgoing_reference_count,
    has_outgoing_references=outgoing_reference_count > 0,
    data_products_url=reverse("architecture_catalog_data_products"),
    insights_url=reverse("architecture_catalog_insights"),
    catalog_url=reverse("architecture_catalog"),
  )
  return context.__dict__


def _clean_choice(
  value: Any,
  *,
  allowed: set[str],
  fallback: str,
) -> str:
  """
  Return a normalized request choice if it is allowed.
  """
  cleaned = str(value or "").strip()
  return cleaned if cleaned in allowed else fallback


def _build_dataset_queryset(
  filters: ArchitectureCatalogFilters,
):
  """
  Return the filtered TargetDataset queryset for the Catalog.
  """
  ownership_queryset = (
    TargetDatasetOwnership.objects
    .select_related("person")
    .order_by("-is_primary_owner", "role", "person__name", "person__email")
  )

  queryset = (
    TargetDataset.objects
    .select_related("target_schema", "query_root", "query_head")
    .prefetch_related(
      Prefetch("target_dataset_ownerships", queryset=ownership_queryset),
    )
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

  if filters.search:
    queryset = queryset.filter(
      Q(target_dataset_name__icontains=filters.search)
      | Q(description__icontains=filters.search)
      | Q(lineage_key__icontains=filters.search)
      | Q(target_schema__short_name__icontains=filters.search)
      | Q(owner__name__icontains=filters.search)
      | Q(owner__email__icontains=filters.search)
    )

  if filters.schema_short:
    queryset = queryset.filter(target_schema__short_name=filters.schema_short)

  if filters.owner_id:
    queryset = queryset.filter(owner__pk=filters.owner_id)

  if filters.status == "active":
    queryset = queryset.filter(active=True)
  elif filters.status == "inactive":
    queryset = queryset.filter(active=False)

  if filters.system_managed == "yes":
    queryset = queryset.filter(is_system_managed=True)
  elif filters.system_managed == "no":
    queryset = queryset.filter(is_system_managed=False)

  if filters.materialization_type:
    queryset = queryset.filter(
      Q(materialization_type=filters.materialization_type)
      | (
        (
          Q(materialization_type__isnull=True)
          | Q(materialization_type="")
        )
        & Q(target_schema__default_materialization_type=filters.materialization_type)
      )
    )

  if filters.incremental_strategy:
    queryset = queryset.filter(
      incremental_strategy=filters.incremental_strategy,
    )

  if filters.query_logic == "custom":
    queryset = queryset.filter(query_root__isnull=False)
  elif filters.query_logic == "standard":
    queryset = queryset.filter(query_root__isnull=True)

  if filters.catalog_signal == "missing_ownership":
    queryset = queryset.filter(owner_count=0)
  elif filters.catalog_signal == "missing_contract":
    queryset = queryset.filter(contract_column_count=0)

  return queryset.distinct()


def _apply_runtime_catalog_signal_filter(
  datasets: tuple[TargetDataset, ...],
  catalog_signal: str,
) -> tuple[TargetDataset, ...]:
  """
  Return datasets matching a runtime-derived Catalog signal.
  """
  if catalog_signal == "health_attention":
    return tuple(
      dataset
      for dataset in datasets
      if _health_summary(dataset)[0] in {"warning", "error"}
    )

  if catalog_signal == "review_attention":
    return _review_attention_datasets(datasets)

  if catalog_signal == "missing_execution_evidence":
    execution_scope_keys = _execution_scope_keys()
    return tuple(
      dataset
      for dataset in datasets
      if _dataset_key(dataset) not in execution_scope_keys
    )
  
  if catalog_signal == "inactive_with_consumers":
    return tuple(
      dataset
      for dataset in datasets
      if not dataset.active and _downstream_target_count(dataset) > 0
    )

  return datasets


def _catalog_signal_context(catalog_signal: str) -> dict[str, str] | None:
  """
  Return the active Catalog signal context for the template.
  """
  definition = CATALOG_SIGNAL_DEFINITIONS.get(catalog_signal)
  if definition is None:
    return None

  return {
    "key": catalog_signal,
    "label": definition["label"],
    "description": definition["description"],
  }


def _catalog_filter_context(
  filters: ArchitectureCatalogFilters,
) -> dict[str, str] | None:
  """
  Return the active Catalog filter context for reset guidance.
  """
  signal_context = _catalog_signal_context(filters.catalog_signal)
  if signal_context is not None:
    return {
      "label": f"Portfolio worklist: {signal_context['label']}",
      "description": signal_context["description"],
    }

  if not _has_non_default_catalog_filter(filters):
    return None

  return {
    "label": "Catalog filter active",
    "description": "The Catalog is filtered by selected search or filter criteria.",
  }


def _has_non_default_catalog_filter(filters: ArchitectureCatalogFilters) -> bool:
  """
  Return whether the Catalog is filtered beyond its default active dataset view.
  """
  return any((
    bool(filters.search),
    bool(filters.schema_short),
    bool(filters.owner_id),
    filters.status != "active",
    filters.system_managed != "all",
    bool(filters.materialization_type),
    bool(filters.incremental_strategy),
    filters.query_logic != "all",
  ))


def _review_attention_datasets(
  datasets: tuple[TargetDataset, ...],
) -> tuple[TargetDataset, ...]:
  """
  Return datasets whose Architecture Control review state needs attention.
  """
  try:
    build_context = build_architecture_review_status_context()
  except Exception as exc:
    logger.exception("Catalog signal review context failed: %s", exc)
    return datasets

  return tuple(
    dataset
    for dataset in datasets
    if _dataset_needs_review_attention(dataset, build_context=build_context)
  )


def _dataset_needs_review_attention(
  target_dataset: TargetDataset,
  *,
  build_context: Any,
) -> bool:
  """
  Return whether a dataset scope needs Architecture Control review attention.
  """
  try:
    review_status = build_target_dataset_architecture_review_status(
      target_dataset,
      build_context=build_context,
    )
  except ArchitectureReviewStatusError as exc:
    logger.info("Catalog signal review status unavailable: %s", exc)
    return True
  except Exception as exc:
    logger.exception("Catalog signal review status failed: %s", exc)
    return True

  return str(getattr(review_status, "status", "") or "") not in {
    "approved",
    "no_changes",
  }


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


def _health_summary(target_dataset: TargetDataset) -> tuple[str, tuple[str, ...]]:
  """
  Return normalized metadata health findings for one TargetDataset.
  """
  health_level, messages = summarize_targetdataset_health(target_dataset)
  return str(health_level or "ok"), tuple(messages or ())


def _summarize_dataset(
  target_dataset: TargetDataset,
) -> ArchitectureCatalogDatasetSummary:
  """
  Build a read-only Catalog summary for one TargetDataset.
  """
  schema_short = target_dataset.target_schema.short_name
  target_name = target_dataset.target_dataset_name
  dataset_key = f"{schema_short}.{target_name}"
  health_level, health_messages = summarize_targetdataset_health(target_dataset)

  return ArchitectureCatalogDatasetSummary(
    target_dataset_id=target_dataset.pk,
    dataset_key=dataset_key,
    schema_short=schema_short,
    target_dataset_name=target_name,
    description=target_dataset.description or "",
    effective_materialization=_effective_materialization(target_dataset),
    incremental_strategy=target_dataset.incremental_strategy or "full",
    active=bool(target_dataset.active),
    is_system_managed=bool(target_dataset.is_system_managed),
    owner_labels=_owner_labels(target_dataset),
    upstream_count=_upstream_target_count(target_dataset),
    downstream_count=_downstream_target_count(target_dataset),
    health_level=health_level,
    health_issue_count=len(health_messages),
    has_query_root=bool(getattr(target_dataset, "query_root", None)),
    query_head_type=_query_head_type(target_dataset),
    catalog_detail_url=reverse("architecture_catalog_detail", args=[target_dataset.pk]),
    detail_url=reverse("targetdataset_detail", args=[target_dataset.pk]),
    lineage_url=reverse("targetdataset_lineage", args=[target_dataset.pk]),
    query_contract_url=reverse(
      "targetdataset_query_contract",
      args=[target_dataset.pk],
    ),
    architecture_control_url=_architecture_control_url(target_dataset.pk),
  )


def _column_summaries(
  target_dataset: TargetDataset,
) -> tuple[ArchitectureCatalogColumnSummary, ...]:
  """
  Return read-only column summaries for one TargetDataset.
  """
  columns = (
    TargetColumn.objects
    .filter(target_dataset=target_dataset)
    .order_by("ordinal_position", "id")
  )

  return tuple(
    ArchitectureCatalogColumnSummary(
      target_column_name=column.target_column_name,
      ordinal_position=column.ordinal_position,
      datatype_label=_display_value(column, "datatype"),
      nullable_label="Nullable" if column.nullable else "Required",
      system_role_label=_display_value(column, "system_role"),
      lineage_origin_label=_display_value(column, "lineage_origin"),
      active=bool(column.active),
      description=column.description or "",
    )
    for column in columns
  )


def _upstream_input_summaries(
  target_dataset: TargetDataset,
) -> tuple[ArchitectureCatalogInputSummary, ...]:
  """
  Return active upstream input summaries for one TargetDataset.
  """
  links = (
    TargetDatasetInput.objects
    .filter(target_dataset=target_dataset)
    .select_related(
      "source_dataset",
      "source_dataset__source_system",
      "upstream_target_dataset",
      "upstream_target_dataset__target_schema",
    )
    .order_by("role", "id")
  )

  return tuple(_input_summary(link, direction="upstream") for link in links)


def _downstream_consumer_summaries(
  target_dataset: TargetDataset,
) -> tuple[ArchitectureCatalogInputSummary, ...]:
  """
  Return downstream consumer summaries for one TargetDataset.
  """
  links = (
    TargetDatasetInput.objects
    .filter(upstream_target_dataset=target_dataset)
    .select_related(
      "target_dataset",
      "target_dataset__target_schema",
    )
    .order_by(
      "target_dataset__target_schema__short_name",
      "target_dataset__target_dataset_name",
      "id",
    )
  )

  return tuple(_input_summary(link, direction="downstream") for link in links)


def _latest_execution_record_summary(
  scope_key: str,
) -> ArchitectureCatalogExecutionSummary | None:
  """
  Return the latest Architecture Execution Record for one Catalog scope.
  """
  records = ArchitectureExecutionRecordStore().list_records(
    ArchitectureExecutionRecordFilters(scope_key=scope_key),
    limit=1,
  )

  if not records:
    return None

  record = records[0]
  return ArchitectureCatalogExecutionSummary(
    execution_id=record.execution_id,
    started_at=record.started_at,
    status=record.status,
    duration_label=record.duration_label,
    dependency_mode=record.dependency_mode,
    record_fingerprint=record.record_fingerprint,
  )


def _detail_insights(
  dataset: ArchitectureCatalogDatasetSummary,
  latest_execution_record: ArchitectureCatalogExecutionSummary | None,
  review_status: ArchitectureCatalogReviewStatusSummary | None,
  health_messages: tuple[str, ...],
) -> tuple[ArchitectureCatalogDetailInsight, ...]:
  """
  Return dataset-specific Catalog Insight signals for the detail view.
  """
  insights = []

  if not dataset.owner_labels:
    insights.append(
      ArchitectureCatalogDetailInsight(
        key="missing_owner",
        label="Missing owner",
        message="No owner is assigned to this dataset.",
        badge_class="text-bg-warning",
      ),
    )

  if dataset.health_level == "error":
    insights.append(
      ArchitectureCatalogDetailInsight(
        key="health_error",
        label="Health issues",
        message=_health_insight_message(
          health_messages,
          fallback="Metadata health checks found issues.",
        ),
        badge_class="text-bg-danger",
      ),
    )
  elif dataset.health_level == "warning":
    insights.append(
      ArchitectureCatalogDetailInsight(
        key="health_warning",
        label="Health warnings",
        message=_health_insight_message(
          health_messages,
          fallback="Metadata health checks found warnings.",
        ),
        badge_class="text-bg-warning",
      ),
    )

  review_insight = _review_status_detail_insight(review_status)
  if review_insight is not None:
    insights.append(review_insight)

  if dataset.has_query_root:
    insights.append(
      ArchitectureCatalogDetailInsight(
        key="custom_query_logic",
        label="Custom query logic",
        message="This dataset output is defined by a Query Tree.",
        badge_class="text-bg-info",
      ),
    )

  if dataset.active and dataset.downstream_count == 0:
    insights.append(
      ArchitectureCatalogDetailInsight(
        key="without_downstream_consumers",
        label="No downstream consumers",
        message="No active TargetDataset consumers are linked downstream.",
        badge_class="text-bg-secondary",
      ),
    )

  if not dataset.active and dataset.downstream_count > 0:
    insights.append(
      ArchitectureCatalogDetailInsight(
        key="inactive_with_downstream",
        label="Inactive with consumers",
        message=(
          f"This inactive dataset still has {dataset.downstream_count} "
          "active downstream consumer(s)."
        ),
        badge_class="text-bg-warning",
      ),
    )

  if dataset.active and latest_execution_record is None:
    insights.append(
      ArchitectureCatalogDetailInsight(
        key="missing_execution_evidence",
        label="No execution evidence",
        message="No Architecture Execution Record exists for this dataset scope.",
        badge_class="text-bg-secondary",
      ),
    )

  return tuple(insights)


def _review_status_summary(
  target_dataset: TargetDataset,
  *,
  architecture_control_url: str,
) -> tuple[ArchitectureCatalogReviewStatusSummary | None, str]:
  """
  Return the read-only Architecture Control review status for Catalog detail.
  """
  try:
    review_status = build_target_dataset_architecture_review_status(
      target_dataset,
    )
  except ArchitectureReviewStatusError as exc:
    return None, str(exc)
  except Exception as exc:
    logger.exception("Catalog review status summary failed: %s", exc)
    return None, str(exc)

  return (
    ArchitectureCatalogReviewStatusSummary(
      status=review_status.status,
      label=review_status.label,
      message=review_status.message,
      badge_class=review_status.badge_class,
      icon=review_status.icon,
      report_fingerprint=review_status.report_fingerprint,
      approval_id=review_status.approval_id,
      has_changes=review_status.has_changes,
      is_blocked=review_status.is_blocked,
      architecture_control_url=architecture_control_url,
    ),
    "",
  )


def _review_status_detail_insight(
  review_status: ArchitectureCatalogReviewStatusSummary | None,
) -> ArchitectureCatalogDetailInsight | None:
  """
  Return a dataset-specific review status signal for Catalog detail.
  """
  if review_status is None:
    return None

  if review_status.status not in {"blocked", "pending", "drift", "invalid"}:
    return None

  return ArchitectureCatalogDetailInsight(
    key=f"review_{review_status.status}",
    label=f"Review: {review_status.label}",
    message=review_status.message,
    badge_class=review_status.badge_class,
  )


def _health_insight_message(
  health_messages: tuple[str, ...],
  *,
  fallback: str,
) -> str:
  """
  Return a compact health insight message for the Catalog detail view.
  """
  if health_messages:
    return f"{len(health_messages)} metadata health finding(s)."

  return fallback


def _input_summary(
  link: TargetDatasetInput,
  *,
  direction: str,
) -> ArchitectureCatalogInputSummary:
  """
  Return a read-only Catalog relationship summary.
  """
  if direction == "downstream":
    dataset = link.target_dataset
    return ArchitectureCatalogInputSummary(
      label=_dataset_key(dataset),
      role_label=_display_value(link, "role"),
      active=bool(link.active),
      kind="TargetDataset",
      catalog_detail_url=reverse("architecture_catalog_detail", args=[dataset.pk]),
      detail_url=reverse("targetdataset_detail", args=[dataset.pk]),
    )

  upstream_target = getattr(link, "upstream_target_dataset", None)
  if upstream_target is not None:
    return ArchitectureCatalogInputSummary(
      label=_dataset_key(upstream_target),
      role_label=_display_value(link, "role"),
      active=bool(link.active),
      kind="TargetDataset",
      catalog_detail_url=reverse(
        "architecture_catalog_detail",
        args=[upstream_target.pk],
      ),
      detail_url=reverse("targetdataset_detail", args=[upstream_target.pk]),
    )

  source_dataset = getattr(link, "source_dataset", None)
  return ArchitectureCatalogInputSummary(
    label=str(source_dataset) if source_dataset is not None else "—",
    role_label=_display_value(link, "role"),
    active=bool(link.active),
    kind="SourceDataset",
    catalog_detail_url="",
    detail_url=(
      reverse("sourcedataset_detail", args=[source_dataset.pk])
      if source_dataset is not None else ""
    ),
  )


def _effective_materialization(target_dataset: TargetDataset) -> str:
  """
  Return the effective materialization type for a TargetDataset.
  """
  effective = getattr(target_dataset, "effective_materialization_type", None)
  if callable(effective):
    return str(effective() or "")
  if effective:
    return str(effective)

  return str(
    target_dataset.materialization_type
    or target_dataset.target_schema.default_materialization_type
    or ""
  )


def _owner_labels(target_dataset: TargetDataset) -> tuple[str, ...]:
  """
  Return compact owner labels for a TargetDataset.
  """
  ownerships = list(
    getattr(target_dataset, "target_dataset_ownerships", []).all()
  )
  labels = []

  for ownership in ownerships:
    person = getattr(ownership, "person", None)
    person_label = (
      getattr(person, "name", None)
      or getattr(person, "email", None)
      or ""
    )
    role = getattr(ownership, "role", None) or ""
    if person_label and role:
      labels.append(f"{person_label} ({role})")
    elif person_label:
      labels.append(person_label)

  return tuple(labels)


def _query_head_type(target_dataset: TargetDataset) -> str:
  """
  Return the active query head type for a TargetDataset.
  """
  query_root = getattr(target_dataset, "query_root", None)
  query_head = getattr(target_dataset, "query_head", None) or query_root
  return str(getattr(query_head, "node_type", "") or "").strip()


def _upstream_target_count(target_dataset: TargetDataset) -> int:
  """
  Return the direct upstream TargetDataset count.
  """
  annotated = getattr(target_dataset, "upstream_target_count", None)
  if annotated is not None:
    return int(annotated or 0)

  return target_dataset.input_links.filter(
    active=True,
    upstream_target_dataset__isnull=False,
  ).count()


def _downstream_target_count(target_dataset: TargetDataset) -> int:
  """
  Return the direct downstream TargetDataset consumer count.
  """
  annotated = getattr(target_dataset, "downstream_target_count", None)
  if annotated is not None:
    return int(annotated or 0)

  return target_dataset.downstream_input_links.filter(active=True).count()


def _dataset_key(target_dataset: TargetDataset) -> str:
  """
  Return the stable display key for a TargetDataset.
  """
  return (
    f"{target_dataset.target_schema.short_name}."
    f"{target_dataset.target_dataset_name}"
  )


def _display_value(instance: Any, field_name: str) -> str:
  """
  Return the display value for a model field when available.
  """
  display = getattr(instance, f"get_{field_name}_display", None)
  return str(display() if callable(display) else getattr(instance, field_name, "") or "")


def _architecture_control_url(target_dataset_id: int) -> str:
  """
  Return the Architecture Control URL for a TargetDataset scope.
  """
  query = urlencode({
    "scope_mode": "target_dataset",
    "target_dataset_id": target_dataset_id,
  })
  return f"{reverse('architecture_control')}?{query}"


def _schema_options(selected: str) -> tuple[dict[str, Any], ...]:
  """
  Return selectable TargetSchema options for the Catalog filter.
  """
  return tuple(
    {
      "value": schema.short_name,
      "label": schema.short_name,
      "selected": schema.short_name == selected,
    }
    for schema in TargetSchema.objects.order_by("short_name")
  )


def _owner_options(selected: str) -> tuple[dict[str, Any], ...]:
  """
  Return selectable owner options for the Catalog filter.
  """
  owners = (
    Person.objects
    .filter(target_dataset_ownerships__isnull=False)
    .distinct()
    .order_by("name", "email")
  )

  return tuple(
    {
      "value": str(owner.pk),
      "label": owner.name or owner.email,
      "selected": str(owner.pk) == selected,
    }
    for owner in owners
  )


def _field_choice_options(
  model,
  field_name: str,
  selected: str,
) -> tuple[dict[str, Any], ...]:
  """
  Return model field choices as template-friendly options.
  """
  field = model._meta.get_field(field_name)
  return tuple(
    {
      "value": str(value),
      "label": str(label),
      "selected": str(value) == selected,
    }
    for value, label in field.choices
  )