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

from metadata.architecture.execution_record import ArchitectureExecutionRecordStore
from metadata.architecture.review_status import (
  ArchitectureReviewStatusError,
  build_target_dataset_architecture_review_status,
)
from metadata.generation.validators import summarize_targetdataset_health
from metadata.models import (
  TargetDataset,
  TargetDatasetOwnership,
  TargetSchema,
)


logger = logging.getLogger(__name__)

CONSUMER_LAYER_ORDER = (
  "serving",
)
READINESS_KEYS = {
  "ready",
  "review",
  "not_ready",
}


@dataclass(frozen=True)
class ArchitectureCatalogDataProductFilters:
  """
  User-facing filter values for the Catalog Data Products page.
  """
  search: str = ""
  schema_short: str = ""
  readiness: str = "all"
  status: str = "active"

  @classmethod
  def from_values(
    cls,
    values: Mapping[str, Any],
  ) -> "ArchitectureCatalogDataProductFilters":
    """
    Build normalized Catalog Data Product filters from request values.
    """
    return cls(
      search=str(values.get("q") or "").strip(),
      schema_short=str(values.get("schema_short") or "").strip(),
      readiness=_clean_choice(
        values.get("readiness"),
        allowed=READINESS_KEYS | {"all"},
        fallback="all",
      ),
      status=_clean_choice(
        values.get("status"),
        allowed={"active", "inactive", "all"},
        fallback="active",
      ),
    )


@dataclass(frozen=True)
class ArchitectureCatalogConsumerSignal:
  """
  Read-only signal contributing to consumer readiness.
  """
  key: str
  label: str
  message: str
  severity: str
  badge_class: str


@dataclass(frozen=True)
class ArchitectureCatalogReviewSignal:
  """
  Read-only Architecture Control review signal for consumer readiness.
  """
  status: str
  label: str
  message: str
  badge_class: str
  icon: str
  has_changes: bool
  is_blocked: bool


@dataclass(frozen=True)
class ArchitectureCatalogExecutionSignal:
  """
  Read-only execution evidence signal for consumer readiness.
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
    Return a compact execution fingerprint label.
    """
    return self.record_fingerprint[:12] if self.record_fingerprint else ""


@dataclass(frozen=True)
class ArchitectureCatalogConsumerReadinessSummary:
  """
  Read-only consumer readiness summary for one Catalog dataset.
  """
  target_dataset_id: int
  dataset_key: str
  schema_short: str
  target_dataset_name: str
  description: str
  active: bool
  owner_labels: tuple[str, ...]
  health_level: str
  health_label: str
  health_issue_count: int
  has_query_root: bool
  query_logic_label: str
  contract_column_count: int
  upstream_count: int
  downstream_count: int
  review_status: ArchitectureCatalogReviewSignal | None
  review_status_error: str
  latest_execution_record: ArchitectureCatalogExecutionSignal | None
  signals: tuple[ArchitectureCatalogConsumerSignal, ...]
  readiness_key: str
  readiness_label: str
  readiness_badge_class: str
  readiness_message: str
  catalog_detail_url: str
  detail_url: str
  lineage_url: str
  query_contract_url: str
  architecture_control_url: str

  @property
  def is_consumption_ready(self) -> bool:
    """
    Return whether the dataset has no blocking or review signals.
    """
    return self.readiness_key == "ready"

  @property
  def blocking_signal_count(self) -> int:
    """
    Return the number of blocking consumer readiness signals.
    """
    return sum(1 for signal in self.signals if signal.severity == "blocking")

  @property
  def warning_signal_count(self) -> int:
    """
    Return the number of review-level consumer readiness signals.
    """
    return sum(1 for signal in self.signals if signal.severity == "warning")


@dataclass(frozen=True)
class ArchitectureCatalogDataProductContext:
  """
  Template context for the Catalog Data Products page.
  """
  filters: ArchitectureCatalogDataProductFilters
  data_products: tuple[ArchitectureCatalogConsumerReadinessSummary, ...]
  readiness_counts: tuple[dict[str, Any], ...]
  schema_options: tuple[dict[str, Any], ...]
  total_candidate_count: int
  filtered_count: int
  catalog_url: str
  insights_url: str
  map_url: str
  clear_url: str


def build_architecture_catalog_data_products_context(
  values: Mapping[str, Any],
) -> dict[str, Any]:
  """
  Build read-only Catalog Data Products and consumer readiness context.
  """
  filters = ArchitectureCatalogDataProductFilters.from_values(values)
  execution_records = _execution_record_by_scope_key()
  datasets = tuple(_data_product_queryset(filters))
  data_products = tuple(
    _consumer_readiness_summary(
      dataset,
      execution_records=execution_records,
    )
    for dataset in datasets
  )
  filtered_data_products = tuple(
    data_product
    for data_product in data_products
    if (
      filters.readiness == "all"
      or data_product.readiness_key == filters.readiness
    )
  )

  context = ArchitectureCatalogDataProductContext(
    filters=filters,
    data_products=filtered_data_products,
    readiness_counts=_readiness_counts(data_products),
    schema_options=_schema_options(filters.schema_short),
    total_candidate_count=len(data_products),
    filtered_count=len(filtered_data_products),
    catalog_url=reverse("architecture_catalog"),
    insights_url=reverse("architecture_catalog_insights"),
    map_url=reverse("architecture_catalog_map"),
    clear_url=reverse("architecture_catalog_data_products"),
  )

  return context.__dict__


def build_architecture_catalog_consumer_readiness_for_dataset(
  target_dataset: TargetDataset,
) -> ArchitectureCatalogConsumerReadinessSummary:
  """
  Build read-only consumer readiness for one Catalog dataset.
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
    .annotate(
      consumer_contract_column_count=Count("target_columns", distinct=True),
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
    .get(pk=target_dataset.pk)
  )
  execution_records = _execution_record_by_scope_key()
  return _consumer_readiness_summary(
    dataset,
    execution_records=execution_records,
  )


def _data_product_queryset(
  filters: ArchitectureCatalogDataProductFilters,
):
  """
  Return TargetDatasets for the consumer-facing Catalog perspective.
  """
  ownership_queryset = (
    TargetDatasetOwnership.objects
    .select_related("person")
    .order_by("-is_primary_owner", "role", "person__name", "person__email")
  )

  queryset = (
    TargetDataset.objects
    .filter(target_schema__short_name__in=CONSUMER_LAYER_ORDER)
    .select_related("target_schema", "query_root", "query_head")
    .prefetch_related(
      Prefetch("target_dataset_ownerships", queryset=ownership_queryset),
    )
    .annotate(
      consumer_contract_column_count=Count("target_columns", distinct=True),
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

  if filters.status == "active":
    queryset = queryset.filter(active=True)
  elif filters.status == "inactive":
    queryset = queryset.filter(active=False)

  return queryset.distinct()


def _consumer_readiness_summary(
  target_dataset: TargetDataset,
  *,
  execution_records: dict[str, ArchitectureCatalogExecutionSignal],
) -> ArchitectureCatalogConsumerReadinessSummary:
  """
  Return consumer readiness for one TargetDataset.
  """
  schema_short = target_dataset.target_schema.short_name
  target_name = target_dataset.target_dataset_name
  dataset_key = f"{schema_short}.{target_name}"
  owner_labels = _owner_labels(target_dataset)
  health_level, health_messages = summarize_targetdataset_health(target_dataset)
  review_status, review_status_error = _review_signal(target_dataset)
  latest_execution_record = execution_records.get(dataset_key)
  upstream_count = _upstream_target_count(target_dataset)
  downstream_count = _downstream_target_count(target_dataset)
  contract_column_count = _contract_column_count(target_dataset)
  has_query_root = bool(getattr(target_dataset, "query_root", None))
  signals = _consumer_signals(
    target_dataset=target_dataset,
    owner_labels=owner_labels,
    health_level=str(health_level or "ok"),
    health_messages=tuple(health_messages or ()),
    review_status=review_status,
    review_status_error=review_status_error,
    latest_execution_record=latest_execution_record,
    contract_column_count=contract_column_count,
    upstream_count=upstream_count,
    downstream_count=downstream_count,
  )
  readiness_key, readiness_label, readiness_badge_class, readiness_message = (
    _readiness_metadata(signals)
  )

  return ArchitectureCatalogConsumerReadinessSummary(
    target_dataset_id=target_dataset.pk,
    dataset_key=dataset_key,
    schema_short=schema_short,
    target_dataset_name=target_name,
    description=target_dataset.description or "",
    active=bool(target_dataset.active),
    owner_labels=owner_labels,
    health_level=str(health_level or "ok"),
    health_label=_health_label(str(health_level or "ok")),
    health_issue_count=len(tuple(health_messages or ())),
    has_query_root=has_query_root,
    query_logic_label=_query_logic_label(target_dataset),
    contract_column_count=contract_column_count,
    upstream_count=upstream_count,
    downstream_count=downstream_count,
    review_status=review_status,
    review_status_error=review_status_error,
    latest_execution_record=latest_execution_record,
    signals=signals,
    readiness_key=readiness_key,
    readiness_label=readiness_label,
    readiness_badge_class=readiness_badge_class,
    readiness_message=readiness_message,
    catalog_detail_url=reverse("architecture_catalog_detail", args=[target_dataset.pk]),
    detail_url=reverse("targetdataset_detail", args=[target_dataset.pk]),
    lineage_url=reverse("targetdataset_lineage", args=[target_dataset.pk]),
    query_contract_url=reverse(
      "targetdataset_query_contract",
      args=[target_dataset.pk],
    ),
    architecture_control_url=_architecture_control_url(target_dataset.pk),
  )


def _consumer_signals(
  *,
  target_dataset: TargetDataset,
  owner_labels: tuple[str, ...],
  health_level: str,
  health_messages: tuple[str, ...],
  review_status: ArchitectureCatalogReviewSignal | None,
  review_status_error: str,
  latest_execution_record: ArchitectureCatalogExecutionSignal | None,
  contract_column_count: int,
  upstream_count: int,
  downstream_count: int,
) -> tuple[ArchitectureCatalogConsumerSignal, ...]:
  """
  Return transparent readiness signals for one consumer-facing dataset.
  """
  signals = [
    _active_signal(target_dataset),
    _layer_signal(target_dataset),
    _description_signal(target_dataset),
    _owner_signal(owner_labels),
    _health_signal(health_level, health_messages),
    _contract_signal(contract_column_count),
    _lineage_signal(upstream_count, direction="upstream"),
    _lineage_signal(downstream_count, direction="downstream"),
    _review_signal_item(review_status, review_status_error),
    _execution_signal_item(latest_execution_record),
    _query_logic_signal(target_dataset),
  ]

  return tuple(signal for signal in signals if signal is not None)


def _active_signal(
  target_dataset: TargetDataset,
) -> ArchitectureCatalogConsumerSignal:
  """
  Return the lifecycle readiness signal.
  """
  if target_dataset.active:
    return _signal(
      "active_lifecycle",
      "Active lifecycle",
      "Dataset is active.",
      "ok",
    )

  return _signal(
    "inactive_lifecycle",
    "Inactive lifecycle",
    "Inactive datasets are not offered as consumption-ready assets.",
    "blocking",
  )


def _layer_signal(
  target_dataset: TargetDataset,
) -> ArchitectureCatalogConsumerSignal:
  """
  Return the layer suitability signal.
  """
  schema_short = target_dataset.target_schema.short_name

  if schema_short in CONSUMER_LAYER_ORDER:
    return _signal(
      "consumer_layer",
      "Consumer-facing layer",
      f"Dataset belongs to the {schema_short} layer and is eligible for consumer readiness.",
      "ok",
    )

  if schema_short == "bizcore":
    return _signal(
      "business_logic_layer",
      "Business logic layer",
      (
        "Bizcore datasets implement business logic and are not offered as "
        "consumption-ready assets."
      ),
      "blocking",
    )

  return _signal(
    "non_consumer_layer",
    "Non-consumer layer",
    (
      "This layer is not offered as a consumption layer. Use serving datasets "
      "for consumer-facing assets."
    ),
    "blocking",
  )


def _description_signal(
  target_dataset: TargetDataset,
) -> ArchitectureCatalogConsumerSignal:
  """
  Return the business description signal.
  """
  if target_dataset.description:
    return _signal(
      "description_available",
      "Description available",
      "Dataset has a business-facing description.",
      "ok",
    )

  return _signal(
    "description_missing",
    "Missing description",
    "Consumer-facing datasets should explain their business meaning.",
    "warning",
  )


def _owner_signal(
  owner_labels: tuple[str, ...],
) -> ArchitectureCatalogConsumerSignal:
  """
  Return the ownership readiness signal.
  """
  if owner_labels:
    return _signal(
      "ownership_assigned",
      "Ownership assigned",
      f"{len(owner_labels)} owner assignment(s) available.",
      "ok",
    )

  return _signal(
    "ownership_missing",
    "Missing ownership",
    "No owner is assigned to this dataset.",
    "warning",
  )


def _health_signal(
  health_level: str,
  health_messages: tuple[str, ...],
) -> ArchitectureCatalogConsumerSignal:
  """
  Return the metadata health readiness signal.
  """
  if health_level == "error":
    return _signal(
      "health_issues",
      "Health issues",
      _finding_message(health_messages, "Metadata health checks found issues."),
      "blocking",
    )

  if health_level == "warning":
    return _signal(
      "health_warnings",
      "Health warnings",
      _finding_message(health_messages, "Metadata health checks found warnings."),
      "warning",
    )

  return _signal(
    "health_ok",
    "Health OK",
    "Metadata health checks are clear.",
    "ok",
  )


def _contract_signal(
  contract_column_count: int,
) -> ArchitectureCatalogConsumerSignal:
  """
  Return the query contract readiness signal.
  """
  if contract_column_count > 0:
    return _signal(
      "contract_available",
      "Contract available",
      f"{contract_column_count} output column(s) available.",
      "ok",
    )

  return _signal(
    "contract_missing",
    "Missing contract columns",
    "No output columns are available for the query contract.",
    "blocking",
  )


def _lineage_signal(
  count: int,
  *,
  direction: str,
) -> ArchitectureCatalogConsumerSignal:
  """
  Return a direct lineage readiness signal.
  """
  if direction == "upstream":
    if count > 0:
      return _signal(
        "upstream_lineage_available",
        "Upstream lineage",
        f"{count} direct upstream TargetDataset link(s).",
        "ok",
      )

    return _signal(
      "upstream_lineage_missing",
      "Missing upstream lineage",
      "No active upstream TargetDataset links are available.",
      "warning",
    )

  if count > 0:
    return _signal(
      "downstream_consumers_present",
      "Downstream consumers",
      f"{count} active downstream TargetDataset consumer(s).",
      "ok",
    )

  return _signal(
    "downstream_consumers_absent",
    "No declared downstream consumers",
    "No active downstream TargetDataset consumers are linked.",
    "info",
  )


def _review_signal_item(
  review_status: ArchitectureCatalogReviewSignal | None,
  review_status_error: str,
) -> ArchitectureCatalogConsumerSignal:
  """
  Return the Architecture Control review readiness signal.
  """
  if review_status is None:
    return _signal(
      "review_unavailable",
      "Review unavailable",
      review_status_error or "Architecture Control review state is unavailable.",
      "warning",
    )

  if review_status.status in {"approved", "no_changes"}:
    return _signal(
      f"review_{review_status.status}",
      review_status.label,
      review_status.message,
      "ok",
    )

  if review_status.status in {"blocked", "invalid"}:
    return _signal(
      f"review_{review_status.status}",
      review_status.label,
      review_status.message,
      "blocking",
    )

  return _signal(
    f"review_{review_status.status}",
    review_status.label,
    review_status.message,
    "warning",
  )


def _execution_signal_item(
  latest_execution_record: ArchitectureCatalogExecutionSignal | None,
) -> ArchitectureCatalogConsumerSignal:
  """
  Return the execution evidence readiness signal.
  """
  if latest_execution_record is None:
    return _signal(
      "execution_evidence_missing",
      "Missing execution evidence",
      "No Architecture Execution Record exists for this dataset scope.",
      "warning",
    )

  if latest_execution_record.status == "success":
    return _signal(
      "execution_evidence_success",
      "Execution evidence",
      "Latest Architecture Execution Record is successful.",
      "ok",
    )

  return _signal(
    "execution_evidence_attention",
    "Execution evidence needs review",
    f"Latest Architecture Execution Record status: {latest_execution_record.status}.",
    "warning",
  )


def _query_logic_signal(
  target_dataset: TargetDataset,
) -> ArchitectureCatalogConsumerSignal:
  """
  Return the query logic transparency signal.
  """
  if getattr(target_dataset, "query_root", None) is not None:
    return _signal(
      "custom_query_logic",
      "Custom query logic",
      "Dataset output is defined by a Query Tree.",
      "info",
    )

  return _signal(
    "standard_query_logic",
    "Standard query logic",
    "Dataset output follows the standard dataset definition.",
    "ok",
  )


def _review_signal(
  target_dataset: TargetDataset,
) -> tuple[ArchitectureCatalogReviewSignal | None, str]:
  """
  Return Architecture Control review status as a consumer readiness signal.
  """
  try:
    review_status = build_target_dataset_architecture_review_status(
      target_dataset,
    )
  except ArchitectureReviewStatusError as exc:
    return None, str(exc)
  except Exception as exc:
    logger.exception("Catalog consumer review status failed: %s", exc)
    return None, str(exc)

  return (
    ArchitectureCatalogReviewSignal(
      status=review_status.status,
      label=review_status.label,
      message=review_status.message,
      badge_class=review_status.badge_class,
      icon=review_status.icon,
      has_changes=review_status.has_changes,
      is_blocked=review_status.is_blocked,
    ),
    "",
  )


def _execution_record_by_scope_key() -> dict[str, ArchitectureCatalogExecutionSignal]:
  """
  Return the latest Architecture Execution Record keyed by scope.
  """
  records_by_scope_key: dict[str, ArchitectureCatalogExecutionSignal] = {}

  for record in ArchitectureExecutionRecordStore().list_records(limit=None):
    scope_key = str(getattr(record, "scope_key", "") or "")
    if not scope_key or scope_key in records_by_scope_key:
      continue

    records_by_scope_key[scope_key] = ArchitectureCatalogExecutionSignal(
      execution_id=record.execution_id,
      started_at=record.started_at,
      status=record.status,
      duration_label=record.duration_label,
      dependency_mode=record.dependency_mode,
      record_fingerprint=record.record_fingerprint,
    )

  return records_by_scope_key


def _readiness_metadata(
  signals: tuple[ArchitectureCatalogConsumerSignal, ...],
) -> tuple[str, str, str, str]:
  """
  Return the overall consumer readiness label and badge metadata.
  """
  blocking_count = sum(1 for signal in signals if signal.severity == "blocking")
  warning_count = sum(1 for signal in signals if signal.severity == "warning")

  if blocking_count:
    return (
      "not_ready",
      "Not consumption-ready",
      "text-bg-danger",
      f"{blocking_count} blocking readiness signal(s).",
    )

  if warning_count:
    return (
      "review",
      "Review recommended",
      "text-bg-warning",
      f"{warning_count} readiness signal(s) need review.",
    )

  return (
    "ready",
    "Consumption-ready",
    "text-bg-success",
    "All readiness signals are clear.",
  )


def _readiness_counts(
  data_products: tuple[ArchitectureCatalogConsumerReadinessSummary, ...],
) -> tuple[dict[str, Any], ...]:
  """
  Return count badges for consumer readiness groups.
  """
  counts = {
    "ready": 0,
    "review": 0,
    "not_ready": 0,
  }
  for data_product in data_products:
    counts[data_product.readiness_key] += 1

  return (
    {
      "key": "ready",
      "label": "Consumption-ready",
      "count": counts["ready"],
      "badge_class": "text-bg-success",
    },
    {
      "key": "review",
      "label": "Review recommended",
      "count": counts["review"],
      "badge_class": "text-bg-warning",
    },
    {
      "key": "not_ready",
      "label": "Not consumption-ready",
      "count": counts["not_ready"],
      "badge_class": "text-bg-danger",
    },
  )


def _signal(
  key: str,
  label: str,
  message: str,
  severity: str,
) -> ArchitectureCatalogConsumerSignal:
  """
  Return a display-ready consumer readiness signal.
  """
  return ArchitectureCatalogConsumerSignal(
    key=key,
    label=label,
    message=message,
    severity=severity,
    badge_class=_signal_badge_class(severity),
  )


def _signal_badge_class(severity: str) -> str:
  """
  Return the badge class for a signal severity.
  """
  return {
    "ok": "badge-health-ok",
    "warning": "badge-health-warning",
    "blocking": "badge-health-error",
    "info": "badge-lineage-ref",
  }.get(severity, "badge-lineage-inactive")


def _finding_message(
  messages: tuple[str, ...],
  fallback: str,
) -> str:
  """
  Return a compact metadata health finding message.
  """
  if messages:
    return f"{len(messages)} metadata health finding(s)."

  return fallback


def _contract_column_count(target_dataset: TargetDataset) -> int:
  """
  Return the output column count used for the query contract signal.
  """
  annotated = getattr(target_dataset, "consumer_contract_column_count", None)
  if annotated is not None:
    return int(annotated or 0)

  return target_dataset.target_columns.count()


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


def _query_logic_label(target_dataset: TargetDataset) -> str:
  """
  Return a compact query logic label.
  """
  query_root = getattr(target_dataset, "query_root", None)
  query_head = getattr(target_dataset, "query_head", None) or query_root
  if query_root is None:
    return "Standard"

  return str(getattr(query_head, "node_type", "") or "Custom")


def _health_label(health_level: str) -> str:
  """
  Return the compact health label used by consumer readiness views.
  """
  return {
    "ok": "OK",
    "warning": "Needs review",
    "error": "Issues",
  }.get(health_level, health_level or "Unknown")


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
  Return consumer-facing TargetSchema options for the Catalog filter.
  """
  layers = tuple(
    TargetSchema.objects
    .filter(short_name__in=CONSUMER_LAYER_ORDER)
    .order_by("short_name")
  )
  return tuple(
    {
      "value": layer.short_name,
      "label": layer.short_name,
      "selected": layer.short_name == selected,
    }
    for layer in sorted(
      layers,
      key=lambda layer: CONSUMER_LAYER_ORDER.index(layer.short_name),
    )
  )


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
