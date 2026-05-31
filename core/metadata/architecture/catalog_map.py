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
from typing import Any

from django.db.models import Count, Q
from django.urls import reverse

from metadata.models import TargetDataset, TargetDatasetInput, TargetSchema


LAYER_DATASET_LIMIT = 5
TRANSITION_EXAMPLE_LIMIT = 5
CANONICAL_LAYER_ORDER = (
  "raw",
  "stage",
  "rawcore",
  "bizcore",
  "serving",
)


@dataclass(frozen=True)
class ArchitectureCatalogMapDatasetItem:
  """
  Read-only dataset reference displayed inside the Architecture Catalog Map.
  """
  target_dataset_id: int
  dataset_key: str
  schema_short: str
  target_dataset_name: str
  description: str
  active: bool
  upstream_count: int
  downstream_count: int
  has_query_root: bool
  catalog_detail_url: str
  lineage_url: str

  @property
  def status_label(self) -> str:
    """
    Return the compact lifecycle label used by the Catalog Map.
    """
    return "Active" if self.active else "Inactive"

  @property
  def query_logic_label(self) -> str:
    """
    Return the compact query logic label used by the Catalog Map.
    """
    return "Custom query" if self.has_query_root else "Standard"


@dataclass(frozen=True)
class ArchitectureCatalogLayerSummary:
  """
  Read-only layer summary displayed in the Architecture Catalog Map.
  """
  schema_short: str
  display_name: str
  description: str
  dataset_count: int
  active_dataset_count: int
  inactive_dataset_count: int
  incoming_transition_count: int
  outgoing_transition_count: int
  custom_query_count: int
  dataset_examples: tuple[ArchitectureCatalogMapDatasetItem, ...]
  remaining_datasets: tuple[ArchitectureCatalogMapDatasetItem, ...]

  @property
  def remaining_dataset_count(self) -> int:
    """
    Return the number of collapsed datasets in this layer.
    """
    return len(self.remaining_datasets)

  @property
  def has_more(self) -> bool:
    """
    Return whether the layer contains additional datasets beyond the examples.
    """
    return bool(self.remaining_datasets)


@dataclass(frozen=True)
class ArchitectureCatalogTransitionExample:
  """
  Read-only example for one direct TargetDataset dependency.
  """
  source_dataset_key: str
  target_dataset_key: str
  source_catalog_detail_url: str
  target_catalog_detail_url: str
  target_lineage_url: str
  role_label: str


@dataclass(frozen=True)
class ArchitectureCatalogLayerTransition:
  """
  Read-only direct dependency summary between two Catalog layers.
  """
  source_schema_short: str
  target_schema_short: str
  count: int
  examples: tuple[ArchitectureCatalogTransitionExample, ...]
  remaining_examples: tuple[ArchitectureCatalogTransitionExample, ...]

  @property
  def remaining_example_count(self) -> int:
    """
    Return the number of collapsed dependency examples in this transition.
    """
    return len(self.remaining_examples)

  @property
  def has_more(self) -> bool:
    """
    Return whether the transition contains additional examples.
    """
    return bool(self.remaining_examples)

  @property
  def label(self) -> str:
    """
    Return the compact transition label.
    """
    return f"{self.source_schema_short} → {self.target_schema_short}"


@dataclass(frozen=True)
class ArchitectureCatalogLayerFlowStep:
  """
  Read-only layer step displayed in the Catalog Map flow overview.
  """
  schema_short: str
  display_name: str
  dataset_count: int
  active_dataset_count: int
  next_schema_short: str
  next_transition_count: int

  @property
  def has_next_layer(self) -> bool:
    """
    Return whether the flow step points to another displayed layer.
    """
    return bool(self.next_schema_short)


@dataclass(frozen=True)
class ArchitectureCatalogLayerMatrixColumn:
  """
  Read-only layer column displayed in the Catalog Map dependency matrix.
  """
  schema_short: str
  display_name: str


@dataclass(frozen=True)
class ArchitectureCatalogLayerMatrixCell:
  """
  Read-only dependency count for one source-to-target layer pair.
  """
  target_schema_short: str
  count: int

  @property
  def has_count(self) -> bool:
    """
    Return whether the matrix cell contains direct dependencies.
    """
    return self.count > 0


@dataclass(frozen=True)
class ArchitectureCatalogLayerMatrixRow:
  """
  Read-only dependency matrix row for one source layer.
  """
  source_schema_short: str
  source_display_name: str
  cells: tuple[ArchitectureCatalogLayerMatrixCell, ...]
  total_count: int

  @property
  def has_count(self) -> bool:
    """
    Return whether this source layer has outgoing direct dependencies.
    """
    return self.total_count > 0


@dataclass(frozen=True)
class ArchitectureCatalogMapContext:
  """
  Template context for the Architecture Catalog Map page.
  """
  layer_summaries: tuple[ArchitectureCatalogLayerSummary, ...]
  layer_flow_steps: tuple[ArchitectureCatalogLayerFlowStep, ...]
  layer_matrix_columns: tuple[ArchitectureCatalogLayerMatrixColumn, ...]
  layer_matrix_rows: tuple[ArchitectureCatalogLayerMatrixRow, ...]
  matrix_total_dependency_count: int
  transitions: tuple[ArchitectureCatalogLayerTransition, ...]
  total_dataset_count: int
  active_dataset_count: int
  catalog_url: str
  insights_url: str


def build_architecture_catalog_map_context() -> dict[str, Any]:
  """
  Build a read-only layer and dependency map for the Architecture Catalog.
  """
  datasets = tuple(_map_dataset_queryset())
  dataset_items_by_id = {
    dataset.pk: _dataset_item(dataset)
    for dataset in datasets
  }
  transitions = _transition_summaries(dataset_items_by_id)
  incoming_counts = _transition_counts_by_layer(transitions, direction="incoming")
  outgoing_counts = _transition_counts_by_layer(transitions, direction="outgoing")

  layer_summaries = _layer_summaries(
    datasets,
    dataset_items_by_id,
    incoming_counts,
    outgoing_counts,
  )

  layer_matrix_columns, layer_matrix_rows = _layer_matrix(
    layer_summaries,
    transitions,
  )

  context = ArchitectureCatalogMapContext(
    layer_summaries=layer_summaries,
    layer_flow_steps=_layer_flow_steps(layer_summaries, transitions),
    layer_matrix_columns=layer_matrix_columns,
    layer_matrix_rows=layer_matrix_rows,
    matrix_total_dependency_count=sum(transition.count for transition in transitions),
    transitions=transitions,
    total_dataset_count=len(datasets),
    active_dataset_count=sum(1 for dataset in datasets if dataset.active),
    catalog_url=reverse("architecture_catalog"),
    insights_url=reverse("architecture_catalog_insights"),
  )

  return context.__dict__


def _map_dataset_queryset():
  """
  Return TargetDatasets with direct dependency counts for the Catalog Map.
  """
  return (
    TargetDataset.objects
    .select_related("target_schema", "query_root")
    .annotate(
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


def _transition_queryset():
  """
  Return active direct TargetDataset dependency links for the Catalog Map.
  """
  return (
    TargetDatasetInput.objects
    .filter(
      active=True,
      upstream_target_dataset__isnull=False,
    )
    .select_related(
      "target_dataset",
      "target_dataset__target_schema",
      "upstream_target_dataset",
      "upstream_target_dataset__target_schema",
    )
    .order_by(
      "upstream_target_dataset__target_schema__short_name",
      "target_dataset__target_schema__short_name",
      "upstream_target_dataset__target_dataset_name",
      "target_dataset__target_dataset_name",
      "id",
    )
  )


def _layer_summaries(
  datasets: tuple[TargetDataset, ...],
  dataset_items_by_id: dict[int, ArchitectureCatalogMapDatasetItem],
  incoming_counts: dict[str, int],
  outgoing_counts: dict[str, int],
) -> tuple[ArchitectureCatalogLayerSummary, ...]:
  """
  Return read-only Catalog Map summaries for TargetSchemas.
  """
  datasets_by_layer: dict[str, list[ArchitectureCatalogMapDatasetItem]] = defaultdict(list)
  for dataset in datasets:
    datasets_by_layer[dataset.target_schema.short_name].append(
      dataset_items_by_id[dataset.pk],
    )

  layers = tuple(
    TargetSchema.objects.order_by("short_name")
  )

  return tuple(
    _layer_summary(
      layer,
      tuple(datasets_by_layer.get(layer.short_name, ())),
      incoming_counts,
      outgoing_counts,
    )
    for layer in sorted(layers, key=lambda layer: _layer_sort_key(layer.short_name))
  )


def _layer_summary(
  layer: TargetSchema,
  datasets: tuple[ArchitectureCatalogMapDatasetItem, ...],
  incoming_counts: dict[str, int],
  outgoing_counts: dict[str, int],
) -> ArchitectureCatalogLayerSummary:
  """
  Return one read-only Catalog Map layer summary.
  """
  examples = datasets[:LAYER_DATASET_LIMIT]
  remaining_datasets = datasets[LAYER_DATASET_LIMIT:]

  return ArchitectureCatalogLayerSummary(
    schema_short=layer.short_name,
    display_name=layer.display_name or layer.short_name,
    description=layer.description or "",
    dataset_count=len(datasets),
    active_dataset_count=sum(1 for dataset in datasets if dataset.active),
    inactive_dataset_count=sum(1 for dataset in datasets if not dataset.active),
    incoming_transition_count=incoming_counts.get(layer.short_name, 0),
    outgoing_transition_count=outgoing_counts.get(layer.short_name, 0),
    custom_query_count=sum(1 for dataset in datasets if dataset.has_query_root),
    dataset_examples=examples,
    remaining_datasets=remaining_datasets,
  )


def _layer_flow_steps(
  layer_summaries: tuple[ArchitectureCatalogLayerSummary, ...],
  transitions: tuple[ArchitectureCatalogLayerTransition, ...],
) -> tuple[ArchitectureCatalogLayerFlowStep, ...]:
  """
  Return compact flow steps for layers that contain datasets.
  """
  visible_layers = tuple(
    layer
    for layer in layer_summaries
    if layer.dataset_count > 0
  )
  transition_counts = {
    (transition.source_schema_short, transition.target_schema_short): transition.count
    for transition in transitions
  }

  return tuple(
    _layer_flow_step(
      layer,
      visible_layers[index + 1] if index + 1 < len(visible_layers) else None,
      transition_counts,
    )
    for index, layer in enumerate(visible_layers)
  )


def _layer_flow_step(
  layer: ArchitectureCatalogLayerSummary,
  next_layer: ArchitectureCatalogLayerSummary | None,
  transition_counts: dict[tuple[str, str], int],
) -> ArchitectureCatalogLayerFlowStep:
  """
  Return one compact layer flow step.
  """
  next_schema_short = next_layer.schema_short if next_layer else ""

  return ArchitectureCatalogLayerFlowStep(
    schema_short=layer.schema_short,
    display_name=layer.display_name,
    dataset_count=layer.dataset_count,
    active_dataset_count=layer.active_dataset_count,
    next_schema_short=next_schema_short,
    next_transition_count=transition_counts.get(
      (layer.schema_short, next_schema_short),
      0,
    ),
  )


def _layer_matrix(
  layer_summaries: tuple[ArchitectureCatalogLayerSummary, ...],
  transitions: tuple[ArchitectureCatalogLayerTransition, ...],
) -> tuple[
  tuple[ArchitectureCatalogLayerMatrixColumn, ...],
  tuple[ArchitectureCatalogLayerMatrixRow, ...],
]:
  """
  Return a direct dependency matrix across populated Catalog layers.
  """
  matrix_layers = tuple(
    layer
    for layer in layer_summaries
    if layer.dataset_count > 0
  )
  columns = tuple(
    ArchitectureCatalogLayerMatrixColumn(
      schema_short=layer.schema_short,
      display_name=layer.display_name,
    )
    for layer in matrix_layers
  )
  transition_counts = {
    (transition.source_schema_short, transition.target_schema_short): transition.count
    for transition in transitions
  }

  rows = tuple(
    _layer_matrix_row(
      layer,
      columns,
      transition_counts,
    )
    for layer in matrix_layers
  )

  return columns, rows


def _layer_matrix_row(
  source_layer: ArchitectureCatalogLayerSummary,
  columns: tuple[ArchitectureCatalogLayerMatrixColumn, ...],
  transition_counts: dict[tuple[str, str], int],
) -> ArchitectureCatalogLayerMatrixRow:
  """
  Return one direct dependency matrix row for a source layer.
  """
  cells = tuple(
    ArchitectureCatalogLayerMatrixCell(
      target_schema_short=column.schema_short,
      count=transition_counts.get(
        (source_layer.schema_short, column.schema_short),
        0,
      ),
    )
    for column in columns
  )

  return ArchitectureCatalogLayerMatrixRow(
    source_schema_short=source_layer.schema_short,
    source_display_name=source_layer.display_name,
    cells=cells,
    total_count=sum(cell.count for cell in cells),
  )


def _transition_summaries(
  dataset_items_by_id: dict[int, ArchitectureCatalogMapDatasetItem],
) -> tuple[ArchitectureCatalogLayerTransition, ...]:
  """
  Return direct dependency summaries grouped by source and target layer.
  """
  buckets: dict[tuple[str, str], list[ArchitectureCatalogTransitionExample]] = defaultdict(list)

  for link in _transition_queryset():
    source = link.upstream_target_dataset
    target = link.target_dataset
    source_layer = source.target_schema.short_name
    target_layer = target.target_schema.short_name
    key = (source_layer, target_layer)

    source_item = dataset_items_by_id.get(source.pk) or _dataset_item(source)
    target_item = dataset_items_by_id.get(target.pk) or _dataset_item(target)
    buckets[key].append(
      ArchitectureCatalogTransitionExample(
        source_dataset_key=source_item.dataset_key,
        target_dataset_key=target_item.dataset_key,
        source_catalog_detail_url=source_item.catalog_detail_url,
        target_catalog_detail_url=target_item.catalog_detail_url,
        target_lineage_url=target_item.lineage_url,
        role_label=_display_value(link, "role"),
      ),
    )

  return tuple(
    _transition_summary(source_layer, target_layer, tuple(examples))
    for (source_layer, target_layer), examples in sorted(
      buckets.items(),
      key=lambda item: (
        _layer_sort_key(item[0][0]),
        _layer_sort_key(item[0][1]),
      ),
    )
  )


def _transition_summary(
  source_layer: str,
  target_layer: str,
  examples: tuple[ArchitectureCatalogTransitionExample, ...],
) -> ArchitectureCatalogLayerTransition:
  """
  Return one read-only Catalog Map transition summary.
  """
  return ArchitectureCatalogLayerTransition(
    source_schema_short=source_layer,
    target_schema_short=target_layer,
    count=len(examples),
    examples=examples[:TRANSITION_EXAMPLE_LIMIT],
    remaining_examples=examples[TRANSITION_EXAMPLE_LIMIT:],
  )


def _transition_counts_by_layer(
  transitions: tuple[ArchitectureCatalogLayerTransition, ...],
  *,
  direction: str,
) -> dict[str, int]:
  """
  Return transition counts keyed by source or target layer.
  """
  counts: dict[str, int] = defaultdict(int)
  for transition in transitions:
    layer = (
      transition.target_schema_short
      if direction == "incoming"
      else transition.source_schema_short
    )
    counts[layer] += transition.count
  return dict(counts)


def _dataset_item(target_dataset: TargetDataset) -> ArchitectureCatalogMapDatasetItem:
  """
  Return a read-only Catalog Map dataset item.
  """
  schema_short = target_dataset.target_schema.short_name
  target_name = target_dataset.target_dataset_name
  dataset_key = f"{schema_short}.{target_name}"

  return ArchitectureCatalogMapDatasetItem(
    target_dataset_id=target_dataset.pk,
    dataset_key=dataset_key,
    schema_short=schema_short,
    target_dataset_name=target_name,
    description=target_dataset.description or "",
    active=bool(target_dataset.active),
    upstream_count=_upstream_target_count(target_dataset),
    downstream_count=_downstream_target_count(target_dataset),
    has_query_root=bool(getattr(target_dataset, "query_root", None)),
    catalog_detail_url=reverse("architecture_catalog_detail", args=[target_dataset.pk]),
    lineage_url=reverse("targetdataset_lineage", args=[target_dataset.pk]),
  )


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


def _display_value(instance: Any, field_name: str) -> str:
  """
  Return the display value for a model field when available.
  """
  display = getattr(instance, f"get_{field_name}_display", None)
  return str(display() if callable(display) else getattr(instance, field_name, "") or "")


def _layer_sort_key(schema_short: str) -> tuple[int, str]:
  """
  Return the stable Catalog Map ordering key for a layer.
  """
  try:
    return CANONICAL_LAYER_ORDER.index(schema_short), schema_short
  except ValueError:
    return len(CANONICAL_LAYER_ORDER), schema_short
