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

from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
from typing import Any, TYPE_CHECKING

from metadata.generation import naming
from metadata.generation.mappers import build_surrogate_key_column_draft
from metadata.generation.target_generation_plan import (
  TargetGenerationAction,
  TargetGenerationPlan,
  build_target_generation_plan,
)
from metadata.models import (
  TargetColumn,
  TargetColumnInput,
  TargetDataset,
  TargetDatasetReference,
  TargetDatasetReferenceComponent,
  TargetSchema,
)

if TYPE_CHECKING:
  from metadata.generation.target_generation_service import (
    TargetGenerationService,
  )


_UNIQUE_SYSTEM_ROLES = frozenset({
  "surrogate_key",
  "row_hash",
  "payload",
  "load_run_id",
  "loaded_at",
  "version_started_at",
  "version_ended_at",
  "version_state",
})
_HIST_TAIL_ROLES = frozenset({
  "version_started_at",
  "version_ended_at",
  "version_state",
  "load_run_id",
  "loaded_at",
})


@dataclass
class _ColumnProjection:
  """Projected semantic state for one target column."""

  object_key: str
  state: dict[str, Any]
  inputs: list[dict[str, Any]]
  current: TargetColumn | None
  source_keys: tuple[str, ...]
  effect_origin: str = "DIRECT"
  reason: str = ""


@dataclass
class _DatasetProjection:
  """Projected semantic state for one target dataset bundle."""

  dataset_key: str
  state: dict[str, Any]
  inputs: list[dict[str, Any]]
  current: TargetDataset | None
  source_keys: tuple[str, ...]
  columns: list[_ColumnProjection]
  effect_origin: str = "DIRECT"
  reason: str = ""


class TargetGenerationPlanner:
  """
  Derive an immutable Target Generation Plan without mutating metadata.

  Planning deliberately reuses the existing service's pure bucket, naming,
  draft, upstream-resolution, and policy helpers. Persistence helpers and
  model save methods are never called. Existing apply and signal behavior
  remain unchanged.
  """

  def __init__(self, service: "TargetGenerationService") -> None:
    self.service = service

  def build_schema_plan(
    self,
    eligible_source_datasets,
    target_schema,
    *,
    reconcile_lifecycle: bool = False,
  ) -> TargetGenerationPlan:
    """Build one schema-scoped plan from current service semantics."""
    source_datasets = list(eligible_source_datasets)
    source_keys = tuple(
      self._source_dataset_key(source_dataset)
      for source_dataset in source_datasets
    )

    source_fingerprint = self._source_metadata_fingerprint(
      source_datasets=source_datasets,
      target_schema=target_schema,
    )
    target_fingerprint = self._target_metadata_fingerprint(
      target_schema=target_schema,
    )

    projections: list[_DatasetProjection] = []
    expected_current_ids: set[int] = set()

    buckets = self.service._bucket_source_datasets(
      source_datasets,
      target_schema,
    )
    for physical_name, src_list in buckets.items():
      base_projection = self._project_base_dataset(
        physical_name=physical_name,
        src_list=src_list,
        target_schema=target_schema,
      )
      projections.append(base_projection)
      if base_projection.current is not None:
        expected_current_ids.add(base_projection.current.pk)

      if (
        target_schema.short_name == "rawcore"
        and bool(base_projection.state.get("historize"))
        and not bool(base_projection.state.get("is_hist"))
      ):
        self._apply_rawcore_key_former_names(base_projection)
        hist_projection = self._project_hist_dataset(
          base_projection=base_projection,
          target_schema=target_schema,
        )
        projections.append(hist_projection)
        if hist_projection.current is not None:
          expected_current_ids.add(hist_projection.current.pk)

    actions: list[TargetGenerationAction] = []
    for projection in projections:
      actions.extend(self._actions_for_projection(projection))

    if reconcile_lifecycle:
      actions.extend(
        self._lifecycle_actions(
          target_schema=target_schema,
          expected_current_ids=expected_current_ids,
          projections=projections,
        )
      )

    return build_target_generation_plan(
      scope_mode="schema",
      target_schema_short_names=(target_schema.short_name,),
      source_dataset_keys=source_keys,
      reconcile_lifecycle=reconcile_lifecycle,
      source_metadata_fingerprint=source_fingerprint,
      target_metadata_fingerprint=target_fingerprint,
      actions=actions,
    )

  def _project_base_dataset(
    self,
    *,
    physical_name: str,
    src_list: list,
    target_schema,
  ) -> _DatasetProjection:
    representative = src_list[0]
    bundle = self.service.build_dataset_bundle(
      representative,
      target_schema,
    )
    dataset_draft = bundle["dataset"]
    column_drafts = bundle["columns"]
    dataset_draft.target_dataset_name = physical_name

    lineage_key = self.service.build_lineage_key_for_bucket(
      target_schema,
      src_list,
    )
    dataset_key = self._generated_dataset_key(
      target_schema=target_schema,
      lineage_key=lineage_key,
      is_hist=False,
    )
    source_keys = tuple(
      self._source_dataset_key(source_dataset)
      for source_dataset in src_list
    )

    current, lookup_mode = self._find_base_dataset(
      target_schema=target_schema,
      lineage_key=lineage_key,
      target_dataset_name=physical_name,
    )
    state = self._desired_base_dataset_state(
      current=current,
      lookup_mode=lookup_mode,
      target_schema=target_schema,
      dataset_draft=dataset_draft,
      src_list=src_list,
      lineage_key=lineage_key,
    )

    dataset_inputs, upstreams = self._desired_base_dataset_inputs(
      target_schema=target_schema,
      src_list=src_list,
      representative=representative,
    )

    dataset_name = state["target_dataset_name"]
    for col_draft in column_drafts:
      if (
        (getattr(col_draft, "system_role", "") or "").strip()
        == "surrogate_key"
      ):
        col_draft.target_column_name = f"{dataset_name}_key"

    if target_schema.short_name == "stage" and len(src_list) > 1:
      column_drafts = self.service._extend_stage_column_drafts_for_union(
        target_schema=target_schema,
        column_drafts=column_drafts,
        src_list=src_list,
      )

    columns = self._project_base_columns(
      dataset_key=dataset_key,
      current_dataset=current,
      target_schema=target_schema,
      column_drafts=column_drafts,
      source_keys=source_keys,
      upstream_raw_dataset=upstreams.get("raw"),
      upstream_stage_dataset=upstreams.get("stage"),
    )

    return _DatasetProjection(
      dataset_key=dataset_key,
      state=state,
      inputs=dataset_inputs,
      current=current,
      source_keys=source_keys,
      columns=columns,
      reason="Generated target dataset derived from the selected source bucket.",
    )

  def _find_base_dataset(
    self,
    *,
    target_schema,
    lineage_key: str,
    target_dataset_name: str,
  ) -> tuple[TargetDataset | None, str]:
    qs = TargetDataset.objects.filter(
      target_schema=target_schema,
      lineage_key=lineage_key,
    )
    if target_schema.short_name == "rawcore":
      qs = qs.exclude(target_dataset_name__endswith="_hist")

    current = qs.first()
    if current is not None:
      return current, "lineage"

    current = TargetDataset.objects.filter(
      target_schema=target_schema,
      target_dataset_name=target_dataset_name,
    ).first()
    if current is not None:
      return current, "name"
    return None, "create"

  def _desired_base_dataset_state(
    self,
    *,
    current: TargetDataset | None,
    lookup_mode: str,
    target_schema,
    dataset_draft,
    src_list: list,
    lineage_key: str,
  ) -> dict[str, Any]:
    incremental_strategy = self.service._determine_incremental_strategy(
      target_schema,
      src_list,
    )
    incremental_source = self.service._determine_incremental_source(src_list)
    if incremental_strategy == "full":
      incremental_source = None

    combination_mode = self.service._determine_combination_mode(
      target_schema,
      src_list,
    )

    if current is None:
      return {
        "target_schema_short_name": target_schema.short_name,
        "target_dataset_name": dataset_draft.target_dataset_name,
        "description": dataset_draft.description,
        "handle_deletes": True,
        "historize": bool(
          getattr(target_schema, "default_historize", False)
        ),
        "combination_mode": combination_mode,
        "incremental_strategy": incremental_strategy,
        "incremental_source_key": (
          self._source_dataset_key(incremental_source)
          if incremental_source is not None
          else None
        ),
        "active": True,
        "retired_at_state": "unset",
        "lineage_key": lineage_key,
        "former_names": [],
        "is_system_managed": bool(dataset_draft.is_system_managed),
        "is_hist": (
          target_schema.short_name == "rawcore"
          and incremental_strategy == "historize"
        ),
      }

    state = self._dataset_state(current)
    direct_save = False

    if lookup_mode == "lineage":
      updates = {
        "description": dataset_draft.description,
        "is_system_managed": bool(dataset_draft.is_system_managed),
        "combination_mode": combination_mode,
      }
      for field_name, value in updates.items():
        if state[field_name] != value:
          state[field_name] = value
          direct_save = True

    if state["lineage_key"] != lineage_key:
      state["lineage_key"] = lineage_key
      direct_save = True

    if getattr(target_schema, "is_system_managed", False):
      incremental_source_key = (
        self._source_dataset_key(incremental_source)
        if incremental_source is not None
        else None
      )
      if state["incremental_strategy"] != incremental_strategy:
        state["incremental_strategy"] = incremental_strategy
        direct_save = True
      if state["incremental_source_key"] != incremental_source_key:
        state["incremental_source_key"] = incremental_source_key
        direct_save = True

    if direct_save:
      state = self._apply_dataset_save_side_effects(state)
    state["is_hist"] = (
      target_schema.short_name == "rawcore"
      and state["incremental_strategy"] == "historize"
    )
    return state

  def _desired_base_dataset_inputs(
    self,
    *,
    target_schema,
    src_list: list,
    representative,
  ) -> tuple[list[dict[str, Any]], dict[str, TargetDataset | None]]:
    inputs: list[dict[str, Any]] = []
    upstreams: dict[str, TargetDataset | None] = {
      "raw": None,
      "stage": None,
    }

    if target_schema.short_name == "rawcore":
      stage_schema = TargetSchema.objects.filter(short_name="stage").first()
      if stage_schema is None:
        return inputs, upstreams

      stage_lineage_key = self.service.build_lineage_key_for_bucket(
        stage_schema,
        src_list,
      )
      stage_dataset = TargetDataset.objects.filter(
        target_schema=stage_schema,
        lineage_key=stage_lineage_key,
      ).first()
      if stage_dataset is None:
        stage_name = naming.build_physical_dataset_name(
          target_schema=stage_schema,
          source_dataset=representative,
        )
        stage_dataset = TargetDataset.objects.filter(
          target_schema=stage_schema,
          target_dataset_name=stage_name,
        ).first()

      upstreams["stage"] = stage_dataset
      if stage_dataset is not None:
        inputs.append({
          "kind": "upstream_target_dataset",
          "upstream_key": self._current_dataset_key(stage_dataset),
          "role": self.service._resolve_role_for_source_dataset(
            representative
          ),
          "active": True,
        })
      return self._sorted_inputs(inputs), upstreams

    if target_schema.short_name == "stage":
      raw_schema = TargetSchema.objects.filter(short_name="raw").first()
      for src_ds in src_list:
        role = self.service._resolve_role_for_source_dataset(src_ds)
        generate_raw = src_ds.generate_raw_table
        if generate_raw is None:
          generate_raw = getattr(
            src_ds.source_system,
            "generate_raw_tables",
            False,
          )

        raw_dataset = None
        if generate_raw and raw_schema is not None:
          raw_lineage_key = self.service.build_lineage_key_for_bucket(
            raw_schema,
            [src_ds],
          )
          raw_dataset = TargetDataset.objects.filter(
            target_schema=raw_schema,
            lineage_key=raw_lineage_key,
          ).first()
          if raw_dataset is None:
            raw_name = naming.build_physical_dataset_name(
              target_schema=raw_schema,
              source_dataset=src_ds,
            )
            raw_dataset = TargetDataset.objects.filter(
              target_schema=raw_schema,
              target_dataset_name=raw_name,
            ).first()

        if raw_dataset is not None:
          if upstreams["raw"] is None:
            upstreams["raw"] = raw_dataset
          inputs.append({
            "kind": "upstream_target_dataset",
            "upstream_key": self._current_dataset_key(raw_dataset),
            "role": role,
            "active": True,
          })
        else:
          inputs.append({
            "kind": "source_dataset",
            "source_key": self._source_dataset_key(src_ds),
            "role": role,
            "active": True,
          })
      return self._sorted_inputs(inputs), upstreams

    for src_ds in src_list:
      inputs.append({
        "kind": "source_dataset",
        "source_key": self._source_dataset_key(src_ds),
        "role": self.service._resolve_role_for_source_dataset(src_ds),
        "active": True,
      })
    return self._sorted_inputs(inputs), upstreams

  def _project_base_columns(
    self,
    *,
    dataset_key: str,
    current_dataset: TargetDataset | None,
    target_schema,
    column_drafts: list,
    source_keys: tuple[str, ...],
    upstream_raw_dataset: TargetDataset | None,
    upstream_stage_dataset: TargetDataset | None,
  ) -> list[_ColumnProjection]:
    current_columns = (
      list(
        TargetColumn.objects
        .filter(target_dataset=current_dataset)
        .order_by("ordinal_position", "id")
      )
      if current_dataset is not None
      else []
    )
    used_current_ids: set[int] = set()
    generated: list[_ColumnProjection] = []

    for col_draft in column_drafts:
      upstream_column = self._resolve_upstream_column(
        target_schema=target_schema,
        col_draft=col_draft,
        upstream_raw_dataset=upstream_raw_dataset,
        upstream_stage_dataset=upstream_stage_dataset,
      )
      if (
        target_schema.short_name == "rawcore"
        and upstream_column is not None
      ):
        col_draft.nullable = bool(upstream_column.nullable)

      current_column = self._match_current_column(
        current_dataset=current_dataset,
        target_schema=target_schema,
        col_draft=col_draft,
        upstream_column=upstream_column,
      )
      if current_column is not None:
        used_current_ids.add(current_column.pk)

      object_key = self._desired_column_key(
        col_draft=col_draft,
        upstream_column=upstream_column,
      )
      state = self._desired_generated_column_state(
        current=current_column,
        col_draft=col_draft,
      )
      inputs = self._desired_column_inputs(
        current=current_column,
        target_schema=target_schema,
        source_column_id=getattr(col_draft, "source_column_id", None),
        upstream_column=upstream_column,
      )
      generated.append(
        _ColumnProjection(
          object_key=f"{dataset_key}:column:{object_key}",
          state=state,
          inputs=inputs,
          current=current_column,
          source_keys=source_keys,
          effect_origin=(
            "MODEL_SIDE_EFFECT"
            if (
              current_column is not None
              and current_column.target_column_name
              != state["target_column_name"]
            )
            else "DIRECT"
          ),
          reason="Generated column derived from the existing mapping semantics.",
        )
      )

    extras: list[_ColumnProjection] = []
    for current_column in current_columns:
      if current_column.pk in used_current_ids:
        continue
      extras.append(
        _ColumnProjection(
          object_key=(
            f"{dataset_key}:column:"
            f"{self._current_column_key(current_column)}"
          ),
          state=self._column_state(current_column),
          inputs=self._column_inputs_state(current_column),
          current=current_column,
          source_keys=source_keys,
          reason="Existing non-generator column retained by current apply semantics.",
        )
      )

    projections = generated + extras
    if getattr(target_schema, "is_system_managed", False):
      for ordinal, projection in enumerate(projections, start=1):
        projection.state["ordinal_position"] = ordinal

    self._project_technical_columns(
      projections=projections,
      target_schema=target_schema,
      dataset_key=dataset_key,
      source_keys=source_keys,
    )
    return projections

  def _resolve_upstream_column(
    self,
    *,
    target_schema,
    col_draft,
    upstream_raw_dataset: TargetDataset | None,
    upstream_stage_dataset: TargetDataset | None,
  ) -> TargetColumn | None:
    source_column_id = getattr(col_draft, "source_column_id", None)

    if (
      target_schema.short_name == "stage"
      and source_column_id
      and upstream_raw_dataset is not None
    ):
      return (
        TargetColumn.objects
        .filter(
          target_dataset=upstream_raw_dataset,
          input_links__source_column_id=source_column_id,
        )
        .distinct()
        .first()
      )

    if (
      target_schema.short_name == "rawcore"
      and source_column_id
      and upstream_stage_dataset is not None
    ):
      upstream_column = (
        TargetColumn.objects
        .filter(
          target_dataset=upstream_stage_dataset,
          input_links__source_column_id=source_column_id,
        )
        .distinct()
        .first()
      )
      if upstream_column is not None:
        return upstream_column

      raw_column = (
        TargetColumn.objects
        .filter(
          target_dataset__target_schema__short_name="raw",
          input_links__source_column_id=source_column_id,
        )
        .distinct()
        .first()
      )
      if raw_column is not None:
        upstream_column = (
          TargetColumn.objects
          .filter(
            target_dataset=upstream_stage_dataset,
            input_links__upstream_target_column=raw_column,
          )
          .distinct()
          .first()
        )
        if upstream_column is not None:
          return upstream_column

    if (
      target_schema.short_name == "rawcore"
      and upstream_stage_dataset is not None
    ):
      return TargetColumn.objects.filter(
        target_dataset=upstream_stage_dataset,
        target_column_name=col_draft.target_column_name,
        active=True,
      ).first()

    return None

  def _match_current_column(
    self,
    *,
    current_dataset: TargetDataset | None,
    target_schema,
    col_draft,
    upstream_column: TargetColumn | None,
  ) -> TargetColumn | None:
    if current_dataset is None:
      return None

    source_column_id = getattr(col_draft, "source_column_id", None)
    current = None

    if target_schema.short_name == "rawcore" and upstream_column is not None:
      current = (
        TargetColumn.objects
        .filter(
          target_dataset=current_dataset,
          input_links__upstream_target_column=upstream_column,
        )
        .distinct()
        .first()
      )
    elif source_column_id:
      current = (
        TargetColumn.objects
        .filter(
          target_dataset=current_dataset,
          input_links__source_column_id=source_column_id,
        )
        .distinct()
        .first()
      )

    role = (getattr(col_draft, "system_role", "") or "").strip()
    is_surrogate = role == "surrogate_key"
    if not is_surrogate:
      is_surrogate = (
        col_draft.target_column_name
        == f"{current_dataset.target_dataset_name}_key"
      )

    if current is None and is_surrogate:
      current = TargetColumn.objects.filter(
        target_dataset=current_dataset,
        system_role="surrogate_key",
      ).first()

    if current is None:
      current = TargetColumn.objects.filter(
        target_dataset=current_dataset,
        target_column_name=col_draft.target_column_name,
      ).first()
    return current

  def _desired_generated_column_state(
    self,
    *,
    current: TargetColumn | None,
    col_draft,
  ) -> dict[str, Any]:
    if current is None:
      return {
        "target_column_name": col_draft.target_column_name,
        "ordinal_position": col_draft.ordinal_position or 1,
        "datatype": col_draft.datatype,
        "max_length": col_draft.max_length,
        "decimal_precision": col_draft.decimal_precision,
        "decimal_scale": col_draft.decimal_scale,
        "nullable": bool(col_draft.nullable),
        "system_role": col_draft.system_role or "",
        "lineage_origin": col_draft.lineage_origin or "direct",
        "surrogate_expression": col_draft.surrogate_expression,
        "description": None,
        "remark": None,
        "active": True,
        "retired_at_state": "unset",
        "lineage_key": None,
        "former_names": [],
        "is_system_managed": True,
      }

    state = self._column_state(current)
    update_values = {
      "datatype": col_draft.datatype,
      "max_length": col_draft.max_length,
      "decimal_precision": col_draft.decimal_precision,
      "decimal_scale": col_draft.decimal_scale,
      "nullable": bool(col_draft.nullable),
      "surrogate_expression": col_draft.surrogate_expression,
      "is_system_managed": True,
    }
    draft_role = (getattr(col_draft, "system_role", "") or "").strip()
    if draft_role:
      update_values["system_role"] = draft_role
    draft_origin = (
      getattr(col_draft, "lineage_origin", "") or ""
    ).strip()
    if draft_origin:
      update_values["lineage_origin"] = draft_origin

    if (
      draft_role == "surrogate_key"
      and state["system_role"] == "surrogate_key"
    ):
      update_values["target_column_name"] = col_draft.target_column_name

    changed = False
    old_name = state["target_column_name"]
    for field_name, value in update_values.items():
      if state[field_name] != value:
        state[field_name] = value
        changed = True

    if old_name != state["target_column_name"]:
      state["former_names"] = self._append_former_name(
        state["former_names"],
        old_name,
      )

    if changed:
      state = self._apply_column_save_side_effects(state)
    return state

  def _desired_column_inputs(
    self,
    *,
    current: TargetColumn | None,
    target_schema,
    source_column_id: int | None,
    upstream_column: TargetColumn | None,
  ) -> list[dict[str, Any]]:
    current_inputs = (
      self._column_inputs_state(current)
      if current is not None
      else []
    )

    if target_schema.short_name == "rawcore":
      desired = [
        item
        for item in current_inputs
        if item["kind"] != "source_column"
      ]
      if upstream_column is not None:
        desired = self._ensure_column_input(
          desired,
          kind="upstream_target_column",
          upstream_key=self._current_column_reference_key(
            upstream_column
          ),
        )
      return self._sorted_inputs(desired)

    if target_schema.short_name == "stage" and upstream_column is not None:
      desired = [
        item
        for item in current_inputs
        if item["kind"] != "source_column"
      ]
      desired = self._ensure_column_input(
        desired,
        kind="upstream_target_column",
        upstream_key=self._current_column_reference_key(upstream_column),
      )
      return self._sorted_inputs(desired)

    desired = [
      item
      for item in current_inputs
      if item["kind"] != "upstream_target_column"
    ]
    if source_column_id:
      desired = self._ensure_column_input(
        desired,
        kind="source_column",
        source_key=self._source_column_key(source_column_id),
      )
    return self._sorted_inputs(desired)

  def _ensure_column_input(
    self,
    inputs: list[dict[str, Any]],
    *,
    kind: str,
    source_key: str | None = None,
    upstream_key: str | None = None,
  ) -> list[dict[str, Any]]:
    lookup_key = source_key or upstream_key
    key_name = "source_key" if source_key is not None else "upstream_key"
    for item in inputs:
      if item.get("kind") == kind and item.get(key_name) == lookup_key:
        return inputs

    inputs.append({
      "kind": kind,
      key_name: lookup_key,
      "manual_expression": None,
      "ordinal_position": 1,
      "active": True,
    })
    return inputs

  def _project_technical_columns(
    self,
    *,
    projections: list[_ColumnProjection],
    target_schema,
    dataset_key: str,
    source_keys: tuple[str, ...],
  ) -> None:
    layer = target_schema.short_name
    max_ordinal = max(
      (projection.state["ordinal_position"] for projection in projections),
      default=0,
    )

    for offset, spec in enumerate(
      self.service._tech_specs_for_layer(layer),
      start=1,
    ):
      projection = next(
        (
          item
          for item in projections
          if item.state["target_column_name"] == spec["name"]
        ),
        None,
      )
      if projection is None and not spec.get("create_if_missing", True):
        continue

      if projection is None:
        projection = _ColumnProjection(
          object_key=(
            f"{dataset_key}:column:system_role:{spec['system_role']}"
          ),
          state={
            "target_column_name": spec["name"],
            "ordinal_position": max_ordinal + offset,
            "datatype": spec["datatype"],
            "max_length": spec.get("max_length"),
            "decimal_precision": None,
            "decimal_scale": None,
            "nullable": bool(spec["nullable"]),
            "system_role": spec["system_role"],
            "lineage_origin": "direct",
            "surrogate_expression": None,
            "description": spec.get("description"),
            "remark": None,
            "active": True,
            "retired_at_state": "unset",
            "lineage_key": None,
            "former_names": [],
            "is_system_managed": True,
          },
          inputs=[],
          current=None,
          source_keys=source_keys,
          reason="System-managed technical column required by the target layer.",
        )
        projections.append(projection)
        continue

      updates = {
        "active": True,
        "is_system_managed": True,
        "system_role": spec["system_role"],
        "datatype": spec["datatype"],
        "max_length": spec.get("max_length"),
        "nullable": bool(spec["nullable"]),
        "description": spec.get("description"),
        "remark": None,
      }
      changed = False
      for field_name, value in updates.items():
        if projection.state[field_name] != value:
          projection.state[field_name] = value
          changed = True
      if changed:
        projection.state = self._apply_column_save_side_effects(
          projection.state
        )
        projection.reason = (
          "Existing technical column patched from the current layer registry."
        )

  def _apply_rawcore_key_former_names(
    self,
    projection: _DatasetProjection,
  ) -> None:
    key_name = f"{projection.state['target_dataset_name']}_key"
    key_projection = next(
      (
        column
        for column in projection.columns
        if column.state["target_column_name"] == key_name
      ),
      None,
    )
    if key_projection is None:
      return

    former_names = list(key_projection.state["former_names"])
    for old_dataset_name in projection.state["former_names"]:
      former_names = self._append_former_name(
        former_names,
        f"{old_dataset_name}_key",
      )
    if former_names != key_projection.state["former_names"]:
      key_projection.state["former_names"] = former_names
      key_projection.effect_origin = "MODEL_SIDE_EFFECT"
      key_projection.reason = (
        "Rawcore key rename history maintained by the existing rename helper."
      )

  def _project_hist_dataset(
    self,
    *,
    base_projection: _DatasetProjection,
    target_schema,
  ) -> _DatasetProjection:
    hist_name = f"{base_projection.state['target_dataset_name']}_hist"
    lineage_key = base_projection.state["lineage_key"]
    dataset_key = self._generated_dataset_key(
      target_schema=target_schema,
      lineage_key=lineage_key,
      is_hist=True,
    )

    current, lookup_mode = self._find_hist_dataset(
      target_schema=target_schema,
      lineage_key=lineage_key,
      hist_name=hist_name,
    )
    state = self._desired_hist_dataset_state(
      current=current,
      lookup_mode=lookup_mode,
      target_schema=target_schema,
      base_projection=base_projection,
      hist_name=hist_name,
      lineage_key=lineage_key,
    )
    inputs = self._desired_hist_dataset_inputs(
      current=current,
      base_projection=base_projection,
    )
    columns = self._project_hist_columns(
      current_hist=current,
      hist_dataset_key=dataset_key,
      hist_dataset_state=state,
      base_projection=base_projection,
      target_schema=target_schema,
    )

    return _DatasetProjection(
      dataset_key=dataset_key,
      state=state,
      inputs=inputs,
      current=current,
      source_keys=base_projection.source_keys,
      columns=columns,
      effect_origin="HISTORY_COMPANION",
      reason="History companion derived from the effective rawcore contract.",
    )

  def _find_hist_dataset(
    self,
    *,
    target_schema,
    lineage_key: str | None,
    hist_name: str,
  ) -> tuple[TargetDataset | None, str]:
    if lineage_key:
      current = TargetDataset.objects.filter(
        target_schema=target_schema,
        lineage_key=lineage_key,
        target_dataset_name__endswith="_hist",
      ).first()
      if current is not None:
        return current, "lineage"

    current = TargetDataset.objects.filter(
      target_schema=target_schema,
      target_dataset_name=hist_name,
    ).first()
    if current is not None:
      return current, "name"
    return None, "create"

  def _desired_hist_dataset_state(
    self,
    *,
    current: TargetDataset | None,
    lookup_mode: str,
    target_schema,
    base_projection: _DatasetProjection,
    hist_name: str,
    lineage_key: str | None,
  ) -> dict[str, Any]:
    if current is None:
      return {
        "target_schema_short_name": target_schema.short_name,
        "target_dataset_name": hist_name,
        "description": (
          "History table for "
          f"{base_projection.state['target_dataset_name']}"
        ),
        "handle_deletes": False,
        "historize": False,
        "combination_mode": "single",
        "incremental_strategy": "historize",
        "incremental_source_key": None,
        "active": True,
        "retired_at_state": "unset",
        "lineage_key": lineage_key,
        "former_names": [],
        "is_system_managed": True,
        "is_hist": True,
      }

    state = self._dataset_state(current)
    changed = False

    if lookup_mode == "lineage":
      updates = {
        "incremental_strategy": "historize",
        "target_dataset_name": hist_name,
        "historize": False,
        "handle_deletes": False,
        "is_system_managed": True,
      }
      for field_name, value in updates.items():
        if state[field_name] != value:
          state[field_name] = value
          changed = True

    if lineage_key and state["lineage_key"] != lineage_key:
      state["lineage_key"] = lineage_key
      changed = True

    clean_former_names = [
      value
      for value in state["former_names"]
      if isinstance(value, str)
      and value.strip().lower().endswith("_hist")
    ]
    if clean_former_names != state["former_names"]:
      state["former_names"] = clean_former_names
      changed = True

    if changed:
      state = self._apply_dataset_save_side_effects(state)
    state["is_hist"] = (
      target_schema.short_name == "rawcore"
      and state["incremental_strategy"] == "historize"
    )
    return state

  def _desired_hist_dataset_inputs(
    self,
    *,
    current: TargetDataset | None,
    base_projection: _DatasetProjection,
  ) -> list[dict[str, Any]]:
    inputs = self._dataset_inputs_state(current) if current is not None else []
    base_key = base_projection.dataset_key
    for item in inputs:
      if (
        item["kind"] == "upstream_target_dataset"
        and item["upstream_key"] == base_key
      ):
        return self._sorted_inputs(inputs)

    inputs.append({
      "kind": "upstream_target_dataset",
      "upstream_key": base_key,
      "role": "primary",
      "active": True,
    })
    return self._sorted_inputs(inputs)

  def _project_hist_columns(
    self,
    *,
    current_hist: TargetDataset | None,
    hist_dataset_key: str,
    hist_dataset_state: dict[str, Any],
    base_projection: _DatasetProjection,
    target_schema,
  ) -> list[_ColumnProjection]:
    current_columns = (
      list(
        TargetColumn.objects
        .filter(target_dataset=current_hist)
        .order_by("ordinal_position", "id")
      )
      if current_hist is not None
      else []
    )
    source_keys = base_projection.source_keys
    desired: list[_ColumnProjection] = []
    next_ordinal = 1

    base_surrogate = next(
      (
        column
        for column in base_projection.columns
        if column.state["system_role"] == "surrogate_key"
      ),
      None,
    )
    hist_expression = None
    if (
      base_surrogate is not None
      and self.service.schema_requires_surrogate_key(target_schema)
    ):
      draft = build_surrogate_key_column_draft(
        target_dataset_name=hist_dataset_state["target_dataset_name"],
        natural_key_colnames=(
          base_surrogate.state["target_column_name"],
          "version_started_at",
        ),
        pepper=self.service.pepper,
        ordinal=1,
        null_token=target_schema.surrogate_key_null_token,
        pair_sep=target_schema.surrogate_key_pair_separator,
        comp_sep=target_schema.surrogate_key_component_separator,
      )
      hist_expression = draft.surrogate_expression

    hist_sk_name = f"{hist_dataset_state['target_dataset_name']}_key"
    desired.append(
      self._hist_column_projection(
        hist_dataset_key=hist_dataset_key,
        current_columns=current_columns,
        object_identity="system_role:surrogate_key",
        state={
          "target_column_name": hist_sk_name,
          "ordinal_position": next_ordinal,
          "datatype": "STRING",
          "max_length": 64,
          "decimal_precision": None,
          "decimal_scale": None,
          "nullable": False,
          "system_role": "surrogate_key",
          "lineage_origin": "direct",
          "surrogate_expression": hist_expression,
          "description": (
            "Surrogate key for history rows of "
            f"{base_projection.state['target_dataset_name']}."
          ),
          "remark": None,
          "active": True,
          "retired_at_state": "unset",
          "lineage_key": None,
          "former_names": [],
          "is_system_managed": True,
        },
        inputs=[],
        source_keys=source_keys,
        match_role="surrogate_key",
      )
    )
    next_ordinal += 1

    for base_column in base_projection.columns:
      state = deepcopy(base_column.state)
      state.update({
        "ordinal_position": next_ordinal,
        "nullable": True,
        "system_role": (
          "entity_key"
          if state["system_role"] == "surrogate_key"
          else state["system_role"]
        ),
        "lineage_origin": "direct",
        "surrogate_expression": None,
        "remark": None,
        "active": True,
        "retired_at_state": "unset",
        "lineage_key": None,
        "is_system_managed": True,
      })
      state["former_names"] = self._clean_former_names(
        state["former_names"]
      )
      desired.append(
        self._hist_column_projection(
          hist_dataset_key=hist_dataset_key,
          current_columns=current_columns,
          object_identity=f"base:{base_column.object_key}",
          state=state,
          inputs=[{
            "kind": "upstream_target_column",
            "upstream_key": (
              self._current_column_reference_key(base_column.current)
              if base_column.current is not None
              else base_column.object_key
            ),
            "manual_expression": None,
            "ordinal_position": 1,
            "active": True,
          }],
          source_keys=source_keys,
          upstream_current=(
            base_column.current
            if base_column.current is not None
            else None
          ),
          match_name=state["target_column_name"],
        )
      )
      next_ordinal += 1

    orphan_specs = self._preserved_hist_orphans(
      current_hist=current_hist,
      current_columns=current_columns,
      base_projection=base_projection,
      hist_sk_name=hist_sk_name,
    )
    orphan_projections: list[tuple[_ColumnProjection, int | None]] = []
    for orphan in orphan_specs:
      state = deepcopy(orphan["state"])
      state["ordinal_position"] = next_ordinal
      projection = self._hist_column_projection(
        hist_dataset_key=hist_dataset_key,
        current_columns=current_columns,
        object_identity=f"orphan:{state['target_column_name']}",
        state=state,
        inputs=[],
        source_keys=source_keys,
        match_name=state["target_column_name"],
      )
      desired.append(projection)
      orphan_projections.append((projection, orphan["old_ordinal"]))
      next_ordinal += 1

    used_ordinals = {
      projection.state["ordinal_position"]
      for projection in desired
    }
    for projection, old_ordinal in orphan_projections:
      if (
        isinstance(old_ordinal, int)
        and old_ordinal > 0
        and old_ordinal not in used_ordinals
      ):
        used_ordinals.discard(projection.state["ordinal_position"])
        projection.state["ordinal_position"] = old_ordinal
        used_ordinals.add(old_ordinal)

    for spec in self.service._tech_specs_for_layer("hist"):
      role = (spec.get("system_role") or "").strip()
      if role not in _HIST_TAIL_ROLES:
        continue

      desired = [
        projection
        for projection in desired
        if projection.state["target_column_name"] != spec["name"]
      ]
      desired.append(
        self._hist_column_projection(
          hist_dataset_key=hist_dataset_key,
          current_columns=current_columns,
          object_identity=f"system_role:{role}",
          state={
            "target_column_name": spec["name"],
            "ordinal_position": next_ordinal,
            "datatype": spec["datatype"],
            "max_length": spec.get("max_length"),
            "decimal_precision": None,
            "decimal_scale": None,
            "nullable": bool(spec["nullable"]),
            "system_role": role,
            "lineage_origin": "direct",
            "surrogate_expression": None,
            "description": spec.get("description"),
            "remark": None,
            "active": True,
            "retired_at_state": "unset",
            "lineage_key": None,
            "former_names": [],
            "is_system_managed": True,
          },
          inputs=[],
          source_keys=source_keys,
          match_role=role,
          match_name=spec["name"],
        )
      )
      next_ordinal += 1

    self._apply_hist_key_former_names(
      desired=desired,
      hist_dataset_state=hist_dataset_state,
      base_projection=base_projection,
    )

    matched_ids = {
      projection.current.pk
      for projection in desired
      if projection.current is not None
    }
    for current_column in current_columns:
      if current_column.pk in matched_ids:
        continue
      desired.append(
        _ColumnProjection(
          object_key=(
            f"{hist_dataset_key}:column:removed:"
            f"{self._current_column_key(current_column)}"
          ),
          state={
            "present": False,
            "target_column_name": current_column.target_column_name,
          },
          inputs=[],
          current=current_column,
          source_keys=source_keys,
          effect_origin="HISTORY_COMPANION",
          reason=(
            "Column removed from the generated history companion contract."
          ),
        )
      )
    return desired

  def _hist_column_projection(
    self,
    *,
    hist_dataset_key: str,
    current_columns: list[TargetColumn],
    object_identity: str,
    state: dict[str, Any],
    inputs: list[dict[str, Any]],
    source_keys: tuple[str, ...],
    upstream_current: TargetColumn | None = None,
    match_role: str | None = None,
    match_name: str | None = None,
  ) -> _ColumnProjection:
    current = None
    if upstream_current is not None and current_columns:
      current_id = (
        TargetColumnInput.objects
        .filter(
          target_column_id__in=[column.pk for column in current_columns],
          upstream_target_column=upstream_current,
        )
        .values_list("target_column_id", flat=True)
        .first()
      )
      current = next(
        (column for column in current_columns if column.pk == current_id),
        None,
      )
    if current is None and match_role:
      current = next(
        (
          column
          for column in current_columns
          if (column.system_role or "") == match_role
        ),
        None,
      )
    if current is None and match_name:
      current = next(
        (
          column
          for column in current_columns
          if column.target_column_name == match_name
        ),
        None,
      )

    return _ColumnProjection(
      object_key=f"{hist_dataset_key}:column:{object_identity}",
      state=state,
      inputs=inputs,
      current=current,
      source_keys=source_keys,
      effect_origin="HISTORY_COMPANION",
      reason="History column derived from the effective rawcore contract.",
    )

  def _preserved_hist_orphans(
    self,
    *,
    current_hist: TargetDataset | None,
    current_columns: list[TargetColumn],
    base_projection: _DatasetProjection,
    hist_sk_name: str,
  ) -> list[dict[str, Any]]:
    if current_hist is None:
      return []

    base_names = {
      column.state["target_column_name"]
      for column in base_projection.columns
    }
    base_former_names = {
      former_name
      for column in base_projection.columns
      for former_name in column.state["former_names"]
    }
    linked_hist_ids = set(
      TargetColumnInput.objects
      .filter(
        upstream_target_column__target_dataset=(
          base_projection.current
          if base_projection.current is not None
          else None
        )
      )
      .values_list("target_column_id", flat=True)
    ) if base_projection.current is not None else set()

    orphans: list[dict[str, Any]] = []
    for current_column in current_columns:
      name = (current_column.target_column_name or "").strip()
      role = (current_column.system_role or "").strip()
      if not name or name == hist_sk_name:
        continue
      if role in _HIST_TAIL_ROLES or role == "surrogate_key":
        continue
      if name in base_names:
        continue
      if current_column.pk in linked_hist_ids:
        continue
      if name in base_former_names:
        continue

      state = self._column_state(current_column)
      state.update({
        "nullable": True,
        "lineage_origin": "direct",
        "surrogate_expression": None,
        "remark": None,
        "active": False,
        "retired_at_state": "set",
        "lineage_key": None,
        "is_system_managed": True,
      })
      state["former_names"] = self._clean_former_names(
        state["former_names"]
      )
      orphans.append({
        "state": state,
        "old_ordinal": current_column.ordinal_position,
      })
    return orphans

  def _apply_hist_key_former_names(
    self,
    *,
    desired: list[_ColumnProjection],
    hist_dataset_state: dict[str, Any],
    base_projection: _DatasetProjection,
  ) -> None:
    hist_key_name = f"{hist_dataset_state['target_dataset_name']}_key"
    hist_key = next(
      (
        column
        for column in desired
        if column.state.get("target_column_name") == hist_key_name
      ),
      None,
    )
    if hist_key is not None:
      former_names = list(hist_key.state["former_names"])
      for old_hist_name in hist_dataset_state["former_names"]:
        former_names = self._append_former_name(
          former_names,
          f"{old_hist_name}_key",
        )
      hist_key.state["former_names"] = former_names

    entity_key = next(
      (
        column
        for column in desired
        if column.state.get("system_role") == "entity_key"
      ),
      None,
    )
    if entity_key is not None:
      former_names = list(entity_key.state["former_names"])
      for old_base_name in base_projection.state["former_names"]:
        former_names = self._append_former_name(
          former_names,
          f"{old_base_name}_key",
        )
      entity_key.state["former_names"] = former_names

  def _actions_for_projection(
    self,
    projection: _DatasetProjection,
  ) -> list[TargetGenerationAction]:
    actions: list[TargetGenerationAction] = []
    current_state = (
      self._dataset_state(projection.current)
      if projection.current is not None
      else None
    )

    if projection.current is None:
      actions.append(
        TargetGenerationAction(
          action_type="CREATE_TARGET_DATASET",
          dataset_key=projection.dataset_key,
          object_key=projection.dataset_key,
          effect_origin=projection.effect_origin,
          change_classification="ADDITIVE",
          source_keys=projection.source_keys,
          before=None,
          after=projection.state,
          reason=projection.reason,
        )
      )
    elif current_state != projection.state:
      actions.append(
        TargetGenerationAction(
          action_type="UPDATE_TARGET_DATASET",
          dataset_key=projection.dataset_key,
          object_key=projection.dataset_key,
          effect_origin=projection.effect_origin,
          change_classification=self._dataset_update_classification(
            current_state,
            projection.state,
          ),
          source_keys=projection.source_keys,
          before=current_state,
          after=projection.state,
          reason=projection.reason,
        )
      )

    current_inputs = (
      self._dataset_inputs_state(projection.current)
      if projection.current is not None
      else []
    )
    if current_inputs != projection.inputs:
      actions.append(
        TargetGenerationAction(
          action_type="SYNC_TARGET_DATASET_INPUTS",
          dataset_key=projection.dataset_key,
          object_key=projection.dataset_key,
          effect_origin=projection.effect_origin,
          change_classification=self._input_sync_classification(
            current_inputs,
            projection.inputs,
          ),
          source_keys=projection.source_keys,
          before={"inputs": current_inputs},
          after={"inputs": projection.inputs},
          reason=(
            "Dataset-level lineage synchronized from current generation rules."
          ),
        )
      )

    for column in projection.columns:
      actions.extend(
        self._actions_for_column(
          dataset_key=projection.dataset_key,
          column=column,
        )
      )
    return actions

  def _actions_for_column(
    self,
    *,
    dataset_key: str,
    column: _ColumnProjection,
  ) -> list[TargetGenerationAction]:
    if column.state.get("present") is False:
      if column.current is None:
        return []
      return [
        TargetGenerationAction(
          action_type="RETIRE_TARGET_COLUMN",
          dataset_key=dataset_key,
          object_key=column.object_key,
          effect_origin=column.effect_origin,
          change_classification="BREAKING",
          source_keys=column.source_keys,
          before=self._column_state(column.current),
          after=column.state,
          reason=column.reason,
        )
      ]

    actions: list[TargetGenerationAction] = []
    current_state = (
      self._column_state(column.current)
      if column.current is not None
      else None
    )
    if column.current is None:
      actions.append(
        TargetGenerationAction(
          action_type="CREATE_TARGET_COLUMN",
          dataset_key=dataset_key,
          object_key=column.object_key,
          effect_origin=column.effect_origin,
          change_classification="ADDITIVE",
          source_keys=column.source_keys,
          before=None,
          after=column.state,
          reason=column.reason,
        )
      )
    elif current_state != column.state:
      action_type = "UPDATE_TARGET_COLUMN"
      if not current_state["active"] and column.state["active"]:
        action_type = "REACTIVATE_TARGET_COLUMN"
      actions.append(
        TargetGenerationAction(
          action_type=action_type,
          dataset_key=dataset_key,
          object_key=column.object_key,
          effect_origin=column.effect_origin,
          change_classification=self._column_update_classification(
            current_state,
            column.state,
          ),
          source_keys=column.source_keys,
          before=current_state,
          after=column.state,
          reason=column.reason,
        )
      )

    current_inputs = (
      self._column_inputs_state(column.current)
      if column.current is not None
      else []
    )
    if current_inputs != column.inputs:
      actions.append(
        TargetGenerationAction(
          action_type="SYNC_TARGET_COLUMN_INPUTS",
          dataset_key=dataset_key,
          object_key=column.object_key,
          effect_origin=column.effect_origin,
          change_classification=self._input_sync_classification(
            current_inputs,
            column.inputs,
          ),
          source_keys=column.source_keys,
          before={"inputs": current_inputs},
          after={"inputs": column.inputs},
          reason=(
            "Column-level lineage synchronized from current generation rules."
          ),
        )
      )
    return actions

  def _lifecycle_actions(
    self,
    *,
    target_schema,
    expected_current_ids: set[int],
    projections: list[_DatasetProjection],
  ) -> list[TargetGenerationAction]:
    projection_by_current_id = {
      projection.current.pk: projection
      for projection in projections
      if projection.current is not None
    }
    actions: list[TargetGenerationAction] = []

    candidates = (
      TargetDataset.objects
      .filter(
        target_schema=target_schema,
        is_system_managed=True,
      )
      .exclude(lineage_key__isnull=True)
      .order_by("pk")
    )
    for current in candidates:
      if not self.service._is_generated_lineage_key_for_schema(
        current.lineage_key,
        target_schema,
      ):
        continue

      projection = projection_by_current_id.get(current.pk)
      before = (
        deepcopy(projection.state)
        if projection is not None
        else self._dataset_state(current)
      )
      dataset_key = (
        projection.dataset_key
        if projection is not None
        else self._current_dataset_key(current)
      )
      source_keys = (
        projection.source_keys
        if projection is not None
        else self._source_keys_from_lineage(current.lineage_key)
      )

      if current.pk in expected_current_ids:
        after = deepcopy(before)
        after["active"] = True
        after["retired_at_state"] = "unset"
        if before == after:
          continue
        action_type = "REACTIVATE_TARGET_DATASET"
        classification = "ADDITIVE"
        reason = "Generated dataset is eligible again in the selected scope."
      else:
        after = deepcopy(before)
        after["active"] = False
        after["retired_at_state"] = "set"
        if before == after:
          continue
        action_type = "RETIRE_TARGET_DATASET"
        classification = "BREAKING"
        reason = "Generated dataset is no longer produced by the selected scope."

      actions.append(
        TargetGenerationAction(
          action_type=action_type,
          dataset_key=dataset_key,
          object_key=dataset_key,
          effect_origin="GENERATED_LIFECYCLE",
          change_classification=classification,
          source_keys=source_keys,
          before=before,
          after=after,
          reason=reason,
        )
      )
    return actions

  def _dataset_state(self, dataset: TargetDataset) -> dict[str, Any]:
    return {
      "target_schema_short_name": dataset.target_schema.short_name,
      "target_dataset_name": dataset.target_dataset_name,
      "description": dataset.description,
      "handle_deletes": bool(dataset.handle_deletes),
      "historize": bool(dataset.historize),
      "combination_mode": dataset.combination_mode,
      "incremental_strategy": dataset.incremental_strategy,
      "incremental_source_key": (
        self._source_dataset_key(dataset.incremental_source)
        if dataset.incremental_source_id
        else None
      ),
      "active": bool(dataset.active),
      "retired_at_state": (
        "set" if dataset.retired_at is not None else "unset"
      ),
      "lineage_key": dataset.lineage_key,
      "former_names": list(dataset.former_names or []),
      "is_system_managed": bool(dataset.is_system_managed),
      "is_hist": bool(dataset.is_hist),
    }

  def _column_state(self, column: TargetColumn) -> dict[str, Any]:
    return {
      "target_column_name": column.target_column_name,
      "ordinal_position": column.ordinal_position,
      "datatype": column.datatype,
      "max_length": column.max_length,
      "decimal_precision": column.decimal_precision,
      "decimal_scale": column.decimal_scale,
      "nullable": bool(column.nullable),
      "system_role": column.system_role or "",
      "lineage_origin": column.lineage_origin or "direct",
      "surrogate_expression": column.surrogate_expression,
      "description": column.description,
      "remark": column.remark,
      "active": bool(column.active),
      "retired_at_state": (
        "set" if column.retired_at is not None else "unset"
      ),
      "lineage_key": column.lineage_key,
      "former_names": list(column.former_names or []),
      "is_system_managed": bool(column.is_system_managed),
    }

  def _dataset_inputs_state(
    self,
    dataset: TargetDataset | None,
  ) -> list[dict[str, Any]]:
    if dataset is None:
      return []

    inputs = []
    for item in dataset.input_links.select_related(
      "source_dataset",
      "upstream_target_dataset__target_schema",
    ).all():
      if item.source_dataset_id:
        inputs.append({
          "kind": "source_dataset",
          "source_key": self._source_dataset_key(item.source_dataset),
          "role": item.role,
          "active": bool(item.active),
        })
      elif item.upstream_target_dataset_id:
        inputs.append({
          "kind": "upstream_target_dataset",
          "upstream_key": self._current_dataset_key(
            item.upstream_target_dataset
          ),
          "role": item.role,
          "active": bool(item.active),
        })
    return self._sorted_inputs(inputs)

  def _column_inputs_state(
    self,
    column: TargetColumn | None,
  ) -> list[dict[str, Any]]:
    if column is None:
      return []

    inputs = []
    for item in column.input_links.select_related(
      "source_column",
      "upstream_target_column__target_dataset__target_schema",
    ).all():
      common = {
        "manual_expression": item.manual_expression,
        "ordinal_position": item.ordinal_position,
        "active": bool(item.active),
      }
      if item.source_column_id:
        inputs.append({
          "kind": "source_column",
          "source_key": self._source_column_key(item.source_column_id),
          **common,
        })
      elif item.upstream_target_column_id:
        inputs.append({
          "kind": "upstream_target_column",
          "upstream_key": self._current_column_reference_key(
            item.upstream_target_column
          ),
          **common,
        })
    return self._sorted_inputs(inputs)

  def _current_dataset_key(self, dataset: TargetDataset) -> str:
    if (
      dataset.lineage_key
      and self.service._is_generated_lineage_key_for_schema(
        dataset.lineage_key,
        dataset.target_schema,
      )
    ):
      return self._generated_dataset_key(
        target_schema=dataset.target_schema,
        lineage_key=dataset.lineage_key,
        is_hist=(
          dataset.is_hist
          or dataset.target_dataset_name.endswith("_hist")
        ),
      )
    return (
      f"{dataset.target_schema.short_name}:target_dataset:{dataset.pk}"
    )

  def _generated_dataset_key(
    self,
    *,
    target_schema,
    lineage_key: str,
    is_hist: bool,
  ) -> str:
    suffix = "hist" if is_hist else "base"
    return f"{target_schema.short_name}:{lineage_key}:{suffix}"

  def _desired_column_key(
    self,
    *,
    col_draft,
    upstream_column: TargetColumn | None,
  ) -> str:
    role = (getattr(col_draft, "system_role", "") or "").strip()
    if role in _UNIQUE_SYSTEM_ROLES:
      return f"system_role:{role}"
    source_column_id = getattr(col_draft, "source_column_id", None)
    if source_column_id:
      return self._source_column_key(source_column_id)
    if upstream_column is not None:
      return f"upstream:{self._current_column_reference_key(upstream_column)}"
    return f"name:{col_draft.target_column_name}"

  def _current_column_key(self, column: TargetColumn) -> str:
    role = (column.system_role or "").strip()
    if role in _UNIQUE_SYSTEM_ROLES:
      return f"system_role:{role}"
    source_input = column.input_links.filter(
      source_column__isnull=False
    ).order_by("id").first()
    if source_input is not None:
      return self._source_column_key(source_input.source_column_id)
    if column.lineage_key:
      return f"lineage:{column.lineage_key}"
    return f"name:{column.target_column_name}"

  def _current_column_reference_key(self, column: TargetColumn) -> str:
    return (
      f"{self._current_dataset_key(column.target_dataset)}:column:"
      f"{self._current_column_key(column)}"
    )

  def _source_dataset_key(self, source_dataset_or_id) -> str:
    source_dataset_id = getattr(
      source_dataset_or_id,
      "pk",
      source_dataset_or_id,
    )
    return f"source_dataset:{source_dataset_id}"

  def _source_column_key(self, source_column_or_id) -> str:
    source_column_id = getattr(
      source_column_or_id,
      "pk",
      source_column_or_id,
    )
    return f"source_column:{source_column_id}"

  def _source_keys_from_lineage(
    self,
    lineage_key: str | None,
  ) -> tuple[str, ...]:
    value = str(lineage_key or "").strip()
    if ":" not in value:
      return ()
    source_part = value.split(":", 1)[1]
    return tuple(
      self._source_dataset_key(int(item))
      for item in source_part.split(",")
      if item.isdigit()
    )

  def _apply_dataset_save_side_effects(
    self,
    state: dict[str, Any],
  ) -> dict[str, Any]:
    updated = deepcopy(state)
    updated["retired_at_state"] = (
      "unset" if updated["active"] else "set"
    )
    return updated

  def _apply_column_save_side_effects(
    self,
    state: dict[str, Any],
  ) -> dict[str, Any]:
    updated = deepcopy(state)
    updated["retired_at_state"] = (
      "unset" if updated["active"] else "set"
    )
    return updated

  def _clean_former_names(
    self,
    former_names: list[Any],
  ) -> list[str]:
    return [
      value
      for value in list(former_names or [])
      if isinstance(value, str) and value.strip()
    ]

  def _append_former_name(
    self,
    former_names: list[str],
    old_name: str,
  ) -> list[str]:
    normalized = list(former_names or [])
    old = (old_name or "").strip()
    if not old:
      return normalized
    known = {
      value.lower()
      for value in normalized
      if isinstance(value, str)
    }
    if old.lower() not in known:
      normalized.append(old)
    return normalized

  def _dataset_update_classification(
    self,
    before: dict[str, Any],
    after: dict[str, Any],
  ) -> str:
    breaking_fields = (
      "target_dataset_name",
      "lineage_key",
      "combination_mode",
    )
    if any(before[field] != after[field] for field in breaking_fields):
      return "BREAKING"
    if before["active"] and not after["active"]:
      return "BREAKING"
    if not before["active"] and after["active"]:
      return "ADDITIVE"
    return "NEUTRAL"

  def _column_update_classification(
    self,
    before: dict[str, Any],
    after: dict[str, Any],
  ) -> str:
    if before["target_column_name"] != after["target_column_name"]:
      return "BREAKING"
    if before["datatype"] != after["datatype"]:
      return "BREAKING"
    if before["nullable"] and not after["nullable"]:
      return "BREAKING"
    if before["active"] and not after["active"]:
      return "BREAKING"
    if not before["active"] and after["active"]:
      return "ADDITIVE"
    return "NEUTRAL"

  def _input_sync_classification(
    self,
    before: list[dict[str, Any]],
    after: list[dict[str, Any]],
  ) -> str:
    """Classify generated lineage changes by their semantic direction."""
    before_items = {
      json.dumps(
        item,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
      )
      for item in before
    }
    after_items = {
      json.dumps(
        item,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
      )
      for item in after
    }

    if before_items - after_items:
      return "BREAKING"
    if after_items - before_items:
      return "ADDITIVE"
    return "NEUTRAL"

  def _sorted_inputs(
    self,
    inputs: list[dict[str, Any]],
  ) -> list[dict[str, Any]]:
    return sorted(
      (deepcopy(item) for item in inputs),
      key=lambda item: json.dumps(
        item,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
      ),
    )

  def _source_metadata_fingerprint(
    self,
    *,
    source_datasets: list,
    target_schema,
  ) -> str:
    payload = {
      "selection_order": [
        self._source_dataset_key(source_dataset)
        for source_dataset in source_datasets
      ],
      "target_schema_short_name": target_schema.short_name,
      "source_datasets": [
        self._source_dataset_fingerprint_payload(source_dataset)
        for source_dataset in source_datasets
      ],
    }
    return self._stable_hash(payload)

  def _source_dataset_fingerprint_payload(
    self,
    source_dataset,
  ) -> dict[str, Any]:
    memberships = []
    manager = getattr(source_dataset, "dataset_groups", None)
    if manager is not None:
      for membership in manager.select_related("group").all():
        memberships.append({
          "group_id": membership.group_id,
          "target_short_name": getattr(
            membership.group,
            "target_short_name",
            None,
          ),
          "unified_source_dataset_name": getattr(
            membership.group,
            "unified_source_dataset_name",
            None,
          ),
          "is_primary_system": bool(membership.is_primary_system),
          "source_identity_id": getattr(
            membership,
            "source_identity_id",
            None,
          ),
          "source_identity_ordinal": getattr(
            membership,
            "source_identity_ordinal",
            None,
          ),
        })

    columns = []
    for column in source_dataset.source_columns.all().order_by(
      "ordinal_position",
      "id",
    ):
      columns.append({
        "source_column_id": column.pk,
        "source_column_name": column.source_column_name,
        "ordinal_position": column.ordinal_position,
        "datatype": column.datatype,
        "max_length": column.max_length,
        "decimal_precision": column.decimal_precision,
        "decimal_scale": column.decimal_scale,
        "nullable": bool(column.nullable),
        "primary_key_column": bool(column.primary_key_column),
        "integrate": bool(column.integrate),
      })

    source_system = source_dataset.source_system
    return {
      "source_dataset_key": self._source_dataset_key(source_dataset),
      "source_dataset_name": source_dataset.source_dataset_name,
      "schema_name": source_dataset.schema_name,
      "description": source_dataset.description,
      "integrate": bool(source_dataset.integrate),
      "active": bool(source_dataset.active),
      "generate_raw_table": source_dataset.generate_raw_table,
      "incremental": bool(getattr(source_dataset, "incremental", False)),
      "source_system": {
        "id": source_system.pk,
        "short_name": source_system.short_name,
        "target_short_name": source_system.target_short_name,
        "type": source_system.type,
        "generate_raw_tables": bool(source_system.generate_raw_tables),
      },
      "memberships": sorted(
        memberships,
        key=lambda item: json.dumps(item, sort_keys=True),
      ),
      "columns": columns,
    }

  def _target_metadata_fingerprint(
    self,
    *,
    target_schema,
  ) -> str:
    schema_short_names = {target_schema.short_name}
    if target_schema.short_name in {"stage", "rawcore"}:
      schema_short_names.add("raw")
    if target_schema.short_name == "rawcore":
      schema_short_names.add("stage")

    schemas = list(
      TargetSchema.objects
      .filter(short_name__in=schema_short_names)
      .order_by("short_name", "id")
    )
    datasets = list(
      TargetDataset.objects
      .filter(target_schema__in=schemas)
      .select_related(
        "target_schema",
        "incremental_source",
      )
      .order_by("target_schema__short_name", "target_dataset_name", "id")
    )

    dataset_payloads = []
    for dataset in datasets:
      columns = list(
        TargetColumn.objects
        .filter(target_dataset=dataset)
        .order_by("ordinal_position", "id")
      )
      dataset_payloads.append({
        "dataset_id": dataset.pk,
        "state": self._dataset_state(dataset),
        "inputs": self._dataset_inputs_state(dataset),
        "columns": [
          {
            "column_id": column.pk,
            "state": self._column_state(column),
            "inputs": self._column_inputs_state(column),
          }
          for column in columns
        ],
      })

    selected_dataset_ids = [
      dataset.pk
      for dataset in datasets
      if dataset.target_schema_id == target_schema.pk
    ]
    references = list(
      TargetDatasetReference.objects
      .filter(
        referencing_dataset_id__in=selected_dataset_ids
      )
      .values(
        "id",
        "referencing_dataset_id",
        "referenced_dataset_id",
        "reference_prefix",
        "relationship_type",
        "join_condition_hint",
        "inferred_members_enabled",
        "default_member_fallback_enabled",
      )
      .order_by("id")
    ) + list(
      TargetDatasetReference.objects
      .filter(
        referenced_dataset_id__in=selected_dataset_ids
      )
      .exclude(referencing_dataset_id__in=selected_dataset_ids)
      .values(
        "id",
        "referencing_dataset_id",
        "referenced_dataset_id",
        "reference_prefix",
        "relationship_type",
        "join_condition_hint",
        "inferred_members_enabled",
        "default_member_fallback_enabled",
      )
      .order_by("id")
    )
    reference_ids = [item["id"] for item in references]
    components = list(
      TargetDatasetReferenceComponent.objects
      .filter(reference_id__in=reference_ids)
      .values(
        "id",
        "reference_id",
        "from_column_id",
        "to_column_id",
        "ordinal_position",
      )
      .order_by("id")
    )

    payload = {
      "target_schemas": [
        self._target_schema_fingerprint_payload(schema)
        for schema in schemas
      ],
      "target_datasets": dataset_payloads,
      "references": references,
      "reference_components": components,
    }
    return self._stable_hash(payload)

  def _target_schema_fingerprint_payload(self, schema) -> dict[str, Any]:
    return {
      "id": schema.pk,
      "short_name": schema.short_name,
      "physical_prefix": schema.physical_prefix,
      "generate_layer": bool(schema.generate_layer),
      "is_system_managed": bool(schema.is_system_managed),
      "default_historize": bool(schema.default_historize),
      "surrogate_keys_enabled": bool(schema.surrogate_keys_enabled),
      "surrogate_key_null_token": schema.surrogate_key_null_token,
      "surrogate_key_pair_separator": (
        schema.surrogate_key_pair_separator
      ),
      "surrogate_key_component_separator": (
        schema.surrogate_key_component_separator
      ),
      "incremental_strategy_default": (
        schema.incremental_strategy_default
      ),
    }

  def _stable_hash(self, value: Any) -> str:
    payload = json.dumps(
      value,
      sort_keys=True,
      ensure_ascii=False,
      allow_nan=False,
      separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
