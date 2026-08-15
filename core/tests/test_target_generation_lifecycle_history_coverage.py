"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2026 Ilona Tag

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

import pytest

from metadata.generation.target_generation_service import (
  TargetGenerationService,
)
from metadata.models import (
  SourceColumn,
  SourceDataset,
  SourceDatasetGroup,
  SourceDatasetGroupMembership,
  System,
  TargetColumn,
  TargetDataset,
  TargetSchema,
)


def _target_schema(
  short_name: str,
  *,
  default_historize: bool = False,
  surrogate_keys_enabled: bool = False,
) -> TargetSchema:
  prefix_by_schema = {
    "raw": "raw",
    "stage": "stg",
    "rawcore": "rc",
  }
  schema, _ = TargetSchema.objects.get_or_create(
    short_name=short_name,
    defaults={
      "display_name": short_name.title(),
      "schema_name": short_name,
    },
  )
  schema.display_name = short_name.title()
  schema.schema_name = short_name
  schema.physical_prefix = prefix_by_schema[short_name]
  schema.generate_layer = True
  schema.is_system_managed = True
  schema.default_historize = default_historize
  schema.surrogate_keys_enabled = surrogate_keys_enabled
  schema.incremental_strategy_default = "merge"
  schema.save()
  return schema


def _source_dataset(
  *,
  system_short_name: str,
  dataset_name: str = "Customer",
  target_short_name: str | None = None,
  generate_raw_table: bool = True,
) -> SourceDataset:
  source_system = System.objects.create(
    short_name=system_short_name,
    name=f"{system_short_name.title()} Source",
    type="db",
    is_source=True,
    is_target=False,
    target_short_name=target_short_name or system_short_name,
    generate_raw_tables=generate_raw_table,
  )
  source_dataset = SourceDataset.objects.create(
    source_system=source_system,
    schema_name="dbo",
    source_dataset_name=dataset_name,
    description=f"{dataset_name} source",
    integrate=True,
    active=True,
    generate_raw_table=generate_raw_table,
  )
  SourceColumn.objects.create(
    source_dataset=source_dataset,
    source_column_name="customer_id",
    ordinal_position=1,
    datatype="INTEGER",
    nullable=False,
    primary_key_column=True,
    integrate=True,
  )
  SourceColumn.objects.create(
    source_dataset=source_dataset,
    source_column_name="customer_name",
    ordinal_position=2,
    datatype="STRING",
    max_length=100,
    nullable=True,
    primary_key_column=False,
    integrate=True,
  )
  return source_dataset


def _stable_rawcore_chain(
  *,
  service: TargetGenerationService,
  source_dataset: SourceDataset,
) -> tuple[TargetSchema, TargetSchema, TargetSchema]:
  raw_schema = _target_schema("raw")
  stage_schema = _target_schema(
    "stage",
    surrogate_keys_enabled=True,
  )
  rawcore_schema = _target_schema(
    "rawcore",
    default_historize=True,
    surrogate_keys_enabled=True,
  )

  service.apply_all_result([source_dataset], raw_schema)
  service.apply_all_result([source_dataset], stage_schema)
  service.apply_all_result([source_dataset], rawcore_schema)
  service.apply_all_result([source_dataset], rawcore_schema)
  return raw_schema, stage_schema, rawcore_schema


def _action_payloads(plan, action_type: str) -> list[dict]:
  return [
    action.to_dict()
    for action in plan.actions
    if action.action_type == action_type
  ]


@pytest.mark.django_db
def test_history_companion_plan_covers_generated_inputs() -> None:
  """Verify history dataset and column lineage is explicit in the plan."""
  raw_schema = _target_schema("raw")
  stage_schema = _target_schema(
    "stage",
    surrogate_keys_enabled=True,
  )
  rawcore_schema = _target_schema(
    "rawcore",
    default_historize=True,
    surrogate_keys_enabled=True,
  )
  source_dataset = _source_dataset(
    system_short_name="histinp",
  )
  service = TargetGenerationService(pepper="test-pepper")

  service.apply_all_result([source_dataset], raw_schema)
  service.apply_all_result([source_dataset], stage_schema)

  plan = service.build_plan(
    [source_dataset],
    rawcore_schema,
    reconcile_lifecycle=True,
  )
  history_dataset_actions = [
    action.to_dict()
    for action in plan.actions
    if action.effect_origin == "HISTORY_COMPANION"
    and action.action_type == "CREATE_TARGET_DATASET"
  ]
  history_dataset_input_actions = [
    action.to_dict()
    for action in plan.actions
    if action.effect_origin == "HISTORY_COMPANION"
    and action.action_type == "SYNC_TARGET_DATASET_INPUTS"
  ]
  history_column_input_actions = [
    action.to_dict()
    for action in plan.actions
    if action.effect_origin == "HISTORY_COMPANION"
    and action.action_type == "SYNC_TARGET_COLUMN_INPUTS"
  ]

  assert len(history_dataset_actions) == 1
  assert len(history_dataset_input_actions) == 1
  assert history_dataset_input_actions[0]["change_classification"] == (
    "ADDITIVE"
  )
  assert history_dataset_input_actions[0]["after"]["inputs"] == [
    {
      "active": True,
      "kind": "upstream_target_dataset",
      "role": "primary",
      "upstream_key": history_dataset_actions[0]["dataset_key"].replace(
        ":hist",
        ":base",
      ),
    }
  ]
  assert history_column_input_actions
  assert all(
    action["change_classification"] == "ADDITIVE"
    for action in history_column_input_actions
  )
  assert all(
    any(
      item["kind"] == "upstream_target_column"
      for item in action["after"]["inputs"]
    )
    for action in history_column_input_actions
  )


@pytest.mark.django_db(transaction=True)
def test_guarded_lifecycle_retires_and_reactivates_history_companions() -> None:
  """Verify base and history datasets share guarded lifecycle transitions."""
  source_dataset = _source_dataset(
    system_short_name="histlife",
  )
  service = TargetGenerationService(pepper="test-pepper")
  _raw_schema, _stage_schema, rawcore_schema = _stable_rawcore_chain(
    service=service,
    source_dataset=source_dataset,
  )
  lineage_key = service.build_lineage_key_for_bucket(
    rawcore_schema,
    [source_dataset],
  )
  generated = TargetDataset.objects.filter(
    target_schema=rawcore_schema,
    lineage_key=lineage_key,
  ).order_by("pk")
  original_ids = tuple(generated.values_list("pk", flat=True))
  assert len(original_ids) == 2

  source_dataset.integrate = False
  source_dataset.save(update_fields=["integrate"])
  retire_plan = service.build_plan(
    [],
    rawcore_schema,
    reconcile_lifecycle=True,
  )
  retire_actions = _action_payloads(
    retire_plan,
    "RETIRE_TARGET_DATASET",
  )

  assert len(retire_actions) == 2
  assert {
    action["dataset_key"].rsplit(":", 1)[-1]
    for action in retire_actions
  } == {"base", "hist"}
  assert all(
    action["effect_origin"] == "GENERATED_LIFECYCLE"
    and action["change_classification"] == "BREAKING"
    for action in retire_actions
  )

  retire_result = service.apply_plan(retire_plan)

  assert retire_result.retired_dataset_count == 2
  assert retire_result.reactivated_dataset_count == 0
  assert retire_result.converged is True
  assert TargetDataset.objects.filter(
    pk__in=original_ids,
    active=False,
    retired_at__isnull=False,
  ).count() == 2

  source_dataset.integrate = True
  source_dataset.save(update_fields=["integrate"])
  eligible = service.get_eligible_source_datasets_for_schema(
    rawcore_schema
  )
  reactivate_plan = service.build_plan(
    eligible,
    rawcore_schema,
    reconcile_lifecycle=True,
  )
  reactivate_actions = _action_payloads(
    reactivate_plan,
    "REACTIVATE_TARGET_DATASET",
  )

  assert len(reactivate_actions) == 2
  assert all(
    action["effect_origin"] == "GENERATED_LIFECYCLE"
    and action["change_classification"] == "ADDITIVE"
    for action in reactivate_actions
  )

  reactivate_result = service.apply_plan(reactivate_plan)

  assert reactivate_result.retired_dataset_count == 0
  assert reactivate_result.reactivated_dataset_count == 2
  assert reactivate_result.converged is True
  assert tuple(
    TargetDataset.objects
    .filter(
      target_schema=rawcore_schema,
      lineage_key=lineage_key,
    )
    .order_by("pk")
    .values_list("pk", flat=True)
  ) == original_ids
  assert TargetDataset.objects.filter(
    pk__in=original_ids,
    active=True,
    retired_at__isnull=True,
  ).count() == 2


@pytest.mark.django_db(transaction=True)
def test_guarded_multi_source_withdrawal_reuses_target_and_syncs_inputs() -> None:
  """Verify a reduced source bucket is reviewed and applied in place."""
  stage_schema = _target_schema("stage")
  source_one = _source_dataset(
    system_short_name="bucket1",
    dataset_name="PersonOne",
    target_short_name="person",
    generate_raw_table=False,
  )
  source_two = _source_dataset(
    system_short_name="bucket2",
    dataset_name="PersonTwo",
    target_short_name="person",
    generate_raw_table=False,
  )
  group = SourceDatasetGroup.objects.create(
    target_short_name="person",
    unified_source_dataset_name="Person",
  )
  SourceDatasetGroupMembership.objects.create(
    group=group,
    source_dataset=source_one,
    is_primary_system=True,
  )
  SourceDatasetGroupMembership.objects.create(
    group=group,
    source_dataset=source_two,
    is_primary_system=False,
  )
  service = TargetGenerationService(pepper="test-pepper")
  eligible = service.get_eligible_source_datasets_for_schema(stage_schema)

  service.apply_all_result(
    eligible,
    stage_schema,
    reconcile_lifecycle=True,
  )
  shared_target = TargetDataset.objects.get(target_schema=stage_schema)
  original_id = shared_target.pk
  original_lineage_key = shared_target.lineage_key
  assert set(
    shared_target.input_links.values_list(
      "source_dataset_id",
      flat=True,
    )
  ) == {source_one.pk, source_two.pk}

  source_two.integrate = False
  source_two.save(update_fields=["integrate"])
  eligible = service.get_eligible_source_datasets_for_schema(stage_schema)
  plan = service.build_plan(
    eligible,
    stage_schema,
    reconcile_lifecycle=True,
  )
  dataset_updates = _action_payloads(plan, "UPDATE_TARGET_DATASET")
  input_updates = _action_payloads(plan, "SYNC_TARGET_DATASET_INPUTS")

  assert len(dataset_updates) == 1
  assert dataset_updates[0]["before"]["lineage_key"] == original_lineage_key
  assert dataset_updates[0]["after"]["lineage_key"] == (
    service.build_lineage_key_for_bucket(stage_schema, [source_one])
  )
  assert dataset_updates[0]["before"]["combination_mode"] == "union"
  assert dataset_updates[0]["after"]["combination_mode"] == "union"
  assert dataset_updates[0]["change_classification"] == "BREAKING"

  matching_inputs = [
    action
    for action in input_updates
    if action["dataset_key"] == dataset_updates[0]["dataset_key"]
  ]
  assert len(matching_inputs) == 1
  assert matching_inputs[0]["change_classification"] == "BREAKING"
  assert {
    item["source_key"]
    for item in matching_inputs[0]["after"]["inputs"]
  } == {f"source_dataset:{source_one.pk}"}

  first_result = service.apply_plan(plan)

  shared_target.refresh_from_db()
  assert first_result.converged is False
  assert first_result.residual_action_count == 1
  assert shared_target.pk == original_id
  assert shared_target.active is True
  assert shared_target.retired_at is None
  assert shared_target.combination_mode == "union"
  assert shared_target.lineage_key == (
    service.build_lineage_key_for_bucket(stage_schema, [source_one])
  )
  assert set(
    shared_target.input_links.values_list(
      "source_dataset_id",
      flat=True,
    )
  ) == {source_one.pk}
  assert TargetDataset.objects.filter(target_schema=stage_schema).count() == 1
  
  follow_up_plan = service.build_plan(
    eligible,
    stage_schema,
    reconcile_lifecycle=True,
  )
  follow_up_updates = _action_payloads(
    follow_up_plan,
    "UPDATE_TARGET_DATASET",
  )

  assert follow_up_plan.plan_fingerprint == (
    first_result.residual_plan_fingerprint
  )
  assert follow_up_plan.action_count == 1
  assert len(follow_up_updates) == 1
  assert follow_up_updates[0]["before"]["lineage_key"] == (
    follow_up_updates[0]["after"]["lineage_key"]
  )
  assert follow_up_updates[0]["before"]["combination_mode"] == "union"
  assert follow_up_updates[0]["after"]["combination_mode"] == "single"
  assert follow_up_updates[0]["change_classification"] == "BREAKING"

  second_result = service.apply_plan(follow_up_plan)

  shared_target.refresh_from_db()
  assert second_result.converged is True
  assert shared_target.pk == original_id
  assert shared_target.combination_mode == "single"
  assert TargetDataset.objects.filter(target_schema=stage_schema).count() == 1


@pytest.mark.django_db(transaction=True)
def test_rawcore_rename_plan_uses_lineage_and_updates_existing_history() -> None:
  """Verify a base rename updates its history companion without duplication."""
  source_dataset = _source_dataset(
    system_short_name="renline",
  )
  service = TargetGenerationService(pepper="test-pepper")
  _raw_schema, _stage_schema, rawcore_schema = _stable_rawcore_chain(
    service=service,
    source_dataset=source_dataset,
  )
  lineage_key = service.build_lineage_key_for_bucket(
    rawcore_schema,
    [source_dataset],
  )
  base_dataset = (
    TargetDataset.objects
    .filter(
      target_schema=rawcore_schema,
      lineage_key=lineage_key,
    )
    .exclude(target_dataset_name__endswith="_hist")
    .get()
  )
  hist_dataset = TargetDataset.objects.get(
    target_schema=rawcore_schema,
    lineage_key=lineage_key,
    target_dataset_name__endswith="_hist",
  )
  base_id = base_dataset.pk
  hist_id = hist_dataset.pk
  old_base_name = base_dataset.target_dataset_name
  old_hist_name = hist_dataset.target_dataset_name
  new_base_name = f"{old_base_name}_renamed"
  new_hist_name = f"{new_base_name}_hist"

  base_dataset.former_names = [old_base_name]
  base_dataset.target_dataset_name = new_base_name
  base_dataset.save(
    update_fields=[
      "target_dataset_name",
      "former_names",
    ]
  )

  plan = service.build_plan(
    [source_dataset],
    rawcore_schema,
    reconcile_lifecycle=True,
  )
  create_datasets = _action_payloads(plan, "CREATE_TARGET_DATASET")
  history_updates = [
    action.to_dict()
    for action in plan.actions
    if action.action_type == "UPDATE_TARGET_DATASET"
    and action.effect_origin == "HISTORY_COMPANION"
  ]
  key_history_updates = [
    action.to_dict()
    for action in plan.actions
    if action.action_type == "UPDATE_TARGET_COLUMN"
    and action.effect_origin == "MODEL_SIDE_EFFECT"
  ]

  assert create_datasets == []
  assert len(history_updates) == 1
  assert history_updates[0]["before"]["target_dataset_name"] == (
    old_hist_name
  )
  assert history_updates[0]["after"]["target_dataset_name"] == (
    new_hist_name
  )
  assert history_updates[0]["before"]["lineage_key"] == lineage_key
  assert history_updates[0]["after"]["lineage_key"] == lineage_key
  assert history_updates[0]["change_classification"] == "BREAKING"
  assert any(
    f"{old_base_name}_key" in action["after"]["former_names"]
    for action in key_history_updates
  )

  result = service.apply_plan(plan)

  base_dataset.refresh_from_db()
  hist_dataset.refresh_from_db()
  assert result.consumed_action_count == plan.action_count
  assert base_dataset.pk == base_id
  assert hist_dataset.pk == hist_id
  assert base_dataset.target_dataset_name == new_base_name
  assert hist_dataset.target_dataset_name == new_hist_name
  assert base_dataset.lineage_key == lineage_key
  assert hist_dataset.lineage_key == lineage_key
  assert old_base_name in base_dataset.former_names
  assert TargetDataset.objects.filter(
    target_schema=rawcore_schema,
    lineage_key=lineage_key,
  ).count() == 2
  base_key = TargetColumn.objects.get(
    target_dataset=base_dataset,
    system_role="surrogate_key",
  )
  assert f"{old_base_name}_key" in list(base_key.former_names or [])
