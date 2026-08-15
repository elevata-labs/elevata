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

from io import StringIO

import pytest

from metadata.generation.target_generation_control import (
  build_target_generation_approval,
  build_target_generation_review,
)
from metadata.generation.target_generation_plan import (
  render_target_generation_plan_json,
)
from metadata.generation.target_generation_service import (
  TargetGenerationService,
)
from metadata.management.commands import generate_targets as command_module
from metadata.models import (
  SourceColumn,
  SourceDataset,
  System,
  TargetColumn,
  TargetColumnInput,
  TargetDataset,
  TargetDatasetInput,
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
) -> tuple[SourceDataset, SourceColumn]:
  source_system = System.objects.create(
    short_name=system_short_name,
    name=f"{system_short_name.title()} Source",
    type="db",
    is_source=True,
    is_target=False,
    target_short_name=system_short_name,
    generate_raw_tables=True,
  )
  source_dataset = SourceDataset.objects.create(
    source_system=source_system,
    schema_name="dbo",
    source_dataset_name=dataset_name,
    description="Customer source",
    integrate=True,
    active=True,
    generate_raw_table=True,
  )
  source_column = SourceColumn.objects.create(
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
  return source_dataset, source_column


def _target_counts() -> tuple[int, int, int, int]:
  return (
    TargetDataset.objects.count(),
    TargetColumn.objects.count(),
    TargetDatasetInput.objects.count(),
    TargetColumnInput.objects.count(),
  )


def _action_payloads(plan, action_type: str) -> list[dict]:
  return [
    action.to_dict()
    for action in plan.actions
    if action.action_type == action_type
  ]


@pytest.mark.django_db
def test_build_plan_is_read_only_and_describes_initial_raw_generation(
  monkeypatch,
) -> None:
  raw_schema = _target_schema("raw")
  source_dataset, _ = _source_dataset(
    system_short_name="plan_read_only",
  )
  service = TargetGenerationService(pepper="test-pepper")
  before_counts = _target_counts()

  def _unexpected_mutation(*args, **kwargs):
    raise AssertionError("Target Generation planning must not persist metadata.")

  for method_name in (
    "_save_instance",
    "_create_instance",
    "_get_or_create_instance",
    "_update_or_create_instance",
    "ensure_hist_dataset_for_rawcore",
  ):
    monkeypatch.setattr(service, method_name, _unexpected_mutation)

  plan = service.build_plan(
    [source_dataset],
    raw_schema,
    reconcile_lifecycle=False,
  )

  assert _target_counts() == before_counts
  assert plan.scope_mode == "schema"
  assert plan.target_schema_short_names == ("raw",)
  assert plan.source_dataset_keys == (
    f"source_dataset:{source_dataset.pk}",
  )
  assert plan.action_counts["CREATE_TARGET_DATASET"] == 1
  assert plan.action_counts["CREATE_TARGET_COLUMN"] >= 2
  assert plan.action_counts["SYNC_TARGET_DATASET_INPUTS"] == 1
  assert plan.action_counts["SYNC_TARGET_COLUMN_INPUTS"] >= 2


@pytest.mark.django_db
def test_build_plan_is_empty_after_stable_raw_apply() -> None:
  raw_schema = _target_schema("raw")
  source_dataset, _ = _source_dataset(
    system_short_name="plan_raw_parity",
  )
  service = TargetGenerationService(pepper="test-pepper")

  initial_plan = service.build_plan(
    [source_dataset],
    raw_schema,
  )
  assert initial_plan.action_count > 0

  service.apply_all_result(
    [source_dataset],
    raw_schema,
  )

  stable_plan = service.build_plan(
    [source_dataset],
    raw_schema,
  )

  assert stable_plan.action_count == 0
  assert stable_plan.actions == ()


@pytest.mark.django_db
def test_build_plan_reports_source_column_change_without_target_mutation() -> None:
  raw_schema = _target_schema("raw")
  source_dataset, source_column = _source_dataset(
    system_short_name="plan_column_change",
  )
  service = TargetGenerationService(pepper="test-pepper")
  service.apply_all_result(
    [source_dataset],
    raw_schema,
  )

  target_column = TargetColumn.objects.get(
    target_dataset__target_schema=raw_schema,
    input_links__source_column=source_column,
  )
  assert target_column.datatype == "INTEGER"
  assert target_column.nullable is False

  source_column.datatype = "STRING"
  source_column.max_length = 40
  source_column.nullable = True
  source_column.save(
    update_fields=[
      "datatype",
      "max_length",
      "nullable",
    ]
  )

  plan = service.build_plan(
    [source_dataset],
    raw_schema,
  )
  updates = _action_payloads(plan, "UPDATE_TARGET_COLUMN")
  matching = [
    action
    for action in updates
    if action["after"]["target_column_name"] == "customer_id"
  ]

  assert len(matching) == 1
  assert matching[0]["before"]["datatype"] == "INTEGER"
  assert matching[0]["after"]["datatype"] == "STRING"
  assert matching[0]["after"]["max_length"] == 40
  assert matching[0]["after"]["nullable"] is True

  target_column.refresh_from_db()
  assert target_column.datatype == "INTEGER"
  assert target_column.max_length is None
  assert target_column.nullable is False


@pytest.mark.django_db
def test_build_plan_is_empty_after_stable_rawcore_history_apply() -> None:
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
  source_dataset, _ = _source_dataset(
    system_short_name="plan_hist_parity",
  )
  service = TargetGenerationService(pepper="test-pepper")

  service.apply_all_result([source_dataset], raw_schema)
  service.apply_all_result([source_dataset], stage_schema)

  initial_plan = service.build_plan(
    [source_dataset],
    rawcore_schema,
  )
  assert initial_plan.action_count > 0
  assert any(
    action.effect_origin == "HISTORY_COMPANION"
    for action in initial_plan.actions
  )
  before_counts = _target_counts()

  repeated_plan = service.build_plan(
    [source_dataset],
    rawcore_schema,
  )
  assert repeated_plan.plan_fingerprint == initial_plan.plan_fingerprint
  assert _target_counts() == before_counts

  service.apply_all_result([source_dataset], rawcore_schema)

  convergence_plan = service.build_plan(
    [source_dataset],
    rawcore_schema,
  )
  assert convergence_plan.action_count > 0
  assert all(
    action.action_type == "UPDATE_TARGET_COLUMN"
    for action in convergence_plan.actions
  )

  def _changed_fields(action) -> set[str]:
    payload = action.to_dict()
    before = payload["before"] or {}
    after = payload["after"] or {}
    return {
      field_name
      for field_name, before_value in before.items()
      if before_value != after.get(field_name)
    }

  assert all(
    _changed_fields(action) == {"ordinal_position"}
    for action in convergence_plan.actions
  )

  service.apply_all_result([source_dataset], rawcore_schema)

  stable_plan = service.build_plan(
    [source_dataset],
    rawcore_schema,
  )
  assert stable_plan.action_count == 0


@pytest.mark.django_db
def test_build_plan_describes_lifecycle_retirement_without_applying_it() -> None:
  stage_schema = _target_schema("stage")
  source_dataset, _ = _source_dataset(
    system_short_name="plan_lifecycle",
  )
  service = TargetGenerationService(pepper="test-pepper")
  service.apply_all_result(
    [source_dataset],
    stage_schema,
    reconcile_lifecycle=True,
  )
  generated = TargetDataset.objects.get(
    target_schema=stage_schema,
    lineage_key=service.build_lineage_key_for_bucket(
      stage_schema,
      [source_dataset],
    ),
  )

  plan = service.build_plan(
    [],
    stage_schema,
    reconcile_lifecycle=True,
  )
  retire_actions = _action_payloads(plan, "RETIRE_TARGET_DATASET")

  assert len(retire_actions) == 1
  assert retire_actions[0]["effect_origin"] == "GENERATED_LIFECYCLE"
  assert retire_actions[0]["before"]["active"] is True
  assert retire_actions[0]["after"]["active"] is False
  assert retire_actions[0]["after"]["retired_at_state"] == "set"

  generated.refresh_from_db()
  assert generated.active is True
  assert generated.retired_at is None


@pytest.mark.django_db
def test_generate_targets_command_dry_run_matches_plan_without_mutation(
  monkeypatch,
) -> None:
  """Verify command dry-run renders the exact read-only planner result."""
  raw_schema = _target_schema("raw")
  source_dataset, _ = _source_dataset(
    system_short_name="command_dry_run",
  )
  service = TargetGenerationService(pepper="test-pepper")
  expected_plan = service.build_plan(
    [source_dataset],
    raw_schema,
    reconcile_lifecycle=True,
  )
  before_counts = _target_counts()

  monkeypatch.setattr(
    command_module,
    "get_runtime_pepper",
    lambda: "test-pepper",
  )

  stdout = StringIO()
  command = command_module.Command(stdout=stdout, no_color=True)
  command.handle(
    actor_id=None,
    schema_short_name="raw",
    dry_run=True,
  )

  assert _target_counts() == before_counts
  assert render_target_generation_plan_json(expected_plan) in stdout.getvalue()
  assert TargetDataset.objects.count() == 0
  assert TargetColumn.objects.count() == 0
  assert TargetDatasetInput.objects.count() == 0
  assert TargetColumnInput.objects.count() == 0


@pytest.mark.django_db(transaction=True)
def test_guarded_apply_consumes_exact_raw_plan_after_signal_completion() -> None:
  """Verify guarded apply consumes the reviewed plan and reaches parity."""
  raw_schema = _target_schema("raw")
  source_dataset, _ = _source_dataset(
    system_short_name="guarded_apply_raw",
  )
  service = TargetGenerationService(pepper="test-pepper")
  plan = service.build_plan(
    [source_dataset],
    raw_schema,
    reconcile_lifecycle=True,
  )

  result = service.apply_plan(plan)

  assert result.plan_fingerprint == plan.plan_fingerprint
  assert result.generation_review_fingerprint == (
    build_target_generation_review(plan).review_fingerprint
  )
  assert result.generation_approval_id is None
  assert result.planned_action_count == plan.action_count
  assert result.consumed_action_count == plan.action_count
  assert result.residual_action_count == 0
  assert result.converged is True
  assert result.target_metadata_fingerprint_before == (
    plan.target_metadata_fingerprint
  )
  assert result.target_metadata_fingerprint_after != (
    plan.target_metadata_fingerprint
  )
  assert TargetDataset.objects.filter(target_schema=raw_schema).exists()


@pytest.mark.django_db(transaction=True)
def test_guarded_apply_records_exact_generation_approval_evidence() -> None:
  """Verify guarded metadata mutation records its distinct approval binding."""
  raw_schema = _target_schema("raw")
  source_dataset, _ = _source_dataset(
    system_short_name="guarded_apply_approved_raw",
  )
  service = TargetGenerationService(pepper="test-pepper")
  plan = service.build_plan(
    [source_dataset],
    raw_schema,
    reconcile_lifecycle=True,
  )
  review = build_target_generation_review(plan)
  approval = build_target_generation_approval(
    review=review,
    decided_by="Ilona Tag",
    decided_at="2026-07-30T17:30:00Z",
  )

  result = service.apply_plan(
    plan,
    approval=approval,
    require_approval=True,
  )

  assert result.converged is True
  assert result.generation_review_fingerprint == review.review_fingerprint
  assert result.generation_approval_id == approval.approval_id


@pytest.mark.django_db(transaction=True)
def test_guarded_apply_rejects_source_metadata_drift() -> None:
  """Verify a reviewed plan cannot be applied after Source Metadata changes."""
  from metadata.generation.target_generation_guarded_apply import (
    TargetGenerationPlanDriftError,
  )

  raw_schema = _target_schema("raw")
  source_dataset, source_column = _source_dataset(
    system_short_name="guarded_source_drift",
  )
  service = TargetGenerationService(pepper="test-pepper")
  plan = service.build_plan(
    [source_dataset],
    raw_schema,
    reconcile_lifecycle=True,
  )

  source_column.datatype = "STRING"
  source_column.max_length = 30
  source_column.save(update_fields=["datatype", "max_length"])

  with pytest.raises(
    TargetGenerationPlanDriftError,
    match="source metadata",
  ):
    service.apply_plan(plan)

  assert TargetDataset.objects.filter(target_schema=raw_schema).count() == 0


@pytest.mark.django_db(transaction=True)
def test_guarded_apply_rejects_target_metadata_drift() -> None:
  """Verify a reviewed plan cannot be applied after Target Metadata changes."""
  from metadata.generation.target_generation_guarded_apply import (
    TargetGenerationPlanDriftError,
  )

  raw_schema = _target_schema("raw")
  source_dataset, _ = _source_dataset(
    system_short_name="guarded_target_drift",
  )
  service = TargetGenerationService(pepper="test-pepper")
  plan = service.build_plan(
    [source_dataset],
    raw_schema,
    reconcile_lifecycle=True,
  )

  TargetDataset.objects.create(
    target_schema=raw_schema,
    target_dataset_name="manual_drift",
  )

  with pytest.raises(
    TargetGenerationPlanDriftError,
    match="target metadata",
  ):
    service.apply_plan(plan)

  assert not TargetDataset.objects.filter(
    target_schema=raw_schema,
    target_dataset_name__contains="guarded_target_drift",
  ).exists()


@pytest.mark.django_db(transaction=True)
def test_guarded_apply_empty_plan_skips_generation_replay(monkeypatch) -> None:
  """Verify a converged plan does not rebuild stable metadata relationships."""
  raw_schema = _target_schema("raw")
  source_dataset, _ = _source_dataset(
    system_short_name="guarded_empty_plan",
  )
  service = TargetGenerationService(pepper="test-pepper")
  service.apply_all_result(
    [source_dataset],
    raw_schema,
    reconcile_lifecycle=True,
  )
  plan = service.build_plan(
    [source_dataset],
    raw_schema,
    reconcile_lifecycle=True,
  )
  assert plan.action_count == 0
  before_counts = _target_counts()

  def _unexpected_apply(*args, **kwargs):
    raise AssertionError("An empty guarded plan must not replay generation.")

  monkeypatch.setattr(service, "apply_all_result", _unexpected_apply)

  result = service.apply_plan(plan)

  assert result.converged is True
  assert result.planned_action_count == 0
  assert result.processed_dataset_count == 0
  assert result.processed_column_count == 0
  assert _target_counts() == before_counts
