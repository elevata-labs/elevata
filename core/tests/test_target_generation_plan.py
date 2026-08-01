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

import json

import pytest

from metadata.generation.target_generation_plan import (
  TARGET_GENERATION_PLAN_ARTIFACT_TYPE,
  TARGET_GENERATION_PLAN_ARTIFACT_VERSION,
  TargetGenerationAction,
  TargetGenerationPlan,
  parse_target_generation_plan_json,
  render_target_generation_plan_json,
  target_generation_plan_from_dict,
)


def _fingerprint(character: str) -> str:
  """Return one deterministic SHA-256-shaped test fingerprint."""
  return character * 64


def _dataset_create_action() -> TargetGenerationAction:
  """Return one direct generated TargetDataset creation action."""
  return TargetGenerationAction(
    action_type="CREATE_TARGET_DATASET",
    dataset_key="stage.stg_crm_customer",
    object_key="stage.stg_crm_customer",
    effect_origin="DIRECT",
    change_classification="ADDITIVE",
    source_keys=("crm.public.customer",),
    after={
      "target_dataset_name": "stg_crm_customer",
      "active": True,
      "combination_mode": "single",
    },
    reason="Generated from the selected source dataset bucket.",
  )


def _column_update_action() -> TargetGenerationAction:
  """Return one history-derived TargetColumn change action."""
  return TargetGenerationAction(
    action_type="UPDATE_TARGET_COLUMN",
    dataset_key="rawcore.rc_crm_customer_hist",
    object_key="rawcore.rc_crm_customer_hist.customer_name",
    effect_origin="HISTORY_COMPANION",
    change_classification="BREAKING",
    source_keys=("crm.public.customer.customer_name",),
    before={
      "target_column_name": "customer_name",
      "datatype": "STRING",
      "nullable": True,
      "former_names": [],
    },
    after={
      "nullable": False,
      "former_names": [],
      "datatype": "STRING",
      "target_column_name": "customer_name",
    },
    reason="History companion follows the generated rawcore contract.",
  )


def _plan(
  *,
  actions: tuple[TargetGenerationAction, ...] | None = None,
) -> TargetGenerationPlan:
  """Return one deterministic schema-scoped generation plan."""
  return TargetGenerationPlan(
    scope_mode="schema",
    target_schema_short_names=("stage",),
    source_dataset_keys=(
      "crm.public.customer",
      "crm.public.address",
    ),
    reconcile_lifecycle=True,
    source_metadata_fingerprint=_fingerprint("a"),
    target_metadata_fingerprint=_fingerprint("b"),
    actions=actions or (
      _column_update_action(),
      _dataset_create_action(),
    ),
  )


def test_plan_normalizes_scope_actions_and_nested_state_deterministically():
  """Equivalent unordered inputs must produce one canonical plan."""
  mutable_after = {
    "combination_mode": "single",
    "active": True,
    "target_dataset_name": "stg_crm_customer",
    "nested": {
      "z": 2,
      "a": [
        {"second": 2, "first": 1},
      ],
    },
  }
  create_action = TargetGenerationAction(
    action_type="create_target_dataset",
    dataset_key="stage.stg_crm_customer",
    object_key="stage.stg_crm_customer",
    effect_origin="direct",
    change_classification="additive",
    source_keys=("crm.public.customer",),
    after=mutable_after,
  )
  lifecycle_action = TargetGenerationAction(
    action_type="RETIRE_TARGET_DATASET",
    dataset_key="stage.stg_legacy_customer",
    object_key="stage.stg_legacy_customer",
    effect_origin="GENERATED_LIFECYCLE",
    change_classification="BREAKING",
    before={"active": True, "retired_at": None},
    after={"retired_at": "<planned>", "active": False},
  )

  first = TargetGenerationPlan(
    scope_mode="all",
    target_schema_short_names=("stage", "raw"),
    source_dataset_keys=("crm.customer", "crm.address"),
    reconcile_lifecycle=True,
    source_metadata_fingerprint=_fingerprint("A"),
    target_metadata_fingerprint=_fingerprint("B"),
    actions=(lifecycle_action, create_action),
  )
  second = TargetGenerationPlan(
    scope_mode="all",
    target_schema_short_names=("raw", "stage"),
    source_dataset_keys=("crm.address", "crm.customer"),
    reconcile_lifecycle=True,
    source_metadata_fingerprint=_fingerprint("a"),
    target_metadata_fingerprint=_fingerprint("b"),
    actions=(create_action, lifecycle_action),
  )

  mutable_after["active"] = False
  mutable_after["nested"]["a"][0]["first"] = 999

  assert first == second
  assert first.target_schema_short_names == ("raw", "stage")
  assert first.source_dataset_keys == ("crm.address", "crm.customer")
  assert tuple(action.action_type for action in first.actions) == (
    "CREATE_TARGET_DATASET",
    "RETIRE_TARGET_DATASET",
  )
  assert first.actions[0].to_dict()["after"]["active"] is True
  assert (
    first.actions[0].to_dict()["after"]["nested"]["a"][0]["first"]
    == 1
  )
  assert first.plan_fingerprint == second.plan_fingerprint


def test_plan_payload_contains_stable_counts_and_fingerprint_binding():
  """The public payload must bind scope, states, counts and fingerprints."""
  plan = _plan()

  payload = plan.to_dict()

  assert payload["artifact_type"] == TARGET_GENERATION_PLAN_ARTIFACT_TYPE
  assert payload["artifact_version"] == TARGET_GENERATION_PLAN_ARTIFACT_VERSION
  assert payload["generator_contract_version"] == 1
  assert payload["scope_mode"] == "schema"
  assert payload["target_schema_short_names"] == ["stage"]
  assert payload["source_dataset_keys"] == [
    "crm.public.address",
    "crm.public.customer",
  ]
  assert payload["reconcile_lifecycle"] is True
  assert payload["action_count"] == 2
  assert payload["action_counts"]["CREATE_TARGET_DATASET"] == 1
  assert payload["action_counts"]["UPDATE_TARGET_COLUMN"] == 1
  assert payload["action_counts"]["RETIRE_TARGET_DATASET"] == 0
  assert payload["plan_fingerprint"] == plan.plan_fingerprint

  changed_scope = TargetGenerationPlan(
    scope_mode="schema",
    target_schema_short_names=("stage",),
    source_dataset_keys=("crm.public.customer",),
    reconcile_lifecycle=True,
    source_metadata_fingerprint=_fingerprint("a"),
    target_metadata_fingerprint=_fingerprint("b"),
    actions=plan.actions,
  )
  changed_lifecycle = TargetGenerationPlan(
    scope_mode="schema",
    target_schema_short_names=("stage",),
    source_dataset_keys=plan.source_dataset_keys,
    reconcile_lifecycle=False,
    source_metadata_fingerprint=_fingerprint("a"),
    target_metadata_fingerprint=_fingerprint("b"),
    actions=plan.actions,
  )

  assert changed_scope.plan_fingerprint != plan.plan_fingerprint
  assert changed_lifecycle.plan_fingerprint != plan.plan_fingerprint


def test_render_and_parse_round_trip_uses_canonical_json():
  """Canonical JSON must round-trip to the same immutable plan."""
  plan = _plan()

  rendered = render_target_generation_plan_json(plan)
  parsed = parse_target_generation_plan_json(rendered)

  assert rendered.endswith("\n")
  assert parsed == plan
  assert parsed.plan_fingerprint == plan.plan_fingerprint
  assert render_target_generation_plan_json(parsed) == rendered
  assert json.loads(rendered) == plan.to_dict()


def test_payload_rejects_tampered_action_count_and_fingerprint():
  """Serialized plans must fail closed when counts or actions are changed."""
  plan = _plan()
  payload = plan.to_dict()

  payload["action_count"] = 99
  with pytest.raises(ValueError, match="action count"):
    target_generation_plan_from_dict(payload)

  payload = plan.to_dict()
  payload["actions"][0]["after"]["nullable"] = True
  with pytest.raises(ValueError, match="fingerprint"):
    target_generation_plan_from_dict(payload)


def test_payload_rejects_unknown_versioned_fields():
  """Versioned plan and action payloads require exact field sets."""
  payload = _plan().to_dict()
  payload["unexpected"] = True

  with pytest.raises(ValueError, match="unexpected"):
    target_generation_plan_from_dict(payload)

  payload = _plan().to_dict()
  payload["actions"][0]["unexpected"] = True

  with pytest.raises(ValueError, match="unexpected"):
    target_generation_plan_from_dict(payload)


def test_action_contract_rejects_invalid_state_shapes_and_no_ops():
  """Actions must carry immutable JSON-object states with a real change."""
  with pytest.raises(ValueError, match="must not define a before state"):
    TargetGenerationAction(
      action_type="CREATE_TARGET_COLUMN",
      dataset_key="stage.customer",
      object_key="stage.customer.customer_id",
      effect_origin="DIRECT",
      change_classification="ADDITIVE",
      before={"datatype": "INTEGER"},
      after={"datatype": "INTEGER"},
    )

  with pytest.raises(ValueError, match="semantic state change"):
    TargetGenerationAction(
      action_type="UPDATE_TARGET_COLUMN",
      dataset_key="stage.customer",
      object_key="stage.customer.customer_id",
      effect_origin="DIRECT",
      change_classification="NEUTRAL",
      before={"datatype": "INTEGER"},
      after={"datatype": "INTEGER"},
    )

  with pytest.raises(ValueError, match="JSON object"):
    TargetGenerationAction(
      action_type="CREATE_TARGET_COLUMN",
      dataset_key="stage.customer",
      object_key="stage.customer.customer_id",
      effect_origin="DIRECT",
      change_classification="ADDITIVE",
      after=["not", "an", "object"],
    )

  with pytest.raises(ValueError, match="non-JSON value"):
    TargetGenerationAction(
      action_type="CREATE_TARGET_COLUMN",
      dataset_key="stage.customer",
      object_key="stage.customer.customer_id",
      effect_origin="DIRECT",
      change_classification="ADDITIVE",
      after={"invalid": {"set"}},
    )


def test_plan_rejects_ambiguous_scope_and_duplicate_actions():
  """Scope and action identity must be explicit and unambiguous."""
  action = _dataset_create_action()

  with pytest.raises(ValueError, match="exactly one"):
    TargetGenerationPlan(
      scope_mode="schema",
      target_schema_short_names=("raw", "stage"),
      source_dataset_keys=(),
      reconcile_lifecycle=False,
      source_metadata_fingerprint=_fingerprint("a"),
      target_metadata_fingerprint=_fingerprint("b"),
      actions=(),
    )

  with pytest.raises(ValueError, match="duplicate actions"):
    TargetGenerationPlan(
      scope_mode="schema",
      target_schema_short_names=("stage",),
      source_dataset_keys=("crm.public.customer",),
      reconcile_lifecycle=False,
      source_metadata_fingerprint=_fingerprint("a"),
      target_metadata_fingerprint=_fingerprint("b"),
      actions=(action, action),
    )
