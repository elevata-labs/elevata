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

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import hashlib
import json
import math
import re
from typing import Any, Literal


TARGET_GENERATION_PLAN_ARTIFACT_TYPE = "target_generation_plan"
TARGET_GENERATION_PLAN_ARTIFACT_VERSION = 1
TARGET_GENERATOR_CONTRACT_VERSION = 1

TargetGenerationScopeMode = Literal[
  "schema",
  "all",
]
TargetGenerationActionType = Literal[
  "CREATE_TARGET_DATASET",
  "UPDATE_TARGET_DATASET",
  "RETIRE_TARGET_DATASET",
  "REACTIVATE_TARGET_DATASET",
  "CREATE_TARGET_COLUMN",
  "UPDATE_TARGET_COLUMN",
  "RETIRE_TARGET_COLUMN",
  "REACTIVATE_TARGET_COLUMN",
  "SYNC_TARGET_DATASET_INPUTS",
  "SYNC_TARGET_COLUMN_INPUTS",
]
TargetGenerationEffectOrigin = Literal[
  "DIRECT",
  "HISTORY_COMPANION",
  "GENERATED_LIFECYCLE",
  "MODEL_SIDE_EFFECT",
]
TargetGenerationChangeClassification = Literal[
  "ADDITIVE",
  "BREAKING",
  "NEUTRAL",
]

_ACTION_TYPE_ORDER: tuple[TargetGenerationActionType, ...] = (
  "CREATE_TARGET_DATASET",
  "UPDATE_TARGET_DATASET",
  "RETIRE_TARGET_DATASET",
  "REACTIVATE_TARGET_DATASET",
  "CREATE_TARGET_COLUMN",
  "UPDATE_TARGET_COLUMN",
  "RETIRE_TARGET_COLUMN",
  "REACTIVATE_TARGET_COLUMN",
  "SYNC_TARGET_DATASET_INPUTS",
  "SYNC_TARGET_COLUMN_INPUTS",
)
_ALLOWED_SCOPE_MODES = frozenset({
  "schema",
  "all",
})
_ALLOWED_ACTION_TYPES = frozenset(_ACTION_TYPE_ORDER)
_ALLOWED_EFFECT_ORIGINS = frozenset({
  "DIRECT",
  "HISTORY_COMPANION",
  "GENERATED_LIFECYCLE",
  "MODEL_SIDE_EFFECT",
})
_ALLOWED_CHANGE_CLASSIFICATIONS = frozenset({
  "ADDITIVE",
  "BREAKING",
  "NEUTRAL",
})
_CREATE_ACTION_TYPES = frozenset({
  "CREATE_TARGET_DATASET",
  "CREATE_TARGET_COLUMN",
})
_DATASET_ACTION_TYPES = frozenset({
  "CREATE_TARGET_DATASET",
  "UPDATE_TARGET_DATASET",
  "RETIRE_TARGET_DATASET",
  "REACTIVATE_TARGET_DATASET",
  "SYNC_TARGET_DATASET_INPUTS",
})
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")

_PLAN_PAYLOAD_KEYS = frozenset({
  "artifact_type",
  "artifact_version",
  "generator_contract_version",
  "scope_mode",
  "target_schema_short_names",
  "source_dataset_keys",
  "reconcile_lifecycle",
  "source_metadata_fingerprint",
  "target_metadata_fingerprint",
  "action_count",
  "action_counts",
  "actions",
  "plan_fingerprint",
})
_ACTION_PAYLOAD_KEYS = frozenset({
  "action_type",
  "dataset_key",
  "object_key",
  "effect_origin",
  "change_classification",
  "source_keys",
  "before",
  "after",
  "reason",
})


@dataclass(frozen=True)
class _CanonicalJsonObject:
  """Immutable canonical representation of one JSON object."""

  items: tuple[tuple[str, Any], ...]


@dataclass(frozen=True)
class _CanonicalJsonArray:
  """Immutable canonical representation of one JSON array."""

  items: tuple[Any, ...]


@dataclass(frozen=True)
class TargetGenerationAction:
  """
  Describe one semantic Target Metadata change in a generation plan.

  The action is deliberately independent from Django ORM mutation methods.
  It binds one normalized before/after contract to a stable object key and
  records whether the effect is direct, history-derived, lifecycle-derived,
  or caused by an existing model side effect.
  """

  action_type: TargetGenerationActionType
  dataset_key: str
  object_key: str
  effect_origin: TargetGenerationEffectOrigin
  change_classification: TargetGenerationChangeClassification
  source_keys: tuple[str, ...] = ()
  before: Mapping[str, Any] | _CanonicalJsonObject | None = None
  after: Mapping[str, Any] | _CanonicalJsonObject | None = None
  reason: str = ""

  def __post_init__(self) -> None:
    action_type = str(self.action_type or "").strip().upper()
    if action_type not in _ALLOWED_ACTION_TYPES:
      raise ValueError(
        f"Unsupported Target Generation action type: {action_type}"
      )

    dataset_key = _required_text(
      self.dataset_key,
      label="Target Generation action dataset key",
    )
    object_key = _required_text(
      self.object_key,
      label="Target Generation action object key",
    )

    if action_type in _DATASET_ACTION_TYPES and object_key != dataset_key:
      raise ValueError(
        "Dataset-level Target Generation actions must use the dataset key "
        "as their object key."
      )

    effect_origin = str(self.effect_origin or "").strip().upper()
    if effect_origin not in _ALLOWED_EFFECT_ORIGINS:
      raise ValueError(
        "Unsupported Target Generation effect origin: "
        f"{effect_origin}"
      )

    change_classification = str(
      self.change_classification or ""
    ).strip().upper()
    if change_classification not in _ALLOWED_CHANGE_CLASSIFICATIONS:
      raise ValueError(
        "Unsupported Target Generation change classification: "
        f"{change_classification}"
      )

    source_keys = _normalized_text_tuple(
      self.source_keys,
      label="Target Generation action source key",
    )
    before = _normalize_state(
      self.before,
      label="Target Generation action before state",
    )
    after = _normalize_state(
      self.after,
      label="Target Generation action after state",
    )
    reason = str(self.reason or "").strip()

    if action_type in _CREATE_ACTION_TYPES:
      if before is not None:
        raise ValueError(
          "Create Target Generation actions must not define a before state."
        )
      if after is None:
        raise ValueError(
          "Create Target Generation actions require an after state."
        )
    else:
      if before is None or after is None:
        raise ValueError(
          "Non-create Target Generation actions require before and after "
          "states."
        )
      if before == after:
        raise ValueError(
          "Target Generation actions must describe a semantic state change."
        )

    object.__setattr__(self, "action_type", action_type)
    object.__setattr__(self, "dataset_key", dataset_key)
    object.__setattr__(self, "object_key", object_key)
    object.__setattr__(self, "effect_origin", effect_origin)
    object.__setattr__(
      self,
      "change_classification",
      change_classification,
    )
    object.__setattr__(self, "source_keys", source_keys)
    object.__setattr__(self, "before", before)
    object.__setattr__(self, "after", after)
    object.__setattr__(self, "reason", reason)

  def to_dict(self) -> dict[str, Any]:
    """Return the canonical public action payload."""
    return {
      "action_type": self.action_type,
      "dataset_key": self.dataset_key,
      "object_key": self.object_key,
      "effect_origin": self.effect_origin,
      "change_classification": self.change_classification,
      "source_keys": list(self.source_keys),
      "before": _json_value_to_public(self.before),
      "after": _json_value_to_public(self.after),
      "reason": self.reason,
    }

  @property
  def sort_key(self) -> tuple[Any, ...]:
    """Return the deterministic ordering key for this action."""
    return (
      _ACTION_TYPE_ORDER.index(self.action_type),
      self.dataset_key,
      self.object_key,
      self.effect_origin,
      self.change_classification,
      self.source_keys,
      _canonical_json_text(_json_value_to_public(self.before)),
      _canonical_json_text(_json_value_to_public(self.after)),
      self.reason,
    )


@dataclass(frozen=True)
class TargetGenerationPlan:
  """
  Immutable deterministic contract for one Target Generation decision set.

  The plan binds normalized semantic actions to the exact selected generation
  scope and to fingerprints of the relevant Source and current Target Metadata.
  It does not read or mutate metadata and is not an apply implementation.
  """

  scope_mode: TargetGenerationScopeMode
  target_schema_short_names: tuple[str, ...]
  source_dataset_keys: tuple[str, ...]
  reconcile_lifecycle: bool
  source_metadata_fingerprint: str
  target_metadata_fingerprint: str
  actions: tuple[TargetGenerationAction, ...] = ()
  generator_contract_version: int = TARGET_GENERATOR_CONTRACT_VERSION

  def __post_init__(self) -> None:
    scope_mode = str(self.scope_mode or "").strip().lower()
    if scope_mode not in _ALLOWED_SCOPE_MODES:
      raise ValueError(
        f"Unsupported Target Generation scope mode: {scope_mode}"
      )

    target_schema_short_names = _normalized_text_tuple(
      self.target_schema_short_names,
      label="Target Generation target schema short name",
    )
    if not target_schema_short_names:
      raise ValueError(
        "Target Generation plans require at least one target schema."
      )
    if scope_mode == "schema" and len(target_schema_short_names) != 1:
      raise ValueError(
        "Schema-scoped Target Generation plans require exactly one target "
        "schema."
      )

    source_dataset_keys = _normalized_text_tuple(
      self.source_dataset_keys,
      label="Target Generation source dataset key",
    )

    if not isinstance(self.reconcile_lifecycle, bool):
      raise ValueError(
        "Target Generation reconcile_lifecycle must be a boolean."
      )

    source_metadata_fingerprint = _fingerprint(
      self.source_metadata_fingerprint,
      label="source metadata",
    )
    target_metadata_fingerprint = _fingerprint(
      self.target_metadata_fingerprint,
      label="target metadata",
    )
    generator_contract_version = _positive_int(
      self.generator_contract_version,
      label="generator contract version",
    )

    actions = tuple(self.actions or ())
    if any(
      not isinstance(action, TargetGenerationAction)
      for action in actions
    ):
      raise ValueError(
        "Target Generation plans require validated generation actions."
      )
    actions = tuple(sorted(actions, key=lambda action: action.sort_key))

    action_payloads = tuple(
      _canonical_json_text(action.to_dict())
      for action in actions
    )
    if len(action_payloads) != len(set(action_payloads)):
      raise ValueError(
        "Target Generation plans must not contain duplicate actions."
      )

    object.__setattr__(self, "scope_mode", scope_mode)
    object.__setattr__(
      self,
      "target_schema_short_names",
      target_schema_short_names,
    )
    object.__setattr__(
      self,
      "source_dataset_keys",
      source_dataset_keys,
    )
    object.__setattr__(
      self,
      "source_metadata_fingerprint",
      source_metadata_fingerprint,
    )
    object.__setattr__(
      self,
      "target_metadata_fingerprint",
      target_metadata_fingerprint,
    )
    object.__setattr__(self, "actions", actions)
    object.__setattr__(
      self,
      "generator_contract_version",
      generator_contract_version,
    )

  @property
  def action_count(self) -> int:
    """Return the number of semantic actions in the plan."""
    return len(self.actions)

  @property
  def action_counts(self) -> dict[TargetGenerationActionType, int]:
    """Return stable counts for every Target Generation action type."""
    return {
      action_type: sum(
        1
        for action in self.actions
        if action.action_type == action_type
      )
      for action_type in _ACTION_TYPE_ORDER
    }

  @property
  def plan_fingerprint(self) -> str:
    """Return the deterministic fingerprint of the complete plan."""
    return _stable_json_hash(
      self.to_dict(include_fingerprint=False)
    )

  def to_dict(
    self,
    *,
    include_fingerprint: bool = True,
  ) -> dict[str, Any]:
    """Return the canonical public Target Generation Plan payload."""
    payload = {
      "artifact_type": TARGET_GENERATION_PLAN_ARTIFACT_TYPE,
      "artifact_version": TARGET_GENERATION_PLAN_ARTIFACT_VERSION,
      "generator_contract_version": self.generator_contract_version,
      "scope_mode": self.scope_mode,
      "target_schema_short_names": list(
        self.target_schema_short_names
      ),
      "source_dataset_keys": list(self.source_dataset_keys),
      "reconcile_lifecycle": self.reconcile_lifecycle,
      "source_metadata_fingerprint": (
        self.source_metadata_fingerprint
      ),
      "target_metadata_fingerprint": (
        self.target_metadata_fingerprint
      ),
      "action_count": self.action_count,
      "action_counts": self.action_counts,
      "actions": [
        action.to_dict()
        for action in self.actions
      ],
    }

    if include_fingerprint:
      payload["plan_fingerprint"] = self.plan_fingerprint

    return payload


def build_target_generation_plan(
  *,
  scope_mode: TargetGenerationScopeMode,
  target_schema_short_names: Sequence[str],
  source_dataset_keys: Sequence[str],
  reconcile_lifecycle: bool,
  source_metadata_fingerprint: str,
  target_metadata_fingerprint: str,
  actions: Sequence[TargetGenerationAction],
  generator_contract_version: int = TARGET_GENERATOR_CONTRACT_VERSION,
) -> TargetGenerationPlan:
  """Build and validate one deterministic Target Generation Plan."""
  return TargetGenerationPlan(
    scope_mode=scope_mode,
    target_schema_short_names=tuple(target_schema_short_names),
    source_dataset_keys=tuple(source_dataset_keys),
    reconcile_lifecycle=reconcile_lifecycle,
    source_metadata_fingerprint=source_metadata_fingerprint,
    target_metadata_fingerprint=target_metadata_fingerprint,
    actions=tuple(actions),
    generator_contract_version=generator_contract_version,
  )


def target_generation_plan_from_dict(
  value: Mapping[str, Any],
) -> TargetGenerationPlan:
  """Parse and validate one Target Generation Plan payload."""
  payload = _require_mapping(
    value,
    label="Target Generation Plan payload",
  )
  _require_exact_keys(
    payload,
    expected=_PLAN_PAYLOAD_KEYS,
    label="Target Generation Plan",
  )

  if payload.get("artifact_type") != TARGET_GENERATION_PLAN_ARTIFACT_TYPE:
    raise ValueError(
      "Target Generation Plan artifact type is invalid."
    )
  if (
    payload.get("artifact_version")
    != TARGET_GENERATION_PLAN_ARTIFACT_VERSION
  ):
    raise ValueError(
      "Unsupported Target Generation Plan artifact version: "
      f"{payload.get('artifact_version')}"
    )

  raw_schema_names = _require_sequence(
    payload.get("target_schema_short_names"),
    label="target schema short names",
  )
  raw_source_dataset_keys = _require_sequence(
    payload.get("source_dataset_keys"),
    label="source dataset keys",
  )
  raw_actions = _require_sequence(
    payload.get("actions"),
    label="actions",
  )

  actions: list[TargetGenerationAction] = []
  for raw_action in raw_actions:
    action_payload = _require_mapping(
      raw_action,
      label="action",
    )
    _require_exact_keys(
      action_payload,
      expected=_ACTION_PAYLOAD_KEYS,
      label="Target Generation action",
    )

    raw_source_keys = _require_sequence(
      action_payload.get("source_keys"),
      label="action source keys",
    )
    before = action_payload.get("before")
    after = action_payload.get("after")

    if before is not None:
      before = _require_mapping(
        before,
        label="action before state",
      )
    if after is not None:
      after = _require_mapping(
        after,
        label="action after state",
      )

    actions.append(
      TargetGenerationAction(
        action_type=action_payload.get("action_type"),
        dataset_key=action_payload.get("dataset_key"),
        object_key=action_payload.get("object_key"),
        effect_origin=action_payload.get("effect_origin"),
        change_classification=action_payload.get(
          "change_classification"
        ),
        source_keys=tuple(raw_source_keys),
        before=before,
        after=after,
        reason=action_payload.get("reason"),
      )
    )

  plan = TargetGenerationPlan(
    scope_mode=payload.get("scope_mode"),
    target_schema_short_names=tuple(raw_schema_names),
    source_dataset_keys=tuple(raw_source_dataset_keys),
    reconcile_lifecycle=payload.get("reconcile_lifecycle"),
    source_metadata_fingerprint=payload.get(
      "source_metadata_fingerprint"
    ),
    target_metadata_fingerprint=payload.get(
      "target_metadata_fingerprint"
    ),
    actions=tuple(actions),
    generator_contract_version=payload.get(
      "generator_contract_version"
    ),
  )

  action_count = _non_negative_int(
    payload.get("action_count"),
    label="action count",
  )
  if action_count != plan.action_count:
    raise ValueError(
      "Target Generation Plan action count does not match its actions."
    )

  raw_action_counts = _require_mapping(
    payload.get("action_counts"),
    label="action counts",
  )
  _require_exact_keys(
    raw_action_counts,
    expected=frozenset(_ACTION_TYPE_ORDER),
    label="Target Generation Plan action counts",
  )
  action_counts = {
    action_type: _non_negative_int(
      raw_action_counts.get(action_type),
      label=f"{action_type} action count",
    )
    for action_type in _ACTION_TYPE_ORDER
  }
  if action_counts != plan.action_counts:
    raise ValueError(
      "Target Generation Plan action counts do not match its actions."
    )

  expected_fingerprint = _fingerprint(
    payload.get("plan_fingerprint"),
    label="plan",
  )
  if expected_fingerprint != plan.plan_fingerprint:
    raise ValueError(
      "Target Generation Plan fingerprint does not match its canonical "
      "payload."
    )

  return plan


def parse_target_generation_plan_json(value: str) -> TargetGenerationPlan:
  """Parse and validate one Target Generation Plan JSON document."""
  try:
    payload = json.loads(str(value))
  except (TypeError, ValueError) as exc:
    raise ValueError(
      "Target Generation Plan JSON is invalid."
    ) from exc

  return target_generation_plan_from_dict(payload)


def render_target_generation_plan_json(plan: TargetGenerationPlan) -> str:
  """Render one Target Generation Plan as canonical readable JSON."""
  if not isinstance(plan, TargetGenerationPlan):
    raise ValueError(
      "Target Generation Plan rendering requires a validated plan."
    )

  return json.dumps(
    plan.to_dict(),
    sort_keys=True,
    ensure_ascii=False,
    allow_nan=False,
    indent=2,
  ) + "\n"


def _normalize_state(
  value: Any,
  *,
  label: str,
) -> _CanonicalJsonObject | None:
  """Return one immutable canonical JSON-object-shaped state."""
  if value is None:
    return None
  if isinstance(value, _CanonicalJsonObject):
    return value
  if not isinstance(value, Mapping):
    raise ValueError(f"{label} must be a JSON object or null.")

  normalized = _canonicalize_json_value(value, label=label)
  if not isinstance(normalized, _CanonicalJsonObject):
    raise ValueError(f"{label} must be a JSON object or null.")
  return normalized


def _canonicalize_json_value(value: Any, *, label: str) -> Any:
  """Deep-freeze and normalize one JSON-compatible value."""
  if value is None or isinstance(value, (str, bool, int)):
    return value

  if isinstance(value, float):
    if not math.isfinite(value):
      raise ValueError(f"{label} contains a non-finite number.")
    return value

  if isinstance(value, Mapping):
    items: list[tuple[str, Any]] = []
    for key, item_value in value.items():
      if not isinstance(key, str):
        raise ValueError(f"{label} contains a non-string object key.")
      items.append((
        key,
        _canonicalize_json_value(
          item_value,
          label=label,
        ),
      ))
    items.sort(key=lambda item: item[0])
    return _CanonicalJsonObject(tuple(items))

  if (
    isinstance(value, Sequence)
    and not isinstance(value, (str, bytes, bytearray))
  ):
    return _CanonicalJsonArray(tuple(
      _canonicalize_json_value(item, label=label)
      for item in value
    ))

  raise ValueError(
    f"{label} contains a non-JSON value: {type(value).__name__}."
  )


def _json_value_to_public(value: Any) -> Any:
  """Return the JSON-compatible representation of a canonical value."""
  if isinstance(value, _CanonicalJsonObject):
    return {
      key: _json_value_to_public(item_value)
      for key, item_value in value.items
    }
  if isinstance(value, _CanonicalJsonArray):
    return [
      _json_value_to_public(item)
      for item in value.items
    ]
  return value


def _required_text(value: Any, *, label: str) -> str:
  """Normalize one required non-empty string."""
  normalized = str(value or "").strip()
  if not normalized:
    raise ValueError(f"{label} must not be empty.")
  return normalized


def _normalized_text_tuple(
  value: Any,
  *,
  label: str,
) -> tuple[str, ...]:
  """Return sorted unique non-empty strings from one sequence."""
  if value is None:
    return ()
  if (
    not isinstance(value, Sequence)
    or isinstance(value, (str, bytes, bytearray))
  ):
    raise ValueError(f"{label}s must be a sequence.")

  normalized = tuple(
    _required_text(item, label=label)
    for item in value
  )
  if len(normalized) != len(set(normalized)):
    raise ValueError(f"Duplicate {label}s are not allowed.")
  return tuple(sorted(normalized))


def _fingerprint(value: Any, *, label: str) -> str:
  """Normalize and validate one SHA-256 fingerprint."""
  normalized = _required_text(
    value,
    label=f"Target Generation Plan {label} fingerprint",
  )
  if not _SHA256_RE.fullmatch(normalized):
    raise ValueError(
      f"Target Generation Plan {label} fingerprint must be SHA-256."
    )
  return normalized.lower()


def _positive_int(value: Any, *, label: str) -> int:
  """Return one strict positive integer."""
  if isinstance(value, bool) or not isinstance(value, int):
    raise ValueError(f"Target Generation Plan {label} must be an integer.")
  if value < 1:
    raise ValueError(f"Target Generation Plan {label} must be positive.")
  return value


def _non_negative_int(value: Any, *, label: str) -> int:
  """Return one strict non-negative integer."""
  if isinstance(value, bool) or not isinstance(value, int):
    raise ValueError(f"Target Generation Plan {label} must be an integer.")
  if value < 0:
    raise ValueError(
      f"Target Generation Plan {label} must not be negative."
    )
  return value


def _require_mapping(value: Any, *, label: str) -> Mapping[str, Any]:
  """Return one required JSON-object-shaped value."""
  if not isinstance(value, Mapping):
    raise ValueError(f"{label} must be a JSON object.")
  return value


def _require_sequence(value: Any, *, label: str) -> Sequence[Any]:
  """Return one required JSON-array-shaped value."""
  if (
    not isinstance(value, Sequence)
    or isinstance(value, (str, bytes, bytearray))
  ):
    raise ValueError(
      f"Target Generation Plan {label} must be a JSON array."
    )
  return value


def _require_exact_keys(
  value: Mapping[str, Any],
  *,
  expected: frozenset[str],
  label: str,
) -> None:
  """Enforce the exact field set for one versioned contract object."""
  actual = frozenset(str(key) for key in value.keys())
  if actual == expected:
    return

  missing = tuple(sorted(expected - actual))
  unexpected = tuple(sorted(actual - expected))
  details: list[str] = []

  if missing:
    details.append("missing: " + ", ".join(missing))
  if unexpected:
    details.append("unexpected: " + ", ".join(unexpected))

  raise ValueError(
    f"{label} fields do not match artifact version "
    f"{TARGET_GENERATION_PLAN_ARTIFACT_VERSION}; "
    + "; ".join(details)
    + "."
  )


def _canonical_json_text(value: Any) -> str:
  """Return compact canonical JSON for ordering and duplicate checks."""
  return json.dumps(
    value,
    sort_keys=True,
    ensure_ascii=False,
    allow_nan=False,
    separators=(",", ":"),
  )


def _stable_json_hash(value: Any) -> str:
  """Return a deterministic SHA-256 hash for a JSON value."""
  payload = _canonical_json_text(value)
  return hashlib.sha256(payload.encode("utf-8")).hexdigest()
