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

from typing import Set, Dict, Literal, Optional

FunctionKind = Literal["scalar", "aggregate", "window"]

ALLOWED_NODE_TYPES_BY_SCHEMA: Dict[str, Set[str]] = {
  # Custom logic is restricted to bizcore/serving (see query_tree_allowed_for_dataset()).
  "bizcore": {"select", "aggregate", "union", "window"},
  "serving": {"select", "aggregate", "union", "window"},
}

ALLOWED_FUNCTION_KINDS_BY_SCHEMA: Dict[str, Set[FunctionKind]] = {
  # Custom logic is restricted to bizcore/serving; inside that:
  # - bizcore: business logic (aggregates ok, window ok if needed)
  # - serving: presentation logic (aggregates ok, window ok)
  "bizcore": {"scalar", "aggregate", "window"},
  "serving": {"scalar", "aggregate", "window"},
}

def schema_short_for_dataset(td) -> str:
  """
  Return the schema short name for a TargetDataset (e.g. 'bizcore', 'serving').
  """
  schema = getattr(td, "target_schema", None)
  return getattr(schema, "short_name", "") or ""


def query_tree_allowed_for_dataset(td) -> bool:
  """
  Custom query logic (query trees / operators) is only allowed in bizcore + serving.
  raw/rawcore/stage are auto-generated and must not expose manual logic.
  """
  return schema_short_for_dataset(td) in ("bizcore", "serving")


def downstream_dependents_qs_for_dataset(td):
  """
  Return a queryset of TargetDatasetInput rows where td is used as upstream_target_dataset.
  If the model supports an 'active' flag, only count active dependents.
  Implemented with apps.get_model to avoid import tangles.
  """
  try:
    from django.apps import apps
    TargetDatasetInput = apps.get_model("metadata", "TargetDatasetInput")
    qs = TargetDatasetInput.objects.filter(upstream_target_dataset=td)
    if hasattr(TargetDatasetInput, "active"):
      qs = qs.filter(active=True)
    return qs
  except Exception:
    # Be conservative: if we cannot resolve dependencies reliably, return an empty qs-like list.
    return []


def downstream_dependents_exist_for_dataset(td) -> bool:
  qs = downstream_dependents_qs_for_dataset(td)
  try:
    return qs.exists()
  except Exception:
    return bool(qs)


def downstream_dependents_count_for_dataset(td) -> int:
  qs = downstream_dependents_qs_for_dataset(td)
  try:
    return qs.count()
  except Exception:
    return len(list(qs))


def query_tree_mutations_allowed_for_dataset(td) -> bool:
  """
  A query tree can only be *modified* when:
  - query trees are allowed for the dataset (bizcore/serving), AND
  - no downstream datasets depend on it (otherwise contract changes would break them).
  """
  if not query_tree_allowed_for_dataset(td):
    return False
  if downstream_dependents_exist_for_dataset(td):
    return False
  return True


def query_tree_mutation_block_reason(td) -> str:
  """
  Human-readable reason why mutations are blocked.
  Kept here so the message is consistent across views.
  """
  if not query_tree_allowed_for_dataset(td):
    return "Custom query logic is only allowed in bizcore/serving."
  if downstream_dependents_exist_for_dataset(td):
    return "Blocked: downstream datasets depend on this dataset (see Lineage)."
  return ""


def allowed_function_kinds_for_dataset(td) -> Set[FunctionKind]:
  return set(ALLOWED_FUNCTION_KINDS_BY_SCHEMA.get(schema_short_for_dataset(td), set()))


def allowed_query_node_types_for_dataset(td) -> Set[str]:
  """
  Allowed query node operator types for the dataset's schema.
  """
  return set(ALLOWED_NODE_TYPES_BY_SCHEMA.get(schema_short_for_dataset(td), set()))


def query_head_for_dataset(td):
  """
  Return the effective query head for a dataset.
  Falls back to query_root if query_head is missing or dangling.
  """
  try:
    head = getattr(td, "query_head", None)
  except Exception:
    head = None

  if head is not None:
    return head

  try:
    return getattr(td, "query_root", None)
  except Exception:
    return None