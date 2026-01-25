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

from django.core.validators import RegexValidator
from django.core.exceptions import ValidationError
import re
from typing import List, Dict, Set, Tuple, Literal, TYPE_CHECKING
from django.apps import apps

if TYPE_CHECKING:
  # Only for static analysis; NOT executed at runtime → no circular import
  from metadata.models import TargetDataset

from metadata.generation.policies import (
  query_tree_allowed_for_dataset, allowed_query_node_types_for_dataset,
  allowed_function_kinds_for_dataset
)
from metadata.generation.query_contract import infer_query_node_contract
from metadata.generation.window_fn_registry import get_window_fn_spec

Severity = Literal["error", "warning"]
Issue = Tuple[Severity, str]

# --- Specific Django field validators (for model fields) ---

SHORT_NAME_VALIDATOR = RegexValidator(
  regex=r"^[a-z][a-z0-9]{0,9}$",
  message=(
    "Must start with a lowercase letter and contain only lowercase letters and digits. "
    "Max length is 10 characters."
  ),
)

TARGET_IDENTIFIER_VALIDATOR = RegexValidator(
  regex=r"^[a-z][a-z0-9_]{0,62}$",
  message=(
    "Must start with a lowercase letter and contain only lowercase letters, "
    "digits, and underscores. Max length is 63 characters."
  ),
)

"""
Validator overview (intentional, non-versioned)

This module contains semantic and metadata validations with two categories:

BLOCKING (must be correct for deterministic execution):
- validate_incremental_target_dataset
- validate_semantic_join_integrity

ADVISORY (quality, governance, presentation):
- validate_bizcore_target_dataset
- validate_serving_friendly_names
- validate_materialization_for_target

Aggregation:
- summarize_targetdataset_health determines overall health level
  based on presence of blocking vs advisory findings.
"""

# --- Serving-friendly identifier helpers (Single Source of Truth) ---
#
# Rationale:
# - Model field validators are static and cannot be schema-dependent.
# - Forms + health checks need the same rules (avoid drift).
#
# Philosophy:
# - allow "what most SQL engines allow in quoted identifiers"
# - block only foot-guns that routinely break tools/pipelines


def serving_clean_identifier(value: str, *, kind: str) -> str:
  """
  Validate and return a cleaned serving identifier.
  'kind' is used for human-readable error messages (Dataset/Column).
  """
  if value is None:
    raise ValidationError(f"{kind} name is required.")

  v = str(value)

  if not v.strip():
    raise ValidationError(f"{kind} name must not be empty.")

  if v.strip() != v:
    raise ValidationError(f"{kind} name must not have leading/trailing spaces.")

  if any(ch in v for ch in ["\n", "\t", "\r"]):
    raise ValidationError(f"{kind} name must not contain newline/tab characters.")

  # Conservative: block quote characters unless you're sure all dialects escape correctly
  # across all supported backends + BI tools.
  if any(ch in v for ch in ['"', "`"]):
    raise ValidationError(f"{kind} name must not contain quote characters (\") or (`).")

  return v


def serving_normalize_identifier(value: str) -> str:
  """
  Case-insensitive + collapse whitespace to catch near-duplicates.
  Example: 'Customer  Name' == 'customer name'
  """
  return " ".join(str(value).lower().split())

# NOTE:
# Friendly identifier validation for Serving is handled via health/validators,
# because Django model field validators are static and cannot inspect td.target_schema.
# See validate_serving_friendly_names() further below.

# --- Generic validator for internal name logic (used by naming.py etc.) ---

NAME_REGEX = r"^[a-z_][a-z0-9_]*$"
NAME_VALIDATOR = re.compile(NAME_REGEX)

def validate_or_raise(name: str, context: str = "name"):
  """Validate a free-form name for internal identifiers."""
  if not NAME_VALIDATOR.match(name or ""):
    raise ValidationError(
      f"{context}: '{name}' is not a valid identifier. "
      "Rules: lowercase letters / digits / underscore, must not start with a digit."
    )

def validate_incremental_target_dataset(td: "TargetDataset") -> List[str]:
  """
  Validate the incremental configuration for a single TargetDataset.
  ...
  """
  issues: List[str] = []

  if td.incremental_strategy != "merge":
    return issues

  # 1) Materialization
  if hasattr(td, "effective_materialization_type"):
    # It might be a property or a method, so handle both cases
    eff = getattr(td, "effective_materialization_type")
    mat = eff() if callable(eff) else eff
  else:
    # Fallback: use field value or assume "table"
    mat = getattr(td, "materialization_type", None) or "table"

  if mat != "table":
    issues.append(
      f"incremental_strategy='merge' but effective materialization_type='{mat}' "
      f"(expected 'table')."
    )

  if not td.incremental_source:
    issues.append(
      "incremental_strategy='merge' but incremental_source is not set."
    )

  key_cols = list(td.natural_key_fields or [])
  if not key_cols:
    issues.append(
      "incremental_strategy='merge' but no natural_key_fields are defined."
    )

  return issues


def validate_all_incremental_targets() -> Dict[int, List[str]]:
  """
  Validate incremental configuration for all TargetDatasets that are
  configured with incremental_strategy != 'full'.
  """
  TargetDataset = apps.get_model("metadata", "TargetDataset")

  result: Dict[int, List[str]] = {}

  qs = TargetDataset.objects.exclude(incremental_strategy="full")

  for td in qs.select_related("target_schema", "incremental_source"):
    issues = validate_incremental_target_dataset(td)
    if issues:
      result[td.pk] = issues

  return result


def validate_bizcore_target_dataset(td: "TargetDataset") -> List[str]:
  """
  Validate BizCore-specific semantics for a single TargetDataset.

  Rules (non-strict, but helpful):
  - Only validate if target_schema.short_name == 'bizcore'
  - Check that:
      * biz_entity_role is set
      * biz_grain_note is set
  """
  issues: List[str] = []

  schema = getattr(td, "target_schema", None)
  short_name = getattr(schema, "short_name", None)

  # Only bizcore layer is in scope for this check
  if short_name != "bizcore":
    return issues

  if not td.biz_entity_role:
    issues.append(
      "BizCore dataset has no biz_entity_role set "
      "(expected e.g. 'core_entity', 'fact', 'dimension', 'reference')."
    )

  if not td.biz_grain_note:
    issues.append(
      "BizCore dataset has no biz_grain_note set "
      "(expected a human-readable description of the business grain)."
    )

  return issues


def validate_semantic_join_integrity(td: "TargetDataset") -> List[str]:
  """
  Validate join metadata integrity for semantic layers (bizcore + serving).

  We treat BOTH bizcore and serving as correctness-critical:
    - if multiple active upstream targets exist, joins must be defined
    - non-cross joins must have predicates
    - join chain must be connected (matches builder MVP constraint)
    - all upstream inputs must be covered by the join tree

  Returns human-readable issues prefixed with ERROR/WARN.
  """
  issues: List[str] = []

  schema = getattr(td, "target_schema", None)
  short_name = getattr(schema, "short_name", None)
  if short_name not in {"bizcore", "serving"}:
    return issues

  severity = "ERROR"  # per your decision: serving must be correct as well

  TargetDatasetInput = apps.get_model("metadata", "TargetDatasetInput")
  TargetDatasetJoin = apps.get_model("metadata", "TargetDatasetJoin")

  upstream_input_ids = list(
    TargetDatasetInput.objects
    .filter(
      target_dataset=td,
      active=True,
      upstream_target_dataset__isnull=False,
    )
    .values_list("id", flat=True)
  )

  # 0 or 1 upstream target -> no joins required
  if len(upstream_input_ids) <= 1:
    return issues

  joins = list(
    TargetDatasetJoin.objects
    .filter(target_dataset=td)
    .select_related("left_input", "right_input")
    .prefetch_related("predicates")
    .order_by("join_order", "id")
  )

  if not joins:
    issues.append(
      f"{severity}: dataset has multiple active upstream targets but no join definitions."
    )
    return issues

  # Predicates required unless CROSS; CROSS must not define predicates
  for j in joins:
    join_type = (j.join_type or "").strip().lower()
    preds = list(j.predicates.all())

    if join_type != "cross" and not preds:
      issues.append(
        f"{severity}: join_order={j.join_order} is '{join_type}' but has no predicates."
      )

    if join_type == "cross" and preds:
      issues.append(
        f"{severity}: join_order={j.join_order} is CROSS but defines predicates (must be empty)."
      )

  # Chain connectivity (MVP model: left side must already be part of join tree)
  used_inputs: set[int] = {joins[0].left_input_id}
  for j in joins:
    if j.left_input_id not in used_inputs:
      issues.append(
        f"{severity}: join chain not connected at join_order={j.join_order} "
        f"(left_input_id={j.left_input_id} not yet part of join tree)."
      )
    used_inputs.add(j.right_input_id)

  # Coverage: every active upstream target input should appear in join tree
  missing = [i for i in upstream_input_ids if i not in used_inputs]
  if missing:
    issues.append(
      f"{severity}: not all active upstream inputs are covered by joins "
      f"(missing TargetDatasetInput ids: {missing})."
    )

  return issues


def validate_serving_friendly_names(td: "TargetDataset") -> List[str]:
  """
  Serving is presentation-facing and may use friendly names (spaces/case/etc.).

  We keep this intentionally pragmatic:
  - block only on obviously broken cases (control whitespace, empty after trim)
  - warn on likely-tooling problems (too long, duplicates after normalization)

  IMPORTANT: This does NOT replace model field validators. See discussion in chat:
  model validators are static and cannot be schema-dependent.
  """
  issues: List[str] = []

  schema = getattr(td, "target_schema", None)
  short_name = getattr(schema, "short_name", None)
  if short_name != "serving":
    return issues

  # Dataset name checks via central helper (Single Source of Truth)
  ds_name = (getattr(td, "target_dataset_name", "") or "")
  try:
    serving_clean_identifier(ds_name, kind="Dataset")
  except ValidationError as e:
    issues.append(f"ERROR: {e}")
  else:
    # Keep portability guardrails as advisory
    if len(ds_name) > 127:
      issues.append("WARN: dataset name is longer than 127 characters (tool compatibility risk).")

  # Column checks
  TargetColumn = apps.get_model("metadata", "TargetColumn")
  cols = list(
    TargetColumn.objects
    .filter(target_dataset=td)
    .order_by("ordinal_position", "id")
    .values_list("target_column_name", flat=True)
  )

  # Normalization: case-insensitive + collapse whitespace
  # This catches confusing near-duplicates in BI tools.
  seen = {}
  for nm in cols:
    name = (nm or "")
    try:
      # Use central helper for hard constraints
      serving_clean_identifier(name, kind="Column")
    except ValidationError as e:
      issues.append(f"ERROR: {e}")
      continue

    norm = serving_normalize_identifier(name)

    if norm in seen:
      issues.append(
        f"ERROR: duplicate serving column names after normalization: '{seen[norm]}' and '{name}'."
      )
    else:
      seen[norm] = name

    # Length: 63 is common, but quoted identifiers can be longer on many platforms.
    # Treat as warning to keep it portable; can be hardened later per dialect.
    if len(name) > 63:
      issues.append(f"WARN: column '{name}' is longer than 63 characters (tool compatibility risk).")

  return issues

def validate_all_semantic_targets() -> Dict[int, List[str]]:
  """
  Validate semantic-layer hints for all TargetDatasets that belong to
  target schemas in {'bizcore', 'serving'}.  
  """
  TargetDataset = apps.get_model("metadata", "TargetDataset")

  result: Dict[int, List[str]] = {}

  qs = (
    TargetDataset.objects
    .select_related("target_schema")
    .filter(target_schema__short_name__in=["bizcore", "serving"])
  )

  for td in qs:
    issues = validate_bizcore_target_dataset(td)
    issues.extend(validate_semantic_join_integrity(td))
    issues.extend(validate_serving_friendly_names(td))
    if issues:
      result[td.pk] = issues

  return result


def validate_materialization_for_target(td: "TargetDataset") -> List[str]:
  """
  Validate effective materialization type of a single TargetDataset
  against the TargetSchema.default_materialization_type.

  This is a soft check; issues are hints, not fatal errors.
  """
  issues: List[str] = []

  schema = getattr(td, "target_schema", None)
  if schema is None:
    return issues

  schema_default = getattr(schema, "default_materialization_type", None)
  if not schema_default:
    # No default configured -> nothing to validate
    return issues

  # Compute effective materialization type (schema default + override)
  if hasattr(td, "effective_materialization_type"):
    eff_attr = getattr(td, "effective_materialization_type")
    effective_mat = eff_attr() if callable(eff_attr) else eff_attr
  else:
    effective_mat = getattr(td, "materialization_type", None) or schema_default

  if effective_mat != schema_default:
    issues.append(
      "Effective materialization_type='{}' differs from schema default "
      "'{}' for schema '{}'.".format(
        effective_mat, schema_default, getattr(schema, "short_name", "<?>")
      )
    )

  return issues


def validate_all_materialization() -> Dict[int, List[str]]:
  """
  Validate materialization type for all TargetDatasets against
  their schema's default_materialization_type.

  Returns:
      Dict[TargetDataset.pk, list of human-readable issues]
  """
  TargetDataset = apps.get_model("metadata", "TargetDataset")

  result: Dict[int, List[str]] = {}

  qs = TargetDataset.objects.select_related("target_schema")

  for td in qs:
    issues = validate_materialization_for_target(td)
    if issues:
      result[td.pk] = issues

  return result


def summarize_targetdataset_health(td: "TargetDataset") -> tuple[str, List[str]]:
  """
  Aggregate health information for a single TargetDataset.

  Combines:
    - incremental configuration issues
    - bizcore semantics (if applicable)
    - materialization expectations

  Returns:
      (level, issues)
      level: 'ok' | 'warning' | 'error'
      issues: list of human-readable messages
  """
  issues: List[str] = []
  has_error = False

  # Incremental
  inc_issues = validate_incremental_target_dataset(td)
  if inc_issues:
    has_error = True
    issues.extend(f"Incremental: {msg}" for msg in inc_issues)

  # BizCore semantics
  biz_issues = validate_bizcore_target_dataset(td)
  issues.extend(f"BizCore: WARN: {msg}" for msg in biz_issues)

  # Join integrity (bizcore + serving) => correctness-critical
  join_issues = validate_semantic_join_integrity(td)
  if join_issues:
    has_error = True
    issues.extend(f"Joins: {msg}" for msg in join_issues)

  # Query tree integrity (if dataset uses a query_root) => correctness-critical
  qt_issues = validate_query_tree_integrity(td)
  for sev, msg in qt_issues:
    issues.append(f"Query: {sev.upper()}: {msg}")
    if sev == "error":
      has_error = True

  # Serving friendly name checks (presentation layer)
  srv_issues = validate_serving_friendly_names(td)
  if srv_issues:
    issues.extend(f"Serving: {msg}" for msg in srv_issues)
    if any("ERROR:" in m for m in srv_issues):
      has_error = True 

  # Materialization
  mat_issues = validate_materialization_for_target(td)
  issues.extend(f"Materialization: WARN: {msg}" for msg in mat_issues)

  # Query contract / governance (if query tree exists)
  q_issues = validate_query_tree_integrity(td)
  if q_issues:
    issues.extend(f"Query: {m}" for m in q_issues)
    if any("ERROR:" in m for m in q_issues):
      has_error = True

  if not issues:
    level = "ok"
  else:
    level = "error" if has_error else "warning"

  return level, issues

# ------------------------------------------------------------------------------
# QueryTree validation (blocking correctness checks)
# ------------------------------------------------------------------------------
def validate_query_tree_integrity(td: "TargetDataset") -> List[str]:
  """
  Validate query graph definition attached to a TargetDataset (if present).
  Returns messages with 'ERROR:' (blocking) or 'WARN:' (advisory).
  """
  issues: List[Issue] = []

  query_root = getattr(td, "query_root", None)
  if not query_root:
    return issues

  if not query_tree_allowed_for_dataset(td):
    issues.append((
      "error",
      f"Custom query logic is only allowed in bizcore/serving. "
      f"Schema '{td.target_schema.short_name}' must not define a query root."
    ))
    return issues

  # Resolve models lazily
  QueryNode = apps.get_model("metadata", "QueryNode")
  QueryNodeType = apps.get_model("metadata", "QueryNodeType") if False else None  # type: ignore
  QuerySelectNode = apps.get_model("metadata", "QuerySelectNode")
  QueryAggregateNode = apps.get_model("metadata", "QueryAggregateNode")
  QueryAggregateGroupKey = apps.get_model("metadata", "QueryAggregateGroupKey")
  QueryAggregateMeasure = apps.get_model("metadata", "QueryAggregateMeasure")
  QueryUnionNode = apps.get_model("metadata", "QueryUnionNode")
  QueryUnionOutputColumn = apps.get_model("metadata", "QueryUnionOutputColumn")
  QueryUnionBranch = apps.get_model("metadata", "QueryUnionBranch")
  QueryUnionBranchMapping = apps.get_model("metadata", "QueryUnionBranchMapping")
  TargetColumn = apps.get_model("metadata", "TargetColumn")

  schema_cache: Dict[int, Set[str]] = {}
  visiting: Set[int] = set()

  # Shared contract cache for the whole validation run
  contract_cache: Dict[int, object] = {}
  contract_visiting: Set[int] = set()

  # Inject root contract issues once (structure/contract-level governance)
  root_contract = infer_query_node_contract(query_root, cache=contract_cache, visiting=contract_visiting)
  for msg in root_contract.issues:
    level = "error" if msg.startswith("ERROR:") else "warning"
    issues.append((level, msg))

  def _infer_output_cols(n) -> Set[str]:
    nid = int(getattr(n, "id", 0) or 0)
    if nid in schema_cache:
      return schema_cache[nid]
    if nid in visiting:
      return set()
    visiting.add(nid)

    # Keep the allowed-node-type check (this is policy, not contract inference)
    ntype = (getattr(n, "node_type", "") or "").strip().lower()
    allowed = allowed_query_node_types_for_dataset(td)
    if ntype not in allowed:
      issues.append((
        "error",
        f"Query node type '{ntype}' is not allowed for schema '{td.target_schema.short_name}'."
      ))

    # Contract-based inference (single source of truth)
    cr = infer_query_node_contract(n, cache=contract_cache, visiting=contract_visiting)
    cols = {c.lower() for c in cr.columns}

    visiting.remove(nid)
    schema_cache[nid] = cols
    return cols

  # Ownership check: root must belong to this dataset
  if getattr(query_root, "target_dataset_id", None) != td.id:
    issues.append(("error", "Query root does not belong to this dataset."))
    return issues

  seen: Set[int] = set()
  stack: Set[int] = set()

  def _node_label(n) -> str:
    nm = getattr(n, "name", "") or ""
    return nm or f"node:{getattr(n, 'id', '?')}"

  def _validate_aggregate(node) -> None:
    agg = getattr(node, "aggregate", None)
    if not agg:
      issues.append(("error", f"Aggregate node '{_node_label(node)}' has no aggregate details."))
      return

    # Input node existence
    if not getattr(agg, "input_node_id", None):
      issues.append(("error", f"Aggregate node '{_node_label(node)}' has no input_node."))
      return

    mode = (getattr(agg, "mode", "") or "").strip().lower()
    group_keys = list(agg.group_keys.all().order_by("ordinal_position", "id"))
    measures = list(agg.measures.all().order_by("ordinal_position", "id"))
    input_cols = _infer_output_cols(agg.input_node) if getattr(agg, "input_node", None) else set()

    allowed_kinds = allowed_function_kinds_for_dataset(td)

    if not measures:
      issues.append(("error", f"Aggregate node '{_node_label(node)}' has no measures."))

    if mode != "global" and not group_keys:
      issues.append(("error", f"Aggregate node '{_node_label(node)}' is grouped but has no group keys."))

    # Basic name collision rules inside aggregate output
    out_names: Set[str] = set()
    for g in group_keys:
      in_name = (getattr(g, "input_column_name", "") or "").strip()
      if not in_name:
        issues.append(("error", f"Aggregate group key in '{_node_label(node)}' has empty input_column_name."))
        continue

      if input_cols and in_name.lower() not in input_cols:
        issues.append((
          "error",
          f"Aggregate node '{_node_label(node)}' references missing input column '{in_name}' in group keys."
        ))

      out_name = (getattr(g, "output_name", "") or "").strip() or in_name
      key = out_name.lower()
      if key in out_names:
        issues.append(("error", f"Aggregate node '{_node_label(node)}' has duplicate output column '{out_name}'."))
      out_names.add(key)

    for m in measures:
      out_name = (getattr(m, "output_name", "") or "").strip()
      if not out_name:
        issues.append(("error", f"Aggregate measure in '{_node_label(node)}' has empty output_name."))
        continue
      key = out_name.lower()
      if key in out_names:
        issues.append(("error", f"Aggregate node '{_node_label(node)}' has duplicate output column '{out_name}'."))
      out_names.add(key)

      fn = (getattr(m, "function", "") or "").strip()
      if not fn:
        issues.append(("error", f"Aggregate measure '{out_name}' in '{_node_label(node)}' has no function."))
      else:
        fn_upper = fn.upper()
        # Aggregate measures are always aggregate functions by definition.
        if "aggregate" not in allowed_kinds:
          issues.append((
            "error",
            f"Aggregate functions are not allowed for schema '{td.target_schema.short_name}' "
            f"(measure '{out_name}' uses '{fn_upper}')."
          ))

        # Determinism rule: STRING_AGG should define ordering
        if fn_upper == "STRING_AGG":
          delim = (getattr(m, "delimiter", "") or ",")
          if delim == "":
            issues.append((
              "warning",
              f"Measure '{out_name}' uses STRING_AGG with an empty delimiter."
            ))

          ob = getattr(m, "order_by", None)
          if not ob:
            issues.append((
              "warning",
              f"Measure '{out_name}' uses STRING_AGG without an explicit ORDER BY; results may be non-deterministic."
            ))
          else:
            items = list(ob.items.all().order_by("ordinal_position", "id"))
            if not items:
              issues.append((
                "error",
                f"Measure '{out_name}' references ORDER BY '{ob.name}', but it has no items."
              ))
            else:
              for it in items:
                col = (it.input_column_name or "").strip()
                if not col:
                  issues.append((
                    "error",
                    f"ORDER BY '{ob.name}' contains an empty column name."
                  ))
                  continue
                if input_cols and col.lower() not in input_cols:
                  issues.append((
                    "error",
                    f"ORDER BY '{ob.name}' references missing input column '{col}' (used by measure '{out_name}')."
                  ))

      # COUNT(*) allowed via empty input column; otherwise require arg
      in_col = (getattr(m, "input_column_name", "") or "").strip()
      if fn.strip().upper() != "COUNT" and not in_col:
        issues.append(("error", f"Aggregate measure '{out_name}' in '{_node_label(node)}' requires an input column."))

      if in_col and input_cols and in_col.lower() not in input_cols:
        issues.append((
          "error",
          f"Aggregate node '{_node_label(node)}' references missing input column '{in_col}' in measure '{out_name}'."
        ))


  def _validate_union(node) -> None:
    un = getattr(node, "union", None)
    if not un:
      issues.append(("error", f"Union node '{_node_label(node)}' has no union details."))
      return

    out_cols = list(un.output_columns.all().order_by("ordinal_position", "id"))
    if not out_cols:
      issues.append(("error", f"Union node '{_node_label(node)}' has no output columns (schema contract required)."))
      return

    # Ensure output column names are unique (case-insensitive)
    seen_names: Set[str] = set()
    for oc in out_cols:
      name = (getattr(oc, "name", "") or "").strip()
      if not name:
        issues.append(("error", f"Union node '{_node_label(node)}' has an empty output column name."))
        continue
      key = name.lower()
      if key in seen_names:
        issues.append(("error", f"Union node '{_node_label(node)}' has duplicate output column '{name}'."))
      seen_names.add(key)

    branches = list(un.branches.all().order_by("ordinal_position", "id"))
    if not branches:
      issues.append(("error", f"Union node '{_node_label(node)}' has no branches."))
      return

    # For each branch, ensure mappings cover all output columns exactly once.
    out_ids = [oc.id for oc in out_cols]
    for b in branches:
      if not getattr(b, "input_node_id", None):
        issues.append(("error", f"Union branch in '{_node_label(node)}' has no input_node."))
        continue

      branch_cols = _infer_output_cols(b.input_node)

      mappings = list(b.mappings.select_related("output_column").all())
      by_out: Dict[int, List[str]] = {}
      for m in mappings:
        oc_id = getattr(m, "output_column_id", None)
        in_name = (getattr(m, "input_column_name", "") or "").strip()
        if not oc_id:
          issues.append(("error", f"Union branch {getattr(b,'id','?')} has mapping without output_column."))
          continue
        if not in_name:
          issues.append(("error", f"Union branch {getattr(b,'id','?')} mapping for '{m.output_column.name}' has empty input_column_name."))
          continue
        if branch_cols and in_name.lower() not in branch_cols:
          issues.append((
            "error",
            f"Union branch {getattr(b,'id','?')} references missing input column '{in_name}' for output '{m.output_column.name}'."
          ))
        by_out.setdefault(int(oc_id), []).append(in_name)

      for oc in out_cols:
        hits = by_out.get(int(oc.id), [])
        if not hits:
          issues.append(("error", f"Union branch {getattr(b,'id','?')} has no mapping for output column '{oc.name}'."))
        elif len(hits) > 1:
          issues.append(("error", f"Union branch {getattr(b,'id','?')} has multiple mappings for output column '{oc.name}'."))

  def _walk(n) -> None:
    nid = int(getattr(n, "id", 0) or 0)
    if nid in stack:
      issues.append(("error", f"Query graph cycle detected at '{_node_label(n)}'."))
      return
    if nid in seen:
      return
    seen.add(nid)
    stack.add(nid)

    ntype = (getattr(n, "node_type", "") or "").strip().lower()
    if ntype == "select":
      # Ensure select details exist
      if not getattr(n, "select", None):
        issues.append(("error", f"Select node '{_node_label(n)}' has no select details."))
    elif ntype == "aggregate":
      _validate_aggregate(n)
      agg = getattr(n, "aggregate", None)
      if agg and getattr(agg, "input_node", None):
        _walk(agg.input_node)
    elif ntype == "union":
      _validate_union(n)
      un = getattr(n, "union", None)
      if un:
        for b in un.branches.all():
          if getattr(b, "input_node", None):
            _walk(b.input_node)
    elif ntype == "window":
      w = getattr(n, "window", None)
      if not w:
        issues.append(("error", f"Window node '{_node_label(n)}' has no window details."))
      else:
        if not getattr(w, "input_node_id", None):
          issues.append(("error", f"Window node '{_node_label(n)}' has no input_node."))
        input_cols = _infer_output_cols(w.input_node) if getattr(w, "input_node", None) else set()

        cols = list(w.columns.all().order_by("ordinal_position", "id"))
        if not cols:
          issues.append(("error", f"Window node '{_node_label(n)}' has no window output columns."))

        for c in cols:
          out_name = (c.output_name or "").strip()
          fn = (c.function or "").strip().upper()
          if not out_name:
            issues.append(("error", f"Window column in '{_node_label(n)}' has empty output_name."))
            continue
          if fn == "ROW_NUMBER":
            if not c.order_by:
              issues.append(("warning", f"Window column '{out_name}' uses ROW_NUMBER without ORDER BY; results may be non-deterministic."))

          spec = get_window_fn_spec(fn)
          if not spec:
            issues.append(("error", f"Unsupported window function '{fn}' (column '{out_name}')."))
            _walk(w.input_node)
            continue

          args = list(c.args.all().order_by("ordinal_position", "id"))

          if not (spec.min_args <= len(args) <= spec.max_args):
            issues.append((
              "error",
              f"Window function {fn} expects {spec.min_args}..{spec.max_args} args, got {len(args)} (column '{out_name}')."
            ))

          # Validate positional arg types (where defined)
          for idx, a in enumerate(args):
            at = (a.arg_type or "").strip().lower()
            if idx < len(spec.arg_schema):
              allowed = spec.arg_schema[idx]
              if at not in allowed:
                issues.append((
                  "error",
                  f"Window function {fn} arg {idx+1} must be one of {allowed}, got '{at}' (column '{out_name}')."
                ))

            # extra numeric constraints
            if fn == "NTILE" and at == "int" and a.int_value is not None and a.int_value <= 0:
              issues.append(("error", f"Window function NTILE requires n > 0 (column '{out_name}')."))
            if fn == "NTH_VALUE" and idx == 1 and at == "int" and a.int_value is not None and a.int_value <= 0:
              issues.append(("error", f"Window function NTH_VALUE requires n > 0 (column '{out_name}')."))
            if fn in ("LAG", "LEAD") and idx == 1 and at == "int" and a.int_value is not None and a.int_value < 0:
              issues.append(("error", f"Window function {fn} offset must be >= 0 (column '{out_name}')."))

            # Verify referenced columns exist in input projection
            if at == "column":
              nm = (a.column_name or "").strip()
              if not nm:
                issues.append(("error", f"Window function {fn} arg {idx+1} column name is empty (column '{out_name}')."))
              elif input_cols and nm.lower() not in input_cols:
                issues.append(("error", f"Window function {fn} arg references missing input column '{nm}' (used by '{out_name}')."))

          if spec.requires_order_by and not c.order_by:
            issues.append(("warning", f"Window column '{out_name}' uses {fn} without ORDER BY; results may be non-deterministic."))

          if c.order_by:
            items = list(c.order_by.items.all())
            if not items:
              issues.append(("error", f"Window column '{out_name}' references ORDER BY '{c.order_by.name}' with no items."))
            for it in items:
              nm = (it.input_column_name or "").strip()
              if input_cols and nm.lower() not in input_cols:
                issues.append(("error", f"ORDER BY '{c.order_by.name}' references missing input column '{nm}' (used by window column '{out_name}')."))
          if c.partition_by:
            items = list(c.partition_by.items.all())
            if not items:
              issues.append(("error", f"Window column '{out_name}' references PARTITION BY '{c.partition_by.name}' with no items."))
            for it in items:
              nm = (it.input_column_name or "").strip()
              if input_cols and nm.lower() not in input_cols:
                issues.append(("error", f"PARTITION BY '{c.partition_by.name}' references missing input column '{nm}' (used by window column '{out_name}')."))

        _walk(w.input_node)
    else:
      issues.append(("error", f"Unsupported query node type '{ntype}' in '{_node_label(n)}'."))

    stack.remove(nid)

  _walk(query_root)

  # Deduplicate messages (same level + same message)
  seen_msgs: Set[Tuple[str, str]] = set()
  out: List[Issue] = []
  for lvl, msg in issues:
    key = (lvl, msg)
    if key in seen_msgs:
      continue
    seen_msgs.add(key)
    out.append((lvl, msg))
  return out