"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025 Ilona Tag

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

from typing import List, Optional, TYPE_CHECKING
import re

from metadata.rendering.expr import (
  Expr,
  ColumnRef,
  RawSql,
)
from metadata.rendering.logical_plan import (
  SourceTable,
  SelectItem,
  LogicalSelect,
  LogicalUnion,
)
from metadata.generation.naming import build_surrogate_key_name

if TYPE_CHECKING:
  # Only for type hints, no runtime import -> no circular import
  from metadata.models import TargetDataset, TargetColumn, TargetDatasetReference

"""
Logical Query Builder for TargetDatasets.
Responsible for SK/FK hashing, lineage resolution,
and merging multiple inputs (UNION or single-path).
"""

# ------------------------------------------------------------------------------
# Regex that matches col("column_name") in DSL expressions
# ------------------------------------------------------------------------------
COL_PATTERN = re.compile(r'col\(["\']([^"\']+)["\']\)')


# ------------------------------------------------------------------------------
# Extract Stage-level lineage of a RawCore column
# ------------------------------------------------------------------------------
def _get_stage_expr_for_rawcore_col(col: TargetColumn) -> Optional[str]:
  """
  Resolve the Stage-level expression feeding this RawCore column.

  Priority:
    1) manual_expression (overrides all)
    2) upstream_target_column (Stage → RawCore lineage)

  Returns:
    DSL expression string such as 'col("stage_col")'
    or a manual expression, or None if no Stage lineage is found.
  """
  # Manual expression always wins
  if col.manual_expression:
    return col.manual_expression

  # Upstream Stage column
  link = (
    col.input_links
    .filter(active=True)
    .select_related("upstream_target_column")
    .order_by("ordinal_position", "id")
    .first()
  )
  if link and link.upstream_target_column:
    stage_col = link.upstream_target_column
    return f'col("{stage_col.target_column_name}")'

  # No Stage lineage
  return None


# ------------------------------------------------------------------------------
# Rewrite parent SK expression so that left side stays parent-BK,
# and right side becomes the child Stage expression.
# ------------------------------------------------------------------------------
def _rewrite_parent_sk_expr(parent_expr_sql: str, mapping: dict[str, str]) -> str:
  """
  Rewrite a parent's SK DSL expression into a child FK expression.

  Robust version:
  - Detect left/right occurrences *by counting* how many times a BK column appears.
  - For each BK column:
      1st occurrence → left side, keep col("bk")
      2nd occurrence → right side, map to child Stage expression
  """

  # Count how many times each BK name was seen
  seen_counts = {}

  result = []
  idx = 0

  while True:
    m = COL_PATTERN.search(parent_expr_sql, idx)
    if not m:
      result.append(parent_expr_sql[idx:])
      break

    # text before match
    result.append(parent_expr_sql[idx:m.start()])

    colname = m.group(1)

    if colname not in mapping:
      # not a BK component → unchanged
      replacement = f'col("{colname}")'
    else:
      # count appearances
      count = seen_counts.get(colname, 0) + 1
      seen_counts[colname] = count

      if count == 1:
        # left side → keep BK name
        replacement = f'col("{colname}")'
      else:
        # right side → mapped Stage expression
        replacement = mapping[colname]

    result.append(replacement)
    idx = m.end()

  return "".join(result)


def _resolve_source_identity_id_for_raw(raw_ds):
  """
  Returns the identity name for a RAW dataset if it exists,
  otherwise None.
  """
  # RAW → SourceDataset via TargetDatasetInput
  inp = raw_ds.target_inputs.filter(source_dataset__isnull=False).first()
  if not inp:
    return None

  src = inp.source_dataset
  memberships = getattr(src, "dataset_groups", None)
  if not memberships:
    return None

  mem = memberships.first()
  return getattr(mem, "source_identity_id", None)


def build_surrogate_fk_expression(reference: "TargetDatasetReference") -> Expr:
  """
  Build the surrogate FK expression for a single TargetDatasetReference.

  The FK expression is derived from the parent's surrogate key expression by
  rewriting the occurrences of the parent's BK columns:

    - 1st occurrence of a BK column name: left side -> stays col("bk")
    - 2nd occurrence: right side -> becomes the child Stage expression

  This mirrors the logic in _build_fk_surrogate_expr_map() and ensures that
  FK and SK use the exact same hashing / concatenation DSL.
  """

  parent_ds = reference.referenced_dataset

  # 1) Parent SK column with surrogate_expression
  parent_sk = (
    parent_ds.target_columns
    .filter(surrogate_key_column=True, active=True)
    .order_by("ordinal_position", "id")
    .first()
  )
  if not parent_sk or not parent_sk.surrogate_expression:
    raise ValueError(
      f"Parent dataset '{parent_ds}' has no surrogate key expression."
    )

  parent_expr = parent_sk.surrogate_expression

  # 2) Build mapping: parent BK -> child Stage expression
  components = list(reference.key_components.all())
  if not components:
    raise ValueError(
      f"Reference '{reference}' has no key components defined."
    )

  mapping: dict[str, str] = {}
  for comp in components:
    # Parent BK column name as referenced in the parent SK expression
    parent_bk_name = comp.to_column.target_column_name

    # Child RawCore column -> get Stage-level expression feeding it
    child_stage_expr = _get_stage_expr_for_rawcore_col(comp.from_column)
    if not child_stage_expr:
      raise ValueError(
        f"Cannot resolve Stage expression for child column "
        f"'{comp.from_column.target_column_name}' in reference '{reference}'."
      )

    mapping[parent_bk_name] = child_stage_expr

  # 3) Rewrite parent SK expression into FK expression
  fk_expr_sql = _rewrite_parent_sk_expr(parent_expr, mapping)

  # Returned object is compatible with sync_child_fk_column(), which expects
  # an Expr-like object with a .sql attribute (RawSql) or something similar.
  return RawSql(sql=fk_expr_sql)

# ------------------------------------------------------------------------------
# Build FK surrogate expression map
# ------------------------------------------------------------------------------
def _build_fk_surrogate_expr_map(target_dataset: TargetDataset) -> dict[str, str]:
  """
  Return:
      { fk_column_name -> fk_expression_sql }

  A FK expression is generated only if:
    - parent SK exists with surrogate_expression
    - all BK components are fully mapped to child Stage expressions
    - FK column exists on child dataset
  """
  fk_map: dict[str, str] = {}

  refs = (
    target_dataset.outgoing_references
    .select_related("referenced_dataset")
    .prefetch_related(
      "key_components__from_column__input_links__upstream_target_column",
      "key_components__to_column",
    )
  )

  for ref in refs:
    parent_ds = ref.referenced_dataset

    base_fk_name = build_surrogate_key_name(parent_ds.target_dataset_name)
    fk_name = (
      f"{ref.reference_prefix}_{base_fk_name}"
      if ref.reference_prefix
      else base_fk_name
    )

    # FK column has to exist in child, otherwise ignore
    if not target_dataset.target_columns.filter(
      target_column_name=fk_name,
      active=True,
    ).exists():
      continue

    # Try to build the expression via central function
    try:
      fk_expr = build_surrogate_fk_expression(ref)
    except Exception:
      # If something is missing (SK, BK-Mapping, Stage-Lineage ...), then no FK
      continue

    fk_map[fk_name] = (
      fk_expr.sql if hasattr(fk_expr, "sql") else str(fk_expr)
    )

  return fk_map

def _resolve_source_identity_id_for_raw(upstream_dataset: "TargetDataset") -> Optional[str]:
  """
  Try to resolve the source_identity_id for a given RAW TargetDataset.

  Logic:
    - Find the SourceDataset feeding this RAW dataset via its input_links.
    - From that SourceDataset, look up its SourceDatasetGroupMembership.
    - Return membership.source_identity_id if present.
  """
  # RAW TargetDataset -> its input_links point to a SourceDataset
  src_input = (
    upstream_dataset.input_links
    .select_related("source_dataset")
    .filter(active=True, source_dataset__isnull=False)
    .order_by("id")
    .first()
  )

  if not src_input or not src_input.source_dataset:
    return None

  source_dataset = src_input.source_dataset

  # SourceDatasetGroupMembership has related_name="dataset_groups" on SourceDataset :contentReference[oaicite:0]{index=0}
  membership = (
    source_dataset.dataset_groups
    .order_by("-is_primary_system", "id")
    .first()
  )

  if not membership:
    return None

  return getattr(membership, "source_identity_id", None)

def _resolve_source_identity_id_for_raw(
  upstream_dataset: "TargetDataset",
) -> Optional[str]:
  """
  Try to resolve the source_identity_id for a given RAW TargetDataset.

  Logic:
    - RAW TargetDataset has input_links pointing to a SourceDataset.
    - From that SourceDataset, look up its SourceDatasetGroupMembership
      (related_name='dataset_groups').
    - Return membership.source_identity_id if present.
  """
  # RAW TargetDataset -> its input_links point to a SourceDataset
  link = (
    upstream_dataset.input_links
    .select_related("source_dataset", "source_dataset__source_system")
    .filter(active=True)
    .order_by("id")
    .first()
  )
  if not link or not link.source_dataset:
    return None

  source_dataset = link.source_dataset

  # SourceDatasetGroupMembership: related_name="dataset_groups" on SourceDataset
  membership = (
    source_dataset.dataset_groups
    .order_by("-is_primary_system", "id")
    .first()
  )
  if not membership:
    return None

  identity_id = getattr(membership, "source_identity_id", None)
  if not identity_id:
    return None

  return identity_id


def _resolve_source_identity_id_for_source(source_dataset) -> str | None:
  """
  Resolve the source_identity_id for a SourceDataset if present,
  otherwise return None.
  """
  memberships = getattr(source_dataset, "dataset_groups", None)
  if not memberships:
    return None

  membership = memberships.first()
  if not membership:
    return None

  return getattr(membership, "source_identity_id", None)


# ------------------------------------------------------------------------------
# Helper: Build a SELECT for a single upstream dataset (UNION path)
# ------------------------------------------------------------------------------
def _build_single_select_for_upstream(
  target_dataset: TargetDataset,
  upstream_dataset: TargetDataset,
) -> LogicalSelect:
  """
  Build a LogicalSelect from a single upstream TargetDataset.
  Used in UNION ALL scenarios (stage with multiple raw upstreams).

  For each TargetColumn of the target_dataset:

    - If the upstream_dataset (a RAW target dataset) has a target column
      with the same target_column_name, we assume this column is present
      and integrated in this branch -> use s."<that_name>".

    - If not, we render NULL for this column in this branch.

  A special case is the `source_identity_id` column, which is populated
  per branch from SourceDatasetGroupMembership (if configured).
  """
  source_table = SourceTable(
    schema=upstream_dataset.target_schema.schema_name,
    name=upstream_dataset.target_dataset_name,
    alias="s",
  )
  logical = LogicalSelect(from_=source_table)

  upstream_col_names = set(
    upstream_dataset.target_columns.values_list("target_column_name", flat=True)
  )

  # Per-branch source identity id (e.g. 'SAP', 'NAV_01', ...)
  identity_id = _resolve_source_identity_id_for_raw(upstream_dataset)

  tcols = (
    target_dataset.target_columns
    .filter(active=True)
    .order_by("ordinal_position", "id")
  )

  for col in tcols:
    if col.surrogate_key_column and col.surrogate_expression:
      expr: Expr = RawSql(sql=col.surrogate_expression)

    # Special handling for source_identity_id:
    # In UNION branches we emit a literal per upstream if configured.
    elif col.target_column_name == "source_identity_id" and identity_id is not None:
      # Simple string literal, dialect-agnostic enough for v0.5
      expr = RawSql(sql=f"'{identity_id}'")

    else:
      if col.target_column_name in upstream_col_names:
        expr = ColumnRef(table_alias="s", column_name=col.target_column_name)
      else:
        expr = RawSql("NULL")

    logical.select_list.append(
      SelectItem(expr=expr, alias=col.target_column_name)
    )

  return logical


# ------------------------------------------------------------------------------
# Helper: Build a SELECT for a single SourceDataset (UNION path)
# ------------------------------------------------------------------------------
def _build_single_select_for_source_stage(
  target_dataset: TargetDataset,
  source_dataset: SourceDataset,
) -> LogicalSelect:
  """
  Build a LogicalSelect from a single SourceDataset for a STAGE target.

  Used in multi-source STAGE scenarios where the stage dataset reads
  directly from multiple sources (without a RAW layer).

  For each TargetColumn in the stage dataset:

    - If the column is 'source_identity_id', we inject a literal value
      based on the SourceDatasetGroupMembership (e.g. 'aw1', 'aw2').
    - Otherwise, if a matching SourceColumn exists by name, we select
      that column from the source table using alias 's'.
    - If there is no matching SourceColumn, we emit NULL for that column
      in this branch.

  The SELECT alias always uses target_column_name so that the UNION
  branches line up structurally.
  """
  source_table = SourceTable(
    schema=source_dataset.schema_name,
    name=source_dataset.source_dataset_name,
    alias="s",
  )

  logical = LogicalSelect(from_=source_table, select_list=[])

  # Fast lookup of source columns by lowercased name
  src_cols_by_name = {
    sc.source_column_name.lower(): sc
    for sc in source_dataset.source_columns.filter(integrate=True)
  }

  # Resolve identity id (may be None)
  identity_id = _resolve_source_identity_id_for_source(source_dataset)

  for col in (
    target_dataset.target_columns
    .filter(active=True)
    .order_by("ordinal_position", "id")
  ):
    # Special handling for the artificial identity column
    if col.target_column_name == "source_identity_id":
      if identity_id is not None:
        expr = RawSql(sql=f"'{identity_id}'")
      else:
        expr = RawSql(sql="NULL")

    else:
      # Normal mapping: try to find a source column with the same name
      src_col = src_cols_by_name.get(col.target_column_name.lower())
      if src_col:
        expr = ColumnRef(
          table_alias="s",
          column_name=src_col.source_column_name,
        )
      else:
        # No matching source column → NULL for this branch
        expr = RawSql(sql="NULL")

    logical.select_list.append(
      SelectItem(expr=expr, alias=col.target_column_name)
    )

  return logical


# ------------------------------------------------------------------------------
# Main builder: Build logical select for a target dataset
# ------------------------------------------------------------------------------
def build_logical_select_for_target(target_dataset: TargetDataset):
  """
  Build a vendor-neutral logical representation (LogicalSelect or LogicalUnion)
  for the given TargetDataset, using dataset- and column-level lineage.
  """

  schema_short = target_dataset.target_schema.short_name

  inputs_qs = (
    target_dataset.input_links
    .select_related(
      "upstream_target_dataset",
      "upstream_target_dataset__target_schema",
      "source_dataset",
      "source_dataset__source_system",
    )
    .filter(active=True)
  )

  fk_expr_map = _build_fk_surrogate_expr_map(target_dataset)

  # ==========================================================================
  # 1) STAGE: MULTI-SOURCE (RAW or SOURCE) → UNION or RANKED UNION
  # ==========================================================================
  if schema_short == "stage":

    # -------- 1a) Multi-source via RAW -------------------------------------
    raw_inputs = [
      inp for inp in inputs_qs
      if inp.upstream_target_dataset
      and inp.upstream_target_dataset.target_schema.short_name == "raw"
    ]

    if len(raw_inputs) > 1:
      sub_selects = []
      identity_flags = []

      for inp in raw_inputs:
        raw_ds = inp.upstream_target_dataset
        sel = _build_single_select_for_upstream(target_dataset, raw_ds)
        sub_selects.append(sel)
        identity_flags.append(_resolve_source_identity_id_for_raw(raw_ds))

      # For now, both identity and non-identity modes use a simple UNION ALL.
      # Identity-mode still benefits from source_identity_id being part of
      # the natural key; conflict prioritization for no-identity mode will
      # be implemented once the logical plan and dialects support subqueries
      # and window functions generically.
      return LogicalUnion(selects=sub_selects, union_type="ALL")

    # -------- 1b) Multi-source via direct SourceDataset ---------------------
    source_inputs = [inp for inp in inputs_qs if inp.source_dataset is not None]

    if len(source_inputs) > 1:
      sub_selects = []
      identity_flags = []

      for inp in source_inputs:
        src = inp.source_dataset
        sel = _build_single_select_for_source_stage(target_dataset, src)
        sub_selects.append(sel)
        identity_flags.append(_resolve_source_identity_id_for_source(src))

      # Same here: use a plain UNION ALL for now.
      return LogicalUnion(selects=sub_selects, union_type="ALL")

    # -------------------- SINGLE-SOURCE STAGE (fall through) ----------------

  # ==========================================================================
  # 2) single-path FROM resolution (raw, stage, rawcore, fallback)
  # ==========================================================================
  from_schema = target_dataset.target_schema.schema_name
  from_table = target_dataset.target_dataset_name

  if schema_short == "raw":
    src_input = next(
      (inp for inp in inputs_qs if inp.source_dataset is not None),
      None,
    )
    if src_input:
      sd = src_input.source_dataset
      from_schema, from_table = sd.schema_name, sd.source_dataset_name
    else:
      up_input = next(
        (inp for inp in inputs_qs if inp.upstream_target_dataset is not None),
        None,
      )
      if up_input:
        up = up_input.upstream_target_dataset
        from_schema, from_table = up.target_schema.schema_name, up.target_dataset_name

  elif schema_short == "stage":
    raw_input = next(
      (
        inp for inp in inputs_qs
        if inp.upstream_target_dataset
        and inp.upstream_target_dataset.target_schema.short_name == "raw"
      ),
      None,
    )
    if raw_input:
      up = raw_input.upstream_target_dataset
      from_schema, from_table = up.target_schema.schema_name, up.target_dataset_name
    else:
      src_input = next(
        (inp for inp in inputs_qs if inp.source_dataset is not None),
        None,
      )
      if src_input:
        sd = src_input.source_dataset
        from_schema, from_table = sd.schema_name, sd.source_dataset_name

  elif schema_short == "rawcore":
    stage_input = next(
      (
        inp for inp in inputs_qs
        if inp.upstream_target_dataset
        and inp.upstream_target_dataset.target_schema.short_name == "stage"
      ),
      None,
    )
    if stage_input:
      up = stage_input.upstream_target_dataset
      from_schema, from_table = up.target_schema.schema_name, up.target_dataset_name

  # ==========================================================================
  # 3) Build the LogicalSelect for the single-path scenario
  # ==========================================================================
  source_table = SourceTable(schema=from_schema, name=from_table, alias="s")
  logical = LogicalSelect(from_=source_table)

  tcols = (
    target_dataset.target_columns
    .filter(active=True)
    .order_by("ordinal_position", "id")
  )

  for col in tcols:
    if col.surrogate_key_column and col.surrogate_expression:
      expr = RawSql(sql=col.surrogate_expression)

    elif col.target_column_name in fk_expr_map:
      expr = RawSql(sql=fk_expr_map[col.target_column_name])

    else:
      col_input = (
        col.input_links
        .select_related(
          "upstream_target_column",
          "source_column",
          "source_column__source_dataset",
        )
        .filter(active=True)
        .order_by("ordinal_position", "id")
        .first()
      )

      upstream_col_name = (
        col_input.upstream_target_column.target_column_name
        if col_input and col_input.upstream_target_column
        else col_input.source_column.source_column_name
        if col_input and col_input.source_column
        else col.target_column_name
      )

      expr = ColumnRef(table_alias="s", column_name=upstream_col_name)

    logical.select_list.append(SelectItem(expr=expr, alias=col.target_column_name))

  return logical
