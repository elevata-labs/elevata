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

from typing import List, Optional
import re

from metadata.models import (
  TargetDataset,
  TargetColumn,
)
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

    # Parent SK column
    parent_sk = (
      parent_ds.target_columns
      .filter(surrogate_key_column=True, active=True)
      .order_by("ordinal_position", "id")
      .first()
    )
    if not parent_sk or not parent_sk.surrogate_expression:
      continue

    parent_expr = parent_sk.surrogate_expression

    # Determine FK column name on child side
    base_fk_name = build_surrogate_key_name(parent_ds.target_dataset_name)
    fk_name = f"{ref.reference_prefix}_{base_fk_name}" if ref.reference_prefix else base_fk_name

    if not target_dataset.target_columns.filter(target_column_name=fk_name, active=True).exists():
      continue

    # Build mapping: parent BK -> child Stage expression
    components = list(ref.key_components.all())
    if not components:
      continue

    mapping: dict[str, str] = {}
    all_ok = True

    for comp in components:
      parent_bk_name = comp.to_column.target_column_name

      child_stage_expr = _get_stage_expr_for_rawcore_col(comp.from_column)
      if not child_stage_expr:
        all_ok = False
        break

      mapping[parent_bk_name] = child_stage_expr

    if not all_ok:
      continue

    # Rewrite expression
    fk_expr_sql = _rewrite_parent_sk_expr(parent_expr, mapping)
    fk_map[fk_name] = fk_expr_sql

  return fk_map


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

  The alias always uses target_dataset.target_column_name so the preview
  shows the final shape.
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

  tcols = (
    target_dataset.target_columns
    .filter(active=True)
    .order_by("ordinal_position", "id")
  )

  for col in tcols:
    if col.surrogate_key_column and col.surrogate_expression:
      expr: Expr = RawSql(sql=col.surrogate_expression)
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
# Main builder: Build logical select for a target dataset
# ------------------------------------------------------------------------------
def build_logical_select_for_target(target_dataset: TargetDataset):
  """
  Build a vendor-neutral logical representation (LogicalSelect or LogicalUnion)
  for the given TargetDataset, using dataset- and column-level lineage.

  Behavior:

    - raw:
        FROM the SourceDataset (if present as input),
        otherwise from an upstream TargetDataset (fallback),
        otherwise from the raw target itself.

    - stage:
        - if multiple upstream RAW TargetDatasets exist:
            build a UNION ALL between the individual raw SELECTs.
        - if exactly one raw-upstream:
            FROM that raw TargetDataset.
        - if no raw-upstream:
            FROM the first (optionally primary) SourceDataset,
            otherwise from the stage target itself.

    - rawcore:
        FROM upstream stage TargetDataset if present,
        otherwise from the rawcore target itself.

    - other schemas:
        FROM the target_dataset itself.

  Columns:
    - Surrogate-key columns with surrogate_expression → RawSql(...)
    - FK surrogate columns (derived from outgoing TargetDatasetReference)
      → RawSql(fk_expression_sql)
    - Otherwise:
        * Prefer upstream_target_column.target_column_name
        * Else source_column.source_column_name
        * Fallback: target_column_name

      The SELECT alias always uses target_column_name so that
      the preview shows the final target structure while the expression
      reveals the true upstream names.
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

  # Build FK expressions now so they can be reused in all paths
  fk_expr_map = _build_fk_surrogate_expr_map(target_dataset)

  # --------------------------------------------------------------------------
  # 1) Special case: STAGE with multiple RAW upstreams -> UNION ALL
  # --------------------------------------------------------------------------
  if schema_short == "stage":
    raw_inputs = [
      inp for inp in inputs_qs
      if inp.upstream_target_dataset
      and inp.upstream_target_dataset.target_schema.short_name == "raw"
    ]

    if len(raw_inputs) > 1:
      sub_selects: list[LogicalSelect] = []
      for inp in raw_inputs:
        raw_ds = inp.upstream_target_dataset
        sub_sel = _build_single_select_for_upstream(target_dataset, raw_ds)
        sub_selects.append(sub_sel)

      return LogicalUnion(selects=sub_selects, union_type="ALL")

  # --------------------------------------------------------------------------
  # 2) Determine FROM source_table for single-path scenarios
  # --------------------------------------------------------------------------
  from_schema = target_dataset.target_schema.schema_name
  from_table = target_dataset.target_dataset_name

  if schema_short == "raw":
    # Prefer direct SourceDataset input
    src_input = next(
      (inp for inp in inputs_qs if inp.source_dataset is not None),
      None,
    )
    if src_input and src_input.source_dataset:
      sd = src_input.source_dataset
      from_schema = sd.schema_name
      from_table = sd.source_dataset_name
    else:
      # Fallback: upstream TargetDataset if present
      up_input = next(
        (inp for inp in inputs_qs if inp.upstream_target_dataset is not None),
        None,
      )
      if up_input and up_input.upstream_target_dataset:
        up = up_input.upstream_target_dataset
        from_schema = up.target_schema.schema_name
        from_table = up.target_dataset_name

  elif schema_short == "stage":
    # Here we are only in the single-raw or source-input case (multi-raw already handled)
    raw_input = next(
      (
        inp for inp in inputs_qs
        if inp.upstream_target_dataset
        and inp.upstream_target_dataset.target_schema.short_name == "raw"
      ),
      None,
    )
    if raw_input and raw_input.upstream_target_dataset:
      up = raw_input.upstream_target_dataset
      from_schema = up.target_schema.schema_name
      from_table = up.target_dataset_name
    else:
      src_input = next(
        (inp for inp in inputs_qs if inp.source_dataset is not None),
        None,
      )
      if src_input and src_input.source_dataset:
        sd = src_input.source_dataset
        from_schema = sd.schema_name
        from_table = sd.source_dataset_name

  elif schema_short == "rawcore":
    stage_input = next(
      (
        inp for inp in inputs_qs
        if inp.upstream_target_dataset
        and inp.upstream_target_dataset.target_schema.short_name == "stage"
      ),
      None,
    )
    if stage_input and stage_input.upstream_target_dataset:
      up = stage_input.upstream_target_dataset
      from_schema = up.target_schema.schema_name
      from_table = up.target_dataset_name

  # Other schemas: keep default (target_dataset itself)

  source_table = SourceTable(
    schema=from_schema,
    name=from_table,
    alias="s",
  )
  logical = LogicalSelect(from_=source_table)

  # --------------------------------------------------------------------------
  # 3) SELECT list based on TargetColumns and column-level lineage
  # --------------------------------------------------------------------------
  tcols = (
    target_dataset.target_columns
    .filter(active=True)
    .order_by("ordinal_position", "id")
  )

  for col in tcols:
    # Surrogate key column
    if col.surrogate_key_column and col.surrogate_expression:
      expr: Expr = RawSql(sql=col.surrogate_expression)

    # Surrogate FK column (hash based on parent SK)
    elif col.target_column_name in fk_expr_map:
      expr = RawSql(sql=fk_expr_map[col.target_column_name])

    # Normal lineage
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

      upstream_col_name: Optional[str] = None

      if col_input and col_input.upstream_target_column:
        upstream_col_name = col_input.upstream_target_column.target_column_name
      elif col_input and col_input.source_column:
        upstream_col_name = col_input.source_column.source_column_name

      if upstream_col_name:
        expr = ColumnRef(table_alias="s", column_name=upstream_col_name)
      else:
        # Fallback to the target column name
        expr = ColumnRef(table_alias="s", column_name=col.target_column_name)

    logical.select_list.append(
      SelectItem(expr=expr, alias=col.target_column_name)
    )

  return logical
