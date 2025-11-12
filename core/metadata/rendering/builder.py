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

from metadata.models import TargetDataset, SourceColumn
from metadata.rendering.logical_plan import LogicalSelect, LogicalUnion, SourceTable, SelectItem
from metadata.rendering.expr import Expr, ColumnRef, RawSql


def _looks_like_dsl(expr: str) -> bool:
  """
  Heuristic: treat expressions containing {{ ... }} as elevata DSL,
  not as native SQL.
  """
  if expr is None:
    return False
  return "{{" in expr and "}}" in expr


def build_logical_select_for_target(target_dataset: TargetDataset):
  """
  Build a vendor-neutral logical representation (LogicalSelect or LogicalUnion)
  for the given TargetDataset, using dataset- and column-level lineage.

  Behavior:

    - raw:
        FROM the target_dataset itself.

    - stage:
        - if multiple upstream RAW TargetDatasets exist:
            build a UNION ALL between the individual raw SELECTs.
        - if exactly one raw-upstream:
            FROM that raw TargetDataset.
        - if no raw-upstream:
            FROM the first (optionally primary) SourceDataset.

    - rawcore:
        FROM upstream stage TargetDataset.

    - other schemas:
        FROM the target_dataset itself.

  Columns:
    - Surrogate-key columns with surrogate_expression → RawSql(...)
      (expression placeholders like {expr:...} are expanded by the dialect).

    - Otherwise:
        * Prefer upstream_target_column.target_column_name
        * Else source_column.source_column_name
        * Fallback: target_column_name

      The SELECT alias always uses target_column_name so that
      the preview shows the final target structure while the expression
      reveals the true upstream names.
  """

  schema_short = target_dataset.target_schema.short_name

  inputs_qs = target_dataset.input_links.select_related(
    "upstream_target_dataset",
    "upstream_target_dataset__target_schema",
    "source_dataset",
    "source_dataset__source_system",
  )

  # ----------------------------------------------------------------
  # 1) Special case: STAGE with multiple RAW upstreams -> UNION ALL
  # ----------------------------------------------------------------
  if schema_short == "stage":
    raw_inputs = [
      inp for inp in inputs_qs
      if inp.upstream_target_dataset
      and inp.upstream_target_dataset.target_schema.short_name == "raw"
    ]

    if len(raw_inputs) > 1:
      # Build one LogicalSelect per upstream RAW TargetDataset
      # and join them via UNION ALL.
      sub_selects = []
      for inp in raw_inputs:
        raw_ds = inp.upstream_target_dataset
        sub_sel = _build_single_select_for_upstream(target_dataset, raw_ds)
        sub_selects.append(sub_sel)

      return LogicalUnion(selects=sub_selects, union_type="ALL")

  # ----------------------------------------------------------------
  # 2) Normal FROM resolution (single upstream)
  # ----------------------------------------------------------------
  from_schema = None
  from_table = None

  upstream_td = None
  upstream_src = None

  if schema_short == "rawcore":
    # rawcore: expect stage as upstream
    for inp in inputs_qs:
      utd = inp.upstream_target_dataset
      if utd and utd.target_schema.short_name == "stage":
        upstream_td = utd
        break

  elif schema_short == "stage":
    # stage: prefer upstream raw (primary if available), else first source dataset
    raw_candidates = []
    for inp in inputs_qs:
      utd = inp.upstream_target_dataset
      if utd and utd.target_schema.short_name == "raw":
        raw_candidates.append(inp)

    if raw_candidates:
      # prefer role="primary" if present
      primary = next((inp for inp in raw_candidates if getattr(inp, "role", None) == "primary"), None)
      chosen = primary or raw_candidates[0]
      upstream_td = chosen.upstream_target_dataset
    else:
      # no raw upstream: fall back to a source dataset
      src_candidates = [inp for inp in inputs_qs if inp.source_dataset is not None]
      if src_candidates:
        primary_src = next((inp for inp in src_candidates if getattr(inp, "role", None) == "primary"), None)
        chosen_src = primary_src or src_candidates[0]
        upstream_src = chosen_src.source_dataset

  # raw and other schemas: no special logic (use self as fallback)
  if upstream_td is not None:
    from_schema = upstream_td.target_schema.schema_name
    from_table = upstream_td.target_dataset_name
  elif upstream_src is not None:
    # heuristic for SourceDatasets: prefer schema_name, else source_system.target_short_name
    if upstream_src.schema_name:
      from_schema = upstream_src.schema_name
    else:
      from_schema = upstream_src.source_system.target_short_name
    from_table = upstream_src.source_dataset_name
  else:
    # fallback: use the target dataset itself
    from_schema = target_dataset.target_schema.schema_name
    from_table = target_dataset.target_dataset_name

  source_table = SourceTable(
    name=from_table,
    schema=from_schema,
    alias="s",
  )

  logical = LogicalSelect(from_=source_table)

  # ----------------------------------------------------------------
  # 3) SELECT list based on TargetColumns and column-level lineage
  # ----------------------------------------------------------------
  tcols = (
    target_dataset.target_columns
    .all()
    .order_by("ordinal_position", "id")
  )

  for col in tcols:
    # Surrogate key: use the stored RawSql expression
    if col.surrogate_key_column and col.surrogate_expression:
      expr: Expr = RawSql(sql=col.surrogate_expression)
    else:
      # Resolve column lineage
      col_input = (
        col.input_links
        .select_related("upstream_target_column", "source_column", "source_column__source_dataset")
        .filter(active=True)
        .order_by("ordinal_position", "id")
        .first()
      )

      upstream_col_name = None

      if col_input and col_input.upstream_target_column:
        upstream_col_name = col_input.upstream_target_column.target_column_name
      elif col_input and col_input.source_column:
        upstream_col_name = col_input.source_column.source_column_name

      if upstream_col_name:
        expr = ColumnRef(table_alias="s", column_name=upstream_col_name)
      else:
        # fallback to target column name
        expr = ColumnRef(table_alias="s", column_name=col.target_column_name)

    logical.select_list.append(
      SelectItem(
        expr=expr,
        alias=col.target_column_name,
      )
    )

  return logical

def _build_single_select_for_upstream(target_dataset, upstream_dataset):
  """
  Helper: build a LogicalSelect for one upstream TargetDataset (used in UNION ALL mode).

  For each TargetColumn of the target_dataset:

    - If the upstream_dataset (a RAW target dataset) has a target column
      with the same target_column_name, we assume this column is present
      and integrated in this branch -> use s."<that_name>".

    - If not, we render NULL for this column in this branch.

  This uses the fact that RAW target datasets are generated only from
  integrated SourceColumns, so their column set already encodes which
  fields participate in the consolidation.

  The alias always uses target_dataset.target_column_name so the preview
  shows the final shape.
  """

  source_table = SourceTable(
    name=upstream_dataset.target_dataset_name,
    schema=upstream_dataset.target_schema.schema_name,
    alias="s",
  )
  logical = LogicalSelect(from_=source_table)

  # Collect the column names that actually exist on this upstream dataset
  upstream_col_names = set(
    upstream_dataset.target_columns.values_list("target_column_name", flat=True)
  )

  tcols = (
    target_dataset.target_columns
    .all()
    .order_by("ordinal_position", "id")
  )

  for col in tcols:
    # Surrogate key: same behavior as in main builder (usually not used on stage)
    if col.surrogate_key_column and col.surrogate_expression:
      expr: Expr = RawSql(sql=col.surrogate_expression)
    else:
      if col.target_column_name in upstream_col_names:
        # Column exists in this raw dataset -> use it
        expr = ColumnRef(table_alias="s", column_name=col.target_column_name)
      else:
        # Column does not exist in this raw dataset -> NULL for this union branch
        expr = RawSql("NULL")

    logical.select_list.append(
      SelectItem(
        expr=expr,
        alias=col.target_column_name,
      )
    )

  return logical
