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

"""
Load SQL rendering for TargetDatasets.

This module is separate from the SELECT preview rendering and focuses on:
- full refresh loads
- incremental loads (append, merge, snapshot)

For v0.4.0 we implement:

- full refresh loads (INSERT-based)
- merge-based incrementals with optional delete detection

Other modes (append, snapshot) still return descriptive SQL comments
as placeholders.
"""

from typing import Sequence
import re

from metadata.models import TargetDataset, TargetColumn
from metadata.rendering.load_planner import build_load_plan
from metadata.rendering.renderer import render_select_for_target
from metadata.rendering.builder import build_logical_select_for_target
from metadata.rendering.logical_plan import LogicalSelect


def _get_target_columns_in_order(td: TargetDataset) -> Sequence[TargetColumn]:
  """
  Return all target columns of this dataset in a stable order,
  typically by ordinal_position then by name as a fallback.
  """
  return (
    td.target_columns
    .order_by("ordinal_position", "target_column_name")
  )


def _get_rendered_column_exprs_for_target(td: TargetDataset, dialect) -> dict[str, str]:
  """
  Helper for merge: use the same logical SELECT as preview/full load
  to obtain the expression for each target column, rendered with the
  given dialect.

  Returns a mapping: target_column_name -> SQL expression string.
  """
  plan = build_logical_select_for_target(td)

  # For rawcore we expect a single LogicalSelect (no UNION).
  if not isinstance(plan, LogicalSelect):
    # Fallback: not supported yet -> let caller decide how to proceed.
    return {}

  expr_map: dict[str, str] = {}

  for item in plan.select_list:
    # item.alias is the final target column name
    expr_sql = dialect.render_expr(item.expr)
    expr_map[item.alias] = expr_sql

  return expr_map


def _build_incremental_scope_filter_for_target(td: TargetDataset) -> str | None:
  """
  Build a WHERE-clause fragment for delete detection on the target dataset,
  based on the source dataset's incremental_filter.

  Logic:
    - Always take the incremental_filter from td.incremental_source (SourceDataset).
    - The filter is written in terms of source column names.
    - For a rawcore target, we have a lineage chain like:

        SourceColumn -> Raw TargetColumn -> Stage TargetColumn -> Rawcore TargetColumn

      or shorter (without raw), depending on your layer setup.

    - We walk upstream_target_column recursively for each rawcore column and
      collect all SourceColumns coming from this incremental_source. Every
      such SourceColumn name is mapped to the *rawcore* target_column_name.

  # NOTE: This implementation uses full lineage (SourceColumn via TargetColumnInput).
  # It could be simplified in the future to rely on stage.source_field_name,
  # but for now we keep the lineage-based variant because it is more explicit.
  """
  src = getattr(td, "incremental_source", None)
  if not src or not getattr(src, "incremental", False) or not src.increment_filter:
    return None

  # Mapping: source_column_name.lower() -> rawcore target_column_name
  mapping: dict[str, str] = {}

  def _collect_source_to_rawcore(rc_col, current_col, src_ds, mapping, visited):
    """
    Recursively walk upstream_target_column starting from current_col,
    and record any SourceColumn from src_ds as mapping:
      source_col_name.lower() -> rc_col.target_column_name
    """
    if current_col.pk in visited:
      return
    visited.add(current_col.pk)

    # Look at all input_links of the current column
    for link in current_col.input_links.select_related("source_column", "upstream_target_column").all():
      sc = link.source_column
      if sc and sc.source_dataset_id == src_ds.id and sc.source_column_name:
        mapping[sc.source_column_name.lower()] = rc_col.target_column_name

      upstream = link.upstream_target_column
      if upstream:
        _collect_source_to_rawcore(rc_col, upstream, src_ds, mapping, visited)

  # For each rawcore target column, collect all source mappings along its lineage
  for rc_col in td.target_columns.all():
    visited: set[int] = set()
    _collect_source_to_rawcore(rc_col, rc_col, src, mapping, visited)

  if not mapping:
    # No way to map source-level filter onto rawcore columns
    return None

  expr = src.increment_filter

  # Rewrite identifiers that match source column names to rawcore column names.
  def replace_identifier(match: re.Match) -> str:
    token = match.group(0)
    repl = mapping.get(token.lower())
    return repl if repl is not None else token

  expr = re.sub(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", replace_identifier, expr)

  # Keep {{DELTA_CUTOFF}} as placeholder; just normalize whitespace
  return " ".join(expr.split())


def _get_surrogate_expressions_for_target(td: TargetDataset, dialect) -> dict[str, str]:
  """
  Return a mapping of target_column_name -> rendered surrogate key expression
  for all surrogate_key_columns of this target dataset.

  The expressions are taken from the same logical SELECT that is used for
  preview / full load, so the semantics stay consistent.
  """
  # Find surrogate-key columns (by target name)
  sk_names = set(
    td.target_columns
    .filter(surrogate_key_column=True, surrogate_expression__isnull=False)
    .values_list("target_column_name", flat=True)
  )
  if not sk_names:
    return {}

  plan = build_logical_select_for_target(td)
  if not isinstance(plan, LogicalSelect):
    # For rawcore we expect a single SELECT, no UNION
    # (if this ever changes, we should revisit this helper).
    raise TypeError(
      f"Expected LogicalSelect for {td.target_dataset_name}, "
      f"got {type(plan).__name__}."
    )

  mapping: dict[str, str] = {}
  for item in plan.select_list:
    if item.alias in sk_names:
      mapping[item.alias] = dialect.render_expr(item.expr)

  return mapping


def render_delete_missing_rows_sql(td: TargetDataset, dialect) -> str | None:
  """
  Render a DELETE statement that removes rows from the rawcore target table
  which are no longer present in the latest stage snapshot.

  Semantics:
    - only active for merge-incremental rawcore datasets with handle_deletes=True
    - uses natural_key_fields to match rows
    - uses the incremental_filter (rewritten to target column names) to scope
      delete detection to the same incremental window
  """
  if not dialect.supports_delete_detection:
    raise NotImplementedError(
      f"Dialect {dialect.__class__.__name__} does not implement delete detection. "
      f"Disable handle_deletes for {td.target_dataset_name} or use a dialect with "
      f"supports_delete_detection=True."
    )

  plan = build_load_plan(td)
  if plan.mode != "merge" or not plan.handle_deletes:
    return None

  if td.target_schema.short_name != "rawcore":
    # For now, delete detection is only defined for rawcore targets
    return (
      f"-- handle_deletes=True for {td.target_dataset_name}, "
      f"but delete detection is only implemented for rawcore.\n"
      f"-- No delete detection SQL generated.\n"
    )

  scope_filter = _build_incremental_scope_filter_for_target(td)
  if not scope_filter:
    return (
      f"-- handle_deletes=True for {td.target_dataset_name}, "
      f"but no usable incremental_filter could be derived.\n"
      f"-- No delete detection SQL generated.\n"
    )

  src = getattr(td, "incremental_source", None)
  if not src:
    return (
      f"-- handle_deletes=True for {td.target_dataset_name}, "
      f"but incremental_source is not set.\n"
      f"-- No delete detection SQL generated.\n"
    )

  key_cols = td.natural_key_fields
  if not key_cols:
    return (
      f"-- handle_deletes=True for {td.target_dataset_name}, "
      f"but natural_key_fields are not defined.\n"
      f"-- No delete detection SQL generated.\n"
    )

  # Resolve rawcore target table
  target_schema_name = td.target_schema.schema_name
  target_table_name = td.target_dataset_name

  # Resolve stage upstream as the snapshot used for comparison
  stage_td = _find_stage_upstream_for_rawcore(td)
  if not stage_td:
    return (
      f"-- handle_deletes=True for {td.target_dataset_name}, "
      f"but no upstream stage dataset could be resolved.\n"
      f"-- No delete detection SQL generated.\n"
    )

  stage_schema_name = stage_td.target_schema.schema_name
  stage_table_name = stage_td.target_dataset_name

  # Expressions per target column name from the logical SELECT
  expr_map = _get_rendered_column_exprs_for_target(td, dialect)

  target_alias = "t"
  source_alias = "s"
  q = dialect.quote_ident

  join_predicates: list[str] = []
  for col_name in key_cols:
    # Right-hand side: expression as defined for the *target* column when
    # reading from stage (usually s."<stage_col_name>", but inc. manual_expr and CASTs)
    rhs_sql = expr_map.get(col_name)
    if not rhs_sql:
      # Fallback: best effort; should be rare
      rhs_sql = f'{source_alias}.{q(col_name)}'

    join_predicates.append(
      f'{target_alias}.{q(col_name)} = {rhs_sql}'
    )

  sql = dialect.render_delete_detection_statement(
    target_schema=target_schema_name,
    target_table=target_table_name,
    stage_schema=stage_schema_name,
    stage_table=stage_table_name,
    join_predicates=join_predicates,
    scope_filter=scope_filter,
  )
  return sql


def _find_stage_upstream_for_rawcore(td: TargetDataset):
  """
  Find the upstream stage TargetDataset for a rawcore target.

  Rawcore is always expected to take its data from a stage dataset.
  We mirror the logic from the SQL builder: look at input_links and
  pick the first upstream_target_dataset with target_schema.short_name == 'stage'.
  """
  if td.target_schema.short_name != "rawcore":
    return None

  inputs_qs = td.input_links.select_related(
    "upstream_target_dataset",
    "upstream_target_dataset__target_schema",
  )

  for inp in inputs_qs:
    utd = inp.upstream_target_dataset
    if utd and utd.target_schema.short_name == "stage":
      return utd

  return None


def render_merge_sql(td: TargetDataset, dialect) -> str:
  """
  Render a backend-aware MERGE statement for a target dataset.

  For v0.4.0 the intended semantics are:

    - rawcore:
      MERGE rawcore (integrated table) FROM stage (snapshot of the latest extract)

    - other schemas:
      merge is currently not supported and will raise an error

  Assumptions:
    - td.incremental_strategy == 'merge'
    - effective materialization type is 'table'
    - natural_key_fields define the business key in both stage and rawcore

  """
  if not dialect.supports_merge:
    raise NotImplementedError(
      f"Dialect {dialect.__class__.__name__} does not support MERGE-based loads. "
      f"Use incremental_strategy='full' for {td.target_dataset_name}."
    )

  plan = build_load_plan(td)
  if plan.mode != "merge":
    raise ValueError(f"render_merge_sql called for non-merge dataset {td.id}")

  if td.target_schema.short_name != "rawcore":
    raise ValueError(
      f"render_merge_sql is currently only supported for rawcore targets "
      f"(got schema={td.target_schema.short_name} for dataset {td.id})."
    )

  # Resolve integrated target table (rawcore)
  target_schema_name = td.target_schema.schema_name
  target_table_name = td.target_dataset_name
  target_full = dialect.quote_table(target_schema_name, target_table_name)
  target_alias = "t"

  # Resolve stage upstream as the merge source
  stage_td = _find_stage_upstream_for_rawcore(td)
  if not stage_td:
    raise ValueError(
      f"rawcore target {td.id} has merge strategy but no upstream stage dataset "
      f"could be resolved from TargetDatasetInput."
    )

  source_schema_name = stage_td.target_schema.schema_name
  source_table_name = stage_td.target_dataset_name
  source_full = dialect.quote_table(source_schema_name, source_table_name)
  source_alias = "s"

  q = dialect.quote_ident

  # Business key columns shared between stage and rawcore
  key_cols = td.natural_key_fields
  if not key_cols:
    raise ValueError(
      f"TargetDataset {td.id} has merge strategy but no natural_key_fields defined."
    )

  target_cols = list(_get_target_columns_in_order(td))
  non_key_cols = [c for c in target_cols if c.target_column_name not in key_cols]

  q = dialect.quote_ident

  # Expressions per target column from the logical SELECT
  expr_map = _get_rendered_column_exprs_for_target(td, dialect)

  # ON predicate: t.pk = <expr_for_pk_from_stage>
  on_clauses: list[str] = []
  for col in key_cols:
    # right side: same expression which is used in full select too
    rhs_sql = expr_map.get(col)
    if not rhs_sql:
      # Fallback: classic s."col", if something is missing
      rhs_sql = f'{source_alias}.{q(col)}'

    on_clauses.append(
      f'{target_alias}.{q(col)} = {rhs_sql}'
    )

  on_expr = " AND ".join(on_clauses)

  # UPDATE SET col = <expr_for_col> for all non-key columns
  update_assignments: list[str] = []
  for c in non_key_cols:
    col_name = c.target_column_name
    value_sql = expr_map.get(col_name)

    if not value_sql:
      # Fallback: classic s."col" reference, if something is unexpectedly missing
      value_sql = f"{source_alias}.{q(col_name)}"

    update_assignments.append(
      f"{q(col_name)} = {value_sql}"
    )

  update_clause = ",\n      ".join(update_assignments)

  # INSERT (cols) VALUES (<expr_for_col>, ...)
  insert_columns = [c.target_column_name for c in target_cols]
  insert_cols_sql = ", ".join(q(c) for c in insert_columns)

  insert_values: list[str] = []
  for col_name in insert_columns:
    value_sql = expr_map.get(col_name)
    if not value_sql:
      value_sql = f"{source_alias}.{q(col_name)}"
    insert_values.append(value_sql)

  insert_vals_sql = ", ".join(insert_values)

  sql_parts: list[str] = []
  sql_parts.append(
    f"MERGE INTO {target_full} AS {target_alias}"
  )
  sql_parts.append(
    f"USING {source_full} AS {source_alias}"
  )
  sql_parts.append(f"ON {on_expr}")
  sql_parts.append("")  # blank line

  # Standard update branch
  sql_parts.append(
    "WHEN MATCHED THEN\n"
    f"  UPDATE SET\n"
    f"      {update_clause}"
  )

  # Insert branch
  sql_parts.append(
    "WHEN NOT MATCHED THEN\n"
    f"  INSERT ({insert_cols_sql})\n"
    f"  VALUES ({insert_vals_sql});"
  )

  return "\n".join(sql_parts)


def render_full_refresh_sql(td: TargetDataset, dialect) -> str:
  """
  Full refresh load (INSERT-based) for a target dataset.

  Uses the same core SELECT as the SQL preview (logical plan + renderer)
  and wraps it in a dialect-specific INSERT INTO statement.

  For now we assume the target table already exists and do not emit
  TRUNCATE/CREATE statements.
  """
  select_sql = render_select_for_target(td, dialect)

  schema_name = td.target_schema.schema_name
  table_name = td.target_dataset_name

  return dialect.render_insert_into_table(schema_name, table_name, select_sql)


def render_append_sql(td: TargetDataset, dialect) -> str:
  raise NotImplementedError(
    f"Append-only load for {td.target_dataset_name} is not implemented yet. "
    f"Use incremental_strategy='full' or 'merge' instead."
  )


def render_snapshot_sql(td: TargetDataset, dialect) -> str:
  raise NotImplementedError(
    f"Snapshot load for {td.target_dataset_name} is not implemented yet. "
    f"Use incremental_strategy='full' or 'merge' instead."
  )


def render_load_sql_for_target(td: TargetDataset, dialect) -> str:
  """
  High-level entry point for load SQL generation.

  Uses the LoadPlan to decide which concrete renderer to call.
  """
  plan = build_load_plan(td)

  if plan.mode == "full":
    return render_full_refresh_sql(td, dialect)

  if plan.mode == "append":
    return render_append_sql(td, dialect)

  if plan.mode == "merge":
    delete_sql = render_delete_missing_rows_sql(td, dialect)
    merge_sql = render_merge_sql(td, dialect)
    if delete_sql:
      return f"{delete_sql}\n\n{merge_sql}"
    return merge_sql

  if plan.mode == "snapshot":
    return render_snapshot_sql(td, dialect)

  # Defensive fallback
  return (
    f"-- Unsupported load mode '{plan.mode}' for {td.target_dataset_name}.\n"
    f"-- Please check incremental_strategy and materialization_type."
  )

