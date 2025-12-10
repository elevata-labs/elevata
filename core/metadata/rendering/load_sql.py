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

For now we implement:

- full refresh loads (INSERT-based)
- merge-based incrementals with optional delete detection

Other modes (append, snapshot) still return descriptive SQL comments
as placeholders.
"""

from typing import Sequence, Any, Dict
import re

from metadata.models import TargetDataset, TargetColumn, TargetColumnInput
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


def get_rawcore_name_from_hist(hist_name: str) -> str:
  if hist_name and hist_name.endswith("_hist"):
    return hist_name[:-5]
  return hist_name


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
  q = dialect.render_identifier

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
  Render a backend-aware MERGE / upsert statement for a target dataset.

  Semantics:

  - rawcore:
      Merge the integrated rawcore table from its upstream stage snapshot,
      matching on the natural_key_fields (business key).

  - other schemas:
      merge is currently not supported and will raise a ValueError.

  Assumptions:
    - td.incremental_strategy == 'merge'
    - effective materialization type is 'table'
    - natural_key_fields define the business key in both stage and rawcore
  """
  # Safety guard: this helper must only be used for merge datasets
  # when an explicit incremental_strategy is configured.
  incremental_strategy = getattr(td, "incremental_strategy", None)
  if incremental_strategy is not None and incremental_strategy != "merge":
    raise ValueError(
      f"render_merge_sql called for non-merge dataset {getattr(td, 'id', '?')}"
    )

  # Only rawcore targets are currently supported for merge
  if td.target_schema.short_name != "rawcore":
    raise ValueError(
      f"Merge loads are only supported for rawcore targets, "
      f"got schema={td.target_schema.short_name!r} for {td.target_dataset_name}."
    )

  # Find upstream stage dataset
  stage_td = _find_stage_upstream_for_rawcore(td)
  if not stage_td:
    raise ValueError(
      f"Could not resolve upstream stage dataset for rawcore target {td.target_dataset_name}."
    )

  # Resolve fully-qualified target and source table names
  target_schema_name = td.target_schema.schema_name
  target_table_name = td.target_dataset_name
  target_full = dialect.render_table_identifier(target_schema_name, target_table_name)
  target_alias = "t"

  source_schema_name = stage_td.target_schema.schema_name
  source_table_name = stage_td.target_dataset_name
  source_full = dialect.render_table_identifier(source_schema_name, source_table_name)
  source_alias = "s"

  q = dialect.render_identifier

  # Business key columns shared between stage and rawcore
  key_cols = td.natural_key_fields
  if not key_cols:
    raise ValueError(
      f"TargetDataset {td.id} has merge strategy but no natural_key_fields defined."
    )

  # All target columns in stable order
  target_cols = list(_get_target_columns_in_order(td))
  non_key_cols = [c for c in target_cols if c.target_column_name not in key_cols]

  # Expressions per target column from the logical SELECT
  expr_map = _get_rendered_column_exprs_for_target(td, dialect)

  # If the dialect explicitly opts out of native MERGE support, use the
  # UPDATE + INSERT fallback strategy.
  supports_merge = getattr(dialect, "supports_merge", True)
  if not supports_merge:
    return _render_update_then_insert_sql(
      td=td,
      dialect=dialect,
      source_full=source_full,
      source_alias=source_alias,
      target_full=target_full,
      target_alias=target_alias,
      key_cols=key_cols,
      expr_map=expr_map,
      target_cols=target_cols,
    )

  # ON predicate: t.pk = <expr_for_pk_from_stage>
  on_clauses: list[str] = []
  for col in key_cols:
    rhs_sql = expr_map.get(col)
    if not rhs_sql:
      # Fallback: simple s."col" if the expression map does not contain it
      rhs_sql = f"{source_alias}.{q(col)}"
    on_clauses.append(
      f"{target_alias}.{q(col)} = {rhs_sql}"
    )
  on_clause = " AND\n      ".join(on_clauses)

  # UPDATE SET col = <expr_for_col_from_stage>
  update_assignments: list[str] = []
  for c in non_key_cols:
    col_name = c.target_column_name
    value_sql = expr_map.get(col_name)
    if not value_sql:
      # Fallback: classic s."col" reference, if something is unexpectedly missing
      value_sql = f"{source_alias}.{q(col_name)}"
    update_assignments.append(f"{q(col_name)} = {value_sql}")
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

  # Final MERGE statement; we emit a generic MERGE INTO ... USING ... ON ...
  sql_parts: list[str] = []

  sql_parts.append(
    f"MERGE INTO {target_full} AS {target_alias}\n"
    f"USING {source_full} AS {source_alias}\n"
    f"ON {on_clause}\n"
  )

  # Update branch
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


def _render_update_then_insert_sql(
  td: TargetDataset,
  dialect,
  source_full: str,
  source_alias: str,
  target_full: str,
  target_alias: str,
  key_cols: list[str],
  expr_map: dict[str, str],
  target_cols: Sequence[TargetColumn],
) -> str:
  """
  Fallback implementation for dialects that do not support native MERGE.

  Strategy:
    1) UPDATE target t
       SET non-key columns = expressions from source
       FROM source s
       WHERE business-key join

    2) INSERT INTO target (...)
       SELECT expressions FROM source s
       WHERE NOT EXISTS (SELECT 1 FROM target t WHERE business-key join)
  """
  q = dialect.render_identifier

  # Build join predicate based on business key
  join_predicates: list[str] = []
  for col in key_cols:
    rhs_sql = expr_map.get(col) or f"{source_alias}.{q(col)}"
    join_predicates.append(f"{target_alias}.{q(col)} = {rhs_sql}")
  on_expr = " AND ".join(join_predicates)

  # UPDATE branch: only non-key columns are updated
  non_key_cols = [c for c in target_cols if c.target_column_name not in key_cols]
  update_assignments: list[str] = []
  for c in non_key_cols:
    col_name = c.target_column_name
    value_sql = expr_map.get(col_name) or f"{source_alias}.{q(col_name)}"
    update_assignments.append(f"{q(col_name)} = {value_sql}")
  update_clause = ", ".join(update_assignments)

  update_sql = (
    f"UPDATE {target_full} AS {target_alias}\n"
    f"SET {update_clause}\n"
    f"FROM {source_full} AS {source_alias}\n"
    f"WHERE {on_expr};"
  )

  # INSERT branch: insert rows that do not yet exist in the target
  insert_columns = [c.target_column_name for c in target_cols]
  insert_cols_sql = ", ".join(q(c) for c in insert_columns)

  select_values: list[str] = []
  for col_name in insert_columns:
    value_sql = expr_map.get(col_name) or f"{source_alias}.{q(col_name)}"
    select_values.append(value_sql)
  select_values_sql = ", ".join(select_values)

  not_exists_predicates: list[str] = []
  for col in key_cols:
    rhs_sql = expr_map.get(col) or f"{source_alias}.{q(col)}"
    not_exists_predicates.append(f"{target_alias}.{q(col)} = {rhs_sql}")
  not_exists_join = " AND ".join(not_exists_predicates)

  insert_sql = (
    f"INSERT INTO {target_full} ({insert_cols_sql})\n"
    f"SELECT {select_values_sql}\n"
    f"FROM {source_full} AS {source_alias}\n"
    f"WHERE NOT EXISTS (\n"
    f"  SELECT 1 FROM {target_full} AS {target_alias}\n"
    f"  WHERE {not_exists_join}\n"
    f");"
  )

  return update_sql + "\n\n" + insert_sql


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


def render_hist_incremental_sql(td: TargetDataset, dialect) -> str:
  """
  Renderer for *_hist datasets.

  Returns:
    - a descriptive SCD Type 2 comment block,
    - plus *real* SQL for changed rows (UPDATE),
    - plus *real* SQL for deleted business keys (UPDATE),
    - and, for real TargetDataset instances, INSERT statements
      for changed and new business keys.
  """
  schema = getattr(td, "target_schema", None)
  schema_name = getattr(schema, "schema_name", "<unknown_schema>")
  hist_name = getattr(td, "target_dataset_name", "<unknown_table>")

  # rawcore_name may be needed later for diagnostic or routing purposes.
  rawcore_name = get_rawcore_name_from_hist(hist_name)

  comment = (
    f"-- History load for {schema_name}.{hist_name} is not implemented yet.\n"
    f"-- Planned SCD Type 2 semantics based on:\n"
    f"--   * surrogate key for history rows\n"
    f"--   * row_hash for change detection\n"
    f"--   * version_started_at / version_ended_at\n"
    f"--   * version_state ('new', 'changed', 'deleted')\n"
    f"--   * load_run_id and load_timestamp provided by executor\n"
    f"--\n"
    f"-- Real SQL for new, changed and deleted business keys follows below.\n"
  )

  changed_update_sql = render_hist_changed_update_sql(td, dialect)
  delete_sql = render_hist_delete_sql(td, dialect)

  parts: list[str] = [
    comment,
    "",
    changed_update_sql,
    "",
    delete_sql,
  ]

  # only real TargetDataset instances (with int-PK) get the INSERT blocks.
  has_pk = isinstance(getattr(td, "id", None), int)

  if has_pk:
    changed_insert_sql = render_hist_changed_insert_sql(td, dialect)
    new_insert_sql = render_hist_new_insert_sql(td, dialect)
    parts.extend([
      "",
      changed_insert_sql,
      "",
      new_insert_sql,
    ])
  else:
    # Dummy context (e.g. tests with DummyHistTargetDataset):
    # No ORM access, only append a remark.
    parts.append(
      "\n-- INSERT statements for changed/new rows are omitted "
      "because this is not a real TargetDataset instance.\n"
    )

  return "\n".join(parts) + "\n"


def render_hist_delete_sql(td: TargetDataset, dialect) -> str:
  """
  Generate the real SQL statement for marking deleted rows
  in the history table. This is the first active piece of the
  SCD Type 2 pipeline.
  """
  schema = td.target_schema
  schema_name = schema.schema_name
  hist_name = td.target_dataset_name

  if not hist_name.endswith("_hist"):
    raise ValueError("render_hist_delete_sql called for non-hist dataset.")

  # Corresponding rawcore name
  rawcore_name = get_rawcore_name_from_hist(hist_name)

  # Surrogate key name is always: <rawcorename>_key
  sk_name = dialect.render_identifier(f"{rawcore_name}_key")

  return (
    f"UPDATE {dialect.render_table_identifier(schema_name, hist_name)} AS h\n"
    f"SET\n"
    f"  version_ended_at = {{ load_timestamp }},\n"
    f"  version_state    = 'deleted',\n"
    f"  load_run_id      = {{ load_run_id }}\n"
    f"WHERE h.version_ended_at IS NULL\n"
    f"  AND NOT EXISTS (\n"
    f"    SELECT 1\n"
    f"    FROM {dialect.render_table_identifier(schema_name, rawcore_name)} AS r\n"
    f"    WHERE r.{sk_name} = h.{sk_name}\n"
    f"  );"
  )


def render_hist_changed_update_sql(td: TargetDataset, dialect) -> str:
  """
  Generate the real SQL statement for closing changed rows in the
  history table (same business key, different row_hash).
  """
  schema = td.target_schema
  schema_name = schema.schema_name
  hist_name = td.target_dataset_name

  if not hist_name.endswith("_hist"):
    raise ValueError("render_hist_changed_update_sql called for non-hist dataset.")

  # Corresponding rawcore table and its surrogate key
  rawcore_name = get_rawcore_name_from_hist(hist_name)
  sk_name = dialect.render_identifier(f"{rawcore_name}_key")

  return (
    f"UPDATE {dialect.render_table_identifier(schema_name, hist_name)} AS h\n"
    f"SET\n"
    f"  version_ended_at = {{ load_timestamp }},\n"
    f"  version_state    = 'changed',\n"
    f"  load_run_id      = {{ load_run_id }}\n"
    f"WHERE h.version_ended_at IS NULL\n"
    f"  AND EXISTS (\n"
    f"    SELECT 1\n"
    f"    FROM {dialect.render_table_identifier(schema_name, rawcore_name)} AS r\n"
    f"    WHERE r.{sk_name} = h.{sk_name}\n"
    f"      AND r.row_hash <> h.row_hash\n"
    f"  );"
  )

def _get_hist_insert_columns(td: TargetDataset, dialect) -> tuple[list[str], list[str]]:
  """
  Returns a pair:
    (history_table_columns, rawcore_select_columns)
  for INSERT INTO ... SELECT ...

  Left side (history_table_columns):
    - Already rendered column identifiers for the history table
      (quoted as needed).

  Right side (rawcore_select_columns):
    - SQL expressions to select from the rawcore alias (usually "r"),
      using dialect-safe identifier rendering.
  """
  hist_tn = td.target_dataset_name
  if not hist_tn.endswith("_hist"):
    raise ValueError("_get_hist_insert_columns called for non-hist dataset")

  rawcore_name = get_rawcore_name_from_hist(hist_tn)
  sk_name_hist = f"{rawcore_name}_hist_key"
  sk_name_raw = f"{rawcore_name}_key"

  q = dialect.render_identifier
  raw_alias = "r"

  hist_cols: list[str] = []
  rawcore_cols: list[str] = []

  # 1. History SK first → target column name on the hist table
  hist_cols.append(q(sk_name_hist))
  rawcore_cols.append(f"{raw_alias}.{q(sk_name_raw)}")

  # 2. Rawcore columns via lineage (all non-SK upstream columns)
  rawcore_inputs = (
    TargetColumnInput.objects
    .filter(target_column__target_dataset=td)
    .select_related("upstream_target_column")
    .order_by("target_column__ordinal_position")
  )

  for tci in rawcore_inputs:
    rc_col = tci.upstream_target_column
    if rc_col is not None and not rc_col.surrogate_key_column:
      col_name = rc_col.target_column_name
      hist_cols.append(q(col_name))
      rawcore_cols.append(f"{raw_alias}.{q(col_name)}")

  # 3. row_hash (persists changes)
  hist_cols.append(q("row_hash"))
  rawcore_cols.append(f"{raw_alias}.{q('row_hash')}")

  # 4. Versioning metadata (only left side, right side = constants)
  hist_cols.extend([
    q("version_started_at"),
    q("version_ended_at"),
    q("version_state"),
    q("load_run_id"),
  ])

  rawcore_cols.extend([
    "{{ load_timestamp }}",
    "NULL",
    "'changed'",  # default for changed path
    "{{ load_run_id }}",
  ])

  return hist_cols, rawcore_cols


def render_hist_changed_insert_sql(td: TargetDataset, dialect) -> str:
  schema = td.target_schema
  hist_name = td.target_dataset_name
  schema_name = schema.schema_name

  rawcore_name = get_rawcore_name_from_hist(hist_name)
  sk_name_raw = dialect.render_identifier(f"{rawcore_name}_key")

  hist_cols, rawcore_cols = _get_hist_insert_columns(td, dialect)

  return (
    f"INSERT INTO {dialect.render_table_identifier(schema_name, hist_name)} (\n"
    f"  " + ",\n  ".join(hist_cols) + "\n"
    f")\n"
    f"SELECT\n"
    f"  " + ",\n  ".join(rawcore_cols) + "\n"
    f"FROM {dialect.render_table_identifier(schema_name, rawcore_name)} AS r\n"
    f"WHERE EXISTS (\n"
    f"  SELECT 1\n"
    f"  FROM {dialect.render_table_identifier(schema_name, hist_name)} AS h\n"
    f"  WHERE h.version_ended_at = {{ load_timestamp }}\n"
    f"    AND h.version_state = 'changed'\n"
    f"    AND h.{sk_name_raw} = r.{sk_name_raw}\n"
    f");"
  )


def render_hist_new_insert_sql(td: TargetDataset, dialect) -> str:
  schema = td.target_schema
  hist_name = td.target_dataset_name
  schema_name = schema.schema_name

  rawcore_name = get_rawcore_name_from_hist(hist_name)
  sk_name_raw = dialect.render_identifier(f"{rawcore_name}_key")

  hist_cols, rawcore_cols = _get_hist_insert_columns(td, dialect)

  # last col → override state from 'changed'
  rawcore_cols = list(rawcore_cols)
  rawcore_cols[-2] = "'new'"  # version_state

  return (
    f"INSERT INTO {dialect.render_table_identifier(schema_name, hist_name)} (\n"
    f"  " + ",\n  ".join(hist_cols) + "\n"
    f")\n"
    f"SELECT\n"
    f"  " + ",\n  ".join(rawcore_cols) + "\n"
    f"FROM {dialect.render_table_identifier(schema_name, rawcore_name)} AS r\n"
    f"WHERE NOT EXISTS (\n"
    f"  SELECT 1\n"
    f"  FROM {dialect.render_table_identifier(schema_name, hist_name)} AS h\n"
    f"  WHERE h.{sk_name_raw} = r.{sk_name_raw}\n"
    f");"
  )


def render_hist_changed_insert_template(td: TargetDataset) -> str:
  """
  Comment-only template for the 'insert new versions for changed rows'
  part of the SCD Type 2 history load.

  This is *not* executed yet, but documents the intended SQL pattern.
  """
  schema = td.target_schema
  schema_name = schema.schema_name
  hist_name = td.target_dataset_name

  if not hist_name.endswith("_hist"):
    raise ValueError("render_hist_changed_insert_template called for non-hist dataset.")

  rawcore_name = hist_name[:-5]
  sk_name = f"{rawcore_name}_key"

  return (
    f"--\n"
    f"-- 2) Insert new *versions* for changed rows:\n"
    f"--\n"
    f"-- INSERT INTO {schema_name}.{hist_name} (\n"
    f"--   /* TODO: history-row SK column, e.g. {rawcore_name}_hist_key */\n"
    f"--   /* TODO: all rawcore columns (including {sk_name}, row_hash, ...) */\n"
    f"--   /* TODO: version_started_at, version_ended_at, version_state, load_run_id */\n"
    f"-- )\n"
    f"-- SELECT\n"
    f"--   /* TODO: history SK expression based on ({sk_name}, version_started_at) */\n"
    f"--   /* TODO: r.* columns in the right order */\n"
    f"--   {{ load_timestamp }}      AS version_started_at,\n"
    f"--   NULL                      AS version_ended_at,\n"
    f"--   'changed'                 AS version_state,\n"
    f"--   {{ load_run_id }}         AS load_run_id\n"
    f"-- FROM {schema_name}.{rawcore_name} AS r\n"
    f"-- WHERE EXISTS (\n"
    f"--   SELECT 1\n"
    f"--   FROM {schema_name}.{hist_name} AS h\n"
    f"--   WHERE h.version_ended_at = {{ load_timestamp }}\n"
    f"--     AND h.version_state    = 'changed'\n"
    f"--     AND h.{sk_name}        = r.{sk_name}\n"
    f"-- );\n"
  )


def build_load_run_summary(
  td: TargetDataset,
  dialect: Any,
  plan: Any | None = None,
) -> Dict[str, Any]:
  """
  Build a small, serializable summary of how this dataset will be loaded.

  This is used by the elevata_load command for logging and debug output.
  """
  if plan is None:
    plan = build_load_plan(td)

  schema = getattr(td.target_schema, "short_name", None) or getattr(
    td.target_schema,
    "schema_name",
    None,
  )

  if dialect is None:
    dialect_name = "<unknown>"
  else:
    dialect_name = getattr(
      dialect,
      "DIALECT_NAME",
      dialect.__class__.__name__.lower(),
    )

  return {
    "schema": schema,
    "dataset": td.target_dataset_name,
    "mode": getattr(plan, "mode", None),
    "handle_deletes": bool(getattr(plan, "handle_deletes", False)),
    "historize": bool(getattr(plan, "historize", False)),
    "dialect": dialect_name,
  }


def format_load_run_summary(summary: Dict[str, Any]) -> str:
  """
  Return a compact, human-readable one-line description of a load run.

  Example:
    [duckdb] rawcore.rc_customer mode=merge, deletes=True, historize=False
  """
  schema = summary.get("schema") or "?"
  dataset = summary.get("dataset") or "?"
  mode = summary.get("mode") or "?"
  dialect = summary.get("dialect") or "?"
  handle_deletes = bool(summary.get("handle_deletes"))
  historize = bool(summary.get("historize"))

  return (
    f"[{dialect}] {schema}.{dataset} "
    f"mode={mode}, deletes={handle_deletes}, historize={historize}"
  )


def render_load_sql_for_target(td: TargetDataset, dialect) -> str:
  """
  High-level entry point for load SQL generation.

  Uses the LoadPlan to decide which concrete renderer to call.
  """

  # Special case: history datasets – use dedicated history renderer.
  # Guarded with getattr so DummyTargetDataset in tests still works.
  schema = getattr(td, "target_schema", None)
  schema_short = getattr(schema, "short_name", None)
  name = getattr(td, "target_dataset_name", None)

  if (
    schema_short == "rawcore"
    and isinstance(name, str)
    and name.endswith("_hist")
  ):
    return render_hist_incremental_sql(td, dialect)

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
