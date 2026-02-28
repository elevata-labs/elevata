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
from metadata.rendering.renderer import get_effective_materialization, render_select_for_target
from metadata.rendering.builder import build_logical_select_for_target
from metadata.rendering.logical_plan import LogicalSelect, SourceTable, SelectItem
from metadata.rendering.dsl import parse_surrogate_dsl
from metadata.rendering.expr import (
  Expr, ColumnRef, Cast, Concat, Coalesce, FuncCall, RawSql
)
from metadata.ingestion.types_map import canonicalize_type


def _get_target_columns_in_order(td: TargetDataset) -> Sequence[TargetColumn]:
  """
  Return all target columns of this dataset in a stable order,
  typically by ordinal_position then by name as a fallback.
  """
  return (
    td.target_columns
    .order_by("ordinal_position", "target_column_name")
  )


def _render_typed_stage_projection_for_target(
  td: TargetDataset,
  *,
  dialect,
  stage_td: TargetDataset,
  source_alias: str = "s",
  target_columns: list[str] | None = None,
) -> tuple[str, dict[str, str]]:
  """
  Build a stage-based source SELECT that yields *target-typed* columns.

  Returns:
    (source_select_sql, typed_expr_map)

  This is used by merge and delete detection to ensure assignments and join
  predicates are type-compatible (e.g. BigQuery TIMESTAMP -> DATE).
  """

  # Expressions per target column name from the logical SELECT
  expr_map = _get_rendered_column_exprs_for_target(td, dialect)
  expr_map = _apply_target_type_casts(
    td=td,
    dialect=dialect,
    expr_map=expr_map,
  )

  # Determine projection columns
  if target_columns is None:
    tcols = list(_get_target_columns_in_order(td))
    target_columns = [c.target_column_name for c in tcols]

  # Strict mode: every requested projection column must have a rendered expression
  # from the logical SELECT. We do not fall back to "s.<col>" because that can
  # silently produce incorrect SQL (wrong column names, missing manual_expr, missing CASTs).
  missing = [c for c in target_columns if c not in expr_map]
  if missing:
    raise ValueError(
      "Strict mode: logical SELECT did not produce expressions for required target columns: "
      + ", ".join(missing)
    )

  # Build a vendor-neutral LogicalSelect and let the dialect render the SQL shape.
  src = SourceTable(
    schema=stage_td.target_schema.schema_name,
    name=stage_td.target_dataset_name,
    alias=source_alias,
  )

  select_list: list[SelectItem] = []
  for col in target_columns:
    expr_sql = expr_map.get(col)
    if expr_sql:
      # expr_sql is already rendered for the active dialect (incl. casts),
      # so we wrap it as RawSql to avoid re-rendering expression structure.
      expr_obj: Expr = RawSql(sql=expr_sql, default_table_alias=source_alias, is_template=False)
    else:
      # Fall back to a direct column reference from the stage table.
      expr_obj = ColumnRef(table_alias=source_alias, column_name=col)
      # Keep expr_map complete for downstream callers (join predicates, assignments).
      expr_map[col] = dialect.render_expr(expr_obj)

    select_list.append(SelectItem(expr=expr_obj, alias=col))

  sel = LogicalSelect(
    from_=src,
    select_list=select_list,
  )

  source_select_sql = dialect.render_select(sel)
  return source_select_sql, expr_map


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
    raise ValueError(
      "Merge source rendering currently requires a single LogicalSelect. "
      "UNION-based logical plans are not supported for merge yet."
    )  

  expr_map: dict[str, str] = {}

  for item in plan.select_list:
    # item.alias is the final target column name
    expr_sql = dialect.render_expr(item.expr)
    expr_map[item.alias] = expr_sql

  return expr_map

def _build_incremental_scope_filter_for_target(
  td: TargetDataset,
  *,
  dialect,
  target_alias: str = "t",
) -> str | None:
  """
  Build a WHERE-clause fragment for delete detection on the target dataset,
  based on the incremental_filter defined on td.incremental_source (SourceDataset).

  Key behavior:
  - The filter is authored in terms of SourceDataset column names.
  - We map those source column names to rawcore target column names via full lineage.
  - Replacements are rendered as qualified + quoted target references:
      <target_alias>.<dialect.render_identifier(rawcore_col_name)>
  - The placeholder {{DELTA_CUTOFF}} is preserved for runtime substitution.

  Notes:
  - This is intentionally a string-based rewrite (not a full boolean DSL parser).
  - We only rewrite identifiers that match known source column names (case-insensitive).
  """

  # Unit tests (and some callers) may provide lightweight stubs that do not
  # include the ORM relation. In that case, we cannot do a safe rewrite.
  if not getattr(td, "target_columns", None):
    return None

  src = getattr(td, "incremental_source", None)
  if not src:
    return None

  # Only apply when an incremental filter exists (and is enabled on the source dataset).
  if not getattr(src, "incremental", False):
    return None

  inc_filter = (getattr(src, "increment_filter", None) or "").strip()
  if not inc_filter:
    return None

  def _norm(name: str) -> str:
    # Normalize identifiers so ModifiedDate and modified_date match.
    return re.sub(r"[^a-z0-9]+", "", (name or "").lower())

  # Mapping: stage_column_name.lower() -> rawcore target_column_name
  # This is stable even if rawcore columns are renamed, because rawcore is built from stage.
  mapping: dict[str, str] = {}

  # Iterate rawcore columns and resolve their immediate stage upstream column (active link)
  cols_obj = getattr(td, "target_columns", None)
  if cols_obj is None:
    return None
  cols = cols_obj.all() if hasattr(cols_obj, "all") else cols_obj

  for rc_col in cols:
    # Find the primary upstream stage column feeding this rawcore column
    link_qs = getattr(rc_col, "input_links", None)
    if not link_qs:
      continue

    link = (
      link_qs
        .filter(active=True, upstream_target_column__isnull=False)
        .select_related("upstream_target_column")
        .order_by("ordinal_position", "id")
        .first()
    )

    if not link:
      link = (
        link_qs
          .filter(upstream_target_column__isnull=False)
          .select_related("upstream_target_column")
          .order_by("ordinal_position", "id")
          .first()
    )

    if not link or not link.upstream_target_column:
      continue

    stage_col_name = getattr(link.upstream_target_column, "target_column_name", None)
    if not stage_col_name:
      continue

    mapping[_norm(stage_col_name)] = rc_col.target_column_name

  if not mapping:
    return None

  expr = inc_filter

  # Optional normalization: allow col("X") / col('X') in increment_filter
  # without requiring a full boolean DSL parser.
  expr = re.sub(r"col\(\s*['\"]([^'\"]+)['\"]\s*\)", r"\1", expr)

  # Rewrite identifiers that match source column names to qualified rawcore columns.
  # We keep placeholders like {{DELTA_CUTOFF}} intact.
  #
  # IMPORTANT: This is a conservative rewrite. It only replaces tokens that
  # exactly match known source column identifiers (by regex word boundary).

  ident_pattern = re.compile(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b")

  def replace_identifier(match: re.Match) -> str:
    token = match.group(0)

    # Do not touch the placeholder keyword itself (DELTA_CUTOFF)
    if token.upper() == "DELTA_CUTOFF":
      return token

    target_col = mapping.get(_norm(token))
    if target_col is None:
      return token

    return f"{target_alias}.{dialect.render_identifier(target_col)}"

  expr = ident_pattern.sub(replace_identifier, expr)

  # Normalize whitespace
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

  scope_filter = _build_incremental_scope_filter_for_target(
    td,
    dialect=dialect,
    target_alias="t",
  )
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

  # Build typed expressions (for join predicate RHS)
  # NOTE: We only need expr_map here, but we build through the shared helper
  # so casting behavior stays consistent across merge + delete detection.
  _source_select_sql, expr_map = _render_typed_stage_projection_for_target(
    td,
    dialect=dialect,
    stage_td=stage_td,
    source_alias="s",
    target_columns=[c for c in td.natural_key_fields],
  )

  target_alias = "t"
  source_alias = "s"
  q = dialect.render_identifier

  join_predicates: list[str] = []
  for col_name in key_cols:
    # Right-hand side: expression as defined for the *target* column when
    # reading from stage (usually s."<stage_col_name>", but inc. manual_expr and CASTs)
    rhs_sql = expr_map.get(col_name)
    if not rhs_sql:
      raise ValueError(
        "Strict mode: delete detection requires a rendered key expression for "
        f"natural key column {col_name!r} on target {td.target_dataset_name}."
      )

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


def _apply_target_type_casts(td, dialect, expr_map):
  """
  Apply dialect-aware CAST only when source and target types differ.
  """
  result = dict(expr_map or {})

  # In unit tests we often pass lightweight TargetDataset stubs without ORM relations.
  # If target_columns are not available, we cannot resolve target types reliably.
  if not getattr(td, "target_columns", None):
    return result

  def _canonical(t: str | None) -> str | None:
    if not t:
      return None
    # Prefer dialect canonicalization if available (new base API).
    if hasattr(dialect, "canonicalize_logical_type"):
      try:
        return dialect.canonicalize_logical_type(t, strict=False)
      except Exception:
        pass
    try:
      return canonicalize_type(dialect.DIALECT_NAME, t)
    except Exception:
      return (t or "").strip().upper() or None

  def _resolve_upstream_logical_type(target_col) -> str | None:
    """
    Best-effort: follow active upstream link (rawcore <- stage) and read upstream_target_column.datatype.
    If we can't resolve it (e.g. in unit tests with fake columns), return None.
    """
    links = getattr(target_col, "input_links", None)
    if not links:
      return None

    # Try queryset-style first (Django ORM).
    try:
      link = (
        links
          .filter(active=True, upstream_target_column__isnull=False)
          .select_related("upstream_target_column")
          .order_by("ordinal_position", "id")
          .first()
      )
      if not link:
        link = (
          links
            .filter(upstream_target_column__isnull=False)
            .select_related("upstream_target_column")
            .order_by("ordinal_position", "id")
            .first()
        )
      if link and getattr(link, "upstream_target_column", None):
        return getattr(link.upstream_target_column, "datatype", None)
      return None
    except Exception:
      pass

    # Fallback: iterable list of links.
    try:
      for link in list(links):
        utc = getattr(link, "upstream_target_column", None)
        if utc is None:
          continue
        return getattr(utc, "datatype", None)
    except Exception:
      return None
    return None

  try:
    cols_in_order = _get_target_columns_in_order(td)
  except Exception:
    return result

  for c in cols_in_order:
    col_name = c.target_column_name
    expr = result.get(col_name)
    if not expr:
      continue

    # Compare canonical logical types (metadata-driven).
    src_logical = _resolve_upstream_logical_type(c)
    tgt_logical = getattr(c, "datatype", None)
    src_can = _canonical(src_logical)
    tgt_can = _canonical(tgt_logical)

    # If we cannot determine upstream type (e.g. tests / missing lineage),
    # we do NOT cast. This keeps behavior conservative and avoids noisy CASTs.
    if not src_can or not tgt_can or src_can == tgt_can:
      continue

    target_physical = dialect.map_logical_type(
      datatype=c.datatype,
      max_length=getattr(c, "max_length", None),
      precision=getattr(c, "decimal_precision", None),
      scale=getattr(c, "decimal_scale", None),
      strict=True,
    )

    try:
      result[col_name] = dialect.cast_expression(expr, target_physical)

    except Exception:
      pass

  return result


def render_merge_sql(td: TargetDataset, dialect) -> str:
  """
  Render a backend-aware MERGE / UPSERT statement for a target dataset.

  Convention:
    - load_sql provides only the semantic ingredients:
        * source SELECT SQL
        * key columns
        * update columns
        * insert columns
    - the SQL shape (native MERGE, UPDATE+INSERT, INSERT..ON CONFLICT, ...)
      is the dialect's responsibility via dialect.render_merge_statement().

  Assumptions:
    - td.incremental_strategy == 'merge' (if set)
    - target schema is rawcore (merge loads currently defined for rawcore only)
    - natural_key_fields define the business key
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

  key_cols = list(getattr(td, "natural_key_fields", None) or [])

  if not key_cols:
    raise ValueError(
      f"TargetDataset {getattr(td, 'id', '?')} has merge strategy but no natural_key_fields defined."
    )
  
  # Find upstream stage dataset (needed to build a stable, typed merge source SELECT)
  stage_td = _find_stage_upstream_for_rawcore(td)
  if not stage_td:
    raise ValueError(
      f"Could not resolve upstream stage dataset for rawcore target {td.target_dataset_name}."
    )

  # All target columns in stable order
  target_cols = list(_get_target_columns_in_order(td))
  insert_columns = [c.target_column_name for c in target_cols]
  update_columns = [c for c in insert_columns if c not in set(key_cols)]

  source_select_sql, _expr_map = _render_typed_stage_projection_for_target(
    td,
    dialect=dialect,
    stage_td=stage_td,
    source_alias="s",
    target_columns=insert_columns,
  )

  target_fqn = dialect.render_table_identifier(
    td.target_schema.schema_name,
    td.target_dataset_name,
  )

  return dialect.render_merge_statement(
    target_fqn=target_fqn,
    source_select_sql=source_select_sql,
    key_columns=key_cols,
    update_columns=update_columns,
    insert_columns=insert_columns,
    target_alias="t",
    source_alias="s",
  )


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

  target_cols = [c.target_column_name for c in _get_target_columns_in_order(td)]
  return dialect.render_insert_into_table(
    schema_name,
    table_name,
    select_sql,
    target_columns=target_cols,
  )


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


def render_hist_incremental_sql(td: TargetDataset, dialect, *, include_comment: bool = False) -> str:
  """
  Renderer for *_hist datasets.

  elevata convention:
    - load_sql provides semantic ingredients
    - dialect owns SQL shape AND orchestration for the history pipeline
  """
  schema = getattr(td, "target_schema", None)
  schema_name = getattr(schema, "schema_name", "<unknown_schema>")
  hist_name = getattr(td, "target_dataset_name", "<unknown_table>")

  if not getattr(td, "is_hist", False):
    raise ValueError("render_hist_incremental_sql called for non-historize dataset.")

  rawcore_name = get_rawcore_name_from_hist(hist_name)
  has_pk = isinstance(getattr(td, "id", None), int)

  # Strict mode: validate presence of required history columns when metadata is available.
  cols_obj = getattr(td, "target_columns", None)
  if cols_obj is not None:
    cols = cols_obj.all() if hasattr(cols_obj, "all") else cols_obj
    names = {getattr(c, "target_column_name", None) for c in cols}

    required_hist = {
      "row_hash",
      "version_started_at",
      "version_ended_at",
      "version_state",
      "load_run_id",
      "loaded_at",
      f"{rawcore_name}_key",
    }
    missing = sorted([c for c in required_hist if c not in names])
    if missing:
      raise ValueError(
        "Strict mode: historization requires the following columns on "
        f"'{hist_name}': {', '.join(missing)}"
      )

  changed_insert_kwargs = None
  new_insert_kwargs = None

  if has_pk:
    hist_cols, rawcore_cols = _get_hist_insert_columns(td, dialect)

    # Strict mode: ensure version_state exists (required for setting 'new')
    q = dialect.render_identifier
    vs_ident = q("version_state")
    if vs_ident not in hist_cols:
      raise ValueError(
        f"Strict mode: history dataset '{hist_name}' is missing required column 'version_state'."
      )

    sk_name_raw = q(f"{rawcore_name}_key")

    changed_insert_kwargs = dict(
      hist_schema=schema_name,
      hist_table=hist_name,
      hist_columns_sql=hist_cols,
      source_schema=schema_name,
      source_table=rawcore_name,
      source_alias="r",
      select_exprs_sql=list(rawcore_cols),
      exists_schema=schema_name,
      exists_table=hist_name,
      exists_alias="h",
      exists_predicates=[
        "h.version_ended_at = {{ load_timestamp }}",
        "h.version_state = 'changed'",
        f"h.{sk_name_raw} = r.{sk_name_raw}",
      ],
      exists_negated=False,
    )

    # New insert: clone select exprs, override version_state to 'new'
    rawcore_cols_new = list(rawcore_cols)
    state_idx = hist_cols.index(vs_ident)
    rawcore_cols_new[state_idx] = "'new'"

    new_insert_kwargs = dict(
      hist_schema=schema_name,
      hist_table=hist_name,
      hist_columns_sql=hist_cols,
      source_schema=schema_name,
      source_table=rawcore_name,
      source_alias="r",
      select_exprs_sql=rawcore_cols_new,
      exists_schema=schema_name,
      exists_table=hist_name,
      exists_alias="h",
      exists_predicates=[f"h.{sk_name_raw} = r.{sk_name_raw}"],
      exists_negated=True,
    )

  if not hasattr(dialect, "render_hist_incremental_statement"):
    raise NotImplementedError(
      f"{dialect.__class__.__name__} does not implement render_hist_incremental_statement()."
    )

  return dialect.render_hist_incremental_statement(
    schema_name=schema_name,
    hist_table=hist_name,
    rawcore_table=rawcore_name,
    include_comment=include_comment,
    include_inserts=has_pk,
    changed_insert_kwargs=changed_insert_kwargs,
    new_insert_kwargs=new_insert_kwargs,
  )


def render_hist_delete_sql(td: TargetDataset, dialect) -> str:
  """
  Generate the real SQL statement for marking deleted rows
  in the history table. This is the first active piece of the
  SCD Type 2 pipeline.
  """
  schema = td.target_schema
  schema_name = schema.schema_name
  hist_name = td.target_dataset_name

  if not td.is_hist:
    raise ValueError("render_hist_delete_sql called for non-historize dataset.")

  # Corresponding rawcore name
  rawcore_name = get_rawcore_name_from_hist(hist_name)

  if not hasattr(dialect, "render_hist_delete_sql"):
    raise NotImplementedError(
      f"{dialect.__class__.__name__} does not implement render_hist_delete_sql()."
    )

  return dialect.render_hist_delete_sql(
    schema_name=schema_name,
    hist_table=hist_name,
    rawcore_table=rawcore_name,
  )


def render_hist_changed_update_sql(td: TargetDataset, dialect) -> str:
  """
  Generate the real SQL statement for closing changed rows in the
  history table (same business key, different row_hash).
  """
  schema = td.target_schema
  schema_name = schema.schema_name
  hist_name = td.target_dataset_name

  if not td.is_hist:
    raise ValueError("render_hist_changed_update_sql called for non-historize dataset.")

  # Corresponding rawcore table and its surrogate key
  rawcore_name = get_rawcore_name_from_hist(hist_name)

  if not hasattr(dialect, "render_hist_changed_update_sql"):
    raise NotImplementedError(
      f"{dialect.__class__.__name__} does not implement render_hist_changed_update_sql()."
    )

  return dialect.render_hist_changed_update_sql(
    schema_name=schema_name,
    hist_table=hist_name,
    rawcore_table=rawcore_name,
  )


# NOTE: We keep surrogate_expression vendor-neutral (DSL) and render it via dialect.render_expr.
# The only special override needed for hist SK: version_started_at is a runtime value ({{ load_timestamp }}),
# not a physical column on the rawcore table alias.
def _replace_colref(
  expr: Expr,
  *,
  table_alias: str | None,
  column_name: str,
  replacement: Expr,
) -> Expr:
  # Replace a specific ColumnRef inside an Expr tree.
  if isinstance(expr, ColumnRef):
    if expr.column_name == column_name and (table_alias is None or expr.table_alias == table_alias):
      return replacement
    return expr

  if isinstance(expr, Cast):
    return Cast(
      expr=_replace_colref(expr.expr, table_alias=table_alias, column_name=column_name, replacement=replacement),
      target_type=expr.target_type,
    )

  if isinstance(expr, Concat):
    return Concat(parts=[
      _replace_colref(p, table_alias=table_alias, column_name=column_name, replacement=replacement)
      for p in expr.parts
    ])

  if isinstance(expr, Coalesce):
    return Coalesce(parts=[
      _replace_colref(p, table_alias=table_alias, column_name=column_name, replacement=replacement)
      for p in expr.parts
    ])

  if isinstance(expr, FuncCall):
    return FuncCall(
      name=expr.name,
      args=[
        _replace_colref(a, table_alias=table_alias, column_name=column_name, replacement=replacement)
        for a in expr.args
      ],
    )

  # RawSql and unknown nodes: leave as-is
  return expr


def _get_hist_insert_columns(td: TargetDataset, dialect) -> tuple[list[str], list[str]]:
  """
  Returns:
    (history_table_columns, rawcore_select_columns)
  for INSERT INTO ... SELECT ...

  - Left side: already rendered identifiers for hist table
  - Right side: SQL expressions selecting from rawcore alias "r" and runtime placeholders
  """
  hist_tn = td.target_dataset_name
  if not td.is_hist:
    raise ValueError("_get_hist_insert_columns called for non-historize dataset.")


  rawcore_name = get_rawcore_name_from_hist(hist_tn)
  sk_name_hist = f"{rawcore_name}_hist_key"   # history-row SK (new per version)
  sk_name_raw = f"{rawcore_name}_key"         # entity key from rawcore

  q = dialect.render_identifier
  raw_alias = "r"

  # Only use ORM lookups when td is a real database object.
  # Tests use dummy datasets without a PK.
  has_pk = isinstance(getattr(td, "id", None), int)

  hist_cols: list[str] = []
  rawcore_cols: list[str] = []
  added: set[str] = set()

  # 1) History-row surrogate key first.
  hist_cols.append(q(sk_name_hist))
  added.add(sk_name_hist)

  if has_pk:
    hist_sk_col = (
      TargetColumn.objects
      .filter(
        target_dataset_id=td.id,
        target_column_name=sk_name_hist,
        system_role="surrogate_key",
        active=True,
      )
      .first()
    )

    if hist_sk_col is None or not getattr(hist_sk_col, "surrogate_expression", None):
      raise ValueError(
        f"History dataset '{td.target_dataset_name}' is missing surrogate_expression "
        f"for column '{sk_name_hist}'."
      )

    # Parse DSL → Expr tree (keeps vendor-neutral functions like HASH256)
    sk_expr = parse_surrogate_dsl(hist_sk_col.surrogate_expression, table_alias=raw_alias)

    # Override: version_started_at is a runtime placeholder, not r.version_started_at.
    sk_expr = _replace_colref(
      sk_expr,
      table_alias=raw_alias,
      column_name="version_started_at",
      replacement=RawSql(sql="{{ load_timestamp }}"),
    )

    # Render via dialect (this maps HASH256 correctly, e.g. to SHA256(...) on DuckDB)
    rawcore_cols.append(dialect.render_expr(sk_expr))

  else:
    # Dummy context (tests): no ORM access
    rawcore_cols.append(f"{raw_alias}.{q(sk_name_raw)}")

  # 1b) Ensure entity key column exists on hist (e.g. rc_aw_product_key).
  if sk_name_raw not in added:
    hist_cols.append(q(sk_name_raw))
    rawcore_cols.append(f"{raw_alias}.{q(sk_name_raw)}")
    added.add(sk_name_raw)

  # 2) Rawcore columns via lineage (all non-SK upstream columns)
  rawcore_inputs = (
    TargetColumnInput.objects
    .filter(target_column__target_dataset=td)
    .select_related("upstream_target_column")
    .order_by("target_column__ordinal_position")
  )

  for tci in rawcore_inputs:
    rc_col = tci.upstream_target_column
    if rc_col is None:
      continue

    # Skip upstream rawcore surrogate key (already handled as entity key)
    if rc_col.system_role == "surrogate_key":
      continue

    col_name = rc_col.target_column_name
    if col_name not in added:
      hist_cols.append(q(col_name))
      rawcore_cols.append(f"{raw_alias}.{q(col_name)}")
      added.add(col_name)

  # 3) row_hash (only if not already included)
  if "row_hash" not in added:
    hist_cols.append(q("row_hash"))
    rawcore_cols.append(f"{raw_alias}.{q('row_hash')}")
    added.add("row_hash")

  # 4) Versioning metadata (left side identifiers, right side constants/placeholders)
  hist_cols.extend([
    q("version_started_at"),
    q("version_ended_at"),
    q("version_state"),
    q("load_run_id"),
    q("loaded_at"),
  ])

  rawcore_cols.extend([
    "{{ load_timestamp }}",
    "NULL",
    "'changed'",          # default for changed path
    "{{ load_run_id }}",
    "{{ load_timestamp }}",
  ])

  # Defensive guard: prevent duplicate columns in INSERT lists
  def _unquote_ident(x: str) -> str:
    x = x.strip()
    if x.startswith('"') and x.endswith('"') and len(x) >= 2:
      return x[1:-1]
    if x.startswith("[") and x.endswith("]") and len(x) >= 2:
      return x[1:-1]
    return x

  unquoted = [_unquote_ident(c) for c in hist_cols]
  dupes = sorted({c for c in unquoted if unquoted.count(c) > 1})
  if dupes:
    raise ValueError(
      f"Duplicate history insert columns detected: {dupes}. "
      "This indicates a metadata/lineage overlap (e.g. row_hash duplicated)."
    )
  
  # Strict mode: INSERT list alignment must be 1:1
  if len(hist_cols) != len(rawcore_cols):
    raise ValueError(
      f"Strict mode: history INSERT column mismatch for '{td.target_dataset_name}'. "
      f"hist_cols={len(hist_cols)}, select_exprs={len(rawcore_cols)}."
    ) 

  return hist_cols, rawcore_cols


def render_hist_changed_insert_sql(td: TargetDataset, dialect) -> str:
  schema = td.target_schema
  hist_name = td.target_dataset_name
  schema_name = schema.schema_name

  rawcore_name = get_rawcore_name_from_hist(hist_name)
  sk_name_raw = dialect.render_identifier(f"{rawcore_name}_key")

  hist_cols, rawcore_cols = _get_hist_insert_columns(td, dialect)

  # Strict guard: ensure version_state column exists
  if dialect.render_identifier("version_state") not in hist_cols:
    raise ValueError(
      f"Strict mode: history dataset '{td.target_dataset_name}' "
      f"is missing required column 'version_state'."
    )

  return dialect.render_hist_insert_statement(
    hist_schema=schema_name,
    hist_table=hist_name,
    hist_columns_sql=hist_cols,
    source_schema=schema_name,
    source_table=rawcore_name,
    source_alias="r",
    select_exprs_sql=list(rawcore_cols),
    exists_schema=schema_name,
    exists_table=hist_name,
    exists_alias="h",
    exists_predicates=[
      "h.version_ended_at = {{ load_timestamp }}",
      "h.version_state = 'changed'",
      f"h.{sk_name_raw} = r.{sk_name_raw}",
    ],
    exists_negated=False,
  )


def render_hist_new_insert_sql(td: TargetDataset, dialect) -> str:
  schema = td.target_schema
  hist_name = td.target_dataset_name
  schema_name = schema.schema_name

  rawcore_name = get_rawcore_name_from_hist(hist_name)
  sk_name_raw = dialect.render_identifier(f"{rawcore_name}_key")

  hist_cols, rawcore_cols = _get_hist_insert_columns(td, dialect)

  rawcore_cols = list(rawcore_cols)

  # Override version_state to 'new' using the aligned column index.
  try:
    state_idx = hist_cols.index(dialect.render_identifier("version_state"))
  except ValueError:
    raise ValueError(
      f"Strict mode: history dataset '{td.target_dataset_name}' "
      f"is missing required column 'version_state'."
    )

  rawcore_cols[state_idx] = "'new'"

  # Defensive alignment check after override
  if len(hist_cols) != len(rawcore_cols):
    raise ValueError(
      f"Strict mode: history INSERT column mismatch for '{td.target_dataset_name}'. "
      f"hist_cols={len(hist_cols)}, select_exprs={len(rawcore_cols)}."
    )

  return dialect.render_hist_insert_statement(
    hist_schema=schema_name,
    hist_table=hist_name,
    hist_columns_sql=hist_cols,
    source_schema=schema_name,
    source_table=rawcore_name,
    source_alias="r",
    select_exprs_sql=rawcore_cols,
    exists_schema=schema_name,
    exists_table=hist_name,
    exists_alias="h",
    exists_predicates=[f"h.{sk_name_raw} = r.{sk_name_raw}"],
    exists_negated=True,
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
  if td.is_hist:
    return render_hist_incremental_sql(td, dialect)
  
  # Materialization handling
  materialization = get_effective_materialization(td)

  if materialization == "external_passthrough":
    return ""  # runner will mark as skipped

  if materialization == "view":
    select_sql = render_select_for_target(td, dialect)
    return dialect.render_create_or_replace_view(
      schema=td.target_schema.schema_name,
      view=td.target_dataset_name,
      select_sql=select_sql,
    )

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
