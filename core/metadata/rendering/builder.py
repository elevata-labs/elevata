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

from typing import Optional, TYPE_CHECKING
import re

from metadata.rendering.logical_plan import (
  SourceTable,
  SelectItem,
  LogicalSelect,
  LogicalUnion,
  SubquerySource,
)
from metadata.generation.naming import build_surrogate_key_name
from metadata.rendering.expr import Expr
from metadata.rendering.dsl import col, raw, row_number, parse_surrogate_dsl

if TYPE_CHECKING:
  # Only for type hints, no runtime import -> no circular import
  from metadata.models import SourceDataset, TargetDataset, TargetColumn, TargetDatasetReference

"""
Logical Query Builder for TargetDatasets.
Responsible for SK/FK hashing, lineage resolution,
and merging multiple inputs (UNION or single-path).
"""

# ------------------------------------------------------------------------------
# Regex patterns
# ------------------------------------------------------------------------------
COL_PATTERN = re.compile(r'col\(["\']([^"\']+)["\']\)')
_IDENT_PATTERN = re.compile(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b")


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


def qualify_source_filter(
  source_dataset,
  filter_sql: str,
  *,
  source_alias: str = "s",
) -> str:
  """
  Qualify identifiers in a SourceDataset filter with the given alias.

  - Input filter is authored in terms of source column names.
  - We prefix matching identifiers with "<alias>.".
  - We keep {{DELTA_CUTOFF}} intact.
  - Conservative rewrite: replaces only tokens that match known source columns.
  """
  if not filter_sql:
    return ""

  # Include ALL source columns (not only integrate=True),
  # because filters often reference technical flags not integrated.
  src_cols = getattr(source_dataset, "source_columns", None)
  if not src_cols:
    return filter_sql

  cols = src_cols.all() if hasattr(src_cols, "all") else src_cols
  known = {getattr(c, "source_column_name", "").lower() for c in cols if getattr(c, "source_column_name", None)}
  if not known:
    return filter_sql

  # allow col("X") / col('X')
  expr = re.sub(r"col\(\s*['\"]([^'\"]+)['\"]\s*\)", r"\1", filter_sql)

  def repl(m: re.Match) -> str:
    tok = m.group(0)

    if tok.upper() == "DELTA_CUTOFF":
      return tok

    # Skip already-qualified tokens like "s.ModifiedDate"
    # (We detect by checking the preceding char in original string via span.)
    start = m.start()
    if start > 0 and expr[start - 1] == ".":
      return tok

    if tok.lower() in known:
      return f"{source_alias}.{tok}"
    return tok

  expr = _IDENT_PATTERN.sub(repl, expr)
  return " ".join(expr.split())


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
    .filter(system_role="surrogate_key", active=True)
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
  return raw(fk_expr_sql)

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

  memberships = getattr(source_dataset, "dataset_groups", None)
  if not memberships:
    return None

  membership = memberships.order_by("-is_primary_system", "id").first()
  if not membership:
    return None

  return getattr(membership, "source_identity_id", None)


def _resolve_source_identity_id_for_source(source_dataset) -> str | None:
  """
  Resolve the source_identity_id for a SourceDataset if present,
  otherwise return None.
  """
  memberships = getattr(source_dataset, "dataset_groups", None)
  if not memberships:
    return None

  membership = memberships.order_by("-is_primary_system", "id").first()
  if not membership:
    return None

  return getattr(membership, "source_identity_id", None)


def _resolve_source_identity_ordinal_for_raw(
  upstream_dataset: "TargetDataset",
) -> Optional[int]:
  """
  Resolve SourceDatasetGroupMembership.source_identity_ordinal
  for a RAW TargetDataset, if present.
  """
  link = (
    upstream_dataset.input_links
    .select_related("source_dataset")
    .filter(active=True)
    .order_by("id")
    .first()
  )
  if not link or not link.source_dataset:
    return None

  source_dataset = link.source_dataset
  memberships = getattr(source_dataset, "dataset_groups", None)
  if not memberships:
    return None

  membership = memberships.order_by("-is_primary_system", "id").first()
  if not membership:
    return None

  return getattr(membership, "source_identity_ordinal", None)


def _resolve_source_identity_ordinal_for_source(
  source_dataset: "SourceDataset",
) -> Optional[int]:
  """
  Resolve SourceDatasetGroupMembership.source_identity_ordinal
  for a SourceDataset, if present.
  """
  memberships = getattr(source_dataset, "dataset_groups", None)
  if not memberships:
    return None

  membership = memberships.order_by("-is_primary_system", "id").first()
  if not membership:
    return None

  return getattr(membership, "source_identity_ordinal", None)


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

  for col_meta in tcols:
    if col_meta.system_role=="surrogate_key" and col_meta.surrogate_expression:
      # Parse SK DSL into an Expr tree; "s" is the source alias in this SELECT
      expr: Expr = parse_surrogate_dsl(
        col_meta.surrogate_expression,
        table_alias="s",
      )

    # Special handling for source_identity_id:
    # In UNION branches we emit a literal per upstream if configured.
    elif col_meta.target_column_name == "source_identity_id" and identity_id is not None:
      # Simple string literal, dialect-agnostic enough for now
      expr = raw(f"'{identity_id}'")

    else:
      if col_meta.target_column_name in upstream_col_names:
        expr = col(col_meta.target_column_name, "s")
      else:
        expr = raw("NULL")

    logical.select_list.append(
      SelectItem(expr=expr, alias=col_meta.target_column_name)
    )

  # Attach hidden rank ordinal based on SourceDatasetGroupMembership
  ordinal = _resolve_source_identity_ordinal_for_raw(upstream_dataset)
  _attach_hidden_rank_ordinal(logical, ordinal)

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

  # Apply incremental extraction filter (if configured on the SourceDataset)
  if getattr(source_dataset, "incremental", False):
    inc_filter = (getattr(source_dataset, "increment_filter", None) or "").strip()
    if inc_filter:
      qualified = qualify_source_filter(source_dataset, inc_filter, source_alias="s")
      logical.where = raw(qualified)

  # Fast lookup of source columns by lowercased name
  src_cols_by_name = {
    sc.source_column_name.lower(): sc
    for sc in source_dataset.source_columns.filter(integrate=True)
  }

  # Resolve identity id (may be None)
  identity_id = _resolve_source_identity_id_for_source(source_dataset)

  for tcol in (
    target_dataset.target_columns
    .filter(active=True)
    .order_by("ordinal_position", "id")
  ):
    # Special handling for the artificial identity column
    if tcol.target_column_name == "source_identity_id":
      if identity_id is not None:
        expr = raw(f"'{identity_id}'")
      else:
        expr = raw("NULL")

    else:
      # Normal mapping: try to find a source column with the same name
      src_col = src_cols_by_name.get(tcol.target_column_name.lower())
      if src_col:
        expr = col(src_col.source_column_name, "s")
      else:
        # No matching source column → NULL for this branch
        expr = raw("NULL")

    logical.select_list.append(
      SelectItem(expr=expr, alias=tcol.target_column_name)
    )

  # Attach hidden rank ordinal for this SourceDataset branch
  ordinal = _resolve_source_identity_ordinal_for_source(source_dataset)
  _attach_hidden_rank_ordinal(logical, ordinal)

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
      sub_selects: list[LogicalSelect] = []
      identity_flags: list[Optional[str]] = []

      for inp in raw_inputs:
        raw_ds = inp.upstream_target_dataset
        sel = _build_single_select_for_upstream(target_dataset, raw_ds)
        sub_selects.append(sel)

        identity_flags.append(_resolve_source_identity_id_for_raw(raw_ds))

      union_plan = LogicalUnion(selects=sub_selects, union_type="ALL")

      mode, _ = _detect_stage_identity_mode(identity_flags)

      if mode == "identity":
        # Identity-mode: no ranking, plain UNION ALL as top-level plan.
        # Optional: remove hidden __src_rank_ord from branches for cleaner SQL.
        _strip_hidden_rank_ordinal_from_union(union_plan)
        return union_plan

      # Non-identity-mode: use window-based ranking on top of the UNION.
      return _build_ranked_stage_union(target_dataset, union_plan)

    # -------- 1b) Multi-source via direct SourceDataset ---------------------
    source_inputs = [inp for inp in inputs_qs if inp.source_dataset is not None]

    if len(source_inputs) > 1:
      sub_selects: list[LogicalSelect] = []
      identity_flags: list[Optional[str]] = []

      for inp in source_inputs:
        src = inp.source_dataset
        sel = _build_single_select_for_source_stage(target_dataset, src)
        sub_selects.append(sel)

        identity_flags.append(_resolve_source_identity_id_for_source(src))

      union_plan = LogicalUnion(selects=sub_selects, union_type="ALL")

      mode, _ = _detect_stage_identity_mode(identity_flags)

      if mode == "identity":
        _strip_hidden_rank_ordinal_from_union(union_plan)
        return union_plan

      return _build_ranked_stage_union(target_dataset, union_plan)

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

  # If STAGE reads directly from SourceDataset (no RAW),
  # apply the incremental extraction filter on the source table.
  if schema_short == "stage":
    src_input = next((inp for inp in inputs_qs if inp.source_dataset is not None), None)
    if src_input and src_input.source_dataset:
      sd = src_input.source_dataset
      if getattr(sd, "incremental", False):
        inc_filter = (getattr(sd, "increment_filter", None) or "").strip()
        if inc_filter:
          qualified = qualify_source_filter(sd, inc_filter, source_alias="s")
          logical.where = raw(qualified)

  tcols = (
    target_dataset.target_columns
    .filter(active=True)
    .order_by("ordinal_position", "id")
  )

  for tcol in tcols:
    # 1) Surrogate key column
    if tcol.system_role == "surrogate_key" and tcol.surrogate_expression:
      # Dialect-aware SK: parse DSL into Expr, then render via dialect
      expr = parse_surrogate_dsl(
        tcol.surrogate_expression,
        table_alias="s",  # Source/FROM alias in this SELECT
      )

    # 2) Foreign key hash column
    elif tcol.target_column_name in fk_expr_map:
      # FK: same DSL → AST treatment as SKs
      fk_dsl = fk_expr_map[tcol.target_column_name]
      expr = parse_surrogate_dsl(
        fk_dsl,
        table_alias="s",
      )

    # 3) Any other surrogate / technical expression (e.g. row_hash)
    elif tcol.surrogate_expression:
      # Technical / derived column: compute it at this layer (rawcore)
      expr = parse_surrogate_dsl(
        tcol.surrogate_expression,
        table_alias="s",
      )

    # 4) Normal column: derive from upstream or fall back to same-name column
    else:
      col_input = (
        tcol.input_links
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
        else tcol.target_column_name
      )

      expr = col(upstream_col_name, "s")

    logical.select_list.append(
      SelectItem(expr=expr, alias=tcol.target_column_name)
    )

  return logical


def _detect_stage_identity_mode(
  identity_flags: list[str | None],
) -> tuple[str, str | None]:
  """
  Decide whether we are in identity-mode or non-identity-mode.

  identity_flags contains, per input branch, either a concrete
  source_identity_id (string) or None.

  Rules:

    - If at least one branch has no identity id (None):
        -> non-identity-mode (ranking)
    - If all branches have a non-null identity id:
        -> identity-mode (no ranking needed)
    - If the list is empty:
        -> non-identity-mode (no multi-source case)
  """
  if not identity_flags:
    # No inputs or not a multi-source case
    return "non_identity", None

  # Identity-mode if *all* branches have a non-null id
  if all(flag is not None for flag in identity_flags):
    return "identity", None

  # At least one branch has no identity id -> enable ranking
  return "non_identity", None


def _attach_hidden_rank_ordinal(
  logical: LogicalSelect,
  ordinal: int | None,
  alias: str = "__src_rank_ord",
) -> LogicalSelect:
  """
  Ensure the given LogicalSelect has a hidden technical column used
  for ranking priority.

  The column is appended as the *last* select item with a literal
  integer value per branch, derived from SourceDatasetGroupMembership.

  It is *not* part of the TargetDataset's column metadata and will
  not be projected in the final Stage SELECT.
  """
  if ordinal is None:
    # Fallback priority if not set; you can decide another default
    literal_sql = "999999"
  else:
    literal_sql = str(int(ordinal))

  logical.select_list.append(
    SelectItem(
      expr=raw(literal_sql),
      alias=alias,
    )
  )

  return logical


def _strip_hidden_rank_ordinal_from_union(
  union_plan: LogicalUnion,
  hidden_alias: str = "__src_rank_ord",
) -> None:
  """
  Remove the hidden technical ranking column from all UNION branches.
  Used in identity-mode where we do not need ranking logic at all.
  """
  for sel in union_plan.selects:
    if not isinstance(sel, LogicalSelect):
      continue
    sel.select_list = [
      item for item in sel.select_list
      if item.alias != hidden_alias
    ]


def _get_stage_ranking_partition_exprs(
  target_dataset: "TargetDataset",
  table_alias: str,
) -> list[Expr]:
  """
  Return the list of expressions that define the *business key* for de-duplication.

  Typical pattern:
    - use one or more stage columns that represent the natural key
    - e.g. all TargetColumns with system_role='business_key'
  """
  bk_cols = (
    target_dataset.target_columns
    .filter(active=True, system_role="business_key")
    .order_by("ordinal_position", "id")
  )

  if not bk_cols:
    # Fallback: there is no BK
    raise ValueError(
      f"Dataset '{target_dataset}' has no business key columns defined for "
      f"multi-source Stage ranking."
    )

  exprs: list[Expr] = []
  for col_meta in bk_cols:
    exprs.append(col(col_meta.target_column_name, table_alias))

  return exprs


def _get_stage_ranking_order_exprs(
  target_dataset: "TargetDataset",
  table_alias: str,
) -> list[Expr]:
  """
  ORDER BY expressions for Stage ranking.

  With the hidden technical column '__src_rank_ord' attached per
  UNION branch, the ranking order is simply based on that column.

  Lower values mean higher priority.
  """
  return [
    col("__src_rank_ord", table_alias),
  ]


def _build_ranked_stage_union(
  target_dataset: "TargetDataset",
  union_plan: LogicalUnion,
  alias_all: str = "u_all",
  alias_ranked: str = "r",
) -> LogicalSelect:
  """
  Build ranked Stage SELECT using hidden __src_rank_ord.
  """
  inner_subquery = SubquerySource(select=union_plan, alias=alias_all)
  rank_select = LogicalSelect(from_=inner_subquery)

  tcols = (
    target_dataset.target_columns
    .filter(active=True)
    .order_by("ordinal_position", "id")
  )

  # u_all.<target_column> plus ROW_NUMBER()
  for col_meta in tcols:
    rank_select.select_list.append(
      SelectItem(
        expr=col(col_meta.target_column_name, alias_all),
        alias=col_meta.target_column_name,
      )
    )

  # PARTITION BY business key columns (Stage columns)
  partition_exprs = _get_stage_ranking_partition_exprs(
    target_dataset=target_dataset,
    table_alias=alias_all,
  )

  # ORDER BY hidden __src_rank_ord
  order_exprs = _get_stage_ranking_order_exprs(
    target_dataset=target_dataset,
    table_alias=alias_all,
  )

  rn_expr = row_number(
    partition_by=partition_exprs,
    order_by=order_exprs
  )

  rank_select.select_list.append(
    SelectItem(expr=rn_expr, alias="_rn")
  )

  # Outer SELECT: only real target columns, filter _rn = 1
  outer_subquery = SubquerySource(select=rank_select, alias=alias_ranked)
  final_select = LogicalSelect(from_=outer_subquery)

  for col_meta in tcols:
    final_select.select_list.append(
      SelectItem(
        expr=col(col_meta.target_column_name, alias_ranked),
        alias=col_meta.target_column_name,
      )
    )

  final_select.where = raw(f"{alias_ranked}._rn = 1")

  return final_select
