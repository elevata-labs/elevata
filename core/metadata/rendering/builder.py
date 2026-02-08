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

from typing import Optional, TYPE_CHECKING
import re

from metadata.rendering.logical_plan import (
  SourceTable,
  SelectItem,
  LogicalSelect,
  LogicalUnion,
  SubquerySource,
  Join,
  SubquerySource,
)
from metadata.generation.naming import build_surrogate_key_name
from metadata.rendering.expr import (
  Expr,
  ColumnRef,
  FuncCall,
  OrderByExpr,
  OrderByClause,
  RawSql,
  WindowSpec,
  WindowFunction,
)
from metadata.rendering.dsl import col, lit, raw, row_number, parse_surrogate_dsl

if TYPE_CHECKING:
  # Only for type hints, no runtime import -> no circular import
  from metadata.models import SourceDataset, TargetDataset, TargetColumn, TargetDatasetReference, TargetDatasetJoinPredicate

"""
Logical Query Builder for TargetDatasets.
Responsible for SK/FK hashing, lineage resolution,
and merging multiple inputs (UNION or single-path).
"""

# ------------------------------------------------------------------------------
# Regex patterns
# ------------------------------------------------------------------------------
COL_PATTERN = re.compile(r'col\(["\']([^"\']+)["\']\)')
EXPR_REF_PATTERN = re.compile(r"\{expr:([^}]+)\}")
_IDENT_RE = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\b")


def _collect_input_column_names(tcol) -> set[str]:
  """
  Collect upstream column names for this TargetColumn from its active input_links.
  These names are used to qualify bare identifiers in manual_expression.
  """
  names: set[str] = set()
  for link in (
    tcol.input_links
    .select_related("upstream_target_column", "source_column")
    .filter(active=True)
  ):
    if link.upstream_target_column_id:
      names.add(link.upstream_target_column.target_column_name)
    if link.source_column_id:
      names.add(link.source_column.source_column_name)
  return names


def _qualify_expr_identifiers(expr_text: str, *, col_names: set[str], alias: str) -> str:
  """
  Heuristically qualify bare column identifiers with `<alias>.`.
  - Only qualifies identifiers that match known input column names.
  - Does NOT touch:
      - already qualified identifiers (preceded by '.')
      - placeholders like {{ ... }}
      - quoted identifiers (best effort)
  This keeps authoring simple: `CONCAT(a, b)` instead of `CONCAT(s.a, s.b)`.
  """
  if not expr_text or not col_names:
    return expr_text

  # Do not attempt to qualify runtime placeholders.
  if "{{" in expr_text and "}}" in expr_text:
    return expr_text

  def repl(match: re.Match) -> str:
    token = match.group(0)
    if token not in col_names:
      return token
    i0 = match.start()
    # If already qualified (e.g. s.col), keep as-is.
    if i0 > 0 and expr_text[i0 - 1] == ".":
      return token
    # If in a quoted identifier (very rough guard), keep as-is.
    if i0 > 0 and expr_text[i0 - 1] in ('"', "'"):
      return token
    return f"{alias}.{token}"

  return _IDENT_RE.sub(repl, expr_text)

def _collect_input_column_alias_map(tcol, *, ds_alias_by_dataset_id: dict[int, str]) -> dict[str, str]:
  """
  Build a mapping { column_name -> table_alias } from TargetColumn input_links.

  We prefer upstream_target_column links because they provide a stable dataset_id.
  If a column name appears from multiple upstream datasets, we mark it ambiguous
  and do not qualify it automatically.
  """
  name_to_alias: dict[str, str] = {}
  ambiguous: set[str] = set()

  for link in (
    tcol.input_links
      .select_related("upstream_target_column", "source_column")
      .filter(active=True)
  ):
    if link.upstream_target_column_id:
      up_col = link.upstream_target_column
      up_ds_id = up_col.target_dataset_id
      alias = ds_alias_by_dataset_id.get(up_ds_id)
      if not alias:
        continue

      name = up_col.target_column_name
      if name in ambiguous:
        continue
      if name in name_to_alias and name_to_alias[name] != alias:
        name_to_alias.pop(name, None)
        ambiguous.add(name)
        continue
      name_to_alias[name] = alias

    # NOTE: We deliberately do NOT map source_column_name here because
    # bizcore/serving join path currently joins target datasets, not sources.

  return name_to_alias

def _qualify_expr_identifiers_by_map(expr_text: str, *, name_to_alias: dict[str, str]) -> str:
  """
  Qualify bare identifiers using a {name -> alias} map.
  - Keeps already qualified identifiers (preceded by '.')
  - Preserves placeholders like {{ ... }}
  - Conservative: only touches tokens present in name_to_alias
  """
  if not expr_text or not name_to_alias:
    return expr_text

  if "{{" in expr_text and "}}" in expr_text:
    return expr_text

  def repl(match: re.Match) -> str:
    token = match.group(0)
    alias = name_to_alias.get(token)
    if not alias:
      return token
    i0 = match.start()
    if i0 > 0 and expr_text[i0 - 1] == ".":
      return token
    if i0 > 0 and expr_text[i0 - 1] in ('"', "'"):
      return token
    return f"{alias}.{token}"

  return _IDENT_RE.sub(repl, expr_text)

def _pick_single_upstream_target_dataset(target_dataset) -> Optional["TargetDataset"]:
  """
  For non-stage layers, we treat multiple upstream TargetDatasets as unsupported
  (MVP) because it would require explicit join semantics.
  """
  upstreams = [
    inp.upstream_target_dataset
    for inp in (
      target_dataset.input_links
      .select_related("upstream_target_dataset", "upstream_target_dataset__target_schema")
      .filter(active=True, upstream_target_dataset__isnull=False)
    )
  ]
  if not upstreams:
    return None
  if len(upstreams) > 1:
    return "MULTIPLE"
  return upstreams[0]

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

  # 1) First: rewrite {expr:bk} placeholders (these represent the value side in our DSL).
  #    For FK generation, we want these to come from the child stage expressions.
  def _expr_repl(m):
    name = (m.group(1) or "").strip()
    if name in mapping:
      return mapping[name]
    return m.group(0)  # keep as-is if unknown

  rewritten = EXPR_REF_PATTERN.sub(_expr_repl, parent_expr_sql or "")

  # 2) Then: also support col("bk")-style expressions (older/newer variants).
  #    Here we keep the existing "count occurrences" logic.
  seen_counts: dict[str, int] = {}
  result: list[str] = []
  idx = 0

  while True:
    m = COL_PATTERN.search(rewritten, idx)
    if not m:
      result.append(rewritten[idx:])
      break

    result.append(rewritten[idx:m.start()])
    colname = m.group(1)

    if colname not in mapping:
      replacement = f'col("{colname}")'
    else:
      count = seen_counts.get(colname, 0) + 1
      seen_counts[colname] = count
      replacement = f'col("{colname}")' if count == 1 else mapping[colname]

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

  expr = _IDENT_RE.sub(repl, expr)
  return " ".join(expr.split())


def build_source_dataset_where_sql(
  source_dataset,
  *,
  source_alias: str = "s",
) -> str:
  """Build a fully qualified WHERE clause for SourceDataset extraction.

  Combines:
    - static_filter (always, if present)
    - increment_filter (only if source_dataset.incremental is True)

  Notes:
    - Uses qualify_source_filter() to conservatively qualify identifiers.
    - Preserves runtime placeholders like {{DELTA_CUTOFF}}.
  """
  parts: list[str] = []

  static_filter = (getattr(source_dataset, "static_filter", None) or "").strip()
  if static_filter:
    parts.append(
      qualify_source_filter(source_dataset, static_filter, source_alias=source_alias)
    )

  if getattr(source_dataset, "incremental", False):
    inc_filter = (getattr(source_dataset, "increment_filter", None) or "").strip()
    if inc_filter:
      parts.append(
        qualify_source_filter(source_dataset, inc_filter, source_alias=source_alias)
      )

  if not parts:
    return ""
  if len(parts) == 1:
    return parts[0]

  return " AND ".join(f"({p})" for p in parts)


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
    # Parent BK name as used in the parent's SK DSL.
    # The parent's SK expression is built from Stage lineage (e.g. businessentityid),
    # but the parent RawCore column may have been renamed (e.g. person_id).
    parent_bk_name = comp.to_column.target_column_name
    parent_stage_expr = _get_stage_expr_for_rawcore_col(comp.to_column)
    if parent_stage_expr:
      m = COL_PATTERN.fullmatch(parent_stage_expr.strip())
      if m:
        parent_bk_name = m.group(1)

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
    .exclude(lineage_origin="query_derived")
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

  # Apply SourceDataset extraction filters (static + optional incremental)
  where_sql = build_source_dataset_where_sql(source_dataset, source_alias="s")
  if where_sql:
    logical.where = raw(where_sql)

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
    .exclude(lineage_origin="query_derived")
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

def _qualify_expr(expr_sql: str, left_alias: str, right_alias: str, left_cols: set[str], right_cols: set[str], side: str) -> str:
  """
  Qualify bare identifiers in expr_sql.

  side:
    - "left": prefer left alias for ambiguous columns
    - "right": prefer right alias for ambiguous columns

  Rules (MVP):
    - keep already qualified identifiers (a.col)
    - if token in only one side's colset -> qualify with that side
    - if token in both -> qualify with preferred side
    - else -> leave as-is
  """
  if not expr_sql:
    return expr_sql

  def repl(m: re.Match) -> str:
    tok = m.group(0)
    start = m.start()

    # Already qualified?
    if start > 0 and expr_sql[start - 1] == ".":
      return tok

    in_left = tok in left_cols
    in_right = tok in right_cols

    if in_left and not in_right:
      return f"{left_alias}.{tok}"
    if in_right and not in_left:
      return f"{right_alias}.{tok}"
    if in_left and in_right:
      pref = left_alias if side == "left" else right_alias
      return f"{pref}.{tok}"

    return tok

  return _IDENT_RE.sub(repl, expr_sql)

def _get_active_target_colnames(upstream_ds: "TargetDataset") -> set[str]:
  return set(
    upstream_ds.target_columns
      .filter(active=True)
      .values_list("target_column_name", flat=True)
  )

def _render_join_predicate_sql(
  pred: "TargetDatasetJoinPredicate",
  left_alias: str,
  right_alias: str,
  left_cols: set[str],
  right_cols: set[str],
) -> str:
  """
  Render a single structured predicate into SQL.
  MVP rules:
    - left_expr is qualified against left input
    - right_expr/right_expr_2 qualified against right input
  """
  op = (pred.operator or "").strip().upper()

  left_sql = _qualify_expr((pred.left_expr or "").strip(), left_alias, right_alias, left_cols, right_cols, side="left")

  if op in ("IS NULL", "IS NOT NULL"):
    return f"{left_sql} {op}"

  if op == "BETWEEN":
    r1 = _qualify_expr((pred.right_expr or "").strip(), left_alias, right_alias, left_cols, right_cols, side="right")
    r2 = _qualify_expr((pred.right_expr_2 or "").strip(), left_alias, right_alias, left_cols, right_cols, side="right")
    return f"{left_sql} BETWEEN {r1} AND {r2}"

  right_sql = _qualify_expr((pred.right_expr or "").strip(), left_alias, right_alias, left_cols, right_cols, side="right")
  return f"{left_sql} {op} {right_sql}"

def _build_joined_select_for_target(
  target_dataset: "TargetDataset",
  inputs_qs,
) -> "LogicalSelect":
  """
  Build a joined LogicalSelect for Bizcore/Serving when multiple upstream inputs exist.

  MVP:
    - requires explicit TargetDatasetJoin rows (join_order defines chain)
    - predicates are AND combined
    - CROSS join => no predicates / no ON
  """

  # collect inputs that are upstream TargetDatasets
  upstream_inputs = [i for i in inputs_qs if i.upstream_target_dataset is not None]
  if len(upstream_inputs) <= 1:
    raise ValueError("_build_joined_select_for_target called with <= 1 upstream input")

  joins = list(
    target_dataset.joins
      .select_related(
        "left_input", "right_input",
        "left_input__upstream_target_dataset",
        "right_input__upstream_target_dataset",
      )
      .prefetch_related("predicates")
      .order_by("join_order", "id")
  )

  if not joins:
    raise ValueError(
      f"bizcore/serving dataset '{target_dataset.target_dataset_name}' has multiple upstream inputs "
      f"but no TargetDatasetJoin definitions."
    )

  # --- assign stable aliases per input (deterministic) ---
  # We keep it simple: i1, i2, i3 ... based on sorted input ids
  input_by_id = {i.id: i for i in upstream_inputs}
  input_ids_sorted = sorted(input_by_id.keys())

  alias_by_input_id: dict[int, str] = {}
  for idx, inp_id in enumerate(input_ids_sorted, start=1):
    alias_by_input_id[inp_id] = f"i{idx}"

  # --- determine base FROM input from first join's left_input ---
  base_input = joins[0].left_input
  if base_input_id := getattr(base_input, "id", None):
    pass
  else:
    raise ValueError("Join left_input has no id (unexpected)")

  base_alias = alias_by_input_id[base_input.id]
  base_upstream = base_input.upstream_target_dataset
  if base_upstream is None:
    raise ValueError("Join left_input has no upstream_target_dataset (unexpected)")

  logical = LogicalSelect(
    from_=SourceTable(
      schema=base_upstream.target_schema.schema_name,
      name=base_upstream.target_dataset_name,
      alias=base_alias,
    )
  )

  # --- add JOIN chain in order ---
  used_inputs: set[int] = {base_input.id}

  for j in joins:
    left_inp = j.left_input
    right_inp = j.right_input

    if left_inp.id not in used_inputs:
      # MVP chain constraint: each join must attach to already-built relation
      raise ValueError(
        f"Join chain is not connected at join_order={j.join_order}. "
        f"left_input {left_inp.id} not yet part of join tree."
      )

    right_up = right_inp.upstream_target_dataset
    if right_up is None:
      raise ValueError("Join right_input has no upstream_target_dataset (unexpected)")

    left_alias = alias_by_input_id[left_inp.id]
    right_alias = alias_by_input_id[right_inp.id]

    join_type = (j.join_type or "").strip().lower()

    on_expr = None
    if join_type != "cross":
      preds = list(j.predicates.all().order_by("ordinal_position", "id"))
      if not preds:
        raise ValueError(
          f"Join join_order={j.join_order} is not CROSS but has no predicates."
        )

      left_cols = _get_active_target_colnames(left_inp.upstream_target_dataset)
      right_cols = _get_active_target_colnames(right_inp.upstream_target_dataset)

      pred_sqls = [
        _render_join_predicate_sql(p, left_alias, right_alias, left_cols, right_cols)
        for p in preds
      ]
      on_sql = " AND ".join([s for s in pred_sqls if s])
      on_expr = raw(on_sql)
    else:
      # CROSS join must have no predicates (model validation should already enforce)
      on_expr = None

    logical.joins.append(
      Join(
        left_alias=left_alias,
        right=SourceTable(
          schema=right_up.target_schema.schema_name,
          name=right_up.target_dataset_name,
          alias=right_alias,
        ),
        on=on_expr,           # may be None for CROSS
        join_type=join_type,  # "left" / "inner" / "cross" ...
      )
    )

    used_inputs.add(right_inp.id)

  return logical


# ------------------------------------------------------------------------------
# Query graph compiler (QueryNode -> LogicalPlan)
# ------------------------------------------------------------------------------
def _build_plan_from_query_root(target_dataset: TargetDataset):
  """
  Compile a query graph (QueryNode tree) into a LogicalSelect / LogicalUnion.
  The query graph is owned by the TargetDataset; only the root is referenced from it.
  """
  query_root = getattr(target_dataset, "query_root", None)
  query_head = getattr(target_dataset, "query_head", None) or query_root
  if query_head is None:
    return None

  return _build_plan_for_query_node(query_head, required_input_columns=None)


def _build_plan_for_query_node(node, required_input_columns: set[str] | None = None):
  """
  Recursively compile a QueryNode into a logical plan.
  """
  node_type = getattr(node, "node_type", None)
  if not node_type:
    raise ValueError("QueryNode has no node_type")

  node_type = str(node_type).lower()

  if node_type == "select":
    # Reuse the dataset definition (joins/columns/manual expressions).
    td = node.target_dataset
    return _build_plan_from_dataset_definition(td, required_input_columns=required_input_columns)

  if node_type == "aggregate":
    return _build_aggregate_plan_for_node(node)
  
  if node_type == "window":
    return _build_window_plan_for_node(node, required_input_columns=required_input_columns)

  if node_type == "union":
    return _build_union_plan_for_node(node, required_input_columns=required_input_columns)

  raise ValueError(f"Unsupported QueryNode type: {node_type}")


def _build_aggregate_plan_for_node(node):
  """
  Build an aggregation as a wrapped SELECT:
    outer SELECT (group keys + measures) FROM (inner plan) u GROUP BY ...
  """
  agg = getattr(node, "aggregate", None)
  if agg is None:
    raise ValueError("Aggregate node has no aggregate details (node.aggregate is None)")

  # Aggregate requires certain input columns to exist in its inner plan:
  # - group key input columns
  # - measure input columns (if applicable)
  required: set[str] = set()
  for g in agg.group_keys.all().order_by("ordinal_position", "id"):
    in_name = (g.input_column_name or "").strip()
    if in_name:
      required.add(in_name)
  for m in agg.measures.all().order_by("ordinal_position", "id"):
    in_name = (m.input_column_name or "").strip()
    if in_name:
      required.add(in_name)

  inner_plan = _build_plan_for_query_node(agg.input_node, required_input_columns=required)

  # With dialect.render_plan() we can safely embed unions as subqueries.
  if not isinstance(inner_plan, (LogicalSelect, LogicalUnion)):
    raise TypeError(
      f"Aggregate input must compile to LogicalSelect or LogicalUnion, got {type(inner_plan).__name__}"
    )

  src = SubquerySource(select=inner_plan, alias="u")

  group_items: list[SelectItem] = []
  group_exprs = []
  for g in agg.group_keys.all().order_by("ordinal_position", "id"):
    in_name = (g.input_column_name or "").strip()
    if not in_name:
      continue
    out_name = (g.output_name or "").strip() or in_name
    expr = ColumnRef(table_alias="u", column_name=in_name)
    group_exprs.append(expr)
    group_items.append(SelectItem(expr=expr, alias=out_name))

  measure_items: list[SelectItem] = []
  for m in agg.measures.all().order_by("ordinal_position", "id"):
    out_name = (m.output_name or "").strip()
    if not out_name:
      continue

    fn = (m.function or "").strip().upper()
    in_name = (m.input_column_name or "").strip()
    delimiter = (getattr(m, "delimiter", "") or ",")
    order_by_obj = getattr(m, "order_by", None)

    # COUNT(*) support
    if fn == "COUNT" and not in_name:
      expr = FuncCall(name="COUNT", args=[RawSql("*")])

    else:
      arg = ColumnRef(table_alias="u", column_name=in_name) if in_name else None

      # DISTINCT handling: prefer a logical function name to keep dialect rendering clean.
      # Dialects can map COUNT_DISTINCT(x) -> COUNT(DISTINCT x), etc.
      if getattr(m, "distinct", False):
        if fn == "COUNT":
          expr = FuncCall(name="COUNT_DISTINCT", args=[arg] if arg else [])
        else:
          expr = FuncCall(name=f"{fn}_DISTINCT", args=[arg] if arg else [])
      else:
        # Optional deterministic ordering for STRING_AGG (and future order-sensitive aggregates).
        if fn == "STRING_AGG":
          args = [arg] if arg else []
          # Delimiter from model (default ',')
          args.append(lit(delimiter))
          if order_by_obj:
            items = []
            for it in order_by_obj.items.all().order_by("ordinal_position", "id"):
              dir_sql = (it.direction or "ASC").strip().upper()
              if dir_sql not in ("ASC", "DESC"):
                dir_sql = "ASC"
              items.append(
                OrderByExpr(
                  expr=ColumnRef(table_alias="u", column_name=it.input_column_name),
                  direction=dir_sql,
                )
              )
            if items:
              args.append(OrderByClause(items=items))
          expr = FuncCall(name="STRING_AGG", args=args)
        else:
          expr = FuncCall(name=fn, args=[arg] if arg else [])

    measure_items.append(SelectItem(expr=expr, alias=out_name))

  mode = (getattr(agg, "mode", "") or "").strip().lower()
  if mode != "global" and not group_exprs:
    raise ValueError("Aggregate mode is grouped but no group keys are defined.")
  if not measure_items:
    raise ValueError("Aggregate node has no measures.")

  return LogicalSelect(
    from_=src,
    select_list=(group_items + measure_items),
    group_by=group_exprs,
  )


def _build_union_plan_for_node(node, required_input_columns: set[str] | None = None):
  """
  Build a UNION / UNION ALL from fully compiled branch nodes.
  Each branch is aligned to the union output contract using a derived-table select.
  """
  un = getattr(node, "union", None)
  if un is None:
    raise ValueError("Union node has no union details (node.union is None)")

  # Map union mode -> LogicalUnion.union_type ("ALL" or "DISTINCT")
  mode = (getattr(un, "mode", "") or "").strip().lower()
  union_type = "ALL" if mode in ("union_all", "all") else "DISTINCT"

  out_cols = list(un.output_columns.all().order_by("ordinal_position", "id"))
  if not out_cols:
    raise ValueError("Union has no output_columns defined (contract required).")

  aligned_selects: list[LogicalSelect] = []
  branches = list(un.branches.all().order_by("ordinal_position", "id"))
  if not branches:
    raise ValueError("Union has no branches.")
  
  required_input_columns = required_input_columns or set()

  for b in branches:
    branch_plan = _build_plan_for_query_node(b.input_node, required_input_columns=required_input_columns)

    # Nested unions are now supported by embedding the branch plan as a subquery.
    if not isinstance(branch_plan, (LogicalSelect, LogicalUnion)):
      raise TypeError(
        f"Union branch must compile to LogicalSelect or LogicalUnion, got {type(branch_plan).__name__}"
      )

    src = SubquerySource(select=branch_plan, alias="b")

    mappings = {
      m.output_column_id: (m.input_column_name or "").strip()
      for m in b.mappings.select_related("output_column").all()
    }

    items: list[SelectItem] = []
    for oc in out_cols:
      in_name = mappings.get(oc.id, "")
      if not in_name:
        raise ValueError(
          f"Union branch {b.id} has no mapping for output column '{oc.output_name}'."
        )
      items.append(
        SelectItem(
          expr=ColumnRef(table_alias="b", column_name=in_name),
          alias=oc.output_name,
        )
      )

    aligned_selects.append(
      LogicalSelect(
        from_=src,
        select_list=items,
      )
    )

  return LogicalUnion(selects=aligned_selects, union_type=union_type)


def _build_window_select_items(window_node, upstream_alias: str) -> list[SelectItem]:
  items: list[SelectItem] = []

  for col in window_node.columns.all().order_by("ordinal_position", "id"):
    fn = (col.function or "").strip().upper()
    out_name = (col.output_name or "").strip()
    if not out_name:
      continue

    # Build function args (normalized)
    fn_args = []
    for a in col.args.all().order_by("ordinal_position", "id"):
      t = (a.arg_type or "").strip().lower()
      if t == "column":
        nm = (a.column_name or "").strip()
        if nm:
          fn_args.append(ColumnRef(table_alias=upstream_alias, column_name=nm))
      elif t == "int":
        if a.int_value is not None:
          fn_args.append(lit(a.int_value))
      elif t == "str":
        fn_args.append(lit(a.str_value or ""))

    partition_by_exprs = []
    if col.partition_by:
      for it in col.partition_by.items.all().order_by("ordinal_position", "id"):
        partition_by_exprs.append(
          ColumnRef(table_alias=upstream_alias, column_name=it.input_column_name)
        )

    order_by_exprs = []
    if col.order_by:
      for it in col.order_by.items.all().order_by("ordinal_position", "id"):
        direction = (it.direction or "ASC").strip().upper()
        if direction not in ("ASC", "DESC"):
          direction = "ASC"
        order_by_exprs.append(
          OrderByExpr(
            expr=ColumnRef(table_alias=upstream_alias, column_name=it.input_column_name),
            direction=direction,
          )
        )

    wf = WindowFunction(
      name=fn,
      args=fn_args,
      window=WindowSpec(
        partition_by=partition_by_exprs,
        order_by=order_by_exprs,
      ),
    )
    items.append(SelectItem(expr=wf, alias=out_name))

  return items


def _get_plan_output_column_names(plan) -> list[str]:
  """
  Return output column aliases of a compiled plan.
  Used to project pass-through columns when wrapping a plan as subquery.
  """
  if isinstance(plan, LogicalSelect):
    return [i.alias for i in plan.select_list if getattr(i, "alias", None)]

  if isinstance(plan, LogicalUnion):
    # UNION branches are aligned to the same contract; use first branch aliases.
    if not plan.selects:
      return []
    first = plan.selects[0]
    if isinstance(first, LogicalSelect):
      return [i.alias for i in first.select_list if getattr(i, "alias", None)]
    return []

  return []


def _build_window_plan_for_node(node, required_input_columns: set[str] | None = None):
  win = getattr(node, "window", None)
  if win is None:
    raise ValueError("Window node has no window details (node.window is None)")

  inner_plan = _build_plan_for_query_node(win.input_node, required_input_columns=required_input_columns)

  # Same safety rule as aggregate/union: only these can be embedded as subqueries.
  if not isinstance(inner_plan, (LogicalSelect, LogicalUnion)):
    raise TypeError(
      f"Window input must compile to LogicalSelect or LogicalUnion, got {type(inner_plan).__name__}"
    )

  upstream_alias = "u"
  src = SubquerySource(select=inner_plan, alias=upstream_alias)
  outer = LogicalSelect(from_=src, select_list=[])

  # 1) pass-through input projection (u.<col> AS <col>)
  existing_aliases: set[str] = set()
  for name in _get_plan_output_column_names(inner_plan):
    if name in existing_aliases:
      raise ValueError(f"Duplicate output column alias in window input: '{name}'")
    existing_aliases.add(name)
    outer.select_list.append(
      SelectItem(
        expr=ColumnRef(table_alias=upstream_alias, column_name=name),
        alias=name,
      )
    )

  # 2) add window expressions (using existing helper)
  win_items = _build_window_select_items(win, upstream_alias)
  for it in win_items:
    alias = (getattr(it, "alias", "") or "").strip()
    if not alias:
      raise ValueError("Window select item has empty alias.")
    if alias in existing_aliases:
      raise ValueError(
        f"Window output alias '{alias}' collides with an existing column in the input projection."
      )
    existing_aliases.add(alias)
  outer.select_list.extend(win_items)

  return outer


# ------------------------------------------------------------------------------
# Main builder: Build logical select for a target dataset
# ------------------------------------------------------------------------------
def build_logical_select_for_target(target_dataset: TargetDataset):
  """
  Build a vendor-neutral logical representation (LogicalSelect or LogicalUnion)
  for the given TargetDataset, using dataset- and column-level lineage.
  """

  # If a query graph root is defined, compile the query tree instead of the classic path.
  query_plan = _build_plan_from_query_root(target_dataset)
  if query_plan is not None:
    return query_plan

  return _build_plan_from_dataset_definition(target_dataset, required_input_columns=None)


def _build_plan_from_dataset_definition(
  target_dataset: TargetDataset,
  required_input_columns: set[str] | None = None,
):
  """
  Classic elevata path: build from TargetDataset definition (inputs, joins, columns, manual expressions).
  This function contains the previous body of build_logical_select_for_target().
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
  # 1c) BIZCORE / SERVING: MULTI-UPSTREAM via explicit joins
  # ==========================================================================
  if schema_short in ("bizcore", "serving"):
    upstream_inputs = [i for i in inputs_qs if i.upstream_target_dataset is not None]

    if len(upstream_inputs) > 1:
      # Use explicit join semantics from metadata
      logical = _build_joined_select_for_target(target_dataset, inputs_qs)

      # Build projection list based on target columns, but now FROM alias is not always "s"
      # For MVP, we keep existing column logic, but you likely want:
      #   - if TargetColumn has upstream_columns, pick the upstream alias based on that column's dataset
      # For now: if a column has exactly one upstream_target_column, we qualify against that upstream dataset alias.

      required_input_columns = required_input_columns or set()
      tcols_qs = (
        target_dataset.target_columns
          .filter(active=True)
          .order_by("ordinal_position", "id")
      )
      # Default: hide query-derived outputs to avoid double projection,
      # but keep explicitly required inputs (e.g. aggregate measure inputs).
      tcols = []
      for c in tcols_qs:
        name = (getattr(c, "target_column_name", "") or "").strip()
        if getattr(c, "lineage_origin", "") == "query_derived" and name not in required_input_columns:
          continue
        tcols.append(c)

      # helper: map upstream dataset id -> alias by inspecting the FROM and joins we built
      # base
      ds_alias: dict[int, str] = {}
      # from_
      # we need to find dataset id for from_ table: easiest is via joins metadata again
      # (MVP) build ds_alias from inputs_qs ids -> alias logic of helper:
      input_ids_sorted = sorted([i.id for i in upstream_inputs])
      for idx, inp_id in enumerate(input_ids_sorted, start=1):
        inp = next(i for i in upstream_inputs if i.id == inp_id)
        if inp.upstream_target_dataset_id:
          ds_alias[inp.upstream_target_dataset_id] = f"i{idx}"

      # project columns
      for tcol in tcols:
        col_input = (
          tcol.input_links
            .select_related("upstream_target_column", "source_column")
            .filter(active=True)
            .order_by("ordinal_position", "id")
            .first()
        )

        # Expressions:
        if tcol.manual_expression:
          # manual_expression may contain bare identifiers; qualify based on input_links.
          manual = (tcol.manual_expression or "").strip()
          name_to_alias = _collect_input_column_alias_map(
            tcol,
            ds_alias_by_dataset_id=ds_alias,
          )
          qualified = _qualify_expr_identifiers_by_map(
            manual,
            name_to_alias=name_to_alias,
          )
          expr = raw(qualified)

        elif col_input and col_input.upstream_target_column:
          up_col = col_input.upstream_target_column
          up_ds_id = up_col.target_dataset_id
          alias = ds_alias.get(up_ds_id)
          if not alias:
            # fallback to first FROM alias
            alias = getattr(logical.from_, "alias", "i1")
          expr = col(up_col.target_column_name, alias)
        else:
          # fallback: assume column exists on base alias
          base_alias = getattr(logical.from_, "alias", "i1")
          expr = col(tcol.target_column_name, base_alias)

        logical.select_list.append(SelectItem(expr=expr, alias=tcol.target_column_name))

      # Ensure required input columns exist in the projection even if they are not
      # part of the current dataset output schema (e.g. aggregate measure inputs).
      present = set()
      for si in (logical.select_list or []):
        alias = getattr(si, "alias", None)
        if alias:
          present.add(alias)
      for name in sorted(required_input_columns):
        if not name or name in present:
          continue
        # MVP: passthrough from base alias (i1) if available.
        base_alias = getattr(logical.from_, "alias", "i1")
        logical.select_list.append(SelectItem(expr=col(name, base_alias), alias=name))

      return logical

    # Single upstream: fall through into single-path resolution below,
    # but we should treat FROM as upstream dataset, not the target itself.
    if len(upstream_inputs) == 1:
      up = upstream_inputs[0].upstream_target_dataset
      from_schema = up.target_schema.schema_name
      from_table = up.target_dataset_name
      source_table = SourceTable(schema=from_schema, name=from_table, alias="s")
      logical = LogicalSelect(from_=source_table)
      # and then continue with your existing column projection logic (which uses alias "s")
      # easiest: jump into the existing code by not returning here

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

  # --------------------------------------------------------------------------
  # Non-stage curated layers: bizcore / serving (and any future non-stage layer)
  # --------------------------------------------------------------------------
  elif schema_short in ("bizcore", "serving"):
    utd = _pick_single_upstream_target_dataset(target_dataset)
    if utd == "MULTIPLE":
      raise ValueError(
        f"{schema_short} dataset '{target_dataset.target_dataset_name}' has multiple upstream "
        "TargetDatasetInputs. This requires explicit join semantics and is not supported yet."
      )
    if utd:
      from_schema, from_table = utd.target_schema.schema_name, utd.target_dataset_name

  # ==========================================================================
  # 3) Build the LogicalSelect for the single-path scenario
  # ==========================================================================
  source_table = SourceTable(schema=from_schema, name=from_table, alias="s")
  logical = LogicalSelect(from_=source_table)

  # If RAW reads directly from SourceDataset, apply extraction filters on the source table.
  if schema_short == "raw":
    src_input = next((inp for inp in inputs_qs if inp.source_dataset is not None), None)
    if src_input and src_input.source_dataset:
      sd = src_input.source_dataset
      where_sql = build_source_dataset_where_sql(sd, source_alias="s")
      if where_sql:
        logical.where = raw(where_sql)

  # If STAGE reads directly from SourceDataset (no RAW),
  # apply SourceDataset extraction filters on the source table.
  if schema_short == "stage":
    src_input = next((inp for inp in inputs_qs if inp.source_dataset is not None), None)
    if src_input and src_input.source_dataset:
      sd = src_input.source_dataset
      where_sql = build_source_dataset_where_sql(sd, source_alias="s")
      if where_sql:
        logical.where = raw(where_sql)

  required_input_columns = required_input_columns or set()
  tcols_qs = (
    target_dataset.target_columns
    .filter(active=True)
    .order_by("ordinal_position", "id")
  )
  # Default: hide query-derived columns in the base dataset-definition SELECT
  # to avoid double projection (e.g. window outputs).
  # Exception: if a downstream operator (e.g. AGGREGATE) explicitly requires
  # an input column, we must keep it even if it is marked query_derived.
  tcols = []
  for c in tcols_qs:
    name = (getattr(c, "target_column_name", "") or "").strip()
    if getattr(c, "lineage_origin", "") == "query_derived" and name not in required_input_columns:
      continue
    tcols.append(c)

  for tcol in tcols:
    # 1) Surrogate key column
    if tcol.system_role == "surrogate_key" and tcol.surrogate_expression:
      # Dialect-aware SK: parse DSL into Expr, then render via dialect
      expr = parse_surrogate_dsl(
        tcol.surrogate_expression,
        table_alias="s",  # Source/FROM alias in this SELECT
      )

    # 1b) RAW technical columns (render runtime placeholders, not source columns)
    elif schema_short == "raw" and tcol.system_role in ("load_run_id", "loaded_at"):
      if tcol.system_role == "load_run_id":
        expr = raw("{{ load_run_id }}")
      else:
        # loaded_at uses the same runtime timestamp placeholder used elsewhere
        expr = raw("{{ load_timestamp }}")

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
      # 0) Manual override: author does not need to type alias; we qualify from inputs.
      manual = (tcol.manual_expression or "").strip()
      if manual:
        input_names = _collect_input_column_names(tcol)
        qualified = _qualify_expr_identifiers(manual, col_names=input_names, alias="s")

        # Keep DSL text without braces or plain SQL as-is (renderer will handle raw text).
        if qualified.startswith("{{") and qualified.endswith("}}"):
          qualified = qualified[2:-2].strip()
        expr = raw(qualified)
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

  # Ensure required input columns exist even if they are not part of TargetColumns anymore.
  present = set()
  for si in (logical.select_list or []):
    alias = getattr(si, "alias", None)
    if alias:
      present.add(alias)
  for name in sorted(required_input_columns):
    if not name or name in present:
      continue
    logical.select_list.append(SelectItem(expr=col(name, "s"), alias=name))

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
    .exclude(lineage_origin="query_derived")
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
