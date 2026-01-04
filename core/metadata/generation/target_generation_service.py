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

from django.db import transaction
from django.db.models import Max
from typing import Dict, List
from copy import deepcopy

from metadata.models import (
  SourceDataset,
  TargetSchema,
  TargetDataset,
  TargetDatasetInput,
  TargetColumn,
  TargetColumnInput,
  TargetDatasetReference,
  TargetDatasetReferenceComponent,
)

from metadata.intent.landing import landing_required
from metadata.generation import naming, security
from metadata.generation.mappers import (
  TargetDatasetDraft,
  TargetColumnDraft,
  map_source_column_to_target_column, 
  build_surrogate_key_column_draft,
)
from metadata.services.rename_common import sync_key_former_names_for_rawcore_dataset


class TargetGenerationService:
  """
  Responsible for deriving TargetDatasets and TargetColumns
  from SourceDatasets + TargetSchemas (raw, stage, rawcore, serving, ...).
  """

  # Central registry for system-managed technical columns and their UI docs.
  # Note: For computed columns (e.g. row_hash), we may want to PATCH but NOT CREATE
  # to avoid missing expressions.
  TECH_COLUMN_REGISTRY = [
    # Common load tracking
    {
      "name": "load_run_id",
      "datatype": "STRING",
      "max_length": 64,
      "nullable": False,
      "system_role": "load_run_id",
      "layers": {"raw", "stage", "rawcore", "hist"},
      "description": "Identifier of the load run which produced this row.",
      "create_if_missing": True,
      "order": 900,
    },
    {
      "name": "loaded_at",
      "datatype": "TIMESTAMP",
      "max_length": None,
      "nullable": False,
      "system_role": "loaded_at",
      "layers": {"raw", "stage", "rawcore", "hist"},
      "description": "UTC timestamp when this row was written.",
      "create_if_missing": True,
      "order": 910,
    },

    # Computed column (created in build_dataset_bundle for rawcore, copied into hist)
    {
      "name": "row_hash",
      "datatype": "STRING",
      "max_length": 64,
      "nullable": False,
      "system_role": "row_hash",
      "layers": {"rawcore", "hist"},
      "description": "Deterministic hash over all non-key, non-technical attributes of the row.",
      "create_if_missing": False,
      "order": 500,
    },

    # History / SCD2 columns (created in ensure_hist_dataset_for_rawcore)
    {
      "name": "version_started_at",
      "datatype": "TIMESTAMP",
      "max_length": None,
      "nullable": False,
      "system_role": "version_started_at",
      "layers": {"hist"},
      "description": "Timestamp at which this version became active.",
      "create_if_missing": True,
      "order": 1000,
    },
    {
      "name": "version_ended_at",
      "datatype": "TIMESTAMP",
      "max_length": None,
      "nullable": True,
      "system_role": "version_ended_at",
      "layers": {"hist"},
      "description": "Timestamp at which this version was superseded or closed.",
      "create_if_missing": True,
      "order": 1010,
    },
    {
      "name": "version_state",
      "datatype": "STRING",
      "max_length": 20,
      "nullable": False,
      "system_role": "version_state",
      "layers": {"hist"},
      "description": "State of this version: 'new', 'changed', or 'deleted'.",
      "create_if_missing": True,
      "order": 1020,
    },
  ]

  @classmethod
  def _technical_column_names(cls) -> set[str]:
    # Single source for reserved technical identifiers.
    return {c["name"] for c in cls.TECH_COLUMN_REGISTRY}

  @classmethod
  def _tech_specs_for_layer(cls, layer: str) -> list[dict]:
    specs = [c for c in cls.TECH_COLUMN_REGISTRY if layer in c.get("layers", set())]
    specs.sort(key=lambda s: int(s.get("order", 0)))
    return specs


  def __init__(self, pepper: str | None = None):
    self.pepper = pepper or security.get_runtime_pepper()


  def get_relevant_source_datasets(self) -> List[SourceDataset]:
    """
    Returns all SourceDatasets that should be considered for auto-generation.
    Rule:
    - integrate == True
    - active == True
    """
    return list(
      SourceDataset.objects.filter(
        integrate=True,
        active=True,
      )
      .select_related("source_system")
      .prefetch_related("source_columns")
    )


  def _ensure_no_reserved_name_conflict(self, col_draft, reserved_names: set):
    """
    If a source-mapped column conflicts with a reserved technical column name,
    rename the target column draft by appending '_src'.
    """
    if col_draft.target_column_name in reserved_names:
      original = col_draft.target_column_name
      new_name = f"{original}_src"
      col_draft.target_column_name = new_name

      # also update ordinal to a unique value later
      col_draft.ordinal_position = None

      # Add optional description note for traceability
      desc = col_draft.description or ""
      col_draft.description = (
        f"{desc} (Renamed from '{original}' to avoid conflict with a reserved"
        " technical column name.)"
      )


  def _ensure_tech_columns(self, td):
    """
    Ensure system-managed technical columns exist for the given TargetDataset.
    Tech columns are appended at the end to keep ordinals stable.

    This method also PATCHES existing columns to keep their documentation consistent
    (description, max_length, nullable, role) and removes legacy 'system column' remarks.
    """
    layer = "hist" if td.target_dataset_name.endswith("_hist") else td.target_schema.short_name
    tech_specs = self._tech_specs_for_layer(layer)
    if not tech_specs:
      return

    max_ord = (
      td.target_columns.aggregate(Max("ordinal_position"))["ordinal_position__max"]
      or 0
    )

    for offset, spec in enumerate(tech_specs, start=1):
      col_name = spec["name"]

      # If the column exists, we always patch it.
      existing = TargetColumn.objects.filter(
        target_dataset=td,
        target_column_name=col_name,
      ).first()

      # Avoid creating computed columns (e.g. row_hash) here,
      # because creation must carry an expression and/or special lineage.
      if existing is None and not spec.get("create_if_missing", True):
        continue

      if existing is None:
        col = TargetColumn.objects.create(
          target_dataset=td,
          target_column_name=col_name,
          datatype=spec["datatype"],
          max_length=spec.get("max_length"),
          nullable=spec["nullable"],
          active=True,
          is_system_managed=True,
          system_role=spec["system_role"],
          ordinal_position=max_ord + offset,
          description=spec.get("description"),
          remark=None,
        )
        continue

      col = existing
      changed = False

      # Always keep system-managed tech columns active and tagged.
      if not col.active:
        col.active = True
        changed = True

      if not col.is_system_managed:
        col.is_system_managed = True
        changed = True

      if col.system_role != spec["system_role"]:
        col.system_role = spec["system_role"]
        changed = True

      # Keep type / constraints consistent
      if col.datatype != spec["datatype"]:
        col.datatype = spec["datatype"]
        changed = True

      if col.max_length != spec.get("max_length"):
        col.max_length = spec.get("max_length")
        changed = True

      if col.nullable != spec["nullable"]:
        col.nullable = spec["nullable"]
        changed = True

      # Unify UI documentation
      if col.description != spec.get("description"):
        col.description = spec.get("description")
        changed = True

      # Remove legacy remark like "elevata system column" / "system column"
      if col.remark:
        col.remark = None
        changed = True

      if changed:
        col.save()


  def _build_row_hash_expression(self, all_columns, target_schema) -> str:
    """
    Build a dialect-agnostic DSL expression for row_hash, based on all
    non-key, non-technical columns of a dataset.

    Pattern (simplified):

      HASH256(
        CONCAT_WS(comp_sep,
          CONCAT(COALESCE({expr:col1}, null_token)),
          CONCAT(COALESCE({expr:col2}, null_token)),
          ...,
          'pepper'
        )
      )
    """
    EXCLUDED_ROLES_FOR_ROW_HASH = {
      "surrogate_key",
      "business_key",
    }

    dialect_neutral_cols: list[str] = []

    for col in all_columns:
      name = col.target_column_name

    tech_names = self._technical_column_names()

    for col in all_columns:
      role = getattr(col, "system_role", None)
      name = col.target_column_name

      # Exclude keys (semantic rule)
      if role in EXCLUDED_ROLES_FOR_ROW_HASH:
        continue

      # Exclude technical columns (except row_hash itself)
      if (
        (getattr(col, "is_system_managed", False) or name in tech_names)
        and role != "row_hash"
      ):
        continue

      dialect_neutral_cols.append(name)

    if not dialect_neutral_cols:
      return "HASH256('no_columns')"

    null_token = getattr(target_schema, "surrogate_key_null_token", "null")
    comp_sep = getattr(target_schema, "surrogate_key_component_separator", "|")

    value_exprs = [
      f"COALESCE({{expr:{name}}}, '{null_token}')"
      for name in dialect_neutral_cols
    ]

    args = [f"'{comp_sep}'"] + value_exprs

    inner = ", ".join(args)
    return f"HASH256(CONCAT_WS({inner}))"


  def get_target_schemas_in_scope(self) -> List[TargetSchema]:
      """
      Only return schemas that are configured for auto-generation
      and ensure a stable generation order:

      1) raw     (closest to source)
      2) stage   (depends on raw if present)
      3) rawcore (depends on stage)
      4) all other generate_layer=True schemas afterwards
      """

      qs = TargetSchema.objects.filter(generate_layer=True)

      # We sort in Python so we can define a custom logical order.
      order_map = {
          "raw": 10,
          "stage": 20,
          "rawcore": 30,
          # later: "bizcore": 40, "serving": 50, ...
      }

      schemas = list(qs)
      schemas.sort(
          key=lambda s: (
              order_map.get(s.short_name, 100),
              s.short_name,
          )
      )
      return schemas


  def _determine_incremental_source(self, src_list):
    """
    Decide which SourceDataset should be stored as incremental_source
    on the TargetDataset.

    Rules:
    - consider only SourceDatasets with incremental=True
    - if any such dataset has a SourceDatasetGroupMembership with
      is_primary_system=True -> prefer that one
    - otherwise fall back to the first incremental dataset
    - if none is incremental -> return None
    """
    # 1) collect incremental candidates
    incremental_candidates = [
      ds for ds in src_list
      if getattr(ds, "incremental", False)
    ]
    if not incremental_candidates:
      return None

    # 2) prefer those whose group membership is marked as primary system
    for ds in incremental_candidates:
      # dataset_groups is the related_name on SourceDatasetGroupMembership
      membership_qs = getattr(ds, "dataset_groups", None)
      if membership_qs is None:
        continue

      primary_membership = membership_qs.filter(is_primary_system=True).first()
      if primary_membership is not None:
        return ds

    # 3) fallback: first incremental source in the bucket
    return incremental_candidates[0]


  def build_dataset_bundle(self, source_dataset, target_schema):
    """
    Build a draft TargetDataset + TargetColumns for a single source_dataset in a given target_schema.

    - We do not persist here. That's apply_all().
    - We do not do multi-source merge here. That's handled by apply_all() bucketing.
    - We DO generate a surrogate key column first if the schema requires surrogate keys.
    """

    # 1. Compute physical name for this dataset in this schema
    physical_name = naming.build_physical_dataset_name(
      target_schema=target_schema,
      source_dataset=source_dataset,
    )

    # 2. Dataset draft
    dataset_draft = TargetDatasetDraft(
      target_schema_id=target_schema.id,
      target_dataset_name=physical_name,
      description=getattr(source_dataset, "description", None),
      is_system_managed=getattr(target_schema, "is_system_managed", False),
      surrogate_key_column_name=None,
    )

    # 3. Collect source columns from this dataset
    #    IMPORTANT: use your actual related_name, not sourcecolumn_set.
    #    Fallback defensively if attribute not present.
    if hasattr(source_dataset, "source_columns"):
      src_cols_qs = source_dataset.source_columns.all()
    else:
      # last resort fallback if model actually DOES use Django default reverse name
      src_cols_qs = source_dataset.sourcecolumn_set.all()

    # always filter integrate=True
    src_cols_qs = src_cols_qs.filter(integrate=True).order_by("ordinal_position")

    mapped_columns: list[TargetColumnDraft] = []
    for src_col in src_cols_qs:
      col_draft = map_source_column_to_target_column(src_col, ordinal=0)  # we'll assign ordinals after
      mapped_columns.append(col_draft)

    # 4. Identify natural key columns (business key columns)
    natural_key_cols = [c for c in mapped_columns if c.system_role=="business_key"]
    natural_key_colnames = [c.target_column_name for c in natural_key_cols]

    column_drafts: list[TargetColumnDraft] = []
    ordinal_counter = 1


    # 4b. Auto-add source_identity_id as artificial BK column
    #     for STAGE and RAWCORE if any group membership has an identity.
    if target_schema.short_name in ("stage", "rawcore"):
      memberships = getattr(source_dataset, "dataset_groups", None)
      has_identity = False
      if memberships is not None:
        for m in memberships.all():
          ident = getattr(m, "source_identity_id", None)
          if ident:
            has_identity = True
            break

      if has_identity:
        already_present = any(
          c.target_column_name == "source_identity_id"
          for c in natural_key_cols
        )

        if not already_present:
          identity_draft = TargetColumnDraft(
            target_column_name="source_identity_id",
            datatype="STRING",
            max_length=30,
            decimal_precision=None,
            decimal_scale=None,
            nullable=False,
            system_role="business_key",
            artificial_column=True,
            lineage_origin="source_identity",
            source_column_id=None,
            ordinal_position=0,
            surrogate_expression=None,
          )

          # Identity should be on first position in BK (Ordinal 1 in STAGE/RAWCORE),
          # however, the hash order stays alphabetic.
          natural_key_cols.insert(0, identity_draft)
          natural_key_colnames = [
            c.target_column_name for c in natural_key_cols
          ]

    # 4c. Prevent naming conflicts with technical / SK column names
    reserved_names = set(self._technical_column_names())
    reserved_names.add(f"{dataset_draft.target_dataset_name}_key")

    for col in mapped_columns:
      self._ensure_no_reserved_name_conflict(col, reserved_names)

    # 5. If this schema wants surrogate keys, create surrogate key column FIRST
    if self.schema_requires_surrogate_key(target_schema):
      pepper = self.pepper
      surrogate_col = build_surrogate_key_column_draft(
        target_dataset_name=dataset_draft.target_dataset_name,
        natural_key_colnames=natural_key_colnames,
        pepper=pepper,
        ordinal=ordinal_counter,
        null_token=target_schema.surrogate_key_null_token,
        pair_sep=target_schema.surrogate_key_pair_separator,
        comp_sep=target_schema.surrogate_key_component_separator,
      )
      surrogate_col.ordinal_position = ordinal_counter

      column_drafts.append(surrogate_col)
      dataset_draft.surrogate_key_column_name = surrogate_col.target_column_name
      ordinal_counter += 1    

    # 6. Add natural key columns (business key columns) next
    for col in natural_key_cols:
      col.ordinal_position = ordinal_counter
      column_drafts.append(col)
      ordinal_counter += 1

    # 7. Add the rest of the columns
    for col in mapped_columns:
      if col in natural_key_cols:
        continue
      col.ordinal_position = ordinal_counter
      column_drafts.append(col)
      ordinal_counter += 1

    # 8. Add row_hash only for rawcore datasets
    if target_schema.short_name == "rawcore":
      row_hash_expr = self._build_row_hash_expression(
        column_drafts,
        target_schema,
      )

      row_hash_col = TargetColumnDraft(
        target_column_name="row_hash",
        datatype="STRING",
        max_length=64,
        decimal_precision=None,
        decimal_scale=None,
        nullable=False,
        system_role="row_hash",
        artificial_column=True,
        lineage_origin="technical",
        source_column_id=None,
        ordinal_position=ordinal_counter,
        surrogate_expression=row_hash_expr,
      )
      column_drafts.append(row_hash_col)
      ordinal_counter += 1

    return {
      "dataset": dataset_draft,
      "columns": column_drafts,
    }


  def schema_requires_surrogate_key(self, target_schema) -> bool:
    """
    Check TargetSchema configuration to determine if this layer
    requires a surrogate key.
    """
    # If the schema itself defines it, use that
    return getattr(target_schema, "surrogate_keys_enabled", False)
  

  def get_eligible_source_datasets_for_schema(self, target_schema):
    """
    Return SourceDatasets that should feed this target_schema.

    - Basis: only integrate=True, active=True (get_relevant_source_datasets)
    - RAW (consolidate_groups == False):
        only datasets, for which actually a RAW object should be generated
        (landing_required)
    - STAGE / RAWCORE:
        all relevant Datasets (integrate+active), if raw exists or not
    """
    all_ds = self.get_relevant_source_datasets()

    if not getattr(target_schema, "consolidate_groups", False):
        # RAW / raw-like layer
        return [
            ds for ds in all_ds
            if landing_required(ds)
        ]

    # STAGE / RAWCORE
    return all_ds

  def preview_all(self) -> List[Dict[str, object]]:
    """
    Build a full dry-run preview of what WOULD be generated.
    Output is a list of entries, each describing (source_dataset x target_schema).
    """
    results = []
    src_datasets = self.get_relevant_source_datasets()
    schemas = self.get_target_schemas_in_scope()

    for src_ds in src_datasets:
      for schema in schemas:
        # Skip if this layer shouldn't be generated
        if not schema.generate_layer:
          continue

        # Layer specific additional rules (only if necessary)
        # Example: there are special integration rules for raw layer
        if schema.short_name == "raw" and not landing_required(src_ds):
          continue

        # Create bundle
        bundle = self.build_dataset_bundle(
          source_dataset=src_ds,
          target_schema=schema,
        )

        results.append({
          "source_dataset_id": src_ds.id,
          "source_dataset_name": src_ds.source_dataset_name,
          "target_schema": schema.short_name,
          "preview": bundle,
        })

    return results

  # ------------------------------------------------------------
  # Lineage helpers
  # ------------------------------------------------------------
  def build_lineage_key_for_bucket(self, target_schema, src_list):
    """
    Build a stable technical lineage key for a (schema, source-dataset-bucket) combination.

    We use the target_schema.pk and the sorted list of source_dataset.pk values.
    This survives renames of target_dataset_name and keeps the grouping stable.
    """
    schema_part = str(target_schema.pk)
    src_ids = sorted(ds.pk for ds in src_list)
    src_part = ",".join(str(pk) for pk in src_ids)
    return f"{schema_part}:{src_part}"

  def _bucket_source_datasets(self, eligible_source_datasets, target_schema):
    """
    Group SourceDatasets by the physical table name they should land in
    (e.g. stg_aw_person, raw_aw1_product, rc_aw_product_model, ...).
    """
    buckets = {}
    for src_ds in eligible_source_datasets:
      physical_name = naming.build_physical_dataset_name(
        target_schema=target_schema,
        source_dataset=src_ds,
      )
      buckets.setdefault(physical_name, []).append(src_ds)
    return buckets

  def _determine_combination_mode(self, target_schema, src_list):
    """
    Decide whether this dataset conceptually represents a single source or a union of many.
    """
    if target_schema.short_name == "rawcore":
      return "single"
    if target_schema.short_name == "stage":
      return "union" if len(src_list) > 1 else "single"
    # raw + others
    return "union" if len(src_list) > 1 else "single"
  

  def _determine_incremental_strategy(self, target_schema, src_list):
    """
    Decide the default incremental strategy for a generated TargetDataset.

    Rules:
    - If none of the source datasets in this bucket is incremental -> 'full'
    - If at least one source dataset is incremental                -> use
      target_schema.incremental_strategy_default (fallback to 'full').
    """
    # Any incremental source in this bucket?
    any_incremental = any(getattr(ds, "incremental", False) for ds in src_list)
    if not any_incremental:
      return "full"

    # Source is incremental -> use schema-level default for this layer
    default = getattr(target_schema, "incremental_strategy_default", None)
    return default or "full"


  def _get_or_create_target_dataset(self, target_schema, dataset_draft, src_list, combination_mode):
    """
    Find or create the TargetDataset for this bucket, based on lineage_key
    (schema + source_dataset IDs), with a fallback to name-based matching.

    Robust against legacy rows (without lineage_key) and duplicate calls.
    """
    lineage_key = self.build_lineage_key_for_bucket(target_schema, src_list)
    incremental_strategy = self._determine_incremental_strategy(target_schema, src_list)
    incremental_source = self._determine_incremental_source(src_list)

    historize = getattr(target_schema, "default_historize", False)

    if incremental_strategy == "full":
      incremental_source = None

    # 1) Preferred lookup: by lineage_key (excluding *_hist in rawcore)
    qs = TargetDataset.objects.filter(
      target_schema=target_schema,
      lineage_key=lineage_key,
    )
    if target_schema.short_name == "rawcore":
      qs = qs.exclude(target_dataset_name__endswith="_hist")

    target_dataset_obj = qs.first()
    created = False

    # 2) If nothing by lineage_key: try get_or_create by name
    if target_dataset_obj is None:
      defaults = {
        "description": dataset_draft.description,
        "is_system_managed": dataset_draft.is_system_managed,
        "combination_mode": combination_mode,
        "lineage_key": lineage_key,
        "incremental_strategy": incremental_strategy,
        "incremental_source": incremental_source,
        "historize": historize,
      }
      target_dataset_obj, created = TargetDataset.objects.get_or_create(
        target_schema=target_schema,
        target_dataset_name=dataset_draft.target_dataset_name,
        defaults=defaults,
      )
    else:
      # 3) Existing dataset found by lineage_key: update mutable fields
      changed = False
      if target_dataset_obj.description != dataset_draft.description:
        target_dataset_obj.description = dataset_draft.description
        changed = True
      if target_dataset_obj.is_system_managed != dataset_draft.is_system_managed:
        target_dataset_obj.is_system_managed = dataset_draft.is_system_managed
        changed = True
      if target_dataset_obj.combination_mode != combination_mode:
        target_dataset_obj.combination_mode = combination_mode
        changed = True
      if not target_dataset_obj.lineage_key:
        target_dataset_obj.lineage_key = lineage_key
        changed = True
      if getattr(target_schema, "is_system_managed", False):
        if target_dataset_obj.incremental_strategy != incremental_strategy:
          target_dataset_obj.incremental_strategy = incremental_strategy
          changed = True
        if target_dataset_obj.incremental_source != incremental_source:
          target_dataset_obj.incremental_source = incremental_source
          changed = True
      if changed:
        target_dataset_obj.save()

    # 4) If we found per name (created == False), lineage_key
    #    or the Incremental columns cannot fit
    if not created:
      changed = False
      if target_dataset_obj.lineage_key != lineage_key:
        target_dataset_obj.lineage_key = lineage_key
        changed = True
      if getattr(target_schema, "is_system_managed", False):
        if target_dataset_obj.incremental_strategy != incremental_strategy:
          target_dataset_obj.incremental_strategy = incremental_strategy
          changed = True
        if target_dataset_obj.incremental_source != incremental_source:
          target_dataset_obj.incremental_source = incremental_source
          changed = True
      if changed:
        target_dataset_obj.save()

    return target_dataset_obj, created


  @transaction.atomic
  def ensure_hist_dataset_for_rawcore(self, rawcore_td: TargetDataset) -> TargetDataset | None:
    """
    Ensure that a *_hist TargetDataset exists for the given rawcore dataset and
    is schema-synced to its columns.

    - Hist dataset lives in the same TargetSchema as the rawcore dataset
    - Name pattern: <rawcore_name>_hist
    - First column: <rawcore_name>_hist_key (surrogate key for the history row)
    - Then: a 1:1 copy of all rawcore columns (same order)
    - Finally: version_started_at, version_ended_at, version_state, load_run_id

    Dataset-level link is via lineage_key so that renames of the rawcore
    dataset name are propagated safely without creating a second hist dataset.

    This implementation is deliberately defensive: it removes any existing
    columns for the hist dataset before rebuilding them to avoid uniqueness
    conflicts on (target_dataset, target_column_name) and (target_dataset, ordinal_position).
    """
    schema = rawcore_td.target_schema

    # Only rawcore datasets participate in history tracking
    if schema.short_name != "rawcore":
      return None

    # Respect per-dataset historization flag
    if not rawcore_td.historize:
      return None

    # Do not accidentally create hist for hist datasets themselves
    if rawcore_td.target_dataset_name.endswith("_hist"):
      return None

    hist_name = f"{rawcore_td.target_dataset_name}_hist"
    lineage_key = rawcore_td.lineage_key

    # Locate hist dataset by lineage_key if available
    hist_td: TargetDataset | None = None
    if lineage_key:
      hist_td = (
        TargetDataset.objects
        .filter(
          target_schema=schema,
          lineage_key=lineage_key,
          target_dataset_name__endswith="_hist",
        )
        .first()
      )

    if hist_td is None:
      # No hist dataset for this lineage yet -> create it with the current name
      defaults = {
        "description": f"History table for {rawcore_td.target_dataset_name}",
        "handle_deletes": False,
        "historize": False,  # no history of history
        "is_system_managed": True,
      }
      if lineage_key:
        defaults["lineage_key"] = lineage_key

      hist_td, _ = TargetDataset.objects.get_or_create(
        target_schema=schema,
        target_dataset_name=hist_name,
        defaults=defaults,
      )

      if lineage_key and hist_td.lineage_key != lineage_key:
        hist_td.lineage_key = lineage_key
        hist_td.save(update_fields=["lineage_key"])

    else:
      # Hist dataset exists for this lineage -> keep name + lineage in sync
      changed = False
      if hist_td.target_dataset_name != hist_name:
        hist_td.target_dataset_name = hist_name
        changed = True
      if lineage_key and hist_td.lineage_key != lineage_key:
        hist_td.lineage_key = lineage_key
        changed = True
      if hist_td.historize:
        hist_td.historize = False
        changed = True
      if hist_td.handle_deletes:
        hist_td.handle_deletes = False
        changed = True
      if not hist_td.is_system_managed:
        hist_td.is_system_managed = True
        changed = True
      if changed:
        hist_td.save()

    # ------------------------------------------------------------
    # Guardrail: hist former_names must never contain non-hist names.
    # Otherwise planner may rename base table -> hist when hist is missing.
    # ------------------------------------------------------------
    fn = list(getattr(hist_td, "former_names", None) or [])
    fn_clean = [n for n in fn if isinstance(n, str) and n.strip().lower().endswith("_hist")]
    if fn_clean != fn:
      hist_td.former_names = fn_clean
      hist_td.save(update_fields=["former_names"])

    # Remove any reference components that point to hist columns,
    # otherwise PROTECT will block deleting those columns.
    TargetDatasetReferenceComponent.objects.filter(
      from_column__target_dataset=hist_td,
    ).delete()

    # If there also exists a to_column and hist appears in that:
    TargetDatasetReferenceComponent.objects.filter(
      to_column__target_dataset=hist_td,
    ).delete()

    TargetDatasetReference.objects.filter(
      referencing_dataset=hist_td,
    ).delete()

    TargetDatasetReference.objects.filter(
      referenced_dataset=hist_td,
    ).delete()

    TargetColumnInput.objects.filter(
      upstream_target_column__target_dataset=hist_td,
    ).delete()

    # Now it is safe to delete all hist-columns
    TargetColumn.objects.filter(target_dataset=hist_td).delete()

    next_ord = 1

    # Small helper to enforce "delete then create" per name,
    # even if there are rests for any reason.
    def _create_hist_column(
      name: str,
      datatype: str,
      nullable: bool = True,
      surrogate_key: bool = False,
      max_length: int | None = None,
      decimal_precision=None,
      decimal_scale=None,
      description: str | None = None,
      business_key: bool = False,
      surrogate_expression: str | None = None,
      system_role: str = "",
      former_names: list[str] | None = None,
    ) -> TargetColumn:

      nonlocal next_ord
      # Ensure there is really no column with this name left
      TargetColumn.objects.filter(
        target_dataset=hist_td,
        target_column_name=name,
      ).delete()

      kwargs = {
        "target_dataset": hist_td,
        "target_column_name": name,
        "ordinal_position": next_ord,
        "datatype": datatype,
        "nullable": nullable,
        "is_system_managed": True,
      }
      if max_length is not None:
        kwargs["max_length"] = max_length
      if decimal_precision is not None:
        kwargs["decimal_precision"] = decimal_precision
      if decimal_scale is not None:
        kwargs["decimal_scale"] = decimal_scale
      if description is not None:
        kwargs["description"] = description
      if surrogate_key:
        kwargs["system_role"] = "surrogate_key"
      if business_key:
        kwargs["system_role"] = "business_key"
      if system_role:
        kwargs["system_role"] = system_role

      if surrogate_expression is not None:
        kwargs["surrogate_expression"] = surrogate_expression

      # Preserve rename lineage for planner-driven RENAME_COLUMN.
      # Important: copy to avoid sharing a mutable list between ORM instances.
      if former_names is not None:
        cleaned = [n for n in former_names if isinstance(n, str) and n.strip()]
        # Keep as-is (including possible "current name" duplicates), planner can dedupe.
        kwargs["former_names"] = deepcopy(cleaned)

      col = TargetColumn.objects.create(**kwargs)
      next_ord += 1
      return col

    # Copy all rawcore columns (including entity SK) with lineage links
    rawcore_td_id = rawcore_td.id  # keep it stable

    rc_sk_col = TargetColumn.objects.filter(
      target_dataset_id=rawcore_td_id,
      system_role="surrogate_key",
    ).first()

    rawcore_cols = list(
      TargetColumn.objects.filter(target_dataset_id=rawcore_td_id)
      .order_by("ordinal_position")
    )

    hist_sk_expression = None
    # Hist "business key" intentionally uses the Rawcore surrogate key as the entity identifier.
    # This keeps history keys narrow and stable, even if the rawcore business key is composite.
    if rawcore_td.historize and self.schema_requires_surrogate_key(schema) and rc_sk_col is None:
      raise ValueError(
        f"Rawcore dataset '{rawcore_td.target_dataset_name}' has surrogate_keys_enabled=True "
        "but no TargetColumn with system_role='surrogate_key' was found."
      )
        
    if rc_sk_col is not None and self.schema_requires_surrogate_key(schema):
      natural_key_colnames = [
        rc_sk_col.target_column_name,
        "version_started_at",
      ]

      # build_surrogate_key_column_draft knows Pepper & Separators
      pepper = self.pepper
      sk_draft = build_surrogate_key_column_draft(
        target_dataset_name=hist_name,
        natural_key_colnames=natural_key_colnames,
        pepper=pepper,
        ordinal=1,
        null_token=schema.surrogate_key_null_token,
        pair_sep=schema.surrogate_key_pair_separator,
        comp_sep=schema.surrogate_key_component_separator,
      )
      hist_sk_expression = sk_draft.surrogate_expression

    hist_sk_name = f"{hist_name}_key"
    _create_hist_column(
      name=hist_sk_name,
      datatype="STRING",
      nullable=False,
      surrogate_key=True,
      max_length=64,
      description=f"Surrogate key for history rows of {rawcore_td.target_dataset_name}.",
      surrogate_expression=hist_sk_expression,
      system_role="surrogate_key",
      former_names=None,
    )

    for rc_col in rawcore_cols:
      role = rc_col.system_role or ""
      if role == "surrogate_key":
        role = "entity_key"

      # Carry over former_names so hist can also RENAME_COLUMN instead of ADD_COLUMN
      # after multiple renames in base.
      rc_former_names = list(getattr(rc_col, "former_names", None) or [])

      hist_col = _create_hist_column(
        name=rc_col.target_column_name,
        datatype=rc_col.datatype,
        nullable=True,
        max_length=rc_col.max_length,
        decimal_precision=rc_col.decimal_precision,
        decimal_scale=rc_col.decimal_scale,
        description=rc_col.description,
        system_role=role,
        former_names=rc_former_names,
      )
      TargetColumnInput.objects.create(
        target_column=hist_col,
        upstream_target_column=rc_col,
      )

    # Technical versioning columns (append after copying rawcore columns)
    # NOTE: row_hash is already copied from rawcore, so we must NOT create it here.
    hist_tail_roles = {
      "version_started_at",
      "version_ended_at",
      "version_state",
      "load_run_id",
      "loaded_at",
    }

    for spec in self._tech_specs_for_layer("hist"):
      role = (spec.get("system_role") or "").strip()
      if role not in hist_tail_roles:
        continue

      _create_hist_column(
        name=spec["name"],
        datatype=spec["datatype"],
        nullable=spec["nullable"],
        max_length=spec.get("max_length"),
        description=spec.get("description"),
        system_role=role,
        former_names=None,
      )

    # Ensure key-column former_names survive the defensive hist rebuild
    sync_key_former_names_for_rawcore_dataset(base_td=rawcore_td, hist_td=hist_td)

    return hist_td


  def _ensure_surrogate_key_draft_names(self, target_dataset_obj, column_drafts):
    """
    Ensure the surrogate key draft uses the current dataset name.
    Only touch the *actual* dataset surrogate key, not arbitrary *_key columns.
    """
    expected_sk_name = f"{target_dataset_obj.target_dataset_name}_key"

    for col_draft in column_drafts:
      role = (getattr(col_draft, "system_role", "") or "").strip()
      is_sk = (role == "surrogate_key")

      # Narrow fallback: only treat as SK if it already matches the expected SK name
      # (covers older drafts or cases where role wasn't set, without misclassifying customer_key etc.)
      if not is_sk:
        is_sk = (getattr(col_draft, "target_column_name", "") == expected_sk_name)

      if is_sk:
        col_draft.target_column_name = expected_sk_name


  def _resolve_role_for_source_dataset(self, src_ds):
    """
    Decide the logical role of a source dataset within a bucket (primary/enrichment...).
    """
    membership = src_ds.dataset_groups.first()
    if membership and membership.is_primary_system:
      return "primary"
    return "enrichment"

  def _sync_dataset_inputs(self, target_dataset_obj, target_schema, src_list, representative):
    """
    Maintain TargetDatasetInput rows for a given target dataset
    based on the current target_schema (raw, stage, rawcore).

    Rules:
    - raw:   inputs = SourceDatasets
    - stage: if generate_raw_table then upstream = raw, else direct SourceDatasets
    - rawcore: upstream = stage
    """
    from metadata.models import TargetDatasetInput, TargetSchema as TS  # avoid circulars at import time

    # Clear previous inputs for a clean rebuild
    TargetDatasetInput.objects.filter(target_dataset=target_dataset_obj).delete()

    if target_schema.short_name == "rawcore":
      # rawcore: exactly one upstream TargetDataset (the stage dataset)
      try:
        stage_schema = TS.objects.get(short_name="stage")
      except TS.DoesNotExist:
        return

      stage_lineage_key = self.build_lineage_key_for_bucket(stage_schema, src_list)
      stage_ds = (
        TargetDataset.objects
        .filter(target_schema=stage_schema, lineage_key=stage_lineage_key)
        .first()
      )

      if stage_ds is None:
        # legacy fallback by name
        stage_name = naming.build_physical_dataset_name(
          target_schema=stage_schema,
          source_dataset=representative,
        )
        stage_ds = (
          TargetDataset.objects
          .filter(target_schema=stage_schema, target_dataset_name=stage_name)
          .first()
        )

      if stage_ds is not None:
        TargetDatasetInput.objects.update_or_create(
          target_dataset=target_dataset_obj,
          upstream_target_dataset=stage_ds,
          defaults={
            "source_dataset": None,
            "role": self._resolve_role_for_source_dataset(representative),
          },
        )
      return

    if target_schema.short_name == "stage":
      # stage: read from raw if generate_raw_table, otherwise from SourceDatasets
      try:
        raw_schema = TS.objects.get(short_name="raw")
      except TS.DoesNotExist:
        raw_schema = None

      for src_ds in src_list:
        role = self._resolve_role_for_source_dataset(src_ds)

        gen_flag = src_ds.generate_raw_table
        if gen_flag is None:
          gen_flag = getattr(src_ds.source_system, "generate_raw_tables", False)

        raw_ds = None
        if gen_flag and raw_schema is not None:
          raw_lineage_key = self.build_lineage_key_for_bucket(raw_schema, [src_ds])
          raw_ds = (
            TargetDataset.objects
            .filter(target_schema=raw_schema, lineage_key=raw_lineage_key)
            .first()
          )

          if raw_ds is None:
            raw_physical_name = naming.build_physical_dataset_name(
              target_schema=raw_schema,
              source_dataset=src_ds,
            )
            raw_ds = (
              TargetDataset.objects
              .filter(
                target_schema=raw_schema,
                target_dataset_name=raw_physical_name,
              )
              .first()
            )

        if raw_ds is not None:
          TargetDatasetInput.objects.update_or_create(
            target_dataset=target_dataset_obj,
            upstream_target_dataset=raw_ds,
            defaults={
              "source_dataset": None,
              "role": role,
            },
          )
        else:
          TargetDatasetInput.objects.update_or_create(
            target_dataset=target_dataset_obj,
            source_dataset=src_ds,
            defaults={"role": role},
          )
      return

    # raw (and other schemas): always direct SourceDataset → TargetDataset
    from metadata.models import TargetDatasetInput as TDI
    for src_ds in src_list:
      role = self._resolve_role_for_source_dataset(src_ds)
      TDI.objects.update_or_create(
        target_dataset=target_dataset_obj,
        source_dataset=src_ds,
        defaults={"role": role},
      )

  def _sync_target_columns(self, target_dataset_obj, target_schema, column_drafts):
    """
    Upsert TargetColumns and their TargetColumnInput lineage for a single TargetDataset,
    and normalize ordinal_position for system-managed schemas.

    Returns the number of columns that were processed (generated/updated).
    """
    is_sys_managed_schema = getattr(target_schema, "is_system_managed", False)

    # Snapshot of existing columns (before any create)
    existing_cols = list(
      TargetColumn.objects.filter(target_dataset=target_dataset_obj)
    )

    # For system-managed schemas we use a high temporary ordinal base for *new* columns
    temp_base = 0
    if is_sys_managed_schema:
      max_ord = 0
      for c in existing_cols:
        if c.ordinal_position is not None and c.ordinal_position > max_ord:
          max_ord = c.ordinal_position
      temp_base = max_ord + 1000

    generator_columns = []
    created_or_updated = 0

    # Convenience: cache dataset-level upstream mapping
    dataset_inputs = list(
      target_dataset_obj.input_links.select_related("upstream_target_dataset", "source_dataset")
    )
    upstream_raw_ds = None
    upstream_stage_ds = None
    if target_schema.short_name == "stage":
      for inp in dataset_inputs:
        if inp.upstream_target_dataset and inp.upstream_target_dataset.target_schema.short_name == "raw":
          upstream_raw_ds = inp.upstream_target_dataset
          break
    elif target_schema.short_name == "rawcore":
      for inp in dataset_inputs:
        if inp.upstream_target_dataset and inp.upstream_target_dataset.target_schema.short_name == "stage":
          upstream_stage_ds = inp.upstream_target_dataset
          break

    for col_draft in column_drafts:
      src_col_id = getattr(col_draft, "source_column_id", None)

      # Resolve upstream column (Stage/Rawcore)
      upstream_col = None

      if target_schema.short_name == "stage" and src_col_id and upstream_raw_ds is not None:
        upstream_col = (
          TargetColumn.objects
          .filter(
            target_dataset=upstream_raw_ds,
            input_links__source_column_id=src_col_id,
          )
          .distinct()
          .first()
        )

      elif target_schema.short_name == "rawcore" and src_col_id and upstream_stage_ds is not None:
        # Try direct SourceColumn -> STAGE column
        upstream_col = (
          TargetColumn.objects
          .filter(
            target_dataset=upstream_stage_ds,
            input_links__source_column_id=src_col_id,
          )
          .distinct()
          .first()
        )

        if upstream_col is None:
          # Fallback: Source → RAW column → STAGE column
          raw_col = (
            TargetColumn.objects
            .filter(
              target_dataset__target_schema__short_name="raw",
              input_links__source_column_id=src_col_id,
            )
            .distinct()
            .first()
          )
          if raw_col is not None:
            upstream_col = (
              TargetColumn.objects
              .filter(
                target_dataset=upstream_stage_ds,
                input_links__upstream_target_column=raw_col,
              )
              .distinct()
              .first()
            )

      # Find existing TargetColumn
      existing_col = None

      if target_schema.short_name == "rawcore" and upstream_col is not None:
        existing_col = (
          TargetColumn.objects
          .filter(
            target_dataset=target_dataset_obj,
            input_links__upstream_target_column=upstream_col,
          )
          .distinct()
          .first()
        )
      elif src_col_id:
        existing_col = (
          TargetColumn.objects
          .filter(
            target_dataset=target_dataset_obj,
            input_links__source_column_id=src_col_id,
          )
          .distinct()
          .first()
        )

      # Surrogate-key fallback: reuse existing surrogate key column
      is_sk_draft = (getattr(col_draft, "system_role", "") == "surrogate_key")
      if not is_sk_draft:
        is_sk_draft = (col_draft.target_column_name == f"{target_dataset_obj.target_dataset_name}_key")

      if existing_col is None and is_sk_draft:
        existing_col = TargetColumn.objects.filter(
          target_dataset=target_dataset_obj,
          system_role="surrogate_key",
        ).first()

      # Name-based fallback for older rows / migration
      if existing_col is None:
        existing_col = TargetColumn.objects.filter(
          target_dataset=target_dataset_obj,
          target_column_name=col_draft.target_column_name,
        ).first()

      # Upsert column itself
      if existing_col is not None:
        target_col_obj = existing_col
        changed = False

        if target_col_obj.datatype != col_draft.datatype:
          target_col_obj.datatype = col_draft.datatype
          changed = True
        if target_col_obj.max_length != col_draft.max_length:
          target_col_obj.max_length = col_draft.max_length
          changed = True
        if target_col_obj.decimal_precision != col_draft.decimal_precision:
          target_col_obj.decimal_precision = col_draft.decimal_precision
          changed = True
        if target_col_obj.decimal_scale != col_draft.decimal_scale:
          target_col_obj.decimal_scale = col_draft.decimal_scale
          changed = True
        if target_col_obj.nullable != col_draft.nullable:
          target_col_obj.nullable = col_draft.nullable
          changed = True
        draft_role = (getattr(col_draft, "system_role", "") or "").strip()
        if draft_role and target_col_obj.system_role != draft_role:
          target_col_obj.system_role = draft_role
          changed = True
        draft_origin = (getattr(col_draft, "lineage_origin", "") or "").strip()
        if draft_origin and target_col_obj.lineage_origin != draft_origin:
          target_col_obj.lineage_origin = draft_origin
          changed = True
        if target_col_obj.surrogate_expression != col_draft.surrogate_expression:
          target_col_obj.surrogate_expression = col_draft.surrogate_expression
          changed = True
        if not target_col_obj.is_system_managed:
          target_col_obj.is_system_managed = True
          changed = True

        # For surrogate key columns we also keep the name in sync with the draft
        if (
          col_draft.system_role == "surrogate_key"
          and target_col_obj.system_role == "surrogate_key"
          and target_col_obj.target_column_name != col_draft.target_column_name
        ):
          target_col_obj.target_column_name = col_draft.target_column_name
          changed = True

        if changed:
          target_col_obj.save()

      else:
        # New column
        if is_sys_managed_schema:
          temp_base += 1
          ord_val = temp_base
          target_col_obj = TargetColumn.objects.create(
            target_dataset=target_dataset_obj,
            target_column_name=col_draft.target_column_name,
            ordinal_position=ord_val,
            datatype=col_draft.datatype,
            max_length=col_draft.max_length,
            decimal_precision=col_draft.decimal_precision,
            decimal_scale=col_draft.decimal_scale,
            nullable=col_draft.nullable,
            system_role=col_draft.system_role,
            lineage_origin=col_draft.lineage_origin,
            surrogate_expression=col_draft.surrogate_expression,
            is_system_managed=True,
            active=True,
            retired_at=None,
          )
        else:
          ord_val = col_draft.ordinal_position or 1
          target_col_obj, _ = TargetColumn.objects.update_or_create(
            target_dataset=target_dataset_obj,
            target_column_name=col_draft.target_column_name,
            defaults={
              "ordinal_position": ord_val,
              "datatype": col_draft.datatype,
              "max_length": col_draft.max_length,
              "decimal_precision": col_draft.decimal_precision,
              "decimal_scale": col_draft.decimal_scale,
              "nullable": col_draft.nullable,
              "system_role": col_draft.system_role,
              "lineage_origin": col_draft.lineage_origin,
              "surrogate_expression": col_draft.surrogate_expression,
              "is_system_managed": True,
            },
          )

      created_or_updated += 1
      generator_columns.append(target_col_obj)

      # Maintain TargetColumnInput lineage
      if target_schema.short_name == "raw":
        TargetColumnInput.objects.filter(
          target_column=target_col_obj,
          upstream_target_column__isnull=False,
        ).delete()
        if src_col_id:
          TargetColumnInput.objects.update_or_create(
            target_column=target_col_obj,
            source_column_id=src_col_id,
            upstream_target_column=None,
            defaults={},
          )

      elif target_schema.short_name == "stage":
        if upstream_col is not None:
          TargetColumnInput.objects.filter(
            target_column=target_col_obj,
            source_column__isnull=False,
          ).delete()
          TargetColumnInput.objects.update_or_create(
            target_column=target_col_obj,
            upstream_target_column=upstream_col,
            source_column=None,
            defaults={},
          )
        else:
          TargetColumnInput.objects.filter(
            target_column=target_col_obj,
            upstream_target_column__isnull=False,
          ).delete()
          if src_col_id:
            TargetColumnInput.objects.update_or_create(
              target_column=target_col_obj,
              source_column_id=src_col_id,
              upstream_target_column=None,
              defaults={},
            )

      elif target_schema.short_name == "rawcore":
        TargetColumnInput.objects.filter(
          target_column=target_col_obj,
          source_column__isnull=False,
        ).delete()
        if upstream_col is not None:
          TargetColumnInput.objects.update_or_create(
            target_column=target_col_obj,
            upstream_target_column=upstream_col,
            source_column=None,
            defaults={},
          )

      else:
        # default: behave like raw
        TargetColumnInput.objects.filter(
          target_column=target_col_obj,
          upstream_target_column__isnull=False,
        ).delete()
        if src_col_id:
          TargetColumnInput.objects.update_or_create(
            target_column=target_col_obj,
            source_column_id=src_col_id,
            upstream_target_column=None,
            defaults={},
          )

    # Normalize ordinals for system-managed schemas
    if is_sys_managed_schema:
      all_cols = list(
        TargetColumn.objects.filter(target_dataset=target_dataset_obj)
      )
      total_final = len(all_cols)

      generator_ids = [c.id for c in generator_columns]
      extra_cols = [c for c in all_cols if c.id not in generator_ids]
      extra_cols.sort(key=lambda c: (c.ordinal_position or 0, c.id))

      ordered_cols = generator_columns + extra_cols

      # Compute a truly safe ordinal base that cannot collide with any existing ordinal
      max_ord = 0
      for c in all_cols:
        if c.ordinal_position is not None and c.ordinal_position > max_ord:
          max_ord = c.ordinal_position

      safe_base = max_ord + 1000

      # Pass 1: move everything into a safe high range (no collisions possible)
      for idx, col in enumerate(ordered_cols, start=1):
        col.ordinal_position = safe_base + idx
        col.save(update_fields=["ordinal_position"])

      # Pass 2: assign final dense ordinals 1..N
      for idx, col in enumerate(ordered_cols, start=1):
        if col.ordinal_position != idx:
          col.ordinal_position = idx
          col.save(update_fields=["ordinal_position"])

    return created_or_updated


  def _extend_stage_column_drafts_for_union(
    self,
    target_schema,
    column_drafts,
    src_list,
  ):
    """
    For STAGE datasets fed by multiple SourceDatasets, extend the
    column_drafts so that they represent the UNION of all integrated
    source columns.

    The first SourceDataset in src_list is the "representative" and
    already defined the initial column_drafts (via build_dataset_bundle).
    Here we walk the remaining source datasets and add any additional
    columns that only exist there.
    """
    # Only relevant for stage and when more than one source participates
    if target_schema.short_name != "stage" or len(src_list) <= 1:
      return column_drafts

    existing_names = {c.target_column_name for c in column_drafts}

    # Start from the second dataset in the bucket – the first one already
    # drove build_dataset_bundle().
    for src_ds in src_list[1:]:
      if hasattr(src_ds, "source_columns"):
        src_cols_qs = src_ds.source_columns.all()
      else:
        # very defensive fallback for projects that use the Django default
        src_cols_qs = src_ds.sourcecolumn_set.all()

      # Only integrated columns matter here
      src_cols_qs = src_cols_qs.filter(integrate=True).order_by("ordinal_position")

      for src_col in src_cols_qs:
        tmp_draft = map_source_column_to_target_column(src_col, ordinal=0)
        if tmp_draft.target_column_name in existing_names:
          continue

        # New column: add to drafts; ordinals will be normalized later
        column_drafts.append(tmp_draft)
        existing_names.add(tmp_draft.target_column_name)

    return column_drafts


  # ------------------------------------------------------------
  # Main orchestration
  # ------------------------------------------------------------
  def apply_all(self, eligible_source_datasets, target_schema):
    """
    Generate or update TargetDatasets and TargetColumns for the given target_schema.

    Uses group-aware naming (via build_physical_dataset_name) to bucket
    SourceDatasets that share the same physical target name (e.g. stg_sap_kna1).
    """

    buckets = self._bucket_source_datasets(eligible_source_datasets, target_schema)

    created_datasets = 0
    total_columns = 0

    for physical_name, src_list in buckets.items():
      representative = src_list[0]

      bundle = self.build_dataset_bundle(representative, target_schema)
      dataset_draft = bundle["dataset"]
      column_drafts = bundle["columns"]

      # Ensure the draft name matches the bucket key
      dataset_draft.target_dataset_name = physical_name

      combination_mode = self._determine_combination_mode(target_schema, src_list)

      # 1) Dataset itself
      target_dataset_obj, ds_created = self._get_or_create_target_dataset(
        target_schema=target_schema,
        dataset_draft=dataset_draft,
        src_list=src_list,
        combination_mode=combination_mode,
      )
      created_datasets += 1 if ds_created else 0

      # 2) Surrogate key draft names must be based on the *current* dataset name
      self._ensure_surrogate_key_draft_names(target_dataset_obj, column_drafts)

      # 3) For STAGE + multi-source: extend drafts to represent UNION of all sources
      if target_schema.short_name == "stage" and len(src_list) > 1:
        column_drafts = self._extend_stage_column_drafts_for_union(
          target_schema=target_schema,
          column_drafts=column_drafts,
          src_list=src_list,
        )

      # 4) Dataset-level lineage
      self._sync_dataset_inputs(
        target_dataset_obj=target_dataset_obj,
        target_schema=target_schema,
        src_list=src_list,
        representative=representative,
      )

      # 5) Column-level lineage + ordinals
      total_columns += self._sync_target_columns(
        target_dataset_obj=target_dataset_obj,
        target_schema=target_schema,
        column_drafts=column_drafts,
      )

      # 5b) Ensure platform tech columns (load_run_id, loaded_at) exist on this dataset
      self._ensure_tech_columns(target_dataset_obj)

      # 6) Optional: history dataset for rawcore
      if target_schema.short_name == "rawcore":
        # Only create hist dataset when historization is enabled on this dataset
        hist_td = self.ensure_hist_dataset_for_rawcore(target_dataset_obj)

        TargetDatasetInput.objects.get_or_create(
          target_dataset=hist_td,
          upstream_target_dataset=target_dataset_obj,
          defaults={
            "role": "primary",
          },
        )

    return (
      f"{len(buckets)} target datasets and {total_columns} target columns generated/updated."
    )
