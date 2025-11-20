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

from typing import Dict, List
from metadata.models import (
  SourceDataset,
  TargetSchema,
  TargetDataset,
  TargetColumn,
  TargetColumnInput,
)
from metadata.generation import rules, naming, security
from metadata.generation.mappers import (
  TargetDatasetDraft,
  TargetColumnDraft,
  map_source_column_to_target_column, 
  build_surrogate_key_column_draft,
)

class TargetGenerationService:
  """
  Responsible for deriving TargetDatasets and TargetColumns
  from SourceDatasets + TargetSchemas (raw, stage, rawcore, serving, ...).
  """

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
          # später: "bizcore": 40, "serving": 50, ...
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
    natural_key_cols = [c for c in mapped_columns if c.business_key_column]
    natural_key_colnames = [c.target_column_name for c in natural_key_cols]

    column_drafts: list[TargetColumnDraft] = []
    ordinal_counter = 1

    # 5. If this schema wants surrogate keys, create surrogate key column FIRST
    if getattr(target_schema, "surrogate_keys_enabled", False):
      pepper = security.get_runtime_pepper()  # your helper that resolves the pepper from env/profile
      surrogate_col = build_surrogate_key_column_draft(
        target_dataset_name=dataset_draft.target_dataset_name,
        natural_key_colnames=natural_key_colnames,
        pepper=pepper,
        ordinal=ordinal_counter,
        null_token=target_schema.surrogate_key_null_token,
        pair_sep=target_schema.surrogate_key_pair_separator,
        comp_sep=target_schema.surrogate_key_component_separator,
      )
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
        (rules.dataset_creates_raw_object)
    - STAGE / RAWCORE:
        all relevant Datasets (integrate+active), if raw exists or not
    """
    all_ds = self.get_relevant_source_datasets()

    if not getattr(target_schema, "consolidate_groups", False):
        # RAW / raw-like layer
        return [
            ds for ds in all_ds
            if rules.dataset_creates_raw_object(ds)
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
        if schema.short_name == "raw" and not rules.dataset_creates_raw_object(src_ds):
          continue

        # Bundle erzeugen
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
    - If at least one source dataset is incremental               -> use
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
    (schema + source_dataset IDs), with a fallback to name-based matching
    for older rows.
    """
    lineage_key = self.build_lineage_key_for_bucket(target_schema, src_list)
    incremental_strategy = self._determine_incremental_strategy(target_schema, src_list)
    incremental_source = self._determine_incremental_source(src_list)

    # Only store incremental_source for non-full strategies
    if incremental_strategy == "full":
      incremental_source = None

    target_dataset_obj = TargetDataset.objects.filter(
      target_schema=target_schema,
      lineage_key=lineage_key,
    ).first()

    # Fallback: legacy rows that only have the old auto-generated name
    if target_dataset_obj is None:
      existing_by_name = TargetDataset.objects.filter(
        target_schema=target_schema,
        target_dataset_name=dataset_draft.target_dataset_name,
      ).first()
      if existing_by_name is not None:
        if not existing_by_name.lineage_key:
          existing_by_name.lineage_key = lineage_key
          existing_by_name.save(update_fields=["lineage_key"])
        target_dataset_obj = existing_by_name

    created = False
    if target_dataset_obj is None:
      target_dataset_obj = TargetDataset.objects.create(
        target_schema=target_schema,
        target_dataset_name=dataset_draft.target_dataset_name,
        description=dataset_draft.description,
        is_system_managed=dataset_draft.is_system_managed,
        combination_mode=combination_mode,
        lineage_key=lineage_key,
        incremental_strategy=incremental_strategy,
        incremental_source=incremental_source,
      )
      created = True
    else:
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

    return target_dataset_obj, created

  def _ensure_surrogate_key_draft_names(self, target_dataset_obj, column_drafts):
    """
    Make sure all surrogate key column drafts use the *current* dataset name,
    not just the auto-generated physical name.

    This keeps the draft in sync with:
    - TargetDataset.save(), which renames the actual surrogate key column when
      target_dataset_name changes.
    - apply_all(), which reuses existing surrogate key columns.
    """
    for col_draft in column_drafts:
      if getattr(col_draft, "surrogate_key_column", False):
        col_draft.target_column_name = f"{target_dataset_obj.target_dataset_name}_key"

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

      # 4a. Resolve upstream column (Stage/Rawcore)
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

      # 4b. Find existing TargetColumn
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

      # Surrogate-key fallback: reuse existing surrogate key column by flag
      if existing_col is None and col_draft.surrogate_key_column:
        existing_col = TargetColumn.objects.filter(
          target_dataset=target_dataset_obj,
          surrogate_key_column=True,
        ).first()

      # Name-based fallback for older rows / migration
      if existing_col is None:
        existing_col = TargetColumn.objects.filter(
          target_dataset=target_dataset_obj,
          target_column_name=col_draft.target_column_name,
        ).first()

      # 4c. Upsert column itself
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
        if target_col_obj.business_key_column != col_draft.business_key_column:
          target_col_obj.business_key_column = col_draft.business_key_column
          changed = True
        if target_col_obj.surrogate_key_column != col_draft.surrogate_key_column:
          target_col_obj.surrogate_key_column = col_draft.surrogate_key_column
          changed = True
        if target_col_obj.lineage_origin != col_draft.lineage_origin:
          target_col_obj.lineage_origin = col_draft.lineage_origin
          changed = True
        if target_col_obj.surrogate_expression != col_draft.surrogate_expression:
          target_col_obj.surrogate_expression = col_draft.surrogate_expression
          changed = True
        if not target_col_obj.is_system_managed:
          target_col_obj.is_system_managed = True
          changed = True

        # For surrogate key columns we also keep the name in sync with the draft
        if (
          col_draft.surrogate_key_column
          and target_col_obj.surrogate_key_column
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
            business_key_column=col_draft.business_key_column,
            surrogate_key_column=col_draft.surrogate_key_column,
            lineage_origin=col_draft.lineage_origin,
            surrogate_expression=col_draft.surrogate_expression,
            is_system_managed=True,
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
              "business_key_column": col_draft.business_key_column,
              "surrogate_key_column": col_draft.surrogate_key_column,
              "lineage_origin": col_draft.lineage_origin,
              "surrogate_expression": col_draft.surrogate_expression,
              "is_system_managed": True,
            },
          )

      created_or_updated += 1
      generator_columns.append(target_col_obj)

      # 4d. Maintain TargetColumnInput lineage
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

    # 4e. Normalize ordinals for system-managed schemas
    if is_sys_managed_schema:
      all_cols = list(
        TargetColumn.objects.filter(target_dataset=target_dataset_obj)
      )
      total_final = len(all_cols)

      generator_ids = [c.id for c in generator_columns]
      extra_cols = [c for c in all_cols if c.id not in generator_ids]
      extra_cols.sort(key=lambda c: (c.ordinal_position or 0, c.id))

      ordered_cols = generator_columns + extra_cols

      # Pass 1: move everything into a safe high range
      for idx, col in enumerate(ordered_cols, start=1):
        col.ordinal_position = total_final + idx
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

    return (
      f"{len(buckets)} target datasets and {total_columns} target columns generated/updated."
    )
