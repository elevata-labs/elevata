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
from metadata.models import SourceDataset, TargetSchema, TargetDataset, TargetColumn, TargetColumnInput
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
    Only return schemas that are configured for auto-generation.
    This lets us include raw/stage/rawcore, but exclude serving.
    """
    return list(
      TargetSchema.objects.filter(generate_layer=True)
    )


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
      target_schema_short_name=getattr(target_schema, "short_name", ""),
      target_dataset_name=physical_name,
      source_dataset_id=source_dataset.id,
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
    Return the list of SourceDatasets that should feed this target_schema.
    - For RAW-like schemas (consolidate_groups == False):
        only datasets that should actually materialize RAW tables
        according to dataset_creates_raw_object()
    - For STAGE / RAWCORE (consolidate_groups == True):
        all relevant datasets, regardless of integrate/generate_raw_table
    """
    all_ds = list(SourceDataset.objects.all())

    # RAW layer behavior: no consolidation, and not all datasets should land here
    if getattr(target_schema, "consolidate_groups", False) is False:
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

  def apply_all(self, eligible_source_datasets, target_schema):
    """
    Generate or update TargetDatasets and TargetColumns for the given target_schema.

    Uses group-aware naming (via resolve_dataset_group_context) to bucket
    SourceDatasets that share the same physical target name (e.g. stg_sap_kna1).
    """

    # 1. Bucket datasets by final physical table name
    buckets = {}  # { "stg_sap_kna1": [ds1, ds2], "raw_sap1_kna1": [ds3], ... }

    for src_ds in eligible_source_datasets:
      physical_name = naming.build_physical_dataset_name(
        target_schema=target_schema,
        source_dataset=src_ds,
      )
      buckets.setdefault(physical_name, []).append(src_ds)

    created_datasets = 0
    created_columns = 0

    # 2. For each final table, pick one representative
    for physical_name, src_list in buckets.items():
      representative = src_list[0]

      bundle = self.build_dataset_bundle(representative, target_schema)
      dataset_draft = bundle["dataset"]
      column_drafts = bundle["columns"]

      # force physical_name just to be 100% aligned with bucketing
      dataset_draft.target_dataset_name = physical_name

      # 3. Upsert TargetDataset
      target_dataset_obj, _ = TargetDataset.objects.update_or_create(
        target_schema=target_schema,
        target_dataset_name=dataset_draft.target_dataset_name,
        defaults={
          "description": dataset_draft.description,
          "is_system_managed": dataset_draft.is_system_managed,
        },
      )
      created_datasets += 1

      # 4. Upsert TargetColumns
      for col_draft in column_drafts:
        target_col_obj, _ = TargetColumn.objects.update_or_create(
          target_dataset=target_dataset_obj,
          target_column_name=col_draft.target_column_name,
          defaults={
            "ordinal_position": col_draft.ordinal_position,
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
        created_columns += 1

        if getattr(col_draft, "source_column_id", None):
          TargetColumnInput.objects.update_or_create(
            target_column=target_col_obj,
            source_column_id=col_draft.source_column_id,
          )

    return (
      f"{len(buckets)} target datasets and {created_columns} target columns generated/updated."
    )
