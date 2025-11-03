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

from dataclasses import dataclass
from typing import Optional
from metadata.generation import naming
from metadata.generation.hashing import build_surrogate_expression


@dataclass
class TargetDatasetDraft:
  """
  Draft representation of a TargetDataset before DB save.
  """
  target_schema_id: int
  target_schema_short_name: str  # if needed for UI/previews
  target_dataset_name: str
  source_dataset_id: int
  description: Optional[str]
  is_system_managed: bool
  # surrogate_key_column_name is not stored on TargetDataset,
  # but we keep it here for convenience inside the bundle so build_dataset_bundle()
  # can pass it down to TargetColumnDraft ordering logic etc.
  surrogate_key_column_name: Optional[str] = None


@dataclass
class TargetColumnDraft:
  """
  Draft representation of a TargetColumn before DB save.
  """
  target_column_name: str
  datatype: str
  max_length: Optional[int]
  decimal_precision: Optional[int]
  decimal_scale: Optional[int]
  nullable: bool

  business_key_column: bool
  surrogate_key_column: bool  # True only for the generated SK column
  artificial_column: bool = False  # optional, may stay default False

  lineage_origin: str = "direct"  # e.g. "direct", "surrogate_key", "derived"

  source_column_id: Optional[int] = None  # for lineage linking
  ordinal_position: int = 0

  surrogate_expression: Optional[str] = None  # only for SK, else None



def map_source_to_target_dataset(source_dataset, target_schema) -> TargetDatasetDraft:
  """
  Create a TargetDatasetDraft for this (source_dataset → target_schema) pairing.
  """
  physical_name = naming.build_physical_dataset_name(
    target_schema=target_schema,
    source_dataset=source_dataset,
  )

  return TargetDatasetDraft(
    target_schema_id=target_schema.id,
    target_schema_short_name=target_schema.short_name,
    target_dataset_name=physical_name,
    source_dataset_id=source_dataset.id,
    description=source_dataset.description,
    is_system_managed=getattr(target_schema, "is_system_managed", False),
    lineage_origin="direct",
  )


def map_source_column_to_target_column(source_column, ordinal: int) -> TargetColumnDraft:
  """
  Produce a TargetColumnDraft from a SourceColumn.
  """
  # We take the physical name directly from source_column.source_column
  # and run it through sanitize for consistency.
  colname = naming.sanitize_name(source_column.source_column)

  return TargetColumnDraft(
    target_column_name=colname,
    datatype=source_column.datatype,
    max_length=source_column.max_length,
    decimal_precision=source_column.decimal_precision,
    decimal_scale=source_column.decimal_scale,
    nullable=source_column.nullable,
    primary_key_column=source_column.primary_key_column, 
    lineage_origin="direct",
    source_column_id=source_column.id,
    ordinal_position=ordinal,
  )


def map_source_column_to_target_column(source_column, ordinal: int) -> TargetColumnDraft:
  """
  Map a SourceColumn to a TargetColumnDraft.
  business_key_column is derived from SourceColumn.primary_key_column.
  surrogate_key_column is always False here (those are generated separately).
  """
  colname = naming.sanitize_name(source_column.source_column)

  return TargetColumnDraft(
    target_column_name=colname,
    datatype=source_column.datatype,
    max_length=source_column.max_length,
    decimal_precision=source_column.decimal_precision,
    decimal_scale=source_column.decimal_scale,
    nullable=source_column.nullable,
    business_key_column=bool(source_column.primary_key_column),
    surrogate_key_column=False,
    lineage_origin="direct",
    source_column_id=source_column.id,
    ordinal_position=ordinal,
    surrogate_expression=None,
  )


def build_surrogate_key_column_draft(
  target_dataset_name: str,
  natural_key_colnames: list[str],
  pepper: str,
  ordinal: int,
  null_token: str,
  pair_sep: str,
  comp_sep: str,
) -> TargetColumnDraft:
  """
  Create the system-managed surrogate key column draft.

  This column:
  - defines the physical join key
  - is always first in the table (ordinal will be 1 when called)
  - is never user-editable
  """

  sk_name = naming.build_surrogate_key_name(target_dataset_name)

  expr = build_surrogate_expression(
    natural_key_cols=natural_key_colnames,
    pepper=pepper,
    null_token=null_token,
    pair_sep=pair_sep,
    comp_sep=comp_sep,
  )

  return TargetColumnDraft(
    target_column_name=sk_name,
    datatype="string",          # Dialect-specific refinement comes later
    max_length=64,
    decimal_precision=None,
    decimal_scale=None,
    nullable=False,
    business_key_column=False,      # surrogate is not the business key
    surrogate_key_column=True,      # <-- this is the surrogate key
    lineage_origin="surrogate_key",
    source_column_id=None,
    ordinal_position=ordinal,       # will usually be 1
    surrogate_expression=expr,
  )
