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

from metadata.models import SourceDatasetGroupMembership
from metadata.generation.naming import sanitize_name


def resolve_dataset_group_context(source_dataset, target_schema):
  """
  Handles dataset grouping / consolidation logic.

  This module is intentionally separated to avoid a circular import:
  - naming.py must never import models
  - models.py imports naming, so grouping must live elsewhere
  """

  schema_short = (getattr(target_schema, "short_name", "") or "").lower()

  #  raw layer uses source system short name
  if schema_short == "raw":
    sys_obj = getattr(source_dataset, "source_system", None)
    short_raw = getattr(sys_obj, "short_name", "")
    base_raw = getattr(source_dataset, "source_dataset_name", "")
    return {
      "group": None,
      "short_name": sanitize_name(short_raw),
      "base_name": sanitize_name(base_raw),
    }

  # STAGE: consolidation is allowed, but ONLY if groups exist
  if schema_short == "stage":
    membership = (
      SourceDatasetGroupMembership.objects
      .filter(source_dataset=source_dataset)
      .select_related("group")
      .first()
    )

    if membership and membership.group:
      grp = membership.group
      short_raw = getattr(grp, "target_short_name", "")
      base_raw = getattr(grp, "unified_source_dataset_name", "")
      return {
        "group": membership.group,
        "short_name": sanitize_name(short_raw),
        "base_name": sanitize_name(base_raw),
      }

    # Stage but no group membership => do NOT consolidate
    sys_obj = getattr(source_dataset, "source_system", None)
    short_raw = getattr(sys_obj, "target_short_name", "")
    base_raw = getattr(source_dataset, "source_dataset_name", "")
    return {
      "group": None,
      "short_name": sanitize_name(short_raw),
      "base_name": sanitize_name(base_raw),
    }

  # Other schemas (rawcore etc.): never consolidate, but use target_short_name
  sys_obj = getattr(source_dataset, "source_system", None)
  short_raw = getattr(sys_obj, "target_short_name", "")
  base_raw = getattr(source_dataset, "source_dataset_name", "")
  return {
    "group": None,
    "short_name": sanitize_name(short_raw),
    "base_name": sanitize_name(base_raw),
  }