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

import re
import unicodedata
from metadata.generation.validators import validate_or_raise
from metadata.models import SourceDatasetGroupMembership

def _normalize_umlauts(value: str) -> str:
  """
  Very basic German-ish transliteration.
  ä -> ae, ö -> oe, ü -> ue, ß -> ss
  This avoids having to rely purely on unicode normalization for these cases.
  """
  replacements = {
    "ä": "ae",
    "ö": "oe",
    "ü": "ue",
    "Ä": "ae",
    "Ö": "oe",
    "Ü": "ue",
    "ß": "ss",
  }
  for src, tgt in replacements.items():
    value = value.replace(src, tgt)
  return value


def sanitize_name(raw: str) -> str:
  """
  Take any free-form dataset/table name and turn it into a safe identifier.
  Steps:
  1. Trim
  2. Replace umlauts etc.
  3. Unicode normalize & drop non-ASCII
  4. Replace all non [a-z0-9] with underscore
  5. Collapse multiple underscores
  6. Strip leading/trailing underscores
  7. Lowercase
  8. Validate with validate_or_raise()
  """
  if raw is None:
    raw = ""

  # 1 trim
  cleaned = raw.strip()

  # 2 German umlauts etc.
  cleaned = _normalize_umlauts(cleaned)

  # 3 normalize unicode -> ASCII-ish
  cleaned = unicodedata.normalize("NFKD", cleaned)
  cleaned = cleaned.encode("ascii", "ignore").decode("ascii")

  # 4 replace invalid chars with underscore
  cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", cleaned)

  # 5 collapse multiple underscores
  cleaned = re.sub(r"_+", "_", cleaned)

  # 6 strip underscores at ends
  cleaned = cleaned.strip("_")

  # 7 lowercase
  cleaned = cleaned.lower()

  # 8 final validation (also ensures it doesn't start with digit etc)
  validate_or_raise(cleaned, context="sanitized_name")

  return cleaned


def resolve_dataset_group_context(source_dataset, target_schema):
  """
  Decide naming parts (short_name, base_name) for a dataset
  in the context of a specific target_schema.

  RAW layer case (consolidate_groups == False):
    - Never merge systems.
    - Use the physical system short name (e.g. 'sap1', 'sap2'),
      NOT the harmonized target_short_name.
    - Use the dataset's own source_dataset_name.

    Result raw_*_*
      short_name = source_dataset.source_system.short_name
      base_name  = source_dataset.source_dataset_name

  STAGE / RAWCORE case (consolidate_groups == True):
    - Merging/grouping is allowed across systems that conceptually belong together.
    - Try SourceDatasetGroupMembership first:
        short_name = group.target_short_name
        base_name  = group.unified_source_dataset_name
      If no group:
        short_name = source_system.target_short_name
        base_name  = source_dataset.source_dataset_name
  """
  consolidate = getattr(target_schema, "consolidate_groups", False)

  # RAW-like behavior: no consolidation
  if not consolidate:
    sys_obj = getattr(source_dataset, "source_system", None)
    short_raw = getattr(sys_obj, "short_name", "")  # IMPORTANT: physical system short_name (sap1, sap2)
    base_raw  = getattr(source_dataset, "source_dataset_name", "")
    return dict(
      group=None,
      short_name=sanitize_name(short_raw),
      base_name=sanitize_name(base_raw),
    )

  # STAGE / RAWCORE behavior: consolidation allowed
  group = None
  membership = (
    SourceDatasetGroupMembership.objects
    .filter(source_dataset=source_dataset)
    .select_related("group")
    .first()
  )
  if membership and membership.group:
    group = membership.group

  if group:
    short_raw = getattr(group, "target_short_name", "")                  # e.g. "sap"
    base_raw  = getattr(group, "unified_source_dataset_name", "")        # e.g. "kna1"
  else:
    sys_obj = getattr(source_dataset, "source_system", None)
    short_raw = getattr(sys_obj, "target_short_name", "")                # e.g. "sap"
    base_raw  = getattr(source_dataset, "source_dataset_name", "")       # e.g. "kna1"

  return dict(
    group=group,
    short_name=sanitize_name(short_raw),
    base_name=sanitize_name(base_raw),
  )


def build_physical_dataset_name(target_schema, source_dataset) -> str:
  """
  Build final physical table name for (source_dataset in target_schema),
  using target_schema.physical_prefix and the schema's consolidation policy.
  """
  prefix_raw = getattr(target_schema, "physical_prefix", "") or ""
  prefix = sanitize_name(prefix_raw)

  ctx = resolve_dataset_group_context(source_dataset, target_schema)
  short_part = ctx["short_name"]
  base_part  = ctx["base_name"]

  candidate = f"{prefix}_{short_part}_{base_part}"
  validate_or_raise(candidate, context="target_dataset_name")
  return candidate


def build_history_name(base_table_name: str) -> str:
  """
  Returns the name for a history table derived from the given base table.
  Example: 'sap_customer' -> 'sap_customer_hist'
  """
  from .validators import validate_or_raise
  cleaned = f"{base_table_name}_hist"
  validate_or_raise(cleaned, context="history_table_name")
  return cleaned


def build_surrogate_key_name(target_dataset_name: str) -> str:
  # <target_dataset_name>_key
  """
  Returns the name for the surrogate key field of a target dataset.
  Convention: <target_dataset_name>_key 
  Example: 'rc_sap_customer' -> 'rc_sap_customer_key'
  """  
  base = sanitize_name(target_dataset_name)
  candidate = f"{base}_key"
  validate_or_raise(candidate, context="surrogate_key_name")
  return candidate
