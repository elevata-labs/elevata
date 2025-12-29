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

from sqlalchemy.exc import NoSuchTableError, SQLAlchemyError
import logging

from typing import Iterable, List, Dict, Any

from django.db import transaction
from django.core.exceptions import ImproperlyConfigured

from metadata.system.introspection import read_table_metadata
from .types_map import map_sql_type
from .connectors import engine_for_source_system

from metadata.models import SourceColumn
from metadata.constants import SUPPORTED_SQLALCHEMY, BETA_SQLALCHEMY

# Allow auto import for these types (stable + beta)
ALLOWED_FOR_IMPORT = SUPPORTED_SQLALCHEMY | BETA_SQLALCHEMY

log = logging.getLogger(__name__)

def _materialize_with_related(datasets: Iterable) -> List:
  """
  Turn a queryset or generic iterable into a list.
  If it's a queryset, select_related('source_system') for efficiency.
  """
  if hasattr(datasets, "select_related"):
    return list(datasets.select_related("source_system"))
  return list(datasets)

def _clean_description(val):
  """Normalize SQLAlchemy comment/description values."""
  if isinstance(val, (tuple, list)):
    val = val[0] if val else ""
  if not isinstance(val, str):
    return ""
  val = val.strip()
  if val.startswith("('") and val.endswith("',)"):
    val = val[2:-3].strip()
  elif val.startswith("(") and val.endswith(")"):
    val = val[1:-1].strip("', ")
  return val

def import_metadata_for_datasets(
  datasets: Iterable,
  *,
  autointegrate_pk: bool = True,
  reset_flags: bool = False
) -> Dict[str, Any]:
  """
  Upsert metadata for all given SourceDataset rows.

  Behavior
  --------
  - Refresh technical fields from the source:
    datatype, max_length, decimal_precision, decimal_scale,
    nullable, primary_key_column, referenced_source_dataset_name, ordinal_position.
  - Preserve user-maintained fields (description, integrate, pii_level),
    unless reset_flags=True → then reset to neutral defaults.
  - Optionally mark PK columns integrate=True (autointegrate_pk=True).
  - Columns that disappeared in the source are deleted.

  Returns summary dict:
    {
      "datasets": <count>,
      "columns_imported": <total>,
      "created": <total>,
      "updated": <total>,
      "removed": <total>
    }
  """
  ds_list = _materialize_with_related(datasets)
  if not ds_list:
    return {"datasets": 0, "columns_imported": 0, "created": 0, "updated": 0, "removed": 0}

  engines = {}  # {source_system_id: engine}
  totals = {
    "datasets": 0,
    "columns_imported": 0,
    "created": 0,
    "updated": 0,
    "removed": 0,
  }

  # track failed datasets
  skipped: list[str] = []

  for ds in ds_list:
    ss = ds.source_system
    system_type = (ss.type or "").lower()

    if system_type not in ALLOWED_FOR_IMPORT:
      raise NotImplementedError(
        f"Source type '{system_type}' is not supported for automated metadata import yet. "
        "You can still document it manually in elevata."
      )

    # Reuse or create engine per source system
    if ss.id not in engines:
      try:
        engines[ss.id] = engine_for_source_system(system_type=system_type, short_name=ss.short_name)
      except Exception as e:
        raise ImproperlyConfigured(
          f"Failed to create engine for source system '{ss.short_name}' ({system_type}): {e}"
        ) from e
    engine = engines[ss.id]

    # --- TRY/EXCEPT around metadata introspection ---
    try:
      meta = read_table_metadata(engine, ds.schema_name, ds.source_dataset_name)
    except NoSuchTableError:
      msg = f"{ds.schema_name}.{ds.source_dataset_name}"
      log.warning("Skipping dataset %s: table not found in source", msg)
      skipped.append(msg)
      continue
    except SQLAlchemyError as e:
      msg = f"{ds.schema_name}.{ds.source_dataset_name}"
      log.error("Error introspecting %s: %s", msg, e)
      skipped.append(msg + f" (error: {e})")
      continue

    # Normal flow
    pk_cols = set(meta.get("primary_key_cols") or [])
    fk_map = meta.get("fk_map") or {}
    columns = meta.get("columns") or []

    # Optionally reset user flags for this dataset before re-sync
    if reset_flags:
      ds.source_columns.update(
        integrate=False,
        pii_level="none",
        description="",
        primary_key_column=False,
      )

    # Current columns in DB (to detect create/update/remove)
    existing: Dict[str, SourceColumn] = {c.source_column_name: c for c in ds.source_columns.all()}
    seen_names = set()

    created = 0
    updated = 0
    removed = 0

    with transaction.atomic():
      # Prevent UNIQUE(source_dataset_id, ordinal_position) collisions during reordering
      if existing:
        base = 10000
        n = 0
        for sc0 in existing.values():
          n += 1
          sc0.ordinal_position = base + n
          sc0.save(update_fields=["ordinal_position"])

      for i, c in enumerate(columns, start=1):
        name = c["name"]
        sqla_type = c["type"]
        raw_type = str(sqla_type)
        comment = c.get("comment") or c.get("description")
        desc = _clean_description(comment)

        nullable = bool(c.get("nullable", True))
        dtype, max_len, dec_prec, dec_scale = map_sql_type(engine.dialect.name, sqla_type)
        is_pk = name in pk_cols

        sc = existing.get(name)
        if sc is None:
          # New column → start with neutral defaults
          sc = SourceColumn(
            source_dataset=ds,
            source_column_name=name,
            integrate=False,
            pii_level="none",
          )
          created += 1

        # Refresh technical fields from source on every sync
        sc.ordinal_position = int(c.get("ordinal_position") or i)
        sc.description = (desc or "")[:255]
        sc.datatype = dtype
        sc.source_datatype_raw = raw_type
        sc.max_length = max_len
        sc.decimal_precision = dec_prec
        sc.decimal_scale = dec_scale
        sc.nullable = nullable
        sc.primary_key_column = is_pk
        sc.referenced_source_dataset_name = fk_map.get(name) or None

        # Auto-integrate PK columns if desired
        if autointegrate_pk and is_pk:
          sc.integrate = True

        sc.save()
        if name in existing:
          updated += 1
        seen_names.add(name)

      # Remove columns that no longer exist in source
      to_remove = [c for col_name, c in existing.items() if col_name not in seen_names]
      if to_remove:
        removed = len(to_remove)
        SourceColumn.objects.filter(pk__in=[c.pk for c in to_remove]).delete()

    # Aggregate totals
    totals["datasets"] += 1
    totals["columns_imported"] += len(seen_names)
    totals["created"] += created
    totals["updated"] += updated
    totals["removed"] += removed

  # Dispose engines
  for eng in engines.values():
    try:
      eng.dispose()
    except Exception:
      pass

  # skipped summary for UI feedback
  totals["skipped"] = skipped
  totals["skipped_count"] = len(skipped)

  return totals
