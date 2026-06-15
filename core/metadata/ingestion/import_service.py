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

from sqlalchemy.exc import NoSuchTableError, SQLAlchemyError
import logging

from typing import Iterable, List, Dict, Any

from django.db import transaction
from django.core.exceptions import ImproperlyConfigured

from metadata.system.introspection import read_table_metadata
from .types_map import map_sql_type
from .connectors import engine_for_source_system

from metadata.models import SourceColumn
from metadata.constants import SUPPORTED_SQLALCHEMY, BETA_SQLALCHEMY, AUTO_IMPORT_NON_SQLALCHEMY
from metadata.ingestion.import_report import (
  SourceMetadataImportColumnChange,
  SourceMetadataImportDatasetReport,
  SourceMetadataImportReport,
)
from metadata.ingestion.rest_import import import_rest_metadata_for_dataset
from metadata.ingestion.file_import import import_file_metadata_for_dataset

# Allow auto import for these types (stable + beta)
ALLOWED_FOR_IMPORT = SUPPORTED_SQLALCHEMY | BETA_SQLALCHEMY | AUTO_IMPORT_NON_SQLALCHEMY

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


def _dataset_key(ds) -> str:
  """
  Return a stable display key for SourceDataset import reporting.
  """
  schema_name = (getattr(ds, "schema_name", None) or "").strip()
  dataset_name = (getattr(ds, "source_dataset_name", None) or "").strip()
  return f"{schema_name}.{dataset_name}".strip(".") or "<unknown>"


def _source_system_short_name(ds) -> str:
  """Return the source system short name for reporting."""
  return str(getattr(getattr(ds, "source_system", None), "short_name", "") or "")


def _add_key_review_note(dataset_report: SourceMetadataImportDatasetReport) -> None:
  """
  Add a manual key review note when no primary key columns were detected.
  """
  if dataset_report.columns_imported <= 0:
    return
  if dataset_report.pk_detected:
    return
  dataset_report.add_note(
    severity="warning",
    code="manual_key_review",
    message="No primary key columns were detected. Review the natural key before relying on generated integration logic.",
  )


def _add_option_notes(
  dataset_report: SourceMetadataImportDatasetReport,
  *,
  autointegrate_pk: bool,
  reset_flags: bool,
) -> None:
  """
  Add deterministic notes for import options that affect metadata flags.
  """
  if reset_flags:
    dataset_report.add_note(
      severity="info",
      code="flags_reset",
      message="User-maintained integration flags were reset before refreshing source metadata.",
    )
  if autointegrate_pk and dataset_report.pk_detected:
    dataset_report.add_note(
      severity="info",
      code="primary_keys_integrated",
      message="Detected primary key columns were marked for integration.",
    )


def _column_change_from_result(value: dict[str, Any]) -> SourceMetadataImportColumnChange:
  """
  Convert a low-level import result item into a report column change.
  """
  return SourceMetadataImportColumnChange.from_dict(value)


def _source_column_import_signature(sc) -> dict[str, Any]:
  """
  Return the source-owned metadata signature used to distinguish real changes
  from unchanged columns during SQLAlchemy metadata import.
  """
  return {
    "ordinal_position": getattr(sc, "ordinal_position", None),
    "source_datatype_raw": getattr(sc, "source_datatype_raw", None),
    "datatype": getattr(sc, "datatype", None),
    "max_length": getattr(sc, "max_length", None),
    "decimal_precision": getattr(sc, "decimal_precision", None),
    "decimal_scale": getattr(sc, "decimal_scale", None),
    "nullable": getattr(sc, "nullable", None),
    "primary_key_column": bool(getattr(sc, "primary_key_column", False)),
    "referenced_source_dataset_name": getattr(sc, "referenced_source_dataset_name", None),
    "json_path": getattr(sc, "json_path", None),
  }


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

  Returns a legacy-compatible summary dict plus a non-persistent review report.
  """
  ds_list = _materialize_with_related(datasets)
  engines = {}  # {source_system_id: engine}
  report = SourceMetadataImportReport(
    autointegrate_pk=autointegrate_pk,
    reset_flags=reset_flags,
  )

  if not ds_list:
    return report.as_result_dict()

  for ds in ds_list:
    ss = ds.source_system
    system_type = (ss.type or "").lower()
    dataset_key = _dataset_key(ds)
    source_system_short_name = _source_system_short_name(ds)

    if system_type not in ALLOWED_FOR_IMPORT:
      raise NotImplementedError(
        f"Source type '{system_type}' is not supported for automated metadata import yet. "
        "You can still document it manually in elevata."
      )

    # Non-SQLAlchemy import path (REST / files)
    if system_type in AUTO_IMPORT_NON_SQLALCHEMY:
      try:
        if system_type == "rest":
          res = import_rest_metadata_for_dataset(
            ds,
            autointegrate_pk=autointegrate_pk,
            reset_flags=reset_flags,
          )
        else:
          res = import_file_metadata_for_dataset(
            ds,
            file_type=system_type,
            autointegrate_pk=autointegrate_pk,
            reset_flags=reset_flags,
          )
      except Exception as e:
        log.error("Error importing metadata for %s (%s): %s", dataset_key, system_type, e)
        report.add_skipped_dataset(
          dataset_key=dataset_key,
          source_system=source_system_short_name,
          source_type=system_type,
          message=f"error: {e}",
        )
        continue

      dataset_report = SourceMetadataImportDatasetReport(
        dataset_key=dataset_key,
        source_system=source_system_short_name,
        source_type=system_type,
        columns_imported=int(res.get("columns_imported") or 0),
        created=int(res.get("created") or 0),
        updated=int(res.get("updated") or 0),
        changed=int(res.get("changed") or 0),
        unchanged=int(res.get("unchanged") or 0),
        removed=int(res.get("removed") or 0),
        pk_detected=sorted(str(c) for c in (res.get("pk_detected") or [])),
        column_changes=[
          _column_change_from_result(c)
          for c in (res.get("column_changes") or [])
          if isinstance(c, dict)
        ],
      )
      _add_key_review_note(dataset_report)
      _add_option_notes(
        dataset_report,
        autointegrate_pk=autointegrate_pk,
        reset_flags=reset_flags,
      )
      report.add_dataset(dataset_report)

      continue

    # Reuse or create engine per source system (SQLAlchemy sources)
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
      log.warning("Skipping dataset %s: table not found in source", dataset_key)
      report.add_skipped_dataset(
        dataset_key=dataset_key,
        source_system=source_system_short_name,
        source_type=system_type,
        message="table not found in source",
      )
      continue
    except SQLAlchemyError as e:
      log.error("Error introspecting %s: %s", dataset_key, e)
      report.add_skipped_dataset(
        dataset_key=dataset_key,
        source_system=source_system_short_name,
        source_type=system_type,
        message=f"error: {e}",
      )
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
    existing_signatures = {name: _source_column_import_signature(c) for name, c in existing.items()}
    seen_names = set()

    created = 0
    updated = 0
    changed = 0
    unchanged = 0
    removed = 0
    column_changes: list[SourceMetadataImportColumnChange] = []

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
        is_new = sc is None
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
          if existing_signatures.get(name) != _source_column_import_signature(sc):
            changed += 1
            action = "changed"
          else:
            unchanged += 1
            action = "unchanged"
        else:
          action = "created"
        seen_names.add(name)
        column_changes.append(
          SourceMetadataImportColumnChange(
            name=name,
            action=action,
            datatype=dtype,
            source_datatype_raw=raw_type,
            nullable=nullable,
            primary_key_column=is_pk,
          )
        )

      # Remove columns that no longer exist in source
      to_remove = [c for col_name, c in existing.items() if col_name not in seen_names]
      if to_remove:
        removed = len(to_remove)
        SourceColumn.objects.filter(pk__in=[c.pk for c in to_remove]).delete()

        for removed_column in to_remove:
          column_changes.append(
            SourceMetadataImportColumnChange(
              name=removed_column.source_column_name,
              action="removed",
              datatype=getattr(removed_column, "datatype", None),
              source_datatype_raw=getattr(removed_column, "source_datatype_raw", None),
              nullable=getattr(removed_column, "nullable", None),
              primary_key_column=bool(getattr(removed_column, "primary_key_column", False)),
              json_path=getattr(removed_column, "json_path", None),
            )
          )

    dataset_report = SourceMetadataImportDatasetReport(
      dataset_key=dataset_key,
      source_system=source_system_short_name,
      source_type=system_type,
      columns_imported=len(seen_names),
      created=created,
      updated=updated,
      changed=changed,
      unchanged=unchanged,
      removed=removed,
      pk_detected=sorted(pk_cols.intersection(seen_names)),
      column_changes=column_changes,
    )
    _add_key_review_note(dataset_report)
    _add_option_notes(
      dataset_report,
      autointegrate_pk=autointegrate_pk,
      reset_flags=reset_flags,
    )
    report.add_dataset(dataset_report)

  # Dispose engines
  for eng in engines.values():
    try:
      eng.dispose()
    except Exception:
      pass

  return report.as_result_dict()
