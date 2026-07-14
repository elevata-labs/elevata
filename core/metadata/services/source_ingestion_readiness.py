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

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlparse

from metadata.constants import (
  AUTO_IMPORT_NON_SQLALCHEMY,
  BETA_SQLALCHEMY,
  SUPPORTED_SQLALCHEMY,
)
from metadata.intent.ingestion import resolve_ingest_mode
from metadata.intent.landing import landing_required


_FILE_SOURCE_TYPES = {
  "file",
  "csv",
  "json",
  "jsonl",
  "ndjson",
  "parquet",
  "excel",
}
_AUTOMATIC_IMPORT_TYPES = {
  str(value).strip().lower()
  for value in (
    set(SUPPORTED_SQLALCHEMY)
    | set(BETA_SQLALCHEMY)
    | set(AUTO_IMPORT_NON_SQLALCHEMY)
  )
}
_VALID_INGEST_MODES = {"native", "external", "none"}


@dataclass(frozen=True)
class SourceIngestionReadinessSignal:
  """One deterministic Source Ingestion Readiness finding."""
  code: str
  severity: str
  label: str
  message: str


@dataclass(frozen=True)
class SourceIngestionReadiness:
  """Read-only readiness summary for one SourceDataset."""
  dataset_key: str
  source_system: str
  source_type: str
  source_kind: str
  metadata_import_mode: str
  landing_required: bool
  ingest_mode: str
  raw_target_keys: tuple[str, ...]
  integrated_column_count: int
  status: str
  signals: tuple[SourceIngestionReadinessSignal, ...]

  @property
  def is_ready(self) -> bool:
    return self.status == "ready"

  @property
  def blocking_signal_count(self) -> int:
    return sum(
      1
      for signal in self.signals
      if signal.severity == "blocking"
    )

  @property
  def warning_signal_count(self) -> int:
    return sum(
      1
      for signal in self.signals
      if signal.severity == "warning"
    )


def _signal(
  code: str,
  severity: str,
  label: str,
  message: str,
) -> SourceIngestionReadinessSignal:
  return SourceIngestionReadinessSignal(
    code=code,
    severity=severity,
    label=label,
    message=message,
  )


def _dataset_key(source_dataset) -> str:
  schema_name = str(
    getattr(source_dataset, "schema_name", "") or ""
  ).strip()
  dataset_name = str(
    getattr(source_dataset, "source_dataset_name", "") or ""
  ).strip()
  return f"{schema_name}.{dataset_name}".strip(".") or "<unknown>"


def _source_kind(source_type: str) -> str:
  if source_type == "rest":
    return "rest"
  if source_type in _FILE_SOURCE_TYPES:
    return "file"
  return "relational"


def _manager_values(manager) -> list[Any]:
  if manager is None:
    return []
  values = manager.all() if hasattr(manager, "all") else manager
  return list(values)


def _prefetched_relation_values(instance, relation_name: str) -> list[Any] | None:
  """Return a prefetched relation without triggering another ORM query."""
  cache = getattr(instance, "_prefetched_objects_cache", None)
  if not isinstance(cache, dict) or relation_name not in cache:
    return None
  return list(cache[relation_name])


def _integrated_columns(source_dataset) -> list[Any]:
  prefetched = _prefetched_relation_values(
    source_dataset,
    "source_columns",
  )
  if prefetched is not None:
    return [
      column
      for column in prefetched
      if getattr(column, "integrate", False) is True
    ]

  manager = getattr(source_dataset, "source_columns", None)
  if manager is None:
    return []

  if hasattr(manager, "filter"):
    return list(manager.filter(integrate=True))

  return [
    column
    for column in _manager_values(manager)
    if getattr(column, "integrate", False) is True
  ]


def _raw_target_keys(source_dataset) -> tuple[str, ...]:
  prefetched = _prefetched_relation_values(
    source_dataset,
    "output_links",
  )
  if prefetched is not None:
    values = prefetched
  else:
    manager = getattr(source_dataset, "output_links", None)
    if manager is None:
      return ()

    values = manager.all() if hasattr(manager, "all") else manager
    if hasattr(values, "select_related"):
      values = values.select_related(
        "target_dataset",
        "target_dataset__target_schema",
      )

  keys: set[str] = set()
  for link in values:
    if getattr(link, "active", True) is not True:
      continue

    target_dataset = getattr(link, "target_dataset", None)
    target_schema = getattr(target_dataset, "target_schema", None)
    schema_short = str(
      getattr(target_schema, "short_name", "") or ""
    ).strip()
    if schema_short.lower() != "raw":
      continue

    target_name = str(
      getattr(target_dataset, "target_dataset_name", "") or ""
    ).strip()
    if target_name:
      keys.add(f"{schema_short}.{target_name}")

  return tuple(sorted(keys))


def _has_active_increment_policy(source_dataset) -> bool:
  prefetched = _prefetched_relation_values(
    source_dataset,
    "increment_policies",
  )
  if prefetched is not None:
    return any(
      getattr(policy, "active", True) is True
      for policy in prefetched
    )

  manager = getattr(source_dataset, "increment_policies", None)
  if manager is None:
    return False

  if hasattr(manager, "filter"):
    values = manager.filter(active=True)
    if hasattr(values, "exists"):
      return bool(values.exists())
    return bool(list(values))

  return any(
    getattr(policy, "active", True) is True
    for policy in _manager_values(manager)
  )


def _file_config_signals(
  source_type: str,
  config: dict[str, Any],
) -> list[SourceIngestionReadinessSignal]:
  signals: list[SourceIngestionReadinessSignal] = []
  uri_value = str(config.get("uri") or "").strip()

  if not uri_value:
    has_alias = any(
      str(config.get(key) or "").strip()
      for key in ("url", "path", "file_path")
    )
    if has_alias:
      signals.append(_signal(
        "file_uri_alias_not_executable",
        "blocking",
        "Canonical file URI missing",
        (
          "Metadata import accepts alternative file location keys, but native "
          "file execution requires ingestion_config.uri."
        ),
      ))
    else:
      signals.append(_signal(
        "file_uri_missing",
        "blocking",
        "File URI missing",
        "Native file ingestion requires ingestion_config.uri.",
      ))
    return signals

  signals.append(_signal(
    "file_uri_configured",
    "ok",
    "File URI configured",
    "A canonical ingestion_config.uri value is present.",
  ))

  suffix = PurePosixPath(urlparse(uri_value).path).suffix.lower()
  if source_type == "json" and suffix in {".jsonl", ".ndjson"}:
    signals.append(_signal(
      "json_lines_type_mismatch",
      "blocking",
      "JSON Lines source type mismatch",
      (
        "The configured file is JSON Lines, but native execution uses the "
        "Source System type 'json'. Use source type 'jsonl'."
      ),
    ))

  if source_type == "excel":
    if (
      config.get("sheet_name") is not None
      and config.get("sheet_index") is not None
    ):
      signals.append(_signal(
        "excel_sheet_selector_conflict",
        "blocking",
        "Conflicting Excel sheet selectors",
        "Configure either sheet_name or sheet_index, not both.",
      ))

  configured_options = tuple(
    key
    for key in (
      "delimiter",
      "quotechar",
      "encoding",
      "sheet_name",
      "sheet_index",
      "header_row",
      "max_rows",
    )
    if config.get(key) is not None
  )
  if configured_options:
    signals.append(_signal(
      "file_options_declared",
      "info",
      "File options declared",
      "Configured options: " + ", ".join(configured_options) + ".",
    ))

  return signals


def _rest_config_signals(
  source_system: str,
  config: dict[str, Any],
) -> list[SourceIngestionReadinessSignal]:
  signals: list[SourceIngestionReadinessSignal] = []
  path = str(config.get("path") or "").strip()

  if path:
    signals.append(_signal(
      "rest_path_configured",
      "ok",
      "REST path configured",
      "A dataset-level REST path is present.",
    ))
  else:
    signals.append(_signal(
      "rest_path_missing",
      "blocking",
      "REST path missing",
      "Native REST ingestion requires ingestion_config.path.",
    ))

  for key in ("query", "cursor", "validation", "retry"):
    value = config.get(key)
    if value is not None and not isinstance(value, dict):
      signals.append(_signal(
        f"invalid_rest_{key}",
        "blocking",
        f"Invalid REST {key} configuration",
        f"ingestion_config.{key} must be a JSON object.",
      ))

  cursor = config.get("cursor")
  if isinstance(cursor, dict):
    cursor_type = str(
      cursor.get("type") or "page_token"
    ).strip().lower()
    if cursor_type not in {"page_token", "offset"}:
      signals.append(_signal(
        "unsupported_rest_cursor_type",
        "blocking",
        "Unsupported REST cursor type",
        "REST cursor type must be 'page_token' or 'offset'.",
      ))
    else:
      signals.append(_signal(
        "rest_cursor_declared",
        "info",
        "REST cursor declared",
        f"Cursor mode: {cursor_type}.",
      ))

  signals.append(_signal(
    "rest_secret_expected",
    "info",
    "REST connection secret expected",
    (
      "Native REST ingestion expects the connection secret for source system "
      f"'{source_system}' to provide base_url. Secret values are not resolved "
      "by this readiness check."
    ),
  ))
  return signals


def _relational_config_signals(
  source_dataset,
  source_system: str,
) -> list[SourceIngestionReadinessSignal]:
  signals = [_signal(
    "source_connection_secret_expected",
    "info",
    "Source connection secret expected",
    (
      "Native relational ingestion expects a source connection secret for "
      f"system '{source_system}'. Secret values are not resolved by this "
      "readiness check."
    ),
  )]

  if getattr(source_dataset, "incremental", False) is not True:
    return signals

  increment_filter = str(
    getattr(source_dataset, "increment_filter", "") or ""
  ).strip()
  if not increment_filter:
    signals.append(_signal(
      "increment_filter_missing",
      "warning",
      "Incremental filter missing",
      (
        "The dataset is marked incremental, but no increment_filter is "
        "configured."
      ),
    ))
  elif (
    "{{DELTA_CUTOFF" in increment_filter
    and not _has_active_increment_policy(source_dataset)
  ):
    signals.append(_signal(
      "increment_policy_missing",
      "blocking",
      "Increment policy missing",
      (
        "increment_filter uses the DELTA_CUTOFF placeholder, but no active "
        "increment policy is configured."
      ),
    ))

  return signals


def _status(signals: list[SourceIngestionReadinessSignal]) -> str:
  if any(
    signal.severity in {"blocking", "warning"}
    for signal in signals
  ):
    return "attention"
  return "ready"


def _result(
  *,
  dataset_key: str,
  source_system: str,
  source_type: str,
  source_kind: str,
  metadata_import_mode: str,
  requires_landing: bool,
  ingest_mode: str,
  raw_target_keys: tuple[str, ...],
  integrated_column_count: int,
  status: str,
  signals: list[SourceIngestionReadinessSignal],
) -> SourceIngestionReadiness:
  return SourceIngestionReadiness(
    dataset_key=dataset_key,
    source_system=source_system,
    source_type=source_type,
    source_kind=source_kind,
    metadata_import_mode=metadata_import_mode,
    landing_required=requires_landing,
    ingest_mode=ingest_mode,
    raw_target_keys=raw_target_keys,
    integrated_column_count=integrated_column_count,
    status=status,
    signals=tuple(signals),
  )


def build_source_ingestion_readiness(source_dataset) -> SourceIngestionReadiness:
  """
  Explain SourceDataset ingestion readiness without performing I/O.

  The service does not connect to sources, resolve secrets, access files,
  execute ingestion, render SQL, or persist state.
  """
  dataset_key = _dataset_key(source_dataset)
  source_system_obj = getattr(source_dataset, "source_system", None)
  signals: list[SourceIngestionReadinessSignal] = []

  if source_system_obj is None:
    signals.append(_signal(
      "source_system_missing",
      "blocking",
      "Source system missing",
      "Source Ingestion Readiness requires a Source System.",
    ))
    return _result(
      dataset_key=dataset_key,
      source_system="",
      source_type="",
      source_kind="unavailable",
      metadata_import_mode="unavailable",
      requires_landing=False,
      ingest_mode="none",
      raw_target_keys=(),
      integrated_column_count=0,
      status="unavailable",
      signals=signals,
    )

  source_system = str(
    getattr(source_system_obj, "short_name", "") or ""
  ).strip()
  source_type = str(
    getattr(source_system_obj, "type", "") or ""
  ).strip().lower()
  source_kind = _source_kind(source_type)
  metadata_import_mode = (
    "automatic"
    if source_type in _AUTOMATIC_IMPORT_TYPES
    else "manual"
  )

  if getattr(source_system_obj, "is_source", False) is not True:
    signals.append(_signal(
      "system_not_source",
      "blocking",
      "System is not a source",
      "The owning System is not marked as a source system.",
    ))
    return _result(
      dataset_key=dataset_key,
      source_system=source_system,
      source_type=source_type,
      source_kind=source_kind,
      metadata_import_mode=metadata_import_mode,
      requires_landing=False,
      ingest_mode="none",
      raw_target_keys=(),
      integrated_column_count=0,
      status="unavailable",
      signals=signals,
    )

  signals.append(_signal(
    "metadata_import_supported"
    if metadata_import_mode == "automatic"
    else "metadata_import_manual",
    "ok" if metadata_import_mode == "automatic" else "info",
    "Automated metadata import available"
    if metadata_import_mode == "automatic"
    else "Manual metadata maintenance",
    (
      f"Source type '{source_type}' supports automated metadata import."
      if metadata_import_mode == "automatic"
      else (
        f"Source type '{source_type}' is maintained manually. This does not "
        "prevent ingestion when the required metadata is present."
      )
    ),
  ))

  requires_landing = bool(landing_required(source_dataset))
  integrated_columns = _integrated_columns(source_dataset)

  if getattr(source_dataset, "active", True) is not True:
    signals.append(_signal(
      "dataset_inactive",
      "info",
      "Dataset inactive",
      "Inactive source datasets are retained for lineage and audit only.",
    ))
    return _result(
      dataset_key=dataset_key,
      source_system=source_system,
      source_type=source_type,
      source_kind=source_kind,
      metadata_import_mode=metadata_import_mode,
      requires_landing=requires_landing,
      ingest_mode="none",
      raw_target_keys=(),
      integrated_column_count=len(integrated_columns),
      status="not_applicable",
      signals=signals,
    )

  if getattr(source_dataset, "integrate", None) is not True:
    signals.append(_signal(
      "dataset_not_integrated",
      "info",
      "Dataset outside integration scope",
      "The SourceDataset is documented but not selected for integration.",
    ))
    return _result(
      dataset_key=dataset_key,
      source_system=source_system,
      source_type=source_type,
      source_kind=source_kind,
      metadata_import_mode=metadata_import_mode,
      requires_landing=False,
      ingest_mode="none",
      raw_target_keys=(),
      integrated_column_count=len(integrated_columns),
      status="not_applicable",
      signals=signals,
    )

  if not requires_landing:
    signals.append(_signal(
      "landing_not_required",
      "info",
      "RAW landing not required",
      (
        "The dataset is configured for direct or federated access without an "
        "elevata-managed RAW landing."
      ),
    ))
    return _result(
      dataset_key=dataset_key,
      source_system=source_system,
      source_type=source_type,
      source_kind=source_kind,
      metadata_import_mode=metadata_import_mode,
      requires_landing=False,
      ingest_mode="none",
      raw_target_keys=(),
      integrated_column_count=len(integrated_columns),
      status="not_applicable",
      signals=signals,
    )

  try:
    ingest_mode = str(
      resolve_ingest_mode(source_dataset) or "none"
    ).strip().lower()
  except ValueError as exc:
    ingest_mode = str(
      getattr(source_system_obj, "include_ingest", "none") or "none"
    ).strip().lower()
    signals.append(_signal(
      "ingest_mode_inconsistent",
      "blocking",
      "Ingestion mode inconsistent",
      str(exc),
    ))

  if ingest_mode not in _VALID_INGEST_MODES:
    signals.append(_signal(
      "ingest_mode_unsupported",
      "blocking",
      "Unsupported ingestion mode",
      f"Unsupported include_ingest value: '{ingest_mode}'.",
    ))

  raw_target_keys = _raw_target_keys(source_dataset)
  if not raw_target_keys:
    signals.append(_signal(
      "raw_target_missing",
      "blocking",
      "RAW target missing",
      (
        "RAW landing is required, but no active RAW TargetDataset input link "
        "exists for this source dataset."
      ),
    ))
  elif len(raw_target_keys) > 1:
    signals.append(_signal(
      "multiple_raw_targets",
      "warning",
      "Multiple RAW targets",
      "Active RAW targets: " + ", ".join(raw_target_keys) + ".",
    ))
  else:
    signals.append(_signal(
      "raw_target_available",
      "ok",
      "RAW target available",
      f"RAW target '{raw_target_keys[0]}' is linked to this source dataset.",
    ))

  if not integrated_columns:
    signals.append(_signal(
      "integrated_columns_missing",
      "blocking",
      "Integrated source columns missing",
      "At least one SourceColumn must be marked for integration.",
    ))
  else:
    signals.append(_signal(
      "integrated_columns_available",
      "ok",
      "Integrated source columns available",
      f"{len(integrated_columns)} source columns are selected for integration.",
    ))

  if source_kind in {"file", "rest"}:
    missing_paths = sorted(
      str(getattr(column, "source_column_name", "") or "").strip()
      for column in integrated_columns
      if not str(getattr(column, "json_path", "") or "").strip()
    )
    missing_paths = [name for name in missing_paths if name]
    if missing_paths:
      signals.append(_signal(
        "json_paths_missing",
        "blocking",
        "JSON paths missing",
        (
          "Integrated semi-structured source columns require json_path values: "
          + ", ".join(missing_paths)
          + "."
        ),
      ))

  if ingest_mode == "external":
    signals.append(_signal(
      "external_ingestion_declared",
      "info",
      "External ingestion declared",
      (
        "RAW data is expected to be populated outside elevata. Native connector "
        "configuration is therefore not evaluated."
      ),
    ))
  elif ingest_mode == "native":
    config = getattr(source_dataset, "ingestion_config", None)
    if config is None:
      config = {}
    elif not isinstance(config, dict):
      signals.append(_signal(
        "invalid_ingestion_config",
        "blocking",
        "Invalid ingestion configuration",
        "ingestion_config must be a JSON object.",
      ))
      config = None

    if source_kind == "file" and config is not None:
      signals.extend(_file_config_signals(source_type, config))
    elif source_kind == "rest" and config is not None:
      signals.extend(_rest_config_signals(source_system, config))
    elif source_kind == "relational":
      signals.extend(_relational_config_signals(
        source_dataset,
        source_system,
      ))

  return _result(
    dataset_key=dataset_key,
    source_system=source_system,
    source_type=source_type,
    source_kind=source_kind,
    metadata_import_mode=metadata_import_mode,
    requires_landing=True,
    ingest_mode=ingest_mode,
    raw_target_keys=raw_target_keys,
    integrated_column_count=len(integrated_columns),
    status=_status(signals),
    signals=signals,
  )
