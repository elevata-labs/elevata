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
from pathlib import Path
from typing import Any, Literal

from metadata.architecture.paths import (
  ArchitectureArtifactContext,
  resolve_architecture_artifact_context,
)
from metadata.architecture.state import ArchitectureState, ColumnState, DatasetState
from metadata.architecture.store import ArchitectureStateStore
from metadata.config.profiles import load_profile
from metadata.config.targets import get_target_system
from metadata.ingestion.connectors import engine_for_target
from metadata.ingestion.types_map import (
  canonical_type_str,
  canonicalize_type,
  classify_type_drift,
)
from metadata.models import TargetDataset
from metadata.rendering.dialects import get_active_dialect


ArchitectureBaselineSource = Literal[
  "recorded_state",
  "discovered_physical_state",
  "missing_or_unsupported",
]


@dataclass(frozen=True)
class ArchitectureBaselineResolution:
  """
  Result of resolving a comparison baseline for Architecture Control.
  """
  previous_state: ArchitectureState | None
  source: ArchitectureBaselineSource
  can_execute: bool
  message: str
  state_file: Path | None = None
  warning_count: int = 0
  warnings: tuple[str, ...] = ()

  @property
  def is_recorded(self) -> bool:
    """
    Return True when the baseline came from a persisted Architecture State.
    """
    return self.source == "recorded_state"

  @property
  def is_discovered(self) -> bool:
    """
    Return True when the baseline came from read-only physical discovery.
    """
    return self.source == "discovered_physical_state"


@dataclass(frozen=True)
class PhysicalArchitectureDiscoveryResult:
  """
  Read-only discovery result for one physical target platform.
  """
  state: ArchitectureState
  warnings: tuple[str, ...] = ()

  @property
  def dataset_count(self) -> int:
    """
    Return the number of physically discovered managed datasets.
    """
    return len(self.state.datasets)


class PhysicalArchitectureDiscoveryError(ValueError):
  """
  Raised when physical architecture baseline discovery cannot complete safely.
  """


def resolve_architecture_baseline(
  *,
  current_state: ArchitectureState,
  artifact_context: ArchitectureArtifactContext | None = None,
  state_store: ArchitectureStateStore | None = None,
  allow_physical_discovery: bool = True,
  relevant_dataset_keys: set[str] | None = None,
  profile: Any | None = None,
  target_system: Any | None = None,
  dialect: Any | None = None,
) -> ArchitectureBaselineResolution:
  """
  Resolve the comparison baseline for Architecture Control.

  A recorded Architecture State always wins. If it is missing, elevata attempts
  to discover the physical target architecture read-only and uses that as the
  previous state for safe platform catch-up planning.
  """
  context = artifact_context or resolve_architecture_artifact_context()
  store = state_store or ArchitectureStateStore(context=context)
  state_file = store.state_file_path()
  recorded_state = store.load()

  if recorded_state is not None:
    return ArchitectureBaselineResolution(
      previous_state=recorded_state,
      source="recorded_state",
      can_execute=True,
      message="Recorded architecture baseline is available for this runtime context.",
      state_file=state_file,
    )

  if not allow_physical_discovery:
    return ArchitectureBaselineResolution(
      previous_state=None,
      source="missing_or_unsupported",
      can_execute=False,
      message=(
        "Recorded architecture baseline is missing for this runtime context. "
        "Physical discovery is disabled, so controlled execution cannot proceed safely."
      ),
      state_file=state_file,
    )

  try:
    discovery = discover_physical_architecture_state(
      current_state=current_state,
      artifact_context=context,
      relevant_dataset_keys=relevant_dataset_keys,
      profile=profile,
      target_system=target_system,
      dialect=dialect,
    )
  except Exception as exc:
    return ArchitectureBaselineResolution(
      previous_state=None,
      source="missing_or_unsupported",
      can_execute=False,
      message=(
        "Recorded architecture baseline is missing and read-only physical "
        "baseline discovery failed. Controlled execution cannot proceed safely: "
        f"{type(exc).__name__}: {exc}"
      ),
      state_file=state_file,
      warnings=(str(exc),),
      warning_count=1,
    )

  return ArchitectureBaselineResolution(
    previous_state=discovery.state,
    source="discovered_physical_state",
    can_execute=True,
    message=(
      "Recorded architecture baseline is missing. A read-only physical target "
      f"baseline was discovered with {discovery.dataset_count} managed dataset(s). "
      "The recorded state file will be written only after successful controlled execution."
    ),
    state_file=state_file,
    warnings=discovery.warnings,
    warning_count=len(discovery.warnings),
  )


def discover_physical_architecture_state(
  *,
  current_state: ArchitectureState,
  artifact_context: ArchitectureArtifactContext | None = None,
  relevant_dataset_keys: set[str] | None = None,
  profile: Any | None = None,
  target_system: Any | None = None,
  dialect: Any | None = None,
) -> PhysicalArchitectureDiscoveryResult:
  """
  Build an ArchitectureState from the physical target platform.

  Discovery is read-only. It only inspects managed TargetDatasets that exist in
  the desired metadata architecture. Unmanaged physical tables are intentionally
  ignored in this first baseline step.
  """
  context = artifact_context or resolve_architecture_artifact_context()
  profile_obj = profile or load_profile(context.profile_name)
  target_system_obj = target_system or get_target_system(context.target_system_short)
  dialect_obj = dialect or get_active_dialect(getattr(target_system_obj, "type", None))

  exec_engine = None
  introspection_engine = None
  warnings: list[str] = []

  try:
    exec_engine = dialect_obj.get_execution_engine(target_system_obj)
  except Exception as exc:
    raise PhysicalArchitectureDiscoveryError(
      f"Could not create execution engine for target system "
      f"'{getattr(target_system_obj, 'short_name', '')}': {exc}"
    ) from exc

  try:
    if (getattr(target_system_obj, "type", "") or "").lower() != "databricks":
      try:
        introspection_engine = engine_for_target(
          target_short_name=target_system_obj.short_name,
          system_type=target_system_obj.type,
        )
      except Exception as exc:
        warnings.append(
          "SQLAlchemy introspection engine unavailable; falling back to dialect "
          f"execution introspection where supported: {type(exc).__name__}: {exc}"
        )

    desired_datasets = _filter_discovery_datasets(
      current_state=current_state,
      relevant_dataset_keys=relevant_dataset_keys,
    )

    datasets = tuple(
      _discover_dataset_state(
        desired_dataset=desired_dataset,
        dialect=dialect_obj,
        introspection_engine=introspection_engine,
        exec_engine=exec_engine,
        warnings=warnings,
      )
      for desired_dataset in desired_datasets
    )
    discovered = tuple(ds for ds in datasets if ds is not None)

    return PhysicalArchitectureDiscoveryResult(
      state=ArchitectureState(datasets=discovered),
      warnings=tuple(warnings),
    )
  finally:
    _dispose_if_possible(introspection_engine)
    _dispose_if_possible(exec_engine)


def _filter_discovery_datasets(
  *,
  current_state: ArchitectureState,
  relevant_dataset_keys: set[str] | None,
) -> tuple[DatasetState, ...]:
  """
  Return the desired datasets that should be physically discovered.

  Architecture Control scopes must not force physical discovery for unrelated
  datasets. This is especially important for remote platforms such as
  Databricks, where every SHOW/DESCRIBE roundtrip is expensive and may make
  page rendering slow or unstable for narrow scopes.
  """
  datasets = tuple(current_state.datasets or ())
  if relevant_dataset_keys is None:
    return datasets

  wanted = {str(key or "").strip() for key in relevant_dataset_keys if str(key or "").strip()}
  if not wanted:
    return ()

  return tuple(
    ds
    for ds in datasets
    if str(getattr(ds, "dataset_key", "") or "").strip() in wanted
  )


def _discover_dataset_state(
  *,
  desired_dataset: DatasetState,
  dialect: Any,
  introspection_engine: Any,
  exec_engine: Any,
  warnings: list[str],
) -> DatasetState | None:
  """
  Discover one managed dataset from the physical target platform.
  """
  td = _target_dataset_for_state(desired_dataset)
  if td is None:
    warnings.append(
      f"TargetDataset metadata not found for desired state {desired_dataset.dataset_key}."
    )
    return None

  schema_name = getattr(getattr(td, "target_schema", None), "schema_name", None)
  table_name = getattr(td, "target_dataset_name", None)
  if not schema_name or not table_name:
    warnings.append(
      f"TargetDataset metadata incomplete for {desired_dataset.dataset_key}."
    )
    return None

  try:
    physical = dialect.introspect_table(
      schema_name=schema_name,
      table_name=table_name,
      introspection_engine=introspection_engine,
      exec_engine=exec_engine,
      debug_plan=False,
    )
  except Exception as exc:
    raise PhysicalArchitectureDiscoveryError(
      f"Could not introspect {schema_name}.{table_name}: {exc}"
    ) from exc

  if not bool((physical or {}).get("table_exists")):
    return None

  actual_cols = dict((physical or {}).get("actual_cols_by_norm_name") or {})
  column_states = _discover_column_states(
    desired_dataset=desired_dataset,
    target_dataset=td,
    actual_cols_by_norm_name=actual_cols,
    dialect=dialect,
  )

  return DatasetState(
    dataset_key=desired_dataset.dataset_key,
    schema_short_name=desired_dataset.schema_short_name,
    dataset_name=desired_dataset.dataset_name,
    materialization_type=desired_dataset.materialization_type,
    incremental_strategy=desired_dataset.incremental_strategy,
    historize=desired_dataset.historize,
    is_hist=desired_dataset.is_hist,
    active=desired_dataset.active,
    former_names=desired_dataset.former_names,
    column_states=tuple(column_states),
  )


def _discover_column_states(
  *,
  desired_dataset: DatasetState,
  target_dataset: TargetDataset,
  actual_cols_by_norm_name: dict[str, dict[str, Any]],
  dialect: Any,
) -> list[ColumnState]:
  """
  Build physical column states matched against desired metadata columns.
  """
  discovered: list[ColumnState] = []
  used_actual_keys: set[str] = set()
  target_columns_by_name = {
    _norm_name(getattr(col, "target_column_name", "")): col
    for col in target_dataset.target_columns.filter(active=True)
  }

  for desired_col in desired_dataset.column_states:
    actual_key, actual_col = _find_actual_column(
      desired_col=desired_col,
      actual_cols_by_norm_name=actual_cols_by_norm_name,
      used_actual_keys=used_actual_keys,
    )
    if actual_key is None or actual_col is None:
      continue

    used_actual_keys.add(actual_key)
    actual_name = _actual_column_name(actual_col, fallback=desired_col.column_name)
    target_col = target_columns_by_name.get(_norm_name(desired_col.column_name))
    datatype = _physical_baseline_datatype(
      desired_col=desired_col,
      target_column=target_col,
      actual_col=actual_col,
      dialect=dialect,
    )

    discovered.append(ColumnState(
      column_name=actual_name,
      datatype=datatype,
      nullable=_actual_nullable(actual_col, fallback=desired_col.nullable),
      active=desired_col.active,
      lineage_key=desired_col.lineage_key,
      former_names=desired_col.former_names,
      is_system_managed=desired_col.is_system_managed,
      system_role=desired_col.system_role,
    ))

  for actual_key in sorted(set(actual_cols_by_norm_name.keys()) - used_actual_keys):
    actual_col = actual_cols_by_norm_name[actual_key]
    actual_name = _actual_column_name(actual_col, fallback=actual_key)
    discovered.append(ColumnState(
      column_name=actual_name,
      datatype=_raw_physical_datatype(actual_col),
      nullable=_actual_nullable(actual_col, fallback=True),
      active=True,
      lineage_key=None,
      former_names=(),
      is_system_managed=False,
      system_role=None,
    ))

  return discovered


def _target_dataset_for_state(desired_dataset: DatasetState) -> TargetDataset | None:
  """
  Resolve the Django TargetDataset for one desired DatasetState.
  """
  return (
    TargetDataset.objects
    .select_related("target_schema")
    .prefetch_related("target_columns")
    .filter(
      target_schema__short_name=desired_dataset.schema_short_name,
      target_dataset_name=desired_dataset.dataset_name,
    )
    .first()
  )


def _find_actual_column(
  *,
  desired_col: ColumnState,
  actual_cols_by_norm_name: dict[str, dict[str, Any]],
  used_actual_keys: set[str],
) -> tuple[str | None, dict[str, Any] | None]:
  """
  Match a desired column against physical columns by name and former names.
  """
  candidates = [_norm_name(desired_col.column_name)]
  candidates.extend(_norm_name(name) for name in (desired_col.former_names or ()))

  for key in candidates:
    if not key or key in used_actual_keys:
      continue
    actual_col = actual_cols_by_norm_name.get(key)
    if actual_col is not None:
      return key, actual_col

  return None, None


def _physical_baseline_datatype(
  *,
  desired_col: ColumnState,
  target_column: Any | None,
  actual_col: dict[str, Any],
  dialect: Any,
) -> str | None:
  """
  Return the datatype recorded in a discovered physical baseline.

  Equivalent physical types are normalized back to the logical desired datatype
  to avoid false diffs such as STRING vs VARCHAR. Real drift is preserved as a
  physical marker so the Architecture Change Report can surface it.
  """
  actual_type = _raw_physical_datatype(actual_col)
  desired_physical = _desired_physical_type(
    target_column=target_column,
    dialect=dialect,
  )

  if not actual_type or not desired_physical:
    return desired_col.datatype

  dialect_name = (
    getattr(dialect, "DIALECT_NAME", None)
    or getattr(dialect, "dialect_name", None)
    or dialect.__class__.__name__
  )

  try:
    desired_can = canonicalize_type(str(dialect_name).lower(), desired_physical)
    actual_can = canonicalize_type(str(dialect_name).lower(), actual_type)
    kind, _reason = classify_type_drift(desired=desired_can, actual=actual_can)
    if kind == "equivalent":
      return desired_col.datatype

    return f"physical:{canonical_type_str(actual_can)}"
  except Exception:
    return desired_col.datatype


def _desired_physical_type(
  *,
  target_column: Any | None,
  dialect: Any,
) -> str | None:
  """
  Render the expected physical type for a TargetColumn.
  """
  if target_column is None or not hasattr(dialect, "map_logical_type"):
    return None

  try:
    return dialect.map_logical_type(
      datatype=getattr(target_column, "datatype", None),
      max_length=getattr(target_column, "max_length", None),
      precision=getattr(target_column, "decimal_precision", None),
      scale=getattr(target_column, "decimal_scale", None),
      strict=True,
    )
  except Exception:
    return None


def _raw_physical_datatype(actual_col: dict[str, Any]) -> str | None:
  """
  Return the raw physical datatype text from an introspected column.
  """
  value = (
    actual_col.get("type")
    or actual_col.get("datatype")
    or actual_col.get("data_type")
  )
  if value is None:
    return None
  return " ".join(str(value).strip().split())


def _actual_column_name(actual_col: dict[str, Any], *, fallback: str) -> str:
  """
  Return the physical column name from an introspected column.
  """
  return str(
    actual_col.get("name")
    or actual_col.get("column_name")
    or fallback
  ).strip()


def _actual_nullable(actual_col: dict[str, Any], *, fallback: bool) -> bool:
  """
  Return physical nullability from an introspected column if available.
  """
  if "nullable" in actual_col:
    return bool(actual_col.get("nullable"))

  if "notnull" in actual_col:
    return not bool(actual_col.get("notnull"))

  value = actual_col.get("is_nullable")
  if value is not None:
    if isinstance(value, str):
      return value.strip().lower() in {"yes", "true", "1"}
    return bool(value)

  return fallback


def _norm_name(value: str | None) -> str:
  """
  Normalize an identifier for physical matching.
  """
  return str(value or "").strip().lower()


def _dispose_if_possible(obj: Any) -> None:
  """
  Dispose or close an engine-like object when supported.
  """
  if obj is None:
    return

  for method_name in ("dispose", "close"):
    method = getattr(obj, method_name, None)
    if callable(method):
      try:
        method()
      except Exception:
        pass
      return
