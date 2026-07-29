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

from pathlib import Path
from types import SimpleNamespace

import pytest

from metadata.architecture import physical_state
from metadata.architecture.state import (
  ArchitectureState,
  ColumnState,
  DatasetState,
)


class _MissingStateStore:
  """
  Architecture State store test double without a recorded baseline.
  """

  def __init__(self, base_path: Path):
    self.base_path = base_path

  def state_file_path(self) -> Path:
    return self.base_path / "architecture_state.json"

  def load(self):
    return None


def _dataset(
  dataset_key: str = "raw.customer",
) -> DatasetState:
  """
  Return one current managed dataset state.
  """
  schema_short, dataset_name = dataset_key.split(".", 1)
  return DatasetState(
    dataset_key=dataset_key,
    schema_short_name=schema_short,
    dataset_name=dataset_name,
    materialization_type="table",
    incremental_strategy="full",
    historize=False,
    is_hist=False,
    active=True,
    column_states=(
      ColumnState(
        column_name="customer_id",
        datatype="integer",
        nullable=False,
        active=True,
        lineage_key="customer_id",
      ),
    ),
  )


@pytest.mark.parametrize(
  (
    "relevant_dataset_keys",
    "expected_source",
    "expected_initial_deployment",
  ),
  (
    (
      None,
      "verified_empty_target",
      True,
    ),
    (
      {"raw.customer"},
      "discovered_physical_state",
      False,
    ),
  ),
)
def test_empty_physical_discovery_only_bootstraps_complete_scope(
  monkeypatch,
  tmp_path,
  relevant_dataset_keys,
  expected_source,
  expected_initial_deployment,
) -> None:
  """
  Verify an empty scoped discovery cannot authorize initial deployment.
  """
  current_state = ArchitectureState(
    datasets=(_dataset(),)
  )
  empty_state = ArchitectureState(datasets=())

  monkeypatch.setattr(
    physical_state,
    "discover_physical_architecture_state",
    lambda **kwargs: (
      physical_state.PhysicalArchitectureDiscoveryResult(
        state=empty_state,
      )
    ),
  )

  result = physical_state.resolve_architecture_baseline(
    current_state=current_state,
    state_store=_MissingStateStore(tmp_path),
    relevant_dataset_keys=relevant_dataset_keys,
  )

  assert result.previous_state is empty_state
  assert result.source == expected_source
  assert result.can_execute is True
  assert result.is_discovered is True
  assert result.is_initial_deployment is expected_initial_deployment


def test_nonempty_physical_discovery_never_becomes_initial_deployment(
  monkeypatch,
  tmp_path,
) -> None:
  """
  Verify deleting a state file cannot hide an existing managed target.
  """
  current_state = ArchitectureState(
    datasets=(_dataset(),)
  )

  monkeypatch.setattr(
    physical_state,
    "discover_physical_architecture_state",
    lambda **kwargs: (
      physical_state.PhysicalArchitectureDiscoveryResult(
        state=current_state,
      )
    ),
  )

  result = physical_state.resolve_architecture_baseline(
    current_state=current_state,
    state_store=_MissingStateStore(tmp_path),
    relevant_dataset_keys=None,
  )

  assert result.source == "discovered_physical_state"
  assert result.is_initial_deployment is False
  assert result.previous_state is current_state


class _MssqlDialect:
  """
  Minimal SQL Server dialect test double for physical type comparison.
  """

  DIALECT_NAME = "mssql"

  def map_logical_type(
    self,
    *,
    datatype,
    max_length=None,
    precision=None,
    scale=None,
    strict=True,
  ) -> str:
    assert str(datatype or "").upper() == "STRING"
    assert strict is True
    return f"NVARCHAR({int(max_length)})" if max_length else "NVARCHAR(MAX)"


def _string_column_state() -> ColumnState:
  """
  Return the logical source-identity column used by view discovery tests.
  """
  return ColumnState(
    column_name="source_identity_id",
    datatype="STRING",
    nullable=False,
    active=True,
    lineage_key=None,
    is_system_managed=True,
    system_role="business_key",
  )


def _string_target_column() -> SimpleNamespace:
  """
  Return TargetColumn-shaped metadata with the declared logical capacity.
  """
  return SimpleNamespace(
    datatype="STRING",
    max_length=30,
    decimal_precision=None,
    decimal_scale=None,
  )


def test_view_literal_width_is_not_physical_architecture_drift() -> None:
  """
  Verify SQL Server literal-width inference does not invalidate a managed view.
  """
  datatype = physical_state._physical_baseline_datatype(
    desired_col=_string_column_state(),
    target_column=_string_target_column(),
    actual_col={
      "type": "nvarchar(3)",
      "physical_object_type": "view",
    },
    physical_object_type="view",
    dialect=_MssqlDialect(),
  )

  assert datatype == "STRING"


def test_table_string_width_drift_remains_visible() -> None:
  """
  Verify an enforced table width remains part of physical drift detection.
  """
  datatype = physical_state._physical_baseline_datatype(
    desired_col=_string_column_state(),
    target_column=_string_target_column(),
    actual_col={
      "type": "nvarchar(3)",
      "physical_object_type": "table",
    },
    physical_object_type="table",
    dialect=_MssqlDialect(),
  )

  assert datatype == "physical:STRING(3)"


def test_view_incompatible_datatype_remains_visible() -> None:
  """
  Verify view normalization never hides a different logical type family.
  """
  datatype = physical_state._physical_baseline_datatype(
    desired_col=_string_column_state(),
    target_column=_string_target_column(),
    actual_col={
      "type": "int",
      "physical_object_type": "view",
    },
    physical_object_type="view",
    dialect=_MssqlDialect(),
  )

  assert datatype == "physical:INTEGER"
