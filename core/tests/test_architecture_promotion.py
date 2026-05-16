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

import json

import pytest

from metadata.architecture.promotion import (
  ArchitecturePromotionError,
  build_architecture_promotion_report,
  build_architecture_promotion_report_from_files,
)
from metadata.architecture.renderers import (
  render_architecture_promotion_report_json,
  render_architecture_promotion_report_text,
)
from metadata.architecture.state import ArchitectureState, ColumnState, DatasetState
from metadata.architecture.store import ArchitectureStateStore
from metadata.materialization.policy import MaterializationPolicy


def _policy(
  *,
  allow_auto_drop_columns: bool = False,
  allow_auto_drop_hist_columns: bool = False,
  allow_type_alter: bool = False,
) -> MaterializationPolicy:
  """
  Build materialization policy for promotion tests.
  """
  return MaterializationPolicy(
    sync_schema_shorts={"rawcore", "bizcore"},
    allow_auto_drop_columns=allow_auto_drop_columns,
    allow_auto_drop_hist_columns=allow_auto_drop_hist_columns,
    allow_type_alter=allow_type_alter,
  )


def _column(
  name: str,
  *,
  datatype: str = "string",
  nullable: bool = True,
  active: bool = True,
  lineage_key: str | None = None,
) -> ColumnState:
  """
  Build a column state for promotion tests.
  """
  return ColumnState(
    column_name=name,
    datatype=datatype,
    nullable=nullable,
    active=active,
    lineage_key=lineage_key or f"lk_{name}",
    former_names=(),
    is_system_managed=False,
    system_role=None,
  )


def _dataset(
  name: str,
  *,
  columns: tuple[ColumnState, ...],
  schema_short_name: str = "rawcore",
  historize: bool = False,
  is_hist: bool = False,
) -> DatasetState:
  """
  Build a dataset state for promotion tests.
  """
  return DatasetState(
    dataset_key=f"{schema_short_name}.{name}",
    schema_short_name=schema_short_name,
    dataset_name=name,
    materialization_type="table",
    incremental_strategy="full",
    historize=historize,
    is_hist=is_hist,
    active=True,
    former_names=(),
    column_states=columns,
  )


def _state(*datasets: DatasetState) -> ArchitectureState:
  """
  Build an architecture state for promotion tests.
  """
  return ArchitectureState(datasets=tuple(datasets))


def test_architecture_promotion_report_detects_added_column():
  source_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  target_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("customer_name"),
    ),
  ))

  report = build_architecture_promotion_report(
    source_state=source_state,
    target_state=target_state,
    policy=_policy(),
    source_label="dev",
    target_label="prod",
    relevant_dataset_keys={"rawcore.customer"},
    schema_short="rawcore",
  )

  assert report.source_label == "dev"
  assert report.target_label == "prod"
  assert report.has_changes is True
  assert report.is_blocked is False
  assert [a.action_type for a in report.change_report.migration_actions] == ["ADD_COLUMN"]
  assert [d.status for d in report.change_report.policy_decisions] == ["ALLOW"]
  assert len(report.promotion_fingerprint) == 64


def test_architecture_promotion_report_blocks_removed_column_by_policy():
  source_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("legacy_flag", datatype="boolean"),
    ),
  ))
  target_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))

  report = build_architecture_promotion_report(
    source_state=source_state,
    target_state=target_state,
    policy=_policy(),
    source_label="dev",
    target_label="prod",
    relevant_dataset_keys={"rawcore.customer"},
    schema_short="rawcore",
  )

  assert report.is_blocked is True
  assert [a.action_type for a in report.change_report.migration_actions] == ["DROP_COLUMN"]
  assert [d.code for d in report.change_report.policy_decisions] == ["COLUMN_DROP_DISABLED"]


def test_architecture_promotion_report_fingerprint_is_stable():
  source_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  target_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("customer_name"),
    ),
  ))

  report_1 = build_architecture_promotion_report(
    source_state=source_state,
    target_state=target_state,
    policy=_policy(),
    relevant_dataset_keys={"rawcore.customer"},
  )
  report_2 = build_architecture_promotion_report(
    source_state=source_state,
    target_state=target_state,
    policy=_policy(),
    relevant_dataset_keys={"rawcore.customer"},
  )

  assert report_1.promotion_fingerprint == report_2.promotion_fingerprint
  assert report_1.to_dict()["promotion_fingerprint"] == report_1.promotion_fingerprint


def test_architecture_promotion_report_from_files(tmp_path):
  source_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  target_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("customer_name"),
    ),
  ))
  source_path = tmp_path / "dev" / "architecture_state.json"
  target_path = tmp_path / "prod" / "architecture_state.json"
  ArchitectureStateStore.save_file(source_path, source_state)
  ArchitectureStateStore.save_file(target_path, target_state)

  report = build_architecture_promotion_report_from_files(
    source_path=source_path,
    target_path=target_path,
    policy=_policy(),
    source_label="dev",
    target_label="prod",
    relevant_dataset_keys={"rawcore.customer"},
  )

  assert report.source_label == "dev"
  assert report.target_label == "prod"
  assert report.has_changes is True
  assert [a.action_type for a in report.change_report.migration_actions] == ["ADD_COLUMN"]


def test_architecture_promotion_report_from_files_raises_for_missing_source(tmp_path):
  target_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  target_path = tmp_path / "prod" / "architecture_state.json"
  ArchitectureStateStore.save_file(target_path, target_state)

  with pytest.raises(ArchitecturePromotionError) as exc_info:
    build_architecture_promotion_report_from_files(
      source_path=tmp_path / "dev" / "architecture_state.json",
      target_path=target_path,
      policy=_policy(),
    )

  assert "could not be read" in str(exc_info.value)


def test_architecture_promotion_report_renderers_are_deterministic():
  source_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  target_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("customer_name"),
    ),
  ))
  report = build_architecture_promotion_report(
    source_state=source_state,
    target_state=target_state,
    policy=_policy(),
    source_label="dev",
    target_label="prod",
    relevant_dataset_keys={"rawcore.customer"},
  )

  json_1 = render_architecture_promotion_report_json(report)
  json_2 = render_architecture_promotion_report_json(report)
  text = render_architecture_promotion_report_text(report)

  assert json_1 == json_2
  assert json.loads(json_1)["promotion_fingerprint"] == report.promotion_fingerprint
  assert "Architecture Promotion Report" in text
  assert "source: dev" in text
  assert "target: prod" in text
  assert report.promotion_fingerprint in text