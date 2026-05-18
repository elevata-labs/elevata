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

from metadata.architecture.report_builder import build_architecture_change_report
from metadata.architecture.state import ArchitectureState, ColumnState, DatasetState
from metadata.materialization.policy import MaterializationPolicy


def _policy(
  *,
  allow_auto_drop_columns: bool = False,
  allow_auto_drop_hist_columns: bool = False,
  allow_type_alter: bool = False,
) -> MaterializationPolicy:
  """
  Build materialization policy for architecture report tests.
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
  Build a column state for architecture report tests.
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
  Build a dataset state for architecture report tests.
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
  Build an architecture state for architecture report tests.
  """
  return ArchitectureState(datasets=tuple(datasets))


def test_architecture_change_report_allows_added_column():
  previous_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  current_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("customer_name"),
    ),
  ))

  report = build_architecture_change_report(
    previous_state=previous_state,
    current_state=current_state,
    policy=_policy(),
    relevant_dataset_keys={"rawcore.customer"},
    schema_short="rawcore",
    target_name="customer",
  )

  assert report.has_changes is True
  assert [a.action_type for a in report.migration_actions] == ["ADD_COLUMN"]
  assert [d.status for d in report.policy_decisions] == ["ALLOW"]
  assert report.is_blocked is False
  assert report.summary.column_change_count == 1


def test_architecture_change_report_blocks_removed_base_column_by_default():
  previous_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("legacy_flag", datatype="boolean"),
    ),
  ))
  current_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))

  report = build_architecture_change_report(
    previous_state=previous_state,
    current_state=current_state,
    policy=_policy(),
    relevant_dataset_keys={"rawcore.customer"},
    schema_short="rawcore",
    target_name="customer",
  )

  assert [a.action_type for a in report.migration_actions] == ["DROP_COLUMN"]
  assert [d.status for d in report.policy_decisions] == ["BLOCKED_BY_POLICY"]
  assert [d.code for d in report.policy_decisions] == ["COLUMN_DROP_DISABLED"]
  assert report.is_blocked is True
  assert report.summary.blocking_policy_decision_count == 1


def test_architecture_change_report_allows_removed_base_column_when_policy_allows_drop():
  previous_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("legacy_flag", datatype="boolean"),
    ),
  ))
  current_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))

  report = build_architecture_change_report(
    previous_state=previous_state,
    current_state=current_state,
    policy=_policy(allow_auto_drop_columns=True),
    relevant_dataset_keys={"rawcore.customer"},
    schema_short="rawcore",
    target_name="customer",
  )

  assert [a.action_type for a in report.migration_actions] == ["DROP_COLUMN"]
  assert [d.status for d in report.policy_decisions] == ["ALLOW"]
  assert [d.code for d in report.policy_decisions] == ["DROP_COLUMN_ALLOWED"]
  assert report.is_blocked is False


def test_architecture_change_report_blocks_removed_hist_column_without_hist_policy():
  previous_state = _state(_dataset(
    "customer_hist",
    is_hist=True,
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("legacy_flag", datatype="boolean"),
    ),
  ))
  current_state = _state(_dataset(
    "customer_hist",
    is_hist=True,
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))

  report = build_architecture_change_report(
    previous_state=previous_state,
    current_state=current_state,
    policy=_policy(allow_auto_drop_columns=True),
    relevant_dataset_keys={"rawcore.customer_hist"},
    schema_short="rawcore",
    target_name="customer_hist",
  )

  assert [a.action_type for a in report.migration_actions] == ["DROP_COLUMN"]
  assert [d.status for d in report.policy_decisions] == ["BLOCKED_BY_POLICY"]
  assert [d.code for d in report.policy_decisions] == ["HIST_COLUMN_DROP_DISABLED"]
  assert report.is_blocked is True


def test_architecture_change_report_marks_retired_column_as_metadata_only():
  previous_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("legacy_flag", datatype="boolean", active=True),
    ),
  ))
  current_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("legacy_flag", datatype="boolean", active=False),
    ),
  ))

  report = build_architecture_change_report(
    previous_state=previous_state,
    current_state=current_state,
    policy=_policy(),
    relevant_dataset_keys={"rawcore.customer"},
    schema_short="rawcore",
    target_name="customer",
  )

  assert [a.action_type for a in report.migration_actions] == ["RETIRE_COLUMN"]
  assert [d.status for d in report.policy_decisions] == ["METADATA_ONLY"]
  assert [d.code for d in report.policy_decisions] == ["RETIRE_COLUMN_METADATA_ONLY"]
  assert report.is_blocked is False

def test_architecture_change_report_fingerprint_is_stable():
  previous_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))
  current_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("customer_name"),
    ),
  ))

  report_1 = build_architecture_change_report(
    previous_state=previous_state,
    current_state=current_state,
    policy=_policy(),
    relevant_dataset_keys={"rawcore.customer"},
    schema_short="rawcore",
    target_name="customer",
  )
  report_2 = build_architecture_change_report(
    previous_state=previous_state,
    current_state=current_state,
    policy=_policy(),
    relevant_dataset_keys={"rawcore.customer"},
    schema_short="rawcore",
    target_name="customer",
  )

  assert report_1.report_fingerprint == report_2.report_fingerprint
  assert len(report_1.report_fingerprint) == 64
  assert report_1.to_dict()["report_fingerprint"] == report_1.report_fingerprint


def test_architecture_change_report_fingerprint_changes_with_policy_decision():
  previous_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
      _column("legacy_flag", datatype="boolean"),
    ),
  ))
  current_state = _state(_dataset(
    "customer",
    columns=(
      _column("customer_id", datatype="integer", nullable=False),
    ),
  ))

  blocked_report = build_architecture_change_report(
    previous_state=previous_state,
    current_state=current_state,
    policy=_policy(),
    relevant_dataset_keys={"rawcore.customer"},
    schema_short="rawcore",
    target_name="customer",
  )
  allowed_report = build_architecture_change_report(
    previous_state=previous_state,
    current_state=current_state,
    policy=_policy(allow_auto_drop_columns=True),
    relevant_dataset_keys={"rawcore.customer"},
    schema_short="rawcore",
    target_name="customer",
  )

  assert blocked_report.report_fingerprint != allowed_report.report_fingerprint


def test_architecture_change_report_filters_changes_to_report_scope():
  previous_state = _state(
    _dataset(
      "customer",
      columns=(
        _column("customer_id", datatype="integer", nullable=False),
      ),
    ),
    _dataset(
      "product",
      columns=(
        _column("product_id", datatype="integer", nullable=False),
      ),
    ),
  )
  current_state = _state(
    _dataset(
      "customer",
      columns=(
        _column("customer_id", datatype="integer", nullable=False),
        _column("customer_name"),
      ),
    ),
    _dataset(
      "product",
      columns=(
        _column("product_id", datatype="integer", nullable=False),
        _column("product_name"),
      ),
    ),
  )

  report = build_architecture_change_report(
    previous_state=previous_state,
    current_state=current_state,
    policy=_policy(),
    relevant_dataset_keys={"rawcore.customer"},
    schema_short="rawcore",
    target_name="customer",
  )

  assert report.scope.mode == "scoped"
  assert [c.dataset_key for c in report.column_changes] == ["rawcore.customer"]
  assert [a.dataset_key for a in report.migration_actions] == ["rawcore.customer"]
  assert report.summary.column_change_count == 1


def test_architecture_change_report_all_scope_keeps_all_changes():
  previous_state = _state(
    _dataset(
      "customer",
      columns=(
        _column("customer_id", datatype="integer", nullable=False),
      ),
    ),
    _dataset(
      "product",
      columns=(
        _column("product_id", datatype="integer", nullable=False),
      ),
    ),
  )
  current_state = _state(
    _dataset(
      "customer",
      columns=(
        _column("customer_id", datatype="integer", nullable=False),
        _column("customer_name"),
      ),
    ),
    _dataset(
      "product",
      columns=(
        _column("product_id", datatype="integer", nullable=False),
        _column("product_name"),
      ),
    ),
  )

  report = build_architecture_change_report(
    previous_state=previous_state,
    current_state=current_state,
    policy=_policy(),
    relevant_dataset_keys=None,
  )

  assert report.scope.mode == "all"
  assert [c.dataset_key for c in report.column_changes] == [
    "rawcore.customer",
    "rawcore.product",
  ]
  assert report.summary.column_change_count == 2