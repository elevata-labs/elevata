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

from types import SimpleNamespace

import pytest

from metadata.architecture.scope import (
  ArchitectureScopeError,
  dataset_key_from_execution_item,
  dataset_keys_from_execution_items,
  expand_dataset_keys_with_hist,
  resolve_dataset_keys_from_state,
)
from metadata.architecture.state import ArchitectureState, ColumnState, DatasetState


def _column(name: str) -> ColumnState:
  """
  Build a column state for scope tests.
  """
  return ColumnState(
    column_name=name,
    datatype="string",
    nullable=True,
    active=True,
    lineage_key=f"lk_{name}",
    former_names=(),
    is_system_managed=False,
    system_role=None,
  )


def _dataset(
  name: str,
  *,
  schema_short_name: str = "rawcore",
  historize: bool = False,
  is_hist: bool = False,
) -> DatasetState:
  """
  Build a dataset state for scope tests.
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
    column_states=(_column("id"),),
  )


def _state(*datasets: DatasetState) -> ArchitectureState:
  """
  Build an architecture state for scope tests.
  """
  return ArchitectureState(datasets=tuple(datasets))


def test_dataset_key_from_execution_item_accepts_string():
  assert dataset_key_from_execution_item("rawcore.customer") == "rawcore.customer"


def test_dataset_key_from_execution_item_accepts_architecture_state_dataset():
  dataset = _dataset("customer")

  assert dataset_key_from_execution_item(dataset) == "rawcore.customer"


def test_dataset_key_from_execution_item_accepts_target_dataset_shape():
  item = SimpleNamespace(
    target_schema=SimpleNamespace(short_name="rawcore"),
    target_dataset_name="customer",
  )

  assert dataset_key_from_execution_item(item) == "rawcore.customer"


def test_dataset_key_from_execution_item_accepts_wrapped_target_dataset_shape():
  item = SimpleNamespace(
    target_dataset=SimpleNamespace(
      target_schema=SimpleNamespace(short_name="rawcore"),
      target_dataset_name="customer",
    ),
  )

  assert dataset_key_from_execution_item(item) == "rawcore.customer"


def test_expand_dataset_keys_with_hist_adds_related_history_dataset():
  state = _state(
    _dataset("customer", historize=True),
    _dataset("customer_hist", is_hist=True),
  )

  keys = expand_dataset_keys_with_hist(
    state=state,
    dataset_keys={"rawcore.customer"},
  )

  assert keys == {
    "rawcore.customer",
    "rawcore.customer_hist",
  }


def test_expand_dataset_keys_with_hist_keeps_base_key_without_history_dataset():
  state = _state(
    _dataset("customer", historize=True),
  )

  keys = expand_dataset_keys_with_hist(
    state=state,
    dataset_keys={"rawcore.customer"},
  )

  assert keys == {"rawcore.customer"}


def test_expand_dataset_keys_with_hist_does_not_expand_history_dataset():
  state = _state(
    _dataset("customer", historize=True),
    _dataset("customer_hist", is_hist=True),
  )

  keys = expand_dataset_keys_with_hist(
    state=state,
    dataset_keys={"rawcore.customer_hist"},
  )

  assert keys == {"rawcore.customer_hist"}


def test_dataset_keys_from_execution_items_expands_history_dataset():
  state = _state(
    _dataset("customer", historize=True),
    _dataset("customer_hist", is_hist=True),
  )

  keys = dataset_keys_from_execution_items(
    ["rawcore.customer"],
    architecture_state=state,
  )

  assert keys == {
    "rawcore.customer",
    "rawcore.customer_hist",
  }


def test_resolve_dataset_keys_from_state_selects_schema_scoped_target_name():
  state = _state(
    _dataset("customer", schema_short_name="rawcore"),
    _dataset("customer", schema_short_name="bizcore"),
  )

  keys = resolve_dataset_keys_from_state(
    state=state,
    target_name="customer",
    schema_short="bizcore",
    all_datasets=False,
  )

  assert keys == {"bizcore.customer"}


def test_resolve_dataset_keys_from_state_detects_ambiguous_target_name():
  state = _state(
    _dataset("customer", schema_short_name="rawcore"),
    _dataset("customer", schema_short_name="bizcore"),
  )

  with pytest.raises(ArchitectureScopeError) as exc_info:
    resolve_dataset_keys_from_state(
      state=state,
      target_name="customer",
      schema_short=None,
      all_datasets=False,
    )

  assert "ambiguous" in str(exc_info.value)


def test_resolve_dataset_keys_from_state_selects_all_schema_datasets():
  state = _state(
    _dataset("customer", schema_short_name="rawcore"),
    _dataset("product", schema_short_name="rawcore"),
    _dataset("sales", schema_short_name="bizcore"),
  )

  keys = resolve_dataset_keys_from_state(
    state=state,
    target_name=None,
    schema_short="rawcore",
    all_datasets=True,
  )

  assert keys == {
    "rawcore.customer",
    "rawcore.product",
  }


def test_resolve_dataset_keys_from_state_expands_history_dataset():
  state = _state(
    _dataset("customer", historize=True),
    _dataset("customer_hist", is_hist=True),
  )

  keys = resolve_dataset_keys_from_state(
    state=state,
    target_name="customer",
    schema_short="rawcore",
    all_datasets=False,
  )

  assert keys == {
    "rawcore.customer",
    "rawcore.customer_hist",
  }