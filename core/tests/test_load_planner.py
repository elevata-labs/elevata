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

import pytest

from metadata.rendering.load_planner import build_load_plan, LoadPlan


class DummySchema:
  def __init__(self, short_name: str = "rawcore"):
    self.short_name = short_name


class DummyTargetDataset:
  def __init__(
    self,
    name="rc_customer",
    schema_short="rawcore",
    materialization_type="table",
    incremental_strategy="full",
    natural_key_fields=None,
    incremental_source=None,
    handle_deletes=False,
    historize=False,
  ):
    self.target_dataset_name = name
    self.target_schema = DummySchema(schema_short)
    self.materialization_type = materialization_type
    self.incremental_strategy = incremental_strategy
    self.natural_key_fields = natural_key_fields or []
    self.incremental_source = incremental_source
    self.handle_deletes = handle_deletes
    self.historize = historize


def test_build_load_plan_rawcore_merge_with_deletes():
  src = object()  # just a non-None marker

  td = DummyTargetDataset(
    name="rc_customer",
    schema_short="rawcore",
    incremental_strategy="merge",
    natural_key_fields=["customer_id"],
    incremental_source=src,
    handle_deletes=True,
    historize=True,
  )

  plan = build_load_plan(td)

  assert isinstance(plan, LoadPlan)
  assert plan.mode == "merge"
  assert plan.handle_deletes is True
  assert plan.historize is True


def test_build_load_plan_rawcore_merge_without_keys_falls_back_to_full():
  src = object()

  td = DummyTargetDataset(
    name="rc_customer",
    schema_short="rawcore",
    incremental_strategy="merge",
    natural_key_fields=[],  # missing keys
    incremental_source=src,
    handle_deletes=True,
  )

  plan = build_load_plan(td)

  assert plan.mode == "full"
  assert plan.handle_deletes is False


def test_build_load_plan_hist_dataset_never_uses_merge():
  src = object()

  td = DummyTargetDataset(
    name="rc_customer_hist",
    schema_short="rawcore",
    incremental_strategy="merge",
    natural_key_fields=["customer_id"],
    incremental_source=src,
    handle_deletes=True,
  )

  plan = build_load_plan(td)

  # For now, history loads are not implemented and default to full.
  assert plan.mode == "full"
  assert plan.handle_deletes is False
  assert plan.historize is False
