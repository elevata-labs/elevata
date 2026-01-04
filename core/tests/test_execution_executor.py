"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2026 Ilona Tag

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

import logging

import pytest

from metadata.execution.executor import (
  ExecutionPlan,
  ExecutionPolicy,
  ExecutionStep,
  execute_plan,
)


class FakeSchema:
  def __init__(self, short_name: str):
    self.short_name = short_name


class FakeTargetDataset:
  def __init__(self, id: int, schema_short: str, dataset_name: str):
    self.id = id
    self.target_schema = FakeSchema(schema_short)
    self.target_dataset_name = dataset_name


def test_execute_plan_success_runs_all_in_order():
  td1 = FakeTargetDataset(1, "raw", "a")
  td2 = FakeTargetDataset(2, "core", "b")

  plan = ExecutionPlan(
    batch_run_id="batch-1",
    steps=[
      ExecutionStep(dataset_id=1, dataset_key="raw.a", upstream_keys=()),
      ExecutionStep(dataset_id=2, dataset_key="core.b", upstream_keys=("raw.a",)),
    ],
  )

  calls: list[str] = []

  def run_dataset_fn(**kwargs):
    td = kwargs["target_dataset"]
    calls.append(td.target_dataset_name)
    return {
      "status": "success",
      "kind": "ok",
      "dataset": f"{td.target_schema.short_name}.{td.target_dataset_name}",
    }

  results, had_error = execute_plan(
    plan=plan,
    execution_order=[td1, td2],
    policy=ExecutionPolicy(continue_on_error=False, max_retries=0),
    execute=True,
    root_td=td1,
    root_load_run_id="root",
    root_load_plan=None,
    run_dataset_fn=run_dataset_fn,
    logger=logging.getLogger(__name__),
  )

  assert had_error is False
  assert calls == ["a", "b"]
  assert [r["status"] for r in results] == ["success", "success"]


def test_execute_plan_blocks_downstream_when_upstream_errors():
  td1 = FakeTargetDataset(1, "raw", "a")
  td2 = FakeTargetDataset(2, "core", "b")

  plan = ExecutionPlan(
    batch_run_id="batch-1",
    steps=[
      ExecutionStep(dataset_id=1, dataset_key="raw.a", upstream_keys=()),
      ExecutionStep(dataset_id=2, dataset_key="core.b", upstream_keys=("raw.a",)),
    ],
  )

  def run_dataset_fn(**kwargs):
    td = kwargs["target_dataset"]
    if td.target_dataset_name == "a":
      raise RuntimeError("boom")
    pytest.fail("downstream should not run when blocked")

  results, had_error = execute_plan(
    plan=plan,
    execution_order=[td1, td2],
    policy=ExecutionPolicy(continue_on_error=True, max_retries=0),
    execute=True,
    root_td=td1,
    root_load_run_id="root",
    root_load_plan=None,
    run_dataset_fn=run_dataset_fn,
    logger=logging.getLogger(__name__),
  )

  assert had_error is True
  assert results[0]["status"] == "error"
  assert results[0]["dataset"] == "raw.a"
  assert results[1]["status"] == "skipped"
  assert results[1]["kind"] == "blocked"
  assert results[1]["dataset"] == "core.b"
  assert results[1]["blocked_by"] == "raw.a"


def test_execute_plan_retries_until_success_in_execute_mode():
  td1 = FakeTargetDataset(1, "raw", "a")

  plan = ExecutionPlan(
    batch_run_id="batch-1",
    steps=[
      ExecutionStep(dataset_id=1, dataset_key="raw.a", upstream_keys=()),
    ],
  )

  attempts = {"count": 0}
  seen_attempt_nos: list[int] = []

  def run_dataset_fn(**kwargs):
    attempts["count"] += 1
    seen_attempt_nos.append(int(kwargs["attempt_no"]))
    if attempts["count"] < 3:
      raise RuntimeError("temporary")
    return {"status": "success", "kind": "ok", "dataset": "raw.a"}

  results, had_error = execute_plan(
    plan=plan,
    execution_order=[td1],
    policy=ExecutionPolicy(continue_on_error=False, max_retries=2),
    execute=True,
    root_td=td1,
    root_load_run_id="root",
    root_load_plan=None,
    run_dataset_fn=run_dataset_fn,
    logger=logging.getLogger(__name__),
  )

  assert had_error is False
  assert attempts["count"] == 3
  assert seen_attempt_nos == [1, 2, 3]
  assert results[0]["status"] == "success"


def test_execute_plan_does_not_retry_in_dry_run_mode():
  td1 = FakeTargetDataset(1, "raw", "a")

  plan = ExecutionPlan(
    batch_run_id="batch-1",
    steps=[
      ExecutionStep(dataset_id=1, dataset_key="raw.a", upstream_keys=()),
    ],
  )

  attempts = {"count": 0}

  def run_dataset_fn(**kwargs):
    attempts["count"] += 1
    raise RuntimeError("fail")

  results, had_error = execute_plan(
    plan=plan,
    execution_order=[td1],
    policy=ExecutionPolicy(continue_on_error=True, max_retries=5),
    execute=False,  # dry run => no retries
    root_td=td1,
    root_load_run_id="root",
    root_load_plan=None,
    run_dataset_fn=run_dataset_fn,
    logger=logging.getLogger(__name__),
  )

  assert had_error is True
  assert attempts["count"] == 1
  assert results[0]["status"] == "error"

def test_execute_plan_fail_fast_marks_remaining_as_aborted():
  td1 = FakeTargetDataset(1, "raw", "a")
  td2 = FakeTargetDataset(2, "core", "b")

  plan = ExecutionPlan(
    batch_run_id="batch-1",
    steps=[
      ExecutionStep(dataset_id=1, dataset_key="raw.a", upstream_keys=()),
      ExecutionStep(dataset_id=2, dataset_key="core.b", upstream_keys=("raw.a",)),
    ],
  )

  calls: list[str] = []

  def run_dataset_fn(**kwargs):
    td = kwargs["target_dataset"]
    calls.append(td.target_dataset_name)
    if td.target_dataset_name == "a":
      raise RuntimeError("boom")
    pytest.fail("downstream should not run in fail-fast mode")

  results, had_error = execute_plan(
    plan=plan,
    execution_order=[td1, td2],
    policy=ExecutionPolicy(continue_on_error=False, max_retries=0),
    execute=True,
    root_td=td1,
    root_load_run_id="root",
    root_load_plan=None,
    run_dataset_fn=run_dataset_fn,
    logger=logging.getLogger(__name__),
  )

  assert had_error is True
  assert calls == ["a"]

  assert len(results) == 2
  assert results[0]["status"] == "error"
  assert results[0]["dataset"] == "raw.a"

  assert results[1]["status"] == "skipped"
  assert results[1]["kind"] == "aborted"
  assert results[1]["dataset"] == "core.b"
  assert results[1]["status_reason"] == "fail_fast_abort"
