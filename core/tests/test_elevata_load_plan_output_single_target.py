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

from dataclasses import dataclass

from metadata.management.commands.elevata_load import Command


@dataclass(frozen=True)
class DummySchema:
  short_name: str


@dataclass(frozen=True)
class DummyTD:
  target_schema: DummySchema
  target_dataset_name: str


class DummyStyle:
  def NOTICE(self, s: str) -> str:
    return s


class DummyStdout:
  def __init__(self):
    self.lines = []

  def write(self, s: str):
    self.lines.append(str(s))


def test_print_execution_plan_single_target_prints_all_without_guardrails():
  cmd = Command()
  cmd.style = DummyStyle()
  cmd.stdout = DummyStdout()

  schema = DummySchema("core")
  execution_order = [
    DummyTD(schema, "customers"),
    DummyTD(schema, "orders"),
    DummyTD(schema, "order_items"),
  ]

  cmd._print_execution_plan(
    execution_order=execution_order,
    batch_run_id="batch-single",
    all_datasets=False,
    schema_short=None,
    no_print=False,
  )

  lines = cmd.stdout.lines

  # Expected shape:
  # blank
  # header
  # 3 dataset lines
  # blank
  assert len(lines) == 1 + 1 + len(execution_order) + 1

  # Header is the classic one (no all-datasets wording)
  assert lines[1] == "Execution plan (batch_run_id=batch-single):"

  # All datasets are printed
  assert lines[2] == "  1. core.customers"
  assert lines[3] == "  2. core.orders"
  assert lines[4] == "  3. core.order_items"

  # No ellipsis anywhere
  assert not any("... (" in ln for ln in lines)
