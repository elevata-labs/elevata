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

  def WARNING(self, s: str) -> str:
    return s


class DummyStdout:
  def __init__(self):
    self.lines = []

  def write(self, s: str):
    self.lines.append(str(s))


def test_print_execution_plan_all_mode_respects_head_tail_constants():
  cmd = Command()
  cmd.style = DummyStyle()
  cmd.stdout = DummyStdout()

  # Override guard constants explicitly
  cmd.PLAN_PRINT_HEAD = 2
  cmd.PLAN_PRINT_TAIL = 1

  schema = DummySchema("raw")
  execution_order = [
    DummyTD(schema, f"ds_{i}") for i in range(1, 11)
  ]  # 10 datasets

  cmd._print_execution_plan(
    execution_order=execution_order,
    batch_run_id="batch-guard",
    all_datasets=True,
    schema_short=None,
    no_print=False,
  )

  lines = cmd.stdout.lines

  # Expected:
  # blank
  # header
  # head (2)
  # ellipsis
  # tail (1)
  # blank
  assert len(lines) == 1 + 1 + 2 + 1 + 1 + 1

  assert lines[2] == "  1. raw.ds_1"
  assert lines[3] == "  2. raw.ds_2"

  # Ellipsis reflects correct omitted count: 10 - 2 - 1 = 7
  assert "... (7 more)" in lines[4]

  # Tail
  assert lines[5] == "  10. raw.ds_10"
