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
  # Keep it simple: NOTICE returns the same string
  def NOTICE(self, s: str) -> str:
    return s


class DummyStdout:
  def __init__(self):
    self.lines = []

  def write(self, s: str):
    # Django's stdout.write receives strings; we store them verbatim.
    self.lines.append(str(s))


def test_print_execution_plan_all_mode_does_not_flood_stdout():
  cmd = Command()
  cmd.style = DummyStyle()
  cmd.stdout = DummyStdout()

  # Build a large execution_order to trigger head/tail printing.
  schema = DummySchema("raw")
  execution_order = [DummyTD(schema, f"ds_{i:03d}") for i in range(1, 101)]

  cmd._print_execution_plan(
    execution_order=execution_order,
    batch_run_id="batch-123",
    all_datasets=True,
    schema_short="raw",
    no_print=False,
  )

  lines = cmd.stdout.lines

  # Expected shape:
  # blank line
  # header line
  # 25 head dataset lines
  # ellipsis line
  # 10 tail dataset lines
  # blank line
  #
  # Total: 1 + 1 + 25 + 1 + 10 + 1 = 39
  assert len(lines) == 39

  # Header includes all-mode marker + schema scope
  assert "Execution plan (all datasets, schema=raw, batch_run_id=batch-123):" in lines[1]

  # Verify first head + last tail entries are present
  assert "raw.ds_001" in lines[2]
  assert "raw.ds_025" in lines[26]
  assert "... (65 more)" in lines[27]
  assert "raw.ds_091" in lines[28]
  assert "raw.ds_100" in lines[-2]
