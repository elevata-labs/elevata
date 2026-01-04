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


def test_print_execution_plan_all_mode_small_prints_all_without_ellipsis():
  cmd = Command()
  cmd.style = DummyStyle()
  cmd.stdout = DummyStdout()

  schema = DummySchema("stage")
  # Small enough to stay below head+tail+5 threshold (25+10+5 = 40)
  execution_order = [DummyTD(schema, f"ds_{i:02d}") for i in range(1, 13)]

  cmd._print_execution_plan(
    execution_order=execution_order,
    batch_run_id="batch-small",
    all_datasets=True,
    schema_short=None,
    no_print=False,
  )

  lines = cmd.stdout.lines

  # Shape:
  # blank line
  # header line
  # N dataset lines
  # blank line
  assert len(lines) == 1 + 1 + len(execution_order) + 1

  # No ellipsis line
  assert not any("... (" in ln for ln in lines)

  # Header without schema scope
  assert "Execution plan (all datasets, batch_run_id=batch-small):" in lines[1]

  # All datasets present
  assert "stage.ds_01" in lines[2]
  assert "stage.ds_12" in lines[-2]
