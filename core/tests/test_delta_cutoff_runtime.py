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

from datetime import datetime, timezone

import pytest

from metadata.management.commands.elevata_load import apply_runtime_placeholders


class FakeDialect:
  # Minimal literal rendering
  def render_literal(self, value):
    if isinstance(value, str):
      return "'" + value.replace("'", "''") + "'"
    if isinstance(value, datetime):
      # Keep it simple for the test; your real dialect likely differs.
      return "'" + value.isoformat() + "'"
    return str(value)


def test_apply_runtime_placeholders_replaces_delta_cutoff():
  dialect = FakeDialect()

  sql = "SELECT 1 WHERE x >= {{DELTA_CUTOFF}} AND run={{load_run_id}} AND ts={{load_timestamp}}"

  ts = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
  cutoff = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

  out = apply_runtime_placeholders(
    sql,
    dialect=dialect,
    load_run_id="abc",
    load_timestamp=ts,
    delta_cutoff=cutoff,
  )

  assert "{{DELTA_CUTOFF}}" not in out
  assert "{{load_run_id}}" not in out
  assert "{{load_timestamp}}" not in out
  assert cutoff.isoformat() in out
  assert "abc" in out


def test_apply_runtime_placeholders_leaves_delta_cutoff_if_none():
  dialect = FakeDialect()

  sql = "SELECT 1 WHERE x >= {{DELTA_CUTOFF}}"

  out = apply_runtime_placeholders(
    sql,
    dialect=dialect,
    load_run_id="abc",
    load_timestamp=datetime(2025, 1, 2, tzinfo=timezone.utc),
    delta_cutoff=None,
  )

  # Renderer/placeholder layer must not silently replace it.
  assert "{{DELTA_CUTOFF}}" in out
