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

"""
Dialect diagnostics smoke tests.

These tests validate that the diagnostics module can build a consistent
snapshot for all registered dialects.
"""

from metadata.rendering.dialects.dialect_factory import (
  get_available_dialect_names,
  get_active_dialect,
)
from metadata.rendering.dialects.diagnostics import (
  collect_dialect_diagnostics,
  snapshot_all_dialects,
)


def test_collect_dialect_diagnostics_for_each_registered_dialect():
  for name in get_available_dialect_names():
    dialect = get_active_dialect(name)
    diag = collect_dialect_diagnostics(dialect)

    assert diag.name  # non-empty
    assert diag.class_name.endswith("Dialect")

    # Basic capabilities are booleans
    assert isinstance(diag.supports_merge, bool)
    assert isinstance(diag.supports_delete_detection, bool)

    # Literal examples should be non-empty strings
    assert isinstance(diag.literal_true, str) and diag.literal_true
    assert isinstance(diag.literal_false, str) and diag.literal_false
    assert isinstance(diag.literal_null, str) and diag.literal_null
    assert isinstance(diag.literal_sample_date, str) and diag.literal_sample_date

    # Expression samples should look like SQL fragments
    assert isinstance(diag.sample_concat, str) and diag.sample_concat
    assert isinstance(diag.sample_hash256, str) and diag.sample_hash256


def test_snapshot_all_dialects_returns_all_registered_names():
  snapshot = snapshot_all_dialects()
  names = set(get_available_dialect_names())

  assert set(snapshot.keys()) == names

  # Spot-check one entry
  for name, diag in snapshot.items():
    assert diag.name == name or diag.name == diag.class_name.lower()
    # roundtrip to dict should contain at least these keys
    as_dict = diag.to_dict()
    for key in [
      "name",
      "class_name",
      "supports_merge",
      "supports_delete_detection",
      "literal_true",
      "literal_false",
      "literal_null",
      "literal_sample_date",
      "sample_concat",
      "sample_hash256",
    ]:
      assert key in as_dict
