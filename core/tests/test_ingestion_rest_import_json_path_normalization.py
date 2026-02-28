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

from metadata.ingestion.rest_import import _flatten_keys


def test_rest_import_flatten_keys_sets_normalized_json_path_for_top_level():
  rows = [
    {"userId": 1, "id": 1, "title": "t", "body": "b"},
  ]

  m = _flatten_keys(rows, max_nested=0)

  # Column names are normalized
  assert "userid" in m
  assert "id" in m

  # Top-level json_path must match normalized runtime keys
  assert m["userid"] == "$.userid"
  assert m["id"] == "$.id"