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

from metadata.ingestion import rest


def test_extract_records_top_level_list():
  payload = [{"id": 1}, {"id": 2}]
  records = rest._extract_records(payload, record_path=None)
  assert records == [{"id": 1}, {"id": 2}]


def test_extract_records_nested_record_path_data():
  payload = {"page": 1, "data": [{"id": 1}, {"id": 2}]}
  records = rest._extract_records(payload, record_path="data")
  assert records == [{"id": 1}, {"id": 2}]


def test_extract_records_nested_record_path_deeper():
  payload = {"data": {"items": [{"id": 1}, {"id": 2}]}}
  records = rest._extract_records(payload, record_path="data.items")
  assert records == [{"id": 1}, {"id": 2}]


def test_extract_records_single_object_response():
  # Some endpoints return a single object; ingestion should still be able to handle it.
  payload = {"id": 1, "name": "x"}
  records = rest._extract_records(payload, record_path=None)
  assert records == [{"id": 1, "name": "x"}]
