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

import pytest

from metadata.ingestion import file_import


def test_local_path_from_uri_file_windows_drive_letter(monkeypatch):
  # Simulate Windows behavior regardless of runner OS by asserting the string transform.
  uri = "file:///C:/temp/elevata-labs/data/orders.xlsx"
  path = file_import._local_path_from_uri(uri)
  assert path == "C:/temp/elevata-labs/data/orders.xlsx"


def test_local_path_from_uri_file_windows_no_third_slash(monkeypatch):
  uri = "file://C:/temp/elevata-labs/data/orders.xlsx"
  path = file_import._local_path_from_uri(uri)
  assert path == "C:/temp/elevata-labs/data/orders.xlsx"


def test_local_path_from_uri_plain_path_passthrough():
  uri = "C:/temp/elevata-labs/data/orders.xlsx"
  path = file_import._local_path_from_uri(uri)
  assert path == "C:/temp/elevata-labs/data/orders.xlsx"