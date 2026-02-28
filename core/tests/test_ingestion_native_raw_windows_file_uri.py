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

from io import BytesIO
import builtins

from metadata.ingestion import native_raw


def test_read_bytes_normalizes_windows_file_uri(monkeypatch):
  # This must work on any OS: we simulate the Windows path handling.
  uri = "file:///C:/temp/elevata-labs/data/orders.csv"

  # Capture the path passed to os.path.exists/open.
  seen = {"path": None}

  def fake_exists(path):
    seen["path"] = path
    # We expect normalization to remove the leading slash.
    return path == "C:/temp/elevata-labs/data/orders.csv"

  monkeypatch.setattr(native_raw.os.path, "exists", fake_exists)

  # Patch open to avoid filesystem access.
  def fake_open(path, mode="rb"):
    assert path == "C:/temp/elevata-labs/data/orders.csv"
    assert mode == "rb"
    return BytesIO(b"ok")

  monkeypatch.setattr(builtins, "open", fake_open)

  data = native_raw._read_bytes(uri)

  assert data == b"ok"
  assert seen["path"] == "C:/temp/elevata-labs/data/orders.csv"