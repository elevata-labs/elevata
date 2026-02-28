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

from metadata.ingestion import native_raw


def test_csv_encoding_utf8_sig_strips_bom(monkeypatch):
  # Skip if function does not expose encoding in signature.
  if "encoding" not in native_raw._load_file_records.__code__.co_varnames:
    pytest.skip("native_raw._load_file_records does not support encoding parameter")

  csv_text = (
    "id;customer\n"
    "1;Müller\n"
  )
  raw_bytes = ("\ufeff" + csv_text).encode("utf-8")

  monkeypatch.setattr(native_raw, "_read_bytes", lambda uri, max_bytes=10_000_000: raw_bytes)
  monkeypatch.setattr(native_raw, "_suffix_from_uri", lambda uri: ".csv")

  rows = native_raw._load_file_records(
    "file:///tmp/orders_de.csv",
    file_type="csv",
    delimiter=";",
    encoding="utf-8-sig",
  )

  assert len(rows) == 1
  # If encoding is correctly applied for CSV, the key must be 'id' (no BOM prefix).
  assert "id" in rows[0]
  assert "\ufeffid" not in rows[0]
  assert rows[0]["id"] == "1"
  assert rows[0]["customer"] == "Müller"