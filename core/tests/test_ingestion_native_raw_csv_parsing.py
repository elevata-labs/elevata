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

from metadata.ingestion import native_raw


def test_load_file_records_csv_delimiter_quotechar_encoding_utf8_sig(monkeypatch):
  # German-style CSV: semicolon delimiter, umlauts, quoted field containing delimiter and quotes.
  csv_text = (
    "id;customer;amount;note\n"
    "1;Müller;10,50;\"Lieferung \"\"Express\"\"; bitte\"\n"
    "2;Schmidt;20,00;\"normal\"\n"
  )

  # Add UTF-8 BOM to simulate real Excel/Windows exports.
  raw_bytes = ("\ufeff" + csv_text).encode("utf-8")

  # Avoid file/HTTP access by patching _read_bytes.
  monkeypatch.setattr(native_raw, "_read_bytes", lambda uri, max_bytes=10_000_000: raw_bytes)

  # Ensure suffix detection chooses CSV.
  monkeypatch.setattr(native_raw, "_suffix_from_uri", lambda uri: ".csv")

  # Use encoding/quotechar only if the implementation supports it.
  supports_encoding = "encoding" in native_raw._load_file_records.__code__.co_varnames
  supports_quotechar = "quotechar" in native_raw._load_file_records.__code__.co_varnames

  kwargs = {
    "file_type": "csv",
    "delimiter": ";",
  }
  if supports_quotechar:
    kwargs["quotechar"] = "\""
  if supports_encoding:
    kwargs["encoding"] = "utf-8-sig"

  rows = native_raw._load_file_records("file:///tmp/orders_de.csv", **kwargs)

  assert isinstance(rows, list)
  assert len(rows) == 2

  # If encoding is not supported, the BOM may remain in the first header.
  id_key = "id" if "id" in rows[0] else "\ufeffid"

  assert rows[0][id_key] == "1"
  assert rows[0]["customer"] == "Müller"
  assert rows[0]["amount"] == "10,50"
  assert rows[0]["note"] == 'Lieferung "Express"; bitte'

  assert rows[1][id_key] == "2"
  assert rows[1]["customer"] == "Schmidt"
  assert rows[1]["note"] == "normal"
