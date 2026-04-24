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

from metadata.ingestion.types_map import canonicalize_type, canonical_type_str


def test_duckdb_struct_maps_to_json():
  # DuckDB nested type strings can contain inner type names like "varchar".
  # We must not misclassify them as STRING.
  t = canonicalize_type("duckdb", "struct(a int, b varchar)")
  assert canonical_type_str(t) == "JSON"


def test_generic_struct_maps_to_json():
  # Generic fallback should treat semistructured generics as JSON.
  t = canonicalize_type("unknown", "struct<a:int,b:string>")
  assert canonical_type_str(t) == "JSON"