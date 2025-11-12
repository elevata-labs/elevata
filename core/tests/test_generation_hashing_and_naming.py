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

import re
import pytest

from metadata.generation import hashing, naming


# ---------------------------------------------------------------------
# HASHING TESTS
# ---------------------------------------------------------------------

def test_build_surrogate_expression_deterministic():
  """build_surrogate_expression should return a deterministic, sorted expression."""
  expr1 = hashing.build_surrogate_expression(
    natural_key_cols=["b", "a"],
    pepper="xyz",
    null_token="<NULL>",
    pair_sep="~",
    comp_sep="||"
  )
  expr2 = hashing.build_surrogate_expression(
    natural_key_cols=["a", "b"],
    pepper="xyz",
    null_token="<NULL>",
    pair_sep="~",
    comp_sep="||"
  )

  # Both expressions should be identical because columns are sorted internally
  assert expr1 == expr2
  assert "hash256(" in expr1
  assert "{expr:a}" in expr1
  assert "{expr:b}" in expr1
  assert "xyz" in expr1  # pepper included


def test_demo_python_hash_consistency():
  """demo_python_hash must produce deterministic output for the same input."""
  v1 = ["A", "B"]
  v2 = ["A", "B"]
  h1 = hashing.demo_python_hash(v1, pepper="pep")
  h2 = hashing.demo_python_hash(v2, pepper="pep")

  assert h1 == h2
  assert re.fullmatch(r"[0-9a-f]{64}", h1)


# ---------------------------------------------------------------------
# NAMING TESTS
# ---------------------------------------------------------------------

@pytest.mark.parametrize(
  "raw,expected",
  [
    (" SAP-Kunde ", "sap_kunde"),
    ("ÄÖÜ", "aeoeue"),
    ("ß_Test__", "ss_test"),
    ("Crème brûlée", "creme_brulee"),
  ],
)
def test_sanitize_name_basic(raw, expected):
  """sanitize_name should normalize umlauts, accents, and special chars."""
  assert naming.sanitize_name(raw) == expected


def test_build_physical_dataset_name(monkeypatch):
  """Verify build_physical_dataset_name logic using dummy objects."""
  class DummyTargetSchema:
    physical_prefix = "rc"
    consolidate_groups = False

  class DummySystem:
    short_name = "sap1"
    target_short_name = "sap"

  class DummySourceDataset:
    source_system = DummySystem()
    source_dataset_name = "customer"

  schema = DummyTargetSchema()
  source = DummySourceDataset()

  name = naming.build_physical_dataset_name(schema, source)

  assert name == "rc_sap1_customer"  # RAW-like behavior, no consolidation


def test_build_history_and_surrogate_names():
  """Verify basic _hist and _key naming conventions."""
  hist = naming.build_history_name("sap_customer")
  key = naming.build_surrogate_key_name("sap_customer")

  assert hist == "sap_customer_hist"
  assert key == "sap_customer_key"
