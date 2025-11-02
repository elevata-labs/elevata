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

import pytest
from core.metadata.generation.naming import sanitize_name, build_raw_name
from core.metadata.generation.validators import validate_or_raise
from core.metadata.generation.rules import default_is_system_managed_for_layer

def test_sanitize_name_basic():
  assert sanitize_name("Kunden Stammdaten $äV2") == "kunden_stammdaten_aev2"

def test_sanitize_rejects_leading_digit():
  with pytest.raises(Exception):
    sanitize_name("123bad")

def test_build_raw_name():
  assert build_raw_name("sap", "Sales Orders") == "raw_sap_sales_orders"

def test_default_is_system_managed_for_layer():
  assert default_is_system_managed_for_layer("raw") is True
  assert default_is_system_managed_for_layer("stage") is True
  assert default_is_system_managed_for_layer("rawcore") is False
