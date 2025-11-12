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
from metadata.generation.naming import build_surrogate_key_name


@pytest.mark.parametrize(
  "table,expected",
  [
    ("rc_sap_customer", "rc_sap_customer_key"),
    ("rc_aw_person", "rc_aw_person_key"),
  ],
)
def test_build_surrogate_key_name(table, expected):
  assert build_surrogate_key_name(table) == expected
