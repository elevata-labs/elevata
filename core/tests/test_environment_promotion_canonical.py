"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2026 Ilona Tag

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

from decimal import Decimal
from uuid import UUID

from metadata.promotion.canonical import (
  canonical_json,
  canonical_sha256,
)


def test_canonical_json_is_mapping_and_set_order_independent():
  left = {
    "systems": {"dwhprod", "dwhdev"},
    "settings": {"nullable": True, "precision": Decimal("18.00")},
  }
  right = {
    "settings": {"precision": Decimal("18.00"), "nullable": True},
    "systems": {"dwhdev", "dwhprod"},
  }

  assert canonical_json(left) == canonical_json(right)
  assert canonical_sha256(left) == canonical_sha256(right)


def test_canonical_json_normalizes_uuid_values():
  value = UUID("7d7ef888-1af6-4c6d-bde8-99c64fa57e6f")

  assert canonical_json({"logical_key": value}) == (
    '{"logical_key":"7d7ef888-1af6-4c6d-bde8-99c64fa57e6f"}'
  )
