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

import re

import pytest

from metadata.rendering.dialects.mssql import MssqlDialect
from metadata.rendering.dialects.fabric_warehouse import FabricWarehouseDialect


def _is_quoted(ident: str, raw: str) -> bool:
  """
  Return True if `ident` appears quoted in `raw`.
  Accepts common quoting styles: [ident], "ident", `ident`.
  """
  patterns = [
    rf"^\[{re.escape(ident)}\]$",
    rf'^"{re.escape(ident)}"$',
    rf"^`{re.escape(ident)}`$",
  ]
  return any(re.match(p, raw) for p in patterns)


@pytest.mark.parametrize("dialect_cls", [MssqlDialect, FabricWarehouseDialect])
def test_keyword_index_is_quoted(dialect_cls):
  # Ensure reserved keyword 'index' is quoted for the dialect.
  d = dialect_cls()
  assert d.should_quote("index") is True

  rendered = d.render_identifier("index")
  assert rendered != "index"
  assert _is_quoted("index", rendered)


@pytest.mark.parametrize("dialect_cls", [MssqlDialect, FabricWarehouseDialect])
def test_regular_identifier_is_not_quoted(dialect_cls):
  # Ensure a typical identifier remains unquoted (unless base rules require quoting).
  d = dialect_cls()
  assert d.should_quote("customer_id") is False

  rendered = d.render_identifier("customer_id")
  assert rendered == "customer_id"