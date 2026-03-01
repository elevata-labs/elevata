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

from metadata.rendering.dialects.dialect_factory import get_all_registered_dialects


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


def test_reserved_keywords_are_quoted_per_dialect():
  """
  Ensure that each dialect quotes exactly its own reserved keywords.
  """
  for d in get_all_registered_dialects():
    reserved = getattr(d, "RESERVED_KEYWORDS", set())
    assert reserved, f"{d.__class__.__name__} has no RESERVED_KEYWORDS"

    # test a representative subset for performance
    sample = list(reserved)[:20]

    for kw in sample:
      ident = kw.lower()
      rendered = d.render_identifier(ident)

      assert rendered != ident
      assert _is_quoted(ident, rendered)
      assert bool(d.should_quote(ident)) is True


def test_non_reserved_identifier_is_not_quoted():
  """
  Ensure a regular identifier is not quoted.
  """
  for d in get_all_registered_dialects():
    ident = "customer_id"
    rendered = d.render_identifier(ident)

    assert rendered == ident
    assert bool(d.should_quote(ident)) is False
