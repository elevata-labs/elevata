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

"""
elevata - Dialect guardrails.

Ensure every registered dialect defines RESERVED_KEYWORDS.
"""

import pytest

from metadata.rendering.dialects.dialect_factory import (
  get_available_dialect_names,
  get_active_dialect,
  assert_all_dialects_define_reserved_keywords,
)

def test_all_dialects_define_reserved_keywords_guardrail():
  """
  CI guardrail: new dialects must ship with RESERVED_KEYWORDS.
  """
  assert_all_dialects_define_reserved_keywords()

@pytest.mark.parametrize("dialect_name", get_available_dialect_names())
def test_reserved_keywords_have_reasonable_shape(dialect_name: str):
  """
  Ensure keyword sets are non-empty and look like UPPERCASE tokens.
  """
  d = get_active_dialect(dialect_name)
  kw = getattr(d, "RESERVED_KEYWORDS", None)

  assert kw, f"{dialect_name} must define RESERVED_KEYWORDS"

  # Must be iterable of strings
  sample = list(kw)[:50]
  assert sample and all(isinstance(x, str) for x in sample)

  # Convention: store as UPPERCASE tokens
  assert all(x == x.upper() for x in sample)

  # Sanity: common keyword should be present
  assert "SELECT" in kw