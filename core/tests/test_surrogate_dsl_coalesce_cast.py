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

from metadata.rendering.dsl import parse_surrogate_dsl
from metadata.rendering.expr import Coalesce, Cast, ColumnRef, Literal


def test_parse_surrogate_dsl_coalesce_casts_left_side_to_string():
  # This is the exact class of bug that caused DuckDB to try casting
  # 'null_replaced' into INT. The fix is: CAST(left AS string) before COALESCE.
  expr = parse_surrogate_dsl(
    "COALESCE({expr:productid}, 'null_replaced')",
    table_alias="s",
  )

  assert isinstance(expr, Coalesce)
  assert len(expr.parts) == 2

  left, right = expr.parts

  assert isinstance(left, Cast)
  assert isinstance(left.expr, ColumnRef)
  assert left.expr.table_alias == "s"
  assert left.expr.column_name == "productid"

  assert isinstance(right, Literal)
  assert right.value == "null_replaced"
