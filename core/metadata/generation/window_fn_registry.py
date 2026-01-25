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

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Literal, Optional

ArgType = Literal["column", "int", "str"]


@dataclass(frozen=True)
class WindowFnSpec:
  name: str
  min_args: int
  max_args: int
  # positional argument schemas; if shorter than max_args, remaining positions are flexible
  arg_schema: List[List[ArgType]]
  requires_order_by: bool


WINDOW_FN_REGISTRY: Dict[str, WindowFnSpec] = {
  "ROW_NUMBER": WindowFnSpec(
    name="ROW_NUMBER",
    min_args=0,
    max_args=0,
    arg_schema=[],
    requires_order_by=True,
  ),
  "RANK": WindowFnSpec(
    name="RANK",
    min_args=0,
    max_args=0,
    arg_schema=[],
    requires_order_by=True,
  ),
  "DENSE_RANK": WindowFnSpec(
    name="DENSE_RANK",
    min_args=0,
    max_args=0,
    arg_schema=[],
    requires_order_by=True,
  ),
  "NTILE": WindowFnSpec(
    name="NTILE",
    min_args=1,
    max_args=1,
    arg_schema=[["int"]],
    requires_order_by=True,
  ),
  "LAG": WindowFnSpec(
    name="LAG",
    min_args=1,
    max_args=3,
    arg_schema=[["column"], ["int"], ["column", "int", "str"]],
    requires_order_by=True,
  ),
  "LEAD": WindowFnSpec(
    name="LEAD",
    min_args=1,
    max_args=3,
    arg_schema=[["column"], ["int"], ["column", "int", "str"]],
    requires_order_by=True,
  ),
  
  # Value navigation
  "FIRST_VALUE": WindowFnSpec(
    name="FIRST_VALUE",
    min_args=1,
    max_args=1,
    arg_schema=[["column"]],
    requires_order_by=True,
  ),
  "LAST_VALUE": WindowFnSpec(
    name="LAST_VALUE",
    min_args=1,
    max_args=1,
    arg_schema=[["column"]],
    requires_order_by=True,
  ),
  "NTH_VALUE": WindowFnSpec(
    name="NTH_VALUE",
    min_args=2,
    max_args=2,
    arg_schema=[["column"], ["int"]],
    requires_order_by=True,
  ),

  # Windowed aggregates (optional ORDER BY; semantics still deterministic without it)
  "SUM": WindowFnSpec(
    name="SUM",
    min_args=1,
    max_args=1,
    arg_schema=[["column"]],
    requires_order_by=False,
  ),
  "AVG": WindowFnSpec(
    name="AVG",
    min_args=1,
    max_args=1,
    arg_schema=[["column"]],
    requires_order_by=False,
  ),
  "MIN": WindowFnSpec(
    name="MIN",
    min_args=1,
    max_args=1,
    arg_schema=[["column"]],
    requires_order_by=False,
  ),
  "MAX": WindowFnSpec(
    name="MAX",
    min_args=1,
    max_args=1,
    arg_schema=[["column"]],
    requires_order_by=False,
  ),
  "COUNT": WindowFnSpec(
    name="COUNT",
    # Allow COUNT(*) OVER (...) (0 args) and COUNT(col) OVER (...) (1 arg)
    min_args=0,
    max_args=1,
    arg_schema=[["column"]],
    requires_order_by=False,
  ),
}


def get_window_fn_spec(name: str) -> Optional[WindowFnSpec]:
  return WINDOW_FN_REGISTRY.get((name or "").strip().upper())