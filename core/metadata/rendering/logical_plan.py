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

from dataclasses import dataclass, field
from typing import List, Optional

from .expr import Expr


@dataclass
class SourceTable:
  """
  Logical representation of a source table in a FROM or JOIN clause.
  """
  schema: Optional[str]
  name: str
  alias: str


@dataclass
class SubquerySource:
  """
  Logical representation of a subquery in a FROM or JOIN clause.

  Example:
    FROM (
      SELECT ...
    ) AS u
  """
  select: "LogicalSelect | LogicalUnion"
  alias: str


@dataclass
class SelectItem:
  expr: Expr
  alias: Optional[str] = None


@dataclass
class Join:
  left_alias: str
  right: SourceTable | SubquerySource
  on: Expr
  join_type: str = "inner"  # inner, left, etc.


@dataclass
class LogicalSelect:
  """
  Vendor-neutral logical SELECT statement.
  """
  from_: SourceTable | SubquerySource
  joins: List[Join] = field(default_factory=list)
  where: Optional[Expr] = None
  group_by: List[Expr] = field(default_factory=list)
  order_by: List[Expr] = field(default_factory=list)
  select_list: List[SelectItem] = field(default_factory=list)
  distinct: bool = False


class LogicalUnion:
  """Represents a logical UNION or UNION ALL between multiple SELECTs."""

  def __init__(self, selects: list, union_type: str = "ALL"):
    self.selects = selects
    self.union_type = union_type.upper()
