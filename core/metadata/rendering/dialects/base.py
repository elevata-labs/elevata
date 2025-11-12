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

from __future__ import annotations

from abc import ABC, abstractmethod

from ..expr import Expr, ColumnRef, Literal, FuncCall, Concat, Coalesce
from ..logical_plan import LogicalSelect, SourceTable, SelectItem, Join


class SqlDialect(ABC):
  """
  Base interface for SQL dialects.
  Implementations translate Expr / LogicalSelect into final SQL strings.
  """

  @abstractmethod
  def quote_ident(self, name: str) -> str:
    """
    Quote an identifier (schema, table, column) according to the dialect.
    """
    raise NotImplementedError

  @abstractmethod
  def render_expr(self, expr: Expr) -> str:
    raise NotImplementedError

  @abstractmethod
  def render_select(self, select: LogicalSelect) -> str:
    raise NotImplementedError
