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

from collections.abc import Callable, Iterable

from metadata.rendering.expr import ColumnRef, Expr
from metadata.rendering.portable_expression import (
  PortableExpressionError,
  expression_contract_to_ast,
)


def input_link_column_name(link) -> str:
  """Return the physical input-column name represented by a lineage link."""
  upstream = getattr(link, "upstream_target_column", None)
  if upstream is not None:
    return str(getattr(upstream, "target_column_name", "") or "").strip()

  source = getattr(link, "source_column", None)
  if source is not None:
    return str(getattr(source, "source_column_name", "") or "").strip()

  return ""


def build_column_resolver(
  links: Iterable[object],
  *,
  alias_resolver: Callable[[object], str | None] | None = None,
) -> Callable[[str], ColumnRef]:
  """Build a fail-closed resolver from lineage links for a portable expression."""
  candidates: dict[str, list[tuple[str, str | None]]] = {}

  for link in links:
    name = input_link_column_name(link)
    if not name:
      continue
    alias = alias_resolver(link) if alias_resolver is not None else None
    if alias_resolver is not None and not alias:
      continue
    key = name.lower()
    candidate = (name, alias)
    if candidate not in candidates.setdefault(key, []):
      candidates[key].append(candidate)

  def resolve(name: str) -> ColumnRef:
    key = str(name or "").strip().lower()
    hits = candidates.get(key, [])
    if not key or not hits:
      raise PortableExpressionError(
        f"Portable expression references unknown input column: {name!r}."
      )
    if len(hits) != 1:
      raise PortableExpressionError(
        f"Portable expression input column is ambiguous: {name!r}."
      )
    canonical_name, alias = hits[0]
    return ColumnRef(table_alias=alias, column_name=canonical_name)

  return resolve


def column_expression_contract_to_ast(
  contract,
  *,
  links: Iterable[object],
  alias_resolver: Callable[[object], str | None] | None = None,
) -> Expr:
  """Parse a portable column expression and bind its columns to explicit lineage."""
  resolver = build_column_resolver(links, alias_resolver=alias_resolver)
  return expression_contract_to_ast(contract, column_resolver=resolver)
