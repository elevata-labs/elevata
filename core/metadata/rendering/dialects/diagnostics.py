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

from dataclasses import dataclass, asdict
from typing import Any, Dict

from .base import SqlDialect
from .dialect_factory import get_available_dialect_names, get_active_dialect


@dataclass
class DialectDiagnostics:
  """Simple snapshot of a dialect's capabilities and behaviour."""

  name: str
  class_name: str
  supports_merge: bool
  supports_delete_detection: bool

  # Literal rendering examples
  literal_true: str
  literal_false: str
  literal_null: str
  literal_sample_date: str

  # Expression helpers
  sample_concat: str
  sample_hash256: str

  def to_dict(self) -> Dict[str, Any]:
    """Return a JSON-serializable representation."""
    return asdict(self)


def collect_dialect_diagnostics(dialect: SqlDialect) -> DialectDiagnostics:
  """Collect a minimal set of diagnostics for a single dialect instance."""
  # Literal samples
  literal_true = dialect.render_literal(True)
  literal_false = dialect.render_literal(False)
  literal_null = dialect.render_literal(None)

  # Simple fixed sample date to avoid timezone issues
  import datetime as _dt
  sample_date = _dt.date(2025, 1, 2)
  literal_sample_date = dialect.render_literal(sample_date)

  # Expression helpers: use very small synthetic examples
  concat_expr = dialect.concat_expression(["'a'", "'b'", "'c'"])

  # Hash sample: hash simple concatenation (actual hashing implementation
  # lives in the dialect; we only verify that it does not blow up).
  hash_expr = dialect.hash_expression(concat_expr)

  return DialectDiagnostics(
    name=getattr(dialect, "DIALECT_NAME", dialect.__class__.__name__.lower()),
    class_name=dialect.__class__.__name__,
    supports_merge=dialect.supports_merge,
    supports_delete_detection=dialect.supports_delete_detection,
    literal_true=literal_true,
    literal_false=literal_false,
    literal_null=literal_null,
    literal_sample_date=literal_sample_date,
    sample_concat=concat_expr,
    sample_hash256=hash_expr,
  )


def snapshot_all_dialects() -> Dict[str, DialectDiagnostics]:
  """
  Build diagnostics for all registered dialects.

  The keys of the result dict are dialect names as returned by
  get_available_dialect_names().
  """
  result: Dict[str, DialectDiagnostics] = {}

  for name in get_available_dialect_names():
    dialect = get_active_dialect(name)
    diag = collect_dialect_diagnostics(dialect)
    result[name] = diag

  return result
