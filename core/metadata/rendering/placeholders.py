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

import re
from datetime import timedelta
from dateutil.relativedelta import relativedelta

def _render_literal_for_dialect(dialect, value):
  fn = getattr(dialect, "render_literal", None)
  if callable(fn):
    return fn(value)
  return dialect.literal(value)

def resolve_delta_cutoff_for_source_dataset(*, source_dataset, profile, now_ts):
  """
  Resolve a concrete cutoff timestamp for {{DELTA_CUTOFF}} based on:
    - active SourceDatasetIncrementPolicy for environment == profile.name
  Returns a datetime or None.
  """
  if source_dataset is None:
    return None

  inc_filter = (getattr(source_dataset, "increment_filter", None) or "").strip()
  if not inc_filter:
    return None

  env = (getattr(profile, "name", None) or "dev").strip()

  policy = (
    source_dataset.increment_policies
      .filter(active=True, environment=env)
      .order_by("-id")
      .first()
  )
  if not policy:
    return None

  length = int(getattr(policy, "increment_interval_length", 0) or 0)
  unit = (getattr(policy, "increment_interval_unit", None) or "").strip().lower()
  if length <= 0:
    return None

  if unit == "day":
    return now_ts - timedelta(days=length)
  if unit == "month":
    return now_ts - relativedelta(months=length)
  if unit == "year":
    return now_ts - relativedelta(years=length)

  return None

def apply_delta_cutoff_placeholder(sql: str, *, dialect, delta_cutoff) -> str:
  """
  Replace {{DELTA_CUTOFF}} / {DELTA_CUTOFF} in SQL with a dialect-rendered literal.
  """
  if not sql or delta_cutoff is None:
    return sql

  cutoff_sql = _render_literal_for_dialect(dialect, delta_cutoff)
  sql = re.sub(r"\{\{\s*DELTA_CUTOFF\s*\}\}", cutoff_sql, sql)
  sql = re.sub(r"\{\s*DELTA_CUTOFF\s*\}", cutoff_sql, sql)
  return sql
