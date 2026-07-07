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

from metadata.materialization.plan import MaterializationPlan


def apply_materialization_plan(
  *,
  plan: MaterializationPlan,
  exec_engine,
  ensure_schema_sql_state: set[str] | None = None,
) -> None:
  """
  Apply safe steps from a materialization plan.

  ensure_schema_sql_state can be provided by orchestration to suppress
  repeated idempotent ENSURE_SCHEMA statements within one batch run.
  """
  if plan.is_blocked():
    # Caller should surface plan.blocking_errors nicely.
    raise RuntimeError(
      "Materialization plan is blocked: " + "; ".join(plan.blocking_errors)
    )

  for step in plan.steps:
    if not step.safe:
      continue
    if not step.sql:
      continue

    sql = str(step.sql).strip()
    if not sql:
      continue

    if getattr(step, "op", None) == "ENSURE_SCHEMA" and ensure_schema_sql_state is not None:
      key = " ".join(sql.rstrip(";").split()).lower()
      if key in ensure_schema_sql_state:
        continue
      exec_engine.execute(sql)
      ensure_schema_sql_state.add(key)
      continue

    exec_engine.execute(sql)
