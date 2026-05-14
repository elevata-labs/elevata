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

from types import SimpleNamespace
import uuid

import pytest
from django.core.management.base import CommandError


class _Stdout:
  def write(self, *_args, **_kwargs):
    return None


@pytest.mark.django_db
def test_execute_materialization_requires_migration_plan(monkeypatch):
  """
  Guardrail: In execute mode, schema evolution must be driven by MigrationPlan.
  """
  from metadata.models import TargetSchema, TargetDataset
  from metadata.management.commands import elevata_load as mod

  schema = TargetSchema.objects.get_or_create(short_name="rawcore", schema_name="rawcore")[0]
  td = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=f"rc_guard_{uuid.uuid4().hex[:6]}",
    incremental_strategy="full",
    materialization_type="table",
    is_system_managed=False,
  )

  # Force the code path into materialization planning.
  monkeypatch.setattr(mod, "AUTO_PROVISION_TABLES", True)

  # Minimal stubs (we must raise before any engine work is needed).
  target_system = SimpleNamespace(short_name="dummy", type="duckdb")
  profile = SimpleNamespace(name="dev")
  dialect = SimpleNamespace(__class__=SimpleNamespace(__name__="DummyDialect"))

  style = SimpleNamespace(NOTICE=lambda x: x, WARNING=lambda x: x, ERROR=lambda x: x)

  with pytest.raises(CommandError) as exc:
    mod.run_single_target_dataset(
      stdout=_Stdout(),
      style=style,
      target_dataset=td,
      target_system=target_system,
      target_system_engine=None,
      profile=profile,
      dialect=dialect,
      execute=True,
      no_print=True,
      debug_plan=False,
      debug_materialization=False,
      batch_run_id="test",
      migration_plan=None,
    )

  msg = str(exc.value).lower()
  assert "missing migration_plan" in msg
  assert "schema evolution" in msg