"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2026 Ilona Tag

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

from types import SimpleNamespace
from uuid import uuid4

import pytest

from metadata.models import Team
from metadata.promotion.apply import EnvironmentPromotionExecutor


@pytest.mark.django_db
def test_transport_persistence_bypasses_model_save_override(monkeypatch):
  """
  Approved artifact reconstruction must not invoke domain mutation hooks.
  """
  team = Team(name=f"transport-{uuid4().hex[:8]}", description="portable")

  def forbidden_save(*args, **kwargs):
    raise AssertionError("model save override must not run during transport apply")

  monkeypatch.setattr(Team, "save", forbidden_save)

  EnvironmentPromotionExecutor._save_transport_instance(
    instance=team,
    action=SimpleNamespace(action_id="act-transport-test"),
  )

  assert Team.objects.filter(pk=team.pk, description="portable").exists()
