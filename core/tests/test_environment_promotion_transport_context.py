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

from __future__ import annotations

from types import SimpleNamespace

from django.db import transaction

import metadata.generation.policies as generation_policies
import metadata.signals as metadata_signals
from metadata.services import query_contract_sync_trigger
from metadata.transport_context import (
  metadata_artifact_reconstruction_active,
  metadata_artifact_reconstruction_context,
)


def test_metadata_artifact_reconstruction_context_is_nested_and_restored():
  assert metadata_artifact_reconstruction_active() is False

  with metadata_artifact_reconstruction_context():
    assert metadata_artifact_reconstruction_active() is True

    with metadata_artifact_reconstruction_context():
      assert metadata_artifact_reconstruction_active() is True

    assert metadata_artifact_reconstruction_active() is True

  assert metadata_artifact_reconstruction_active() is False


def test_query_contract_signal_does_not_schedule_sync_during_reconstruction(
  monkeypatch,
):
  target_dataset = SimpleNamespace(pk=42)
  instance = SimpleNamespace(
    created_by=None,
    updated_by=None,
  )
  scheduled = []

  monkeypatch.setattr(
    metadata_signals,
    "_td_from_instance",
    lambda _instance: target_dataset,
  )
  monkeypatch.setattr(
    generation_policies,
    "query_tree_allowed_for_dataset",
    lambda _target_dataset: True,
  )
  monkeypatch.setattr(
    metadata_signals,
    "get_current_user",
    lambda: None,
  )
  monkeypatch.setattr(
    transaction,
    "on_commit",
    lambda callback: scheduled.append(callback),
  )

  with metadata_artifact_reconstruction_context():
    metadata_signals._trigger_query_sync(
      sender=object,
      instance=instance,
    )

  assert scheduled == []

  metadata_signals._trigger_query_sync(
    sender=object,
    instance=instance,
  )
  assert len(scheduled) == 1


def test_query_contract_trigger_is_noop_during_reconstruction(monkeypatch):
  target_dataset = SimpleNamespace(pk=84)
  scheduled = []

  query_contract_sync_trigger._pending_td_ids.clear()
  monkeypatch.setattr(
    transaction,
    "on_commit",
    lambda callback: scheduled.append(callback),
  )

  with metadata_artifact_reconstruction_context():
    query_contract_sync_trigger.trigger_query_contract_column_sync(
      target_dataset
    )

  assert scheduled == []
  assert query_contract_sync_trigger._pending_td_ids == set()


def test_target_column_derivation_signals_return_before_touching_instance():
  class ExplodingInstance:
    @property
    def pk(self):
      raise AssertionError("transport guard must run before rename inspection")

    @property
    def target_dataset(self):
      raise AssertionError("transport guard must run before history inspection")

  instance = ExplodingInstance()

  with metadata_artifact_reconstruction_context():
    metadata_signals.track_target_column_rename(
      sender=object,
      instance=instance,
    )
    metadata_signals.sync_hist_on_rawcore_column_change(
      sender=object,
      instance=instance,
    )
