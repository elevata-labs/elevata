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

from typing import Optional

from django.db import transaction

from metadata.models import TargetDataset
from metadata.services.query_contract_column_sync import QueryContractColumnSyncService

# Simple per-transaction debounce to avoid N syncs for N row saves.
_pending_td_ids: set[int] = set()
_running: bool = False


def trigger_query_contract_column_sync(td: Optional[TargetDataset]) -> None:
  global _running

  if td is None or not getattr(td, "pk", None):
    return

  td_id = int(td.pk)

  # Debounce per transaction (multiple saves -> one sync).
  _pending_td_ids.add(td_id)

  def _run():
    global _running
    if _running:
      return
    _running = True
    try:
      svc = QueryContractColumnSyncService()
      # Copy and clear so subsequent commits can schedule again.
      ids = list(_pending_td_ids)
      _pending_td_ids.clear()
      for x in ids:
        td_obj = TargetDataset.objects.filter(pk=x).first()
        if td_obj is None:
          continue
        svc.sync_for_dataset(td_obj)
    finally:
      _running = False

  transaction.on_commit(_run)
