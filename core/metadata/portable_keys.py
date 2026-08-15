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

import hashlib
import json
from collections.abc import Iterable
from typing import Any


def _canonical_json(value: Any) -> str:
  return json.dumps(
    value,
    sort_keys=True,
    ensure_ascii=False,
    separators=(",", ":"),
  )


def _required_text(value: Any, label: str) -> str:
  normalized = str(value or "").strip()
  if not normalized:
    raise ValueError(f"{label} is required for portable lineage identity.")
  return normalized


def source_dataset_portable_identity(source_dataset: Any) -> dict[str, str]:
  """Return the natural, environment-independent identity of a source dataset."""
  source_system = getattr(source_dataset, "source_system", None)
  return {
    "source_system": _required_text(
      getattr(source_system, "short_name", None),
      "Source system short_name",
    ),
    "schema_name": str(getattr(source_dataset, "schema_name", None) or "").strip(),
    "source_dataset_name": _required_text(
      getattr(source_dataset, "source_dataset_name", None),
      "Source dataset name",
    ),
  }


def build_target_dataset_lineage_key(
  target_schema: Any,
  source_datasets: Iterable[Any],
) -> str:
  """
  Build a stable generated TargetDataset lineage key without database IDs.

  Identity is based only on the logical target schema and the sorted natural
  identities of the source datasets in the generation bucket.
  """
  schema_short_name = _required_text(
    getattr(target_schema, "short_name", None),
    "Target schema short_name",
  )
  source_identities = sorted(
    (source_dataset_portable_identity(item) for item in source_datasets),
    key=lambda item: (
      item["source_system"],
      item["schema_name"],
      item["source_dataset_name"],
    ),
  )
  if not source_identities:
    raise ValueError(
      "At least one source dataset is required for generated target lineage."
    )
  payload = {
    "source_datasets": source_identities,
    "target_schema": schema_short_name,
  }
  digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
  return f"generated:{schema_short_name}:{digest}"
