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

from metadata.intent.landing import landing_required

def resolve_ingest_mode(dataset) -> str:
  """
  Decide how ingestion is handled for this dataset.

  Returns: native | external | none
  """

  if not landing_required(dataset):
    return "none"

  mode = getattr(dataset.source_system, "include_ingest", "none")

  if mode == "none":
    raise ValueError(
      "Raw landing required, but include_ingest='none'."
    )

  return mode
