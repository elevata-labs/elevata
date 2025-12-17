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

def landing_required(dataset) -> bool:
  """
  Decide whether this dataset conceptually requires a raw landing.
  """
  # integrate is a hard gate everywhere
  if getattr(dataset, "integrate", None) is not True:
    return False

  ds_flag = getattr(dataset, "generate_raw_table", None)

  if ds_flag is True:
    return True
  if ds_flag is False:
    return False

  # inherit from system
  system = getattr(dataset, "source_system", None)
  return bool(getattr(system, "generate_raw_tables", False))
