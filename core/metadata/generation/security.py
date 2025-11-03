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

import os
from django.conf import settings

def get_runtime_pepper() -> str:
  """
  Return the runtime pepper for deterministic surrogate key hashing.
  Priority:
  1. Environment variable 'ELEVATA_PEPPER'
  2. Profile-specific variable 'SEC_<PROFILE>_PEPPER'
  3. Django settings fallback
  """
  # 1. direct runtime override
  pepper = os.environ.get("ELEVATA_PEPPER")

  # 2. support profile-specific style (e.g. SEC_DEV_PEPPER)
  if not pepper:
    profile = os.environ.get("ELEVATA_PROFILE", "DEV").upper()
    env_key = f"SEC_{profile}_PEPPER"
    pepper = os.environ.get(env_key)

  # 3. fallback to settings
  if not pepper and hasattr(settings, "ELEVATA_PEPPER"):
    pepper = settings.ELEVATA_PEPPER

  if not pepper:
    raise ValueError(
      "No runtime pepper configured. Set ELEVATA_PEPPER or SEC_<PROFILE>_PEPPER in environment."
    )

  return pepper
