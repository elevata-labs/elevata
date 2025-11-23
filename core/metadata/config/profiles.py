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

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any, Optional

import yaml
from django.conf import settings

"""
Profile loading for elevata.

Profiles define environment-specific configuration such as:
- where to load secrets from (providers)
- how to build secret references (secret_ref_template)
- security references (e.g. pepper_ref)
- the default SQL dialect for SQL generation

They do NOT define project-specific metadata such as target systems.
"""

@dataclass
class Profile:
  name: str

  # Where to load secrets from (env, Azure Key Vault, ...)
  providers: List[Dict[str, Any]]

  # Single template for all connections (source + target)
  # e.g. "sec/{profile}/conn/{type}/{short_name}"
  secret_ref_template: str

  # Optional ref overrides (by full ref string)
  overrides: Dict[str, str]

  # Arbitrary security config, e.g. {"pepper_ref": "sec/{profile}/pepper"}
  security: Dict[str, Any]

  # Dialect used for SQL generation (unless env override)
  default_dialect: str


def _find_profiles_path(explicit_path: str | None = None) -> Path:
  """
  Locate elevata_profiles.yaml in several common locations:

  1. explicit_path argument (if provided and exists)
  2. Django settings.ELEVATA_PROFILES_PATH (if set and exists)
  3. common fallback locations relative to the package and CWD

  Raises:
      FileNotFoundError: if no suitable file can be found.
  """
  candidates: list[Path] = []

  # 1) explicit argument
  if explicit_path:
    candidates.append(Path(explicit_path))

  # 2) Django setting (typically based on ELEVATA_PROFILES_PATH env var)
  cfg_path = getattr(settings, "ELEVATA_PROFILES_PATH", None)
  if cfg_path:
    candidates.append(Path(cfg_path))

  # 3) fallbacks
  here = Path(__file__).resolve()
  candidates += [
    here.parents[3] / "config" / "elevata_profiles.yaml",
    Path.cwd() / "config" / "elevata_profiles.yaml",
    Path("/etc/elevata/elevata_profiles.yaml"),
  ]

  for c in candidates:
    if c and c.exists():
      return c

  raise FileNotFoundError(
    "elevata_profiles.yaml not found in expected locations. "
    "Provide an explicit path or configure ELEVATA_PROFILES_PATH."
  )


def load_profile(profiles_path: Optional[str] = None) -> Profile:
  """
  Load and return the current active profile.

  Resolution order:
    - ELEVATA_PROFILE env var
    - `active_profile` key in elevata_profiles.yaml
    - default 'dev'
  """
  path = _find_profiles_path(profiles_path)

  with open(path, "r") as f:
    data = yaml.safe_load(f) or {}

  active = os.getenv("ELEVATA_PROFILE", data.get("active_profile", "dev"))
  profiles = data.get("profiles") or {}

  if active not in profiles:
    available = ", ".join(sorted(profiles)) if profiles else "(none)"
    raise KeyError(
      f"Active profile '{active}' not found in elevata_profiles.yaml "
      f"at {path}. Available profiles: {available}."
    )

  p = profiles[active] or {}

  return Profile(
    name=active,
    providers=p.get("providers", []) or [],
    secret_ref_template=p.get(
      "secret_ref_template",
      "sec/{profile}/conn/{type}/{short_name}",
    ),
    overrides=p.get("overrides", {}) or {},
    security=p.get("security", {}) or {},
    default_dialect=p.get("default_dialect", "duckdb"),
  )


def render_ref(template: str, profile: Profile, **kwargs) -> str:
  """Render a template like 'sec/{profile}/conn/{type}/{short_name}'."""
  return (template or "").format(profile=profile.name, **kwargs)


def apply_overrides(ref: str, profile: Profile) -> str:
  """Apply profile-specific ref overrides if present."""
  return profile.overrides.get(ref, ref)
