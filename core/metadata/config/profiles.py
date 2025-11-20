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
import yaml
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

@dataclass
class Profile:
  name: str
  providers: List[Dict[str, Any]]
  secret_ref_template: str
  overrides: Dict[str, str]
  security: Dict[str, Any]
  default_dialect: str

def _find_profiles_path(explicit_path: str | None = None) -> Path:
  """
  Locate elevata_profiles.yaml in several common locations.
  """
  # 1. Use explicit path from settings or .env if provided
  candidates = []
  if explicit_path:
    candidates.append(explicit_path)

  # 2. Relative to BASE_DIR or project root
  here = Path(__file__).resolve()
  candidates += [
    here.parents[3] / "config" / "elevata_profiles.yaml",  # e.g. elevata/config
    Path.cwd() / "config" / "elevata_profiles.yaml",
    Path("/etc/elevata/elevata_profiles.yaml"),
  ]

  for c in candidates:
    c = Path(c)
    if c.exists():
      return c

  raise FileNotFoundError("elevata_profiles.yaml not found in expected locations")

def load_profile(profiles_path: str) -> Profile:
  """Evaluates and returns the current active profile"""
  path = _find_profiles_path(profiles_path)
  with open(path, "r") as f:
    data = yaml.safe_load(f)

  active = os.getenv("ELEVATA_PROFILE", data.get("active_profile", "dev"))
  p = data["profiles"][active]

  return Profile(
    name=active,
    providers=p["providers"],
    secret_ref_template=p.get("secret_ref_template", "sec/{profile}/conn/{type}/{short_name}"),
    overrides=p.get("overrides", {}) or {},
    security=p.get("security", {}) or {},
    default_dialect=p.get("default_dialect", "duckdb")
  )

def render_ref(template: str, profile: Profile, **kwargs) -> str:
  """Render a template like 'sec/{profile}/pepper' with profile + kwargs."""
  return (template or "").format(profile=profile.name, **kwargs)

def apply_overrides(ref: str, profile: Profile) -> str:
  """Apply profile-specific ref overrides if present."""
  return profile.overrides.get(ref, ref)
