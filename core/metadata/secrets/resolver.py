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

from typing import Optional, List, Dict, Any
from functools import lru_cache
import os
import logging
from .providers_azure import AzureKeyVaultProvider

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Simple ENV provider (kept here for zero-dependency usage)
# -----------------------------------------------------------------------------
class EnvProvider:
  """Simple provider that fetches secrets from environment variables.
  Normalizes ref to ENV key: 'sec/dev/pepper' -> 'SEC_DEV_PEPPER'
  """
  def __init__(self, *, uppercase: bool = True, slash_to_underscore: bool = True, dash_to_underscore: bool = True):
    self.uppercase = uppercase
    self.slash_to_underscore = slash_to_underscore
    self.dash_to_underscore = dash_to_underscore

  def _to_env_key(self, ref: str) -> str:
    key = ref
    if self.slash_to_underscore:
      key = key.replace("/", "_")
    if self.dash_to_underscore:
      key = key.replace("-", "_")
    if self.uppercase:
      key = key.upper()
    return key

  def get_secret_value(self, ref: str) -> Optional[str]:
    env_key = self._to_env_key(ref)
    return os.getenv(env_key)


# -----------------------------------------------------------------------------
# Provider chain builder
# -----------------------------------------------------------------------------
def build_chain(providers_spec: List[Dict[str, Any]]) -> List[Any]:
  """Instantiate providers from a 'providers' spec of the active profile"""
  chain: List[Any] = []
  for spec in (providers_spec or []):
    ptype = (spec.get("type") or "").lower()

    if ptype == "env":
      chain.append(EnvProvider())
      continue

    if ptype == "azure_key_vault":
      if "vault_url" not in spec or not spec["vault_url"]:
        raise ValueError("azure_key_vault provider requires 'vault_url'")

      # Default mapper: Key Vault does not allow dashes, replace by dash
      def mapper(ref: str) -> str:
        return ref.replace("/", "-")

      chain.append(AzureKeyVaultProvider(
        vault_url=spec["vault_url"],
        name_mapper=mapper,
        disable_cache=bool(spec.get("disable_cache", False)),
      ))
      continue

    raise ValueError(f"Unsupported provider type: {ptype}")

  if not chain:
    # Provide a safe default: ENV-only chain
    logger.warning("No providers configured; falling back to ENV provider only.")
    chain.append(EnvProvider())

  return chain


# -----------------------------------------------------------------------------
# Core resolution helpers (value-only)
# -----------------------------------------------------------------------------
def resolve_secret_value_from_chain(ref: str, chain: List[Any]) -> str:
  """Try each provider in order; return the first non-empty value, else raise."""
  for provider in chain:
    try:
      val = provider.get_secret_value(ref)
    except Exception as e:
      # Swallow provider-specific errors so the chain can continue.
      logger.debug("Provider %s failed for ref '%s': %s", type(provider).__name__, ref, e)
      val = None
    if val:
      return val
  raise RuntimeError(f"Secret value not found via provider chain for ref '{ref}'")


def resolve_secret_value(ref: str, providers_spec: List[Dict[str, Any]]) -> str:
  """Convenience: build chain from providers_spec and resolve a ref to a value."""
  chain = build_chain(providers_spec)
  return resolve_secret_value_from_chain(ref, chain)


# -----------------------------------------------------------------------------
# Profile-aware helpers (render + overrides + resolve)
# -----------------------------------------------------------------------------
# We import lazily to avoid circular imports with config.profiles
def _load_profile(profiles_path: str):
  from metadata.config.profiles import load_profile  # local import
  return load_profile(profiles_path)

def _render_and_override(ref_template: str, profile, **kwargs) -> str:
  from metadata.config.profiles import render_ref, apply_overrides  # local import
  ref = render_ref(ref_template, profile, **kwargs)
  return apply_overrides(ref, profile)


def resolve_ref_template_value(
  *,
  profiles_path: str,
  ref_template: str,
  **kwargs
) -> str:
  """Render a ref from template+kwargs with the active profile, apply overrides,
  then resolve to a secret value via the profile's providers chain.
  """
  profile = _load_profile(profiles_path)
  final_ref = _render_and_override(ref_template, profile, **kwargs)
  return resolve_secret_value(final_ref, profile.providers)


def resolve_profile_secret_value(
  *,
  profiles_path: str,
  ref: str
) -> str:
  """Resolve a *final* ref string (already rendered) using the active profile."""
  profile = _load_profile(profiles_path)
  # apply overrides once more in case caller forgot to render via template
  from metadata.config.profiles import apply_overrides  # local import
  final_ref = apply_overrides(ref, profile)
  return resolve_secret_value(final_ref, profile.providers)


# -----------------------------------------------------------------------------
# Optional cached variant (for frequently accessed secrets like pepper)
# -----------------------------------------------------------------------------
@lru_cache(maxsize=256)
def cached_profile_secret_value(*, profiles_path: str, ref: str) -> str:
  """LRU-cached resolution by final ref. Cache key includes ref & profiles_path."""
  return resolve_profile_secret_value(profiles_path=profiles_path, ref=ref)


def get_pepper(*, profiles_path: str) -> str:
  """Convenience helper to fetch the pepper using the profile's security.pepper_ref."""
  profile = _load_profile(profiles_path)
  tpl = (profile.security or {}).get("pepper_ref") or "sec/{profile}/pepper"
  final_ref = _render_and_override(tpl, profile)
  # Optionally use cached variant
  return cached_profile_secret_value(profiles_path=profiles_path, ref=final_ref)
