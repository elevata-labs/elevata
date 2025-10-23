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

from typing import Optional, Callable
from functools import lru_cache

from azure.identity import (
  ChainedTokenCredential,
  DefaultAzureCredential,
  ManagedIdentityCredential,
  AzureCliCredential,
)
from azure.keyvault.secrets import SecretClient
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError


class AzureKeyVaultProvider:
  """
  Secret provider that retrieves *values* from Azure Key Vault.

  Usage:
    provider = AzureKeyVaultProvider(
      vault_url="https://<your-kv-name>.vault.azure.net/",
      name_mapper=lambda ref: ref.replace("/", "-")  # optional mapping for ref->secret name
    )
    value = provider.get_secret_value("sec/prod/pepper")

  Notes:
    - Azure KV secret *names* cannot contain '/'.
      Provide a name_mapper to convert refs (e.g. "sec/prod/pepper" -> "sec-prod-pepper"),
      or pass ref strings that are already KV-safe.
    - Authentication chain prefers:
        1) Azure CLI (for local dev),
        2) Managed Identity (for Azure runtimes),
        3) DefaultAzureCredential (env/service principal etc.)
    - This provider returns None on "not found" so a resolver chain can continue.
  """

  def __init__(
    self,
    vault_url: str,
    *,
    credential=None,
    use_cli: bool = True,
    use_msi: bool = True,
    use_default: bool = True,
    name_mapper: Optional[Callable[[str], str]] = None,
    disable_cache: bool = False,
  ) -> None:
    if not vault_url:
      raise ValueError("vault_url is required (e.g. https://<name>.vault.azure.net/)")
    self.vault_url = vault_url.rstrip("/")
    self.name_mapper = name_mapper or (lambda ref: ref)  # assume ref is already KV-safe
    self._cache_enabled = not disable_cache

    if credential is None:
      chain = []
      if use_cli:
        chain.append(AzureCliCredential())
      if use_msi:
        chain.append(ManagedIdentityCredential())
      if use_default:
        # covers Service Principal via env vars, VisualStudioCodeCredential, etc.
        chain.append(DefaultAzureCredential(exclude_cli_credential=not use_cli))
      if not chain:
        raise ValueError("At least one credential must be enabled or provided.")
      credential = ChainedTokenCredential(*chain)

    # You can customize retry policy if needed; defaults are fine for most cases
    self.client = SecretClient(vault_url=self.vault_url, credential=credential)

  # ----------------- public API -----------------

  def get_secret_value(self, ref: str) -> Optional[str]:
    """
    Resolve a secret *value* by reference. Returns None if not found or disabled.
    """
    name = self._map_name(ref)
    if self._cache_enabled:
      return self._cached_get(name)
    return self._fetch(name)

  # ----------------- internals ------------------

  def _map_name(self, ref: str) -> str:
    """
    Maps a logical ref (e.g. 'sec/prod/pepper') to a KV-valid *name* (e.g. 'sec-prod-pepper').
    """
    name = self.name_mapper(ref)
    if not name or "/" in name:
      # defensive check: KV names cannot contain '/'
      name = name.replace("/", "-") if name else ""
    return name

  def _fetch(self, name: str) -> Optional[str]:
    if not name:
      return None
    try:
      s = self.client.get_secret(name)
      return s.value
    except ResourceNotFoundError:
      return None
    except HttpResponseError:
      # network/transient/server-side errors; let the resolver try next provider
      return None

  @lru_cache(maxsize=256)
  def _cached_get(self, name: str) -> Optional[str]:
    return self._fetch(name)
