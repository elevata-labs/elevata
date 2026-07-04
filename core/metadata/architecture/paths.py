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

from dataclasses import dataclass
import os
from pathlib import Path
import re

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured


DEFAULT_ARCHITECTURE_STATE_DIR = ".elevata/state"
DEFAULT_ARCHITECTURE_APPROVAL_DIR = ".elevata/approvals"
DEFAULT_ARCHITECTURE_EXECUTION_DIR = ".elevata/executions"

ARCHITECTURE_STATE_DIR_ENV = "ELEVATA_ARCH_STATE_DIR"
ARCHITECTURE_APPROVAL_DIR_ENV = "ELEVATA_ARCH_APPROVAL_DIR"
ARCHITECTURE_EXECUTION_DIR_ENV = "ELEVATA_ARCH_EXECUTION_DIR"


@dataclass(frozen=True)
class ArchitectureArtifactContext:
  """
  Runtime context used to scope architecture control artifacts.

  Architecture state, approvals and execution records are target-runtime
  specific. The same metadata can be executed against multiple target systems,
  and each target system needs its own recorded architecture baseline and audit
  evidence.
  """
  profile_name: str
  target_system_short: str

  @property
  def profile_token(self) -> str:
    """
    Return a filesystem-safe profile token.
    """
    return _safe_path_token(self.profile_name, fallback="profile")

  @property
  def target_system_token(self) -> str:
    """
    Return a filesystem-safe target system token.
    """
    return _safe_path_token(self.target_system_short, fallback="target")

  @property
  def label(self) -> str:
    """
    Return a compact human-readable context label.
    """
    return f"{self.profile_name}/{self.target_system_short}"


def _configured_base_path(
  *,
  setting_name: str,
  env_name: str,
  default: str | Path,
) -> Path:
  """
  Return a configured architecture artifact base path.

  Django settings own runtime path normalization. Absolute environment values
  remain valid explicit overrides for tests and deployments. Relative default
  environment values are resolved through Django settings so they are not bound
  to the current working directory.
  """
  setting_value = None

  try:
    setting_value = getattr(settings, setting_name, None)
  except ImproperlyConfigured:
    setting_value = None

  env_value = os.getenv(env_name)
  if env_value and env_value.strip():
    env_path = Path(env_value.strip()).expanduser()

    if env_path.is_absolute():
      return env_path

    if setting_value is None:
      return env_path

    default_text = str(Path(default)).replace("\\", "/").strip()
    env_text = str(env_path).replace("\\", "/").strip()

    if env_text and env_text != default_text:
      return env_path

  if setting_value is not None and str(setting_value).strip():
    return Path(str(setting_value).strip()).expanduser()

  if env_value and env_value.strip():
    return Path(env_value.strip()).expanduser()

  return Path(default).expanduser()


def resolve_architecture_artifact_context(
  *,
  profile_name: str | None = None,
  target_system_short: str | None = None,
) -> ArchitectureArtifactContext:
  """
  Resolve the active profile/target-system artifact context.
  """
  profile_value = (profile_name or "").strip()
  target_value = (target_system_short or "").strip()

  if not profile_value:
    profile_value = _active_profile_name()

  if not target_value:
    target_value = _active_target_system_short()

  return ArchitectureArtifactContext(
    profile_name=profile_value or "default",
    target_system_short=target_value or "default",
  )


def resolve_architecture_state_dir(
  default: str | Path = DEFAULT_ARCHITECTURE_STATE_DIR,
  *,
  context: ArchitectureArtifactContext | None = None,
) -> Path:
  """
  Resolve the architecture state directory for the active runtime context.
  """
  return _configured_artifact_path(
    setting_name="ELEVATA_ARCH_STATE_DIR",
    env_name=ARCHITECTURE_STATE_DIR_ENV,
    default=default,
    context=context,
  )


def resolve_architecture_approval_dir(
  default: str | Path = DEFAULT_ARCHITECTURE_APPROVAL_DIR,
  *,
  context: ArchitectureArtifactContext | None = None,
) -> Path:
  """
  Resolve the architecture approval artifact directory.
  """
  return _configured_artifact_path(
    setting_name="ELEVATA_ARCH_APPROVAL_DIR",
    env_name=ARCHITECTURE_APPROVAL_DIR_ENV,
    default=default,
    context=context,
  )


def resolve_architecture_execution_dir(
  default: str | Path = DEFAULT_ARCHITECTURE_EXECUTION_DIR,
  *,
  context: ArchitectureArtifactContext | None = None,
) -> Path:
  """
  Resolve the architecture execution record directory.
  """
  return _configured_artifact_path(
    setting_name="ELEVATA_ARCH_EXECUTION_DIR",
    env_name=ARCHITECTURE_EXECUTION_DIR_ENV,
    default=default,
    context=context,
  )


def architecture_state_file(
  *,
  context: ArchitectureArtifactContext | None = None,
) -> Path:
  """
  Return the active architecture state file path.
  """
  return resolve_architecture_state_dir(context=context) / "architecture_state.json"


def _configured_artifact_path(
  *,
  setting_name: str,
  env_name: str,
  default: str | Path,
  context: ArchitectureArtifactContext | None,
) -> Path:
  """
  Return a context-scoped architecture artifact path.

  Tests may monkeypatch absolute environment directories after Django settings
  have been loaded. In that case the environment value is an explicit full
  override and is not scoped further. Normal runtime configuration still uses
  the settings-normalized base path and appends profile/target-system scope.
  """
  base_path = _configured_base_path(
    setting_name=setting_name,
    env_name=env_name,
    default=default,
  )

  if _is_explicit_environment_path_override(
    setting_name=setting_name,
    env_name=env_name,
  ):
    return base_path

  return _scoped_path(base_path, context=context)


def _is_explicit_environment_path_override(
  *,
  setting_name: str,
  env_name: str,
) -> bool:
  """
  Return True when the current environment value differs from Django settings.
  """
  env_value = os.getenv(env_name)
  if not env_value or not env_value.strip():
    return False

  try:
    setting_value = getattr(settings, setting_name, None)
  except ImproperlyConfigured:
    return False

  if setting_value is None or not str(setting_value).strip():
    return False

  return _norm_path_text(env_value) != _norm_path_text(setting_value)


def _norm_path_text(value: str | Path) -> str:
  """
  Return a normalized path text for configuration comparison.
  """
  return str(Path(str(value).strip()).expanduser()).replace("\\", "/").rstrip("/")


def _scoped_path(
  base_path: Path,
  *,
  context: ArchitectureArtifactContext | None = None,
) -> Path:
  """
  Append the runtime artifact context to one architecture artifact base path.
  """
  ctx = context or resolve_architecture_artifact_context()
  return base_path / ctx.profile_token / ctx.target_system_token


def _active_profile_name() -> str:
  """
  Return the active profile name without failing hard during standalone imports.
  """
  try:
    from metadata.config.profiles import load_profile

    profile = load_profile(None)
    value = getattr(profile, "name", None)
    if value:
      return str(value)
  except Exception:
    pass

  return os.getenv("ELEVATA_PROFILE", "dev")


def _active_target_system_short() -> str:
  """
  Return the active target system short name without failing hard during imports.
  """
  try:
    from metadata.config.targets import get_target_system

    target_system = get_target_system(None)
    value = getattr(target_system, "short_name", None)
    if value:
      return str(value)
  except Exception:
    pass

  return os.getenv("ELEVATA_TARGET_SYSTEM", "default")


def _safe_path_token(value: str | None, *, fallback: str) -> str:
  """
  Return a deterministic filesystem-safe path token.
  """
  token = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip())
  token = token.strip("._-")
  return token or fallback
