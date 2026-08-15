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

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar


_METADATA_ARTIFACT_RECONSTRUCTION_DEPTH: ContextVar[int] = ContextVar(
  "elevata_metadata_artifact_reconstruction_depth",
  default=0,
)


def metadata_artifact_reconstruction_active() -> bool:
  """
  Return whether approved metadata artifact reconstruction is in progress.

  Derivation hooks must not reinterpret or extend metadata while an immutable
  artifact is being reconstructed. The artifact itself is authoritative.
  """
  return _METADATA_ARTIFACT_RECONSTRUCTION_DEPTH.get() > 0


@contextmanager
def metadata_artifact_reconstruction_context() -> Iterator[None]:
  """
  Suppress metadata derivation side effects during artifact reconstruction.

  ContextVar keeps the guard local to the current execution context and nested
  use is safe. The caller still owns transaction and convergence semantics.
  """
  depth = _METADATA_ARTIFACT_RECONSTRUCTION_DEPTH.get()
  token = _METADATA_ARTIFACT_RECONSTRUCTION_DEPTH.set(depth + 1)
  try:
    yield
  finally:
    _METADATA_ARTIFACT_RECONSTRUCTION_DEPTH.reset(token)
