"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2026 Ilona Tag

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

from types import SimpleNamespace

import pytest
from django.test import RequestFactory

import metadata.architecture.control as control
import metadata.architecture.execution_preview as preview
import metadata.views as views
from metadata.architecture.control import (
  ArchitectureControlError,
  ArchitectureControlScope,
)
from metadata.architecture.execution_preview import ArchitectureExecutionPreviewError


def _target(schema_short: str, name: str):
  """Build a minimal TargetDataset-shaped execution test double."""
  return SimpleNamespace(
    id=1,
    pk=1,
    active=True,
    is_hist=False,
    target_schema=SimpleNamespace(short_name=schema_short),
    target_dataset_name=name,
  )


def test_partial_load_scope_has_stable_control_identity():
  scope = ArchitectureControlScope.for_partial_load("sales")

  assert scope.mode == "partial_load"
  assert scope.partial_load_name == "sales"
  assert scope.key == "partial_load:sales"
  assert scope.label == "Partial load: sales"


def test_partial_load_scope_rejects_empty_name():
  with pytest.raises(ArchitectureControlError, match="requires a Partial Load name"):
    ArchitectureControlScope.for_partial_load("")


def test_partial_load_report_scope_uses_resolved_execution_membership(monkeypatch):
  resolved = SimpleNamespace(
    execution_dataset_keys=(
      "raw.raw_sales",
      "stage.stg_sales",
      "rawcore.rc_sales",
      "rawcore.rc_sales_hist",
      "bizcore.bc_sales",
    ),
  )
  monkeypatch.setattr(
    control,
    "resolve_partial_load_scope",
    lambda name: resolved,
  )

  keys = control._resolve_relevant_dataset_keys(
    scope=ArchitectureControlScope.for_partial_load("sales"),
    current_state=SimpleNamespace(),
  )

  assert keys == set(resolved.execution_dataset_keys)


def test_partial_load_execution_scope_reuses_canonical_load_scope(monkeypatch):
  root = _target("bizcore", "bc_sales")
  raw = _target("raw", "raw_sales")
  stage = _target("stage", "stg_sales")
  rawcore = _target("rawcore", "rc_sales")
  hist = _target("rawcore", "rc_sales_hist")

  resolved = SimpleNamespace(
    roots=(root,),
    execution_order=(raw, stage, rawcore, hist, root),
  )
  seen: list[str] = []

  def resolve(name: str):
    seen.append(name)
    return resolved

  monkeypatch.setattr(preview, "resolve_partial_load_scope", resolve)

  result = preview.resolve_architecture_execution_scope(
    ArchitectureControlScope.for_partial_load("sales"),
  )

  assert seen == ["sales"]
  assert result.roots == (root,)
  assert result.execution_order == resolved.execution_order
  assert result.dependency_mode == "with_dependencies"


def test_partial_load_execution_scope_rejects_target_only_mode(monkeypatch):
  monkeypatch.setattr(
    preview,
    "resolve_partial_load_scope",
    lambda name: pytest.fail("resolver must not run for target-only Partial Load"),
  )

  with pytest.raises(
    ArchitectureExecutionPreviewError,
    match="always includes required dependencies",
  ):
    preview.resolve_architecture_execution_scope(
      ArchitectureControlScope.for_partial_load("sales"),
      no_deps=True,
    )


def test_architecture_control_request_preserves_partial_load_name():
  request = RequestFactory().get(
    "/architecture-control/",
    {
      "scope_mode": "partial_load",
      "partial_load_name": "sales",
      "execution_no_deps": "1",
    },
  )

  params = views._architecture_control_scope_params(request)
  scope = views._architecture_control_scope_from_params(params)

  assert params == {
    "scope_mode": "partial_load",
    "partial_load_name": "sales",
  }
  assert views._architecture_control_no_deps_from_params(params) is False
  assert scope == ArchitectureControlScope.for_partial_load("sales")
