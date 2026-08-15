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

from types import SimpleNamespace

import pytest

from metadata.promotion import deployment_package_store
from metadata.promotion.deployment_package_store import (
  EnvironmentPromotionDeploymentPackageStore,
  EnvironmentPromotionDeploymentPackageStoreError,
)


def _package(*, fingerprint: str):
  return SimpleNamespace(
    target_environment_label="test",
    approval=SimpleNamespace(approval_id="papr-1234567890abcdef"),
    package_fingerprint=fingerprint,
  )


def test_deployment_package_store_is_immutable_and_idempotent(tmp_path, monkeypatch):
  first = _package(fingerprint="a" * 64)
  monkeypatch.setattr(
    deployment_package_store,
    "serialize_environment_promotion_deployment_package",
    lambda package: "exact-package\n",
  )
  monkeypatch.setattr(
    deployment_package_store,
    "deserialize_environment_promotion_deployment_package",
    lambda payload: first,
  )
  store = EnvironmentPromotionDeploymentPackageStore(tmp_path)

  first_path = store.save(first)
  second_path = store.save(first)

  assert second_path == first_path
  assert first_path.name == "papr-1234567890abcdef.deployment.json"
  assert first_path.parent.name == "test"
  assert store.load_for_approval(
    target_environment_label="test",
    approval_id="papr-1234567890abcdef",
  ) is first

  different = _package(fingerprint="b" * 64)
  with pytest.raises(
    EnvironmentPromotionDeploymentPackageStoreError,
    match="different immutable deployment package",
  ):
    store.save(different)
