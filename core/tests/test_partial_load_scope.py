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

import pytest

from metadata.execution.load_scope import (
  FULL_LOAD_SCOPE_NAME,
  LOAD_SCOPE_MODE_PARTIAL_LOAD,
  LoadScopeError,
  resolve_partial_load_scope,
)
from metadata.models import (
  PartialLoad,
  TargetDataset,
  TargetDatasetInput,
  TargetSchema,
)



def _schema(short_name: str) -> TargetSchema:
  schema, _ = TargetSchema.objects.get_or_create(
    short_name=short_name,
    defaults={
      "schema_name": short_name[:10],
      "display_name": short_name,
    },
  )
  return schema



def _dataset(
  schema: TargetSchema,
  name: str,
  *,
  active: bool = True,
  incremental_strategy: str = "full",
  historize: bool = False,
  lineage_key: str | None = None,
) -> TargetDataset:
  return TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=name,
    active=active,
    incremental_strategy=incremental_strategy,
    historize=historize,
    lineage_key=lineage_key,
  )


@pytest.mark.django_db
def test_partial_load_requires_at_least_one_execution_root():
  partial = PartialLoad.objects.create(name="empty")

  with pytest.raises(LoadScopeError, match="at least one execution root"):
    resolve_partial_load_scope(partial)


@pytest.mark.django_db
def test_partial_load_name_full_is_reserved_for_implicit_full_scope():
  rawcore = _schema("rawcore")
  root = _dataset(rawcore, "rc_scope_full_name")
  partial = PartialLoad.objects.create(name=FULL_LOAD_SCOPE_NAME)
  partial.datasets.add(root)

  with pytest.raises(LoadScopeError, match="reserved for the implicit Full Load"):
    resolve_partial_load_scope(partial)


@pytest.mark.django_db
def test_partial_load_rejects_inactive_root():
  rawcore = _schema("rawcore")
  root = _dataset(rawcore, "rc_scope_inactive", active=False)
  partial = PartialLoad.objects.create(name="inactive")
  partial.datasets.add(root)

  with pytest.raises(LoadScopeError, match="inactive execution root"):
    resolve_partial_load_scope(partial)


@pytest.mark.django_db
def test_partial_load_rejects_history_dataset_as_explicit_root():
  rawcore = _schema("rawcore")
  hist = _dataset(
    rawcore,
    "rc_scope_hist_root_hist",
    incremental_strategy="historize",
  )
  partial = PartialLoad.objects.create(name="histroot")
  partial.datasets.add(hist)

  with pytest.raises(LoadScopeError, match="history dataset"):
    resolve_partial_load_scope(partial)


@pytest.mark.django_db
def test_partial_load_resolves_upstreams_and_history_companions():
  stage = _schema("stage")
  rawcore = _schema("rawcore")
  bizcore = _schema("bizcore")

  stage_td = _dataset(stage, "stg_scope_customer")
  base = _dataset(
    rawcore,
    "rc_scope_customer",
    historize=True,
    lineage_key="generated:rawcore:partial-scope-customer",
  )
  hist = _dataset(
    rawcore,
    "rc_scope_customer_hist",
    incremental_strategy="historize",
    lineage_key=base.lineage_key,
  )
  root = _dataset(bizcore, "bc_scope_customer")

  TargetDatasetInput.objects.create(
    target_dataset=base,
    upstream_target_dataset=stage_td,
    source_dataset=None,
    role="primary",
    active=True,
  )
  TargetDatasetInput.objects.create(
    target_dataset=root,
    upstream_target_dataset=base,
    source_dataset=None,
    role="primary",
    active=True,
  )

  partial = PartialLoad.objects.create(name="customer")
  partial.datasets.add(root)

  resolved = resolve_partial_load_scope(partial)

  assert resolved.scope_mode == LOAD_SCOPE_MODE_PARTIAL_LOAD
  assert resolved.scope_key == "customer"
  assert resolved.root_dataset_keys == ("bizcore.bc_scope_customer",)
  assert set(resolved.execution_dataset_keys) == {
    "stage.stg_scope_customer",
    "rawcore.rc_scope_customer",
    "rawcore.rc_scope_customer_hist",
    "bizcore.bc_scope_customer",
  }
  assert resolved.execution_dataset_keys.index("stage.stg_scope_customer") < (
    resolved.execution_dataset_keys.index("rawcore.rc_scope_customer")
  )
  assert resolved.execution_dataset_keys.index("rawcore.rc_scope_customer") < (
    resolved.execution_dataset_keys.index("rawcore.rc_scope_customer_hist")
  )
  assert resolved.execution_dataset_keys.index("rawcore.rc_scope_customer") < (
    resolved.execution_dataset_keys.index("bizcore.bc_scope_customer")
  )

  # Hist is runtime-resolved and never becomes explicit user intent.
  assert hist not in resolved.roots


@pytest.mark.django_db
def test_partial_load_keeps_redundant_explicit_roots_but_deduplicates_execution():
  stage = _schema("stage")
  bizcore = _schema("bizcore")

  upstream_root = _dataset(stage, "stg_scope_redundant")
  downstream_root = _dataset(bizcore, "bc_scope_redundant")
  TargetDatasetInput.objects.create(
    target_dataset=downstream_root,
    upstream_target_dataset=upstream_root,
    source_dataset=None,
    role="primary",
    active=True,
  )

  partial = PartialLoad.objects.create(name="redundant")
  partial.datasets.add(upstream_root, downstream_root)

  resolved = resolve_partial_load_scope(partial)

  assert resolved.root_dataset_keys == (
    "bizcore.bc_scope_redundant",
    "stage.stg_scope_redundant",
  )
  assert len(resolved.execution_dataset_keys) == 2
  assert set(resolved.execution_dataset_keys) == {
    "stage.stg_scope_redundant",
    "bizcore.bc_scope_redundant",
  }


@pytest.mark.django_db
def test_partial_load_fails_closed_when_declared_root_graph_is_invalid():
  rawcore = _schema("rawcore")
  base = _dataset(
    rawcore,
    "rc_scope_missing_hist",
    historize=True,
    lineage_key="generated:rawcore:partial-scope-missing-hist",
  )
  partial = PartialLoad.objects.create(name="broken")
  partial.datasets.add(base)

  with pytest.raises(LoadScopeError) as exc_info:
    resolve_partial_load_scope(partial)

  assert "rc_scope_missing_hist" in str(exc_info.value)
  assert "history companion" in str(exc_info.value)


@pytest.mark.django_db
def test_partial_load_can_be_resolved_by_exact_name():
  bizcore = _schema("bizcore")
  root = _dataset(bizcore, "bc_scope_by_name")
  partial = PartialLoad.objects.create(name="byname")
  partial.datasets.add(root)

  resolved = resolve_partial_load_scope("byname")

  assert resolved.scope_key == "byname"
  assert resolved.root_dataset_keys == ("bizcore.bc_scope_by_name",)
