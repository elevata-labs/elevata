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

from io import StringIO
from types import SimpleNamespace

from metadata.generation.target_generation_control import (
  build_target_generation_approval,
  build_target_generation_review,
  render_target_generation_approval_json,
  render_target_generation_review_json,
)
from metadata.generation.target_generation_plan import (
  TargetGenerationAction,
  build_target_generation_plan,
  render_target_generation_plan_json,
)
from metadata.management.commands import generate_targets as command_module


def _plan():
  """Return one deterministic command-level generation plan."""
  return build_target_generation_plan(
    scope_mode="schema",
    target_schema_short_names=("raw",),
    source_dataset_keys=("source_dataset:23",),
    reconcile_lifecycle=True,
    source_metadata_fingerprint="1" * 64,
    target_metadata_fingerprint="2" * 64,
    actions=(
      TargetGenerationAction(
        action_type="UPDATE_TARGET_COLUMN",
        dataset_key="raw:source_dataset:23:base",
        object_key=(
          "raw:source_dataset:23:base:column:source_column:158"
        ),
        effect_origin="DIRECT",
        change_classification="BREAKING",
        source_keys=("source_dataset:23",),
        before={"max_length": 100},
        after={"max_length": 110},
      ),
    ),
  )


def test_generate_targets_dry_run_writes_source_to_target_review(
  monkeypatch,
  tmp_path,
) -> None:
  """Verify dry-run emits the review and persists its canonical artifact."""
  schema = SimpleNamespace(short_name="raw", physical_prefix="raw")
  plan = _plan()
  review = build_target_generation_review(plan)
  review_path = tmp_path / "target_generation_review.json"

  class FakeTargetGenerationService:
    """Read-only generation service test double."""

    def __init__(self, *, pepper, actor):
      assert pepper == "test-pepper"
      assert actor is None

    def get_target_schemas_in_scope(self):
      return [schema]

    def get_eligible_source_datasets_for_schema(self, selected_schema):
      assert selected_schema is schema
      return [SimpleNamespace(pk=23)]

    def build_plan(self, eligible, selected_schema, *, reconcile_lifecycle):
      assert len(eligible) == 1
      assert selected_schema is schema
      assert reconcile_lifecycle is True
      return plan

  monkeypatch.setattr(
    command_module,
    "TargetGenerationService",
    FakeTargetGenerationService,
  )
  monkeypatch.setattr(
    command_module,
    "get_runtime_pepper",
    lambda: "test-pepper",
  )

  stdout = StringIO()
  command = command_module.Command(stdout=stdout, no_color=True)
  command.handle(
    actor_id=None,
    schema_short_name="raw",
    dry_run=True,
    plan_file=None,
    plan_output=None,
    review_output=str(review_path),
    generation_approval_file=None,
    require_generation_approval=False,
  )

  assert review_path.read_text(encoding="utf-8") == (
    render_target_generation_review_json(review)
  )
  assert "Target Generation Review" in stdout.getvalue()
  assert "source_dataset:23 -> raw:source_dataset:23:base" in (
    stdout.getvalue()
  )


def test_generate_targets_applies_plan_with_explicit_generation_approval(
  monkeypatch,
  tmp_path,
) -> None:
  """Verify command apply passes the exact approval into the guarded service."""
  plan = _plan()
  review = build_target_generation_review(plan)
  approval = build_target_generation_approval(
    review=review,
    decided_by="Ilona Tag",
    decided_at="2026-07-30T17:30:00Z",
  )
  plan_path = tmp_path / "target_generation_plan.json"
  approval_path = tmp_path / "target_generation_approval.json"
  plan_path.write_text(
    render_target_generation_plan_json(plan),
    encoding="utf-8",
  )
  approval_path.write_text(
    render_target_generation_approval_json(approval),
    encoding="utf-8",
  )
  observed = []

  class FakeTargetGenerationService:
    """Approval-aware guarded service test double."""

    def __init__(self, *, pepper, actor):
      assert pepper == "test-pepper"
      assert actor is None

    def apply_plan(
      self,
      supplied_plan,
      *,
      approval=None,
      require_approval=False,
    ):
      observed.append((
        supplied_plan.plan_fingerprint,
        approval.approval_id,
        require_approval,
      ))
      return SimpleNamespace(summary_text="Applied approved generation plan.")

  monkeypatch.setattr(
    command_module,
    "TargetGenerationService",
    FakeTargetGenerationService,
  )
  monkeypatch.setattr(
    command_module,
    "get_runtime_pepper",
    lambda: "test-pepper",
  )

  stdout = StringIO()
  command = command_module.Command(stdout=stdout, no_color=True)
  command.handle(
    actor_id=None,
    schema_short_name=None,
    dry_run=False,
    plan_file=str(plan_path),
    plan_output=None,
    review_output=None,
    generation_approval_file=str(approval_path),
    require_generation_approval=True,
  )

  assert observed == [(plan.plan_fingerprint, approval.approval_id, True)]
  assert f"Generation Approval {approval.approval_id} verified" in (
    stdout.getvalue()
  )
  assert "Applied approved generation plan." in stdout.getvalue()
