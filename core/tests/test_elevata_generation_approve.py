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

from metadata.generation.target_generation_control import (
  parse_target_generation_approval_json,
)
from metadata.generation.target_generation_plan import (
  TargetGenerationAction,
  build_target_generation_plan,
  render_target_generation_plan_json,
)
from metadata.management.commands import elevata_generation_approve


def test_generation_approve_command_writes_distinct_approval_artifact(
  tmp_path,
) -> None:
  """Verify command output binds the exact plan through a Generation Review."""
  plan = build_target_generation_plan(
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
  plan_path = tmp_path / "target_generation_plan.json"
  approval_path = tmp_path / "target_generation_approval.json"
  plan_path.write_text(
    render_target_generation_plan_json(plan),
    encoding="utf-8",
  )

  stdout = StringIO()
  command = elevata_generation_approve.Command(
    stdout=stdout,
    no_color=True,
  )
  command.handle(
    plan_file=str(plan_path),
    approved_by="Ilona Tag",
    note="Reviewed Source-to-Target impact.",
    decided_at="2026-07-30T17:30:00Z",
    output=str(approval_path),
    store=False,
  )

  approval = parse_target_generation_approval_json(
    approval_path.read_text(encoding="utf-8")
  )
  assert approval.approval_id.startswith("gpa_")
  assert approval.review["plan_fingerprint"] == plan.plan_fingerprint
  assert "Generation Approval" in stdout.getvalue()
