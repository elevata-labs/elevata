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

from metadata.generation.target_generation_service import (
  TargetGenerationResult,
  TargetGenerationService,
)
from metadata.management.commands import generate_targets as command_module


def test_target_generation_result_labels_processed_items_truthfully() -> None:
  """Verify processed counters are not presented as generated or updated rows."""
  result = TargetGenerationResult(
    processed_dataset_count=25,
    processed_column_count=168,
    retired_dataset_count=1,
    reactivated_dataset_count=2,
  )

  assert result.summary_text == (
    "25 target datasets processed and 168 target columns processed; "
    "1 retired and 2 reactivated."
  )
  assert "generated/updated" not in result.summary_text
  assert str(result) == result.summary_text


def test_apply_all_returns_structured_processed_and_lifecycle_counts(
  monkeypatch,
) -> None:
  """Verify schema generation returns explicit counters instead of summary text."""
  service = TargetGenerationService(pepper="test-pepper")
  target_schema = SimpleNamespace(short_name="raw")
  source_dataset = SimpleNamespace(pk=1)
  target_dataset = SimpleNamespace(pk=101)
  dataset_draft = SimpleNamespace(target_dataset_name="raw_customer")
  column_drafts = (object(), object(), object())

  monkeypatch.setattr(
    service,
    "_bucket_source_datasets",
    lambda eligible, schema: {"raw_customer": [source_dataset]},
  )
  monkeypatch.setattr(
    service,
    "build_dataset_bundle",
    lambda representative, schema: {
      "dataset": dataset_draft,
      "columns": column_drafts,
    },
  )
  monkeypatch.setattr(
    service,
    "_determine_combination_mode",
    lambda schema, sources: "single",
  )
  monkeypatch.setattr(
    service,
    "_get_or_create_target_dataset",
    lambda **kwargs: (target_dataset, False),
  )
  monkeypatch.setattr(
    service,
    "_ensure_surrogate_key_draft_names",
    lambda *args, **kwargs: None,
  )
  monkeypatch.setattr(
    service,
    "_sync_dataset_inputs",
    lambda **kwargs: None,
  )
  monkeypatch.setattr(
    service,
    "_sync_target_columns",
    lambda **kwargs: len(column_drafts),
  )
  monkeypatch.setattr(
    service,
    "_ensure_tech_columns",
    lambda *args, **kwargs: None,
  )
  monkeypatch.setattr(
    service,
    "_backfill_bundle_audit",
    lambda *args, **kwargs: None,
  )
  monkeypatch.setattr(
    service,
    "_reconcile_generated_target_lifecycle",
    lambda **kwargs: (1, 2),
  )

  result = service.apply_all_result(
    [source_dataset],
    target_schema,
    reconcile_lifecycle=True,
  )

  expected = TargetGenerationResult(
    processed_dataset_count=1,
    processed_column_count=3,
    retired_dataset_count=1,
    reactivated_dataset_count=2,
  )
  assert result == expected

  monkeypatch.setattr(service, "apply_all_result", lambda *args, **kwargs: expected)
  assert service.apply_all([source_dataset], target_schema) == expected.summary_text


def test_generate_targets_command_aggregates_structured_results(
  monkeypatch,
) -> None:
  """Verify the command totals explicit counters without parsing display text."""
  schemas = (
    SimpleNamespace(short_name="raw", physical_prefix=None),
    SimpleNamespace(short_name="stage", physical_prefix="stg"),
  )
  results = {
    "raw": TargetGenerationResult(
      processed_dataset_count=2,
      processed_column_count=10,
      retired_dataset_count=1,
      reactivated_dataset_count=0,
    ),
    "stage": TargetGenerationResult(
      processed_dataset_count=3,
      processed_column_count=12,
      retired_dataset_count=0,
      reactivated_dataset_count=1,
    ),
  }
  calls = []

  class FakeTargetGenerationService:
    """Target generation service test double."""

    def __init__(self, *, pepper, actor):
      assert pepper == "test-pepper"
      assert actor is None

    def get_target_schemas_in_scope(self):
      return list(schemas)

    def get_eligible_source_datasets_for_schema(self, schema):
      return [SimpleNamespace(pk=schema.short_name)]

    def apply_all_result(self, eligible, schema, *, reconcile_lifecycle):
      calls.append((schema.short_name, reconcile_lifecycle, len(eligible)))
      return results[schema.short_name]

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
  )

  output = stdout.getvalue()
  assert calls == [
    ("raw", True, 1),
    ("stage", True, 1),
  ]
  assert (
    "raw: 2 target datasets processed and 10 target columns processed; "
    "1 retired and 0 reactivated."
    in output
  )
  assert (
    "stg: 3 target datasets processed and 12 target columns processed; "
    "0 retired and 1 reactivated."
    in output
  )
  assert (
    "Done. Total: 5 target datasets processed and 22 target columns processed; "
    "1 target datasets retired and 1 reactivated."
    in output
  )
  assert "generated/updated" not in output
