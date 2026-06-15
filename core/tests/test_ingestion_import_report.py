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

from metadata.ingestion import import_service
from metadata.ingestion.import_report import (
  SourceMetadataImportColumnChange,
  SourceMetadataImportDatasetReport,
  SourceMetadataImportReport,
)


class _FakeSystem:
  id = 1
  type = "csv"
  short_name = "csv"


class _FakeDataset:
  schema_name = None
  source_dataset_name = "products"
  source_system = _FakeSystem()


def test_source_metadata_import_report_keeps_legacy_summary_shape():
  report = SourceMetadataImportReport(
    autointegrate_pk=True,
    reset_flags=False,
  )
  dataset_report = SourceMetadataImportDatasetReport(
    dataset_key="products",
    source_system="csv",
    source_type="csv",
    columns_imported=2,
    created=1,
    updated=1,
    changed=1,
    unchanged=0,
    removed=0,
    pk_detected=["id"],
    column_changes=[
      SourceMetadataImportColumnChange(
        name="id",
        action="created",
        datatype="INTEGER",
        primary_key_column=True,
        json_path="$.id",
      ),
    ],
  )
  report.add_dataset(dataset_report)

  result = report.as_result_dict()

  assert result["datasets"] == 1
  assert result["columns_imported"] == 2
  assert result["created"] == 1
  assert result["updated"] == 1
  assert result["removed"] == 0
  assert result["skipped_count"] == 0
  assert result["pk_detected_count"] == 1
  assert result["needs_review_count"] == 0
  assert result["report"]["datasets"][0]["column_changes"][0]["name"] == "id"


def test_import_metadata_for_datasets_adds_review_report_for_file_import(monkeypatch):
  def fake_file_import(ds, *, file_type, autointegrate_pk=True, reset_flags=False):
    return {
      "columns_imported": 2,
      "created": 1,
      "updated": 1,
      "changed": 1,
      "unchanged": 0,
      "removed": 0,
      "pk_detected": ["id"],
      "column_changes": [
        {
          "name": "id",
          "action": "created",
          "datatype": "INTEGER",
          "primary_key_column": True,
          "json_path": "$.id",
        },
        {
          "name": "name",
          "action": "changed",
          "datatype": "STRING",
          "primary_key_column": False,
          "json_path": "$.name",
        },
      ],
    }

  monkeypatch.setattr(import_service, "import_file_metadata_for_dataset", fake_file_import)

  result = import_service.import_metadata_for_datasets(
    [_FakeDataset()],
    autointegrate_pk=True,
    reset_flags=True,
  )

  assert result["datasets"] == 1
  assert result["columns_imported"] == 2
  assert result["created"] == 1
  assert result["updated"] == 1
  assert result["changed"] == 1
  assert result["unchanged"] == 0
  assert result["pk_detected_count"] == 1
  assert result["needs_review_count"] == 0
  assert result["report"]["options"]["reset_flags"] is True
  assert result["report"]["datasets"][0]["pk_detected"] == ["id"]
  assert len(result["report"]["datasets"][0]["column_changes"]) == 2


def test_import_metadata_for_datasets_reports_skipped_file_import(monkeypatch):
  def fake_file_import(ds, *, file_type, autointegrate_pk=True, reset_flags=False):
    raise ValueError("No rows found")

  monkeypatch.setattr(import_service, "import_file_metadata_for_dataset", fake_file_import)

  result = import_service.import_metadata_for_datasets([_FakeDataset()])

  assert result["datasets"] == 0
  assert result["skipped_count"] == 1
  assert result["needs_review_count"] == 1
  assert result["report"]["datasets"][0]["status"] == "skipped"
  assert result["report"]["datasets"][0]["notes"][0]["code"] == "dataset_skipped"
