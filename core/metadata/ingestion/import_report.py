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

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class SourceMetadataImportNote:
  """
  Structured note for a source metadata import report.
  """
  severity: str
  code: str
  message: str

  def as_dict(self) -> dict[str, str]:
    """Return a deterministic dictionary representation."""
    return {
      "severity": self.severity,
      "code": self.code,
      "message": self.message,
    }


@dataclass(frozen=True)
class SourceMetadataImportColumnChange:
  """
  Review-friendly description of one imported SourceColumn change.
  """
  name: str
  action: str
  datatype: str | None = None
  source_datatype_raw: str | None = None
  nullable: bool | None = None
  primary_key_column: bool = False
  json_path: str | None = None

  @classmethod
  def from_dict(cls, value: dict[str, Any]) -> "SourceMetadataImportColumnChange":
    """Build a column change from an import result dictionary."""
    return cls(
      name=str(value.get("name") or ""),
      action=str(value.get("action") or "updated"),
      datatype=value.get("datatype"),
      source_datatype_raw=value.get("source_datatype_raw"),
      nullable=value.get("nullable"),
      primary_key_column=bool(value.get("primary_key_column") or False),
      json_path=value.get("json_path"),
    )

  def as_dict(self) -> dict[str, Any]:
    """Return a deterministic dictionary representation."""
    return {
      "name": self.name,
      "action": self.action,
      "datatype": self.datatype,
      "source_datatype_raw": self.source_datatype_raw,
      "nullable": self.nullable,
      "primary_key_column": self.primary_key_column,
      "json_path": self.json_path,
    }


@dataclass
class SourceMetadataImportDatasetReport:
  """
  Review-friendly import outcome for a single SourceDataset.
  """
  dataset_key: str
  source_system: str
  source_type: str
  status: str = "imported"
  columns_imported: int = 0
  created: int = 0
  updated: int = 0
  changed: int = 0
  unchanged: int = 0
  removed: int = 0
  pk_detected: list[str] = field(default_factory=list)
  column_changes: list[SourceMetadataImportColumnChange] = field(default_factory=list)
  notes: list[SourceMetadataImportNote] = field(default_factory=list)

  @property
  def needs_review(self) -> bool:
    """Return whether this dataset has warning or error notes."""
    return any(n.severity in {"warning", "error"} for n in self.notes)

  def add_note(self, *, severity: str, code: str, message: str) -> None:
    """Append a deterministic report note."""
    self.notes.append(
      SourceMetadataImportNote(
        severity=severity,
        code=code,
        message=message,
      )
    )

  def as_dict(self) -> dict[str, Any]:
    """Return a deterministic dictionary representation."""
    return {
      "dataset_key": self.dataset_key,
      "source_system": self.source_system,
      "source_type": self.source_type,
      "status": self.status,
      "columns_imported": self.columns_imported,
      "created": self.created,
      "updated": self.updated,
      "changed": self.changed,
      "unchanged": self.unchanged,
      "removed": self.removed,
      "pk_detected": sorted(self.pk_detected),
      "pk_detected_count": len(set(self.pk_detected)),
      "needs_review": self.needs_review,
      "notes": [n.as_dict() for n in self.notes],
      "column_changes": [c.as_dict() for c in self.column_changes],
    }


@dataclass
class SourceMetadataImportReport:
  """
  Non-persistent review report for one source metadata import run.
  """
  autointegrate_pk: bool = True
  reset_flags: bool = False
  datasets: list[SourceMetadataImportDatasetReport] = field(default_factory=list)

  def add_dataset(self, dataset_report: SourceMetadataImportDatasetReport) -> None:
    """Add a dataset-level import outcome."""
    self.datasets.append(dataset_report)

  def add_skipped_dataset(
    self,
    *,
    dataset_key: str,
    source_system: str,
    source_type: str,
    message: str,
  ) -> None:
    """Add a skipped dataset outcome."""
    dataset_report = SourceMetadataImportDatasetReport(
      dataset_key=dataset_key,
      source_system=source_system,
      source_type=source_type,
      status="skipped",
    )
    dataset_report.add_note(
      severity="warning",
      code="dataset_skipped",
      message=message,
    )
    self.add_dataset(dataset_report)

  @property
  def imported_datasets(self) -> list[SourceMetadataImportDatasetReport]:
    """Return dataset reports that completed import processing."""
    return [d for d in self.datasets if d.status == "imported"]

  @property
  def skipped_datasets(self) -> list[SourceMetadataImportDatasetReport]:
    """Return dataset reports that were skipped."""
    return [d for d in self.datasets if d.status == "skipped"]

  @property
  def pk_detected_count(self) -> int:
    """Return the total number of detected primary key columns."""
    return sum(len(set(d.pk_detected)) for d in self.imported_datasets)

  @property
  def needs_review_count(self) -> int:
    """Return the number of datasets with review-relevant notes."""
    return sum(1 for d in self.datasets if d.needs_review)

  def as_dict(self) -> dict[str, Any]:
    """Return a deterministic dictionary representation."""
    return {
      "options": {
        "autointegrate_pk": self.autointegrate_pk,
        "reset_flags": self.reset_flags,
      },
      "summary": {
        "datasets": len(self.imported_datasets),
        "columns_imported": sum(d.columns_imported for d in self.imported_datasets),
        "created": sum(d.created for d in self.imported_datasets),
        "updated": sum(d.updated for d in self.imported_datasets),
        "changed": sum(d.changed for d in self.imported_datasets),
        "unchanged": sum(d.unchanged for d in self.imported_datasets),
        "removed": sum(d.removed for d in self.imported_datasets),
        "skipped_count": len(self.skipped_datasets),
        "pk_detected_count": self.pk_detected_count,
        "needs_review_count": self.needs_review_count,
      },
      "datasets": [d.as_dict() for d in self.datasets],
    }

  def as_result_dict(self) -> dict[str, Any]:
    """
    Return the legacy-compatible import result dictionary plus review report.
    """
    summary = self.as_dict()["summary"]
    skipped = []
    for dataset_report in self.skipped_datasets:
      note = dataset_report.notes[0] if dataset_report.notes else None
      if note and note.message:
        skipped.append(f"{dataset_report.dataset_key} ({note.message})")
      else:
        skipped.append(dataset_report.dataset_key)

    return {
      "datasets": summary["datasets"],
      "columns_imported": summary["columns_imported"],
      "created": summary["created"],
      "updated": summary["updated"],
      "changed": summary["changed"],
      "unchanged": summary["unchanged"],
      "removed": summary["removed"],
      "skipped": skipped,
      "skipped_count": summary["skipped_count"],
      "pk_detected_count": summary["pk_detected_count"],
      "needs_review_count": summary["needs_review_count"],
      "report": self.as_dict(),
    }
