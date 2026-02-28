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

import pytest

from metadata.ingestion import file_import


class _Atomic:
  """Minimal context manager for transaction.atomic()."""
  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc, tb):
    return False


class _FakeSourceColumnsManager:
  def __init__(self):
    self._items = []

  def all(self):
    return list(self._items)

  def update(self, **kwargs):
    # Called only when reset_flags=True; keep for completeness.
    for c in self._items:
      for k, v in kwargs.items():
        setattr(c, k, v)


class _FakeSystem:
  def __init__(self, short_name):
    self.short_name = short_name


class _FakeDataset:
  def __init__(self):
    self.source_system = _FakeSystem("csv")
    self.source_dataset_name = "products"
    self.ingestion_config = {"uri": "file:///C:/dummy/products.csv"}
    self.source_columns = _FakeSourceColumnsManager()


class _FakeQS:
  def __init__(self, store, pks):
    self._store = store
    self._pks = set(pks)

  def delete(self):
    self._store[:] = [c for c in self._store if getattr(c, "pk", None) not in self._pks]


class _FakeObjects:
  def __init__(self, store):
    self._store = store

  def filter(self, **kwargs):
    pks = kwargs.get("pk__in") or []
    return _FakeQS(self._store, pks)


class _FakeSourceColumn:
  """In-memory fake for metadata.models.SourceColumn."""
  _pk_seq = 1
  objects = None

  def __init__(self, source_dataset, source_column_name, integrate=True, pii_level="none"):
    self.source_dataset = source_dataset
    self.source_column_name = source_column_name
    self.integrate = integrate
    self.pii_level = pii_level

    # Fields set by import_file_metadata_for_dataset
    self.ordinal_position = None
    self.source_datatype_raw = None
    self.datatype = None
    self.max_length = None
    self.decimal_precision = None
    self.decimal_scale = None
    self.nullable = None
    self.primary_key_column = None
    self.referenced_source_dataset_name = None
    self.json_path = None

    self.pk = None

  def save(self, update_fields=None):
    # Assign pk on first save and add to dataset store.
    if self.pk is None:
      self.pk = _FakeSourceColumn._pk_seq
      _FakeSourceColumn._pk_seq += 1
      self.source_dataset.source_columns._items.append(self)


def test_csv_auto_import_normalizes_headers_and_sets_lowercase_json_path(monkeypatch):
  ds = _FakeDataset()

  # Provide a CSV sample with "human" headers (casing + spaces)
  monkeypatch.setattr(
    file_import,
    "_sample_csv",
    lambda uri, **kwargs: [
      {"Internal ID": 56, "Brand": "Acme"},
      {"Internal ID": 57, "Brand": "Other"},
    ],
  )

  # Avoid DB transaction dependency
  monkeypatch.setattr(file_import.transaction, "atomic", lambda: _Atomic())

  # Make type inference deterministic and irrelevant for this test
  monkeypatch.setattr(file_import, "infer_column_profile", lambda values: ("STRING", None, None, None))
  monkeypatch.setattr(file_import, "infer_pk_columns", lambda rows, col_names: [])

  # Patch SourceColumn model to in-memory fake
  store = ds.source_columns._items
  _FakeSourceColumn.objects = _FakeObjects(store)
  monkeypatch.setattr(file_import, "SourceColumn", _FakeSourceColumn)

  res = file_import.import_file_metadata_for_dataset(ds, file_type="csv")

  # Assert normalized columns exist
  cols = {c.source_column_name: c for c in ds.source_columns.all()}
  assert "internal_id" in cols
  assert "brand" in cols

  # Assert lowercase json_path (this is the bug we fixed)
  assert cols["internal_id"].json_path == "$.internal_id"
  assert cols["brand"].json_path == "$.brand"

  assert res["columns_imported"] >= 2