# 🧪 Test Setup & Guidelines

Automated testing ensures the long-term reliability and maintainability of the metadata generation platform.  
This chapter describes the structure, conventions, and execution of all test suites — from database-integrated lineage validation to pure logic modules such as naming, hashing, and validators.

The goal is to keep the test environment pragmatic yet powerful:
- **Realistic database tests** verify dataset and lineage creation using Django’s ORM.
- **Pure logic tests** ensure correctness of naming, hashing, and SQL generation — without any database dependency.
- **Template tests** define the expected behavior of the upcoming SQL Preview pipeline.

Together, these tests form the foundation for confident releases and safe refactoring across the platform.


## Overview

This project uses **pytest** with **pytest-django** for both database-integrated and pure-logic testing.  
All tests are located in `core/tests/` and organized by logical area.

| Layer | Scope | Example file |
|--------|--------|---------------|
| **Database / Metadata** | Validates `TargetDataset`, `TargetDatasetInput`, and lineage integrity. | `test_target_lineage.py` |
| **Generation (Logic)** | Tests for hashing, naming, and validators. | `test_generation_hashing_and_naming.py`, `test_generation_validators.py` |
| **SQL Preview (Templates)** | Prepared templates for preview/renderer logic. Currently skipped until the preview pipeline is wired. | `test_sql_preview_*.py` |
| **Smoke & Import Tests** | Sanity check for Django setup and module imports. | `test_smoke.py` |

---

## Running Tests

From the project root:

```bash
# Run all tests
python runtests.py
```
## Database Tests

Tests that use Django models automatically create a **temporary test database** (e.g. `test_elevata`) and destroy it afterward.  
Your production data is never touched.

Fixtures that use the database are defined in `core/tests/conftest.py`:

- `source_system_sap`  
- `source_dataset_sap_customer`  
- `target_schemas`  
- `raw_stage_rawcore_datasets`

These fixtures provide a realistic `Raw → Stage → Rawcore` dataset chain for lineage validation.

---

## Logic-Only Tests

Logic-based modules (e.g. `hashing`, `naming`, `validators`, `logical_plan`)  
are tested purely in memory — no Django setup or migrations required.

They are **fast**, **deterministic**, and ideal for CI pipelines.

Examples:

- `test_generation_hashing_and_naming.py`  
- `test_generation_validators.py`  
- `test_logical_union.py`

---

## Skipped SQL Preview Tests

Files under `test_sql_preview_*` are **template tests** for the future SQL Preview pipeline.  
They describe expected output patterns but are **currently skipped** until the preview or renderer implementation is complete.

To activate them later:  

1. Implement your SQL preview renderer (e.g. `build_preview_sql()` in `preview.py`).  
2. Update the import paths in the test files.  
3. Remove the `@pytest.mark.skip` decorators.  

Example command once wired:

```bash
python runtests.py
```
## Local and CI Execution

### Local execution

Run all tests using:

```bash
python runtests.py
```
Run a specific file (verbose mode):
```bash
python runtests.py core/tests/test_generation_validators.py -v
```
Run only tests matching a keyword:
```bash
python runtests.py -k "hashing"
```
## CI Integration

If you use GitHub Actions or GitLab CI, your step can simply call the same command:

```yaml
- name: Run tests
  run: |
    pip install -r requirements-dev.txt
    python runtests.py --maxfail=1
```
Optional flags:  
- `--reuse-db` speeds up local test runs  
- `--cov` enables coverage reports (requires `pytest-cov`)

---

## Conventions

- **Indentation:** 2 spaces (consistent with project code style)  
- **Comments:** English, concise, and descriptive  
- **Naming pattern:**  
  - Test files: `test_*.py`  
  - Test functions: `test_*`  
- **Database fixtures:** defined in `core/tests/conftest.py`  
- **Logic-only tests:** may use lightweight dummy classes (no mocks)

---

## Recommended Next Steps

- Add `pytest-cov` for coverage tracking:
  ```bash
  pip install pytest-cov
  python runtests.py --cov=metadata
  ```  

- Integrate pre-commit hooks to ensure consistent formatting.
- Gradually unskip SQL preview tests as the rendering pipeline evolves.

---

© 2025 elevata Labs — Internal Technical Documentation
