# Metadata Health Check

The **Metadata Health Check** is a central quality assurance mechanism in *elevata*.  
It automatically inspects the entire metadata repository and detects:

- inconsistent incremental configuration  
- conflicting materialization rules  
- missing BizCore semantics  
- technical misconfigurations (e.g., merge on a view)  
- dependencies that do not logically fit together  
- incomplete incremental setups  
- incorrect handling of deletes

The Health Check runs **purely on metadata** and **does not require an active database**.

---

## Goals

The Health Check is designed to:

- detect configuration issues early  
- protect SQL generation from producing invalid code  
- keep BizCore models semantically correct  
- provide safety for CI/CD pipelines  
- give developers and analysts clear and actionable feedback  

---

## Running the Health Check

You can run the check manually using the Django management command:

```bash
python manage.py check_metadata_health
```

The output is a JSON-like structure mapping each affected TargetDataset.id
to a list of detected issues:

```python
  Total target datasets: 14
  Datasets with issues: 2

  [42] rc_aw_salesorderheader (schema=rawcore) 
    - incremental_strategy='merge' but effective materialization_type='view' (expected 'table').
    - handle_deletes=True but no matching logical delete key found.
    -> Health level: warning

  [87] bc_dim_customer (schema=bizcore)
    - BizCore role='dimension' but no business key column defined.
    -> Health level: warning

  Metadata health check found issues.
```

If no issues exist:

```python
  All target datasets look healthy. 🎉
```
## What is Checked?

### 1. Incremental Configuration
The Health Check validates that:

- `incremental_strategy` fits the effective materialization  
  - e.g., `merge` is only valid when the materialization is `table`
- delete handling (`handle_deletes`) is configured correctly  
- required incremental keys exist
- no incremental settings appear in serving/view layers

---

### 2. Materialization Consistency

- `merge` cannot be used on views  
- serving layer datasets are never materialized as tables  
- BizCore datasets do not accidentally inherit staging/raw materializations  
- inconsistent configurations between upstream and downstream datasets are detected

---

### 3. BizCore Semantics

The following rules are checked:

- `biz_entity_role` (e.g. `core_entity`, `dimension`, `fact`, `reference`) is used consistently  
- core entities must define a stable business grain  
- dimensions should contain at least one identifier/business key  
- enrichment datasets do not break expected lineage semantics  
- BizCore models remain logically consistent across the domain

---

### 4. Dependency Integrity

Ensures:

- a dataset only inherits incremental behavior when its upstream supports it  
- no conflicting materializations exist in the lineage chain  
- no circular dependencies exist  
- upstream datasets required for incremental logic are present and active

---

## Where the Health Check Appears in the UI

- Each **TargetDataset → Lineage** page displays a highlighted warning box  
- A badge summarises the health state (`OK`, `WARN`, `ERROR`)  
- Users can jump back to details directly from the lineage page

Example:

> **Metadata Health Check:**
> - Incremental: incremental_strategy='merge' but effective materialization_type='view' (expected 'table').
> - Materialization: Effective materialization_type='view' differs from schema default 'table' for schema  'rawcore'.  

---

## When to Use It

Run the Health Check:

- after onboarding new source datasets  
- before committing metadata changes  
- before triggering SQL generation or deployment  
- inside CI/CD pipelines  
- during regular metadata governance reviews

---

## Extensibility

The Health Check framework is modular.  
Additional rule sets can be added easily, for example:

- naming convention enforcement  
- documentation completeness  
- missing PII classifications  
- key constraint validation  
- SQL semantic linting  
- unused or dead attributes

Validators live in: `metadata/generation/validators.py` and follow a simple reusable pattern.

---

## Summary

The Metadata Health Check provides:

- early detection of misconfigurations  
- prevention of invalid SQL generation  
- semantic governance of BizCore models  
- platform-wide consistency  
- metadata-driven quality assurance  

Running the Health Check regularly helps maintain a stable, predictable, and well-structured Data & Analytics platform.

---

© 2025 elevata Labs — Internal Technical Documentation