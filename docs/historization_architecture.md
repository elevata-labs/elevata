# ⚙️ Historization Architecture (SCD Type 2)

This document describes the architecture, semantics, and SQL generation logic  
used by *elevata* to maintain history (“_hist”) tables for any Rawcore dataset.  
It covers the full lifecycle: new, changed, and deleted business keys, and how  
incremental historization SQL is generated in a dialect-agnostic way.

---

## 🔧 1. Purpose & Scope

Rawcore tables represent **the latest snapshot** of a domain dataset.  

History (“_hist”) tables represent the **temporal evolution** of that dataset  
based on SCD Type 2 semantics:

- Every Business Key (BK) maintains a timeline of versions.  
- Every version is valid for a continuous time interval.  
- Changes in attribute values create new versions.  
- Disappearing business keys generate deleted versions.  

Historization is fully metadata-driven and uses the same logical plan as the  
Rawcore load.

### 🧩 1.1 Placeholder Standardisation

All runtime placeholders must use **double-brace notation** and are resolved during execution:  

- `{{ load_timestamp }}`  
- `{{ load_run_id }}`  


### 🧩 1.2 Execution Semantics

**History tables are never truncated.**

The SCD Type 2 pipeline operates exclusively via:  
- closing existing versions (`version_ended_at`)  
- inserting new versions  

At no point is historical data removed or replaced.

---

## 🔧 2. History Table Structure

Every History table contains:

### 🧩 2.1 Surrogate Keys

- `<rawcorename>_hist_key` (history surrogate key, deterministic hash key)  
- `<rawcorename>_key` (foreign key to the Rawcore surrogate key)

### 🧩 2.2 Business Attributes

Inherited from Rawcore via lineage:  

- all non-surrogate, non-technical Rawcore columns  
- column types and expressions are identical to the Rawcore schema

### 🧩 2.3 SCD Technical Columns

| Column               | Type        | Meaning                                         |
|----------------------|-------------|--------------------------------------------------|
| `version_started_at` | timestamp   | Start of validity interval                       |
| `version_ended_at`   | timestamp   | End of validity; NULL for open versions          |
| `version_state`      | text        | `new`, `changed`, `deleted`                      |
| `load_run_id`        | string      | ID of the load that produced this version        |

`version_ended_at IS NULL` uniquely identifies the **current active version**. 

---

## 🔧 3. Schema Sync, Drift & Renames

History tables are an exact schema mirror of their Rawcore base table (plus SCD technical fields).  
To avoid accidental duplication when columns are renamed, elevata relies on 

### 🧩 Metadata-driven rename tracking:

- Base dataset column rename updates `TargetColumn.former_names`  
- Hist dataset metadata must retain corresponding former names as well  
  so that materialization planning can emit `RENAME COLUMN` instead of `ADD COLUMN`.

### 🧩 Orphan preservation (schema drift)

If a business column disappears from Rawcore, elevata does not drop it from `_hist`.  
Instead, it is preserved as an **inactive column without upstream inputs**, so that:

- historical data remains analyzable  
- old reports continue to work  
- downstream schemas do not silently change

Rename cases are **not** treated as orphans.  
They are detected via existing `TargetColumnInput` lineage links and therefore
migrated as proper renames, not preserved under the old name.

When a FK reference is deleted, elevata detaches dependent `_hist` inputs instead of dropping columns,  
so historized schemas remain analyzable and stable.

### 🧩 Policy: Never drop business columns in `_hist`

`_hist` tables are treated as long-lived audit/history artifacts.  
Therefore, elevata applies a strict policy:

- **Business columns in `_hist` are never physically dropped.**  
- If a business column disappears from Rawcore (e.g., source stopped delivering it, FK reference deleted),  
  the corresponding `_hist` column is:  
  - marked **inactive** (`active=false`, `retired_at` set)  
  - detached from lineage (**all `TargetColumnInput` links removed**)

Technical SCD columns (`version_*`, `load_run_id`, `loaded_at`) remain generator-managed and may be rebuilt,  
but business-history stays analyzable and stable over time.

### 🧩 Operational semantics: retire vs. physical drop

elevata distinguishes between:

- **Semantic removal (retire)**: a column is removed from the architecture contract, but may remain  
  physically present as a legacy/archive field (especially in `_hist`).  
- **Physical drop**: a destructive DDL operation that removes the column from the table schema.

By default, business columns in `_hist` are **retired**, not physically dropped, to preserve history.

### 🧩 Destructive schema operations (explicit opt-in)

Physical drops are intentionally gated behind environment flags:

- `ELEVATA_ALLOW_AUTO_DROP_COLUMNS` (default: `false`)  
  - If enabled, elevata may emit/execute `DROP COLUMN` for **base tables** when metadata no longer  
    contains the column (auto-cleanup of drift / manual mistakes).

- `ELEVATA_ALLOW_AUTO_DROP_HIST_COLUMNS` (default: `false`)  
  - If enabled *in addition*, elevata may also physically drop business columns from `_hist`.  
  - This is destructive and should only be used for explicit cleanup scenarios.

Note: `_hist` physical drops are **disabled by default** even when base auto-drop is enabled.

### 🧩 Guardrails:
- Hist datasets must only rename from hist-like former names (`*_hist`)  
  to prevent accidental base → hist table renames.

---

## 🔧 4. Historization Workflow (Incremental, SCD2)

Historization runs **after** the Rawcore Merge Load.  

The historization pipeline consists of **four steps**, executed in order:  

1. **Changed-UPDATE**  
2. **Delete-UPDATE**  
3. **Changed-INSERT**  
4. **New-INSERT**  

Each step uses surrogate keys and row hashes to detect which BKs require action.  

Worked example:

```
Rawcore snapshot:
  - BK exists and attributes changed → new version
  - BK no longer exists              → deleted version
  - BK completely new                → new version

History table:
  updates old versions, inserts new ones accordingly
```

---

## 🔧 5. Step 1: Version Detection (UPDATE)

Change detection is based on a deterministic `row_hash` computed in Rawcore.  
The hash covers all non-key, non-technical attributes and is reused unchanged  
by the historization pipeline to ensure consistent and dialect-independent  
change detection.  

A version is considered *changed* if:  

- its Business Key exists in Rawcore, **but**  
- its `row_hash` differs from the current Rawcore row.  

SQL shape:

```sql
UPDATE rawcore.rc_aw_product_hist AS h
SET
  version_ended_at = {{ load_timestamp }},
  version_state    = 'changed',
  load_run_id      = {{ load_run_id }}
WHERE h.version_ended_at IS NULL
  AND EXISTS (
    SELECT 1
    FROM rawcore.rc_aw_product AS r
    WHERE r.rc_aw_product_key = h.rc_aw_product_key
      AND r.row_hash <> h.row_hash
  );
```

---

## 🔧 6. Step 2: Delete Detection (UPDATE)

A version is considered *deleted* if:  

- its BK **does not appear** in the Rawcore snapshot.  

SQL shape:

```sql
UPDATE rawcore.rc_aw_product_hist AS h
SET
  version_ended_at = {{ load_timestamp }},
  version_state    = 'deleted',
  load_run_id      = {{ load_run_id }}
WHERE h.version_ended_at IS NULL
  AND NOT EXISTS (
    SELECT 1
    FROM rawcore.rc_aw_product AS r
    WHERE r.rc_aw_product_key = h.rc_aw_product_key
  );
```

No hard-deletes occur – only closing open versions.

---

## 🔧 7. Step 3: Changed Version Insert (INSERT)

After Step 1 closed old changed versions, we must create **new** versions for  
each changed BK.  

SQL shape:

```sql
INSERT INTO rawcore.<hist_table> (<hist_cols...>)
SELECT
  r.<rawcorename>_key,
  <attribute columns>,
  r.row_hash,
  {{ load_timestamp }},
  NULL,
  'changed',
  {{ load_run_id }}
FROM rawcore.<rawcore_table> AS r
WHERE EXISTS (
  SELECT 1
  FROM rawcore.<hist_table> AS h
  WHERE h.version_ended_at = {{ load_timestamp }}
    AND h.version_state = 'changed'
    AND h.<rawcorename>_key = r.<rawcorename>_key
);
```

---

## 🔧 8. Step 4: New Version Insert (INSERT)

A version is considered *new* if:  

- its BK exists in Rawcore,  
- but **no** history entry for this BK exists at all.  

SQL shape:

```sql
INSERT INTO rawcore.<hist_table> (<hist_cols...>)
SELECT
  r.<rawcorename>_key,
  <attribute columns>,
  r.row_hash,
  {{ load_timestamp }},
  NULL,
  'new',
  {{ load_run_id }}
FROM rawcore.<rawcore_table> AS r
WHERE NOT EXISTS (
  SELECT 1
  FROM rawcore.<hist_table> AS h
  WHERE h.<rawcorename>_key = r.<rawcorename>_key
);
```

---

## 🔧 9. Ordering Guarantees

Historization is always executed as a downstream step of the Rawcore load  
and relies on the Rawcore snapshot being fully materialized for the same  
load timestamp and load run ID.  

The SCD2 pipeline must always run in the following order:  

1. Close changed versions  
2. Close deleted versions  
3. Insert new changed versions  
4. Insert new versions  

This ensures:  

- correct temporal consistency  
- no overlapping validity periods  
- all new rows receive a correct SCD state  
- deletes never override changed states  

---

## 🔧 10. Expression Reuse

All attribute expressions come from the **Rawcore logical select plan**.  
This guarantees:  

- identical typing  
- identical coercions  
- identical cast rules  
- identical derived expressions (hashing, concatenations, etc.)  

History never reimplements logic — it inherits all transformations from Rawcore.

---

## 🔧 11. Dialect Abstraction

All historization SQL is generated via the same dialect interface used  
throughout the load engine:  

- identifier rendering  
- table rendering  
- quoting rules  

History SQL is therefore **dialect-safe** and works across:  

- DuckDB  
- PostgreSQL  
- SQL Server  
- any custom dialect with the same contract  

No dialect requires MERGE for historization.

---

## 🔧 12. Interaction with Rawcore Merge Load

Historization relies on Rawcore being loaded with a consistent **incremental  
merge** before it runs.  

Historization consumes:  

- surrogate keys  
- row hashes  
- attribute columns  
- business key lineage  
- load timestamp & load run ID  

and produces a clean SCD timeline for all BKs.

---

## 🔧 13. Guarantees & Invariants

The historization layer guarantees:  

- exactly one open version per BK (or none after deletion)  
- no overlapping date ranges  
- strict sequential versioning  
- deterministic change detection via row_hash  
- idempotent reruns of the same load_run_id  
- append-only history (no data loss)

---

## 🔧 14. Testability

The SQL generation is fully covered via:  

- unit tests for UPDATE and INSERT blocks  
- integration test verifying the combined pipeline  
- monkeypatched lineage & expression maps for deterministic SQL  
- surrogate key joins validated in both EXISTS and NOT EXISTS branches  

Historization never relies on ORM runtime behavior.

---

## 🔧 15. Optional Extensions (Out of Scope)

Optional extensions that can be built later:  

- effective dating vs. load dating  
- metadata-driven state labels  
- soft-deleted Rawcore entries  
- point-in-time reconstruction helpers  
- history pruning / archiving modules  

---

## 🔧 16. Summary

Historization is a complete, metadata-driven SCD Type 2 engine:  

- fully generated SQL  
- lineage-consistent attribute mapping

---

© 2025-2026 elevata Labs — Internal Technical Documentation