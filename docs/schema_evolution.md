# ⚙️ Schema Evolution

Schema evolution in **elevata** is **metadata-driven, deterministic, and lineage-safe**.

Structural changes are never inferred implicitly from generated SQL. Instead, all physical changes are derived explicitly from metadata and applied in a controlled, auditable manner.

This ensures that evolving target schemas remain stable, reproducible, and safe for downstream consumers.

Schema evolution in elevata is not a migration tool and not a best-effort heuristic.

It is a deterministic reconciliation process between metadata and physical warehouse schemas, designed to be safe, lineage-aware, and reproducible across environments.

Architecture reports expose the same schema evolution intent before execution.

Architecture Control makes this intent reviewable, approvable, executable through controlled scopes, and auditable through Architecture Execution Records.

---

## 🔧 Core Principles

- **Metadata is the single source of truth**  
  Physical schemas always converge towards metadata definitions.

- **No implicit inference**  
  elevata never infers schema changes from SQL output or source structures.

- **Deterministic behavior**  
  Given the same metadata state, schema evolution always produces the same result.

- **Lineage-aware safety**  
  Renames and structural changes preserve lineage and avoid duplicate objects.

- **Non-destructive by default**  
  Existing data is never dropped or rewritten implicitly.

---

## 🔧 Type authority

The TargetColumn datatype defined in metadata is authoritative.

Upstream datatypes are treated as advisory input during initial column creation, but do not override explicit datatype changes made at the TargetColumn level.

This allows controlled schema evolution (widening, rebuild, and type alignment) without upstream schema changes forcing unintended reversions.

---

## 🔧 Column and Dataset Renames

Renames are handled explicitly via metadata:

- datasets and columns track historical names via `former_names`
- physical `RENAME TABLE` / `RENAME COLUMN` statements are generated where supported
- duplicate table or column creation is prevented
- ambiguous rename situations are detected and surfaced

This guarantees stable evolution without breaking downstream dependencies.

### 🧩 Dataset Renames

Dataset renames are managed via:

- `TargetDataset.target_dataset_name`  
- `TargetDataset.former_names`

#### 🔎 Behavior
- Physical tables are renamed using `RENAME TABLE`  
- No new table is created  
- No data is copied or lost  
- Lineage remains intact via `lineage_key`

#### 🔎 Guarantees
- Idempotent  
- Safe across multiple consecutive renames  
- Works for base and `_hist` datasets

---

### 🧩 Column Renames

Column renames are managed via:

- `TargetColumn.target_column_name`  
- `TargetColumn.former_names`  

#### 🔎 Behavior
- Schema evolution emits `RENAME COLUMN` (derived from `former_names` and MigrationPlan intent)  
- No duplicate columns are created  
- Former names are preserved for future renames

#### 🔎 Duplicate Detection
If both the desired column name **and** a former name exist physically, the runtime will:

- Emit a warning  
- Skip automatic changes  
- Require manual intervention  

This prevents silent data corruption.

### 🧩 Historization Awareness

Historized datasets (`*_hist`) are treated as **structural mirrors** of their base datasets.

#### 🔎 Guarantees
- Column renames propagate automatically  
- Dataset renames propagate automatically  
- No duplicate history columns are created  
- No accidental base → hist renames occur

This is enforced by:

- Lineage-based dataset lookup  
- Guardrails on `former_names`  
- Deterministic guard logic

### 🧩 Type Drift & Semantic Equivalence

elevata detects type drift by comparing metadata-defined column types with physically introspected types in the target warehouse.

However, certain type differences are treated as **semantically equivalent** and do **not** trigger warnings or schema changes.

Examples include:

- `bool` ↔ `boolean`  
- `int64` ↔ `integer`  
- `timestamp` ↔ `timestamptz` (PostgreSQL)  
- `varchar(n)` ↔ `varchar` (DuckDB)

These equivalence rules are applied during **preflight schema drift detection** and are intentionally **dialect-aware**.

#### 🔎 Design rationale

- safe widening should not create noise  
- vendor-specific type spellings should not create drift churn  
- semantic equivalence reduces false-positive drift warnings

Type equivalence is used for drift classification and noise reduction. Physical DDL is always rendered by the dialect and executed only when schema evolution intent requires it.

---

## 🔧 Architecture Change Reports

Architecture Change Reports describe schema evolution intent before execution.

They are derived from:

```text
Architecture State → Architecture Diff → MigrationPlan → Policy Decisions
```

The report includes:

- dataset and column changes  
- MigrationPlan actions  
- policy decisions for destructive operations  
- deterministic report fingerprint

Reports are review artifacts. They do not apply schema changes by themselves.

`elevata_load` remains responsible for execution preflight and applies schema changes only after guard checks pass.

Architecture Control uses the same schema evolution intent and adds a controlled review and execution layer:

```text
Architecture Change Report
  ↓
Architecture Approval Artifact
  ↓
Execution Preview
  ↓
Controlled Execution
  ↓
Architecture Execution Record
```

Controlled execution does not bypass schema evolution guardrails. It invokes the load runner through constrained Architecture Control scopes and keeps:

- preflight validation  
- materialization policy checks  
- Architecture Guard enforcement  
- approval matching  
- dialect-owned DDL rendering  
- deterministic execution records

Execution scopes are explicit:

| Scope | Schema evolution behavior |
|---|---|
| All datasets | Applies schema evolution for all active target datasets in dependency order |
| Schema | Applies schema evolution for selected schema roots and required dependencies |
| TargetDataset | Applies schema evolution for the selected TargetDataset and required dependencies |
| TargetDataset, target-only | Applies schema evolution only for the selected TargetDataset |

Target-only execution is available only for TargetDataset scopes. It supports focused iteration when upstream data is already available.

Architecture Execution Records capture who executed which controlled schema evolution scope, with which dependency mode, report fingerprint, approval identifier, preview fingerprint, command metadata and result status.

---

## 🔧 Type Drift Detection and Evolution

elevata includes deterministic type drift detection as part of preflight validation.

Type drift occurs when the physical column datatype differs from the datatype defined in metadata.

### 🧩 Canonical Type Comparison

Type comparison is performed using canonical types instead of dialect-specific physical types.

Physical types are mapped to canonical types during introspection.

Example:

| Physical Type (Dialect) | Canonical Type |
|---|---|
| INT (MSSQL) | INTEGER |
| NUMBER(38,0) (Snowflake) | BIGINT |
| BIGINT | BIGINT |
| VARCHAR(100) | STRING |
| TEXT | STRING |

This allows consistent drift detection across different warehouses.

Canonical types represent the logical datatype used by elevata for schema comparison and drift classification.

They are independent of physical database representations.

### 🧩 Drift Classification

Type drift is classified into three categories:

### 🧩 Equivalent
No effective change. Execution continues without action.

Examples:

- INT vs INTEGER  
- VARCHAR vs STRING (dialect alias)

#### 🔎 Widening (Safe)
The target type can safely represent all existing values.

Examples:

- INT → BIGINT  
- VARCHAR(100) → VARCHAR(200)  
- DECIMAL(10,2) → DECIMAL(18,4)

Safe widening changes are automatically remediated.

#### 🔎 Narrowing / Incompatible (Unsafe)
The new type may truncate or invalidate existing data.

Examples:

- BIGINT → INT  
- VARCHAR(200) → VARCHAR(50)  
- DECIMAL(18,4) → DECIMAL(10,2)

Unsafe drift blocks execution deterministically.

---

### 🧩 Evolution Strategy

When widening drift is detected:

1. If the dialect supports `ALTER COLUMN TYPE`, elevata generates an ALTER statement.  
2. Otherwise, elevata performs a deterministic rebuild:

```sql
CREATE TABLE <table>__rebuild_tmp
INSERT INTO <tmp> SELECT CAST(...) FROM <original>
DROP TABLE <original>
RENAME <tmp> → <original>
```

The rebuild strategy guarantees identical results across dialects.

### 🧩 Deterministic Blocking

Execution is blocked when:

- narrowing drift is detected  
- incompatible type change detected  
- dialect cannot safely evolve schema

Blocking occurs during preflight, before any SQL execution.

---

## 🔧 Incremental Pipelines & Schema Evolution

Schema evolution is fully compatible with incremental execution:

- Missing tables are auto-provisioned before MERGE  
- The runtime distinguishes schema creation from table provisioning  
- Incremental MERGE never runs against a non-existent table

This ensures:

- First-run incremental datasets work correctly  
- Renames do not break MERGE semantics

---

## 🔧 Non-Goals (By Design)

The following operations are **explicitly not automated**:

- ❌ Column drops (by default)  
- ❌ Unsafe type narrowing/incompatible changes (unless explicitly allowed)  
- ❌ Constraint changes  
- ❌ Implicit destructive operations

These require:

- Explicit policies  
- Clear user intent  
- Future controlled rollout
- Controlled runtime execution

Unsafe type drift can be explicitly allowed via `ELEVATA_ALLOW_TYPE_ALTER=true` (or `--allow-type-alter`) and is then executed via dialect-supported ALTER or deterministic rebuild.

### 🧩 Policy-gated column drops

Column drops are disabled by default and require explicit configuration:

- Base tables: `ELEVATA_ALLOW_AUTO_DROP_COLUMNS=true` enables physical `DROP COLUMN` when metadata no longer contains a column  
- `_hist` tables: physical drops additionally require `ELEVATA_ALLOW_AUTO_DROP_HIST_COLUMNS=true`  

Without the hist flag, removed business columns in `_hist` are preserved as retired (inactive + detached lineage).

---

## 🔧 Example Workflow

1. Rename column in metadata UI or API  
2. Previous name is added to `former_names`  
3. Schema evolution detects rename  
4. Physical schema is updated safely  
5. `_hist` table is kept in sync automatically  
6. Architecture Control records the controlled execution result

No SQL changes required.

---

## 🔧 Guarantees Summary

| Aspect | Guarantee |
|------|----------|
| Data safety | ✅ No data loss |
| Determinism | ✅ Same metadata → same plan |
| Incremental safety | ✅ MERGE never breaks |
| Cross-dialect | ✅ BigQuery, Databricks, DuckDB, Fabric Warehouse, MSSQL, Postgres, Snowflake |
| Historization | ✅ Always consistent |
| Reviewability | ✅ Schema evolution intent is visible before execution |
| Auditability | ✅ Controlled executions produce Architecture Execution Records |

---

Schema evolution in elevata is designed to be **boring, predictable, and safe** - exactly what you want in production pipelines.

---

© 2025-2026 elevata - Technical Documentation
