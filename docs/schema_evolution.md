# ⚙️ Schema Evolution

Schema evolution in **elevata** is **metadata-driven, deterministic, and lineage-safe**.  

Structural changes are never inferred implicitly from generated SQL.  
Instead, all physical changes are derived explicitly from metadata and  
applied in a controlled, auditable manner.

This ensures that evolving target schemas remain stable, reproducible,  
and safe for downstream consumers.

Schema evolution in elevata is not a migration tool and not a best-effort heuristic.

It is a deterministic reconciliation process between metadata and physical warehouse schemas,  
designed to be safe, lineage-aware, and reproducible across environments.

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
- Planner emits `RENAME COLUMN`  
- No duplicate columns are created  
- Former names are preserved for future renames

#### 🔎 Duplicate Detection
If both the desired column name **and** a former name exist physically,  
the planner will:

- Emit a warning  
- Skip automatic changes  
- Require manual intervention  

This prevents silent data corruption.

---

### 🧩 Historization Awareness

Historized datasets (`*_hist`) are treated as **structural mirrors** of their
base datasets.

#### 🔎 Guarantees
- Column renames propagate automatically  
- Dataset renames propagate automatically  
- No duplicate history columns are created  
- No accidental base → hist renames occur

This is enforced by:  
- Lineage-based dataset lookup  
- Guardrails on `former_names`  
- Defensive planner logic

---

## 🔧 Incremental Pipelines & Schema Evolution

Schema evolution is fully compatible with incremental execution:

- Missing tables are auto-provisioned before MERGE  
- Planner distinguishes schema creation from table provisioning  
- Incremental MERGE never runs against a non-existent table

This ensures:  
- First-run incremental datasets work correctly  
- Renames do not break MERGE semantics

---

## 🔧 Non-Goals (By Design)

The following operations are **explicitly not automated**:

- ❌ Column drops  
- ❌ Type changes  
- ❌ Constraint changes  
- ❌ Implicit destructive operations

These require:  
- Explicit policies  
- Clear user intent  
- Future controlled rollout

---

## 🔧 Example Workflow

1. Rename column in metadata UI or API  
2. Previous name is added to `former_names`  
3. Materialization planner detects rename  
4. Physical schema is updated safely  
5. `_hist` table is kept in sync automatically

No SQL changes required.

---

## 🔧 Guarantees Summary

| Aspect | Guarantee |
|------|----------|
| Data safety | ✅ No data loss |
| Determinism | ✅ Same metadata → same plan |
| Incremental safety | ✅ MERGE never breaks |
| Cross-dialect | ✅ DuckDB, Postgres, MSSQL, BigQuery |
| Historization | ✅ Always consistent |

---

Schema evolution in elevata is designed to be **boring, predictable, and safe** —  
exactly what you want in production pipelines.

---

© 2025 elevata Labs — Internal Technical Documentation

