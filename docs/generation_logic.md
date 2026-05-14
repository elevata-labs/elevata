# ⚙️ Generation Logic

This document describes how elevata transforms *metadata* into a structured, dialect-neutral **Logical Plan** and then into optimized SQL through the Expression AST and dialect adapters.

---

## 🔧 1. Overview

elevata follows a simple principle:

> **Metadata-in → SQL-out**

The generator inspects datasets, columns, lineage, relationships, and configuration to produce a stable, deterministic Logical Plan. From this plan, SQL is rendered through dialect adapters.

Pipeline:

```
Metadata → Logical Plan → Expression AST → Dialect Rendering → SQL
```

---

## 🔧 2. Dataset Types & Generation Rules

The generation logic depends heavily on the dataset type.

### 🧩 2.1 RAW
- Direct mapping from source fields  
- Column expressions are simple column references  
- No surrogate keys or transformations

### 🧩 2.2 STAGE
STAGE layers unify multiple upstream sources:

#### 🔎 Identity Mode
Used when the upstream provides a `source_identity_id`.  
- No ranking logic  
- Multi-source is handled via `UNION ALL`  
- Each branch injects a literal identity ID

#### 🔎 Non-Identity Mode
Used when multiple upstream sources require conflict resolution.  
- All branches are UNIONed  
- Wrapped into a subquery  
- A `ROW_NUMBER() OVER (...)` window assigns a rank  
- Only rows with rank = 1 are selected

### 🧩 2.3 CORE / BUSINESS
- Surrogate keys are generated from BK columns  
- Foreign keys reference parent surrogate key structure  
- Expression AST builds deterministic hashing expressions

### 🧩 2.4 RAWCORE & HISTORY (HIST)

#### 🔎 RAWCORE:
- historizable layer, source for *_hist.  
- historize=True controls if a _hist-dataset is generated.

#### 🔎 HISTORY (*_hist):
- lives in the same TargetSchema as rawcore.  
- naming: `<rawcore_name>_hist`.  
- generated automatically and fully system-managed.  
- schema structure:  
  - `<rawcore_name>_hist_key` (history SK),  
  - 1:1 copy of rawcore columns (including rawcore SK),  
  - version_started_at, version_ended_at, version_state, load_run_id.  
- linkage:  
  - dataset-level via lineage_key (rename-safe),
  - column-level via TargetColumnInput from rawcore → hist.

#### 🔎 Execution Semantics

History datasets generate fully executable SCD Type 2 SQL.

The historization pipeline is rendered via the active SqlDialect and
includes:

- closing changed versions (UPDATE)  
- closing deleted versions (UPDATE)  
- inserting new changed versions (INSERT..SELECT)  
- inserting first versions for new business keys (INSERT..SELECT)

History SQL is execution-ready.

---

## 🔧 3. Column Expression Generation

Each target column is associated with a **Column Mapping** and an expression. Expressions are built using the **Expression DSL** and then parsed into the Expression AST.  

Example DSL:
```text
HASH256(
  CONCAT_WS('|',
    CONCAT('productid', '~', COALESCE({expr:productid}, 'null_replaced')),
    'pepper'
  )
)
```

The builder never writes SQL directly.

---

## 🔧 4. Business Keys & Surrogate Keys

### 🧩 4.1 Business Keys (BK)
- Defined in metadata per dataset  
- Sorted lexicographically for deterministic ordering  
- Used as inputs to surrogate key expressions

### 🧩 4.2 Surrogate Key Expression
Surrogate keys use a fully dialect-agnostic hashing pattern:  

- Each BK yields a *pair expression*: `CONCAT(name, '~', COALESCE(value, 'null_replaced'))`  
- All pairs joined via `CONCAT_WS('|', ...)`  
- Pepper appended as last component  
- Entire structure wrapped in `HASH256()`  

The resulting Expression AST is rendered differently depending on the dialect.

---

## 🔧 5. Foreign Keys

Foreign keys reuse the exact same hashing structure as the parent surrogate key, but with child column references.

Process:  
1. Inspect parent BK columns  
2. Build pair expressions with child columns  
3. Build ordered AST structure  
4. Wrap in `Hash256Expr`  

This guarantees SK/FK parity across dialects.

### 🧩 Foreign Key Rename Safety

Foreign key columns are system-managed and bound to their originating  
TargetDatasetReference via a stable internal lineage key.

This ensures that:

- renaming a parent or child dataset automatically renames the FK column  
- existing FK columns are reused instead of duplicated  
- multiple references per child dataset are handled safely

Physical FK column renames are emitted via schema evolution (MigrationPlan-driven)  
using `RENAME COLUMN`, preserving data and lineage.

---

## 🔧 6. Expression AST

All expressions use a vendor-neutral AST:  
- `ColumnRef`  
- `Literal`  
- `ExprRef`  
- `ConcatExpr`  
- `ConcatWsExpr`  
- `CoalesceExpr`  
- `WindowFunctionExpr`  
- `Hash256Expr`  

The AST is consumed by the dialect renderer, which decides on actual SQL syntax.

---

## 🔧 7. Logical Plan Construction

Logical Plans represent SQL structures without dialect specifics.  

Main node types:  
- `LogicalSelect`  
- `LogicalUnion`  
- `SubquerySource`  

Examples:

### 🧩 Single-source STAGE
```
LogicalSelect(
  from_=source,
  select_list=[...]
)
```

### 🧩 Multi-source STAGE (identity mode)
```
LogicalSelect(
  from_=LogicalUnion([branch1, branch2])
)
```

### 🧩 Multi-source STAGE (non-identity mode)
```
LogicalSelect(
  from_=SubquerySource(
    select=LogicalSelect(
      from_=LogicalUnion(...),
      select_list=[..., WindowFunctionExpr(...)]
    ),
    alias="ranked"
  ),
  where=rank == 1
)
```

---

## 🔧 8. Dialect Rendering

After Logical Plan + AST construction, SQL is produced by:

```
dialect.render_select(logical_select)
```

Dialect responsibilities:  
- identifier quoting  
- literal rendering  
- hashing syntax  
- function names  
- window functions  
- formatting  

The Logical Plan and AST guarantee correctness; the dialect guarantees syntactic validity.

---

## 🔧 8.5 Schema Evolution & Renames (MigrationPlan)

elevata supports safe schema evolution driven by metadata:  

- `TargetDataset.former_names` tracks previous physical table names  
  → schema evolution emits `RENAME TABLE` when the new table name is missing but a former name exists.

- `TargetColumn.former_names` tracks previous physical column names  
  → schema evolution emits `RENAME COLUMN` when the desired column is missing but a former name exists.

- For historization tables (`*_hist`), schema sync is derived from the corresponding base dataset.  
  Column renames are therefore expected to be reflected in the hist metadata as well,  
  so schema evolution can rename instead of adding duplicate columns.

Schema evolution never provisions missing tables. Provisioning is handled by the load runner  
via `ensure_target_table(...)` before executing DML.

---

## 🔧 10. Deterministic Generation

elevata enforces determinism:  
- Sorted BK pairs  
- Stable column ordering  
- Consistent naming of technical fields (e.g. `__src_rank_ord`)  
- Identical AST for SK/FK  

This ensures:  
- reproducible SQL  
- stable diffs  
- predictable behavior across dialects

---

## 🔧 11. Bizcore Generation Semantics

Bizcore datasets follow the **same generation pipeline**
as Raw, Stage, Core, and Serving datasets.

There is no special-case SQL generation for Bizcore.

Key properties:

- Bizcore datasets are planned via the same Logical Plan builder  
- Expressions, joins, and calculations are compiled identically  
- Execution semantics (ordering, retries, blocking) are unchanged  
- Lineage is preserved across Core → Bizcore boundaries

This ensures that business semantics are:

- deterministic  
- testable  
- explainable  
- warehouse-native

Bizcore is therefore a **semantic layer by metadata**, not by execution logic.

---

## 🔧 11. Summary

The generation logic is the heart of elevata:  
- metadata describes the transformation  
- Logical Plan formalizes the operation  
- Expression AST encodes column semantics  
- Dialect renders valid SQL  
- Deterministic generation of Historization datasets (no manual maintenance)  

This architecture supports multiple SQL backends without changing metadata or Logical Plans.

---

## 🔧 12. Default Generation vs Custom Query Logic

elevata distinguishes between two generation modes:

### 🧩 Default Generation
SQL is derived automatically from:

- dataset inputs and joins  
- column definitions and expressions  
- lineage and metadata rules

This mode requires no explicit query definition and is the default.

### 🧩 Custom Query Logic (Query Tree)
In semantic layers (`bizcore`, `serving`), a dataset may define an explicit
Query Tree.

If a Query Tree is present:

- it defines the query structure  
- output columns are inferred from the tree  
- the resulting plan is rendered via the same Logical Plan and dialect system

If no Query Tree is present, elevata always falls back to default generation.

This opt-in model prevents accidental complexity while enabling
advanced, deterministic transformations where needed.

---

© 2025-2026 elevata Labs — Internal Technical Documentation
