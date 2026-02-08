# ⚙️ elevata Architecture Overview

> A high-level view of how elevata transforms metadata into executable SQL — from ingestion to lineage,
> from logical plans to dialect-aware rendering.

This overview connects the core concepts behind **Generation Logic**, **Incremental Load**, **Load SQL Architecture**, **Lineage & Logical Plan**, and the **Dialect System** into one visual narrative.

---

## 🔧 1. Core Architecture at a Glance

```text
Source Metadata (DB reflection, APIs)
  ↓
Metadata Model (Datasets, Columns, Lineage)
  ↓
Generation Logic (TargetDataset & Columns)
  ↓
Lineage Model (Dataset + Column Lineage)
  ↓
Logical Plan Builder (Structured Query Representation)
  ↓
SQL Renderer (Deterministic SQL Formatting)
  ↓
get_active_dialect() (Dialect Adapter)
  ↓
Load SQL (Full · Merge · Delete Detection)
  ↓
Target Warehouse (Raw · Stage · Rawcore)
  ↓
Materialization Planner (Schema Drift Sync)
  ↓
DDL Applier (safe DDL only)
```

This flow represents the central principle of elevata:

> **Metadata → Logical Plan → Dialect-aware SQL → Warehouse**

---

## 🔧 2. Architecture Layers

### 🧩 2.1 Metadata Ingestion Layer
- Reads schema, columns, keys from source systems  
- Normalizes metadata into elevata’s internal models  
- No SQL generation occurs here

### 🧩 2.2 Generation Layer
- Creates TargetDatasets in Raw, Stage, Rawcore  
- Injects surrogate keys where required  
- Produces column mappings based entirely on lineage  

Incremental scoping and ingestion behavior are derived from SourceDataset metadata and consistently  
applied across ingestion, merge, and delete detection.  

> *Raw datasets may be ingested via native ingestion or skipped entirely in federated setups.*  

### 🧩 2.3 Lineage Layer
- Establishes dataset-level and column-level lineage  
- Feeds the Logical Plan Builder  
- Ensures traceability from source to Rawcore

### 🧩 2.4 Logical Plan Layer
- Builds structured plans (not SQL!)  
- Vendor-neutral representation of SELECT, JOIN, UNION logic  
- Used by Raw → Stage → Rawcore previews and loads

### 🧩 2.5 SQL Rendering Layer
- Applies formatting rules (indentation, aliasing, column order)  
- Hands off dialect-specific tasks to the dialect adapter  
- Deterministic output for UI and CI

### 🧩 2.6 Dialect Adapter Layer
- Implements quoting, merge syntax, hashing, concatenation  
- Ensures SQL runs identically across platforms (BigQuery, Databricks, DuckDB, Fabric Warehouse, MSSQL, Postgres, Snowflake)

### 🧩 2.7 Load SQL Layer
- Full load: INSERT INTO ... SELECT  
- Incremental merge: upsert logic based on natural key lineage  
- Delete detection: anti-join removal of missing rows  

### 🧩 2.7.1 Materialization & Schema Drift (Planner + Applier)
Before executing load SQL, elevata runs a **materialization planner** to safely reconcile  
physical target tables with metadata-defined schemas:  

- **Dataset renames** are detected via `TargetDataset.former_names → RENAME TABLE`  
- **Column renames** are detected via `TargetColumn.former_names → RENAME COLUMN`  
- Missing columns can be added (`ADD COLUMN`) when the dialect can render it  
- Drops are disabled by default (policy-gated)  

Important design principle:  
The planner does not create tables. Table provisioning is handled centrally by the load runner  
(`ensure_target_table`) and executed via the target execution engine.  

With these technical layers in place, elevata enables a clear transition from data engineering  
to business-facing data products.

Schema drift detection includes dialect-aware semantic equivalence rules to suppress non-actionable type differences.

## 🔧 3. Bizcore — Business Semantics as Metadata

elevata introduces a dedicated **Bizcore layer** for modeling business meaning,
rules, and calculations as **first-class metadata**.

Bizcore sits explicitly between **Core** and **Serving**:

**RAW → STAGE → CORE → BIZCORE → SERVING**

### 🧩 What Bizcore is
- A **business semantics layer**, not a technical projection  
- Explicitly modeled datasets and columns  
- Deterministically executed like all other datasets  
- Fully lineage-aware and explainable

Bizcore datasets express:

- business concepts (e.g. Customer, Contract, Revenue)  
- business rules and classifications  
- derived business identifiers  
- KPIs and domain logic as dataset fields

### 🧩 What Bizcore is *not*
- No BI semantic layer  
- No metric store  
- No query-time metric resolution  
- No tool-specific abstraction

Bizcore logic is compiled into the same logical plans and SQL as technical datasets,  
preserving elevata’s guarantees around **determinism, transparency, and reproducibility**.

### 🧩 Serving — Presentation Logic & Consumer Hand-off  
Serving is the **presentation-facing** layer. Serving datasets typically expose Bizcore datasets 1:1  
(often as views), while allowing **consumer-specific shaping** such as naming, ordering, and lightweight joins  
where required. Serving is intended as the **hand-off layer to BI tools / semantic layers / frontend use cases** —  
without moving business logic out of Bizcore.

### 🧩 Custom Query Logic (Query Tree)

For most datasets, elevata generates SQL automatically from metadata.  
In semantic layers (`bizcore`, `serving`), elevata additionally supports
**Custom Query Logic** via an explicit **Query Tree**.

The Query Tree defines the *shape* of a query (e.g. windowing, aggregation steps, union composition)  
while remaining fully metadata-native.

If enabled, the Query Tree is compiled into the same Logical Plan and Expression AST  
used by the default generation pipeline.  
If disabled, elevata falls back to fully automatic SQL generation.

This ensures advanced query shaping without introducing manual SQL or breaking determinism,  
lineage, or governance guarantees.

---

## 🔧 4. Incremental Processing Path

```text
Stage Dataset
  ↓  (Lineage Mapping)
Merge SQL
  ↓
Delete Detection
  ↓
Rawcore Dataset
```

**These two strategies are currently implemented:**  
- `full`  
- `merge`  

Both operate exclusively between **Stage → Rawcore**.

---

## 🔧 5. Dialect Resolution Overview

```text
ELEVATA_SQL_DIALECT env var  →  Dialect Adapter (override)
Active Profile (elevata_profiles.yaml)  →  Dialect Adapter
DuckDBDialect (fallback)  →  Dialect Adapter
```

The resolution order is:  
1. Environment override  
2. Profile definition  
3. DuckDB fallback

---

## 🔧 6. Unified SQL Generation Pipeline

```text
Metadata Model
  → Logical Plan Builder
  → SQL Renderer
  → Dialect Adapter
  → Load SQL (full, merge, delete)
```

---

## 🔧 7. Why This Architecture Matters
- **Vendor neutrality** via dialect adapters  
- **Determinism** via SQL rendering rules  
- **Traceability** via lineage-driven logic  
- **Extensibility** (new dialects, strategies, materializations)  
- **Incremental ready** with merge + delete detection  
- **Safe for CI/CD** — predictable SQL for diffing and testing  
- **Execution & Logging** are part of the system

---

## 🔧 8. Related Documents
- [Generation Logic](generation_logic.md)  
- [Incremental Load Architecture](incremental_load.md)  
- [Load SQL Architecture](load_sql_architecture.md)  
- [Lineage Model & Logical Plan](logical_plan.md)  
- [Dialect System](dialect_system.md)

---

© 2025-2026 elevata Labs — Internal Technical Documentation
