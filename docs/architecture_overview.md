# ⚙️ elevata Architecture Overview

> A high-level view of how elevata transforms metadata into executable SQL - from ingestion to lineage,
> from logical plans to dialect-aware rendering.

This overview connects the core concepts behind **Generation Logic**, **Incremental Load**, **Load SQL Architecture**, **Lineage & Logical Plan**, and the **Dialect System** into one visual narrative.

---

## 🔧 1. Core Architecture at a Glance

```text
Source Metadata (DB reflection, APIs)
  ↓
Metadata Model (Datasets, Columns, Lineage)
  ↓
Source Ingestion Readiness (Landing, Ingestion, RAW Handoff)
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
Schema Evolution (MigrationPlan)
  ↓
DDL Applier (safe DDL only)
```

This flow represents the central principle of elevata:

> **Metadata → Logical Plan → Dialect-aware SQL → Warehouse**

Architecture Control provides review, approval, controlled execution, and audit artifacts around the same architecture state:

```text 
Architecture State
  ↓
Architecture Diff
  ↓
MigrationPlan
  ↓
Policy Decisions
  ↓
Architecture Change Report
  ↓
Architecture Review Briefing
  ↓
Architecture Approval Artifact
  ↓
Execution Preview
  ↓
Controlled Execution
  ↓
Architecture Execution Record
```

---

## 🔧 2. Architecture Layers

### 🧩 2.1 Metadata Ingestion Layer
- Reads schema, columns, keys from source systems  
- Normalizes metadata into elevata’s internal models  
- Reports created, changed, unchanged and removed source metadata outcomes  
- No SQL generation occurs here


#### 🔎 2.1.1 Source Metadata Import Review

Source Metadata Import Review makes source onboarding inspectable immediately after import.

It reports what elevata discovered and how the SourceColumn metadata changed:

- created columns  
- changed columns  
- unchanged columns  
- removed columns  
- detected primary key columns  
- skipped datasets  
- datasets that need manual review

Changed and unchanged are intentionally separated. A changed column means the stored technical source metadata now differs from the previous state. An unchanged column means the source was checked and still matches the previous metadata state.

The review result is transient and read-only as a report. It does not persist import history, introduce a new workflow, execute loads, or generate target architecture. It only makes the existing metadata import outcome transparent before downstream generation and control steps.

#### 🔎 2.1.2 Source Ingestion Readiness

Source Ingestion Readiness evaluates whether a SourceDataset is coherently prepared for its modeled architecture handoff.

It combines:

- Source System and SourceDataset lifecycle metadata  
- integration scope  
- RAW landing intent  
- native, external or no-ingestion mode  
- integrated SourceColumns  
- active RAW TargetDataset links  
- file, REST and relational configuration contracts  
- incremental filter and policy alignment

The result is deterministic and read-only. It does not connect to sources, resolve secrets, import metadata, read files, call APIs or execute loads.

Readiness appears on SourceDataset detail pages and is aggregated in Architecture Catalog Source Systems, Portfolio and Map.
 
### 🧩 2.2 Generation Layer
- Creates TargetDatasets in Raw, Stage, Rawcore  
- Injects surrogate keys where required  
- Produces column mappings based entirely on lineage  

Incremental scoping and ingestion behavior are derived from SourceDataset metadata and consistently applied across ingestion, merge, and delete detection.

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
- Controlled reference members for explicitly enabled rawcore references  
- Reference-parent readiness dependencies for controlled reference completion

### 🧩 2.7.1 Controlled Reference Members

Controlled Reference Members are runtime safeguards for modeled rawcore references.

Default members provide stable artificial fallback rows in reference datasets. Inferred members can be created during child dataset loads when a modeled TargetDatasetReference explicitly enables inferred members and the child contains complete reference values without a matching parent row. Default Member Fallback can map still-unresolved child reference keys to the referenced dataset's default member when `default_member_fallback_enabled` is enabled.

This behavior is intentionally execution-owned:

```text
Reference Integrity Review
  = read-only diagnosis

Controlled Reference Members
  = deterministic load-time handling
```

Inferred members are child-load-driven. Loading the parent dataset does not create inferred members, but a later parent full load can replace an inferred row with real source-backed parent data. Default Member Fallback runs after inferred-member creation, so real and inferred parent rows win before unresolved child reference keys fall back to the default member. When controlled reference completion is enabled, the referenced parent dataset is added as an execution dependency before the child load; this is a scheduling concern, not semantic lineage.

### 🧩 2.7.2 Schema Evolution (MigrationPlan + Applier)

Before executing load SQL, elevata derives a **MigrationPlan** from the Architecture Diff and translates it into deterministic schema evolution steps:

- **Dataset renames** are expressed as `RENAME TABLE`  
- **Column renames** are expressed as `RENAME COLUMN`  
- Missing columns may be added (`ADD COLUMN`) when supported  
- Column drops are policy-gated and disabled by default  
    - Base tables: `ELEVATA_ALLOW_AUTO_DROP_COLUMNS=true` enables physical `DROP COLUMN`  
    - `_hist` tables: physical drops require `ELEVATA_ALLOW_AUTO_DROP_HIST_COLUMNS=true`  
    - Without the hist flag, removed business columns in `_hist` are retired (inactive + detached lineage)  

Important design principle:  
Schema evolution does not provision missing tables. Table provisioning is handled centrally by the load runner (`ensure_target_table(...)`) and executed via the target execution engine.

Preflight validation includes schema introspection and dialect-aware semantic equivalence rules to suppress non-actionable type differences.

### 🧩 2.7.3 Architecture Catalog

Architecture Catalog provides the read-only discovery layer for metadata-defined executable architecture.

It helps users inspect:

- Source System and SourceDataset inventory  
- Source Ingestion Readiness  
- Source-to-RAW handoffs  
- TargetDataset inventory  
- schema / layer placement  
- materialization semantics  
- incremental strategy  
- ownership  
- metadata health  
- query logic  
- upstream and downstream relationships  
- column contract signals  
- serving-layer Data Product readiness  
- layer maps and dependency matrices  
- latest execution evidence references  
- architecture quality and governance insights  
- Architecture Control review status summaries  
- on-demand Architecture Quality Review checks for loaded target data  
- on-demand Reference Integrity checks for modeled outgoing references

The Catalog links to dedicated pages for:

- dataset details  
- lineage  
- query contracts  
- Catalog Source Systems  
- Catalog Target Datasets  
- Catalog Portfolio  
- Catalog Data Products  
- Catalog Insights  
- Catalog Map  
- Architecture Control  
- execution history

Architecture Catalog does not connect to sources, edit metadata or execute loads.

Catalog Source Systems group SourceDatasets by Source System and expose deterministic Source Ingestion Readiness, ingestion posture, RAW landing intent and RAW TargetDataset handoffs.

Catalog Portfolio places Source ingestion readiness beside Data Product readiness while keeping TargetDataset coverage and attention signals separate.

Catalog Data Products provide a read-only consumer-readiness perspective for serving-layer datasets. They combine ownership, metadata health, query contracts, lineage, review state and execution evidence into transparent readiness groups: Consumption-ready, Review recommended and Not consumption-ready.

Catalog Insights provide read-only signals for ownership gaps, metadata health findings, custom query logic, downstream consumer visibility, inactive datasets with consumers, and missing execution evidence. Dataset-specific insight signals are also shown on Catalog detail pages.

Catalog Maps provide a read-only architecture lens from Source Systems through SourceDatasets into RAW TargetDatasets and across populated target schemas. Source-to-RAW handoffs, layer cards, layer flow overview, dependency matrix and transition examples make architecture structure visible without introducing graph editing, source connectivity, execution controls or metadata mutation.

Architecture Quality Review provides read-only, on-demand checks against loaded TargetDataset data in Catalog Detail. It derives checks from metadata and delegates SQL rendering to dialects. Initial checks cover non-nullable columns, duplicate surrogate and business keys, empty datasets, and blank string business keys. It does not mutate metadata, execute loads, persist review history, repair data, or gate execution.

Reference Integrity Review provides a read-only, on-demand check for modeled outgoing references in Catalog Detail. It checks loaded target data for missing parent examples and keeps SQL rendering dialect-owned. It does not execute loads, mutate metadata, persist review history, create inferred members, or apply Default Member Fallback. Controlled Reference Members are handled only by load execution when the modeled reference explicitly allows the relevant behavior.

### 🧩 2.7.4 Architecture Control

Architecture Control makes metadata-defined architecture reviewable, approvable, executable through controlled scopes, and auditable.

It provides deterministic artifacts for:

- Architecture State  
- Architecture Change Reports  
- Architecture Promotion Reports  
- Architecture Approval Artifacts  
- Architecture Execution Records  
- policy decisions  
- report fingerprints

Controlled execution is delegated to the load runner. Architecture Control does not bypass preflight validation, materialization policy checks, Architecture Guard enforcement, or dialect-owned SQL rendering.

Command responsibilities:

| Command | Responsibility |
|---|---|
| `elevata_state` | Render the metadata-defined architecture state |
| `elevata_plan` | Render architecture change intent and policy decisions |
| `elevata_promote` | Compare two architecture state artifacts |
| `elevata_approve` | Create architecture approval artifacts |
| `elevata_approval_check` | Verify approval artifacts |
| `elevata_load` | Execute loads with preflight and guard checks |

Architecture Control uses the same semantic path as execution:

```text 
Architecture State → Architecture Diff → MigrationPlan → Policy Decisions
```

The Architecture Control UI adds a constrained operational layer. Architecture Review Briefing summarizes the current scoped report, review status and execution preview before approval or execution. Allowed and metadata-only policy decisions remain confirmation signals, while preflight and blocked decisions are surfaced as reviewer attention:

- scope-aware report and review status  
- compact Architecture Review Briefing  
- approval artifact creation and verification  
- execution preview  
- controlled load execution  
- target-only execution for TargetDataset scopes  
- captured execution output  
- persisted Architecture Execution Records

Execution scopes are explicit:

| Scope | Execution behavior |
|---|---|
| All datasets | Executes all active target datasets with dependency ordering |
| Schema | Executes selected schema roots with dependency ordering |
| TargetDataset | Executes the selected TargetDataset with dependency ordering |
| TargetDataset, target-only | Executes only the selected TargetDataset |

The default execution path remains lineage-aware. Target-only execution is available only for TargetDataset scopes and is intended for focused iteration when upstream data is already available.

Architecture Execution Records capture the audit context of controlled execution:

- execution identifier  
- operator  
- timestamps and duration  
- status and message  
- Architecture Control scope  
- dependency mode  
- report fingerprint  
- approval identifier  
- preview fingerprint  
- command invocation metadata  
- output and error tails  
- deterministic record fingerprint


## 🔧 3. Bizcore - Business Semantics as Metadata

elevata introduces a dedicated **Bizcore layer** for modeling business meaning, rules, and calculations as **first-class metadata**.

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

Bizcore logic is compiled into the same logical plans and SQL as technical datasets, preserving elevata’s guarantees around **determinism, transparency, and reproducibility**.

### 🧩 Serving - Presentation Logic & Consumer Hand-off
Serving is the **presentation-facing** layer. Serving datasets typically expose Bizcore datasets 1:1 (often as views), while allowing **consumer-specific shaping** such as naming, ordering, and lightweight joins where required. Serving is intended as the **hand-off layer to BI tools / semantic layers / frontend use cases** - without moving business logic out of Bizcore.

### 🧩 Custom Query Logic (Query Tree)

For most datasets, elevata generates SQL automatically from metadata. In semantic layers (`bizcore`, `serving`), elevata additionally supports **Custom Query Logic** via an explicit **Query Tree**.

The Query Tree defines the *shape* of a query (e.g. windowing, aggregation steps, union composition) while remaining fully metadata-native.

If enabled, the Query Tree is compiled into the same Logical Plan and Expression AST used by the default generation pipeline. If disabled, elevata falls back to fully automatic SQL generation.

This ensures advanced query shaping without introducing manual SQL or breaking determinism, lineage, or governance guarantees.

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
- **Safe for CI/CD** - predictable SQL for diffing and testing  
- **Execution & Logging** are part of the system

---

## 🔧 8. Related Documents
- [Architecture Catalog](architecture_catalog.md)  
- [Architecture Catalog Source Systems](architecture_catalog_source_systems.md)  
- [Architecture Catalog Portfolio](architecture_catalog_portfolio.md)  
- [Source Backends](source_backends.md)  
- [Generation Logic](generation_logic.md)  
- [Incremental Load Architecture](incremental_load.md)  
- [Load SQL Architecture](load_sql_architecture.md)  
- [Lineage Model & Logical Plan](logical_plan.md)  
- [Dialect System](dialect_system.md)

---

© 2025-2026 elevata - Technical Documentation
