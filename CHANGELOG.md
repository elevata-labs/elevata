# Changelog

All notable changes to this project will be documented in this file.  
This project adheres to [Semantic Versioning](https://semver.org/) and [Keep a Changelog](https://keepachangelog.com/).

---

📈 For the full roadmap, see [Project Readme](https://github.com/elevata-labs/elevata/blob/main/README.md)

🧾 Licensed under the **AGPL-v3** — open, governed, and community-driven.  
💡 *elevata keeps evolving — one small, meaningful release at a time.*


---

## [Unreleased]

TBD

---

## [1.4.1] - 2026-03-01

This patch release introduces and stabilizes elevata’s
dialect-specific SQL keyword registry.

The focus of this release is deterministic, dialect-owned handling
of reserved identifiers across all supported warehouses.

---

### ✨ Added

#### Deterministic SQL Keyword Registry

- Introduced dialect-specific reserved keyword modules  
  (`rendering/dialects/keywords/*.py`)  
- Added `elevata_generate_reserved_keywords` management command  
- Engine-truth extraction for:  
  - Databricks (`sql_keywords()`)  
  - DuckDB (`duckdb_keywords()`)  
- Documentation-based extraction with strict validation for:  
  - PostgreSQL  
  - Snowflake  
  - MSSQL  
  - Fabric Warehouse  
  - BigQuery  
- Sanity checks guarding against incomplete or malformed keyword sets  
- Optional core SQL overlay for defensive fallback safety

---

### 🔄 Improved

- Fully restored dialect-owned SQL rendering for keyword extraction  
- Ensured no vendor-specific SQL remains in management commands  
- Stabilized documentation parsing across vendor doc layouts  
- Harmonized quoting behavior across all dialects

---

### 🔒 Architectural Integrity

- Keyword extraction is deterministic and reproducible  
- All SQL used in extraction is owned by the dialect layer  
- Identifier quoting behavior is now fully dialect-driven

---

## [1.4.0] - 2026-02-28

This release significantly expands elevata’s ingestion capabilities (REST + Files)  
while further strengthening deterministic, dialect-owned execution semantics  
across all supported warehouses.

---

### ✨ Added

#### Ingestion Framework (RAW)

- Native RAW ingestion for REST APIs and file-based sources via `SourceDataset.ingestion_config`  
- Support for CSV, JSON, JSONL, Excel and Parquet sources  
- Environment variable expansion for file URIs (e.g. `${ELEVATA_INGEST_ROOT}/...`)  
- CSV parsing options (`delimiter`, `quotechar`, `encoding`)  
- Excel ingestion options (`sheet_name` / `sheet_index`, `header_row`, `max_rows`) with strict validation  
- Chunked file processing for large datasets with accurate total row tracking  
- Standardized RAW landing with preserved JSON `payload` (system role)  
- Deterministic JSON path–driven column mapping for REST and JSON ingestion

#### Ingestion Routing

- Unified ingestion dispatcher as single entry point (`ingest_raw_for_source_dataset(...)`)

---

### ✨ Improved

#### Dialect-Owned Merge Rendering

- Refactored merge rendering to delegate SQL shape entirely to dialects  
- `load_sql` now provides semantic ingredients only (source select, keys, columns)  
- Cross-platform MERGE fallbacks where native MERGE is not available  
- Improved dialect diagnostics and merge validation behavior

#### Fully Executable Historization (SCD Type 2)

- History datasets (`*_hist`) now generate execution-ready SQL  
- Historization pipeline fully dialect-owned (incremental history primitives)  
- Strict validation for required SCD technical columns and INSERT alignment

---

### 🛠️ Fixed

- SQLAlchemy 2.0 execution compatibility in ingestion landing paths  
- Correct cumulative row reporting for chunked RAW ingestion  
- Cross-dialect merge, historization and delete-detection edge cases  
- BigQuery hashing stability and literal rendering correctness  
- Windows file URI normalization for file-based ingestion

---

## [1.3.1] - 2026-02-18

This release stabilizes the Airflow example environment and resolves several issues  
that could prevent successful execution when testing elevata with external target  
systems.

The focus of this release is usability and reliability of the Airflow example setup,  
ensuring that users can immediately validate elevata orchestration against their own  
target database without requiring additional infrastructure.

---

### ✨ Improved

#### Airflow Example Stability

- Airflow example now installs backend-specific dependencies at container startup  
  instead of build time  
- Backend selection is now driven entirely by environment configuration
  via `ELEVATA_SQL_DIALECT`
- Removed hardcoded backend assumptions from Dockerfile and docker-compose  
- Airflow containers now run elevata inside an isolated virtual environment,  
  preventing dependency conflicts with Airflow itself  
- Improved compatibility with Databricks, PostgreSQL, MSSQL, Snowflake,  
  BigQuery and other supported targets

#### Configuration & Environment Handling

- Environment configuration now consistently sourced from `.env`  
- Removed docker-compose defaults overriding user configuration  
- Simplified switching between target systems without rebuilding images  
- Improved portability for local testing and Open Source usage

#### Dependency Handling

- Backend-specific requirements are installed only when required  
- Prevented SQLAlchemy version conflicts with Airflow dependencies  
- Optional dialect imports (e.g. DuckDB) no longer fail when backend is not active

---

### 🛠️ Fixed

- Airflow example failing to start due to dependency conflicts  
- SQLAlchemy downgrade caused by backend requirement installation  
- Incorrect backend loading when multiple dialects were present  
- Environment variable precedence issues between `.env`,  
  docker-compose defaults and container runtime  
- Multiple startup issues related to entrypoint execution  
- Improved robustness of dialect imports across environments

---

### 🔧 Internal

- Refactored Airflow entrypoint initialization logic  
- Added backend installation stamp mechanism to avoid repeated installs  
- Improved separation between Airflow runtime dependencies  
  and elevata execution dependencies

---

## [1.3.0] - 2026-02-17

This release introduces execution safety improvements, deterministic schema evolution,  
and orchestration-level predictability enhancements.

The focus of this release is long-term stability and reproducible execution behavior  
across platforms, rather than expanding modeling capabilities.

Execution planning, schema evolution, and load execution are now aligned around a  
deterministic preflight model that guarantees predictable outcomes before execution begins.

---

### ✨ Added

#### Execution Safety & Predictability

- Introduced preflight validation phase before execution  
- Deterministic failure behavior for unsafe schema evolution scenarios  
- Execution now blocks before SQL execution when unsafe changes are detected  
- Consistent error vs warning classification during materialization planning  
- Improved transparency of execution behavior and failure causes

#### Schema Evolution & Type Drift Handling

- Canonical datatype comparison across all supported dialects  
- Type drift detection during materialization planning  
- Drift classification into:  
  - equivalent  
  - widening (safe)  
  - narrowing / incompatible (unsafe)  
- Automatic schema evolution for safe widening changes  
- Dialect-aware ALTER COLUMN generation where supported  
- Deterministic rebuild fallback for dialects without ALTER support  
- Consistent schema evolution behavior across Snowflake, Databricks, PostgreSQL,  
  MSSQL and DuckDB

#### Materialization & Execution Architecture

- Materialization split into planning and application phases  
- MaterializationPlan now explicitly represents:  
  - required DDL steps  
  - warnings  
  - blocking errors  
- Structural synchronization of rawcore `_hist` datasets aligned with base datasets  
- Improved consistency between schema introspection, metadata, and execution behavior

#### Orchestration & Execution Planning

- Metadata-driven execution manifest generation  
- Deterministic dependency graph for dataset execution order  
- Airflow example DAG for lineage-based execution orchestration  
- Execution planning fingerprint for reproducibility validation

#### Query Builder UX Improvements

- Query Builder now displays inline validation and dependency conflict messages  
  directly in the editing context instead of failing silently  
- Blocking mutations caused by downstream dataset dependencies are surfaced  
  as contextual warnings in the grid UI  
- Inline editors remain open when a mutation is rejected, allowing users to  
  immediately correct input  
- Conflict warnings are automatically cleared when cancelling or completing edits  
- Improved transparency when renaming or modifying query-derived columns  
  referenced downstream

---

### 🛠️ Fixed

- Multiple edge cases where schema drift could result in inconsistent execution behavior  
- Incorrect handling of schema evolution across dialect-specific type representations  
- Improved stability of materialization planning for renamed datasets and columns  
- Various internal consistency improvements in execution planning and validation

---

### 🔒 Governance & Determinism

- Execution behavior is now fully determined before execution begins
- Unsafe schema changes are blocked deterministically
- Reduced risk of partial schema application or inconsistent load states
- Improved alignment between metadata contracts and physical schema evolution

---

## [1.2.0] - 2026-02-08

This release introduces a major upgrade to Query Builder contract handling,  
query-derived schema synchronization, and datatype inference.

It significantly improves determinism, usability, and correctness of  
metadata-driven query modeling while simplifying internal typing logic.

### ✨ Added

#### Multi-platform execution support

- Added native execution dialects for:  
  - Snowflake  
  - Databricks SQL (Unity Catalog)  
  - Microsoft Fabric Warehouse  
- Unified execution semantics across platforms using the same metadata model  
- No platform-specific modeling required  
- Architecture defined once, executed consistently across engines

This release marks the first version where elevata execution semantics are aligned across  
multiple modern cloud data warehouse platforms.

#### Query Builder & Contract Handling
- Query-derived TargetColumns are now fully synchronized with the Query Tree output contract  
- Automatic creation, rename, update and deletion of query-derived columns  
- Contract-based schema alignment between:  
  - SQL preview  
  - logical plan  
  - materialized dataset schema  
- Aggregate nodes now redefine output contracts explicitly as:  
  - group keys + measures only  
- Window and Aggregate operators expose correct upstream input columns  
  without requiring intermediate target column assignment

#### Datatype inference
- Deterministic datatype inference for:  
  - window functions (ROW_NUMBER, RANK, DENSE_RANK)  
  - aggregate measures (COUNT, SUM, MIN, MAX, AVG)  
- COUNT and ranking functions now default to INTEGER instead of BIGINT  
- Datatypes inferred from upstream input columns where possible  
- Canonical datatype resolution aligned with DATATYPE_CHOICES

#### Query Builder UX improvements
- Aggregate editor now shows input dataset columns directly  
- Window and Aggregate nodes operate on input contracts instead of
  materialized target schema  
- Eliminates unnecessary intermediate column creation

#### Databricks execution improvements
- Improved raw ingestion performance for Databricks SQL Warehouse by batching multi-row INSERT execution
- Eliminates per-row execution overhead caused by connector-level executemany behavior
- No changes required to metadata models or query definitions

### 🛠️ Fixed

- Window columns incorrectly defaulting to STRING datatype  
- Aggregate measures not inheriting input column datatype  
- Contract sync overriding inferred datatypes  
- Missing input columns in Aggregate editor selection  
- Incorrect system-managed flag for query-derived columns  
- Multiple contract sync race conditions caused by partial state inference

### 🔒 Governance & Determinism

- Query contract inference now consistently evaluates against query_head  
- Dataset schema always reflects effective query output contract  
- Deterministic datatype normalization across query-derived columns  
- Reduced risk of schema drift between metadata and generated SQL

---

## [1.1.0] - 2026-01-31

This release introduces a major upgrade to the Query Builder and UNION workflow,  
together with critical stability and governance improvements across metadata,  
schema sync, and historization.

### ✨ Added

#### Query Builder & UNION workflow
- Full UNION operator support with:  
  - Branch management  
  - Output schema contracts  
  - Column mappings per branch  
- Guided UNION toolbar with:  
  - Output / Branch navigation  
  - Schema copy from branch  
  - Auto-map by name  
  - Validation and “Set as head”  
- Contract snapshot with input → output diff  
- Determinism and governance indicators (ordering, window/aggregate checks)  
- Contextual node summaries (SELECT, AGGREGATE, WINDOW, UNION)

#### Metadata-driven governance
- Clear separation of **query_root** (anchor) and **query_head** (current output)  
- Head-based validation, SQL preview and contract inference  
- Guardrails for destructive operations with downstream dependents

#### Schema sync & historization
- Robust metadata-driven rename propagation via `former_names`  
- Orphan preservation in `_hist` (inactive, detached, not dropped)  
- Safe FK reference deletion that never removes hist columns  
- Technical tail columns always appended last  
- Deterministic ordinal normalization for system-managed schemas

### 🛠️ Fixed

- UNIQUE constraint violations in hist regeneration  
- Broken rename propagation across rawcore → hist  
- False-positive orphan deletions  
- Inconsistent candidate column inference in query builder  
- UNION validation / navigation not triggering correctly  
- Output schema copy errors due to field mismatches  
- Multiple signal / sync race conditions

### 🔒 Governance & Safety

- UNION validation enforces compatible schemas  
- Set-as-head explicitly marks final dataset output  
- Defensive rebuild of hist metadata with key lineage preservation  
- Deterministic ordering guarantees for window and aggregate functions

---

## [1.0.0] - 2026-01-25 — First stable release

This release marks the **first stable, backwards-compatible version** of elevata.

elevata has evolved from a SQL generation layer into a **metadata-driven, deterministic data platform engine**.  
From this version onwards, interfaces, metadata structures and execution semantics are considered stable.

### ✨ Highlights

#### 🪄 Query Builder (Major Feature)
- Explicit, metadata-driven **query planning** with SELECT, AGGREGATE, WINDOW and UNION nodes  
- Deterministic query execution via a formal **Query Tree**  
- Clear separation between **query structure** and generated SQL  
- Guided UI for building and evolving queries without writing SQL  
- SQL preview reflects the *exact* executed query

#### 🧠 Query Contracts
- Explicit, inspectable **output schema contracts**  
- Field-level validation and error reporting  
- Early detection of incompatible unions, missing mappings and invalid transformations

#### 🧬 Lineage & Explainability
- End-to-end lineage across datasets, fields and query nodes  
- Full traceability from source to serving layer  
- Query tree visualization for complex transformations

#### 🔒 Determinism & Governance
- Deterministic execution guarantees for aggregates and window functions  
- ORDER BY / PARTITION BY governance with clear warnings and errors  
- Clear distinction between execution errors and policy violations

#### 🧩 Platform Maturity
- Stable execution semantics  
- Backwards compatibility guarantees from this release onwards  
- Foundation for orchestration and governance integrations in future releases

---

## [0.9.1] – 2026-01-14

### ✨ Improved
- Refined lineage visualization with configurable multi-hop upstream and downstream views  
- Clearer lineage semantics distinguishing direct inputs from extended execution dependencies  
- Improved lineage UX with consistent ordering and scope labeling

### 🧠 Serving Layer
- Serving datasets now support presentation-oriented identifiers (friendly names)  
- Dataset and column naming in Serving layer allows casing, spaces, and special characters  
- Identifier handling is dialect-aware and uses proper quoting where required

### 🛡️ Validation & Health
- Extended metadata validation to distinguish blocking vs advisory findings  
- Improved health checks for Serving and Bizcore datasets  
- Validation logic consolidated and aligned across layers

### 🧩 Internal
- Improved consistency between lineage analysis, validators, and UI  
- Minor internal cleanups in metadata services and views

---

## [0.9.0] – 2026-01-12

### 🧠 Bizcore: Business Semantics as First-Class Metadata

This release introduces **Bizcore**, a dedicated layer for modeling  
business meaning, rules, and calculations as explicit metadata —  
executed deterministically alongside technical datasets.

Bizcore makes elevata **business-capable by design**, without introducing  
BI-style semantic layers or query-time abstractions.

### ✨ Added
- Bizcore datasets and columns as first-class metadata objects  
- Multi-upstream join support for Bizcore datasets  
- UI support for building and validating Bizcore structures  
- Deterministic SQL preview for Bizcore datasets  
- Lineage-driven qualification of expressions and joins  
- End-to-end traceability from Core → Bizcore → Serving

### 🔄 Changed
- SQL generation now fully respects semantic lineage in expressions  
- Join aliasing and qualification are applied consistently across layers

### 🧪 Quality & Stability
- Extensive validation of join correctness and expression rendering  
- Scoped and non-scoped UI flows aligned under a single metadata model  
- No breaking changes to existing Raw, Stage, or Core pipelines

### ✨ Improved
- Manual expressions now automatically qualify unaliased column references  
  with the correct input alias during SQL generation.  
  This ensures consistent, unambiguous SQL for Bizcore calculations  
  without requiring users to manually prefix column names.

> This release marks a major milestone: elevata now supports  
> **explicit business semantics as metadata**, not as BI-layer logic.

---

## [0.8.0] – 2026-01-04

### ⚙️ Execution & Orchestration as First-Class Architecture

This release introduces an explicit, metadata-driven execution model,  
establishing **orchestration, failure semantics, and observability** as first-class concerns in elevata.

Execution is now planned, executed, and explained independently of SQL generation,  
providing a robust foundation for platform-native orchestration and governance.

### ✨ Added
- Explicit **Execution Plan** model separating planning from execution  
- Dependency-graph–based dataset execution with deterministic ordering  
- Multi-dataset batch execution with a shared `batch_run_id`  
- Structured execution policies (`continue_on_error`, `max_retries`)  
- Retry semantics with per-attempt tracking (`attempt_no`)  
- Distinct failure semantics:  
  - `blocked` (dependency-based non-execution)  
  - `aborted` (policy-based fail-fast non-execution)  
- **Load Run Snapshot** (`meta.load_run_snapshot`)  
  - Batch-level, JSON-based execution state  
  - Captures plan, policy, dependencies, and aggregated outcomes  
- Extended **Load Run Log** (`meta.load_run_log`)  
  - Orchestration-only events (blocked / aborted)  
  - Best-effort, non-blocking meta logging  
- CLI execution diagnostics:  
  - Execution snapshot printing (`--debug-execution`)  
  - Snapshot persistence (`--write-execution-snapshot`)  
- Deterministic BigQuery table qualification for execution and metadata writes    
  (prevents sporadic cross-project `NotFound` errors during streaming inserts)  
- Global execution modes:  
  - single-dataset execution with dependencies (default)  
  - platform-wide execution in deterministic order (`--all`)  
  - optional schema-scoped execution (`--schema`)

### 🔄 Changed
- Execution semantics are no longer implicit in SQL or CLI flow  
- Load execution is now driven by an explicit execution model  
- Fail-fast behavior is deterministic and explicitly reported  
- Execution observability is metadata-first and dialect-agnostic  

### 🧪 Quality & Stability
- Extensive unit tests for execution ordering, retries, fail-fast, and blocking  
- Guardrails for orchestration-only events and best-effort persistence  
- Clear separation of execution core vs CLI and dialect adapters  
- No destructive changes to existing materialization or SQL generation logic

> This release establishes elevata as a **self-orchestrating, explainable  
> data platform core**, laying the groundwork for native scheduling,  
> governance rules, and external orchestration integrations.

---

## [0.7.1] – 2025-12-29

### 🧱 Metadata-Driven Schema Evolution

This release completes and stabilizes the first materialization layer for  
**safe, deterministic schema evolution** in target warehouses.

Schema changes are now derived explicitly from metadata and applied in a  
controlled, lineage-aware manner — without implicit inference from SQL.

### ✨ Added
- Metadata-driven materialization planning for target datasets  
- Automatic provisioning of missing target tables  
- Deterministic column synchronization (additive, non-destructive)  
- Explicit handling of dataset and column renames via `former_names`  
- Lineage-aware propagation of renames into history (`_hist`) datasets  
- Deterministic `INSERT … (column list)` generation across all dialects  
- DuckDB-native introspection via PRAGMA with execution-engine consistency

### 🔄 Changed
- Materialization is now planned separately from SQL rendering  
- Table existence is determined by effective provisioning steps, not schema creation alone  
- Incremental loads reliably auto-provision target tables when required  
- History datasets are provisioned deterministically and stay structurally aligned with base tables

### 🧪 Quality & Stability
- Extensive unit tests for materialization planning and rename scenarios  
- Guardrails for ambiguous rename situations (multiple former matches)  
- Improved separation of introspection vs execution concerns  
- Removed duplicate provisioning paths and race conditions  

> This release lays the foundation for controlled schema evolution,  
> future governance rules, and automated validation layers.

---

## [0.7.0] – 2025-12-21

### Added
- Dataset-driven, lineage-aware execution with automatic dependency resolution  
- Unified RAW execution semantics via physical ingestion (Source → RAW)  
- Stable technical column model across all layers  
- BigQuery execution backend with native ingestion support  
- Dialect-aware hashing and surrogate key generation (BigQuery, DuckDB, Postgres, MSSQL)  
- SourceDataset-level static and incremental filters with runtime {{DELTA_CUTOFF}} resolution  

### Changed
- Execution operates on datasets rather than layers  
- RAW datasets are treated as an optional ingestion layer  
- RAW ingestion always rebuilds tables, while source extraction may be incrementally scoped  

### Fixed
- Load plan debug output and execution logging consistency  
- Signal handling for historization execution order  
- Correct application of incremental filters during ingestion and delete detection  
- Lineage-based translation of incremental scope filters across renamed columns  
- Cross-dialect consistency for incremental execution (DuckDB, MSSQL, Postgres, BigQuery)  

---

## [0.6.1] – 2025-12-15

### Fixed
- Correct introspection of SQL Server alias types (e.g. `dbo.Name`, `dbo.Flag`)
- Proper handling of `bit` columns during ingestion (no fallback to string types)
- Correct precision and scale mapping for `money` and `smallmoney` columns
- Stable and deterministic column ordering during metadata import

### Improved
- Lossless ingestion of source datatypes via `source_datatype_raw`
- Strict, fail-fast dialect-specific type rendering to prevent silent fallbacks

### Notes
- This release significantly improves correctness for SQL Server as a source system.
- Re-importing source metadata is recommended to benefit from the improved typing behavior.

---

## [0.6.0] – 2025-12-14

### 🚀 Warehouse-Native Execution & SCD Historization
This release introduces the foundation for a fully warehouse-native execution framework.  
elevata now manages entire data load pipelines end-to-end — from metadata to SQL generation to execution, historization and observability.

### ✨ Major Features

#### 1. Execution Engine (`--execute`)
elevata can now execute rendered SQL directly against target systems, measure performance, record affected rows, and log complete run metadata.  
This shifts elevata beyond SQL rendering into a full pipeline engine.

#### 2. Full SCD Type 2 Historization
A deterministic, metadata-driven historization framework:  
- automatic change detection via row-hash  
- version closing for changed and deleted keys  
- insertion of new and changed versions  
- lineage-aware attribute propagation  

#### 3. Metadata-Driven Incremental Merge Loads
Complete incremental pipeline including:  
- new-row inserts  
- changed-row updates  
- delete detection  
- MERGE or UPDATE+INSERT fallback depending on dialect

#### 4. Auto-Provisioning of Warehouse Structures
elevata can automatically create:  
- target schemas (raw, stage, rawcore, ...)  
- the `meta.load_run_log` table  
- all required objects for execution and logging  

Controlled via `.env` flags.

#### 5. Warehouse-Level Load Logging
A new table `meta.load_run_log` provides full observability into load executions:  
- load mode, historization flags, dialect  
- start/end timestamps, render/execution duration  
- rows affected, error messages, status  
- batch and run identifiers  

#### 6. Documentation Expansion
- New historization architecture document  
- Extended execution, logging, and provisioning sections  
- Revised dialect and SQL generation chapters  

### 🧪 Testing Improvements
- Deterministic SQL tests for merge and historization pipelines  
- Combined historization pipeline tests  
- Prepared E2E execution flow for dialect-specific execution engines  

> This release establishes the execution foundation on which future orchestration, validation and automation layers will be built.

---

## [0.5.3] — 2025-12-10
### 🔹 Historization Structure & Dialect Engine Enhancements

This release completes the metadata foundation required for full historized
incremental loading in v0.6.0. It finalizes *_hist dataset structure, ensures
cross-dialect consistency, and extends SQL rendering to use dialect-driven
identifier rules.

## ✨ Highlights

### Metadata / Historization
- Automatic creation and maintenance of `<dataset>_hist` datasets in RAWCORE
- Full rename propagation for datasets and columns
- All *_hist fields are system-managed and read-only
- New technical field in RAWCORE: `row_hash` for change detection (persisted expression)
- Versioning strategy established:
  - `version_started_at` inclusive, `version_ended_at` exclusive
  - open-ended validity via max timestamp
  - `version_state` (`current`, `changed`, `deleted`)

### SQL Generation / Dialects
- Unified `render_identifier()` and `render_table_identifier()` for consistent quoting
- All SQL generation now uses dialect identifier rendering
- Delete detection routing tested and guarded per dialect capability

### Load Runner
- `elevata_load` supports `--execute` with safe stub execution via `ExecutionEngine`
- Logging improvements and full dry-run support remain functional

### Testing & Stability
- Expanded test coverage for historization and dialect routing
- Full suite green across merge, delete detection & *_hist scenarios

---

## [0.5.2] — 2025-12-07
### 🛠️️ Metadata stability & History (HIST) foundation

This release significantly improves the robustness, determinism, and safety of 
history metadata generation in the RAWCORE schema.

## ✨ Highlights

### Metadata / Historization
- Deterministic generation of *_hist datasets based on lineage_key.  
- Robust schema sync between RAWCORE and *_hist (idempotent, safe deletes).  
- History SK expression based on rawcore SK + version_started_at.  
- History BK definition: rawcore SK + version_started_at.  
- History datasets and columns are fully system-managed (no UI unlock).

### Signals & UI
- Automatic *_hist sync on dataset rename and column changes in rawcore.  
- Inline rename refreshes both rawcore and corresponding *_hist rows.  
- Inline editing is disabled for *_hist datasets and columns.  

### SQL Preview
- build_sql_preview_for_target returns a clear comment for history targets instead of misleading SQL.
- Tests added to guard the _hist-preview behaviour.

---

## [0.5.1] — 2025-12-04
### 🧹 Documentation & Consistency Release

This patch focuses on improving the clarity, coherence, and structure of elevata’s developer documentation.

## ✨ Highlights
- Full harmonization of all architecture documents  
- Removal of outdated version references and legacy wording  
- Unified heading and layout style across all Markdown files  
- Consistent terminology for LogicalPlan, Expression DSL, Dialects, and Load SQL  
- Improved mkdocs navigation structure  
- Minor text corrections and consistency fixes across the docs

## 🚫 No functional changes
This release does not modify the SQL engine, metadata model, or any public API surface.  
All test suites remain unchanged and green.

---

## [0.5.0] — 2025-12-01
### 🛠️️ Multi-Dialect Engine, MSSQL Support & Deterministic FK Hashing

This release delivers the next major milestone of elevata’s SQL engine:
full **multi-dialect SQL generation**, an extensible dialect factory,
runtime dialect switching in the UI, and a complete rewrite of the
surrogate-key and foreign-key hashing system using a vendor-neutral DSL AST.

---

## 🚀 Major Features

### **1. Multi-Dialect SQL Rendering (Postgres, DuckDB, MSSQL)**
- New pluggable dialect architecture (`SqlDialect`, `dialect_factory`).
- Three fully operational dialects:
  - **DuckDBDialect**
  - **PostgresDialect**
  - **MssqlDialect** (new)
- Centralised dialect registry & runtime resolution via:
  - profile  
  - env (`ELEVATA_SQL_DIALECT`)  
  - URL parameter in SQL preview  

All SQL generation (preview + Load Runner) now passes through a unified,
dialect-aware pipeline.

---

### **2. SQL Preview Dialect Selector (UI)**
- New dropdown in TargetDataset detail view.
- Instant SQL refresh via HTMX request.
- Clean display of dialect-specific SQL functions (quoting, hashing, concat, types).

---

### **3. Deterministic, Cross-Dialect Hashing via DSL AST**
A full rewrite of surrogate-key and FK hashing:

- New DSL expression system (`Hash256Expression`, `ConcatWsExpression`, `Literal`, `ColumnRef`).
- Dialect-specific SQL rendering happens **exclusively in dialect classes**.
- Identical logical lineage yields identical hash values across vendors.
- Fully deterministic ordering + null replacement semantics.
- Clean child-lineage FK hashing:
  - BK1, child BK1, BK2, child BK2…
  - `~` and `|` literal separators, ordered alphabetically

All existing hashing tests green after the rewrite.

---

### **4. Multi-Source Stage Identity Mode**
- Correct logical union builder for Stage datasets with multiple upstream sources.
- Clean identity (no ranking) vs. non-identity (ranking) handling.
- Injected `source_identity_id` literal per upstream branch.
- All multi-source identity tests fully passing.

---

### **5. Dialect-Aware FK Rendering**
- Parent surrogate keys and child FK keys now rendered via DSL → dialect.
- MSSQL: `CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', …), 2)`
- Postgres: `ENCODE(DIGEST(CONCAT_WS(...), 'sha256'), 'hex')`
- DuckDB: `SHA256(CONCAT_WS(...))`

---

## 🔧 Internal Improvements
- Entire `builder.py` cleaned, simplified, and refactored.
- Unified `render_select_for_target()` and load-SQL paths.
- Removed legacy manual hashing logic.
- No raw SQL string assembly left in hashing pipeline.
- Strict quoting rules per dialect.
- Sauber extrahierte DSL operators (`col()`, `lit()`, `concat_ws()`, `hash256()`).

---

## 🧪 Testing
- New tests:
  - `test_dialect_postgres.py`
  - `test_hashing_dialects.py`
  - `test_fk_hashing.py`
  - Full MSSQL hashing coverage
- Updated test helpers for DSL AST inspection.
- All Stage multi-source tests green after identity-mode rewrite.

---

## 📘 Documentation
- Updated architecture docs:
  - *Dialect System*
  - *SQL Rendering Conventions*
  - *Hashing Architecture*
- README modernised with new capabilities and architecture.

---

## 🧭 Roadmap Shift
With the 0.5.0 SQL backend complete, the next stage focuses on execution:

- Load Runner CLI (Full, Merge, Dry-Run)
- Caching & improved SQL formatting
- Multi-source incremental merges
- Additional dialects (Snowflake, BigQuery, Databricks)

---

**Impact**  
Version **0.5.0** transforms elevata into a **true multi-backend SQL generator**  
with deterministic hashing, dialect-specific rendering, and a stable architectural core  
for future execution engines.

---

## [0.4.0] — 2025-11-20
### 🧠 Dialect Architecture & Load SQL Modernization

This release marks a major leap for elevata:  
a complete SQL dialect abstraction layer, a unified Load-SQL pipeline,  
and extensive new documentation that sets the foundation for future multi-backend support.

---

### 🚀 Core Features

#### **Fully Modular SQL Dialect System**
A new, extensible dialect layer powers all SQL generation:

- Central `SqlDialect` base class  
- Concrete `DuckDBDialect` reference implementation  
- Dialect resolution via `ELEVATA_SQL_DIALECT`, `ELEVATA_DIALECT`, and active profile  
- Dialect capabilities:
  - `supports_merge`
  - `supports_delete_detection`
- Expression-level hooks:
  - `concat_expression()`
  - `hash_expression()`
  - `cast_expression()`
  - `render_literal()`

This architecture enables clean, vendor-neutral SQL generation for future backends  
(Postgres, MSSQL, Snowflake, BigQuery, Databricks).

---

### 🔧 Load SQL Architecture 2.0

A fully redesigned, dialect-aware Load SQL engine:

#### **Full Load**
- `render_create_replace_table`  
- `render_insert_into_table`  
- Uses dialect quoting, casting, literal handling

#### **Incremental Merge Load**
- Native dialect-specific `MERGE` for DuckDB  
- Clean failure modes for dialects without merge support  
- Deterministic key handling  
- Automatic update/insert column mapping

#### **Delete Detection**
- Dialect-specific implementation (`DELETE … WHERE NOT EXISTS`)  
- Guardrails when delete detection is requested but dialect does not support it

All Load SQL now flows through a single, coherent pipeline via `load_sql.py`.

---

### 🧪 Testing Enhancements

- New test suite for:
  - literal rendering (`NULL`, booleans, strings, dates, datetimes)
  - cast expression rendering
  - concat & hash expression helpers
  - merge & delete detection dialect hooks
- End-to-end tests for Full and Merge load generation
- All tests green across the refactor

This ensures reliable future extensions to new SQL dialects.

---

### 📘 Documentation

Three major new documents added:

- **Dialect System** — full architectural overview of dialect abstraction  
- **Load SQL Architecture** — how Full, Merge, and Delete Detection SQL are generated  
- **Incremental Load Architecture** — planner, merge semantics, delete detection

All are linked from:
- `index.md`
- `README_docs.md`
- `mkdocs.yml` navigation

---

### 🔍 Internal Improvements

- Harmonized `get_active_dialect()` with environment and profile resolution  
- Consolidated SQL preview and load paths to use the same dialect entrypoints  
- Removed legacy assumptions and duplicated logic  
- Fully revised DuckDB implementation as reference for new dialects

---

### 🗺️ Roadmap Impact

With 0.4.0 released, the following items shift to **0.5.x**:

- Target System Selector (Profiles → target backend)  
- Additional SQL dialects (MSSQL, Postgres, Snowflake)  
- Pseudo-Lineage Graph in UI  
- Multi-Source Incremental Loads  
- Load-Runner CLI

These features build directly on the new architecture introduced in 0.4.0.

---

**Impact**  
Version 0.4.0 delivers the foundational SQL engine for elevata’s future:  
clean, extensible, and ready for multiple SQL backends.  
It stabilizes the path toward 0.5.x — where elevata becomes a multi-dialect metadata-driven ETL generator.

---

## [0.3.0] — 2025-11-12
### Lineage-Aware Target Generation & SQL Preview

#### 🚀 Core Features

**Lineage-Driven Target Generation**
- Added a stable `lineage_key` to both `TargetDataset` and `TargetColumn`:
  - Enables fully **idempotent target generation**.
  - Prevents duplicate targets after renaming (`lineage_key` is preserved).
- `TargetGenerationService.apply_all()` refactored into modular steps:
  - Existing datasets are now matched and updated via `lineage_key` instead of physical names.
  - Clean dataset-level and column-level re-numbering during regeneration.

**Three-Layer Data Lineage**
- Explicit dataset-level lineage:
  - `TargetDatasetInput` defines upstream relationships (`source_dataset` and/or `upstream_target_dataset`).
  - `combination_mode` (`single` or `union`) indicates how multiple inputs are combined.

- Explicit column-level lineage:
  - `TargetColumnInput` mirrors the same relationships for individual columns.
  - `upstream_columns` now correctly map transformations between layers.

- Layer-specific rules:
  - **Raw** = only `source_datasets`
  - **Stage** = prefers Raw as upstream (or Source directly if `generate_raw_tables=False`)
  - **Rawcore** = always built from Stage

**Multi-Source Consolidation**
- New `SourceDatasetGroup` + `SourceDatasetGroupMembership` model:
  - Supports joining multiple SourceDatasets into a single Stage target.
  - The “primary system” flag defines which source drives column order.
- `TargetDatasetInput.role` classifies inputs as:
  - `primary`, `enrichment`, `reference_lookup`, or `audit_only`.

**Surrogate & Business Keys**
- Surrogate key columns are automatically renamed when their dataset is renamed  
  → e.g. renaming `rc_aw_productmodel` → `rc_aw_product_model` auto-renames the key column to `rc_aw_product_model_key`.
- Surrogate key expressions now reference **upstream column names** (Raw or Stage), not renamed targets.
- Deterministic column ordering:
  1. Surrogate keys  
  2. Business keys  
  3. Integrated source columns  
  4. Artificial columns

**Column Generation Enhancements**
- Automatic assignment of `ordinal_position` on save:
  - Newly created columns append at the end in numeric sequence.
  - Safe against manual reordering.
- Integrated columns added after initial generation are correctly appended and re-numbered without violating unique constraints.

---

#### 🧠 Logical Query Model & SQL Preview

**Logical Plan Layer**
- New internal model (`logical_plan.py`) represents canonical SQL structure for a target dataset:
  - Supports `LogicalSelect`, `LogicalUnion`, `LogicalExpression`, and lineage mapping.
- `builder.py` now constructs expressions (Surrogate Key, BK, and regular fields) from `TargetColumnInput` lineage.
- Dialect-specific type mapping handled cleanly via `map_logical_to_duckdb_type`.

**SQL Preview 2.0**
- SQL preview now generates **true lineage-based SELECT statements**, e.g.:
  - **Stage:**  
    ```sql
    SELECT … FROM "raw"."raw_aw1_person"
    UNION ALL
    SELECT … FROM "raw"."raw_aw2_person"
    ```
  - **Rawcore:**  
    ```sql
    SELECT hash256(…) AS rc_aw_person_key, … 
    FROM "stage"."stg_aw_person"
    ```
- Automatic field alignment:
  - Columns missing in one upstream are rendered as `NULL AS <column>`.
  - Integrated columns retain their target aliases.
- Supports both `manual_expression` and templated (`{{ … }}`) syntax.
- New visually distinct **green preview box** in UI with proper formatting:
  - Keywords capitalized  
  - Indentation after `SELECT`  
  - Clean separation before `FROM`

---

#### 🧩 UI, Governance & Behavior

- **Context-aware lineage display** in detail views:
  - `Source Datasets` and `Upstream Datasets` shown based on layer.
  - Input relations now read like:
    ```
    raw_aw1_person · businessentityid -> stg_aw_person · businessentityid
    ```
- **System-managed field handling** refined:
  - Layer-specific read-only fields controlled by settings.
  - `lineage_key` treated as an internal system field (hidden in forms and lists).
  - Surrogate key names locked for user editing but updated automatically when renaming datasets.

---

#### 🧪 Testing & Quality

**Structured Testing Foundation**
- Introduced the first complete automated test framework for the metadata generation platform.
- Added dedicated `runtests.py` launcher for reliable execution across environments.
- Integrated **realistic DB-based lineage tests** (`Raw → Stage → Rawcore`).
- Added **logic-only tests** for hashing, naming, and validators.
- Prepared **SQL Preview test templates** for the future rendering pipeline.
- New documentation: [🧪 Testing & Quality](docs/tests.md)

**Impact**  
This milestone establishes a solid foundation for test coverage,  
ensuring safe refactoring, reproducibility, and confidence in every release.

---

## [0.2.6] — 2025-11-03
### ⚙️ Target Generation & Surrogate Key Implementation

**Core Features**
- Introduced fully automated `TargetDataset` and `TargetColumn` generation service (`TargetGenerationService`).
- Deterministic surrogate key creation using SHA-256 and runtime-loaded pepper.
- Added `business_key_column` and `surrogate_key_column` flags to differentiate logical vs. physical keys.
- Layer-aware naming now based on `TargetSchema.physical_prefix` (no hardcoded prefixes).
- Integrated filtering: only `integrate=True` columns included across all layers.

**UI Enhancements**
- Added **“Generate Targets”** button to SourceDataset list with progress spinner & success message.
- Improved error feedback and runtime validation for pepper and target schema scope.
- Consistent Bootstrap iconography (`bi-lightning-charge`) and visual feedback for active operations.

**Technical Refinements**
- Surrogate key expressions persisted in metadata for transparency and traceability.
- Environment-based pepper resolution via `.env` and `get_runtime_pepper()`.
- Refactored naming logic (`naming.py`, `rules.py`, `mappers.py`) for consistent layer-specific conventions.

**Impact**  
This release completes the **Target Automation foundation** for elevata —  
paving the way for v0.3.0’s Meta-SQL and rendering engine. 🚀

---

## [0.2.5] — 2025-10-27  
### 🧩 Metadata Model Finalization & UI Polish  

**Core Enhancements**  
- Completed redesign of the **core metadata model** — fully aligned with the 0.3.x architecture.  
- Added **TargetSchema** as a first-class model defining platform layers (`raw`, `stage`, `rawcore`, `bizcore`, `serving`).  
- Introduced **TargetDatasetInput** and **TargetColumnInput** for multi-source mappings and lineage tracking.  
- Added lifecycle flags (`active`, `retired_at`) for controlled dataset and column deprecation.  
- Simplified incremental-load logic (`increment_filter` placeholder on SourceDataset).  
- Unified naming conventions (`*_schema_name`, `*_dataset_name`) across all models.  
- Extended governance primitives (`sensitivity`, `access_intent`) and surrogate-key configuration per layer.  
- Removed obsolete fields (`get_metadata`, `stage_dataset`, etc.) and harmonized field semantics.  

**UI & Usability**  
- Introduced **SourceDatasetGroup** for managing groups of structurally identical source tables.  
- Added governance badges and toggles for better lineage and visibility cues.  
- Revised navigation order for more natural workflows.  
- Improved help texts, icons, and consistent color themes across all metadata entities.  

**Impact**  
This release finalizes the **metadata foundation** for elevata — stable enough for automation development in 0.3.x.  
No breaking structural changes expected before 0.3.0.  

---

### 🪶 UI Comfort Continuation  

- Unified color scheme for governance badges (`badge-pii-high`, `badge-pk`, …).  
- Improved hover feedback and spacing in list views.  
- All badges defined declaratively via `ELEVATA_CRUD` — no model-specific logic required.  
- Updated `elevata-theme.css` for consistent badge geometry and hover states.  

---

**Why it matters**  
Version 0.2.5 concludes the **“Model & Comfort”** milestone:  
the framework now combines a stable metadata core, polished UI, and ready groundwork for automated target generation in 0.3.x. 🚀  

---

## [0.2.4] — 2025-10-26
### Strategic Documentation & Architecture Alignment

This release finalizes the strategic and architectural foundation for the upcoming **metadata model freeze (v0.3.x)**.  
It does not yet include model changes — instead, it defines the *why* and *how* for the next major milestone.

**Highlights**
- New and refined **README** with philosophy, vision, and AGPLv3 licensing
- Updated **roadmap** outlining the transition toward declarative architecture
- Strategic **dbt decoupling paper**, defining the new “governed SQL through architecture” direction
- Preparations for **TargetSchema**, **TargetDatasetReference**, and **deterministic key generation** to follow in v0.3.x

**Why it matters**  
This release marks the calm before the model storm — the documentation is ready, the vision is clear, and the next step is building it. 🚀


## [0.2.3] – 2025-10-25
### 🪶 UI Comfort Release 

**Highlights**
- Added generic, reusable filter bar for all CRUD list views  
- Added dynamic toggle buttons for boolean fields  
- Improved badge rendering for PII & PK indicators  
- Added sticky table headers for long datasets

**Why it matters**  
This release focuses purely on usability and governance visibility.  
It lays the groundwork for 0.3.0 (TargetDataset automation and lineage features).

---

## [0.2.2] – 2025-10-25
### 🧹 Maintenance Release — dbt Dependency Cleanup

#### Summary
This minor maintenance release removes all remaining dbt-related artefacts and clarifies elevata’s independent direction ahead of the 0.3.x milestone.

#### 🔧 Changes
- Removed unused `dbt_project/` folder from repository.
- Deleted all `DBT_*` variables from `.env` and example configuration files.
- Removed dbt references from `NOTICE.md` and documentation.
- Updated `README.md` and `dbt_decoupling.md` to reflect full **runtime independence**.
- Adjusted Roadmap and strategy wording in `CHANGELOG.md` (dbt now optional adapter, not dependency).
- Minor documentation clean-ups and license consistency fixes (MIT → AGPL v3 in trademark notice).

#### 💡 Notes
This release does **not** introduce new features but marks an important architectural boundary:
elevata ≥ 0.2.2 operates entirely without dbt or its configuration files.  
The foundation for native rendering and execution begins with v0.3.x.

---

## [0.2.1] – 2025-10-23
### 🪶 Improved
- Added truncation for long text fields (`Description`, `Remark`) in list views to improve readability.  
- Full text now appears on hover for better UX.  
- Refined visual highlighting for primary and integrate columns.  
- Minor CSS polish and layout consistency fixes across metadata tables.  

---

## [0.2.0] - 2025-10-22

### 🧩 Metadata Introspection & Profiles Integration

#### Overview  
This release marks a major milestone – elevata now connects to relational sources via SQLAlchemy and imports full schema metadata directly into its core models. The new profile and secret management architecture lays the foundation for secure, declarative, and environment-aware metadata operations.

---

#### 🚀 Highlights  

- **Generic Metadata Import via SQLAlchemy**  
  - Engine factory supporting multiple relational backends (MSSQL, Postgres, SQLite).  
  - Reads column definitions, data types, PK information from `SourceDataset` entries.  
  - Automatic datatype normalization across dialects (e.g. `NVARCHAR` → STRING, `BIT` → BOOLEAN).  

- **Flexible Secrets & Profiles**  
  - Unified `elevata_profiles.yaml` config with environment-based secret resolution.  
  - Connection references derived convention-based from `type` and `short_name`.  
  - Optional Azure Key Vault integration.  

- **Security & Configuration**  
  - Sensitive data never stored in the database.  
  - Secrets resolved dynamically at runtime via `.env` or Key Vault.  
  - Clear separation of metadata and operational configuration.  

- **Developer Experience**  
  - Simplified connector interfaces and improved error reporting.  
  - Cleaner model relationships for `SourceSystem` and `SourceDataset`.  
  - New code organization: `connectors.py`, `resolver.py`, `ref_builder.py`.

---

#### 🧭 Next: v0.3.0 will focus on automated target model generation and metadata lineage.

---

#### 🧠 Technical Notes  

- Fully decoupled from dbt profiles; all runtime connections and secrets resolved through `elevata_profiles.yaml`.  
- All SQL renderers return expressions as plain text templates — ready for downstream ELT tools or custom runners.  
- Surrogate Key hashing implemented engine-specifically (Postgres pgcrypto / MSSQL HASHBYTES).  
- Supports per-profile Overrides for multi-DB systems (e.g. `sap1`, `sap2` → `sap`).  
- Improved ordering, idempotency and error reporting in import and generation routines.  

---

## [0.1.1] - 2025-10-19

### 🪶 *UI Polish & PostgreSQL Power*

#### Overview  
A refinement release that makes **elevata** smoother and more flexible:  
a polished Django UI meets full **PostgreSQL** support — available via Docker or your own setup.  
Better visuals, faster workflows, and real database choice.

---

#### ✨ Improvements  

- **UI & UX Enhancements**  
  - Polished Django interface with cleaner layouts and spacing  
  - Improved responsiveness and overall visual consistency  
  - Optimized inline interactions and usability tweaks  

- **Database Support**  
  - Full PostgreSQL backend support  
  - Works with Docker Compose or a user-provided instance  
  - Updated settings for seamless configuration and migrations  

- **Developer Experience**  
  - Simplified environment setup (SQLite or PostgreSQL)  
  - Improved local testing through Docker Compose  

---

## [0.1.0] - 2025-10-14

### 🧩 *Metadata Management Comes Alive*

#### Overview  
This release marks a major milestone:  
**elevata** now provides a fully functional, metadata-driven web interface for managing your data platform’s core structures — built with **Django**, **HTMX**, and a clean **Bootstrap 5** theme.  

It’s the first end-to-end usable version:  
from user login → to inline editing → to audit tracking — all running securely and responsively out of the box.

---

#### 🚀 Highlights  

- **Complete Metadata Management Module**  
  - Inline CRUD with audit fields and user tracking  
  - Automatic URL & view generation for all models  

- **Modern UI & UX**  
  - Responsive elevata theme (Bootstrap 5.3)  
  - Autofocus & usability improvements for inline editing  
  - Unified form and grid styling  

- **Security & Reliability**  
  - Integrated authentication (login, logout, password change)  
  - Safe CSRF handling for all HTMX requests  

- **Developer Experience**  
  - Default SQLite backend for easy setup  
  - Clean folder structure: `core/`, `metadata/`, `dbt_project/`  
  - Ready for future extensions (PostgreSQL, dbt, etc.)

---

## [0.0.1] - 2025-10-06

### Added
- Project documentation scaffold (`README.md`)
- License file (`LICENSE`) under AGPLv3
- Notice file (`NOTICE.md`) for third-party licenses
- `.gitignore` for Python and dbt projects
- Placeholder `requirements/base.txt`
- Initial backend support for **DuckDB** (`requirements/duckdb.txt`)
- Base `dbt_project/` folder
