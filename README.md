# elevata

<p align="center">
  <img src="https://raw.githubusercontent.com/elevata-labs/elevata/main/docs/logo.png" alt="elevata logo" width="130"/>
</p>

**elevata** is an independent open-source project aiming to make modern data platforms radically simpler.  
It’s designed as a **Declarative Data Architecture & Metadata Framework** — automated, governed, and platform-agnostic.

Instead of manually crafting endless SQL and pipeline code, elevata lets metadata do the work.  
By defining datasets, lineage, and transformation logic declaratively, you can generate consistent, auditable, and future-proof data models —  
including schema evolution and physical execution — ready to run on your preferred platform.

## License & Dependencies

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://github.com/elevata-labs/elevata/blob/main/LICENSE)
[![Built with Django](https://img.shields.io/badge/Built%20with-Django-092E20?logo=django)](https://www.djangoproject.com/)
[![Frontend: HTMX](https://img.shields.io/badge/Frontend-HTMX-3366CC?logo=htmx)](https://htmx.org/)
[![UI: Bootstrap 5](https://img.shields.io/badge/UI-Bootstrap%205-7952B3?logo=bootstrap)](https://getbootstrap.com/)  

---

## 🧭 What is elevata?

**elevata** transforms metadata into architecture.  
It reads source system metadata, derives logical and physical target structures, and enforces consistent, privacy-compliant data models — automatically.  

It codifies architectural best practices, generates high-quality SQL, and provides a declarative way to build Raw → Stage → Core pipelines.  

elevata executes data pipelines in a **dataset-driven** and **lineage-aware** manner. Each execution resolves dependencies automatically and processes datasets in the correct semantic order, rather than layer by layer.

The goal:  
💡 **Turn metadata into executable, dialect-aware SQL pipelines** — reliably, transparently, and with full lineage.

Where most tools stop at SQL generation, elevata goes further:  
it defines **how a modern data architecture should look** — opinionated, governed, reproducible.  
*In other words: elevata brings structure, governance, and automation to modern data platforms — from metadata to SQL.*  

elevata is designed for data engineers and architects who want to build governed, reproducible data platforms without hard-coding architecture into pipelines.

Unlike transformation-centric tools, elevata treats metadata, lineage, and execution semantics as first-class concepts, not as conventions embedded in SQL.


<p align="center">
  <img src="https://raw.githubusercontent.com/elevata-labs/elevata/main/docs/elevata_v0_7_0.png" alt="elevata UI preview" width="700"/>
  <br/>
  <em>Dataset detail view with lineage, metadata, and dialect-aware SQL previews</em>
</p>

**elevata** uses Django models to define:

- **datasets** (sources, raw, stage, core)
- **business keys, surrogate keys, foreign keys**
- **column expressions**
- **incremental strategies**
- **source systems & target systems**
- **dependencies between datasets**

From this metadata, elevata generates:

- clean SQL (`SELECT`, `MERGE`, delete detection)
- surrogate key expressions
- foreign-key lineage expressions
- multi-source stage pipelines
- SQL previews with lineage

elevata includes a complete **LogicalPlan + Expression AST** engine that supports  
multiple SQL dialects with deterministic rendering.  

While SQL previews are an important inspection and debugging tool,  
elevata is fundamentally designed for **warehouse-native execution**.

All generated SQL can be executed deterministically against the target system,  
including incremental loads, merges, historization, and schema synchronization.


> *Modern data platforms often fail not because of missing tools, but because*  
> *architecture, lineage, and governance are encoded implicitly in SQL and pipeline code.*  
> *elevata exists to make these concerns explicit, declarative, and reproducible.*

---

## 🧩 Architecture Overview

elevata is a metadata-driven, warehouse-native data pipeline engine.  
It manages the full lifecycle from metadata to SQL generation to execution — including  
incremental loads, historization, schema evolution, and observability.

The architecture consists of the following layers:

### 1. Metadata Layer (Django models)
Defines datasets, columns, lineage, ownership, incremental policies, and execution semantics.  
All structural and behavioral decisions originate from metadata — not from SQL.

### 2. Logical Plan Layer
A vendor-neutral representation of queries and write operations, including:  
- SELECTs, UNIONs, joins, filters  
- incremental merge semantics  
- delete detection  
- historization logic

Logical plans are deterministic and reproducible.

### 3. Expression AST
A unified Abstract Syntax Tree for expressions:  
- literals and column references  
- function calls and hashing  
- binary operations and concatenations  
- window functions and technical expressions

This ensures consistent semantics across all target systems.

### 4. SQL Dialects
Dialect-specific renderers for DuckDB, Postgres, MSSQL, and BigQuery (more to come).

They handle:  
- SQL syntax differences  
- hashing and surrogate key generation  
- quoting and identifier rules  
- MERGE vs UPDATE/INSERT fallbacks

### 5. Materialization & Schema Evolution
Physical target schemas are synchronized deterministically against metadata definitions.

elevata manages schema evolution explicitly and safely:

- automatic table provisioning  
- additive column evolution  
- safe column and dataset renames  
- deterministic reconciliation on full refresh  
- non-destructive behavior for incremental loads  

Schema changes are:  
- metadata-driven (never inferred from SQL)  
- lineage-aware  
- deterministic and reproducible across dialects

This makes schema evolution a **first-class architectural concern**, not a side effect of execution.

### 6. Execution & Observability
Rendered SQL is executed directly in the warehouse:

- incremental and full loads  
- historization (SCD Type 2)  
- delete detection  
- execution timing and row counts  
- structured load logging via `meta.load_run_log`

elevata is designed for execution — not just preview.

---

## 📚 Example Workflow

1. Define your metadata in Django admin  
2. Inspect lineage in the dataset detail view  
3. Select the SQL dialect in the preview section  
4. Review generated SQL  
5. Run load pipelines via CLI (warehouse-native execution supported)

---

## 📦 Load Runner CLI

elevata includes a dataset-driven load runner (`elevata_load`) that executes
pipelines in dependency order.  

Execution semantics depend on the target dataset and its layer:  
- SQL execution for Stage, Rawcore and downstream layers  
- Ingestion logic for Raw datasets  

The load runner supports dry runs, execution diagnostics, dependency resolution,
and execution logging.

---

## 🔮 Roadmap

elevata is evolving from a SQL-generation layer into a **metadata-driven, warehouse-native data platform engine**.  
The roadmap reflects this direction: structured, ambitious, and aligned with elevata’s long-term vision.

---

### v0.7.x — Productivity & Governance Layer
> *Guiding question: Can execution be explained, governed, and safely extended?*  

- **Metadata-driven ingestion (optional)**  
  Ingestion into Raw datasets based on source metadata, with support for  
  native, external, or no-ingestion strategies. Pipelines may also start  
  directly at the Stage layer using federated access. 

- **Automated schema evolution detection**  
  Detects warehouse–model drifts, identifies breaking changes.

- **Data Quality & Metadata Rule Engine**  
  Rule-based validation directly inside the load pipeline (nullability, domains, patterns, etc.).

- **Execution plan transparency**  
  Refine execution plan annotations (polish)  

- **Column-level lineage & impact analysis**  
  Rich dependency graphing and change-impact visibility.

- **Developer tooling & debugger**  
  Deep SQL preview, AST inspection, execution diagnostics, step-wise load traceability.

- **Optional: simplified steward interface**  
  Lightweight UI for business/data owners to view datasets and rules.

- **Extended schema evolution & drift detection**  
  Detection and governance of breaking schema changes, including type changes  
  and destructive modifications.   
  Deterministic synchronization of physical warehouse schemas based on metadata:  
  - safe table provisioning  
  - additive column evolution  
  - rename-aware planning  
  - non-destructive incremental execution

- **Safe dataset & column renames**  
  First-class rename semantics via metadata:  
  - former_names tracking  
  - lineage-aware physical renames  
  - ambiguity detection and guardrails

**Intent:**  
elevata becomes **governable, productive, and capable of sourcing its own data**.  

The 0.7.x line focuses on completing and extending these capabilities.

---

### v0.8.x — Platform Orchestration Layer
> *Guiding question: Can elevata orchestrate itself reliably at scale?*  

- **Warehouse-native task orchestration**  
(retries, idempotency, scheduling)  

- **Dependency graph–driven pipeline execution**  
with parallelization and batching  

- **Multi-dataset execution with failure handling strategies**  

- **Integrations with orchestration frameworks**  
(Airflow, Dagster, Prefect)

- **Extended execution monitoring**  
(latency, throughput, volume, change rates)  

**Intent:**  
elevata becomes a **self-contained data platform core**, orchestrable without external wrappers.

---

### Future Directions (Post-0.8)
> *Long-term ambitions and ecosystem expansion.*  

- **Bizcore as Data Product Layer**  
  Elevate Bizcore datasets from technical projections to first-class data products.  

- **Business logic & semantic modeling**  
  Explicit modeling of business rules, derivations, and analytical intent  
  at the business domain level.  

- **Product-level metadata & ownership**  
  Ownership, contracts, and usage semantics for business-facing datasets.  

- **Explicit separation of business and presentation layers**  
  Clear distinction between business logic (Bizcore) and consumption-oriented  
  presentation layers (Serving), enabling tool-specific semantic models  
  without polluting core business datasets.  

- **Optional metrics & analytical abstractions**  
  Foundations for a native metrics layer and reusable analytical definitions.  

- **Extended catalog capabilities**  
  (contracts, schema registry, dataset capabilities)  

- **Additional dialects and warehouse platforms**  
  e.g. Snowflake, BigQuery, Databricks SQL, Microsoft Fabric  

- **Warehouse-native metadata and observability features**

---

### ✨ Design Principles Reflected in the Roadmap

- **Metadata-first:** SQL, historization, incremental logic and execution are derived from declarative definitions.  

- **Deterministic & lineage-aware:** Every transformation is predictable and auditable.  

- **Warehouse-native:** elevata optimizes for SQL systems and treats the warehouse as the execution environment.  

- **Extensible:** Dialects, rules, orchestrators and catalog integrations can grow as the platform evolves.

---

## 🛡️ Data Privacy (GDPR/DSGVO)

elevata itself does not require personal data.  
If used with customer datasets, responsibility for compliance remains with the implementing organisation.  
The system supports pseudo-key hashing and consistent anonymisation strategies via its hashing DSL.

---

### 🚧 Note

elevata is currently in an early preview phase (v0.x).
Breaking changes may occur before the 1.0 release as the metadata model stabilizes.

---

## Disclaimer

This project is an independent open-source initiative.  
- It is not a consulting service.  
- It is not a customer project.  
- It does not store or process customer data.  
- It is not in competition with any company.  

The purpose of elevata is to contribute to the community by providing a metadata-centric framework for building data platforms.  
The project is published under the AGPL v3 license and open for use by any organization.

---

## 🧾 License & Notices

© 2025 Ilona Tag — All rights reserved.  
**elevata™** is an open-source software project for data & analytics innovation.  
The name *elevata* is currently under trademark registration with the German Patent and Trade Mark Office (DPMA).  
Other product names, logos, and brands mentioned here are property of their respective owners.

Released under the **GNU Affero General Public License v3 (AGPL-3.0)**.  
See [`LICENSE`](https://github.com/elevata-labs/elevata/blob/main/LICENSE) for terms and [`NOTICE.md`](https://github.com/elevata-labs/elevata/blob/main/NOTICE.md) for third-party license information.