# elevata

<p align="center">
  <img src="https://raw.githubusercontent.com/elevata-labs/elevata/main/docs/logo.png" alt="elevata logo" width="130"/>
</p>

**elevata** is an independent open-source project aiming to make modern data platforms radically simpler.  
It’s designed as a **Declarative Data Architecture & Metadata Framework** — automated, governed, and platform-agnostic.

Instead of manually crafting endless SQL and pipeline code, elevata lets metadata do the work.  
By defining datasets, lineage, and transformation logic declaratively, you can generate consistent, auditable, and future-proof data models — including schema evolution and physical execution — ready to run on your preferred platform.

## License & Dependencies

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://github.com/elevata-labs/elevata/blob/main/LICENSE)
[![Built with Django](https://img.shields.io/badge/Built%20with-Django-092E20?logo=django)](https://www.djangoproject.com/)
[![Frontend: HTMX](https://img.shields.io/badge/Frontend-HTMX-3366CC?logo=htmx)](https://htmx.org/)
[![UI: Bootstrap 5](https://img.shields.io/badge/UI-Bootstrap%205-7952B3?logo=bootstrap)](https://getbootstrap.com/)  

---

## 🧭 What is elevata?

**elevata** transforms metadata into architecture.  
It reads source system metadata, derives logical and physical target structures, and enforces consistent, privacy-compliant data models — automatically.  

elevata transforms metadata into architecture — **from raw ingestion to business-ready, metadata-defined data products**.

It codifies architectural best practices, generates high-quality SQL, and provides a declarative way to build  
**RAW → STAGE → CORE → BIZCORE → SERVING** pipelines.

Business semantics are modeled as **first-class metadata**, not as BI-layer abstractions.

elevata executes data pipelines in a **dataset-driven** and **lineage-aware** manner. Each execution resolves dependencies automatically and processes datasets in the correct semantic order, rather than layer by layer.

The goal:  
💡 **Turn metadata into executable, dialect-aware SQL pipelines** — reliably, transparently, and with full lineage.

Where most tools stop at SQL generation, elevata goes further:  
it defines **how a modern data architecture should look** — opinionated, governed, reproducible.  
*In other words: elevata brings structure, governance, and automation to modern data platforms — from metadata to SQL.*  

elevata is designed for data engineers and architects who want to build governed, reproducible data platforms without hard-coding architecture into pipelines.

Unlike transformation-centric tools, elevata treats metadata, lineage, and execution semantics as first-class concepts, not as conventions embedded in SQL.


<p align="center">
  <img src="https://raw.githubusercontent.com/elevata-labs/elevata/main/docs/elevata_v0_9_0.png" alt="elevata UI preview" width="700"/>
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
- batch-level execution snapshots via `meta.load_run_snapshot`

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

Execution behavior is fully deterministic and observable.  
Each run produces a structured execution log and an optional
batch-level execution snapshot explaining plan, policy, and outcomes.

---

## 🔮 Roadmap

elevata is evolving from a SQL-generation layer into a **metadata-driven, warehouse-native data platform engine**.

The roadmap reflects this direction: structured, ambitious, and aligned with elevata’s long-term vision.

---

### v0.9.x — Business Semantics & Bizcore Layer
> *Guiding question: Can business meaning and business logic be modeled explicitly — without introducing a semantic BI layer?*

elevata includes a dedicated **BIZCORE layer** for modeling business meaning, rules, and calculations as  
**first-class metadata** — executed through the same deterministic planning and execution pipeline  
as technical datasets.

#### ✅ Already shipped in v0.9.0
- **UI support for building Bizcore datasets and columns**  
- **Join semantics for multi-upstream Bizcore datasets**  
- **Deterministic SQL preview and execution**  
- **Lineage-driven qualification and expression handling**  
- **Core → Bizcore traceability**

#### 🔜 Planned within v0.9.x
- **Expanded business rule expressiveness**  
  - derived business fields  
  - rule-based classifications  
  - KPI-style calculations expressed as dataset fields  
- **Additional validation & governance rules for Bizcore**  
  - join completeness & ambiguity detection  
  - expression validation  
  - schema & naming conventions  
- **Cross-Bizcore dependency patterns**  
  - Bizcore-on-Bizcore dependencies with deterministic execution  
- **Enhanced impact analysis & documentation tooling**  
  - structured field-level lineage and semantic dependency inspection

#### ❌ Explicit non-goals (by design)
- No BI semantic layer  
- No metric store or query-time metric resolution  
- No time-intelligence abstractions  
- No dbt-style macro or templating system

**Intent**  
elevata becomes **business-capable by design**, allowing teams to define  
business logic and KPIs natively — while deliberately avoiding  
tool-specific semantic layers or BI-driven abstractions.

---

### v1.0.x — First official release
> *Guiding question: Can execution be governed, validated, and integrated without breaking determinism?*

- **Run- and dataset-level governance rules**  
  - schema drift detection  
  - delete detection  
  - retry and fail-fast policies  
- **Rule-based validation framework**  
  - metadata-defined checks  
  - blocking vs non-blocking violations  
- **Execution hooks & lifecycle callbacks**  
  - stable integration API for Airflow, Dagster, Prefect, etc.  
- **Policy-aware execution outcomes**  
  - clear distinction between execution failures and policy violations  
- **First-class execution metadata**  
  - structured access to execution logs and snapshots

---

### Vision (1.0 onwards)

elevata aims to become a **metadata-native data platform engine**:  
a system where structure, execution, governance, and business intent  
are derived from explicit definitions rather than implicit SQL behavior.

The long-term goal is not to replace orchestration frameworks or BI tools,  
but to act as a **reliable, transparent backbone** that makes data pipelines  
predictable, governable, and evolvable across teams and platforms.

---

### ✨ Design Principles Reflected in the Roadmap

- **Metadata-first:** SQL, historization, incremental logic and execution are derived from declarative definitions.  

- **Deterministic & lineage-aware:** Every transformation is predictable and auditable.  

- **Warehouse-native:** elevata optimizes for SQL systems and treats the warehouse as the execution environment.  

- **Extensible:** Dialects, rules, orchestrators and catalog integrations can grow as the platform evolves.

- **Explainable by design:** Execution decisions, failures, and outcomes are observable and reproducible.

---

### ♟️ Architecture & Strategy

For a deeper architectural and strategic overview of elevata’s direction,
see the [elevata Platform Strategy](https://github.com/elevata-labs/elevata/blob/main/docs/strategy/elevata_platform_strategy.md).

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

© 2025-2026 Ilona Tag — All rights reserved.  
**elevata™** is an open-source software project for data & analytics innovation.  
The name *elevata* is currently under trademark registration with the German Patent and Trade Mark Office (DPMA).  
Other product names, logos, and brands mentioned here are property of their respective owners.

Released under the **GNU Affero General Public License v3 (AGPL-3.0)**.  
See [`LICENSE`](https://github.com/elevata-labs/elevata/blob/main/LICENSE) for terms and [`NOTICE.md`](https://github.com/elevata-labs/elevata/blob/main/NOTICE.md) for third-party license information.