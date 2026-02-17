# elevata

<p align="center">
  <img src="https://raw.githubusercontent.com/elevata-labs/elevata/main/docs/logo.png" alt="elevata logo" width="130"/>
</p>

**elevata** is an independent open-source project aiming to make modern data platforms radically simpler.  
It’s designed as a **Declarative Data Architecture & Metadata Framework** — automated, governed, and platform-agnostic.

Instead of manually crafting endless SQL and pipeline code, elevata lets metadata do the work.  
By defining datasets, lineage, and transformation logic declaratively, you can generate consistent, auditable,  
and future-proof data models — including schema evolution and physical execution — ready to run on your preferred platform.

**elevata defines data platform architecture in metadata and executes it deterministically across warehouse engines —  
allowing architecture to be defined once and executed consistently everywhere.**

---

## ⚡ What elevata enables

With elevata, the same metadata-defined architecture can be executed consistently across modern warehouse engines such as:

- Snowflake  
- Databricks SQL (Unity Catalog)  
- Microsoft Fabric Warehouse  
- PostgreSQL  
- DuckDB  
- Google BigQuery

without rewriting models or introducing platform-specific logic.

SQL becomes an execution artifact — not the architectural source of truth.

Unlike many data tools that rely on adapter-specific behavior or runtime SQL rewriting, elevata derives execution  
deterministically from metadata contracts and logical planning.  
The same metadata definition therefore produces consistent results across platforms.

## License & Dependencies

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://github.com/elevata-labs/elevata/blob/main/LICENSE)
[![Built with Django](https://img.shields.io/badge/Built%20with-Django-092E20?logo=django)](https://www.djangoproject.com/)
[![Frontend: HTMX](https://img.shields.io/badge/Frontend-HTMX-3366CC?logo=htmx)](https://htmx.org/)
[![UI: Bootstrap 5](https://img.shields.io/badge/UI-Bootstrap%205-7952B3?logo=bootstrap)](https://getbootstrap.com/)  

---

## 🧭 What is elevata?

**elevata** transforms metadata into architecture.  
It reads source system metadata, derives logical and physical target structures, and enforces consistent, privacy-compliant data models — automatically.  

From raw ingestion to business-ready data products, elevata derives architecture directly from metadata.

It codifies architectural best practices, generates high-quality SQL, and provides a declarative way to build  
**RAW → STAGE → CORE → BIZCORE → SERVING** pipelines.

Business semantics are modeled as **first-class metadata**, not as BI-layer abstractions.

elevata executes data pipelines in a **dataset-driven** and **lineage-aware** manner. Each execution resolves dependencies automatically and processes datasets in the correct semantic order, rather than layer by layer.

The goal:  
💡 **Turn metadata into executable, dialect-aware SQL pipelines** — reliably, transparently, and with full lineage.

Where most tools stop at SQL generation, elevata goes further:  
it defines **how a modern data architecture should look** — opinionated, governed, reproducible.  
*In other words: elevata brings structure, governance, and automation to modern data platforms — from metadata to SQL.*  

elevata is designed for teams that want governed, predictable, explainable data pipelines —  
without hiding logic behind implicit SQL behavior.

Unlike transformation-centric tools, elevata treats metadata, lineage, and execution semantics as first-class concepts,  
not as conventions embedded in SQL.

elevata emphasizes deterministic execution semantics.

Schema evolution, materialization changes, and execution safety are validated before execution begins,  
ensuring predictable behavior across environments and supported warehouse platforms.



<p align="center">
  <img src="https://raw.githubusercontent.com/elevata-labs/elevata/main/docs/elevata_v1_3_0.png" alt="elevata UI preview" width="700"/>
  <br/>
  <em>Dataset detail view with lineage, metadata, and dialect-aware SQL previews</em>
</p>


## ✨ Why elevata is different

Most data tools treat SQL as the primary source of truth.

Architecture, execution behaviour and governance often emerge implicitly
from pipelines, macros or platform-specific implementations.

elevata takes a different approach.

Architecture is defined explicitly through metadata:  
- datasets describe behaviour  
- lineage describes dependencies  
- governance rules are part of the model  
- SQL is generated deterministically from these definitions

This allows elevata to separate:

- **what** a data platform should do (logical model)  
- **how** SQL is rendered (dialect)  
- **where and how** execution happens (execution engine)

As a result, the same architecture can run consistently across different  
platforms such as Snowflake, Databricks or Microsoft Fabric without rewriting logic.

SQL becomes an artifact — not the architecture itself.

Define architecture once in metadata — elevata generates deterministic SQL and runs it across warehouses consistently.

---

**elevata** uses Django models to define:

- **datasets** (sources and targets: raw, stage, rawcore, bizcore, serving)
- **business keys, surrogate keys, foreign keys**
- **column expressions**
- **incremental strategies**
- **source systems & target systems**
- **dependencies between datasets**
- **joins between datasets**

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

All generated SQL can be executed deterministically against the configured target system across multiple engines,  
including incremental loads, merges, historization, and schema synchronization.    
This allows the same metadata model to be executed on Snowflake, Databricks, Fabric Warehouse,  
Postgres, or DuckDB without changing any dataset definitions.

> *Modern data platforms often fail not because of missing tools, but because*  
> *architecture, lineage, and governance are encoded implicitly in SQL and pipeline code.*  
> *elevata exists to make these concerns explicit, declarative, and reproducible.*

---

## 🧩 Architecture Overview

elevata is a metadata-driven SQL generation engine for building deterministic,  
warehouse-native data pipelines.  
It manages the full lifecycle from metadata to SQL generation to execution — including  
incremental loads, historization, schema evolution, and observability.

At its core, elevata provides a first-class **Query Builder**  
that allows complex transformations to be modeled declaratively  
using structured metadata instead of handwritten SQL.

The Query Builder is not a visual abstraction layer, but a deterministic planning system  
that produces fully explainable SQL based on:

- explicit query trees (select, join, aggregate, union, window)  
- schema-aware governance rules  
- lineage-driven column resolution  
- stable query contracts

> *The Query Builder is the most opinionated and distinctive part of elevata.*

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
Dialect-specific renderers and execution adapters for:  
- BigQuery  
- Databricks (Unity Catalog + SQL Warehouse)  
- DuckDB  
- Microsoft Fabric Warehouse  
- MSSQL  
- Postgres  
- Snowflake

They handle:  
- SQL syntax differences  
- hashing and surrogate key generation  
- quoting and identifier rules  
- MERGE vs UPDATE/INSERT fallbacks  
- target-specific execution semantics

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

## 💻 Load Runner CLI

elevata includes a dataset-driven load runner (`elevata_load`) that executes  
pipelines in dependency order on the configured target system.

Execution semantics depend on the target dataset, its layer,
and the configured execution backend:

- SQL execution for Stage, Rawcore and downstream layers  
- Ingestion logic for Raw datasets  

The load runner supports dry runs, execution diagnostics, dependency resolution,
and execution logging.

Execution behavior is fully deterministic and observable.  
Each run produces a structured execution log and an optional
batch-level execution snapshot explaining plan, policy, and outcomes.

---

## 📐 Query Builder & Query Trees

elevata models transformations explicitly using **Query Trees**.

Each TargetDataset may define a query tree composed of well-defined  
operators such as SELECT, JOIN, AGGREGATE, UNION and WINDOW.  
These operators are represented as metadata objects, not as opaque SQL fragments.

The Query Builder derives executable SQL from this tree in a fully deterministic way.  
This enables:

- reliable SQL previews  
- stable query contracts  
- field-level lineage inspection  
- safe evolution of datasets over time

Query Trees are validated against schema-specific governance rules and downstream  
dependencies before execution.

---

## 🚀 elevata 1.0 — Stable Foundation

With version **1.0**, elevata reaches a major milestone.

This release establishes a **stable, backward-compatible core** for
metadata-driven data platform engineering, including:

- Deterministic SQL planning and generation  
- Explicit dataset modeling with lineage and contracts  
- Query Trees as a first-class metadata model for controlled query logic  
- A metadata-native Query Builder for authoring, inspecting and governing query trees  
- Business semantics via Bizcore datasets (joins, aggregations, rules)  
- Policy-enforced governance and validation

From this release onward, elevata preserves compatibility of
its core metadata models and execution semantics.

---

## 🔮 Roadmap

The roadmap reflects the current development direction of elevata.  
It focuses on architectural depth and practical usability rather than feature breadth.

### Near-term focus — Ingestion Framework

The next development focus expands ingestion capabilities while preserving
deterministic execution semantics.

Planned areas:

- REST API ingestion framework  
- Flat file ingestion with fault-tolerant loading  
- JSON → relational flattening in the Raw layer  
- ingestion validation and low-level data quality safeguards  
- incremental ingestion cursor abstractions

The goal is to make elevata immediately applicable in real-world projects  
where heterogeneous sources are common.

---

### Mid-term focus — Metadata Governance

Once ingestion capabilities are expanded, the next focus area is governance  
and long-term metadata stability.

Planned areas:

- contract breaking change detection  
- dataset semantic versioning  
- lineage completeness validation  
- reproducible execution snapshots

This builds directly on the deterministic execution and schema evolution model.

---

### Long-term direction — Performance Architecture

Future work will focus on performance optimization and adaptive execution.

Planned areas:

- staged ingestion strategies  
- adaptive materialization strategies  
- warehouse-specific optimization layers

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