# elevata®

<p align="center">
  <img src="https://raw.githubusercontent.com/elevata-labs/elevata/main/docs/elevata_logo.png" alt="elevata logo" width="130"/>
</p>

**elevata® is an Architecture Runtime for modern data platforms.**

It turns **metadata into deterministic, executable data architecture**.
Architecture is defined declaratively and executed consistently across warehouses.

SQL becomes an artifact. Architecture becomes metadata.

---

## ⚡ What elevata enables

The same metadata-defined architecture can be rendered, reviewed, and executed consistently on:

Snowflake · Databricks · Fabric · MSSQL · Postgres · DuckDB · BigQuery

without rewriting logic or introducing dialect-specific modeling.

elevata separates:

- **Logical architecture**  
- **Dialect rendering**  
- **Execution backend**

This makes data architecture portable, reproducible, governable, and auditable.

## License & Dependencies

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://github.com/elevata-labs/elevata/blob/main/LICENSE)
[![Built with Django](https://img.shields.io/badge/Built%20with-Django-092E20?logo=django)](https://www.djangoproject.com/)
[![Frontend: HTMX](https://img.shields.io/badge/Frontend-HTMX-3366CC?logo=htmx)](https://htmx.org/)
[![UI: Bootstrap 5](https://img.shields.io/badge/UI-Bootstrap%205-7952B3?logo=bootstrap)](https://getbootstrap.com/)

---

## 🧭 What is elevata?

elevata is an **Architecture Runtime** for metadata-defined data platforms.

It models datasets, lineage, contracts, governance, and execution semantics as explicit metadata.

From these definitions, elevata derives deterministic logical plans, renders dialect-owned SQL, reviews architecture changes, and executes warehouse-native pipelines through controlled runtime scopes.

Schema evolution, incremental loads, historization, approvals, and execution evidence are planned, validated, and applied deterministically.

<p align="center">
  <img src="https://raw.githubusercontent.com/elevata-labs/elevata/main/docs/elevata_v2_8_0.png" alt="elevata UI preview" width="900"/>
  <br/>
  <em>Architecture Runtime UI for discovering, controlling, modeling, and executing metadata-defined data architecture</em>
</p>


## ✨ Why elevata is different

Most data platforms encode architecture implicitly in SQL and pipeline code.

elevata makes architecture explicit.

- Metadata defines behavior.  
- Dialects own SQL shape.  
- Control makes changes reviewable and auditable.  
- Execution is deterministic, observable, and evidence-based.

The result is governed, explainable, portable, and executable data architecture.

---

> *Modern data platforms often fail not because of missing tools, but because*  
> *architecture, lineage, and governance are encoded implicitly in SQL and pipeline code.*  
> *elevata exists to make these concerns explicit, declarative, and reproducible.*

---

## 📖 Architecture Runtime Publications

The Architecture Runtime concept behind elevata is described in the publication section:

- [Architecture Runtime Manifesto](https://github.com/elevata-labs/elevata/blob/main/publications/architecture-runtime-publication/architecture-runtime-manifesto.md)  
- [Why Modern Data Platforms Need an Architecture Runtime](https://github.com/elevata-labs/elevata/blob/main/publications/architecture-runtime-publication/architecture-runtime-essay.md)

These publications explain why modern data platforms need metadata-defined, deterministic, controllable, executable, and auditable architecture.

---

## 🧩 Architecture Overview

The core elevata execution pipeline consists of four explicitly separated layers:

1. **Metadata Model**  
2. **Deterministic Logical Plan**  
3. **Dialect Rendering**  
4. **Warehouse-Native Execution**

Each layer is explicitly separated.

---

## 📚 Example Workflow

1. Define datasets, lineage, contracts, and execution semantics in metadata  
2. Discover architecture through Catalog, Data Products, Portfolio, Insights, and Maps  
3. Inspect generated SQL, lineage, contracts, health, and execution evidence  
4. Review architecture changes through Architecture Review Briefing and approve them through Architecture Control  
5. Execute approved or unchanged scopes deterministically on your target warehouse  
6. Audit execution through Architecture Execution Records

---

## 💻 Execution

Pipelines are executed dataset-driven and lineage-aware.

Execution supports full and incremental loads, historization, schema evolution, and structured load logging.

Behavior is deterministic and observable.

Schema drift is reconciled through Architecture MigrationPlan-driven materialization:  
renames, adds, type evolution and controlled rebuilds are derived from architecture state, while destructive changes remain explicitly policy-gated.

---

## 🧭 Architecture Catalog

elevata provides a read-only Architecture Catalog for discovering metadata-defined
executable architecture.

The Catalog shows what exists, how datasets are defined, how they are connected, how they are controlled, where execution evidence is available, how portfolio posture looks, and which architecture quality and governance signals need attention.

Users can search and filter TargetDatasets by schema, owner, lifecycle status, system-managed status, materialization type, incremental strategy and query logic.

Catalog detail pages summarize architecture metadata, ownership, health, upstream inputs, downstream consumers, column contract signals and the latest Architecture Execution Record for the dataset scope.

Catalog Portfolio summarizes architecture posture across readiness, ownership, contracts, health, review state, execution evidence and layer distribution. Actionable Portfolio KPIs open filtered Catalog worklists so users can inspect affected datasets before navigating to dataset detail or Architecture Control.

Catalog Data Products show which serving-layer datasets are ready for trusted consumption. Readiness is derived from ownership, metadata health, query contracts, lineage, Architecture Control review state and execution evidence.

Catalog Insights highlight ownership gaps, metadata health findings, custom query logic, downstream consumer visibility, inactive datasets with consumers, missing execution evidence, and dataset-specific Architecture Control review status summaries.

Catalog Maps show architecture across layers using layer cards, a layer flow overview, a source-to-target layer dependency matrix and expandable direct dependency examples.

The Catalog does not edit metadata and does not execute loads. Architecture Control remains responsible for approval, execution, execution records and retention workflows.

---

## 🧭 Architecture Control

elevata makes architecture changes reviewable before execution.

Architecture State, Change Reports, Promotion Reports, Approval Artifacts and Execution Records expose deterministic fingerprints, MigrationPlan actions, policy decisions, review decisions and execution outcomes.

This supports controlled review, CI checks and environment-to-environment architecture promotion while keeping execution guardrails inside the load runner.

The Architecture Control UI makes approval state, scope, policy status, change summary, execution preview, dependency mode, captured output and execution records visible for controlled scopes.

Architecture Review Briefing adds compact reviewer guidance directly inside Architecture Control. It summarizes the selected scope, review state, change volume, policy attention, destructive or blocking signals, execution readiness and suggested reviewer focus before approval or execution.

Users can inspect reports, open the Review Briefing details on demand, download report JSON, create Approval Artifacts, verify approvals, execute approved or no-change scopes, inspect the resulting Architecture Execution Record, review stored execution history, download record JSON, and apply execution record retention.


---

## 📐 Query Builder

elevata models transformations explicitly using **Query Trees**.

Each TargetDataset may define a query tree composed of well-defined operators such as SELECT, JOIN, AGGREGATE, UNION and WINDOW. These operators are represented as metadata objects, not as opaque SQL fragments.

The Query Builder produces deterministic SQL with stable contracts, field-level lineage, and transparent query semantics.

---

## 🏷️ Metadata Naming Guidance

elevata supports deterministic Metadata Naming Guidance while editing TargetColumns.

Guidance is derived from existing column mappings and previously used target names. It helps modelers reuse project-specific naming decisions without AI, without a global dictionary and without enforcing naming rules.

For direct source inputs, elevata uses the technical SourceColumn name. For upstream target inputs, it uses the immediate upstream TargetColumn name, so guidance follows the current modeling step instead of tracing back to the original source-system field.

Naming Guidance focuses on rawcore and bizcore technical naming decisions. Serving-layer friendly names and historized rawcore datasets are excluded from recommendation evidence.

Recommended names can be applied directly from the TargetColumn inline editor, but they remain advisory. Existing validation, collision checks and rename handling stay authoritative.

---

## 🔮 Roadmap

elevata evolves along four strategic axes:

**1. Architecture Catalog & Portfolio**  
Making executable architecture discoverable across datasets, lineage, contracts, ownership, readiness, health, and execution evidence.

**2. Architecture Control & Auditability**  
Strengthening review briefing, approval, execution evidence, promotion, retention, and controlled runtime operation.

**3. Source Abstraction & Ingestion**  
Expanding source patterns such as files, APIs, cloud transports, and federated access while preserving deterministic RAW and Stage semantics.
 
**4. Runtime Hardening & Execution Semantics**  
Improving backend coverage, dialect behavior, schema evolution safety, and reproducible execution across supported warehouses.

See `/docs` for architectural depth.

---

### ♟️ Architecture & Strategy

For a deeper architectural and strategic overview of elevata’s direction,
see the [elevata Platform Strategy](https://github.com/elevata-labs/elevata/blob/main/docs/strategy/elevata_platform_strategy.md).

---

## 🛡️ Data Privacy (GDPR/DSGVO)

elevata itself does not require personal data.  
If used with customer datasets, responsibility for compliance remains with the implementing organisation.  
The system supports pseudo-key hashing and consistent pseudonymisation-oriented strategies via its hashing DSL.

---

## Disclaimer

This project is an independent open-source initiative.

- It is not a consulting service.  
- It is not a customer project.  
- It does not store or process customer data.  
- It is not in competition with any company.  

The purpose of elevata is to contribute to the community by providing an Architecture Runtime for metadata-defined data platforms.

The project is published under the AGPL v3 license and open for use by any organization.

---

## 🧾 License & Trademark Notice

© 2025-2026 Ilona Tag - All rights reserved.  
**elevata®** is an open-source software project for metadata-defined data architecture.

elevata® is a registered trademark in Germany.  
Other product names, logos, and brands mentioned here are property of their respective owners.

Released under the **GNU Affero General Public License v3 (AGPL-3.0)**.  
See [`LICENSE`](https://github.com/elevata-labs/elevata/blob/main/LICENSE) for terms and [`NOTICE.md`](https://github.com/elevata-labs/elevata/blob/main/NOTICE.md) for third-party license information.