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

The same metadata-defined platform can run consistently on:

Snowflake · Databricks · Fabric · MSSQL · Postgres · DuckDB · BigQuery

without rewriting logic or introducing dialect-specific modeling.

elevata separates:

- **Logical architecture**  
- **Dialect rendering**  
- **Execution backend**

This makes data architecture portable, reproducible, and governable.

## License & Dependencies

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://github.com/elevata-labs/elevata/blob/main/LICENSE)
[![Built with Django](https://img.shields.io/badge/Built%20with-Django-092E20?logo=django)](https://www.djangoproject.com/)
[![Frontend: HTMX](https://img.shields.io/badge/Frontend-HTMX-3366CC?logo=htmx)](https://htmx.org/)
[![UI: Bootstrap 5](https://img.shields.io/badge/UI-Bootstrap%205-7952B3?logo=bootstrap)](https://getbootstrap.com/)  

---

## 🧭 What is elevata?

elevata is a **metadata-first** data platform engine.

It models datasets, lineage, governance, and execution semantics declaratively.

From these definitions, elevata derives deterministic logical plans, renders dialect-owned SQL,  
and executes warehouse-native pipelines.

Schema evolution, incremental loads and historization are planned,  
validated, and applied deterministically before execution.


<p align="center">
  <img src="https://raw.githubusercontent.com/elevata-labs/elevata/main/docs/elevata_v2_7_0.png" alt="elevata UI preview" width="900"/>
  <br/>
  <em>Architecture Runtime UI for discovering, controlling, modeling, and executing metadata-defined data architecture</em>
</p>


## ✨ Why elevata is different

Most data platforms encode architecture implicitly in SQL and pipeline code.

elevata makes architecture explicit.

- Metadata defines behavior.  
- Dialects own SQL shape.  
- Execution is deterministic and observable.

The result is governed, explainable, and portable data architecture.

---

elevata models datasets, lineage, keys, and execution semantics declaratively.

From this metadata, it derives deterministic logical plans and renders dialect-owned SQL.

The same architecture executes across supported warehouses without changing dataset definitions.

> *Modern data platforms often fail not because of missing tools, but because*  
> *architecture, lineage, and governance are encoded implicitly in SQL and pipeline code.*  
> *elevata exists to make these concerns explicit, declarative, and reproducible.*

---

## 🧩 Architecture Overview

elevata consists of four layers:

1. **Metadata Model**  
2. **Deterministic Logical Plan**  
3. **Dialect Rendering**  
4. **Warehouse-Native Execution**

Each layer is explicitly separated.

---

## 📚 Example Workflow

1. Define datasets and lineage in metadata  
2. Inspect generated SQL and lineage  
3. Execute pipelines deterministically on your target warehouse

---

## 💻 Execution

Pipelines are executed dataset-driven and lineage-aware.

Execution supports full and incremental loads, historization,  
schema evolution, and structured load logging.

Behavior is deterministic and observable.

Schema drift is reconciled through Architecture MigrationPlan-driven materialization:  
renames, adds, type evolution and controlled rebuilds are derived from architecture state,  
while destructive changes remain explicitly policy-gated.

---

## 🧭 Architecture Catalog

elevata provides a read-only Architecture Catalog for discovering metadata-defined
executable architecture.

The Catalog shows what exists, how datasets are defined, how they are connected,  
how they are controlled, where execution evidence is available, how portfolio posture looks,  
and which architecture quality and governance signals need attention.

Users can search and filter TargetDatasets by schema, owner, lifecycle status,  
system-managed status, materialization type, incremental strategy and query logic.

Catalog detail pages summarize architecture metadata, ownership, health, upstream inputs,  
downstream consumers, column contract signals and the latest Architecture Execution Record  
for the dataset scope.

Catalog Portfolio summarizes architecture posture across readiness, ownership, contracts,  
health, review state, execution evidence and layer distribution. Actionable Portfolio KPIs  
open filtered Catalog worklists so users can inspect affected datasets before navigating to  
dataset detail or Architecture Control.

Catalog Data Products show which serving-layer datasets are ready for trusted consumption.  
Readiness is derived from ownership, metadata health, query contracts, lineage, Architecture  
Control review state and execution evidence.

Catalog Insights highlight ownership gaps, metadata health findings, custom query logic,  
downstream consumer visibility, inactive datasets with consumers, missing execution evidence,  
and dataset-specific Architecture Control review status summaries.

Catalog Maps show architecture across layers using layer cards, a layer flow overview,  
a source-to-target layer dependency matrix and expandable direct dependency examples.

The Catalog does not edit metadata and does not execute loads. Architecture Control remains  
responsible for approval, execution, execution records and retention workflows.

---

## 🧭 Architecture Control

elevata makes architecture changes reviewable before execution.

Architecture State, Change Reports, Promotion Reports, Approval Artifacts and Execution Records  
expose deterministic fingerprints, MigrationPlan actions, policy decisions, review decisions and execution outcomes.

This supports controlled review, CI checks and environment-to-environment architecture promotion  
while keeping execution guardrails inside the load runner.

The Architecture Control UI makes approval state, scope, policy status, change summary,  
execution preview, dependency mode, captured output and execution records visible for controlled scopes.

Users can inspect reports, download report JSON, create Approval Artifacts, verify approvals,  
execute approved or no-change scopes, inspect the resulting Architecture Execution Record,  
review stored execution history, download record JSON, and apply execution record retention.

---

## 📐 Query Builder

elevata models transformations explicitly using **Query Trees**.

Each TargetDataset may define a query tree composed of well-defined  
operators such as SELECT, JOIN, AGGREGATE, UNION and WINDOW.  
These operators are represented as metadata objects, not as opaque SQL fragments.

The Query Builder models transformations explicitly using structured metadata.

It produces deterministic SQL with stable contracts and field-level lineage.

---

## 🏷️ Metadata Naming Guidance

elevata supports deterministic Metadata Naming Guidance while editing TargetColumns.

Guidance is derived from existing column mappings and previously used target names.  
It helps modelers reuse project-specific naming decisions without AI, without a global dictionary  
and without enforcing naming rules.

For direct source inputs, elevata uses the technical SourceColumn name.  
For upstream target inputs, it uses the immediate upstream TargetColumn name, so guidance follows  
the current modeling step instead of tracing back to the original source-system field.

Naming Guidance focuses on rawcore and bizcore technical naming decisions.  
Serving-layer friendly names and historized rawcore datasets are excluded from recommendation evidence.

Recommended names can be applied directly from the TargetColumn inline editor, but they remain advisory.  
Existing validation, collision checks and rename handling stay authoritative.

---

## 🔮 Roadmap

elevata evolves along three strategic axes:

**1. Ingestion & Source Abstraction**  
Expanding source patterns (files, APIs, cloud transports)  
while preserving deterministic RAW semantics.

**2. Metadata Governance & Contracts**  
Versioning, breaking-change detection, lineage validation  
and reproducible execution snapshots.

**3. Performance & Adaptive Execution**  
Warehouse-specific optimization layers and adaptive materialization strategies.

See `/docs` for architectural depth.

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

## 🧾 License & Trademark Notice

© 2025-2026 Ilona Tag — All rights reserved.  
**elevata®** is an open-source software project for data & analytics innovation.  

elevata® is a registered trademark in Germany.  
Other product names, logos, and brands mentioned here are property of their respective owners.

Released under the **GNU Affero General Public License v3 (AGPL-3.0)**.  
See [`LICENSE`](https://github.com/elevata-labs/elevata/blob/main/LICENSE) for terms and [`NOTICE.md`](https://github.com/elevata-labs/elevata/blob/main/NOTICE.md) for third-party license information.