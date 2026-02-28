# elevata

<p align="center">
  <img src="https://raw.githubusercontent.com/elevata-labs/elevata/main/docs/logo.png" alt="elevata logo" width="130"/>
</p>

**elevata** turns **metadata into deterministic, executable data architecture**.

It defines how modern data platforms should be modeled —  
and executes that architecture consistently across warehouses.

SQL is an artifact. Architecture is metadata.

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

Schema evolution, incremental loads and historization are
validated before execution.


<p align="center">
  <img src="https://raw.githubusercontent.com/elevata-labs/elevata/main/docs/elevata_v1_4_0.png" alt="elevata UI preview" width="700"/>
  <br/>
  <em>Dataset detail view with lineage, metadata, and dialect-aware SQL previews</em>
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

The same architecture can be executed across supported warehouses  
without changing dataset definitions.

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

---

## 📐 Query Builder

elevata models transformations explicitly using **Query Trees**.

Each TargetDataset may define a query tree composed of well-defined  
operators such as SELECT, JOIN, AGGREGATE, UNION and WINDOW.  
These operators are represented as metadata objects, not as opaque SQL fragments.

The Query Builder models transformations explicitly using structured metadata.

It produces deterministic SQL with stable contracts and field-level lineage.

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

## 🧾 License & Notices

© 2025-2026 Ilona Tag — All rights reserved.  
**elevata™** is an open-source software project for data & analytics innovation.  
The name *elevata* is currently under trademark registration with the German Patent and Trade Mark Office (DPMA).  
Other product names, logos, and brands mentioned here are property of their respective owners.

Released under the **GNU Affero General Public License v3 (AGPL-3.0)**.  
See [`LICENSE`](https://github.com/elevata-labs/elevata/blob/main/LICENSE) for terms and [`NOTICE.md`](https://github.com/elevata-labs/elevata/blob/main/NOTICE.md) for third-party license information.