# elevata

<p align="center">
  <img src="https://raw.githubusercontent.com/elevata-labs/elevata/main/docs/logo.png" alt="elevata logo" width="260"/>
</p>

**elevata** is an independent open-source project aiming to make modern data platforms radically simpler.  
It’s designed as a **Declarative Data Architecture & Metadata Framework** — automated, governed, and platform-agnostic.

Instead of manually crafting endless SQL and pipeline code, elevata lets metadata do the work.  
By defining datasets, lineage, and transformation logic declaratively, you can generate consistent, auditable, and future-proof data models — ready to run on your preferred platform.

## License & Dependencies

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://github.com/elevata-labs/elevata/blob/main/LICENSE)
[![Built with Django](https://img.shields.io/badge/Built%20with-Django-092E20?logo=django)](https://www.djangoproject.com/)
[![Frontend: HTMX](https://img.shields.io/badge/Frontend-HTMX-3366CC?logo=htmx)](https://htmx.org/)
[![UI: Bootstrap 5](https://img.shields.io/badge/UI-Bootstrap%205-7952B3?logo=bootstrap)](https://getbootstrap.com/)  

---

## 🧭 What is elevata?

**elevata** transforms metadata into architecture.  
It reads source system metadata, derives logical and physical target structures, and enforces consistent, privacy-compliant data models — automatically.

Where most tools stop at SQL generation, elevata goes further:  
it defines **how a modern data architecture should look** — opinionated, governed, and reproducible.  
*In other words: elevata brings structure, governance, and automation to modern data platforms — from metadata to SQL.*

<p align="center">
  <img src="https://raw.githubusercontent.com/elevata-labs/elevata/main/docs/elevata_ui_v0_4_0.png" alt="elevata UI preview" width="700"/>
</p>

---

## 💡 Philosophy & Design Principles

elevata is not a query builder — it is a **data architecture engine**.  
Its purpose is to take what is usually scattered across SQL scripts, YAML files, and undocumented conventions — and make it **explicit, governed, and automatable**.

| Principle | Description |
|------------|--------------|
| 🧭 **Opinionated by design** | elevata enforces clear best practices for how data platforms are structured — from `raw` to `serving`. It removes ambiguity, so every dataset has a defined place and purpose. |
| 🧠 **Metadata drives everything** | All logic lives in the metadata — datasets, keys, lineage, governance. This makes data architectures reproducible, transparent, and explainable. |
| 🧩 **Convention over configuration** | Instead of infinite options, elevata provides intelligent defaults. Teams can override them — but only when they truly need to. |
| 🔐 **Privacy by architecture** | Surrogate keys are generated through deterministic, pepper-based hashing. No lookups, no stored secrets, full DSGVO compliance. |
| 🧮 **Declarative, not imperative** | The user declares *what* should exist, not *how* to code it. elevata generates the optimal technical representation. |
| 🌍 **Tool independence** | External engines (like dbt) can consume elevata’s metadata, but elevata does not depend on them. It stands on its own — portable, transparent, future-proof. |
| 🪄 **Lineage as first-class metadata** | Relationships between datasets and columns are explicit — not inferred. This makes SQL generation explainable and auditable. |


> **In short:**  
> elevata does for data architecture what version control did for code —  
> it makes structure explicit, reproducible, and collaborative.

---

## ⚙️ Key Features & Capabilities

- Modular SQL dialect layer enabling backend-specific SQL generation (DuckDB today, Postgres/MSSQL/Snowflake upcoming)
- Automated generation of target datasets and columns 
  from imported metadata, including PK propagation and surrogate key creation  
- Deterministic, lookup-free surrogate keys (SHA-256 + runtime-loaded pepper)  
- Full lineage model for datasets, columns, and relationships  
- Lineage-aware target generation with dataset-level and column-level lineage
- Stable `lineage_key` across renames ensures idempotent regeneration
- Layer-specific input handling (Raw → Stage → Rawcore)
- Modular `apply_all()` generation with deterministic column ordering
- True lineage-based SQL Preview (UNION across multiple sources, field-level alignment)
- Multi-source unification via shared `target_short_name`  
- Integrated governance (sensitivity, ownership, access intent)  
- Optional SQL rendering layer (dbt-compatible, but not dependent)  
- Complete metadata persistence via Django ORM  

---

## 🧩 Architectural Layers

elevata defines and enforces a clean five-layer target architecture:

| Layer | Purpose |
|--------|----------|
| **raw** | Original landing of data, 1:1 from source (audit & compliance) |
| **stage** | Early technical normalization (flattening, type harmonization) |
| **rawcore** | Technically harmonized core (surrogate keys, dedup, historization) |
| **bizcore** | Business logic and truth layer — KPI-ready, regulated |
| **serving** | Consumption layer for analytics, dashboards, and ML models |

Each layer is represented as a `TargetSchema` with defined defaults for materialization, historization, and governance and participates in a complete lineage chain.  
From v0.3.0 onward, elevata maintains dataset- and column-level lineage between layers, forming the basis for its Logical SQL Preview engine.

The following examples illustrate how elevata translates its metadata model into deterministic and auditable SQL.

---

## 🧮 Example: Deterministic Surrogate Key Generation (Privacy by Design)  

*Surrogate keys in elevata are not random — they are deterministic, governed, and fully reproducible across systems.*
```
MANDT~100|KUNNR~4711|null_replaced
↓ (runtime pepper)
SHA-256 = sap_customer_key
```

- Unique and stable across systems  
- No lookup required for FK propagation  
- Fully DSGVO-compliant (hashed with runtime pepper)

---

## 🧠 Example: Lineage-based SQL Preview

elevata automatically generates SQL reflecting real upstream lineage.
For instance, a `stage` dataset combining two `raw` tables:

```sql
SELECT
  s1.businessentityid,
  s1.firstname,
  s1.middlename,
  s1.lastname,
  s1.title
FROM "raw"."raw_aw1_person" AS s1

UNION ALL

SELECT
  s2.businessentityid,
  s2.firstname,
  NULL AS middlename,
  s2.lastname,
  s2.title
FROM "raw"."raw_aw2_person" AS s2
```  
And a `rawcore` dataset built from the stage layer:

```sql
SELECT
  hash256(concat_ws('|', concat('businessentityid', '~', coalesce(s."businessentityid", 'null_replaced')), 'supersecretpeppervalue')) AS rc_aw_person_key,
  s."businessentityid" AS business_entity_id,
  s."firstname" AS first_name,
  s."lastname" AS last_name
FROM "stage"."stg_aw_person" AS s
```

---

## 🚀 Roadmap

### 🎯 Short Term (v0.4.x)
**Sharper insight and smarter previews**  
- Dynamic SQL previews with joins, filters, and contextual logic  
- Interactive lineage validation and graph exploration  
- Optimized UI for large-scale metadata models  
- First test suite for model generation and lineage integrity  

---

### ⚙️ Mid-Term (v0.5–0.6)
**From metadata to managed data**  
- Support for major backends (BigQuery, Databricks, Fabric, Snowflake, …)  
- Declarative export for CI/CD integration  
- Role-based governance and fine-grained permissions  
- Incremental refresh and data-change tracking for real workloads  

---

### 🌌 Long Term (v1.0+)
**From modeling to execution — elevata becomes a Lakehouse automation platform**  
- Automatic generation of **fully orchestratable SQL pipelines** from lineage graphs  
- Native orchestration via Airflow, Dagster, dbt-core, or the built-in elevata scheduler  
- End-to-end dataset materialization with dependency awareness  
- Metadata-driven optimization for cost, freshness, and reuse across the Lakehouse  

> **Vision:** elevata will bridge the gap between design and operation.  
> It will not only describe data — it will make data *move*.

---

📘 **More Documentation**  
See the [`/docs`](docs/) folder for in-depth setup and technical design notes:  
- [Getting Started Guide](docs/getting_started.md)  
- [SQL Rendering & Alias Conventions](docs/sql_rendering_conventions.md)  
- [Automatic Target Generation Logic](docs/generation_logic.md)  
- [Secure Metadata Connectivity](docs/secure_metadata_connectivity.md)  
- [Lineage Model & Logical Plan](docs/lineage_and_logical_plan.md)  
- [SQL Preview & Rendering Pipeline](docs/sql_preview_pipeline.md)  
- [Testing & Quality](docs/tests.md)  
- [Dialect System](docs/dialect_system.md)  
- [Load SQL Architecture](docs/load_sql_architecture.md)  
- [Incremental Load Architecture](docs/incremental_load.md)  

---

### 🚧 Note

elevata is currently in an early preview phase (v0.x).
Breaking changes may occur before the 1.0 release as the metadata model stabilizes.

---

## Disclaimer

This project is an independent open-source initiative.  
- It is not a consulting service.  
- It is not a customer project.  
- It is not in competition with any company.  

The purpose of elevata is to contribute to the community by providing a metadata-driven framework for building data platforms.  
The project is published under the AGPL v3 license and open for use by any organization.

---

## 🧾 License & Notices

© 2025 Ilona Tag — All rights reserved.  
**elevata™** is an open-source software project for data & analytics innovation.  
The name *elevata* is currently under trademark registration with the German Patent and Trade Mark Office (DPMA).  
Other product names, logos, and brands mentioned here are property of their respective owners.

Released under the **GNU Affero General Public License v3 (AGPL-3.0)**.  
See [`LICENSE`](https://github.com/elevata-labs/elevata/blob/main/LICENSE) for terms and [`NOTICE.md`](https://github.com/elevata-labs/elevata/blob/main/NOTICE.md) for third-party license information.