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
  <img src="https://raw.githubusercontent.com/elevata-labs/elevata/main/docs/elevata_v2_15_0.png" alt="elevata UI preview" width="900"/>
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

1. Import or define source metadata, lineage, contracts, and execution semantics  
2. Review source metadata import outcomes before generation  
3. Discover architecture from Source Systems through Target Datasets to Data Products using Catalog, Portfolio, Insights, and Map    
4. Inspect generated SQL, lineage, contracts, health, quality review, reference integrity, and execution evidence  
5. Review architecture changes through Architecture Review Briefing and approve them through Architecture Control  
6. Execute approved or unchanged scopes deterministically on your target warehouse  
7. Resolve controlled reference members during execution where modeled references explicitly allow it  
8. Audit execution through Architecture Execution Records

---

## 💻 Execution

Pipelines are executed dataset-driven and lineage-aware.

Execution supports full and incremental loads, historization, schema evolution, controlled reference members, and structured load logging.

Behavior is deterministic and observable.

Schema drift is reconciled through Architecture MigrationPlan-driven materialization:  
renames, adds, type evolution and controlled rebuilds are derived from architecture state, while destructive changes remain explicitly policy-gated.

Controlled Reference Members complement modeled rawcore references. Default members are maintained as artificial fallback rows, inferred members can be created during child dataset loads when a modeled TargetDatasetReference explicitly enables them, and Default Member Fallback can map still-unresolved child reference keys to the parent default member. Parent datasets remain authoritative: a later parent full load can replace inferred members with real source-backed rows. When controlled reference completion is enabled, referenced parent datasets become execution dependencies so parent readiness is enforced before child loads.

---

## 🔎 Source Metadata Import Review

elevata makes source metadata import reviewable.

Metadata import results explain what was discovered across SQLAlchemy-backed systems, files and REST sources.  
Instead of showing only that columns were processed, elevata distinguishes whether source columns were created, actually changed, unchanged, removed, or need manual review.

This gives users immediate confidence after import:

- **Created** columns are new in elevata metadata.  
- **Changed** columns existed before and now differ from the previous technical metadata state.  
- **Unchanged** columns were checked and still match the previous metadata state.  
- **Removed** columns no longer exist in the imported source shape.  
- **PK columns** show detected primary key candidates.  
- **Needs review** highlights skipped datasets or unresolved metadata decisions.

The import review is deterministic, transient and read-only as a report. It does not introduce a new wizard, does not persist import history, and does not change the existing source import semantics.

---

## 🚦 Source & Ingestion Readiness

elevata makes the handoff from source metadata into executable architecture inspectable before load execution.

For each SourceDataset, Source Ingestion Readiness evaluates metadata-defined conditions such as lifecycle and integration scope, RAW landing intent, native or external ingestion mode, integrated SourceColumns, active RAW TargetDataset links, file or REST configuration shape, JSON paths and incremental policy requirements.

Readiness is expressed through transparent states:

- **Ready** - no blocking or warning signal is present.  
- **Attention** - metadata contains a blocking or warning signal that should be resolved or reviewed.  
- **Not applicable** - the dataset is inactive, outside integration scope, or intentionally uses direct or federated access without elevata-managed RAW landing.  
- **Unavailable** - the dataset cannot be evaluated as a valid source architecture object.

The same readiness semantics appear on SourceDataset detail pages, in the Architecture Catalog Source Systems view, in the Catalog Portfolio and in Source-to-RAW handoffs on the Catalog Map.

Readiness is deterministic and read-only. It does not connect to sources, resolve secrets, read files, call REST endpoints, import metadata, edit configuration or execute loads.

---

## 🧭 Architecture Catalog

elevata provides a read-only Architecture Catalog for discovering metadata-defined executable architecture from Source Systems through Target Datasets to Data Products.

The Catalog shows what exists, how sources enter the platform, how datasets are defined and connected, how architecture is controlled, where execution evidence is available, how portfolio posture looks, and which readiness, quality and governance signals need attention.

The Source Systems view groups SourceDatasets by their owning Source System and shows source type, declared ingestion mode, lifecycle posture, readiness distribution, diagnostic counts, RAW landing intent and linked RAW TargetDatasets. Search and filters support readiness state, Source System, source type and ingestion mode.

The Target Datasets view supports search and filtering by schema, owner, lifecycle status, system-managed status, materialization type, incremental strategy and query logic.

Catalog detail pages summarize architecture metadata, ownership, health, upstream inputs, downstream consumers, column contract signals and the latest Architecture Execution Record for the dataset scope. SourceDataset detail pages expose the same Source Ingestion Readiness semantics used by the global Catalog views.

Catalog Detail also provides an on-demand Architecture Quality Review. It checks loaded target data against metadata-defined expectations such as non-nullable columns, duplicate surrogate or business keys, empty datasets and blank string business keys. The review is read-only, bounded and dialect-owned.

For datasets with modeled outgoing references, Catalog Detail also provides an on-demand Reference Integrity Review. It checks loaded target data for missing parent examples and keeps the review read-only, bounded and dialect-owned.

Reference Integrity Review is diagnostic. It does not create inferred members, apply Default Member Fallback, or repair data automatically. Controlled Reference Members are part of load execution and run only when the modeled reference explicitly enables the relevant behavior.

Catalog Portfolio places Source ingestion readiness beside Data Product readiness while keeping TargetDataset ownership, contracts, health, review state, execution evidence and layer distribution as separate posture signals. Readiness groups and actionable Portfolio KPIs open filtered Catalog worklists for focused drill-down.

Catalog Data Products show which serving-layer datasets are ready for trusted consumption. Readiness is derived from ownership, metadata health, query contracts, lineage, Architecture Control review state and execution evidence.

Catalog Insights highlight ownership gaps, metadata health findings, custom query logic, downstream consumer visibility, inactive datasets with consumers, missing execution evidence, and dataset-specific Architecture Control review status summaries.

Catalog Maps show the end-to-end entry path from Source Systems through SourceDatasets into RAW TargetDatasets, followed by the target-layer flow, dependency matrix and expandable direct dependency examples.

The Catalog does not edit metadata and does not execute loads. Architecture Quality Review and Reference Integrity Review do not persist review history, create inferred members or repair data automatically. Controlled load execution remains responsible for any enabled reference member handling. Architecture Control remains responsible for approval, execution, execution records and retention workflows.

---

## 🧭 Architecture Control

elevata makes architecture changes reviewable before execution.

Architecture State, Change Reports, Promotion Reports, Approval Artifacts and Execution Records expose deterministic fingerprints, MigrationPlan actions, policy decisions, review decisions and execution outcomes.

This supports controlled review, CI checks and environment-to-environment architecture promotion while keeping execution guardrails inside the load runner.

The Architecture Control UI makes approval state, scope, policy status, change summary, execution preview, dependency mode, controlled reference readiness, captured output and execution records visible for controlled scopes.

Architecture Review Briefing adds compact reviewer guidance directly inside Architecture Control. It summarizes the selected scope, review state, change volume, policy evaluation, destructive or blocking signals, execution readiness and suggested reviewer focus before approval or execution. Allowed and metadata-only policy decisions are shown as evaluated outcomes, while preflight and blocked decisions remain reviewer attention.

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

**1. Architecture Discovery & Trust**  
Keeping executable architecture discoverable across datasets, lineage, contracts, ownership, readiness, quality review, reference integrity, health and execution evidence.

**2. Controlled Runtime Operation**  
Strengthening deterministic review, approval, execution, audit evidence, retention and controlled runtime safety without adding unnecessary control layers.

**3. Source & Ingestion Readiness**  
Keeping source onboarding, RAW landing intent, ingestion modes, file/API patterns, external ingestion and federated access explicit, inspectable and deterministic.

**4. Platform Coverage & Runtime Hardening**  
Improving backend coverage, dialect behavior, schema evolution safety, historization, logging and reproducible execution across supported warehouses.

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