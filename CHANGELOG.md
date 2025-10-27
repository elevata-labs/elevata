# Changelog

All notable changes to this project will be documented in this file.  
This project adheres to [Semantic Versioning](https://semver.org/) and [Keep a Changelog](https://keepachangelog.com/).

---

## [Unreleased]

### 🧩 Work in Progress & Upcoming Enhancements

#### Overview  
This section lists features and improvements currently under active development.

---

## 🧭 Roadmap  

### Next Milestone (0.3.x Focus)

#### Metadata Model Automation & Meta-SQL Layer
- Automated generation of **TargetDataset** and **TargetColumn** structures derived from imported metadata  
- **Deterministic, lookup-free surrogate key generation** with runtime-loaded pepper (DSGVO compliant)  
- New **TargetDatasetReference** and **TargetDatasetInput** models for component-based FK mapping and multi-source logic  
- Layer-aware governance and historization defaults via **TargetSchema**  
- Extended metadata profiling and statistics for data quality & consistency  
- Improved governance primitives (sensitivity classification, ownership, access intent)  
- Foundation for **Meta-SQL Logical Plan** and platform-independent rendering layer (preview in 0.3.x)  

---

### Planned Mid-term
- Native SQL rendering and execution directly from elevata metadata  
  (dbt compatibility remains optional)  
- Built-in **core transformation** templates and best-practice automation  
- Environment-aware metadata validation (Dev/Test/Prod consistency checks)  
- Cross-system ingestion and unification via shared `target_short_name`  
- Visual lineage and impact analysis (dataset & field level)

---

### Planned Long-term
- Declarative deployment of **target objects** to physical schemas  
- Automated lineage graph and access policy management  
- REST / GraphQL API for external metadata integration  
- Multi-platform support (Fabric, Snowflake, BigQuery, Databricks, SQL Server)  

---

🧾 Licensed under the **AGPL-v3** — open, governed, and community-driven.  
💡 *elevata keeps evolving — one small, meaningful release at a time.*


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
