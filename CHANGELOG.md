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

#### Metadata Model Freeze & Automated Target Modeling
- Introduction of **five-layer target architecture** (`raw`, `stage`, `rawcore`, `bizcore`, `serving`)  
  with opinionated best-practice defaults for materialization, historization, and governance  
- New model **`TargetSchema`** for layer definition and schema-wide configuration  
- Automatic generation of **`TargetDataset`** and **`TargetColumn`** structures  
  derived from imported metadata, including PK propagation and surrogate key creation  
- **Deterministic, lookup-free surrogate key generation** with runtime-loaded pepper (DSGVO compliant)  
- New **`TargetDatasetReference`** and **`TargetReferenceKeyComponent`** models  
  for component-based FK mapping and automatic join hints for lineage  
- Extended metadata profiling and statistics for quality & consistency checks  
- Improved governance primitives (sensitivity classification, ownership, access intent)  
- UI-assisted field mapping and automated column naming conventions (smart English naming + semantic suffixes)  
- **Meta-SQL Logical Plan** foundation for platform-independent SQL rendering (preview mode)

---

### Planned Mid-term

- Native SQL rendering and execution layer directly from elevata metadata  
  (dbt compatibility remains possible as an integration, not as a runtime dependency)  
- Cross-system **ingestion** and unification of multiple source systems via shared `target_short_name`  
- Built-in **core transformation** patterns and **best-practice templates**  
- Environment-aware **metadata validation** (Dev/Test/Prod consistency checks)  
- Lineage visualization and impact analysis (field-level, dataset-level)

---

### Planned Long-term  

- Declarative deployment of **target objects** to physical schemas  
  with optional auto-materialization on target platform compute  
- Extended **governance and access control** (dataset-level access policies, PII masking)  
- Optional REST / GraphQL API for external metadata integration  
- Cross-platform SQL rendering and deployment  
  *(Microsoft Fabric, Snowflake, BigQuery, Databricks, SQL Server, and more)*  

---

🧾 Licensed under the **AGPL-v3** — open, governed, and community-driven.  
💡 *elevata keeps evolving — one small, meaningful release at a time.*

---

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
