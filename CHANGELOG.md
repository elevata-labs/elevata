# Changelog

All notable changes to this project will be documented in this file.  
This project adheres to [Semantic Versioning](https://semver.org/) and [Keep a Changelog](https://keepachangelog.com/).

---

## 🧭 Roadmap  

### Up Next
- Import functions for external **source-system metadata**
- Table pagination and sorting for large metadata sets

### Planned Mid-term
- Automated generation of **dbt models** based on stored metadata
- **Ingestion** of relational databases and other sources
- Built-in **core transformations** and data-engineering **best practices**

### Planned Long-term
- Support for additional platforms (Microsoft Fabric, Snowflake, BigQuery, Databricks, SQL Server)
- Optional REST / GraphQL API for external metadata integration

---

🧾 Licensed under the **AGPL-v3** — free to use, extend, and share.  
💡 *elevata keeps evolving — one small, meaningful release at a time.*

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
