# elevata – Metadata-driven Data Platform Framework

<p align="center">
  <img src="docs/logo.png" alt="elevata logo" width="200"/>
</p>

**elevata** is an open-source initiative on a mission to make building modern data platforms radically simpler.  
Metadata-driven. Best practices included. Easy to use.

## License & Dependencies

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](LICENSE)
[![Built with Django](https://img.shields.io/badge/Built%20with-Django-092E20?logo=django)](https://www.djangoproject.com/)
[![Frontend: HTMX](https://img.shields.io/badge/Frontend-HTMX-3366CC?logo=htmx)](https://htmx.org/)
[![UI: Bootstrap 5](https://img.shields.io/badge/UI-Bootstrap%205-7952B3?logo=bootstrap)](https://getbootstrap.com/)
<!-- Planned for v0.2.0:
[![dbt-DuckDB](https://img.shields.io/badge/dbt--DuckDB-FF694B?logo=dbt&logoColor=white)](https://docs.getdbt.com/docs/core/connect-data-platform/duckdb)
-->

---

## What is elevata?

**elevata** is an open-source framework for building consistent, metadata-driven data & analytics platforms.  
It provides a structured foundation for **source ingestion**, **metadata governance**, **data transformation** and **lineage-driven orchestration support from source to target** — independent of specific vendors or tools.

The goal is to let teams design and operate data platforms with minimal effort – following the principle:  
**metadata in → best practices out.**


## 🧠 Core Concept

elevata follows three design principles:

| Principle | Meaning |
|------------|----------|
| **Metadata first** | All logic (ingestion, mapping, lineage) is controlled by metadata models. |
| **Single Point of Truth** | Source definitions and technical metadata are centrally maintained. |
| **Environment abstraction** | Profiles manage credentials and connections per environment. |

---

## 🧩 Interoperability and Independence

**elevata** integrates smoothly with existing **dbt projects** but remains fully functional **without dbt**.  
All core capabilities — metadata management, rendering, and execution — are implemented natively within elevata.

This approach ensures long-term flexibility and technical independence, even as the data-transformation landscape evolves.  
Users can continue to run dbt models where convenient or adopt elevata’s built-in rendering engine for a fully self-contained workflow.

For architectural strategy details, see [docs/strategy/dbt_decoupling.md](./docs/strategy/dbt_decoupling.md).

---

## 🎬 Quick Glance

![Screenshot of elevata metadata UI](docs/screenshot_metadata_ui.png)

---

## ✨ What’s inside elevata today

**elevata** already brings together the essentials for a modern, metadata-driven data platform —  
all in one lightweight, open-source framework:

- 🧩 **Metadata Management Made Simple**  
  Manage your platform structures through a clean, responsive web interface.  
  Inline edits, audit tracking, and user management built right in.

- 🔍 **Smart Metadata Import** *(new in v0.2.0)*  
  Automatically read table and column structures from your relational source systems via **SQLAlchemy** —  
  including datatype mapping and key detection, all controlled by your central metadata profiles  
  to accelerate setup and ensure full consistency across environments.

- ⚙️ **Flexible Metadata Database Backend**  
  Start instantly with SQLite — or go production-ready with PostgreSQL, via Docker or your own instance.

- 💡 **Built for Builders**  
  Clean Django + HTMX foundation, easy setup, and extensible architecture for future modules like ingestion and dbt-based transformations.

- 🔒 **Secure by Default**  
  Authentication, CSRF protection, and consistent form handling are already integrated.  
  Connection details and credentials are resolved securely via `.env` or Key Vault — never stored in the database.

- 🌍 **Open, Transparent, and Evolving**  
  100 % open source under the **AGPL-v3** — growing step by step towards a full metadata-driven platform.

---

## 🚀 Quickstart

Get elevata running locally

### ⚙️ Environment:

Install
- Python 3.11+ (currently tested on 3.11)
- Git

Copy file .env.example in root folder and name it **.env**. This is the place where your environment variables are stored.

```bash
# 1. clone the repo
git clone https://github.com/elevata-labs/elevata.git
cd elevata

# 2. create & activate a virtual environment
py -3.11 -m venv .venv
.venv\Scripts\activate # or source .venv/bin/activate on Linux

# 3. install dependencies
python -m pip install --upgrade pip 
pip install -r requirements/base.txt
```

### 🛢️ Metadata Database:

#### Step 1: Choose your database management system
**Option A**: SQLite (default database):
nothing to prepare. Continue with **Step 2**

#### Option B: PostgreSQL: 
For using this option, first update your .env file by DB_ENGINE=postgres.  
Then install postgres extras: 
```bash
pip install -r requirements/postgres.txt
```

**Postgres Alternative 1**: You can run postgres (17) locally with docker:
```bash
docker compose -f core/postgres/docker-compose.yml up -d db
```
**Postgres Alternative 2**: Use your **own** PostgreSQL (no Docker):  
If you already have a PostgreSQL server (managed or self-hosted), configure elevata to use it:
Configure connection via discrete DB_* variables in your .env file.  
Ensure role & database exist (if you need to create them):

```bash
create role elevata login password 'elevata';
create database elevata owner elevata;
```

#### Step 2: Setup database

```bash
# 1. set up database and create an admin user
cd core
python manage.py migrate
python manage.py createsuperuser

# 2. run development server
python manage.py runserver
```
Then open http://localhost:8000 in your browser and log in with your newly created superuser account.

---

## 🔐 Secure Metadata Connectivity (since v0.2.0)

Starting with **elevata 0.2.0**, metadata for source systems can be imported directly from relational databases 
via **SQLAlchemy** – fully parameterized through a unified connection profile.

This makes it possible to automatically read table and column structures for any defined `SourceDataset`  
and populate the `SourceColumn` model, including datatype mapping and primary-key detection.

### How it works

- elevata connects to your defined **SourceSystems** using credentials managed via a **profile configuration file**.
- The default profile file is: **elevata/config/elevata_profiles.yaml**
- Connection and security details (passwords, connection strings, Key Vault references)  
are resolved dynamically through your local `.env` file or optionally through Azure Key Vault.

The logic follows a strict **Single Point of Truth** principle:
- Metadata is always read from the configured source system itself.
- Environment-specific secrets are stored outside the database.
- Profiles are shared across environments — only the `.env` (or Key Vault) differs.

### Supported Sources

For detailed database support and ready-to-copy SQLAlchemy URI examples,
see **[docs/source_backends.md](docs/source_backends.md)**.

*Note:* Some platforms can be selected and documented in elevata,
but are not yet supported for automated metadata import. The UI will
show a clear hint in those cases.

---

### ⚙️ Configuration Overview

| File | Purpose |
|------|----------|
| **.env** | Holds environment-specific variables like DB credentials or Azure Key Vault settings. |
| **config/elevata_profiles.yaml** | Central definition of connection profiles and secret templates. |
| **/etc/elevata/** *(optional)* | System-wide defaults for production or containerized deployments. |

Example `.env` entries see template file **elevata/.env.example**  
Example `elevata_profiles` entries see template file **elevata/config/elevata_profiles.example.yaml**

### 🧠 Supported Sources

- ✅ PostgreSQL
- ✅ DuckDB
- ✅ Microsoft SQL Server
- 🔜 Oracle, Snowflake, MySQL, BigQuery (under development)

Flat files, REST APIs, and other sources will be added step by step.

---

### 🧩 Example Usage

After defining your `SourceSystems` and `SourceDatasets` in the UI:

1. Click **“Import Metadata”** for the selected system or dataset.  
2. elevata connects via SQLAlchemy using your configured secret reference.  
3. It fetches all columns, datatypes, PKs, and nullable flags into `SourceColumn`.  
4. Columns that are part of a primary key will be automatically marked as `integrate = True`.

---

### 🔒 Security & Secrets Handling

Secrets are never stored in the metadata database.  
They are resolved at runtime via a secure lookup chain:

1. `.env` → environment variable  
2. (optional) Azure Key Vault  
3. Fallback to `/etc/elevata/` (if configured)

This mechanism ensures that sensitive data like connection strings or keys  
are managed externally, independent of environment or deployment type.

---

## Target Platform Support

elevata is designed to support multiple backends for flexible data platform development.  

First will be supported:
- ✅ DuckDB

Planned:
- 🔜 Microsoft Fabric
- 🔜 Snowflake
- 🔜 BigQuery
- 🔜 Databricks
- 🔜 SQL Server

Each backend has its own prerequisites.  
See [docs/backends.md](docs/backends.md) for details.

---

## 🧭 Versioning & Stability  

**elevata** follows [Semantic Versioning](https://semver.org) — meaning that version numbers communicate stability and compatibility guarantees.  

| Version type | Meaning |
|---------------|----------|
| **0.x.y** | Active development phase — architecture and APIs may still evolve. |
| **1.0.0** | Marks architectural stability: core models, metadata logic, and CLI interfaces are reliable for production use. |
| **MAJOR** | May introduce breaking changes (API or schema). |
| **MINOR** | Adds new features in a backward-compatible way. |
| **PATCH** | Fixes bugs or improves internal behavior without changing the interface. |

➡ Expect several **0.x milestones** while the core modules (Metadata, Ingestion, Governance) are finalized and refined.  
Once these are stable and consistent, **elevata 1.0.0** will be released to signal production readiness.

---

## Disclaimer

This project is an independent open-source initiative.  
- It is not a consulting service.  
- It is not a customer project.  
- It is not in competition with any company.  

The purpose of elevata is to contribute to the community by providing a metadata-driven framework for building data platforms.  
The project is published under the AGPL v3 license and open for use by any organization.

---

## Trademark Notice

© 2025 Ilona Tag.  
elevata™ is an open-source software project for data and analytics innovation.  
The name *elevata* is a pending trademark registration at the German Patent and Trade Mark Office (DPMA).  
Other product names, logos, and brands mentioned here are property of their respective owners.  
The software is released under the MIT License.