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

elevata is a framework for metadata-driven data platform development.  
The goal is to let teams design and operate data platforms with minimal effort – powered by:  

- **Metadata Management** - lightweight UI built with Django, SQLite and HTMX - bridging business metadata and data engineering
- **Ingestion** – simplified, parameter-driven data loading  
- **Transformation** – built on dbt, with metadata at the core  
- **Orchestration support** – helper files for integration with common orchestrators,  
  fully **lineage-driven from source to target**

It’s still early days – but the direction is clear:  
**metadata in → best practices out.**

---

## 🖼️ Quick Glance

![Screenshot of elevata metadata UI](docs/screenshot_metadata_ui.png)

---

## ✨ Current Features

- Modern web interface for metadata management (Django + HTMX)
- Inline create/edit/delete (CRUD) with automatic audit fields
- Secure user authentication (login, logout, password change)
- Generic CRUD engine for all metadata models
- Metadata-driven dbt model generation scaffold (DuckDB)
- Fully open under the **AGPL-v3 license**

---

## ⚙️ Quickstart

Get elevata running locally

Requirements:

- Python 3.11+ (currently tested on 3.11)
- Git

Steps:

```bash
# 1. clone the repo
git clone https://github.com/elevata-labs/elevata.git
cd elevata

# 2. create & activate a virtual environment
py -3.11 -m venv .env
.env\Scripts\activate # or source .env/bin/activate on Linux

# 3. install dependencies
python -m pip install --upgrade pip 
pip install -r requirements/base.txt

# 4. set up metadata database (SQLite by default) and create an admin user
python manage.py migrate
python manage.py createsuperuser

# 5. run development server
python manage.py runserver
```
Then open http://localhost:8000 in your browser and log in with your newly created superuser account.

---

## Backend Support

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

## Disclaimer

This project is an independent open-source initiative.  
- It is not a consulting service.  
- It is not a customer project.  
- It is not in competition with any company.  

The purpose of elevata is to contribute to the community by providing a metadata-driven framework for building data platforms.  
The project is published under the AGPL v3 license and open for use by any organization.

---

## Trademark Notice

"elevata" is a project name used for this open-source initiative.  
All other product names, logos, and brands are property of their respective owners.  
Use of these names does not imply endorsement.
