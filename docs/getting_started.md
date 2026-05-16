# ⚙️ Getting Started with elevata

> The practical guide to setting up your elevata metadata environment  
> — from installation to first successful metadata import.

---

## 🔧 1. Prerequisites

Before you start, make sure the following are available:

| Requirement | Recommended Version | Notes |
|--------------|---------------------|--------|
| **Python**   | 3.14                | Recommended. Supported: Python 3.11+ |  
| **PostgreSQL** | 14+               | Used as elevata metadata repository (SQLite works fine for local use) |  
| **Git**      | any recent version  | For cloning and version control |

Optional but helpful:  
- **DuckDB** for quick SQL preview and rendering tests  
- **Docker Compose** for local all-in-one setup in case you want PostgreSQL instead of SQLite  

> *Frontend dependencies are handled directly by Django; no separate Node.js build is required.*

---

## 🔧 2. Environment Setup

First clone the repo  

```bash
git clone https://github.com/elevata-labs/elevata.git
cd elevata
```

Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate   # on Linux / macOS
.venv\Scripts\activate      # on Windows
```

### 🧩 2.1 Installation

Install base dependencies (required):

```bash
pip install -r requirements/base.txt
```

Copy the example environment configuration and adjust it:

```bash
cp .env.example .env
```
Edit `.env` according to your local setup, e.g.:

```bash
# Choose which metadata database type you want
DB_ENGINE=sqlite # or postgres

# Pepper value for deterministic surrogate keys
SEC_DEV_PEPPER=supersecretpeppervalue

# Schema evolution guardrails (optional defaults)
ELEVATA_ALLOW_TYPE_ALTER=false
ELEVATA_ALLOW_AUTO_DROP_COLUMNS=false
ELEVATA_ALLOW_AUTO_DROP_HIST_COLUMNS=false

# Architecture state baseline directory
ELEVATA_ARCH_STATE_DIR=.elevata/state
```

Install the target backend you want to execute against:

#### 🔎 BigQuery target:

```bash 
pip install -r requirements/bigquery.txt
```

#### 🔎 Databricks target:

```bash 
pip install -r requirements/databricks.txt
```

#### 🔎 DuckDB target:

```bash
pip install -r requirements/duckdb.txt
```

#### 🔎 Microsoft Fabric Warehouse target:

```bash 
pip install -r requirements/fabric_warehouse.txt
```

#### 🔎 MSSQL target:

```bash
pip install -r requirements/mssql.txt
```

#### 🔎 PostgreSQL target:

```bash
pip install -r requirements/postgres.txt
```

#### 🔎 Snowflake target:

```bash 
pip install -r requirements/snowflake.txt
```


> If you only want SQL preview / SQL generation (no --execute), requirements/base.txt is sufficient.

> *RAW ingestion is optional; elevata also supports federated or pre-existing staging layers.*

---

## 🔧 3. Initialize the Metadata Database

### 🧩 Option SQLite (recommended for first-time setup)

If you just want to explore elevata or run metadata generation locally,  
you don’t need PostgreSQL — SQLite works out of the box.

Just make sure your `.env` contains:

```bash
DB_ENGINE=sqlite
``` 

Then run the standard migrations:

```bash
python manage.py migrate
python manage.py createsuperuser
```

This will create a local file `db.sqlite3` in your project root.  
Perfect for demos, prototyping, or CI pipelines.

### 🧩 Option PostgreSQL (for shared or production environments)

If you prefer PostgreSQL for shared or production use, install postgres extras:

```bash
pip install -r requirements/postgres.txt
```

Alternative 1: run postgres (17) locally with docker:

```bash
docker compose -f core/postgres/docker-compose.yml up -d db
```

Alternative 2: Use your own PostgreSQL (no Docker):  
If you already have a PostgreSQL server (managed or self-hosted), configure elevata to use it:  
Configure connection via discrete DB_* variables in your .env file.  
Ensure role & database exist (if you need to create them):

```bash
create role elevata login password 'elevata';
create database elevata owner elevata;
```

Then run the standard migrations:

```bash
python manage.py migrate
python manage.py createsuperuser
```

---

## 🔧 4. Explore the Metadata UI

Start the development server:
```bash
python manage.py runserver
```
Then open [http://localhost:8000](http://localhost:8000) and log in with your superuser credentials.

You can now:  
- Trigger **auto-import of source system metadata**  
- Inspect **source datasets and columns**  
- Define **integration rules** (`integrate = True`)  
- Trigger **target auto-generation**  
- Preview **auto-generated** SQL renderings (starting with DuckDB dialect)

---

## 🔧 5. Architecture Control Plane Commands

elevata provides read-only commands for architecture state, review, and
promotion workflows.

### 🧩 5.1 Render Architecture State

Render the metadata-defined architecture state:

```bash 
python manage.py elevata_state
```

Write an architecture state artifact:

```bash 
python manage.py elevata_state --output .artifacts/dev_architecture_state.json 
```

Print only the architecture state fingerprint:

```bash 
python manage.py elevata_state --fingerprint-only
```

### 🧩 5.2 Render Architecture Change Report

Render a report for one dataset:

```bash 
python manage.py elevata_plan rc_aw_customer --schema rawcore
```

Render a JSON report for CI:

```bash 
python manage.py elevata_plan --all --schema rawcore --format json
```

Use an explicit baseline state file:

```bash
python manage.py elevata_plan rc_aw_customer \
--schema rawcore \
--previous-state .artifacts/prod_architecture_state.json
```

### 🧩 5.3 Compare Architecture State Artifacts

Compare two architecture state files:

```bash
python manage.py elevata_promote \
.artifacts/dev_architecture_state.json \
.artifacts/prod_architecture_state.json \
--source-label dev \
--target-label prod
```
CI exit policies are available via:

```bash 
--fail-on-changes 
--fail-on-blocked 
--fail-on-destructive 
```

---

## 🔧 6. Secure Connectivity (optional)

If you’re connecting to production metadata systems,
use environment variables instead of plain-text passwords.

For advanced setups, see  
[`secure_metadata_connectivity.md`](secure_metadata_connectivity.md)

---

## 🔧 7. Useful Commands

| Purpose | Command |
|----------|---------|
| Run development server | `python manage.py runserver` |
| Open Django shell | `python manage.py shell` |
| Import source metadata | Trigger via UI (⚡ Import Datasets) |
| Generate target structures | Trigger via UI (⚡ Generate Targets) |
| Run tests | `pytest` |

---

## 🔧 Next Steps

Once your metadata environment is ready, continue with:

- [Automatic Target Generation Logic](generation_logic.md)
- [SQL Rendering & Alias Conventions](sql_rendering_conventions.md)
- [Lineage Model & Logical Plan](logical_plan.md)

---

© 2025-2026 elevata Labs — Internal Technical Documentation