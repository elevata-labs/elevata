# 🚀 Getting Started with elevata

> The practical guide to setting up your elevata metadata environment  
> — from installation to first successful metadata import.

---

## 🧩 1. Prerequisites

Before you start, make sure the following are available:

| Requirement | Recommended Version | Notes |
|--------------|---------------------|--------|
| **Python**   | 3.11                | Required for full feature support |
| **PostgreSQL** | 14+               | Used as elevata metadata repository |
| **Node.js**  | 18+                 | Only needed for front-end builds |
| **Git**      | any recent version  | For cloning and version control |

Optional but helpful:
- **DuckDB** for quick SQL preview and rendering tests  
- **Docker Compose** for local all-in-one setup in case you want PostgreSQL instead of SQLite  

---

## ⚙️ 2. Environment Setup

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

Install dependencies:

```bash
pip install -r requirements.txt
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
```
---

## 🏗️ 3. Initialize the Metadata Database

### Option PostgreSQL

In case you want to use a PostgreSQL, install postgres extras:

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

## In any case

Run the Django migrations to create metadata tables:
```bash
python manage.py migrate
```
Create a superuser for the web interface:
```bash
python manage.py createsuperuser
```
---

## 🧮 4. Explore the Metadata UI

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
- Preview SQL renderings (starting with DuckDB dialect)

---

## 🔑 5. Secure Connectivity (optional)

If you’re connecting to production metadata systems,
use environment variables instead of plain-text passwords.

For advanced setups, see  
➡️ [`secure_metadata_connectivity.md`](secure_metadata_connectivity.md)

---

## 🧰 6. Useful Commands

| Purpose | Command |
|----------|---------|
| Run development server | `python manage.py runserver` |
| Open Django shell | `python manage.py shell` |
| Import source metadata | Trigger via UI (⚡ Import Datasets) |
| Generate target structures | Trigger via UI (⚡ Generate Targets) |
| Run tests | `pytest` |

---

## 🧭 Next Steps

Once your metadata environment is ready, continue with:

- [Automatic Target Generation Logic](generation_logic.md)
- [SQL Rendering & Alias Conventions](sql_rendering_conventions.md)
- [Lineage Model & Logical Plan](lineage_and_logical_plan.md)

---

© 2025 elevata Labs — Internal Technical Documentation