# ⚙️ Getting Started with elevata

> The practical guide to setting up your elevata metadata environment - from installation to first successful metadata import.

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

# Architecture approval artifact directory
ELEVATA_ARCH_APPROVAL_DIR=.elevata/approvals

# Architecture execution record directory
ELEVATA_ARCH_EXECUTION_DIR=.elevata/executions

# Runtime role and metadata environment
ELEVATA_RUNTIME_MODE=authoring
ELEVATA_ENVIRONMENT=dev

# Environment Promotion artifact stores
ELEVATA_PROMOTION_RELEASE_DIR=.elevata/promotion/releases
ELEVATA_PROMOTION_APPROVAL_DIR=.elevata/promotion/approvals
ELEVATA_PROMOTION_PACKAGE_DIR=.elevata/promotion/packages
ELEVATA_PROMOTION_HISTORY_DIR=.elevata/promotion/history
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

If you just want to explore elevata or run metadata generation locally, you don’t need PostgreSQL - SQLite works out of the box.

Just make sure your `.env` contains:

```bash
DB_ENGINE=sqlite
``` 

Then run the standard migrations:

```bash
cd core
python manage.py migrate
python manage.py createsuperuser
```

This will create a local file `db.sqlite3` in your project root. Perfect for demos, prototyping, or CI pipelines.

### 🧩 Option PostgreSQL (for shared or production environments)

If you prefer PostgreSQL for shared or production use, install postgres extras:

```bash
pip install -r requirements/postgres.txt
```

Alternative 1: run postgres (17) locally with docker:

```bash
cd core
docker compose -f postgres/docker-compose.yml up -d db
```

Alternative 2: Use your own PostgreSQL (no Docker):  
If you already have a PostgreSQL server (managed or self-hosted), configure elevata to use it:  
Configure connection via discrete DB_* variables in your .env file. Ensure role & database exist (if you need to create them):

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
- Open **Review target generation** and apply Source-to-Target metadata changes through Architecture Control  
- Preview **auto-generated** SQL renderings (starting with DuckDB dialect)  
- Open **Architecture Catalog** to discover datasets, Data Products, maps, insights, lineage entry points, query contracts and execution evidence references  
- Open **Architecture Control** to review, approve, preview and execute controlled architecture scopes  
- Open **Environment Promotion** to create immutable Architecture Releases and deploy reviewed metadata to separately controlled TEST / PROD runtimes

---

## 🔧 5. Architecture Control

elevata provides deterministic UI workflows and command adapters for target metadata generation, architecture state, review, approval, controlled execution and audit records.

### 🧩 5.1 Review and Apply Target Generation

After importing or editing SourceDataset and SourceColumn metadata, open the SourceDataset list and choose **Review target generation**. The action opens Architecture Control at the all-dataset generated-layer overview.

Follow the sequence shown by the UI:

```text
RAW
  ↓
STAGE
  ↓
RAWCORE
```

For each pending layer:

1. Open the layer review.  
2. Inspect Source-to-Target impact, action classifications, and before/after state.  
3. Create a Generation Approval when the plan contains breaking changes or when an optional review decision should be recorded.  
4. Select **Guarded apply**.  
5. Continue until all three generated layers are up to date.

Later-layer previews remain provisional until the previous layer converges. Architecture Control recalculates Stage after RAW and Rawcore after Stage.

Generation Approval authorizes target metadata mutation only. The resulting physical architecture change is reviewed separately through the Architecture Change Report and Architecture Approval workflow.

The equivalent CLI commands are intended primarily for debugging, CI and explicit automation. See [Controlled Target Generation](controlled_target_generation.md).

### 🧩 5.2 Render Architecture State

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

### 🧩 5.3 Render Architecture Change Report

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

### 🧩 5.4 Compare Architecture State Artifacts

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

### 🧩 5.5 Create and Check Approval Artifacts

Create an Approval Artifact from an Architecture Change Report:

```bash
python manage.py elevata_plan rc_aw_customer \
  --schema rawcore \
  --format json \
  --output .artifacts/architecture_plan_rc_aw_customer.json

python manage.py elevata_approve .artifacts/architecture_plan_rc_aw_customer.json \
  --approved-by "Reviewer Name" \
  --note "Reviewed for deployment." \
  --store
```

Check a stored Approval Artifact:

```bash
python manage.py elevata_approval_check \
  .artifacts/architecture_plan_rc_aw_customer.json \
  .elevata/approvals/<profile>/<target-system>/<report_fingerprint>.approval.json
```

### 🧩 5.6 Use Architecture Control in the UI

The Architecture Control UI provides a guided workflow for controlled architecture scopes.

It supports:

- all-dataset scopes  
- schema scopes  
- TargetDataset scopes  
- TargetDataset scopes with target-only execution

From Architecture Control, you can:

- inspect the generated-layer sequence  
- review and apply Target Generation Plans  
- create and check Generation Approval Artifacts  
- inspect the Architecture Change Report  
- download report JSON  
- create and check Approval Artifacts  
- inspect the Execution Impact Plan  
- inspect the Execution Preview  
- run controlled load execution  
- inspect captured execution output  
- inspect the Architecture Execution Record

Controlled execution uses the load runner and keeps preflight validation, Architecture Guard enforcement, approval matching and dialect-owned SQL rendering in place.

Architecture Execution Records use the configured base directory and are scoped by runtime context:

```text
.elevata/executions/<profile>/<target-system>/
```

### 🧩 5.7 Create an Immutable Scheduler Run Plan

Create a full-scope Run Plan for the active profile and target system:

```bash
python manage.py elevata_run_plan \
  --all-datasets \
  --output .artifacts/full.run_plan.json
```

elevata stores the Run Plan together with a matching Planned Architecture State snapshot. Scheduler tasks validate their dataset metadata against this snapshot and write structured outcome artifacts.

After every planned dataset has a valid outcome, finalize the run:

```bash
python manage.py elevata_finalize_run_plan \
  .artifacts/full.run_plan.json
```

Finalization persists exactly the architecture applied by the completed run. Metadata changes after Run Plan creation require a new Run Plan and a new scheduler run.

---

## 🔧 6. Environment Promotion (optional multi-environment setup)

For a single local metadata environment, no Promotion Target Runner is required.

To operate separate TEST / PROD metadata environments, keep the authoring runtime and every target runtime bound to their own metadata database.

Authoring example:

```env
ELEVATA_RUNTIME_MODE=authoring
ELEVATA_ENVIRONMENT=dev
ELEVATA_PROMOTION_TARGETS=test
ELEVATA_PROMOTION_TARGET_TEST_URL=http://127.0.0.1:8001/metadata
ELEVATA_PROMOTION_TARGET_TEST_TOKEN=<TEST runner token>
```

TEST target example:

```env
ELEVATA_RUNTIME_MODE=promotion_target
ELEVATA_ENVIRONMENT=test
ELEVATA_PROMOTION_RUNNER_TOKEN=<same TEST runner token>
DB_NAME=db_test.sqlite3
```

Run the TEST target separately, for example:

```bash
python manage.py runserver 127.0.0.1:8001
```

The authoring UI then guides the operator through:

```text
Release → Review → Approve → Deploy → Audit
```

Creating a Release, building a plan and approving a package do not mutate target metadata. Target metadata is changed only during guarded **Deploy** after the exact package has passed a live drift check.

The target runtime exposes runner endpoints only. It does not expose the regular modeling UI.

See [Environment Promotion](environment_promotion.md) for the complete topology, security model, artifact chain, migrations and production operating guidance.

---

## 🔧 7. Secure Connectivity (optional)

If you’re connecting to production metadata systems, use environment variables instead of plain-text passwords.

For advanced setups, see
[`secure_metadata_connectivity.md`](secure_metadata_connectivity.md)

---

## 🔧 8. Useful Commands

| Purpose | Command |
|----------|---------|
| Run development server | `python manage.py runserver` |
| Open Django shell | `python manage.py shell` |
| Import source metadata | Trigger via UI (⚡ Import Datasets) |
| Review and apply target structures | Source Datasets → **Review target generation** → Architecture Control |
| Run tests | `python runtests.py` |
| Render architecture state | `python manage.py elevata_state` |
| Render architecture report | `python manage.py elevata_plan --all` |
| Create immutable scheduler Run Plan | `python manage.py elevata_run_plan --all-datasets` |
| Finalize completed scheduler Run Plan | `python manage.py elevata_finalize_run_plan <RUN_PLAN_PATH>` |
| Execute controlled architecture scope | Use Architecture Control in the UI |
| Promote metadata between environments | Use Environment Promotion in the authoring UI |

---

## 🔧 Next Steps

Once your metadata environment is ready, continue with:

- [Automatic Target Generation Logic](generation_logic.md)  
- [Controlled Target Generation](controlled_target_generation.md)  
- [Environment Promotion](environment_promotion.md)  
- [Architecture Control Plane](architecture_control_plane.md)  
- [SQL Rendering & Alias Conventions](sql_rendering_conventions.md)  
- [Lineage Model & Logical Plan](logical_plan.md)

---

© 2025-2026 elevata - Technical Documentation