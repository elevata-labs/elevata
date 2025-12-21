# ⚙️ Target Backends

This document describes the **supported target systems** for elevata execution (`--execute`) and their prerequisites.  

Execution means:  
- SQL is rendered by elevata  
- SQL is **executed inside the target system**  
- Schemas, tables, and run logs are auto-provisioned (idempotent)

---

## 🔧 BigQuery

BigQuery is supported as a **fully executable SQL-based target backend**.  

Schemas are mapped to BigQuery datasets. Tables and execution metadata  
are provisioned automatically if they do not exist.  

Execution is performed via BigQuery query jobs using standard SQL.  

### 🧩 System prerequisites

- Access to a Google Cloud project with BigQuery enabled  
- A project with billing enabled (BigQuery sandbox mode is not sufficient for execution)  

Authentication relies on Application Default Credentials (ADC).  

One of the following must be configured:  

- `gcloud auth application-default login`  
- Service account credentials via `GOOGLE_APPLICATION_CREDENTIALS`  

And you need to set the following environment variables, e.g. in your `.env` file:  

- `GOOGLE_CLOUD_PROJECT="<your GCP project ID>"`  
- `GOOGLE_BIGQUERY_LOCATION="EU"`  
Must match dataset location; meta/raw/... will be created in this location

⚠️ All BigQuery datasets used by elevata (e.g. `meta`, `raw`, `stage`, `rawcore`)  
must be created in the same location as the execution jobs (e.g. EU or US).  
Location mismatches will result in execution errors.

### 🧩 Python dependencies

```bash
pip install -r requirements/bigquery.txt
```

### 🧩 Target configuration

The target system may optionally define a project identifier.  
If omitted, the default project from the active credentials is used.  

Schemas correspond to BigQuery datasets.

---

## 🔧 DuckDB

DuckDB is the **reference target backend** for elevata and requires no external database server.

### 🧩 System prerequisites
- Install DuckDB via Python:  
  ```bash
  pip install duckdb
  ```

- Or via package manager:  
    - macOS: `brew install duckdb`
    - Linux: `apt-get install duckdb`  

- Or download [binaries](https://duckdb.org/docs/installation)  

Verify installation:

```bash
duckdb --version
```

### 🧩 Python dependencies

```bash
pip install -r requirements/duckdb.txt
```

### 🧩 Connection string (target)

DuckDB uses file-based connection strings:

```bash
duckdb:///./dwh.duckdb
```

Example via env secret:

```bash
SEC_DEV_CONN_DUCKDB_DWH=duckdb:///./dwh.duckdb
```

---

## 🔧 Microsoft SQL Server (MSSQL)

Microsoft SQL Server is supported as a **fully executable target backend**.  
SQL Server alias types and money datatypes are handled explicitly.

### 🧩 System prerequisites
- Install **Microsoft ODBC Driver for SQL Server** (recommended: ODBC Driver 18)  

- Verify driver availability (optional):

```python
import pyodbc
print(pyodbc.drivers())
```

### 🧩 Python dependencies

```bash
pip install -r requirements/mssql.txt
```

### 🧩 Connection string (target)

MSSQL uses ODBC-style connection strings:

```text
Driver={ODBC Driver 18 for SQL Server};
Server=HOST,1433;
Database=DB;
UID=USER;
PWD=PASSWORD;
TrustServerCertificate=yes;
```

Example via env secret:

```bash
SEC_DEV_CONN_MSSQL_DWH=Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=dwh;UID=sa;PWD=***;TrustServerCertificate=yes;
```

---

## 🔧 PostgreSQL

PostgreSQL is supported as a **fully executable target backend**.

### 🧩 System prerequisites
- Running PostgreSQL server (local or remote)  
- `psql` client tools available (recommended)  

Verify installation:

```bash
psql --version
```

For SHA256 hashing, elevata relies on PostgreSQL's `pgcrypto` extension.  

Ensure it is enabled in the target database:

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;
```

The database user must have sufficient privileges to create extensions.


### 🧩 Python dependencies

```bash
pip install -r requirements/postgres.txt
```

Recommended alternative package: `psycopg2-binary` (easy setup, suitable for most users)

### 🧩 Connection string (target)
Postgres uses psycopg-compatible connection strings:

```bash
postgresql://USER:PASSWORD@HOST:PORT/DBNAME
```

Example via env secret:

```bash
SEC_DEV_CONN_POSTGRES_DWH=postgresql://postgres:postgres@localhost:5432/dwh
```

---

## 🔧 Notes

- elevata executes datasets in a dataset-driven and lineage-aware manner.  
- Depending on the dataset and target layer, execution may involve SQL execution  
  in the target system or ingestion logic for Raw datasets.  
- Source systems may be accessed either for metadata introspection or as part  
  of federated or external execution strategies at the Stage layer.  
- Raw datasets are an optional landing layer. Pipelines may start directly  
  at the Stage layer if Raw ingestion is not required.  
- All target backends support:  
    - auto-provisioned schemas  
    - auto-provisioned tables (DDL-only)  
    - execution run logging (`meta.load_run_log`)

Execution semantics are determined by the target dataset and its layer.  
For Raw datasets, execution triggers ingestion logic rather than SQL execution.

---

© 2025 elevata Labs — Internal Technical Documentation
