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
If omitted, elevata falls back to the default project from the active BigQuery client credentials.

Internally, elevata always qualifies BigQuery table identifiers as `project.dataset.table`  
when required (e.g. for streaming inserts), to avoid ambiguous or cross-project resolution errors.

Schemas correspond to BigQuery datasets.

Execution metadata tables (e.g. `meta.load_run_log`, `meta.load_run_snapshot`)
are written using BigQuery streaming inserts.

These require:  
- an existing dataset  
- a correctly qualified table identifier  
- matching dataset location

⚠️ Note:
Previously, unqualified table identifiers could lead to sporadic `NotFound` errors during streaming inserts.  
This has been addressed by enforcing deterministic project qualification at execution time.

---

## 🔧 Databricks

Databricks is supported as an executable target backend via **Databricks SQL Warehouse**  
(recommended with **Unity Catalog** for catalog/schema organization).

### 🧩 System prerequisites
- A Databricks workspace with SQL Warehouse access  
- Permissions to create schemas and tables in the target catalog/schema  

### 🧩 Python dependencies

```bash
pip install -r requirements/databricks.txt
```

### 🧩 Target configuration

All elevata target systems use a **generic connection secret structure**.  
This keeps configuration consistent across databases and avoids system-specific
naming differences in environment configuration.

The common fields are:

- `dialect` – SQLAlchemy dialect identifier  
- `host` – server hostname  
- `database` – database or catalog name  
- `schema` – optional default schema (not always used by execution engines)  
- `username`  
- `password`  
- `extra` – optional structured configuration for backend-specific parameters

The `extra` field is a JSON object and may contain backend-specific settings.  
It is normalized internally and passed to the respective execution engine
or SQLAlchemy connection builder as required.

Example (generic format):

```json
{
  "dialect": "databricks+connector",
  "host": "adb-xxxx.azuredatabricks.net",
  "database": "dbdwh",
  "password": "dapiXXXXXXXX",
  "schema": null,
  "extra": {
    "http_path": "/sql/1.0/warehouses/xxxx"
  }
}
```

Databricks execution runs against a **SQL Warehouse** (HTTP endpoint), not a traditional database socket.  
Authentication uses a **Personal Access Token (PAT)**.

Required security fields:  
- `server_hostname`  
- `http_path`  
- `access_token`

Required field for introspection features:  
- `dialect` (for SQL Alchemy)

Recommended additional field (Unity Catalog):  
- `catalog` (default catalog for the session)

Within the generic elevata configuration model, these map to:  
- `host` → Databricks server hostname  
- `database` → Unity Catalog catalog  
- `password` → Personal Access Token  
- `extra.http_path` → SQL Warehouse HTTP path

If you don't provide the catalog, the default catalog will be used to create your warehouse.

Example (env secret as JSON payload):

```bash
SEC_DEV_CONN_DATABRICKS_DBDWH={
  "dialect":"databricks+connector",
  "host":"adb-xxxx.azuredatabricks.net",
  "database":"dbdwh",
  "password":"dapiXXXX",
  "schema": null,
  "extra": { "http_path": "/sql/1.0/warehouses/xxxx" }
}
```

#### 🔎 Unity Catalog and identifier qualification

elevata typically renders target identifiers as **schema.table** (two-part names), e.g.:

```sql
CREATE OR REPLACE VIEW stage.my_view AS ...
```

With Unity Catalog enabled, Databricks resolves such objects inside the **current catalog**  
of the SQL session. Therefore the execution engine must ensure the correct catalog context,  
e.g. by running:

```sql
USE CATALOG dbdwh;
```

before executing DDL/DML statements.

#### 🔎 SQLAlchemy engine (materialization/introspection)

Some execution paths (e.g. materialization planning / introspection) require a SQLAlchemy  
engine. In that case the resolved DB secret must also provide a `dialect` identifier that  
allows building a SQLAlchemy URL (e.g. `databricks+connector`).

#### 🔎 DDL nullability behavior

Databricks SQL accepts `NOT NULL` constraints but does not allow explicitly specifying `NULL`  
in column definitions.

```sql
personid INT NULL      -- invalid in Databricks
personid INT           -- correct (nullable is default)
personid INT NOT NULL  -- valid
```

#### 🔎 Load logging (meta.load_run_log)

elevata can auto-provision and evolve the meta.load_run_log table by adding missing columns.  
On Databricks (Unity Catalog), this requires privileges that allow altering table schemas  
(typically MODIFY on the table, or ownership depending on governance setup).

If a column already exists, Databricks raises:  
`FIELD_ALREADY_EXISTS (SQLSTATE 42710)`.  
To keep logging idempotent across repeated runs, the Databricks backend should:

- determine existing columns via SHOW COLUMNS IN <schema>.load_run_log, and/or  
- treat duplicate-column errors as a no-op when applying ALTER TABLE ... ADD COLUMN.

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

## 🔧 Microsoft Fabric Warehouse

Fabric Warehouse is supported as an executable target backend.  
Note that Warehouse supports schemas; Fabric Lakehouse SQL endpoints do not provide schema isolation in the same way.

### 🧩 System prerequisites
- A Fabric Workspace with a Warehouse  
- Permissions to create schemas and tables  

### 🧩 Python dependencies

```bash
pip install -r requirements/fabric_warehouse.txt
```

### 🧩 Notes
- `uniqueidentifier` has limitations across endpoints (see Microsoft documentation).  
- Microsoft Fabric Warehouse follows SQL Server semantics but currently has limitations  
  regarding certain `ALTER TABLE` operations (for example datatype changes after column creation).  
  elevata therefore recommends treating datatype changes as forward schema evolution where possible.

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

## 🔧 Snowflake

Snowflake is supported as a fully executable target backend.

### 🧩 System prerequisites
- A Snowflake account with a database and warehouse  
- Permissions to create schemas and tables  

### 🧩 Python dependencies

```bash
pip install -r requirements/snowflake.txt
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

© 2025-2026 elevata Labs — Internal Technical Documentation
