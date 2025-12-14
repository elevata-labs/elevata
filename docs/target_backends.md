# ⚙️ Target Backends

This document describes the **supported target systems** for elevata execution (`--execute`) and their prerequisites.  

Execution means:  
- SQL is rendered by elevata  
- SQL is **executed inside the target system**  
- Schemas, tables, and run logs are auto-provisioned (idempotent)

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

## 🔧 Microsoft SQL Server (MSSQL)

Microsoft SQL Server is supported as a **fully executable target backend**.

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

## 🔧 Notes

- elevata executes SQL **inside the target system only**.  
- Source systems are accessed for **metadata introspection**, not for execution.  
- Raw tables must be **ingested externally** (or seeded manually) before Stage / Rawcore / History layers can be executed.  
- All target backends support:  
    - auto-provisioned schemas  
    - auto-provisioned tables (DDL-only)  
    - execution run logging (`meta.load_run_log`)

---

© 2025 elevata Labs — Internal Technical Documentation
