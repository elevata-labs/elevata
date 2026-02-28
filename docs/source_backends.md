# ⚙️ Supported Source Backends

This document provides an overview of supported database platforms, tested SQLAlchemy dialects,  
and connection examples for **elevata**.

It describes how elevata connects to external systems for schema discovery, metadata introspection,  
and optional federated access at the Stage layer.  
Execution semantics are defined separately.

It also documents non-relational ingestion sources (Files and REST) and how to configure them via
`SourceDataset.ingestion_config`.

---

## 🔧 1. General Pattern (SQLAlchemy-backed systems)

All SQL database connections use the standard **SQLAlchemy URI** format:

```text
dialect[+driver]://username:password@host:port/database[?params]
```

Example:

```text
mssql+pyodbc://user:pwd@server,1433/db?driver=ODBC%20Driver%2018%20for%20SQL%20Server
```

In your `.env`, define these values using the **elevata secret convention**:

```text
SEC_<ENVIRONMENT>_CONN_<TYPE>_<SHORTNAME>=<SQLAlchemy URI>
```

Example:

```text
SEC_DEV_CONN_MSSQL_SAP=mssql+pyodbc://user:pwd@sql01,1433/SAPDB?driver=ODBC%20Driver%2018%20for%20SQL%20Server
```

---

## 🔧 2. Fully Supported Backends

| Type | Label | Driver / Package | Reflection Support | Example URI |
|------|--------|------------------|--------------------|-------------|
| `bigquery` | Google BigQuery | `pybigquery` | ✅ Columns only | `bigquery://project-id` |
| `databricks` | Databricks SQL (beta) | `sqlalchemy-databricks` | ⚠️ Columns only | `databricks://token:<TOKEN>@<host>?http_path=/sql/1.0/warehouses/<id>` |
| `duckdb` | DuckDB | `duckdb-engine` | ✅ Full | `duckdb:///C:/data/local.duckdb` |
| `mssql` | Microsoft SQL Server | `pyodbc` | ✅ Full | `mssql+pyodbc://user:pwd@host,1433/db?driver=ODBC%20Driver%2018%20for%20SQL%20Server` |
| `mysql` | MySQL / MariaDB | `mysqlclient` or `pymysql` | ✅ Full | `mysql+pymysql://user:pwd@localhost:3306/db` |
| `oracle` | Oracle | `python-oracledb` | ✅ Columns & PKs | `oracle+oracledb://user:pwd@host:1521/servicename` |
| `postgres` / `postgresql` | PostgreSQL | `psycopg2` | ✅ Full | `postgresql+psycopg2://user:pwd@localhost:5432/dbname` |
| `redshift` | Amazon Redshift | `sqlalchemy-redshift` | ✅ Columns only | `redshift+psycopg2://user:pwd@redshift:5439/dev` |
| `snowflake` | Snowflake | `snowflake-sqlalchemy` | ✅ Columns only | `snowflake://user:pwd@account/DB/SCHEMA?warehouse=WH` |
| `sqlite` | SQLite | built-in | ✅ Full | `sqlite:///C:/data/dev.db` |

---

## 🔧 3. Connection Notes

- Use **URL-encoded driver names** (e.g. `ODBC%20Driver%2018%20for%20SQL%20Server`).
- Backslashes in paths should be replaced by slashes (`C:/path/file.db`).
- SSL parameters and special driver arguments can be appended via query params.
- For cloud systems (e.g. Snowflake, BigQuery, Databricks), credentials and tokens can be stored  
in `.env` or a vault.  
- For BigQuery, SQLAlchemy-based reflection is used for schema and column discovery.  
Execution as a target backend is described separately.

---

## 🔧 4. Beta Dialects (Reflection may be limited)

- `trino` / `presto`  
- `clickhouse`  
- `teradata`  
- `db2`  
- `hana`

You can register these manually under `System.type` if you want to experiment — elevata will attempt a  
generic reflection where possible, but may not retrieve primary or foreign key details.

---

## 🔧 5. File-based Sources (RAW Ingestion)

elevata supports file-based sources for native RAW ingestion. The file type is determined via `System.type`.

### 🧩 Supported File Types

| `System.type` | Format | Notes |
|---------------|--------|-------|
| `csv` | CSV with header row | Header row is used as column names |
| `json` | JSON array of objects (`[{...}, {...}]`) | Each element must be an object |
| `jsonl` | Newline-delimited JSON (`{"a":1} {"a":2}`) | One object per line |
| `parquet` | Parquet | Currently: **local path** or `file://` |
| `excel` | Excel (`.xlsx`, `.xlsm`) | Sheet and header configurable |

> RAW landing is **always Full Replace**: Drop/Create/Truncate/Insert.

RAW tables are system-managed landing zones and always include technical columns such as:  
- `load_run_id`  
- `loaded_at`  
- `payload` (preserved JSON object for the original record)

### 🧩 `SourceDataset.ingestion_config` (File)

#### 🔎 Required fields

```json
{
  "uri": "file:///data/orders.parquet"
}
```

- **`uri`** *(string, required)*  
  Path or URI to the file.

  Supported:
  - `file:///...` (recommended)  
  - local paths (e.g. `/data/orders.csv`, `C:/data/orders.xlsx`)  
  - `http(s)://...` for CSV/JSON/JSONL/Excel (Parquet currently not supported over HTTP)

> **Local paths vs. file URIs**
>
> The `uri` field accepts both plain local paths and `file://` URIs.
>
> Valid examples:
>
> - Linux / macOS:  
>   - `/data/orders.csv`  
>   - `file:///data/orders.csv`
>
> - Windows:  
>   - `C:/data/orders.xlsx`  
>   - `file:///C:/data/orders.xlsx`
>
> Using `file:///` is recommended for portability and clarity in configuration files, but it is not strictly required.
>
> Backslashes (`\`) should be avoided. Use forward slashes (`/`) instead.

#### 🔎 Environment variable expansion

`uri` supports environment variable expansion using the `${VAR_NAME}` syntax.

Example:

```json
{
  "uri": "${ELEVATA_INGEST_ROOT}/finance/orders.xlsx"
}
```

### 🧩 `.env` expectations

Recommended pattern:

```text
ELEVATA_INGEST_ROOT=/mnt/dev/inbox
```

Then reference it in metadata:

```json
{
  "uri": "${ELEVATA_INGEST_ROOT}/finance/orders.xlsx"
}
```

This allows using the same metadata across dev/test/prod while switching file roots via `.env`.

#### 🔎 Optional fields

```json
{
  "uri": "https://example.com/download?id=123",
  "file_type": "jsonl"
}
```

- **`file_type`** *(string, optional)*  
  Overrides detection based on file suffix. Useful if the URL does not contain a file extension (e.g. presigned URLs).

  Allowed values: `csv | json | jsonl | parquet | excel`

  **Important:** If set, `file_type` must match `System.type` (to avoid inconsistent configuration).

Detection rules (if `file_type` is not set):  
- `.csv` → CSV  
- `.json` → JSON array  
- `.jsonl` / `.ndjson` → JSON Lines  
- `.parquet` → Parquet  
- `.xlsx` / `.xlsm` → Excel

### 🧩 CSV-specific notes (`System.type = "csv"`)

- CSV must have a header row.  
- Values are read as strings; type inference happens via auto-import / column profiling.

```json
{
  "uri": "file:///data/orders.csv",
  "delimiter": ";",
  "quotechar": "\"",
  "encoding": "utf-8-sig"
}
```

- **`delimiter`** *(string, optional, default: ,)*  
Column separator character.  
Common in German Excel exports: `;`.

- **`quotechar`** *(string, optional, default: \")*  
Character used to quote fields containing delimiters or line breaks.  
Standard CSV uses double quotes (`"`).

- **`encoding`** *(string, optional, default: utf-8)*  
Character encoding used when decoding the file.  
Useful values:
  - utf-8  
  - utf-8-sig (common for Excel exports on Windows)  
  - cp1252 (legacy Windows encoding)

If not specified, elevata assumes UTF-8 encoding and standard CSV quoting.

### 🧩 JSON-specific notes (`System.type = "json"`)

- The file must contain a JSON array of objects.  
- Non-object elements are ignored.

### 🧩 JSONL-specific notes (`System.type = "jsonl"`)

- One JSON object per line.  
- Empty lines are ignored.

### 🧩 Parquet-specific notes (`System.type = "parquet"`)

- Parquet ingestion is **chunked** for memory safety.  
- Currently supported locations: local path or `file://` URI.

### 🧩 Excel-specific Options (`System.type = "excel"`)

```json
{
  "uri": "file:///data/orders.xlsx",
  "sheet_name": "Orders",
  "header_row": 1,
  "max_rows": 50000
}
```

- **`sheet_name`** *(string, optional)*  
  Name of the worksheet. If not set, the first sheet is used.

- **`sheet_index`** *(int, optional)*  
  0-based sheet index (alternative to `sheet_name`).

- **`header_row`** *(int, optional, default: 1)*  
  1-based row number containing the header.

- **`max_rows`** *(int, optional)*  
  Limits the number of data rows read (after the header).

---

## 🔧 6. REST Sources (RAW Ingestion)

For REST APIs, use `System.type = "rest"`.

REST ingestion configuration is split into:

- **System-level REST connection** (environment-specific):  
`base_url` and default headers

- **Dataset-level ingestion_config** (dataset-specific):  
`path`, record extraction, pagination, cursor rules

This keeps metadata stable across dev/test/prod while allowing endpoint switches via secrets.

Important: `base_url` is environment-/system-level configuration and must be provided via secrets  
(e.g. `.env` or vault). It should not be duplicated per dataset.

### 🧩 System-level REST connection (secret)

REST systems resolve connection details via the profile-aware secret reference:

```text
sec/{profile}/conn/rest/{short_name}
```

The resolved secret must provide at least:

- `base_url` *(string, required)*

Optional:

- `headers` *(object)*: default headers included with every request
- `query` *(object)*: fixed query parameters included with every request

Example secret JSON:

```json
{
  "base_url": "https://jsonplaceholder.typicode.com",
  "headers": {"Authorization": "Bearer <TOKEN>"},
  "query": {"locale": "en_US"}
}
```

### 🧩 `.env` expectations

In local development, this is typically backed by `.env` values using the same convention as SQL connection strings.

Example (conceptual) `.env` entry:

```text
SEC_DEV_CONN_REST_JSONPH={"base_url":"https://jsonplaceholder.typicode.com"}
```

Optional default headers and fixed query params may be provided as well:

```text
SEC_DEV_CONN_REST_REQRES={"base_url":"https://reqres.in","headers":{"Authorization":"Bearer <TOKEN>"},"query":{"locale":"en_US"}}
```

> Note: The exact `.env` representation depends on your secret provider.  
> The important contract is that resolving the secret yields a JSON object containing at least `base_url`.

### 🧩 `SourceDataset.ingestion_config` (REST)

#### 🔎 Required fields

```json
{
  "path": "/posts"
}
```

- **`path`** *(string, required)*  
  Endpoint path relative to the system `base_url`.

#### 🔎 Optional fields

```json
{
  "path": "/api/users",
  "method": "GET",
  "headers": {"X-Debug": "1"},
  "params": {"limit": 100},
  "record_path": "data"
}
```

- **`method`** *(string, optional, default: `GET`)*  
  HTTP method.

- **`headers`** *(object, optional)*  
  Request headers added on top of system default headers.

- **`params`** *(object, optional)*  
  Request query parameters added on top of system fixed query parameters.

- **`record_path`** *(string, optional)*  
  Dotted path to the list of records inside the JSON response (e.g. `data.items`).
  If omitted, the response must be a JSON array.

#### 🔎 Pagination (page-based)

```json
{
  "path": "/api/users",
  "page_param": "page",
  "page_size_param": "per_page",
  "page_size": 6,
  "record_path": "data"
}
```

- **`page_param`** *(string, optional)*  
  Name of the page parameter.

- **`page_size_param`** *(string, optional)*  
  Name of the page size parameter.

- **`page_size`** *(int, optional)*  
  Requested page size.

#### 🔎 Incremental scoping (cursor / since)

> Note: RAW remains **Full Replace**. Cursor configuration affects only the **extraction scope**.

```json
{
  "path": "/events",
  "since_param": "since",
  "cursor_field": "updated_at",
  "record_path": "items"
}
```

- **`since_param`** *(string, optional)*  
  Query parameter receiving the delta cutoff value.

- **`cursor_field`** *(string, optional)*  
  Field name used to derive a cursor/max timestamp.

---

© 2025-2026 elevata Labs — Internal Technical Documentation

