# Supported Source Backends

This document provides an overview of supported database platforms, tested SQLAlchemy dialects, and connection URI examples for **elevata**.

---

## 🧩 General Pattern

All connections use the standard **SQLAlchemy URI** format:
```
dialect[+driver]://username:password@host:port/database[?params]
```
For example:
```
mssql+pyodbc://user:pwd@server,1433/db?driver=ODBC%20Driver%2018%20for%20SQL%20Server
```
In your `.env`, define these values using the **elevata secret convention**:

```
SEC_<ENVIRONMENT>_CONN_<TYPE>_<SHORTNAME>=<SQLAlchemy URI>
```
Example:  
```
SEC_DEV_CONN_MSSQL_SAP=mssql+pyodbc://user:pwd@sql01,1433/SAPDB?driver=ODBC%20Driver%2018%20for%20SQL%20Server
```

---

## ✅ Fully Supported Backends

| Type | Label | Driver / Package | Reflection Support | Example URI |
|------|--------|------------------|--------------------|-------------|
| `mssql` | Microsoft SQL Server | `pyodbc` | ✅ Full | `mssql+pyodbc://user:pwd@host,1433/db?driver=ODBC%20Driver%2018%20for%20SQL%20Server` |
| `postgresql` | PostgreSQL | `psycopg2` | ✅ Full | `postgresql+psycopg2://user:pwd@localhost:5432/dbname` |
| `mysql` | MySQL / MariaDB | `mysqlclient` or `pymysql` | ✅ Full | `mysql+pymysql://user:pwd@localhost:3306/db` |
| `sqlite` | SQLite | built-in | ✅ Full | `sqlite:///C:/data/dev.db` |
| `oracle` | Oracle | `python-oracledb` | ✅ Columns & PKs | `oracle+oracledb://user:pwd@host:1521/servicename` |
| `snowflake` | Snowflake | `snowflake-sqlalchemy` | ✅ Columns only | `snowflake://user:pwd@account/DB/SCHEMA?warehouse=WH` |
| `redshift` | Amazon Redshift | `sqlalchemy-redshift` | ✅ Columns only | `redshift+psycopg2://user:pwd@redshift:5439/dev` |
| `bigquery` | Google BigQuery | `pybigquery` | ✅ Columns only | `bigquery://project-id` |
| `duckdb` | DuckDB | `duckdb-engine` | ✅ Full | `duckdb:///C:/data/local.duckdb` |
| `databricks` | Databricks SQL (Beta) | `sqlalchemy-databricks` | ⚠️ Columns only | `databricks://token:<TOKEN>@<host>?http_path=/sql/1.0/warehouses/<id>` |

---

## ⚙️ Connection Notes

- Use **URL-encoded driver names** (e.g. `ODBC%20Driver%2018%20for%20SQL%20Server`).
- Backslashes in paths should be replaced by slashes (`C:/path/file.db`).
- SSL parameters and special driver arguments can be appended via query params.
- For cloud systems (e.g. Snowflake, BigQuery, Databricks), credentials and tokens can be stored in the `.env` or a Key Vault.

---

## 🧪 Beta Dialects (Reflection may be limited)

- `trino` / `presto`
- `clickhouse`
- `teradata`
- `db2`
- `hana`

You can register these manually under `SourceSystem.type` if you want to experiment — elevata will attempt a generic reflection, but may not retrieve primary/foreign key details.

---

## 🚧 Future Roadmap

Upcoming additions:

- Azure Fabric & Synapse (via MSSQL dialect)
- REST & Flat-file connectors
- Structured (non-URI) connection support: JSON / KeyVault references

---

*Last updated: October 2025*
