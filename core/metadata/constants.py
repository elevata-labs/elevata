"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025 Ilona Tag

This file is part of elevata.

elevata is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of
the License, or (at your option) any later version.

elevata is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with elevata. If not, see <https://www.gnu.org/licenses/>.

Contact: <https://github.com/elevata-labs/elevata>.
"""

TYPE_CHOICES = sorted([
  # SQLAlchemy-supported (auto import)
  ("mssql", "SQL Server"),
  ("postgresql", "PostgreSQL"),
  ("mysql", "MySQL / MariaDB"),
  ("sqlite", "SQLite"),
  ("oracle", "Oracle"),
  ("snowflake", "Snowflake"),
  ("redshift", "Amazon Redshift"),
  ("bigquery", "Google BigQuery"),
  ("duckdb", "DuckDB"),
  ("databricks", "Databricks SQL"),

  # Beta / limited SQLAlchemy reflection
  ("trino", "Trino (beta, limited reflection)"),
  ("presto", "Presto (beta, limited reflection)"),
  ("clickhouse", "ClickHouse (beta, limited reflection)"),

  # Manual / no SQLAlchemy reflection yet
  ("teradata", "Teradata (manual, no reflection)"),
  ("db2", "IBM DB2 (manual, no reflection)"),
  ("hana", "SAP HANA (manual, no reflection)"),
  ("sapr3", "SAP R/3 (manual, no reflection)"),
  ("exasol", "Exasol (manual, no reflection)"),

  # File-based
  ("csv", "CSV File(s) (manual, no reflection)"),
  ("parquet", "Parquet File(s) (manual, no reflection)"),
  ("excel", "Excel / XLSX File(s) (manual, no reflection)"),
  ("json", "JSON File(s) (manual, no reflection)"),
  ("jsonl", "JSON Lines File(s) (manual, no reflection)"),

  # API-based
  ("rest", "REST API (manual, no reflection)"),
  ("graphql", "GraphQL API (manual, no reflection)"),

  # Cloud / Transport layers
  ("s3", "Amazon S3 Storage (manual, no reflection)"),
  ("gcs", "Google Cloud Storage (manual, no reflection)"),
  ("azureblob", "Azure Blob Storage (manual, no reflection)"),
  ("sftp", "SFTP Location (manual, no reflection)"),
], key=lambda x: x[1])

DIALECT_HINTS = {
  # SQLAlchemy-supported
  "mssql": "Driver: ODBC Driver 18 (pyodbc). Install `sqlalchemy` + `pyodbc`. Example in docs/source_backends.md.",
  "postgresql": "Driver: psycopg2. Install `sqlalchemy[postgresql]`.",
  "mysql": "Driver: mysqlclient or pymysql.",
  "sqlite": "Built-in SQLite; perfect for local tests.",
  "oracle": "Driver: python-oracledb.",
  "snowflake": "Install `snowflake-sqlalchemy`. Requires account, warehouse, and database parameters.",
  "redshift": "Install `sqlalchemy-redshift`. PostgreSQL-compatible dialect.",
  "bigquery": "Install `pybigquery`. Supports ADC and service-account auth.",
  "duckdb": "Install `duckdb-engine`. Local or remote connections supported.",
  "databricks": "Install `sqlalchemy-databricks`. Use SQL Warehouse endpoints. Reflection may be limited.",

  # Beta / limited
  "trino": "Install `sqlalchemy-trino`. Beta; reflection partially supported.",
  "presto": "Install `sqlalchemy-presto`. Beta; reflection limited.",
  "clickhouse": "Install `clickhouse-sqlalchemy`. Beta; limited type mapping.",

  # Manual (document-only)
  "teradata": "Manual only; no SQLAlchemy dialect. Schema import not yet supported.",
  "db2": "Manual only; IBM DB2 dialect incomplete. Schema import not supported.",
  "hana": "Manual only; use SAP HANA SQL over ODBC/JDBC bridge if needed.",
  "sapr3": "Manual only; SAP R/3 typically via IDoc, RFC, or flat extracts.",
  "exasol": "Manual only; reflection not supported yet.",

  # File-based
  "csv": "Manual only; specify path or pattern in SourceDataset. Schema inference planned.",
  "parquet": "Manual only; specify folder/prefix. Auto schema detection planned.",
  "excel": "Manual only; specify workbook and sheet name. Adapter planned.",
  "json": "Manual only; specify path or pattern. Schema inference planned.",
  "jsonl": "Manual only; specify folder or file pattern. Adapter planned.",

  # API-based
  "rest": "Manual only; document endpoint and auth. REST adapter planned.",
  "graphql": "Manual only; document endpoint and query. Adapter planned.",

  # Cloud / Transport
  "s3": "Manual only; specify bucket and prefix. Used for file ingestion.",
  "gcs": "Manual only; specify bucket and path. Used for file ingestion.",
  "azureblob": "Manual only; specify container and path. Used for file ingestion.",
  "sftp": "Manual only; specify host and path. Used for file ingestion.",

  # Default fallback
  "default": "No specific notes. See docs/source_backends.md for examples.",
}

INGEST_CHOICES = [
  ("none", "None"), 
  ("external", "External"), 
  ("native", "Native")
]

INCREMENT_INTERVAL_CHOICES = sorted([
  ("year", "Year"), 
  ("month", "Month"), 
  ("day", "Day")
], key=lambda x: x[1])

DATATYPE_CHOICES = sorted([
  ("STRING", "String"), 
  ("INTEGER", "Integer"), 
  ("BIGINT", "Big Integer"), 
  ("DECIMAL", "Decimal"), 
  ("FLOAT", "Float"), 
  ("BOOLEAN", "Boolean"), 
  ("DATE", "Date"), 
  ("TIME", "Time"), 
  ("TIMESTAMP", "Timestamp"),
  ("BINARY", "Binary"), 
  ("UUID", "UUID"), 
  ("JSON", "JSON")
], key=lambda x: x[1])

PII_LEVEL_CHOICES = [
  ("none", "No personal data"),
  ("personal_data", "Personal data"),
  ("special_category_data", "Special category personal data"),
]

ACCESS_INTENT_CHOICES = [
  ("internal_reporting", "Internal Reporting"),
  ("analytics", "Analytics / BI"),
  ("ml_training", "Machine Learning / AI"),
  ("compliance", "Regulatory / Compliance"),
  ("exploration", "Ad-hoc Exploration"),
  ("integration", "System Integration"),
]

ROLE_CHOICES = sorted([
  ("business", "Business Owner"), 
  ("technical", "Technical Owner"), 
  ("steward", "Data Steward")
], key=lambda x: x[1])

SENSITIVITY_CHOICES = [
  ("public", "Public"),
  ("internal", "Internal"),
  ("confidential", "Confidential"),
  ("highly_confidential", "Highly Confidential")
]

MATERIALIZATION_CHOICES=[
  ("table", "table"),
  ("view", "view"),
  ("incremental", "incremental"),
  ("external_passthrough", "external_passthrough"),
]

RELATIONSHIP_TYPE_CHOICES=[
  ("n_to_1", "n to 1"),
  ("1_to_n", "1 to n"),
  ("1_to_1", "1 to 1"),
  ("n_to_m", "n to m (future)"),
]

ENVIRONMENT_CHOICES=[
  ("dev", "Development"),
  ("test", "Test"),
  ("prod", "Production"),
]

LINEAGE_ORIGIN_CHOICES=[
  ("direct", "Direct"),
  ("derived", "Derived"),
  ("lookup_join", "Lookup Join"),
  ("constant", "Constant"),
  ("surrogate_key", "Surrogate Key"),
  ("foreign_key", "Foreign Key"),
]

TARGET_COMBINATION_MODE_CHOICES=[
  ("single", "Single upstream (no combination)"),
  ("union", "Union of multiple upstream datasets"),
  # later: ("join", "Join multiple upstream datasets"),
  # later: ("manual", "Custom SQL defined by user"),  
]

TARGET_DATASET_INPUT_ROLE_CHOICES=[
  ("primary", "Primary (golden source)"),
  ("enrichment", "Enrichment (same entity, more attributes)"),
  ("reference_lookup", "Reference Lookup / Code table"),
  ("audit_only", "Audit / Technical Metadata only"),
]

BIZ_ENTITY_ROLE_CHOICES=[
  ("core_entity", "Core entity"),
  ("fact", "Fact table"),
  ("dimension", "Dimension"),
  ("reference", "Reference / master data"),
]

INCREMENTAL_STRATEGY_CHOICES = [
  ("full", "Full load (truncate + reload)"),
  ("append", "Append-only incremental load"),
  ("merge", "Merge (upsert by business key, handle deletes)"),
  ("snapshot", "Snapshot-based load"),
]

# Auto import via SQLAlchemy (stable)
SUPPORTED_SQLALCHEMY = {
  "mssql","postgresql","mysql","sqlite","oracle",
  "snowflake","redshift","bigquery","duckdb","databricks",
}

# Beta (limited reflection)
BETA_SQLALCHEMY = {
  "trino","presto","clickhouse",
}

# Manual/documentation-only (no reflection)
MANUAL_TYPES = {
  "teradata","db2","hana","sapr3","exasol",
  "csv","parquet","excel","json","jsonl",
  "rest","graphql",
  "s3","gcs","azureblob","sftp",
}

# Optional: human labels for badges
TYPE_SUPPORT_LABEL = {
  "auto": "Auto import",
  "beta": "Beta",
  "manual": "Manual only",
}

# Optional: bootstrap badge classes
TYPE_BADGE_CLASS = {
  "auto": "bg-success",
  "beta": "bg-warning text-dark",
  "manual": "bg-secondary",
}

def classify_type(code: str) -> str:
  """Return 'auto' | 'beta' | 'manual' for a given type code."""
  if code in SUPPORTED_SQLALCHEMY:
    return "auto"
  if code in BETA_SQLALCHEMY:
    return "beta"
  return "manual"
