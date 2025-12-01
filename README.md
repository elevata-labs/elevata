# elevata

<p align="center">
  <img src="https://raw.githubusercontent.com/elevata-labs/elevata/main/docs/logo.png" alt="elevata logo" width="130"/>
</p>

**elevata** is an independent open-source project aiming to make modern data platforms radically simpler.  
It’s designed as a **Declarative Data Architecture & Metadata Framework** — automated, governed, and platform-agnostic.

Instead of manually crafting endless SQL and pipeline code, elevata lets metadata do the work.  
By defining datasets, lineage, and transformation logic declaratively, you can generate consistent, auditable, and future-proof data models — ready to run on your preferred platform.

## License & Dependencies

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://github.com/elevata-labs/elevata/blob/main/LICENSE)
[![Built with Django](https://img.shields.io/badge/Built%20with-Django-092E20?logo=django)](https://www.djangoproject.com/)
[![Frontend: HTMX](https://img.shields.io/badge/Frontend-HTMX-3366CC?logo=htmx)](https://htmx.org/)
[![UI: Bootstrap 5](https://img.shields.io/badge/UI-Bootstrap%205-7952B3?logo=bootstrap)](https://getbootstrap.com/)  

---

## 🧭 What is elevata?

**elevata** transforms metadata into architecture.  
It reads source system metadata, derives logical and physical target structures, and enforces consistent, privacy-compliant data models — automatically.  
It codifies architectural best practices, generates high-quality SQL, and provides a declarative way to build Raw → Stage → Core pipelines.

The goal:  
💡 **Turn metadata into executable, dialect-aware SQL pipelines** — reliably, transparently, and with full lineage.

Where most tools stop at SQL generation, elevata goes further:  
it defines **how a modern data architecture should look** — opinionated, governed, reproducible.  
*In other words: elevata brings structure, governance, and automation to modern data platforms — from metadata to SQL.*

<p align="center">
  <img src="https://raw.githubusercontent.com/elevata-labs/elevata/main/docs/elevata_v0_5_0.png" alt="elevata UI preview" width="700"/>
</p>

**elevata** uses Django models to define:

- **datasets** (sources, raw, stage, core)
- **business keys, surrogate keys, foreign keys**
- **column expressions**
- **incremental strategies**
- **source systems & target systems**
- **dependencies between datasets**

From this metadata, elevata generates:

- clean SQL (`SELECT`, `MERGE`, delete detection)
- surrogate key expressions
- foreign-key lineage expressions
- multi-source stage pipelines
- SQL previews with lineage

Since v0.5.x, elevata includes a complete **LogicalPlan + Expression AST** engine, supporting multiple SQL dialects with deterministic rendering.

---

## 🧩 Architecture Overview

elevata consists of the following layers:

1. **Metadata Layer (Django models)**  
   Defines datasets, columns, lineage, and configuration.

2. **LogicalPlan Layer**  
   Vendor-neutral representation of `SELECT`, `UNION`, subqueries, window functions, column expressions, joins, filters, and more.

3. **Expression AST**  
   Unified representation of literals, column refs, function calls, binary operations, concatenations, and window functions.

4. **SQL Dialects**  
   DuckDB, Postgres, and MSSQL render SQL from the LogicalPlan. Hashing, quoting, and function differences are handled per dialect.

5. **SQL Preview Pipeline**  
   Lineage-aware, dialect-aware, HTMX-enabled preview.

---

## ✨ New in elevata v0.5.0

Version 0.5.0 introduces the largest internal upgrade since the project began.

### 🔧 Multi-Dialect SQL Engine
- New `SqlDialect` base class
- Unified dialect registry & factory
- Built-in dialects:
  - **DuckDB** (reference dialect)
  - **Postgres**
  - **MSSQL**
- Dialect-aware rendering of:
  - identifiers
  - literals
  - functions
  - `CONCAT` / `CONCAT_WS` / `COALESCE`
  - window functions
  - hashing (SHA256, `digest`, `HASHBYTES`)

### 🧠 Hash DSL + Expression Engine Rewrite
A new DSL defines hashing in a vendor-neutral way, for example:

```text
HASH256(
  CONCAT_WS('|',
    CONCAT('productid', '~', COALESCE({expr:productid}, 'null_replaced')),
    'pepper'
  )
)
```

This DSL is parsed into an expression AST and rendered differently per dialect.  
It replaces the previous string-templated SQL approach.

### 🔄 Surrogate & Foreign Key Pipeline Rewrite
- SK and FK expressions are now AST-based
- Child-side FK hashing is aligned with parent SK logic
- Identical execution semantics across DuckDB/Postgres/MSSQL
- High test coverage for deterministic output

### 🔁 Multi-Source Stage Rewrite
- **Identity mode** → simple `UNION ALL`
- **Non-identity mode** → `ROW_NUMBER()` ranking with subqueries
- No duplicate rows
- Fully AST-driven
- All Stage tests green

### 🖥️ SQL Preview Modernisation
- HTMX live refresh
- Dialect dropdown selector
- Clean formatting via beautifier
- Accurate lineage rendering

---

## 🔧 How SQL is generated in v0.5.0

SQL is built from structured, deterministic building blocks instead of hand-written strings:

1. **Metadata → LogicalPlan**  
2. **LogicalPlan → Expression AST**  
3. **Dialect renderer → SQL**  
4. **Beautifier → final formatting**

Benefits:

- reliable identifier quoting
- safe hashing & concatenation
- vendor-neutral function handling
- identical semantics across databases
- clean and readable SQL for review

---

## 🌐 SQL Dialect Support

| Dialect | Status | Notes |
|---------|--------|-------|
| DuckDB  | Stable | Canonical implementation |
| Postgres | Stable | uses `digest()` + `encode()` for hashing |
| MSSQL | Stable | uses `HASHBYTES` + `CONVERT` |

All dialects produce bit-identical HASH256 results for surrogate and foreign keys.  
All dialects support  

- literal rendering
- column reference rendering
- function calls
- window functions
- subqueries in `FROM`
- deterministic hashing

You can extend elevata with custom dialects via:

```python
from metadata.rendering.dialects import register_dialect
register_dialect(MyDialect)
```

---

## 📚 Example Workflow

1. Define your metadata in Django admin  
2. Inspect lineage in the dataset detail view  
3. Select the SQL dialect in the preview section  
4. Review generated SQL  
5. Run load pipelines via CLI (basic runner available; extended runner planned for v0.6.x)

---

## 📦 Load Runner CLI (preview)

elevata currently ships with a minimal `elevata_load` management command.  
A more powerful Load Runner CLI is planned for v0.6.x, including:

- dry-run mode  
- profiling  
- dialect selection  
- dependency graph execution  

---

## 🔮 Roadmap

### v0.5.0 (current release)
- Multi-dialect SQL engine  
- MSSQL + Postgres dialects  
- Hash DSL  
- SK/FK hashing rewrite  
- Multi-source Stage rewrite  
- SQL Preview modernisation  

### v0.6.x
- Full Load Runner CLI  
- Target system selector in profiles  
- Improved incremental MERGE generation  
- SCD/History-ready pipelines  

### v0.7.x
- Multi-source incremental loads  
- Additional dialects (Snowflake, BigQuery, Databricks)  
- Full pipeline orchestrator  

---

## 🛡️ Data Privacy (GDPR/DSGVO)

elevata itself does not require personal data.  
If used with customer datasets, responsibility for compliance remains with the implementing organisation.  
The system supports pseudo-key hashing and consistent anonymisation strategies via its hashing DSL.

---

### 🚧 Note

elevata is currently in an early preview phase (v0.x).
Breaking changes may occur before the 1.0 release as the metadata model stabilizes.

---

## Disclaimer

This project is an independent open-source initiative.  
- It is not a consulting service.  
- It is not a customer project.  
- It does not store or process customer data.  
- It is not in competition with any company.  

The purpose of elevata is to contribute to the community by providing a metadata-driven framework for building data platforms.  
The project is published under the AGPL v3 license and open for use by any organization.

---

## 🧾 License & Notices

© 2025 Ilona Tag — All rights reserved.  
**elevata™** is an open-source software project for data & analytics innovation.  
The name *elevata* is currently under trademark registration with the German Patent and Trade Mark Office (DPMA).  
Other product names, logos, and brands mentioned here are property of their respective owners.

Released under the **GNU Affero General Public License v3 (AGPL-3.0)**.  
See [`LICENSE`](https://github.com/elevata-labs/elevata/blob/main/LICENSE) for terms and [`NOTICE.md`](https://github.com/elevata-labs/elevata/blob/main/NOTICE.md) for third-party license information.