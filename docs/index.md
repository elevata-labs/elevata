# 🧭 elevata Documentation Index

Welcome to the **elevata Labs Documentation Hub** —  
your single source of truth for metadata-driven data & analytics automation.

This index gives you an overview of all major topics and how they fit together.

---

## 🗺️ Table of Contents

### 🚀 Getting Started

- [Getting Started](getting_started.md)  
  Install elevata, run the first migration, and open the UI.

- [Secure Metadata Connectivity](secure_metadata_connectivity.md)  
  Configure profiles, environment variables, secrets, peppers and secure access to source/target systems.

---

### 🧩 Metadata Model & Generation

- [Generation Logic](generation_logic.md)  
  How metadata is transformed into Logical Plans and final SQL.  
  Includes dataset types (RAW, STAGE, CORE, …), dependencies and generation rules.

- [Incremental Load Architecture](incremental_load.md)  
  Incremental patterns, MERGE semantics, deletion handling, and how elevata models change propagation.  

- [Load SQL Architecture](load_sql_architecture.md)  
  How elevata transforms lineage and metadata into executable SQL through the logical plan,  
  renderer, and dialect adapters — covering full loads, merge operations, and delete detection.  

- [Historization Architecture](historization_architecture.md)  
  Complete SCD Type 2 historization model: versioning, change detection, deletion,  
  surrogate keys, lineage-based attribute mapping, and SQL generation.  

---

### 🎨 SQL Rendering & Dialects

- [SQL Rendering Conventions](sql_rendering_conventions.md)  
  General rules for how SQL is formatted and rendered (identifiers, literals, ordering, readability).

- [Dialect System](dialect_system.md)  
  Overview of the dialect abstraction, the dialect registry, and how DuckDB, Postgres and MSSQL are implemented.

- [Target Backends](target_backends.md)  
  Which engines are supported and how they fit into a Lakehouse or Warehouse architecture.

---

### 💡 Concepts

- [Architecture Overview](architecture_overview.md)  
  High-level view of elevata’s architecture: metadata, lineage, Logical Plan, rendering and execution.

- [Lineage Model & Logical Plan](logical_plan.md)  
  How datasets depend on each other, how lineage is represented, and how the Logical Plan encodes queries.

- [Expression DSL & AST](expression_dsl_and_ast.md)  
  The vendor-neutral expression DSL (Domain Specific Language) (HASH256, CONCAT_WS, COALESCE, COL, …), the AST (Abstract Syntax Tree), and how dialects render it.

- [Hashing Architecture](hashing_architecture.md)  
  Surrogate key and foreign key hashing: deterministic rules, cross-dialect SHA-256, null handling and pepper strategy.

- [SQL Preview & Rendering Pipeline](sql_preview_pipeline.md)  
  How the UI builds previews from metadata, Logical Plan and dialect selection (HTMX-based).

- [Metadata Health Check](health_check.md)  
  Built-in checks for incomplete or inconsistent metadata, and how to interpret them.

---

### 🌐 Source Integration

- [Source Backends](source_backends.md)  
  Overview of supported source systems and how to configure them (JDBC/ODBC, file-based, etc.).

---

### ✅ Testing & Quality

- [Test Setup & Guidelines](tests.md)  
  How the core test suite is structured, how to add tests for new features, and how to reason about coverage.

---

### 📦 Project

- [Main Project README](readme_ref.md)  
  The top-level README from the Git repository (architecture, goals, roadmap).

- [Changelog](changelog_ref.md)  
  Release history

---

## 🧭 Where to start?

If you are new to elevata, a good reading path is:

1. [Getting Started](getting_started.md)  
2. [Architecture Overview](architecture_overview.md)  
3. [Generation Logic](generation_logic.md)  
4. [Dialect System](dialect_system.md)  
5. [Hashing Architecture](hashing_architecture.md)  

This will give you a mental model for how metadata flows through the platform and becomes executable SQL.

---

### 🧡 About

elevata Labs builds metadata-centric tooling for modern data platforms —  
bridging semantics, governance and automation in one ecosystem.

> Designed for engineers. Loved by analysts.  
> **elevata: clarity through metadata.**

---

👩‍💻 **Created and maintained by [Ilona Tag](https://www.linkedin.com/in/ilona-tag-a96ab1124)**  
A personal open-source initiative exploring the future of declarative data architecture.

---

_Last updated: 2025-12-04_

© 2025 elevata Labs  
Built with purpose. Rendered with precision. 🪶
