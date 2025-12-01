# 🗺️ elevata Documentation Overview

Welcome to the **elevata platform documentation** —  
your guide to modern, metadata-driven data & analytics engineering.

This documentation explains how elevata turns metadata into clean, dialect-aware SQL pipelines.

---

## 🚀 Getting Started

👉 [Getting Started](getting_started.md)  
Install elevata, run the first migration, log into the UI and explore the main concepts.

👉 [Secure Metadata Connectivity](secure_metadata_connectivity.md)  
Configure environment profiles, secrets and peppers. Learn how to connect source and target systems securely.

---

## 🧩 Metadata Model & Generation

👉 [Generation Logic](generation_logic.md)  
Describes how RAW, STAGE, CORE and other dataset types are modeled, and how the generator builds Logical Plans and SQL from them.

👉 [Incremental Load Architecture](incremental_load.md)  
Details the incremental loading patterns: MERGE semantics, soft deletes, change capture and how elevata tracks updates.

👉 [Load SQL Architecture](load_sql_architecture.md)  
Learn how elevata transforms lineage and metadata into executable SQL through the logical plan,  
renderer, and dialect adapters — covering full loads, merge operations, and delete detection.  

---

## 🎨 SQL Rendering & Dialects

👉 [SQL Rendering Conventions](sql_rendering_conventions.md)  
How elevata renders SQL: identifier quoting, literal handling, SELECT/UNION layout and formatting rules.

👉 [Dialect System](dialect_system.md)  
Introduces the dialect abstraction and the dialect factory. Explains how DuckDB, Postgres and MSSQL share the same expression AST but render different SQL.

👉 [Target Backends](target_backends.md)  
Overview of supported backends (DuckDB, SQL Server, Postgres and future engines like Snowflake, Databricks, BigQuery) and how they fit into a Lakehouse architecture.

---

## 💡 Concepts & Internals

👉 [Architecture Overview](architecture_overview.md)  
High-level view of elevata’s layers: metadata, lineage, Logical Plan, expression AST, dialect rendering and execution.

👉 [Lineage Model & Logical Plan](logical_plan.md)  
How dataset relationships, dependencies and load order are modeled, and how the Logical Plan encodes queries independent of any SQL dialect.

👉 [Expression DSL & AST](expression_dsl_and_ast.md)  
Explains the vendor-neutral expression language used for hashing and column expressions (HASH256, CONCAT_WS, COALESCE, COL, `{expr:...}`) and how it is parsed into an AST.

👉 [Hashing Architecture](hashing_architecture.md)  
Deep dive into the surrogate key and foreign key hashing engine introduced in v0.5.x:  
deterministic ordering, null handling, pepper, and cross-dialect SHA-256 rendering.

👉 [SQL Preview & Rendering Pipeline](sql_preview_pipeline.md)  
How the UI builds SQL previews from metadata, Logical Plan and dialect selection, using HTMX for live refresh.

👉 [Metadata Health Check](health_check.md)  
Overview of built-in metadata validation rules and how to interpret warnings and errors.

---

## 🌐 Source Integration

👉 [Source Backends](source_backends.md)  
Which source systems can be read from, how to configure them and how they show up in lineage and generation logic.

---

## ✅ Testing & Quality

👉 [Test Setup & Guidelines](tests.md)  
How to run the test suite, how to add coverage for new features and how the core tests validate Logical Plans, expressions and SQL rendering.

---

## 📦 Project & Releases

👉 [Main Project README](readme_ref.md)  
The primary README from the repository — goals, positioning, roadmap and architectural intent.

👉 [Changelog](changelog_ref.md)  
Release notes for each version, including the v0.5.x multi-dialect engine and hashing/Stage rewrites.

---

### 🧡 Recommended Reading Path

> If you’re exploring elevata’s metadata model and SQL engine for the first time,  
> start with **Generation Logic**, then read **Incremental Load**, **Dialect System** and **Hashing Architecture**.  
> Together they form the backbone of elevata’s loading and SQL rendering pipeline.

---

© 2025 elevata Labs  
Built with purpose. Rendered with precision. 🪶
