# 🗺️ elevata Documentation Overview

Welcome to the **elevata platform documentation** —  
your guide to modern, metadata-driven data & analytics engineering.

---

## 🚀 Getting Started

👉 [Getting Started](getting_started.md)  
Set up your environment, run the first migration, and explore the UI.  

👉 [Secure Metadata Connectivity](secure_metadata_connectivity.md)  
Learn how to configure environment profiles, connect to source systems securely,  
and manage runtime secrets (`.env`, YAML profiles, peppers, etc.).  

---

## 🧩 Metadata Model & Generation Logic

👉 [Generation Logic](generation_logic.md)  
Understand how Target Datasets, Columns, and Surrogate Keys are automatically generated  
from your imported metadata — layer by layer.  

👉 [Incremental Load Architecture](incremental_load.md)  
Learn how elevata performs metadata-driven incremental processing (full + merge),  
handles delete detection, and keeps Rawcore harmonized using lineage.  

👉 [Load SQL Architecture](load_sql_architecture.md)  
Learn how elevata transforms lineage and metadata into executable SQL through the logical plan,  
renderer, and dialect adapters — covering full loads, merge operations, and delete detection.  

---

## 🖌️ SQL Rendering & Logical Plan

👉 [SQL Rendering Conventions](sql_rendering_conventions.md)  
Explore the rendering layer and dialect adapters that translate  
elevata’s logical metadata into executable SQL.  

👉 [Dialect System](dialect_system.md)  
Understand how elevata abstracts vendor-specific SQL behavior (merge, delete detection,  
identifier quoting, hashing, concatenation) into a unified dialect layer.  

👉 [Target Backends](target_backends.md)  
Supported target backends (DuckDB, Snowflake, Databricks, etc.)  
and configuration guidelines.  

---

## 💡 Concepts

👉 [Architecture Overview](architecture_overview.md)  
A high-level walkthrough of how metadata flows through lineage, logical plans, rendering and dialects  
to produce platform-ready SQL.

👉 [Lineage Model & Logical Plan](lineage_and_logical_plan.md)  
elevata builds a metadata-driven Lineage Model that captures every dependency and transformation  
across the platform — turning your data flows into a transparent, navigable logical plan.  

👉 [Metadata Health Check](health_check.md)  
elevata includes a metadata-driven Health Check that automatically detects configuration issues,  
incremental inconsistencies, missing BizCore semantics and materialization errors —  
before they break SQL generation.  

---

## 📥 Source System Integration

👉 [Source Backends](source_backends.md)  
Learn how elevata imports and standardizes metadata from diverse data sources.  

---

## 🧪 Testing & Quality

👉 [Test Setup & Guidelines](tests.md)  
Automated testing ensures long-term reliability and maintainability  
of the metadata generation platform and enables confident releases.  

---

## 🧭 Roadmap & Contribution

The elevata core evolves iteratively.  
For upcoming milestones and progress, check the main repository’s  
[CHANGELOG.md](changelog_ref.md) and [README.md](readme_ref.md).  

---

### 🧡 Tip

> If you’re exploring the metadata model for the first time,  
> start with **Generation Logic**, then check **Incremental Load** and **Dialect System**  
> — they form the backbone of elevata’s loading and SQL rendering pipeline.

---

© 2025 elevata Labs  
Built with purpose. Rendered with precision. 🪶
