# 📚 elevata Documentation Overview

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

---

## 🧱 SQL Rendering & Logical Plan

👉 [SQL Rendering Conventions](sql_rendering_conventions.md)  
Explore the rendering layer and dialect adapters that translate  
Elevata’s logical metadata into executable SQL.

👉 [Target Backends](target_backends.md)  
Supported target backends (DuckDB, Snowflake, Databricks, etc.)  
and configuration guidelines.

---

## 🧰 Source System Integration

👉 [Source Backends](source_backends.md)  
Learn how Elevata imports and standardizes metadata from diverse data sources.

---

## 🧪 Testing & Quality

👉 [Test Setup & Guidelines](tests.md)  
Automated testing ensures long-term reliability and maintainability  
of the metadata generation platform and enables confident releases.

---

## 🧭 Roadmap & Contribution

The Elevata core evolves iteratively.  
For upcoming milestones and progress, check the main repository’s  
[CHANGELOG.md](changelog_ref.md) and [README.md](readme_ref.md).

---

### 🧡 Tip

> If you’re exploring the metadata model for the first time,  
> start with **Generation Logic**, then look at **SQL Rendering**  
> — they form the heart of the Elevata pipeline.

---

© 2025 Elevata Labs  
Built with purpose. Rendered with precision. 🪶
