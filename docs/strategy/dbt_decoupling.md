# 🧩 decoupling from dbt – strategic outline

**Last updated:** 2025-10-26  
**Applies from:** elevata 0.2.3 onward

---

The original motivation behind elevata’s dbt compatibility was pragmatic:  
re-use dbt’s SQL rendering and model execution capabilities without reinventing them.  
But as elevata evolved, this dependency became limiting.

## why we decouple

dbt focuses on compiling SQL models;  
elevata focuses on defining *data architecture* — declaratively, governably, and platform-agnostically.

While dbt excels at orchestrating transformations, elevata’s mission is higher-level:  
to **generate governed SQL through architecture — not configuration**.

## new direction

elevata introduces a **Meta-SQL Layer** — a logical plan that captures *what* each dataset represents,  
*how* it is derived, and *which relationships and keys* define it.  
This layer is platform-neutral and can later be rendered into Snowflake SQL, BigQuery SQL, Databricks SQL, Fabric, or even dbt syntax — without binding elevata to any of them.

> elevata can integrate with dbt — but does not depend on it.

The decoupling allows elevata to control:
- metadata generation and lineage tracking,
- deterministic key and relationship management,
- governance primitives (sensitivity, ownership, access intent),
- and deployment across structured architecture layers (`raw`, `stage`, `rawcore`, `bizcore`, `serving`).

## raw as first-class layer

In contrast to dbt’s generic “staging” concept, elevata defines an explicit `raw` layer:  
a transparent landing zone that preserves source data 1:1 for auditability and re-loadability.  
From there, the system generates technical cores (`rawcore`) and business models (`bizcore`) automatically.

This explicit layer separation provides:
- clear lineage between ingestion and semantic models,
- reproducibility of technical transformations,
- and predictable governance behavior per layer.

## long-term vision

elevata will:
- maintain optional dbt compatibility for users who rely on its runner,
- provide its own rendering layer for direct execution,
- and evolve toward a **platform-independent metadata compiler** that can describe, generate, and govern complete data architectures.

Ultimately, elevata’s goal is not to replace dbt —  
but to **make dbt optional** by moving the intelligence where it belongs:  
into metadata-driven, declarative architecture.
