# ⚙️ elevata Platform Strategy  
## From SQL-Centric Pipelines to Metadata-Native Execution and Business Semantics

> **Document type:** Strategy  
> **Last updated:** 2026-01  
> **Applies since:** elevata ≥ 0.8  

---

## 🔧 1. Motivation

Modern data platforms increasingly rely on SQL-based transformation tools  
to express not only technical data processing,  
but also business logic, execution semantics, and governance intent.

This tight coupling leads to:  
- implicit execution behavior  
- limited explainability  
- fragmented governance  
- strong dependency on external orchestration and semantic layers

elevata addresses these limitations by treating **metadata — not SQL —  
as the primary control plane** for data platforms.

---

## 🔧 2. Architecture Runtime

An **Architecture Runtime** executes architecture declaratively.

Instead of describing architecture only in documents, an Architecture Runtime  
renders, validates, and enforces it through executable metadata.

In traditional data platforms, architecture is often expressed implicitly  
through SQL pipelines, orchestration configuration, and BI models.

elevata makes architecture explicit.

Datasets, dependencies, execution semantics, and business intent are  
defined as structured metadata and compiled into deterministic execution plans.

In this sense, elevata acts as an **Architecture Runtime for modern data platforms**.

---

## 🔧 3. elevata Today

With the introduction of an explicit execution model,  
elevata has evolved beyond SQL generation into a **platform execution core**.

Key characteristics:  
- deterministic execution planning  
- explicit dependency graphs  
- structured failure semantics (blocked vs aborted)  
- batch-level execution identity (`batch_run_id`)  
- execution snapshots and run-level observability

Execution is now:  
- planned explicitly  
- executed deterministically  
- explainable independently of SQL rendering

SQL is an output artifact — not the orchestration mechanism.

---

## 🔧 4. Decoupling Business Logic from SQL

Traditional SQL-centric pipelines embed business meaning directly into SQL:  
- calculations are implicit  
- intent is inferred  
- governance happens after execution

elevata separates concerns by making **business intent explicit and declarative**,  
while keeping execution deterministic and warehouse-native.

---

## 🔧 5. Bizcore: Business Semantics without a Semantic Layer

Bizcore introduces a dedicated layer for **business meaning**.

It allows teams to define:  
- business datasets  
- business rules and classifications  
- business calculations and KPIs expressed as dataset fields

These definitions are:  
- metadata-driven  
- deterministic  
- compiled into execution plans  
- independent of SQL dialects and BI tools

Bizcore is **not**:  
- a BI semantic layer  
- a metric store  
- a query-time metrics engine  
- a templating or macro system

Bizcore defines *what data means* — not *how queries should behave*.

---

## 🔧 6. Separation of Responsibilities

| Layer | Responsibility |
|------|----------------|
| RAW / STAGE / CORE | Technical correctness, historization, lineage |
| BIZCORE | Business meaning, rules, calculations |
| SERVING (optional) | Consumption- or tool-specific shaping |

This separation ensures:  
- centralized business logic  
- deterministic execution  
- tool independence  
- governance based on intent, not inference

---

## 🔧 7. Strategic Outcome

By combining:  
- metadata-native execution  
- explicit business semantics  
- warehouse-native processing

elevata becomes a **platform backbone**, not a transformation tool.

External tools (orchestrators, BI platforms) integrate with elevata,  
but do not define execution, semantics, or governance.

---

## 🔧 8. Long-Term Vision

elevata aims to become a **metadata-native data platform engine**:  
a system where structure, execution, governance, and business intent  
are derived from explicit definitions rather than implicit SQL behavior.  

The goal is not to replace orchestration frameworks or BI tools,  
but to provide a reliable, transparent core that makes data platforms  
predictable, governable, and evolvable over time.
