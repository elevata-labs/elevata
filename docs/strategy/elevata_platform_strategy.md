# ⚙️ elevata Platform Strategy  
## From SQL-Centric Pipelines to Architecture Runtime and Business Semantics

> **Document type:** Strategy  
> **Last updated:** 2026-08  
> **Applies since:** elevata ≥ 2.0  

---

## 🔧 1. Motivation

Modern data platforms increasingly rely on SQL-based transformation tools to express not only technical data processing, but also business logic, execution semantics, and governance intent.

This tight coupling leads to:

- implicit execution behavior  
- limited explainability  
- fragmented governance  
- strong dependency on external orchestration and semantic layers

elevata addresses these limitations by treating **metadata - not SQL - as the primary control plane** for data platforms.

This control plane covers source-to-target metadata generation, structure, lineage, execution semantics, review decisions, controlled execution, and audit evidence.

---

## 🔧 2. Architecture Runtime

An **Architecture Runtime** executes architecture declaratively.

Instead of describing architecture only in documents, an Architecture Runtime renders, validates, and enforces it through executable metadata.

In traditional data platforms, architecture is often expressed implicitly through SQL pipelines, orchestration configuration, and BI models.

elevata makes architecture explicit.

Datasets, dependencies, execution semantics, and business intent are defined as structured metadata and compiled into deterministic execution plans.

In this sense, elevata acts as an **Architecture Runtime for modern data platforms**.

Architecture is not only modeled. It is reviewed, approved, executed, and recorded through deterministic runtime artifacts.

---

## 🔧 3. elevata as Architecture Runtime

elevata is not a SQL generator with metadata around it.

It is an **Architecture Runtime** that turns metadata-defined architecture into deterministic plans, controlled execution, and auditable runtime records.

Key characteristics:

- deterministic Target Generation Plans and Source-to-Target Reviews  
- separate Generation and Architecture Approval boundaries  
- deterministic execution planning  
- explicit dependency graphs  
- structured failure semantics (blocked vs aborted)  
- batch-level execution identity (`batch_run_id`)  
- execution snapshots and run-level observability  
- deterministic Architecture State  
- Architecture Change Reports  
- Architecture Approval Artifacts  
- Architecture Catalog  
- scope-aware Architecture Control  
- Architecture Execution Records

Execution is:

- planned explicitly  
- executed deterministically  
- explainable independently of SQL rendering  
- reviewable before execution  
- auditable after controlled execution

SQL is an output artifact - not the orchestration mechanism.

Architecture Control closes the loop between architecture intent and runtime execution:

```text
Source Metadata
  ↓
Target Generation Plan + Review
  ↓
Generation Approval Artifact, when required
  ↓
Guarded Target Metadata Apply
  ↓
Architecture State
  ↓
Architecture Change Report
  ↓
Architecture Approval Artifact
  ↓
Execution Preview
  ↓
Controlled Execution
  ↓
Architecture Execution Record
```

This makes architecture a governed runtime contract rather than a convention hidden inside transformation scripts.

Architecture Catalog provides the read-only discovery layer for this contract: it shows what exists, how it is connected, how it is controlled, and where execution evidence is available.

---

## 🔧 4. Decoupling Business Logic from SQL

Traditional SQL-centric pipelines embed business meaning directly into SQL:

- calculations are implicit  
- intent is inferred  
- governance happens after execution

elevata separates concerns by making **business intent explicit and declarative**, while keeping execution deterministic and warehouse-native.

Business rules, dataset dependencies, schema evolution intent, and execution control are expressed through metadata and runtime artifacts rather than inferred from SQL text.

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

Bizcore defines *what data means* - not *how queries should behave*.

Bizcore execution remains part of the same Architecture Runtime:

- lineage determines execution order  
- metadata determines generated SQL  
- Architecture Control determines review and execution readiness  
- Execution Records capture controlled runtime outcomes

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
- controlled target metadata generation  
- separate metadata-mutation and physical-execution approval boundaries  
- architecture review and approval  
- controlled execution  
- auditable execution records  
- warehouse-native processing

elevata becomes a **platform backbone**, not a transformation tool.

External tools (orchestrators, BI platforms) integrate with elevata, but do not define execution, semantics, or governance.

They consume or trigger architecture that is defined, controlled, and recorded inside elevata.

---

## 🔧 8. Strategic Direction

elevata is a **metadata-native data platform engine**: a system where structure, execution, governance, and business intent are derived from explicit definitions rather than implicit SQL behavior.

The goal is not to replace orchestration frameworks or BI tools, but to provide a reliable, transparent core that makes data platforms predictable, governable, and evolvable over time.

Architecture Runtime is the central category:

```text
Metadata defines architecture.
Architecture Control governs generation and execution.
Generation and Architecture Approvals protect separate control boundaries.
Execution Records preserve audit evidence.
SQL remains an artifact.
```
