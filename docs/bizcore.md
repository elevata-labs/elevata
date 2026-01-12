# ⚙️ Bizcore — Business Semantics by Design

Bizcore is elevata’s dedicated layer for modeling **business meaning,  
rules, and calculations** as first-class metadata.

It enables teams to define business logic *inside the data platform* —  
without introducing a BI semantic layer, metric store, or query-time abstraction.

---

## 🔧 Why Bizcore exists

Most data platforms blur responsibilities:

- dbt mixes technical transformations and business logic
- semantic layers resolve meaning at query time
- metric stores decouple KPIs from execution logic

Bizcore takes a different approach.

`RAW / STAGE / CORE` → technical truth  
`BIZCORE` → business meaning & rules  
`SERVING` (optional) → consumer-specific shaping

Bizcore exists to make **business semantics explicit, deterministic,  
and executable** — not inferred later.

---

## 🔧 What Bizcore is

Bizcore datasets are:

- first-class datasets
- defined entirely via metadata
- executed through the same deterministic pipeline as all other layers
- fully lineage-aware and explainable

Bizcore fields can represent:

- business identifiers
- domain-specific attributes
- classifications and flags
- KPIs and business calculations
- normalized business rules

All Bizcore definitions are:

- metadata-driven
- compiled into logical plans
- rendered into real SQL
- executed in the warehouse

There is no semantic shortcut.

---

## 🔧 Bizcore joins & multi-source modeling

Bizcore **explicitly supports joins** — including **multi-source joins**.

This is a deliberate design choice.

Bizcore datasets may:

- join multiple Core datasets
- enrich technical entities with business context
- combine facts and dimensions
- express domain relationships explicitly

Join semantics are:

- modeled as metadata
- validated at planning time
- rendered into deterministic SQL
- fully visible in SQL previews

This allows modeling business concepts like:

- Customer (from multiple operational systems)
- Account + Person + Address relationships
- Business entities spanning domains or sources

without introducing a separate semantic modeling layer.

---

## 🔧 What Bizcore is *not*

By design, Bizcore is **not**:

- a BI semantic layer
- a metric store
- a query-time calculation engine
- a macro or templating system

There is no late binding.
There is no runtime resolution.
There is no hidden logic.

What you define is what executes.

---

## 🔧 Execution & lineage

Bizcore uses:

- the same logical planner
- the same dependency graph
- the same execution engine
- the same retry and failure semantics

Every Bizcore field is traceable to:

- its Core inputs
- its joins
- its transformations
- its assumptions

This enables:

- impact analysis
- auditability
- explainable business logic
- governance without guesswork

---

## 🔧 Mini tutorial: A Bizcore dataset in practice

**Goal:**  
Model a business-level `Customer` entity derived from multiple Core datasets.

### 🧩 Inputs
- `core.customer`
- `core.person`
- `core.address`

### 🧩 Bizcore definition
- Join customer → person
- Left join address
- Define business identifiers
- Define business attributes
- Define derived fields

### 🧩 Result
- A single Bizcore dataset
- Expressing business meaning explicitly
- With full SQL preview
- With full lineage back to Core
- Executable like any other dataset

Bizcore logic becomes **part of the platform**, not part of a BI tool.

---

## 🔧 Design intent

Bizcore makes elevata **business-capable by design**.

Business logic becomes:

- explicit
- inspectable
- deterministic
- executable
- governable

without coupling meaning to tools, queries, or runtime semantics.

Bizcore is not an add-on.

It is a foundational layer.

---

© 2025-2026 elevata Labs — Internal Technical Documentation