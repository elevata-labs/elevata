# 🧩 Strategy Note — Gradual Decoupling from dbt

**Last updated:** 2025-10-20  
**Applies from:** elevata 0.2.x onward  

---

## 🎯 Objective

elevata currently plans to integrate with dbt Core for rendering and execution of SQL models.  
While dbt has served as a convenient engine during the early stages, the recent **merger of dbt Labs and Fivetran** and the strategic shift toward **dbt Fusion** make long-term reliance on dbt uncertain.

Therefore, elevata will **gradually decouple** from dbt while **remaining compatible** for teams that still rely on it.  
The final goal is a **native metadata-driven rendering and execution layer** that provides full independence and stable long-term maintenance.

---

## 🧱 Strategic Principles

1. **Compatibility first**  
   dbt integration remains available as an adapter until the native engine reaches feature parity.  
   Users can keep their dbt pipelines intact while exploring the native path.

2. **Abstraction layer instead of hard dependency**  
   All dbt-specific code lives behind clear interfaces:  
   - `RenderService` (SQL generation)  
   - `Runner` (execution logic)  
   - `DAGBuilder` (dependency resolution)  

3. **Metadata as single source of truth**  
   Both dbt and the native renderer read from the same metadata models (`SourceDataset`, `TargetDataset`, etc.).  
   No dbt-specific metadata or YAML duplication will be introduced.

4. **Incremental replacement, not rewrite**  
   Each release replaces small dbt features with native equivalents (rendering → execution → tests → lineage).  
   dbt support is deprecated only after the final feature group reaches stability.

---

## 🚀 Implementation Plan

| Phase | Focus | Notes |
|-------|--------|-------|
| **0.2.x** | Introduce renderer and resolver foundation (already underway) | dbt folder remains unchanged |
| **0.3.x** | Add native materializations (`view`, `table`, `incremental`) | compatible with dbt schema |
| **0.4.x** | Implement DAG resolver and execution runner | metadata-driven, SQLAlchemy-based |
| **0.5.x** | Parity milestone — full native execution path | dbt becomes optional adapter |
| **1.0.0** | Officially independent, dbt adapter moved to optional plugin | stable LTS support |

---

## 🔒 Communication Strategy

- **README:**  
  Mention “dbt-compatible but not dependent” (neutral tone).  
  Avoid statements about dbt’s future; focus on elevata’s flexibility.

- **Roadmap:**  
  Add bullet: “Gradual decoupling from dbt — native rendering and execution engine.”

- **Changelog:**  
  Mention only when native rendering is introduced (e.g., *“Native renderer added — dbt now optional”*).

---

## ⚙️ Technical Direction

- SQL generation will use `render_sql()` from metadata (Jinja optional).  
- Execution via SQLAlchemy for relational sources.  
- Environment & profile management through `elevata_profiles.yaml` (replaces `profiles.yml`).  
- Built-in support for surrogate keys, lineage, and PII metadata.  
- Optional plug-ins: `DbtAdapter`, `FusionAdapter`, future connectors.

---

## 🧭 Rationale

- Maintain **long-term independence** from vendor ecosystems.  
- Ensure **sustainable open-source evolution** even if dbt Core stagnates.  
- Simplify deployment — no external CLI, no package dependencies.  
- Provide a unified experience across relational and non-SQL sources.  

---

## 🧾 Summary

| Goal | Status |
|------|---------|
| Decouple dbt dependency | In progress |
| Maintain compatibility | Guaranteed through 0.x line |
| Native renderer | Introduced in 0.3.x |
| Native runner | Target 0.4.x |
| Full independence | 1.0.0 milestone |

---

> **In short:** elevata stays dbt-friendly — but builds its own foundation.  
> This ensures long-term autonomy, transparency, and control over the full metadata-to-execution pipeline.
