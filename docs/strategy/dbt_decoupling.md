# 🧩 Strategy Note — Runtime Independence and dbt Compatibility

**Last updated:** 2025-10-25  
**Applies from:** elevata 0.2.x onward

---

## 🎯 Objective

elevata defines, stores, and manages all platform-relevant metadata (sources, targets, lineage, keys, governance) natively.  
This metadata is the single source of truth for how data should be shaped and moved.

Transformation frameworks like dbt can consume that metadata — but they are no longer required to run elevata.

In other words: elevata is designed to operate independently, while still being able to export or interoperate with external engines where teams already invested in them.

---

## 🧱 Strategic Principles

1. **Independence by default**  
   elevata ships without any runtime dependency on dbt.  
   You do not need a dbt project, `profiles.yml`, or dbt CLI to use elevata.

2. **Compatibility where useful**  
   Teams with an existing dbt landscape can still generate dbt-friendly artefacts or run downstream in dbt if they choose to.  
   This is provided as an adapter layer, not as core infrastructure.

3. **Metadata as the source of truth**  
   All logic for datasets, columns, surrogate keys, lineage references, environment profiles, and sensitivity classification lives in elevata models.  
   No dbt-specific duplication (YAML etc.) is required.

4. **Replace piece by piece — but start from our side**  
   Instead of “wrapping dbt”, elevata focuses on native capabilities first:  
   - generate target structures, including PK and surrogate key propagation  
   - define join relationships and lineage  
   - attach governance/sensitivity information  
   - plan execution using connection profiles  
   External runners (like dbt) become consumers of that plan, not owners of it.

---

## 🚀 Execution Roadmap

| Phase | Focus | Notes |
|-------|-------|-------|
| **0.3.x** | Native target model generation & lineage-aware relationships | Surrogate keys with pepper, FK references, sensitivity metadata |
| **0.4.x** | Native SQL rendering & execution planning | SQL produced directly from elevata metadata; runs via SQLAlchemy |
| **0.5.x** | Full runnable path without external tooling | Incremental/table/view materialization, scheduling hooks |
| **1.0.0** | Stable, tool-independent runtime | dbt available only as an optional integration plugin |

---

## 🔒 Communication Guidance

- **README:**  
  Say: “elevata is fully functional on its own. External engines (for example dbt) are optional adapters.”

- **Changelog / Roadmap:**  
  Frame milestones in terms of elevata-native features.  
  Mention dbt only as a compatibility/export path, not as a core runtime.

- **Env / Config:**  
  No `DBT_*` variables, no bundled `dbt_project/` folder, no implication that dbt is required to start.

---

## ⚙️ Technical Direction

- SQL rendering is driven by elevata metadata (`TargetDataset`, `TargetColumn`, lineage relationships, surrogate keys).
- Execution planning is handled natively and can connect to relational systems via SQLAlchemy.
- Connection / credential handling is resolved via `elevata_profiles.yaml` and environment variables (or Key Vault).
- Sensitive data is explicitly classified, and surrogate key hashing with runtime-only pepper ensures compliance (e.g. DSGVO).
- External adapters (e.g. dbt) can receive generated artefacts for teams that want to keep their established tooling, but elevata’s runtime does not depend on them.

---

## 🧭 Rationale

- Avoid vendor lock-in and sudden strategy shifts outside our control.
- Keep elevata maintainable and sustainable as an open project.
- Support mixed landscapes (relational DBs, flat files, REST sources) where dbt alone is not enough.
- Make governance, lineage, and security first-class instead of bolted-on.

---

## 🧾 Summary

| Goal | Status |
|------|--------|
| Operate independently of dbt | Effective starting 0.2.x cleanup |
| Keep optional compatibility | Yes, via adapter/export |
| Native renderer and runner | Rolling out in 0.3.x / 0.4.x |
| Full autonomy | 1.0.0 milestone |

---

> In short: elevata runs on elevata.  
> External engines are welcome guests — but not landlords.
