# ⚙️ Controlled Target Generation

Controlled Target Generation makes Source-to-Target metadata mutation explicit, inspectable, approvable, and drift-guarded.

It does not redesign the existing generator semantics. It projects those semantics into immutable artifacts before any TargetDataset, TargetColumn, or input metadata is changed.

---

## 🔧 1. Purpose

Traditional target generation often combines decision and mutation in one action:

```text
Inspect source metadata → immediately mutate target metadata
```

elevata separates these concerns:

```text
Inspect source and target metadata
  ↓
Build immutable Target Generation Plan
  ↓
Review Source-to-Target impact
  ↓
Approve the exact metadata mutation, when required
  ↓
Apply the exact reviewed plan with drift guards
  ↓
Validate convergence through a residual plan
```

The result is a controlled metadata boundary before Architecture State, schema evolution, and load execution.

---

## 🔧 2. Contract Boundary

A Target Generation Plan is one of several distinct plan types in elevata:

| Artifact | Purpose | Mutation or execution |
|---|---|---|
| Target Generation Plan | Describe TargetDataset, TargetColumn, and input metadata changes | Metadata mutation |
| SQL Logical Plan | Describe vendor-neutral query structure | SQL rendering |
| Execution Impact Plan | Classify runtime work as reuse, incremental execution, rebuild, revalidation, or blocked | Read-only execution decision |
| Execution Run Plan | Bind one exact scheduler run to reviewed architecture and ordered executable decisions | Scheduler execution contract |

A Target Generation Plan never contains physical DDL, DML, or load steps.

---

## 🔧 3. Target Generation Plan

A Target Generation Plan is canonical and immutable. It contains:

- artifact and generator contract versions  
- schema scope  
- selected SourceDataset keys  
- lifecycle reconciliation mode  
- source metadata fingerprint  
- target metadata fingerprint  
- ordered actions and action counts  
- deterministic plan fingerprint

Every action contains:

- action type  
- stable dataset and object keys  
- effect origin  
- change classification  
- reason  
- canonical before state  
- canonical after state

### 🧩 3.1 Action Types

The plan supports explicit actions for:

- `CREATE_TARGET_DATASET`  
- `UPDATE_TARGET_DATASET`  
- `RETIRE_TARGET_DATASET`  
- `REACTIVATE_TARGET_DATASET`  
- `CREATE_TARGET_COLUMN`  
- `UPDATE_TARGET_COLUMN`  
- `RETIRE_TARGET_COLUMN`  
- `REACTIVATE_TARGET_COLUMN`  
- `SYNC_TARGET_DATASET_INPUTS`  
- `SYNC_TARGET_COLUMN_INPUTS`

### 🧩 3.2 Effect Origins

Effects distinguish why an action exists:

- `DIRECT` - direct result of the selected generation semantics  
- `HISTORY_COMPANION` - companion effect for a generated history dataset or column  
- `GENERATED_LIFECYCLE` - retirement or reactivation of generator-owned metadata  
- `MODEL_SIDE_EFFECT` - deterministic model or signal behavior that is part of the existing generation contract

### 🧩 3.3 Change Classifications

Actions are classified as:

- `ADDITIVE`  
- `BREAKING`  
- `NEUTRAL`

The classification summarizes review impact. It does not replace the canonical before and after state, which remains the authoritative description of the planned mutation.

---

## 🔧 4. Read-Only Planning and Dry-Run Parity

Planning never writes metadata.

The planner reuses pure decisions from the existing Target Generation Service and projects:

- generated datasets and columns  
- SourceDataset and TargetDataset inputs  
- SourceColumn and TargetColumn inputs  
- technical columns  
- surrogate-key and business-key metadata  
- rawcore history companions  
- generated lifecycle retirement and reactivation  
- rename and `former_names` effects

Dry-run renders the same immutable plan later consumed by guarded apply. There is no separate preview interpretation.

CLI example:

```bash
python manage.py generate_targets \
  --schema raw \
  --dry-run \
  --plan-output .artifacts/target_generation_plan_raw.json \
  --review-output .artifacts/target_generation_review_raw.json
```

The CLI is primarily an adapter for debugging, CI, and explicit automation. The regular user workflow is available in Architecture Control.

---

## 🔧 5. Source-to-Target Generation Review

The Target Generation Review summarizes the plan for a reviewer.

It contains:

- exact Target Generation Plan fingerprint  
- source and target metadata fingerprints  
- action counts by classification  
- effect-origin counts  
- affected SourceDatasets and TargetDatasets  
- Source-to-Target impact groups  
- deterministic review fingerprint  
- whether approval is recommended

Architecture Control shows human-readable dataset, source, and column labels. Stable internal keys remain available in downloaded JSON artifacts for audit, automation, and support.

The plan JSON remains authoritative. The Review is a deterministic decision aid over that exact plan.

---

## 🔧 6. Generation Approval

A Generation Approval authorizes one exact Target Generation Review and Plan.

It records:

- `target_generation_approval` artifact type  
- `gpa_...` approval identifier  
- plan fingerprint  
- review fingerprint  
- scope  
- reviewer identity  
- decision timestamp  
- optional review note  
- deterministic artifact fingerprint

Breaking generation changes require a matching Generation Approval in the Architecture Control UI. Additive and neutral changes can be applied without one, but remain exact-plan and drift-guarded. A reviewer may still create an optional approval for those changes.

CLI example:

```bash
python manage.py elevata_generation_approve \
  .artifacts/target_generation_plan_raw.json \
  --approved-by "Reviewer Name" \
  --note "Source-to-Target impact reviewed." \
  --store
```

Generation Approval artifacts are stored under:

```text
.elevata/approvals/<profile>/<target-system>/generation/
  <review-fingerprint>.generation.approval.json
```

### 🧩 6.1 Separate Approval Boundaries

Generation Approval is not Environment Promotion Approval and is not Architecture Approval.

```text
Generation Approval
  = authorizes exact Target metadata mutation inside one environment

Environment Promotion Approval
  = authorizes one exact release-to-target metadata deployment

Architecture Approval
  = authorizes the resulting Architecture Change Report
    for physical schema evolution and execution readiness
```

These approval boundaries cannot substitute for one another. See [Environment Promotion](environment_promotion.md) for the cross-environment deployment boundary.

The artifact types, identifiers, fingerprints, and storage lookups are intentionally separate.

---

## 🔧 7. Guarded Apply

Guarded apply consumes one previously reviewed Target Generation Plan.

Immediately before mutation, elevata rebuilds the current plan and validates:

- source metadata fingerprint  
- target metadata fingerprint  
- generation decisions  
- exact approval binding when approval is supplied or required

If any of these changed, apply fails before target metadata mutation begins.

CLI examples:

```bash
python manage.py generate_targets \
  --plan-file .artifacts/target_generation_plan_raw.json
```

Require a matching stored Generation Approval:

```bash
python manage.py generate_targets \
  --plan-file .artifacts/target_generation_plan_raw.json \
  --require-generation-approval
```

Validate an explicit approval file:

```bash
python manage.py generate_targets \
  --plan-file .artifacts/target_generation_plan_raw.json \
  --generation-approval-file .artifacts/target_generation_approval_raw.json \
  --require-generation-approval
```

The apply result records:

- applied plan fingerprint  
- generation review fingerprint  
- Generation Approval identifier, when used  
- consumed action count  
- residual action count and fingerprint  
- processed datasets and columns  
- retired and reactivated datasets  
- convergence state

---

## 🔧 8. Residual Plans and Convergence

After applying the reviewed plan, elevata builds a new plan from the resulting metadata state.

```text
Residual action count = 0
  → converged

Residual action count > 0
  → review the residual plan before another guarded apply
```

This keeps existing multi-pass semantics explicit. Examples include:

- rawcore technical-column ordinal convergence  
- multi-source withdrawal where lineage and input synchronization converge before `union → single` mode change

A residual plan receives a new plan and review fingerprint. A previous Generation Approval cannot authorize it.

---

## 🔧 9. Generated-Layer Sequence

Generated target layers depend on each other:

```text
RAW
  ↓
STAGE
  ↓
RAWCORE
```

Architecture Control therefore does not create three independent long-lived plans and apply them blindly. It guides users through a sequence:

1. Review and converge RAW.  
2. Recalculate, review, and converge STAGE.  
3. Recalculate, review, and converge RAWCORE.

The all-dataset generation view shows:

- each generated layer  
- action count or up-to-date state  
- the next safe review step  
- downstream recalculation state

Only the first pending layer is actionable. If an upstream layer is pending, downstream approval and apply are blocked server-side as well as in the UI.

---

## 🔧 10. UI Workflow

The primary user entry points are:

- **Source Datasets → Review target generation**  
- **Architecture Control → All datasets**  
- **Architecture Control → Schema: raw, stage, or rawcore**

The SourceDataset action does not mutate target metadata directly. It opens the same all-dataset Architecture Control overview used by direct navigation.

For an actionable schema, the UI provides:

- plan, classification, source, and target summary  
- Source-to-Target impact table  
- expandable action inspection  
- plan and review JSON downloads  
- Generation Approval creation and verification  
- guarded apply  
- progress feedback during review navigation, approval, and apply  
- apply result and residual-plan feedback

An empty plan displays `not required` for Generation Approval and does not expose an irrelevant approval check.

---

## 🔧 11. Lifecycle, History, and Rename Effects

Controlled planning includes the existing generator-owned lifecycle and model behavior.

### 🧩 Lifecycle

Complete schema generation can retire a generator-owned TargetDataset when it leaves the eligible source scope. Retirement is metadata-only:

- `active=False`  
- `retired_at` is recorded  
- physical deletion is not performed  
- the dataset is excluded from active execution

If the source becomes eligible again, the same metadata object is reactivated in place.

### 🧩 History Companions

Rawcore history effects are represented explicitly as `HISTORY_COMPANION` actions. Base and history datasets remain linked through lineage-safe identifiers and column inputs.

### 🧩 Renames

Rename behavior preserves `former_names`. Existing model signals continue to propagate rawcore column renames to history companion metadata. Planning exposes the expected result without writing it; guarded apply allows the existing signal behavior to occur and validates the residual state after commit.

---

## 🔧 12. Relationship to Architecture Control and Execution

Controlled Target Generation is the first control boundary, not a replacement for the existing Architecture Control Plane.

```text
Source Metadata
  ↓
Controlled Target Generation
  ↓
Target Metadata
  ↓
Architecture State + Architecture Change Report
  ↓
Architecture Approval
  ↓
Execution Impact + Execution Preview
  ↓
Controlled or Scheduler Execution
  ↓
Architecture Execution Record and Finalized State
```

Target generation changes metadata only. Physical tables and columns are created or evolved later by the load runner through schema evolution preflight, policy checks, Architecture Guard, dialect-owned DDL, and controlled execution.

---

## 🔧 13. Guarantees

Controlled Target Generation guarantees:

- no metadata writes during planning or review  
- canonical, deterministic plan and review fingerprints  
- dry-run and guarded-apply parity  
- explicit Source-to-Target impact  
- exact approval binding  
- source, target, and decision drift rejection  
- visible residual plans and convergence  
- dependency-ordered generated-layer guidance  
- preservation of existing generator semantics  
- separation from physical execution approval

---

## 🔧 14. Non-Goals

Controlled Target Generation does not:

- redesign RAW, STAGE, RAWCORE, history, lifecycle, or multi-source semantics  
- apply physical DDL  
- load target data  
- replace Architecture Change Reports or Architecture Approval  
- turn SourceDataset review into a wizard  
- hide multi-pass convergence  
- infer changes through AI or heuristics

---

## 🔧 15. Related Documents

- [Generation Logic](generation_logic.md)  
- [Architecture Control Plane](architecture_control_plane.md)  
- [Architecture Overview](architecture_overview.md)  
- [Schema Evolution](schema_evolution.md)  
- [Determinism & Execution Semantics](determinism_and_execution_semantics.md)  
- [Load Execution & Orchestration Architecture](load_execution_architecture.md)

---

© 2025-2026 elevata - Technical Documentation
