# ⚙️ Architecture Control Plane

The Architecture Control Plane governs deterministic target generation and physical architecture review/execution inside one metadata environment. Cross-environment metadata deployment is handled by the separate Environment Promotion workflow.

It turns architecture state into explicit artifacts:

- Target Generation Plan  
- Target Generation Review  
- Generation Approval Artifact  
- Architecture State  
- Architecture Change Report  
- Architecture State Comparison (`Architecture Promotion Report`)  
- Architecture Approval Artifact  
- Execution Impact Plan  
- immutable Execution Run Plan  
- Planned Architecture State  
- scheduler-step outcomes and finalization evidence  
- Architecture Execution Record  
- deterministic artifact fingerprints

These artifacts make structural architecture changes reviewable, policy-aware, approvable, verifiable, executable through controlled scopes or schedulers, visible in the UI, and suitable for CI pipelines.

Architecture Catalog complements the control plane as a read-only discovery surface. It shows how datasets are defined, connected, controlled and linked to execution evidence without creating approvals or executing loads.

---

## 🔧 1. Purpose

elevata treats architecture as executable metadata.

The Architecture Control Plane defines the review and comparison layer around that metadata:

```text
Source Metadata
  ↓
Target Generation Plan
  ↓
Target Generation Review
  ↓
Generation Approval Artifact, when required
  ↓
Guarded Target Metadata Apply
  ↓
Architecture State
  ↓
Architecture Diff
  ↓
MigrationPlan
  ↓
Policy Decisions
  ↓
Architecture Change Report
  ↓
Architecture Review Briefing
  ↓
Architecture Approval Artifact
  ↓
Execution Impact Plan
  ↓
Execution Run Plan + Planned Architecture State
  ↓
Controlled or Scheduler Execution
  ↓
Structured Outcomes + Finalization
  ↓
Applied Architecture State + Architecture Execution Record
```

This makes schema evolution and execution intent explicit before load execution applies any DDL or DML, and keeps state persistence bound to the architecture that was actually executed.

---

## 🔧 2. Controlled Target Generation

Controlled Target Generation governs mutation of generated TargetDataset, TargetColumn and input metadata before that metadata becomes Architecture State and physical execution intent.

It reuses the existing generator semantics and adds an explicit control contract around them:

```text
Plan
  ↓
Review
  ↓
Approve, when required
  ↓
Guarded Apply
  ↓
Residual Plan or Converged State
```

The immutable Target Generation Plan contains:

- schema scope and lifecycle reconciliation mode  
- SourceDataset keys  
- source and target metadata fingerprints  
- ordered dataset, column and input actions  
- canonical before and after state  
- direct, history companion, generated lifecycle and model-side-effect origins  
- additive, breaking and neutral classifications  
- deterministic plan fingerprint

Planning is read-only. Dry-run, JSON output, Architecture Control preview and guarded apply all use the same plan contract.

A Target Generation Review summarizes SourceDataset-to-TargetDataset impact and receives its own deterministic review fingerprint. Breaking changes require a matching Generation Approval in the UI; additive and neutral changes remain approval-optional but are still drift-guarded.

Generation Approval, Environment Promotion Approval and Architecture Approval are separate decisions:

| Approval | Authorizes | Bound to | Identifier |
|---|---|---|---|
| Generation Approval | Target metadata mutation inside one environment | Target Generation Review and Plan | `gpa_...` |
| Environment Promotion Approval | Release-to-target metadata deployment between environments | Environment Promotion Plan | `papr-...` |
| Architecture Approval | Physical architecture change and execution readiness | Architecture Change Report | `apr_...` |

A Generation Approval cannot authorize warehouse DDL, DML or load execution. An Environment Promotion Approval cannot authorize the target environment's physical Architecture Change Report. After metadata converges in a target environment, that target independently enters the Architecture Approval and execution workflow. See [Environment Promotion](environment_promotion.md).

Architecture Control guides complete generated-layer convergence in dependency order:

```text
RAW review and apply
  ↓
STAGE recalculation, review and apply
  ↓
RAWCORE recalculation, review and apply
```

Only the first pending layer is actionable. A downstream preview is provisional and cannot be approved or applied until its upstream generated layer has converged.

For the complete contract and UI workflow, see [Controlled Target Generation](controlled_target_generation.md).

---

## 🔧 3. Architecture State

Architecture State is a deterministic snapshot of the metadata-defined platform architecture.

It contains:

- datasets  
- columns  
- materialization semantics  
- incremental strategy  
- historization metadata  
- lineage-safe rename metadata  
- stable fingerprints

The command:

```bash
python manage.py elevata_state
```

renders the architecture state as deterministic JSON.

To write a state artifact:

```bash
python manage.py elevata_state --output .artifacts/dev_architecture_state.json
```

To print only the fingerprint:

```bash
python manage.py elevata_state --fingerprint-only
```

The persisted runtime baseline directory is configured via:

```bash
ELEVATA_ARCH_STATE_DIR=.elevata/state
```

The load runner builds the current Architecture State during execution planning and uses it for architecture diffing, MigrationPlan derivation, impact planning, and guard checks. The current state represents the active metadata contract; inactive TargetDatasets are excluded from executable state.

The persisted runtime baseline represents the architecture that was successfully applied to one profile and target system. Scheduler-managed runs do not persist whatever metadata happens to be current at finalization time. They persist the immutable Planned Architecture State that was bound to the Execution Run Plan.
 
When no recorded baseline exists, a first state can be established only through a full-scope initial deployment whose managed target was verified as empty through read-only physical discovery. A guarded recovery path exists for legacy interrupted initial deployments that completed before Planned Architecture State snapshots were introduced.

Dry-run persistence is controlled via:

```bash 
ELEVATA_PERSIST_ARCH_STATE_ON_DRY_RUN=false 
```

---

## 🔧 4. Architecture Change Report

An Architecture Change Report describes the difference between a baseline state and the metadata-defined architecture state.

The report includes:

- state fingerprints  
- affected dataset scope  
- dataset changes  
- column changes  
- MigrationPlan actions  
- policy decisions  
- deterministic report fingerprint

Report scope is part of the report contract.

| Invocation | Scope mode | Scope meaning |
|---|---|---|
| `elevata_plan --all` | `all` | all active target datasets |
| `elevata_plan --all --schema rawcore` | `scoped` | all active target datasets in the selected schema |
| `elevata_plan rc_aw_customer` | `scoped` | the selected dataset and related architecture scope |

For scoped reports, the report payload contains only changes, migration actions, policy decisions and summary counts that belong to the selected scope. The report fingerprint therefore represents the selected architecture scope.

Architecture Control UI schema reviews use a broader review-key resolution than execution. They combine current active datasets in the schema with datasets from the previous Architecture State that belonged to the same schema. This keeps generated dataset retirement visible as an approvable architecture change even though the retired dataset is no longer selectable or executable.

Dataset retirement is represented atomically:

```text
DATASET_REMOVED
  ↓
RETIRE_DATASET
  ↓
METADATA_ONLY
```

A removed dataset does not emit one top-level `COLUMN_REMOVED` change per former column, and metadata-only retirement does not imply automatic physical deletion.

When a target dataset name is unique, `--schema` can be omitted. Use `--schema` when the same dataset name exists in multiple schemas or when CI scripts should declare the intended schema explicitly.

Render a report for one dataset:

```bash
python manage.py elevata_plan rc_aw_customer
```

Render a report for one dataset with an explicit schema:

```bash
python manage.py elevata_plan rc_aw_customer --schema rawcore
```

Render a report for all datasets in a schema:

```bash
python manage.py elevata_plan --all --schema rawcore
```

Render JSON for CI:

```bash
python manage.py elevata_plan --all --format json
```

Use an explicit baseline state file:

```bash
python manage.py elevata_plan rc_aw_customer \
  --previous-state .artifacts/prod_architecture_state.json
```

---

## 🔧 5. Architecture Approval Artifact

An Architecture Approval Artifact records a review decision for one exact Architecture Change Report fingerprint.

It answers:

```text
Has this exact architecture change report been reviewed and approved?
```

Approval artifacts are deterministic JSON artifacts. They bind a review decision to the report fingerprint, report scope, report state, summary counts, and policy status of the approved Architecture Change Report.

An approval does not override policy decisions. If a report contains blocking policy decisions, execution remains blocked by the load runner and materialization policy.

Approval artifacts are created from Architecture Change Report JSON:

```bash
python manage.py elevata_plan rc_aw_customer \
  --format json \
  --output .artifacts/architecture_plan_rc_aw_customer.json

python manage.py elevata_approve .artifacts/architecture_plan_rc_aw_customer.json \
  --approved-by "Reviewer Name" \
  --note "Reviewed for deployment." \
  --output .artifacts/architecture_approval_rc_aw_customer.json
```

To store the approval artifact in the configured approval directory:

```bash
python manage.py elevata_approve .artifacts/architecture_plan_rc_aw_customer.json \
  --approved-by "Reviewer Name" \
  --note "Reviewed for deployment." \
  --store
```

The approval artifact directory is configured via:

```bash
ELEVATA_ARCH_APPROVAL_DIR=.elevata/approvals
```

The stored file name is derived from the approved report fingerprint:

```text
<report_fingerprint>.approval.json
```

To verify that an approval artifact matches an Architecture Change Report:

```bash
python manage.py elevata_approval_check \
  .artifacts/architecture_plan_rc_aw_customer.json \
  .elevata/approvals/<profile>/<target-system>/<report_fingerprint>.approval.json
```

The approval check fails when:

- the approval artifact fingerprint does not match its payload  
- the approval identifier does not match the artifact fingerprint  
- the approval references another Architecture Change Report  
- the review decision is not `approved`

---

## 🔧 6. Architecture Review Status UI

The Architecture Review Status UI shows the review state for a selected TargetDataset architecture scope.

It displays:

- review status  
- report fingerprint  
- approval identifier and artifact fingerprint  
- reviewer and decision timestamp  
- policy status  
- architecture scope  
- change summary  
- state fingerprints

Review states include approved, pending review, approval drift, blocked by policy, no architecture changes, and invalid approval artifact.

---

## 🔧 7. Architecture Control UI

The Architecture Control UI makes Architecture Control Plane workflows operable across controlled architecture scopes.

It supports:

- all-dataset generated-layer overview  
- RAW, STAGE and RAWCORE generation review scopes  
- all-dataset architecture review and execution scopes  
- schema architecture review and execution scopes  
- TargetDataset scopes  
- TargetDataset scopes with target-only execution

### 🧩 7.1 Architecture Review Briefing

Architecture Review Briefing is a compact, deterministic decision aid inside Architecture Control.

It summarizes what a reviewer should understand before approving or executing the selected scope:

- selected scope and scope size  
- review state  
- architecture change counts  
- policy attention  
- destructive or blocking migration signals  
- execution readiness  
- suggested reviewer focus

The briefing is derived from existing Architecture Control signals: the current scoped Architecture Change Report, the Architecture Review Status and the Execution Preview. It is read-only and does not create approvals, run approval checks, execute loads or mutate metadata.

The UI keeps the briefing compact by showing the main reviewer signals first. Detailed sections are available on demand through an expandable detail area.

### 🧩 7.2 Review Scope and Execution Scope

Architecture review and execution intentionally resolve different dataset sets:

```text
Schema review scope
= current active schema datasets
  ∪ previous-state datasets from the same schema

Execution scope
= active TargetDatasets only
```

The TargetDataset selector continues to show active datasets only. An inactive generated dataset therefore cannot be selected as an execution root. Its retirement remains visible through the schema or full review scope, can require an Approval Artifact, and is excluded from Execution Preview, Execution Impact, manifests, and Run Plans.

This separation prevents a retired metadata object from silently re-entering execution while preserving explicit review of the architecture contract change.

### 🧩 7.3 Execution Impact Plan and Immutable Run Plan

Execution Impact evaluates the exact active execution scope and classifies each dataset as:

- `REUSE`  
- `INCREMENTAL_EXECUTE`  
- `FULL_REBUILD`  
- `REVALIDATE`  
- `BLOCKED`

Only executable decisions can become an immutable Execution Run Plan. `REVALIDATE` and `BLOCKED` prevent Run Plan creation.

Create and store a full-scope scheduler plan:

```bash
python manage.py elevata_run_plan \
  --all-datasets \
  --output .artifacts/full.run_plan.json
```

The Run Plan binds the exact dataset order, dependency mode, review status, approval identifier, Architecture State fingerprint, report fingerprint, Execution Impact fingerprint, Execution Preview fingerprint, and ExecutionPlan fingerprint. It is stored together with a matching Planned Architecture State snapshot.

Each scheduler step validates its dataset metadata against that snapshot before execution and writes one structured outcome artifact. Finalization requires one semantically valid outcome for every planned dataset:

```bash
python manage.py elevata_finalize_run_plan \
  .artifacts/full.run_plan.json
```

Successful finalization persists exactly the Planned Architecture State applied by that run. Metadata changes detected after Run Plan creation are reported as post-plan drift and belong to a new run. They do not invalidate a successfully completed older plan and do not redefine it silently.

For a legacy interrupted initial deployment without a Planned Architecture State snapshot, recovery is available only through the explicit `--recover-interrupted-initial-deployment` option. Recovery validates complete outcomes, absence of another recorded state, current metadata scope, and the physical target architecture before writing state and recovery evidence.

The Architecture Control UI first provides controlled actions for target metadata generation:

- inspect the complete RAW, STAGE and RAWCORE sequence  
- inspect Source-to-Target impact and action classifications  
- inspect canonical before and after state  
- download Target Generation Plan and Review JSON  
- create and check Generation Approval Artifacts  
- apply the exact reviewed plan with drift guards  
- inspect apply evidence, convergence and residual plans

It then provides controlled actions for architecture artifacts and execution:

- show the scoped Architecture Change Report  
- download the scoped Architecture Change Report as JSON  
- create an Architecture Approval Artifact  
- check the stored Approval Artifact against the report  
- refresh the Architecture Review Status  
- inspect the Architecture Review Briefing  
- inspect the Execution Impact Plan  
- inspect the Execution Preview  
- inspect controlled reference readiness signals in the Execution Preview  
- run controlled load execution  
- inspect captured execution output  
- inspect the Architecture Execution Record  
- browse stored Architecture Execution Records  
- filter execution history by scope, status, date range and dependency mode  
- download stored execution record JSON  
- apply retention cleanup for older execution records

Approval Artifact creation records the logged-in reviewer, the decision timestamp, and an optional review note. The artifact is stored in the configured approval artifact directory and is bound to the report fingerprint.

Approval checks compare the stored Approval Artifact with the current scoped Architecture Change Report and surface the result in the UI.

Controlled execution runs through the load runner. It does not bypass preflight validation, materialization policy checks, Architecture Guard enforcement, or approval matching.

Execution uses the selected Architecture Control scope:

| Scope | Execution behavior |
|---|---|
| All datasets | Executes all active target datasets with dependency ordering |
| Schema | Executes selected schema roots with dependency ordering |
| TargetDataset | Executes the selected TargetDataset with dependency ordering |
| TargetDataset, target-only | Executes only the selected TargetDataset |

Dependency execution includes lineage inputs and runtime execution dependencies. When controlled reference completion is enabled through inferred members or Default Member Fallback, the referenced parent dataset is treated as an execution dependency so parent data and default members are available before the child load applies controlled reference handling.

Target-only execution is available only for TargetDataset scopes. It is intended for focused iteration when upstream data is already available. Target-only execution skips upstream execution dependencies, including controlled reference parent readiness dependencies.

Controlled execution produces an Architecture Execution Record.

When modeled rawcore references explicitly enable controlled member behavior, controlled load execution can also apply Controlled Reference Member handling. This remains part of the load runner path: Reference Integrity Review is diagnostic only, while default member creation, inferred member creation and Default Member Fallback happen during execution.

---

## 🔧 8. Artifact Storage for Shared Deployments

Architecture Control Plane artifacts are stored on the server-side filesystem.

`ELEVATA_ARCH_STATE_DIR`, `ELEVATA_ARCH_APPROVAL_DIR`, and `ELEVATA_ARCH_EXECUTION_DIR` configure base directories. Normal runtime paths are scoped by profile and target system so one metadata repository can safely serve multiple execution contexts.

Default artifact layout:

```text
.elevata/state/<profile>/<target-system>/architecture_state.json
.elevata/approvals/<profile>/<target-system>/
.elevata/approvals/<profile>/<target-system>/generation/
.elevata/executions/<profile>/<target-system>/
.elevata/executions/<profile>/<target-system>/run_plans/
```

The Run Plan directory may contain:

```text
<run-plan>.run_plan.json
<run-plan>.run_plan.planned_architecture_state.json
<run-plan>.run_plan.outcomes/
<run-plan>.run_plan.finalized.json
<run-plan>.run_plan.recovered.json
```

Exact names depend on the selected Run Plan path, but the planned-state, outcomes, finalization, and recovery artifacts are always derived deterministically from it.

For single-instance environments, the default `.elevata` paths provide a compact artifact layout inside the elevata runtime directory. For shared deployments, these base directories must point to persistent server-side storage that is available to every application instance and scheduler worker using the same metadata database.

Recommended deployment pattern:

```bash
ELEVATA_ARCH_STATE_DIR=/var/lib/elevata/state
ELEVATA_ARCH_APPROVAL_DIR=/var/lib/elevata/approvals
ELEVATA_ARCH_EXECUTION_DIR=/var/lib/elevata/executions
```

In containerized or multi-instance deployments, these paths are backed by a shared persistent volume. This ensures that Architecture State, Generation and Architecture Approval Artifacts, Review Status, Approval Checks, Execution Run Plans, Planned Architecture State snapshots, scheduler outcomes, finalization evidence, and Architecture Execution Records are resolved consistently across application and scheduler processes.

Architecture Approval files remain directly below the profile and target-system approval directory for compatibility. Generation Approval files use the dedicated `generation/` subdirectory and are named from the Target Generation Review fingerprint.

The metadata database stores metadata definitions. Architecture Control Plane artifacts are stored in the configured artifact directories.

Architecture Execution Records use a table-shaped JSON contract. This keeps the file-backed store compact while preserving a stable record structure for operational audit processing.

Architecture Execution Record history is resolved from the configured execution record directory. Retention cleanup removes stored execution record artifacts older than the selected retention window. The cleanup operates on audit artifacts only and does not alter metadata definitions, approval artifacts, Architecture State artifacts, load run logs or load run snapshots.

---

## 🔧 9. Architecture State Comparison (Architecture Promotion Report)

An Architecture Promotion Report compares two Architecture State artifacts.

It answers:

```text
What would change when this target architecture state is compared to that source state?
```

!!! important
    This report is a read-only Architecture State comparison. It does **not** deploy metadata between environments. Use [Environment Promotion](environment_promotion.md) for controlled DEV → TEST / PROD metadata deployment.

Example:

```bash
python manage.py elevata_promote \
  .artifacts/dev_architecture_state.json \
  .artifacts/prod_architecture_state.json \
  --source-label dev \
  --target-label prod
```

For schema-scoped comparison:

```bash
python manage.py elevata_promote \
  .artifacts/dev_architecture_state.json \
  .artifacts/prod_architecture_state.json \
  --schema rawcore
```

For dataset-scoped comparison:

```bash
python manage.py elevata_promote \
  .artifacts/dev_architecture_state.json \
  .artifacts/prod_architecture_state.json \
  --target-dataset rc_aw_customer
```

For dataset-scoped comparison with an explicit schema:

```bash
python manage.py elevata_promote \
  .artifacts/dev_architecture_state.json \
  .artifacts/prod_architecture_state.json \
  --target-dataset rc_aw_customer \
  --schema rawcore
```

Promotion reports use the same scope semantics as change reports. The embedded Architecture Change Report exposes the effective scope in JSON and text output.

---

## 🔧 10. CI Exit Policies

Architecture reports and promotion reports support explicit exit policies:

| Option | Behavior |
|---|---|
| `--fail-on-changes` | Fails when architecture changes are present |
| `--fail-on-blocked` | Fails when policy decisions block execution |
| `--fail-on-destructive` | Fails when destructive actions are present |

Example:

```bash
python manage.py elevata_plan --all \
  --format json \
  --fail-on-blocked
```

Example:

```bash
python manage.py elevata_promote \
  .artifacts/dev_architecture_state.json \
  .artifacts/prod_architecture_state.json \
  --format json \
  --fail-on-blocked \
  --fail-on-destructive
```

---

## 🔧 11. Execution Guardrails

The Architecture Control Plane separates architecture review, approval, execution control, and load-run enforcement.

Load execution remains protected by the load runner. `elevata_load` performs its own preflight checks before DDL or DML can be executed.

This preserves a strict separation:

| Command | Responsibility |
|---|---|
| `generate_targets --dry-run` | Render Target Generation Plans and optional Review artifacts |
| `elevata_generation_approve` | Create a Generation Approval for one exact Target Generation Plan |
| `generate_targets --plan-file` | Apply one exact plan with source, target and decision drift guards |
| `elevata_state` | Render architecture state |
| `elevata_plan` | Render architecture change report |
| `elevata_promote` | Compare architecture state artifacts |
| `elevata_approve` | Create architecture approval artifact |
| `elevata_approval_check` | Verify approval artifact against a change report |
| `elevata_run_plan` | Create an immutable scheduler-facing Execution Run Plan and Planned Architecture State |
| `elevata_load` | Execute loads with preflight and guard checks |
| `elevata_finalize_run_plan` | Validate scheduler outcomes and persist the planned applied state |

The Architecture Control UI invokes the same load runner through a constrained execution path. The UI does not expose arbitrary load runner flags. It exposes controlled scope selection, approval status, Execution Impact, execution preview, controlled reference readiness, target-only execution for TargetDataset scopes, captured output, and execution records.

Scheduler-managed execution adds a stricter immutable boundary. A dataset task must match the Run Plan runtime context and Planned Architecture State before it can execute. Clearing or retrying dataset tasks after changing metadata is not a supported rescue workflow; create a new Run Plan and start a new scheduler run.

---

## 🔧 12. Architecture Execution Record

An Architecture Execution Record describes one controlled Architecture Control execution.

It captures:

- execution identifier  
- reviewer or operator  
- timestamps and duration  
- execution status  
- Architecture Control scope  
- dependency mode  
- report fingerprint  
- approval identifier  
- preview fingerprint  
- command invocation metadata  
- output and error tails  
- output and error line counts  
- deterministic record fingerprint

Execution records answer:

```text
Who executed which approved architecture scope, with which dependency mode, and what happened?
```

The record is stored as deterministic JSON:

```text
<execution_id>.execution.json
```

Stored records can be listed in Architecture Control, filtered by scope, status, date range and dependency mode, opened as detail views, and downloaded as JSON audit artifacts. Retention cleanup removes older stored records from the execution record store.

Architecture Execution Records are audit artifacts. They complement load-run logs and snapshots:

| Artifact | Purpose |
|---|---|
| Load Run Log | Dataset- and attempt-level operational events |
| Load Run Snapshot | Batch-level execution state and outcomes |
| Architecture Execution Record | Architecture Control execution decision and result |

---

## 🔧 13. Deterministic Fingerprints

Target Generation Plan, Target Generation Review, Generation Approval Artifact, Architecture State, Architecture Change Report, Architecture State Comparison (`Architecture Promotion Report`), Architecture Approval Artifact, Execution Impact Plan, Execution Preview, Execution Run Plan, finalization evidence, and Architecture Execution Record expose deterministic fingerprints or bind directly to fingerprinted artifacts.

Fingerprints are derived from canonical JSON representations and allow CI, review processes, approval decisions, scheduler runs, finalization, Architecture State comparisons, and audit processes to reference exact architecture artifacts. Environment Promotion uses its own Release, Plan, Approval, Package and Record fingerprints.

---

## 🔧 14. Operational Smoke Checks
 
The following commands provide a compact validation set for architecture artifacts.
 
Render one schema-scoped Target Generation Plan and Review:

```bash
python manage.py generate_targets \
  --schema raw \
  --dry-run \
  --plan-output .artifacts/target_generation_plan_raw.json \
  --review-output .artifacts/target_generation_review_raw.json
```

Create and store a Generation Approval when the reviewed plan requires or warrants one:

```bash
python manage.py elevata_generation_approve \
  .artifacts/target_generation_plan_raw.json \
  --approved-by "Reviewer Name" \
  --note "Source-to-Target impact reviewed." \
  --store
```

Apply the exact reviewed plan:

```bash
python manage.py generate_targets \
  --plan-file .artifacts/target_generation_plan_raw.json \
  --require-generation-approval
```

Export the current architecture state:

```bash
python manage.py elevata_state --output .artifacts/current_architecture_state.json
```

Render a platform-wide report:

```bash
python manage.py elevata_plan --all \
  --format json \
  --output .artifacts/architecture_plan_all.json
```

Render a schema-scoped report:

```bash
python manage.py elevata_plan --all \
  --schema rawcore \
  --format json \
  --output .artifacts/architecture_plan_rawcore.json
```

Render a dataset-scoped report:

```bash
python manage.py elevata_plan rc_aw_customer \
  --format json \
  --output .artifacts/architecture_plan_rc_aw_customer.json
```

Compare two state artifacts:

```bash
python manage.py elevata_promote \
  .artifacts/current_architecture_state.json \
  .artifacts/current_architecture_state.json \
  --format json \
  --output .artifacts/architecture_promotion_self_check.json
```

Create and store an approval artifact:

```bash
python manage.py elevata_approve .artifacts/architecture_plan_rc_aw_customer.json \
  --approved-by "Reviewer Name" \
  --note "Reviewed for deployment." \
  --store
```

Verify the stored approval artifact:

```bash
python manage.py elevata_approval_check \
  .artifacts/architecture_plan_rc_aw_customer.json \
  .elevata/approvals/<profile>/<target-system>/<report_fingerprint>.approval.json
```

Validate no-change exit behavior against an explicit baseline:

```bash
python manage.py elevata_plan --all \
  --previous-state .artifacts/current_architecture_state.json \
  --format json \
  --fail-on-changes
```

Create an immutable full-scope scheduler Run Plan:

```bash
python manage.py elevata_run_plan \
  --all-datasets \
  --output .artifacts/full.run_plan.json
```

After every scheduler step has written a valid outcome, finalize the plan:

```bash
python manage.py elevata_finalize_run_plan \
  .artifacts/full.run_plan.json
```

---

© 2025-2026 elevata - Technical Documentation