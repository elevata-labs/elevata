# ⚙️ Architecture Control Plane

The Architecture Control Plane provides deterministic review, comparison, and promotion workflows  
for metadata-defined architecture.

It turns architecture state into explicit artifacts:

- Architecture State  
- Architecture Change Report  
- Architecture Promotion Report  
- Architecture Approval Artifact  
- deterministic report fingerprints

These artifacts make structural architecture changes reviewable, policy-aware,
approvable, verifiable, visible in the UI, and suitable for CI pipelines.

---

## 🔧 1. Purpose

elevata treats architecture as executable metadata.

The Architecture Control Plane defines the review and comparison layer around
that metadata:

```text
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
Architecture Approval Artifact
```

This makes schema evolution intent explicit before load execution applies any
DDL or DML.

---

## 🔧 2. Architecture State

Architecture State is a deterministic snapshot of the metadata-defined platform
architecture.

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

The load runner builds the current Architecture State during execution planning  
and uses it for architecture diffing, MigrationPlan derivation, and guard checks.

The persisted runtime baseline represents the applied architecture state. It is  
written after successful load execution. Dry-run persistence is controlled via:

```bash 
ELEVATA_PERSIST_ARCH_STATE_ON_DRY_RUN=false 
```

---

## 🔧 3. Architecture Change Report

An Architecture Change Report describes the difference between a baseline state  
and the metadata-defined architecture state.

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

For scoped reports, the report payload contains only changes, migration actions,  
policy decisions and summary counts that belong to the selected scope.  
The report fingerprint therefore represents the selected architecture scope.

When a target dataset name is unique, `--schema` can be omitted.  
Use `--schema` when the same dataset name exists in multiple schemas or when  
CI scripts should declare the intended schema explicitly.

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

## 🔧 4. Architecture Approval Artifact

An Architecture Approval Artifact records a review decision for one exact
Architecture Change Report fingerprint.

It answers:

```text
Has this exact architecture change report been reviewed and approved?
```

Approval artifacts are deterministic JSON artifacts. They bind a review decision to the report fingerprint,  
report scope, report state, summary counts, and policy status of the approved Architecture Change Report.

An approval does not override policy decisions. If a report contains blocking policy decisions,  
execution remains blocked by the load runner and materialization policy.

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
  .elevata/approvals/<report_fingerprint>.approval.json
```

The approval check fails when:

- the approval artifact fingerprint does not match its payload  
- the approval identifier does not match the artifact fingerprint  
- the approval references another Architecture Change Report  
- the review decision is not `approved`

---

## 🔧 5. Architecture Review Status UI

The Architecture Review Status UI shows the review state for a selected
TargetDataset architecture scope.

It displays:

- review status  
- report fingerprint  
- approval identifier and artifact fingerprint  
- reviewer and decision timestamp  
- policy status  
- architecture scope  
- change summary  
- state fingerprints

Review states include approved, pending review, approval drift, blocked by policy, no architecture changes,  
and invalid approval artifact.

---

## 🔧 6. Architecture Promotion Report

An Architecture Promotion Report compares two Architecture State artifacts.

It answers:

```text
What would change when this target architecture state is compared to that source state?
```

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

Promotion reports use the same scope semantics as change reports.
The embedded Architecture Change Report exposes the effective scope in JSON and text output.

---

## 🔧 7. CI Exit Policies

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

## 🔧 8. Execution Guardrails

The Architecture Control Plane is read-only.

Load execution remains protected by the load runner. `elevata_load` performs its own preflight checks  
before DDL or DML can be executed.

This preserves a strict separation:

| Command | Responsibility |
|---|---|
| `elevata_state` | Render architecture state |
| `elevata_plan` | Render architecture change report |
| `elevata_promote` | Compare architecture state artifacts |
| `elevata_approve` | Create architecture approval artifact |
| `elevata_approval_check` | Verify approval artifact against a change report |
| `elevata_load` | Execute loads with preflight and guard checks |

---

## 🔧 9. Deterministic Fingerprints

Architecture State, Architecture Change Report, Architecture Promotion Report, and Architecture Approval Artifact  
each expose deterministic fingerprints.

Fingerprints are derived from canonical JSON representations and allow CI, review processes,  
approval decisions, and promotion workflows to reference exact architecture artifacts.

---

## 🔧 10. Operational Smoke Checks

The following commands provide a compact validation set for architecture artifacts.

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
  .elevata/approvals/<report_fingerprint>.approval.json
```

Validate no-change exit behavior against an explicit baseline:

```bash
python manage.py elevata_plan --all \
  --previous-state .artifacts/current_architecture_state.json \
  --format json \
  --fail-on-changes
```

---

© 2025-2026 elevata Labs — Internal Technical Documentation