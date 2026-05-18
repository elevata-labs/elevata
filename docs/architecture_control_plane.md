# ⚙️ Architecture Control Plane

The Architecture Control Plane provides deterministic review, comparison, and promotion workflows  
for metadata-defined architecture.

It turns architecture state into explicit artifacts:

- Architecture State  
- Architecture Change Report  
- Architecture Promotion Report  
- deterministic report fingerprints

These artifacts make structural architecture changes reviewable, policy-aware,
and suitable for CI pipelines.

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

## 🔧 4. Architecture Promotion Report

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

## 🔧 5. CI Exit Policies

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

## 🔧 6. Execution Guardrails

The Architecture Control Plane is read-only.

Load execution remains protected by the load runner. `elevata_load` performs its own preflight checks  
before DDL or DML can be executed.

This preserves a strict separation:

| Command | Responsibility |
|---|---|
| `elevata_state` | Render architecture state |
| `elevata_plan` | Render architecture change report |
| `elevata_promote` | Compare architecture state artifacts |
| `elevata_load` | Execute loads with preflight and guard checks |

---

## 🔧 7. Deterministic Fingerprints

Architecture State, Architecture Change Report, and Architecture Promotion Report each expose  
deterministic fingerprints.

Fingerprints are derived from canonical JSON representations and allow CI, review processes,  
and promotion workflows to reference exact architecture artifacts.

---

## 🔧 8. Operational Smoke Checks

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

Validate no-change exit behavior against an explicit baseline:

```bash
python manage.py elevata_plan --all \
  --previous-state .artifacts/current_architecture_state.json \
  --format json \
  --fail-on-changes
```

---

© 2025-2026 elevata Labs — Internal Technical Documentation