# ⚙️ Environment Promotion

Environment Promotion moves **portable, immutable metadata definitions** from an authoring environment such as DEV into separately controlled metadata environments such as TEST and PROD.

It extends elevata's deterministic control model across environments without giving the authoring runtime direct access to target metadata databases.

Environment Promotion is intentionally separate from Architecture Control:

- **Environment Promotion** deploys metadata between environments.  
- **Architecture Control** reviews and executes the resulting physical architecture inside each environment.

---

## 🔧 1. Promotion Flow at a Glance

The operator workflow is:

```text
Release
  ↓
Review
  ↓
Approve
  ↓
Deploy
  ↓
Audit
```

| Step | Purpose | Target metadata mutation? |
|---|---|---:|
| **Release** | Capture the current authoring metadata as an immutable Architecture Release | No |
| **Review** | Compare the release with the current TEST / PROD metadata state | No |
| **Approve** | Approve one exact deterministic Promotion Plan and bind it into an immutable Deployment Package | No |
| **Deploy** | Re-check live target drift, confirm the exact package ID, and apply through the target runner | **Yes** |
| **Audit** | Read immutable convergence evidence from the target runner | No |

Target metadata is mutated only during **Deploy**, after exact approval and a live drift check.

Environment Promotion deploys metadata only. Physical architecture changes remain a separate Architecture Control workflow.

---

## 🔧 2. Portable Metadata Foundation

Environment Promotion depends on metadata identity that remains stable across independently stored metadata databases.

### 🧩 2.1 Stable Transport Identity

Portable metadata uses logical model identity rather than local database identity.

Core identity rules include:

- `TargetSchema.short_name` is the stable schema identity  
- `System.short_name` is a stable modeled identifier  
- `QueryNode.logical_key` provides stable unique identity for query metadata   
- `SourceDatasetGroup` identity is derived from portable modeled attributes  
- active SourceDataset increment-policy uniqueness is environment-aware  
- local database PKs never participate in functional transport identity  
- reference-derived FK identity is derived from portable dataset/reference identity

Environment-specific physical naming, secrets, runtime paths and connection details remain outside the portable identity contract.

### 🧩 2.2 Portable Generated Lineage

System-managed TargetDatasets use deterministic logical lineage that is independent of local source/schema IDs.

Generated lineage uses the form:

```text
generated:<schema-short-name>:<sha256>
```

Reference-derived FK lineage is likewise built from portable dataset/reference identity rather than local database PKs.

This makes independently stored metadata environments comparable without requiring identical local row identifiers.

### 🧩 2.3 Portable Runtime Hash Binding

Generated SK/FK expressions persist the symbolic runtime binding:

```text
{runtime:pepper}
```

The symbolic expression is portable. The concrete pepper remains environment-local and is resolved through `get_runtime_pepper()` only when the generated DSL is parsed for runtime SQL rendering.

Portable generated SK/FK expressions must contain the symbolic runtime binding rather than an embedded concrete pepper. The same contract applies to active and inactive generated metadata so later reactivation preserves the intended hash semantics.

---

## 🔧 3. Runtime Topology

A real deployment uses separate authoritative metadata databases and separate runtime responsibilities.

```text
DEV Authoring Runtime
+--------------------------------+
| DEV metadata database          |
| Environment Promotion UI       |
| Architecture Release Store     |
| Approval / Package Stores      |
+----------------+---------------+
                 |
                 | authenticated Promotion Target Runner API
                 v
TEST / PROD Promotion Target Runtime
+--------------------------------+
| target metadata database       |
| headless Promotion Runner      |
| target Promotion History       |
+----------------+---------------+
                 |
                 v
Architecture Control
for physical target changes
```

Core rules:

- DEV, TEST and PROD each own an authoritative metadata database.  
- One running elevata process binds to exactly one metadata database.  
- The authoring process never switches Django's local DB binding to operate on TEST or PROD.  
- Target metadata database credentials remain local to the target runtime.  
- The authoring process reaches targets only through the authenticated Promotion Target Runner API.  
- A target runtime is headless for promotion operations; it does not expose the normal modeling UI.

---

## 🔧 4. Runtime Modes and Environment Identity

### 🧩 4.1 Authoring Runtime

```env
ELEVATA_RUNTIME_MODE=authoring
ELEVATA_ENVIRONMENT=dev
```

The authoring runtime hosts normal metadata modeling and the Environment Promotion workspace.

### 🧩 4.2 Promotion Target Runtime

```env
ELEVATA_RUNTIME_MODE=promotion_target
ELEVATA_ENVIRONMENT=test
ELEVATA_PROMOTION_RUNNER_TOKEN=<target-local secret with at least 32 characters>
```

Optional package-size limit:

```env
ELEVATA_PROMOTION_RUNNER_MAX_PACKAGE_BYTES=67108864
```

`ELEVATA_ENVIRONMENT` identifies the metadata environment owned by the running process.

It is distinct from `ELEVATA_PROFILE`. A profile controls runtime connection/target execution context; the environment label identifies the metadata deployment boundary. They may use the same text such as `dev`, but they are not the same contract.

---

## 🔧 5. Authoring-Side Promotion Target Registry

Configured targets are declared on the authoring runtime.

Example:

```env
ELEVATA_PROMOTION_TARGETS=test,prod

ELEVATA_PROMOTION_TARGET_TEST_URL=https://elevata-test.example.com/metadata
ELEVATA_PROMOTION_TARGET_TEST_TOKEN=<TEST runner token>

ELEVATA_PROMOTION_TARGET_PROD_URL=https://elevata-prod.example.com/metadata
ELEVATA_PROMOTION_TARGET_PROD_TOKEN=<PROD runner token>

ELEVATA_PROMOTION_TARGET_TIMEOUT_SECONDS=30
```

Use a different bearer secret for each target environment.

Tokens stay server-side. They are not rendered into browser-side HTML or JavaScript.

The base URL points to the Django mount under which the Promotion Runner endpoints are available. With the default elevata URL layout, runner endpoints are below:

```text
/metadata/promotion-runner/
```

---

## 🔧 6. What Is Portable?

Architecture Releases contain the metadata definition required to reconstruct the approved target metadata state.

Portable artifacts may contain:

- stable logical model identities  
- TargetSchemas, TargetDatasets, TargetColumns and their portable configuration  
- modeled lineage and query contracts  
- ownership and governance metadata included by the transport contract  
- lifecycle state defined by the portable contract  
- symbolic runtime references such as environment-variable placeholders  
- symbolic generated hash binding `{runtime:pepper}`

Portable artifacts do **not** contain:

- local database PKs as functional identity  
- `.env` contents  
- passwords or bearer tokens  
- source/target database credentials  
- concrete runtime connection strings  
- concrete pepper values  
- local absolute paths  
- Architecture Approval artifacts from another environment  
- Generation Approval artifacts from another environment  
- Execution Run Plans or Architecture Execution Records

The target environment supplies its own runtime profile, providers, credentials, target-system bindings, secrets and concrete runtime paths.

---

## 🔧 7. Immutable Artifact Chain

Environment Promotion is expressed through an explicit artifact chain:

```text
EnvironmentMetadataSnapshot
        ↓
ArchitectureReleaseBundle
        ↓
EnvironmentPromotionPlan
        ↓
EnvironmentPromotionApprovalArtifact
        ↓
EnvironmentPromotionDeploymentPackage
        ↓
EnvironmentPromotionRecord
```

### 🧩 Environment Metadata Snapshot

A read-only canonical snapshot of one environment's portable metadata state.

It contains deterministic object/relationship identity and a metadata fingerprint plus snapshot fingerprint.

### 🧩 Architecture Release Bundle

An immutable release captures the complete portable authoring metadata definition.

A release is identified by a release coordinate (`name + version`) and a deterministic release ID / bundle fingerprint. Existing immutable coordinates cannot be overwritten with different content.

### 🧩 Environment Promotion Plan

A plan compares:

```text
incoming Architecture Release
vs.
current target Environment Metadata Snapshot
```

It contains deterministic actions, before/after state, change classifications, readiness, blocker/destructive/lifecycle summaries, source/target fingerprints and a plan fingerprint.

The same release and the same target state produce the same deterministic plan.

### 🧩 Environment Promotion Approval

Environment Promotion Approval authorizes one exact Promotion Plan.

It cannot be reused for another plan and does not authorize warehouse DDL/DML or Architecture Control execution.

### 🧩 Deployment Package

The immutable Deployment Package binds the exact:

- Architecture Release  
- Promotion Plan  
- Environment Promotion Approval  
- target environment

The package is the only artifact accepted by guarded promotion apply.

### 🧩 Environment Promotion Record

A successful target apply produces an immutable Promotion Record containing:

- release, plan and approval bindings  
- source and target environment labels  
- pre-apply target fingerprints  
- post-apply target fingerprints  
- exact action results  
- post-validation plan identity/fingerprint  
- actor and timestamp  
- deterministic record fingerprint

The post-apply metadata fingerprint must equal the approved release metadata fingerprint.

---

## 🔧 8. UI Workflow

The **Environment Promotion** workspace is organized around the same five-step flow shown at the top of the page.

### 🧩 8.1 Release

In **Architecture Releases**:

1. Enter a release name and version.  
2. Optionally add a release note.  
3. Select **Create immutable release**.  
4. Review the stored release ID, object count and fingerprints.

Creating a release contacts no Promotion Target and mutates no target metadata.

### 🧩 8.2 Review

In **Promotion Review**:

1. Select TEST or PROD.  
2. Optionally check target connectivity/current state.  
3. Select an Architecture Release.  
4. Select **Build read-only plan**.  
5. Review action counts, mutating changes, blockers and lifecycle/destructive classifications.

Planning fetches the target snapshot through the runner but does not modify the target.

### 🧩 8.3 Approve

For a `ready` plan with mutating actions:

1. Review grouped plan details.  
2. Add an optional approval note.  
3. Create the exact Environment Promotion Approval and Deployment Package.

Before approval is created, elevata re-fetches the target and rebuilds the reviewed plan. The rebuilt plan must remain identical.

No approval/package is required for a `no_changes` plan.

### 🧩 8.4 Deploy

Before apply:

1. Select **Check live target drift**.  
2. Require `Target unchanged`.  
3. Enter the exact Deployment Package ID in the confirmation field.  
4. Select **Apply approved package**.

The target runner repeats its authoritative target/package/drift guards during apply. The earlier UI drift check is not the final safety boundary.

### 🧩 8.5 Audit

After a successful apply:

- the returned Promotion Record is validated against the approved package  
- target history is re-fetched through the runner  
- the exact target-side record is confirmed  
- the UI exposes convergence evidence

The main Promotion History overview is remote-authoritative. No target is contacted merely by opening the page; the operator selects a target and explicitly refreshes its history.

---

## 🔧 9. Promotion Target Runner

The Promotion Target Runner is an authenticated headless API surface.

Current endpoints:

```text
GET  /metadata/promotion-runner/health/
GET  /metadata/promotion-runner/snapshot/
POST /metadata/promotion-runner/check/
POST /metadata/promotion-runner/apply/
GET  /metadata/promotion-runner/history/
GET  /metadata/promotion-runner/history/<record_id>/
```

All runner operations require the configured bearer token.

In `promotion_target` mode the normal modeling UI is not exposed. The runtime is intentionally limited to promotion-target responsibilities.

---

## 🔧 10. Drift, Convergence and Fail-Closed Behavior

Environment Promotion never assumes that a previously reviewed target state is still current.

Important guards:

- Approval creation re-fetches the target and requires the reviewed plan to remain identical.  
- A live package check compares expected and current target metadata/snapshot fingerprints.  
- Apply re-checks the target again inside the target runtime.  
- Apply validates the target environment against the immutable package target.  
- Apply requires explicit exact package-ID confirmation.  
- Apply runs transactionally against the target metadata database.  
- Post-apply planning must converge to a plan with no mutating actions.  
- Post-apply metadata fingerprint must equal the Architecture Release metadata fingerprint.  
- Target Promotion History stores immutable convergence evidence.

Drift, invalid packages, blocked plans or post-apply non-convergence fail closed.

---

## 🔧 11. Artifact Reconstruction Boundary

Promotion reconstructs an **already approved complete metadata artifact**. It must not reinterpret that artifact through ordinary modeling/generation side effects.

During promotion reconstruction elevata suppresses normal derivation callbacks that would otherwise create or modify metadata after a transported row is saved, including relevant Query Contract / TargetColumn and generated history derivations.

This suppression is local to Environment Promotion reconstruction. Normal UI modeling and Controlled Target Generation retain their ordinary derivation behavior.

---

## 🔧 12. Approval Boundaries

Three approval types protect different contracts:

| Approval | Authorizes | Bound to |
|---|---|---|
| **Generation Approval** | one exact Source-to-Target metadata mutation inside an environment | Target Generation Review + Plan |
| **Environment Promotion Approval** | one exact release-to-target metadata deployment | Environment Promotion Plan |
| **Architecture Approval** | one exact resulting physical architecture change | Architecture Change Report |

An approval from one boundary cannot authorize another boundary.

After metadata has been promoted, the target environment independently evaluates its own Architecture State and physical changes. A DEV Architecture Approval is never a PROD Architecture Approval.

---

## 🔧 13. Relationship to Architecture Control

Environment Promotion is a cross-environment metadata deployment boundary:

```text
DEV Metadata
  ↓
Architecture Release
  ↓
Environment Promotion Plan
  ↓
Environment Promotion Approval
  ↓
Deployment Package
  ↓
Guarded Metadata Deploy to TEST / PROD
```

Architecture Control then operates inside the target environment:

```text
Promoted Target Metadata
  ↓
Architecture State
  ↓
Architecture Change Report
  ↓
Architecture Approval
  ↓
Execution Impact / Run Plan
  ↓
Physical Execution
  ↓
Applied Architecture State
```

The same metadata definition may produce different physical Architecture Change Reports in DEV, TEST and PROD because the environments can have different physical starting states.

### 🧩 Architecture Promotion Report is not Environment Promotion

The legacy command `elevata_promote` produces an **Architecture Promotion Report**, which compares two Architecture State artifacts.

It is a read-only **Architecture State Comparison**. It does not deploy metadata between environments.

Use Environment Promotion for metadata deployment.

---

## 🔧 14. Artifact Storage

Authoring-side immutable promotion artifacts use:

```env
ELEVATA_PROMOTION_RELEASE_DIR=.elevata/promotion/releases
ELEVATA_PROMOTION_APPROVAL_DIR=.elevata/promotion/approvals
ELEVATA_PROMOTION_PACKAGE_DIR=.elevata/promotion/packages
```

Target-side Promotion Records use:

```env
ELEVATA_PROMOTION_HISTORY_DIR=.elevata/promotion/history
```

In shared/multi-instance deployments these directories should use persistent storage appropriate to the runtime role.

Do not depend on a shared filesystem between the authoring runtime and target runtimes. Promotion History is authoritative on the target runner and is fetched remotely by the authoring UI.

---

## 🔧 15. CLI Adapters

The UI is the normal operator path. CLI commands remain useful for CI, diagnostics and explicit automation.

### 🧩 Architecture Releases

```bash
python manage.py elevata_release create \
  --release-name customer-platform \
  --release-version 1.0.0 \
  --environment-label dev \
  --created-by operator@example.com
```

Other `elevata_release` actions support listing, downloading and validating stored release bundles.

### 🧩 Promotion artifacts

`elevata_promotion` exposes these actions:

```text
snapshot
plan
approve
package
check
apply
```

Representative local/CI sequence:

```bash
python manage.py elevata_promotion snapshot \
  --target-environment test \
  --created-by operator@example.com \
  --output .artifacts/test.snapshot.json

python manage.py elevata_promotion plan \
  --release-id <release-id> \
  --target-snapshot .artifacts/test.snapshot.json \
  --output .artifacts/test.plan.json

python manage.py elevata_promotion approve \
  --plan .artifacts/test.plan.json \
  --decided-by reviewer@example.com \
  --store \
  --output .artifacts/test.approval.json

python manage.py elevata_promotion package \
  --release-id <release-id> \
  --plan .artifacts/test.plan.json \
  --approval .artifacts/test.approval.json \
  --created-by reviewer@example.com \
  --output .artifacts/test.deployment.json

python manage.py elevata_promotion check \
  --deployment-package .artifacts/test.deployment.json \
  --target-environment test \
  --live-target

python manage.py elevata_promotion apply \
  --deployment-package .artifacts/test.deployment.json \
  --target-environment test \
  --confirm-package-id <deployment-package-id> \
  --applied-by operator@example.com
```

The CLI `--live-target` / `apply` path evaluates the metadata database bound to that running command. For remote TEST / PROD operation from the authoring UI, use the Promotion Target Runner architecture instead of giving DEV direct target DB credentials.

---

## 🔧 16. TEST / PROD Operating Model

Recommended production shape:

- Authoring/DEV exposes the full UI and creates immutable releases.  
- TEST and PROD run their own authoritative metadata databases.  
- TEST and PROD expose only the authenticated Promotion Target Runner for promotion operations.  
- Target DB credentials and target-local secrets stay on the target host/runtime.  
- Use separate bearer tokens per target.  
- Environment Promotion deploys metadata only.  
- After deployment, run target-local Architecture Control to review physical architecture consequences.  
- Keep Promotion History on persistent target storage and read it through the runner.

---

## 🔧 17. Troubleshooting

| Symptom | Meaning / action |
|---|---|
| `401 Unauthorized` from runner | Bearer token is missing or does not match the target-local runner token |
| `404 ... exposes runner endpoints only` | Expected behavior when attempting to open normal UI routes on a `promotion_target` runtime |
| Promotion Plan is `no_changes` | Target metadata already matches the selected Architecture Release; no approval/package is required |
| Live target drift check reports drift | Stop. Rebuild/review the plan against the new target state; do not apply the stale package |
| Apply confirmation rejected | Enter the exact Deployment Package ID shown in the approved package section |
| Generated hash rendering cannot resolve the runtime pepper | Configure the environment-local runtime pepper; portable generated expressions must retain `{runtime:pepper}` |
| Main authoring History is empty on page load | Expected; select a Promotion Target and explicitly refresh remote authoritative history |

---

## 🔧 18. Related Documents

- [Controlled Target Generation](controlled_target_generation.md)  
- [Architecture Control Plane](architecture_control_plane.md)  
- [Architecture Overview](architecture_overview.md)  
- [Determinism & Execution Semantics](determinism_and_execution_semantics.md)  
- [Secure Metadata Connectivity](secure_metadata_connectivity.md)  
- [Hashing Architecture](hashing_architecture.md)

---

© 2025-2026 elevata - Technical Documentation
