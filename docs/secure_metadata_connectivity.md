# ⚙️ Secure Metadata Connectivity

> How elevata securely manages credentials, runtime secrets, and dynamic profiles - without storing sensitive data in plain text.

---

## 🔧 1. Overview

elevata separates **connectivity profiles** (technical connection info) from **runtime secrets** (passwords, tokens, peppers) for both security and portability.

You define lightweight YAML profiles that describe *where* to connect, and `.env` variables that define *how* to authenticate.

This pattern allows:

- Secure credentials in environment variables  
- Reusable, shareable YAML configurations (without secrets)  
- Consistent access from both CLI and Django runtime  

All metadata transport is strictly read-only for external clients

---

## 🔧 2. The Profile Architecture

Profiles are located in `config/` and follow a clear naming pattern:

```bash
elevata_profiles.yaml
elevata_profiles.example.yaml
```
Each profile defines one or more environments (e.g. `dev`, `test`, `prod`):

```yaml
# config/elevata_profiles.yaml
profiles:
  prod:
    secret_ref_template: "kv://sec/{profile}/conn/{type}/{short_name}"
    overrides:
      erp: "sec/prod/conn/mssql/erp-core-01" # maps short_name 'erp' to different secret id
    providers:
      - type: azure_key_vault
        vault_url: https://kv-elevata.vault.azure.net/
    security:
      pepper_ref: "sec/{profile}/pepper"
```
---

## 🔧 3. Runtime Secret Resolution

Secrets like passwords or peppers are never stored in the YAML file. They are dynamically resolved by `profiles.py`, which merges data from:

1. The active profile file (`elevata_profiles.yaml`)
2. The system environment (`os.environ`)
3. The `.env` file (loaded automatically via `dotenv`)

Example `.env`:

```bash
# Database credentials
PG_DEV_PASSWORD=mysecretpassword

# Pepper for deterministic surrogate key hashing
SEC_DEV_PEPPER=supersecretpeppervalue
```
In code, this is handled via:
```python
from metadata.generation.security import get_runtime_pepper

pepper = get_runtime_pepper()
```
This ensures that even if `.env` or OS variables change, the runtime always pulls the correct, environment-scoped pepper.

---

## 🔧 4. Pepper and Surrogate Keys

The pepper is a random secret string used to salt deterministic hash keys. It makes surrogate key generation both non-reversible and dataset-consistent. The pepper must remain stable within an environment.

Each environment should have its own distinct pepper value.

Example:
```bash
# .env (development)
SEC_DEV_PEPPER=devpepper_ABC123

# .env (production)
SEC_PROD_PEPPER=prodpepper_XYZ789
```

Generated SK/FK metadata stores the symbolic token `{runtime:pepper}`. The concrete pepper is resolved through `get_runtime_pepper()` when the generated DSL is parsed for runtime SQL rendering. This keeps the portable metadata contract identical across environments while each runtime retains its own secret pepper value.

--- 

## 🔧 5. Using the Profile Resolver

At runtime, elevata uses the resolver in `core/metadata/config/profiles.py` to dynamically select the right connection and inject secrets:

```python
from metadata.config.profiles import load_profile
profile = load_profile(profiles_path)

print(profile["database"])
```

The function will:

1. Load the YAML profile  
2. Merge any environment overrides (e.g. passwords, peppers)  
3. Return a ready-to-use dictionary  

---

## 🔧 6. Debugging Tips

| Symptom                            | Likely Cause                      | Fix                                            |
|------------------------------------|------------------------------------|------------------------------------------------|
| `KeyError: 'SEC_DEV_PEPPER'`       | Pepper variable missing in `.env` | Add the line and restart                      |
| `Invalid profile: NoneType`        | YAML file not found               | Verify `elevata_profiles.yaml` exists in `config/` |

---

## 🔧 7. Security Best Practices

✅ Never commit `.env` files to Git  
✅ Use `.env.example` with dummy values for reference  
✅ Keep each environment’s pepper unique and private  
✅ Avoid storing DB passwords directly in YAML  
✅ Rotate secrets regularly if shared among teams

---

## 🔧 8. Environment Promotion Connectivity

Environment Promotion deliberately avoids direct authoring-to-target metadata DB connectivity.

Authoring runtime:

```env
ELEVATA_RUNTIME_MODE=authoring
ELEVATA_ENVIRONMENT=dev
ELEVATA_PROMOTION_TARGETS=test
ELEVATA_PROMOTION_TARGET_TEST_URL=https://elevata-test.example.com/metadata
ELEVATA_PROMOTION_TARGET_TEST_TOKEN=<target runner token>
```

Target runtime:

```env
ELEVATA_RUNTIME_MODE=promotion_target
ELEVATA_ENVIRONMENT=test
ELEVATA_PROMOTION_RUNNER_TOKEN=<same target-local runner token>
```

The authoring-side target token is used only by the server-side runner client. It is not exposed to the browser.

Target metadata DB credentials, runtime profiles, providers, concrete connection strings and peppers stay on the target runtime and are never written into Architecture Releases or Deployment Packages.

Use a different runner token for every target environment.

See [Environment Promotion](environment_promotion.md) for the full topology and runtime contract.

---

## 🔧 9. Related Docs

- [Getting Started Guide](getting_started.md)  
- [Automatic Target Generation Logic](generation_logic.md)

---

© 2025-2026 elevata - Technical Documentation