# Two CDE Private Cloud prerequisites that block *every* job

Neither of these has anything to do with GPUs, RAPIDS or custom images. Both present as
authentication or configuration failures, and both cost real time to diagnose because the error
surfaces far from the cause. Check them before blaming a job.

## 1. The user needs a DE resource role (IAM), not a virtual-cluster ACL

**Symptom:** every `cde` command fails with exit 5 and `authorization policy denies`, even though
authentication clearly succeeded.

**Cause:** the CDP user holds no Data Engineering resource role. The `dex-authz` pod says so exactly:

```
kubectl logs -n <dex-base-ns> deploy/dex-authz | grep 'DE roles'
WARN db_manager.go:89 user: admin does not have any DE roles
```

**The trap:** the natural first guess is the virtual cluster's access-control list — `aclUsers` /
`--full-access-users`, which is what the CDE UI exposes. That is a *different, later* check.
`cdp de update-vc --full-access-users admin` does not fix this.

**Fix** — an IAM grant against the **environment** CRN:

```bash
cdp iam assign-user-resource-role \
  --user <userCrn> \
  --resource-role-crn crn:altus:iam:us-west-1:altus:resourceRole:DEAdmin \
  --resource-crn <environmentCrn>
```

Available DE roles: `DEAdmin`, `DEServiceAdmin`, `DEServiceUser`, `DEUser`, `DEVirtualClusterAdmin`,
`DEVirtualClusterUser`, `DEVirtualClusterViewer`. `DEAdmin` on the environment is the broadest and
covers future virtual clusters too.

To check what a user already holds, the subcommand is `list-user-assigned-resource-roles`. There is
no `list-resource-roles-assigned-to-user`.

## 2. The user must self-onboard a Kerberos keytab

**Symptom:** `cde spark submit` uploads the file, creates a job run, then fails in ~3 seconds:

```
job run N failed, keytab is not present for user <user>
```

No pod is ever created — this is a pre-flight check, so there is nothing in any executor log.

**Cause:** on CDE Private Cloud, user keytabs are **not** minted by the control plane. Verified on
CDE 1.26.0 / PVC CE 1.5.5:

- `cdp iam set-workload-password` succeeds and returns `{}` — and creates no keytab. The IAM service
  logs the call; nothing downstream reacts to it. `isPasswordSet: true` on the user is **not**
  evidence a keytab exists.
- The environment user-sync API is unsupported on this form factor: `getEnvironmentUserSyncState` and
  `lastSyncStatus` both return **501 NOT_IMPLEMENTED**.
- `thunderhead-kerberosmgmt-api` in the control plane only ever served *service* principals
  (e.g. `mlgov/<cluster>@REALM`). It is not in this path.

Instead, CDE runs its own service, `dex-base-keytab-management` (namespace `dex-base-<clusterId>`,
port 9195, backed by the `dex_keytabdb` schema). Its own swagger calls it
*"Keytab management APIs for **user self-onboarding**"*:

| Method | Path | Operation |
|---|---|---|
| `POST` | `/user-auth/api/v1/kerberos` | upload keytab **or** provide password |
| `GET` | `/user-auth/api/v1/kerberos` | keytab metadata (404 until onboarded) |
| `DELETE` | `/user-auth/api/v1/kerberos` | revoke |
| `GET` | `/user-auth/api/v1/admin/kerberos` | list all onboarded users (DEAdmin / DEServiceAdmin) |

`POST` takes `principal` (required, fully qualified with realm) plus **either** a `file` (keytab)
**or** a `password`, from which it generates the keytab itself.

**Fix:** in the CDE UI, the **Kerberos Authentication** panel (component
`dex-kerberos-authentication`, tab id `hadoop-authentication-tab`). Choose the *Password* option,
enter `<user>@<REALM>` and the user's password. To change the principal afterwards you must click
**Revoke Authentication** first — it does not overwrite in place.

Verify the credential independently first, so a rejected upload is unambiguous:

```bash
kinit <user>@<REALM>     # must return a TGT; check with klist
```

**Not available from the CLI.** CDE CLI 1.24.0 has no kerberos/keytab command (`airflow backup
credential job profile repository resource run session spark`), and the `cdp` CLI's sync commands are
501 here. UI or REST only.

## Why these two look like the same problem

Both fail while the CLI is demonstrably authenticating, and neither names the thing that is actually
missing. The distinguishing signal:

| Message | Layer | Fix |
|---|---|---|
| `authorization policy denies` | IAM resource role | `assign-user-resource-role` |
| `keytab is not present for user X` | CDE keytab self-onboarding | Kerberos Authentication panel |

Read `dex-authz` for the first and `dex-base-keytab-management` for the second; each names its own
cause in one line, which is much faster than reasoning from the CLI's output.
