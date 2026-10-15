# CDE Private Cloud prerequisites that block *every* job

None of these has anything to do with GPUs, RAPIDS or custom images. All present as
authentication or configuration failures, and all cost real time to diagnose because the error
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

**If `cdp iam` answers `Access key not found`**, it is talking to the *public* CDP control plane, not
the Private Cloud one. This was seen on the 7.1.9 / CDE 1.27 cluster. Call the PVC control plane's
IAM API directly with `cdpcurl`, signed with the PVC access key, the same way the deployment
playbooks call it. The request bodies are camelCase:

```bash
cdpcurl -X POST -H 'content-type: application/json' \
  -d '{"user":"<userCrn>","resourceRoleCrn":"crn:altus:iam:us-west-1:altus:resourceRole:DEAdmin","resourceCrn":"<environmentCrn>"}' \
  https://console-cdp.apps.ecs.<gateway-ip>.nip.io/api/v1/iam/assignUserResourceRole
```

`/api/v1/iam/listUsers` on the same base URL returns the user CRN.

**The grant is not instant.** `dex-authz` caches roles (`EnableUmsRoleAuthz:true`, `TTLInterval:5m0s`
in its startup log). The same `authorization policy denies` keeps coming back for up to 5 minutes
after a correct grant. Wait the TTL out before concluding the grant was wrong.

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

**On CDE 1.27** (7.1.9 cluster) the upload is on the virtual cluster, not in the Management
Console. Go to Administration, open the VC's details page, then **Actions** → **Upload User
Keytab**. The keytab can be generated on the FreeIPA server. Two pitfalls:

- `kvno` cannot fetch a key version for a user principal; it fails with "valid for user2user only".
  Read the key version and salt with `kadmin.local` instead.
- FreeIPA uses the `:special` salt, so `ktutil` must be told to fetch it with `-f`. Without `-f`, it
  derives the default salt and writes a keytab that does not authenticate.

```bash
sudo kadmin.local -q "getprinc <user>"    # note the vno and enctypes (one addent per enctype)
ktutil
  addent -password -p <user>@<REALM> -k <vno> -e aes256-cts-hmac-sha1-96 -f
  addent -password -p <user>@<REALM> -k <vno> -e aes128-cts-hmac-sha1-96 -f
  wkt /tmp/<user>.keytab
  quit
kinit -kt /tmp/<user>.keytab <user>@<REALM> && klist    # prove it before uploading
```

Delete the keytab from the IPA host once it is uploaded. It is a password equivalent.

**Not available from the CLI.** CDE CLI 1.24.0 has no kerberos/keytab command (`airflow backup
credential job profile repository resource run session spark`), and the `cdp` CLI's sync commands are
501 here. UI or REST only.

## 3. Deployment-specific: `421 Misdirected Request` on the VC endpoint

This one is not CDE; it is the reverse proxy in front of it. It was seen on the 7.1.9 / CDE 1.27
cluster. Every `cde` call to `https://<vc>.cde-<id>.apps.ecs.<gateway-ip>.nip.io` returned **421**,
while the service-level host worked.

**Cause:** the deployment's Caddy sent the bare CDE service domain as the upstream TLS SNI on its
wildcard site, but forwarded the VC hostname as `Host`. ingress-nginx refuses any request whose SNI
and `Host` differ.

**Fix:** in the wildcard site's `reverse_proxy` block (deployment repo,
`playbooks/templates/cloudera_cde.caddyfile.j2`), make the SNI follow the request:

```
transport http {
  tls_insecure_skip_verify
  tls_server_name {http.request.host}
}
```

Then reload Caddy on the gateway.

## Why the first two look like the same problem

Both fail while the CLI is demonstrably authenticating, and neither names the thing that is actually
missing. The distinguishing signal:

| Message | Layer | Fix |
|---|---|---|
| `authorization policy denies` | IAM resource role | `assign-user-resource-role` |
| `keytab is not present for user X` | CDE keytab self-onboarding | Kerberos Authentication panel (1.26) / VC **Actions** → Upload User Keytab (1.27) |
| HTTP `421 Misdirected Request` | reverse proxy SNI | `tls_server_name {http.request.host}` |

Read `dex-authz` for the first and `dex-base-keytab-management` for the second; each names its own
cause in one line, which is much faster than reasoning from the CLI's output.
