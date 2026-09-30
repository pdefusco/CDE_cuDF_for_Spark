# Image probe

`probe_images.sh` answers one question: **do the CDE GPU Spark runtime images exist in the registry
this cluster can reach?** It launches one throwaway pod per image in the VC namespace, reusing that
namespace's own `imagePullSecrets`, so kubelet authenticates exactly the way a real CDE job would.

The **CPU images are the control.** They must pull for the result to mean anything — if they fail too,
the problem is credentials or networking, not image availability, and the GPU result tells you
nothing.

Reading the outcome:

| result | meaning |
|---|---|
| `NOT FOUND` (`manifest unknown`) | The registry resolved the reference and reported the tag **absent**. The image is genuinely not published where this cluster looks. |
| `UNAUTHORIZED` (`denied`, `forbidden`) | Credentials or entitlement, **not** a missing tag. Different problem — fix auth and re-probe. |
| `Succeeded` / `Running` | Pulled fine. |

Re-run this after any registry, entitlement, or CDE version change — it is the cheapest way to tell
whether the blocker in the top-level README still stands.

Three things make the conclusion airtight, and are worth preserving if you modify the script:

1. **Get the registry prefix from the cluster, not from memory.** The real path includes a
   `cloudera/dex/` segment — `container.repository.cloudera.com/cdp-private/cloudera/dex/`. The first
   probe run omitted it, which is why the top-level README now treats the result as unconfirmed: a
   wrong prefix returns `NotFound` for every image and says nothing about GPU availability. Check with
   `kubectl get cm <vcId>-api-cm -n <vcId> -o jsonpath='{.data.dex\.yaml}' | grep -E 'image:|Image:'`,
   or just run `scripts/verify_gpu_vc.sh`, whose check 4 prints them.
2. **Pin all pods to the same node** (`NODE=...`) so node-local registry config cannot differ between
   them.
3. **Keep the CPU variants in the list.** They are the control, and they must pull from the *same
   prefix* as the GPU ones for the comparison to hold.
