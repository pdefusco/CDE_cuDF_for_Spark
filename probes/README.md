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

Two things that make the conclusion airtight, and are worth preserving if you modify the script:
pin all pods to the **same node** (`NODE=...`) so node-local registry config cannot differ between
them, and keep the CPU variants in the list.
