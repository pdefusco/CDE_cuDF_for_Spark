#!/usr/bin/env bash
# YARN GPU scheduling on a Data Hub, through the CM API behind Knox (cdp-proxy-api, PAM:
# workload user + workload password). The same Cloudera Manager settings as on Base
# (README-719-base.md §2). Only show, find, raw and revert have been run; cgroups was run and was
# refused by CM (see below); gpu and nodes are UNTESTED.
#   DH_GATEWAY=https://<cluster>-master0.<env-domain>/<cluster>/cdp-proxy-api \
#   CDP_WORKLOAD_USER=<workload user> datahub/cm-yarn-gpu.sh <mode>
#   show    read-only: YARN cgroup/LCE/GPU config, NodeManager role config groups per host
#            template, hosts with their cgroup-related host config
#   cgroups  YARN service: yarn_service_cgroups + yarn_service_lce_always, rm_enabled on every
#            NodeManager host (CM requires it), then restart YARN.
#            Run before any p4d exists: it proves the r5d workers' NMs still start on cgroup v2.
#            Needs CM Full Administrator: the DataHub's _c_cm_admins_* group maps to Limited Cluster
#            Administrator, which gets 403 on host config, and the rm_enabled check has no suppression.
#   revert   undo the service half of a `cgroups` that never got past validation (no restart)
#   cgroups-off  undo `cgroups` (then restart YARN)
#   gpu      after compute is scaled up AND rebooted into cgroup v1: GPU settings on the compute
#            host template's NODEMANAGER role config group only, rm_enabled on the compute
#            host(s), then restart YARN
#   nodes    read-only: yarn.io/gpu per NodeManager, from the RM REST API
#   find RX  read-only: YARN service/NM/RM settings (incl. suppressions) matching regex RX
#   raw P    read-only: print GET <CM API path P>, e.g. raw /clusters
set -euo pipefail
MODE=${1:-show}
GW=${DH_GATEWAY:?set DH_GATEWAY to the Data Hub cdp-proxy-api URL (Data Hub > Endpoints)}
WU=${CDP_WORKLOAD_USER:?set CDP_WORKLOAD_USER to your workload user name}
# With no terminal, read the password from a private file you write yourself:
# the password never appears in argv, the environment or shell history.
PWF=${CDP_WORKLOAD_PASSWORD_FILE:-$HOME/.dh-workload-pw}
if [ -t 0 ]; then read -rsp "Workload password for $WU: " WP; echo
elif [ -r "$PWF" ]; then WP=$(cat "$PWF")
else echo "No terminal and no $PWF. In your own terminal run:" >&2
     echo "  read -rs P && printf %s \"\$P\" > $PWF && chmod 600 $PWF; unset P" >&2; exit 1; fi

# credentials go through curl's config on an fd, never argv
call() { local m=$1 url=$2; shift 2
  curl -fsS -K <(printf 'user = "%s:%s"\n' "$WU" "$WP") -X "$m" -H 'Content-Type: application/json' "$@" "$url" \
    || { echo "FAILED: $m $url" >&2; return 1; }; }
# Knox returns an empty body for /version; CM still serves older API versions, so fall back.
V=$(call GET "$GW/cm-api/version" || true); V=${V:-v51}
cm() { local m=$1 p=$2; shift 2; call "$m" "$GW/cm-api/$V$p" "$@"; }
py() { python3 -c "$1"; }
if [ "$MODE" = raw ]; then cm GET "$2"; echo; exit; fi   # read-only debugging: raw <path>
CL=$(cm GET "/clusters?clusterType=any" | py 'import json,sys;i=json.load(sys.stdin)["items"];print(i[0]["name"] if i else "")')
if [ -z "$CL" ]; then
  echo "CM ($V) lists no clusters for $WU. CM's view of this user:" >&2
  cm GET "/users/current" >&2 || true; echo >&2
  cm GET "/cm/version" >&2 || true; echo >&2
  exit 1
fi
YARN=$(cm GET "/clusters/$CL/services" | py 'import json,sys;print([s["name"] for s in json.load(sys.stdin)["items"] if s["type"]=="YARN"][0])')
echo "CM API $V, cluster '$CL', YARN service '$YARN'"
if [ "$MODE" = find ]; then   # read-only: settings (suppressions too) whose name/label matches $2
  for path in "/clusters/$CL/services/$YARN/config" \
              /clusters/$CL/services/$YARN/roleConfigGroups/$YARN-NODEMANAGER-{WORKER,COMPUTE}/config \
              /clusters/$CL/services/$YARN/roleConfigGroups/$YARN-RESOURCEMANAGER-BASE/config; do
    cm GET "$path?view=full" | P=${path##*/$YARN} RX="$2" py '
import json,sys,os,re
rx=re.compile(os.environ["RX"],re.I)
for i in json.load(sys.stdin)["items"]:
    if rx.search(i["name"]) or rx.search(i.get("displayName","")):
        print(os.environ["P"],"|",i["name"],"|",i.get("displayName",""),"|",i.get("value","<default %s>"%i.get("default")))' || true
  done; exit; fi

wait_cmd() { local id=$1
  while :; do
    out=$(cm GET "/commands/$id")
    if echo "$out" | py 'import json,sys;sys.exit(0 if json.load(sys.stdin)["active"] else 1)'; then sleep 15; continue; fi
    echo "$out" | py 'import json,sys;c=json.load(sys.stdin);print("command",c["name"],"success=%s"%c.get("success"),c.get("resultMessage",""))'
    echo "$out" | py 'import json,sys;sys.exit(0 if json.load(sys.stdin).get("success") else 1)'
    return
  done; }
restart_yarn() { echo "Restarting $YARN ..."; wait_cmd "$(cm POST "/clusters/$CL/services/$YARN/commands/restart" | py 'import json,sys;print(json.load(sys.stdin)["id"])')"; }
items() { python3 -c 'import json,sys;a=sys.argv[1:];print(json.dumps({"items":[{"name":k,"value":v} for k,v in zip(a[::2],a[1::2])]}))' "$@"; }
compute_nm_rcg() { cm GET "/clusters/$CL/hostTemplates" | py '
import json,sys
for t in json.load(sys.stdin)["items"]:
    if t["name"]=="compute":
        print([r["roleConfigGroupName"] for r in t["roleConfigGroupRefs"] if "NODEMANAGER" in r["roleConfigGroupName"]][0])'; }
nm_hosts() { cm GET "/clusters/$CL/services/$YARN/roles" | py '
import json,sys
for r in json.load(sys.stdin)["items"]:
    if r["type"]=="NODEMANAGER": print(r["hostRef"]["hostId"], r["hostRef"].get("hostname",""))'; }
set_rm() { while read -r id name; do
    [ -n "$id" ] || continue
    cm PUT "/hosts/$id/config" -d "$(items rm_enabled "$1")" >/dev/null; echo "rm_enabled=$1 on ${name:-$id}"
  done <<< "$(nm_hosts)"; }
compute_hosts() { cm GET "/hosts" | py '
import json,sys
for h in json.load(sys.stdin)["items"]:
    if "-compute" in h["hostname"]: print(h["hostId"], h["hostname"])'; }

case "$MODE" in
show)
  echo "--- YARN service config (non-default + cgroup/LCE/GPU keys)"
  cm GET "/clusters/$CL/services/$YARN/config?view=full" | py '
import json,sys
for i in json.load(sys.stdin)["items"]:
    n=i["name"]
    if "suppression" in n: continue
    if any(k in n for k in ("cgroup","lce","gpu","resource_types","container_executor")):
        print(" ",n,"=",i.get("value","<default %s>"%i.get("default")))'
  echo "--- host templates -> NODEMANAGER role config group"
  cm GET "/clusters/$CL/hostTemplates" | py '
import json,sys
for t in json.load(sys.stdin)["items"]:
    print(" ",t["name"],[r["roleConfigGroupName"] for r in t["roleConfigGroupRefs"] if "NODEMANAGER" in r["roleConfigGroupName"]])'
  echo "--- NODEMANAGER role config groups: GPU/cgroup keys"
  for g in $(cm GET "/clusters/$CL/services/$YARN/roleConfigGroups" | py 'import json,sys;[print(g["name"]) for g in json.load(sys.stdin)["items"] if g["roleType"]=="NODEMANAGER"]'); do
    cm GET "/clusters/$CL/services/$YARN/roleConfigGroups/$g/config?view=full" | G=$g py '
import json,sys,os
for i in json.load(sys.stdin)["items"]:
    n=i["name"]
    if "suppression" in n: continue
    if "gpu" in n or "cgroup" in n or "resource_plugins" in n:
        print(" ",os.environ["G"],n,"=",i.get("value","<default %s>"%i.get("default")))'
  done
  echo "--- hosts: rm_enabled"
  for h in $(cm GET "/hosts" | py 'import json,sys;[print(h["hostId"]+"|"+h["hostname"]) for h in json.load(sys.stdin)["items"]]'); do
    cm GET "/hosts/${h%%|*}/config?view=full" | H=${h#*|} py '
import json,sys,os
v=[i.get("value","<default %s>"%i.get("default")) for i in json.load(sys.stdin)["items"] if i["name"]=="rm_enabled"]
print(" ",os.environ["H"],"rm_enabled =",v[0] if v else "n/a")'
  done ;;
cgroups)
  cm PUT "/clusters/$CL/services/$YARN/config" -d "$(items yarn_service_cgroups true yarn_service_lce_always true)" >/dev/null
  echo "Set yarn_service_cgroups=true, yarn_service_lce_always=true"
  set_rm true   # CM refuses YARN cgroups unless every NodeManager host has rm_enabled
  restart_yarn ;;
revert)   # undo a `cgroups` whose restart never ran: service config only, no host PUT, no restart
  cm PUT "/clusters/$CL/services/$YARN/config" -d "$(items yarn_service_cgroups false yarn_service_lce_always false)" >/dev/null
  echo "Set yarn_service_cgroups=false, yarn_service_lce_always=false (never deployed, so no restart)" ;;
cgroups-off)   # revert of `cgroups`, if the cgroup v2 workers' NMs don't come back
  cm PUT "/clusters/$CL/services/$YARN/config" -d "$(items yarn_service_cgroups false yarn_service_lce_always false)" >/dev/null
  echo "Set yarn_service_cgroups=false, yarn_service_lce_always=false"
  set_rm false
  restart_yarn ;;
gpu)
  rcg=$(compute_nm_rcg); hosts=$(compute_hosts)
  test -n "$hosts" || { echo "No compute host in CM yet: scale compute to 1 first."; exit 1; }
  cm PUT "/clusters/$CL/services/$YARN/roleConfigGroups/$rcg/config" \
     -d "$(items gpu_enabled true gpu_plugin_allowed_devices auto gpu_plugin_detector_path /usr/bin/nvidia-smi)" >/dev/null
  echo "GPU settings on role config group $rcg"
  while read -r id name; do
    cm PUT "/hosts/$id/config" -d "$(items rm_enabled true)" >/dev/null; echo "rm_enabled on $name"
  done <<< "$hosts"
  restart_yarn ;;
nodes)
  call GET "$GW/resourcemanager/v1/cluster/nodes" | py '
import json,sys
for n in json.load(sys.stdin)["nodes"]["node"]:
    r={x["name"]:x["value"] for x in n["totalResource"].get("resourceInformations",{}).get("resourceInformation",[])}
    print(n["nodeHostName"],n["state"],"vcores=%s mem=%s gpu=%s"%(r.get("vcores"),r.get("memory-mb"),r.get("yarn.io/gpu",0)))' ;;
*) echo "usage: $0 show|cgroups|gpu|nodes|find <regex>|raw <path>|revert|cgroups-off"; exit 2 ;;
esac
