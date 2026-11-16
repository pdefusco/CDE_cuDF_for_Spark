#!/usr/bin/env bash
# Does the stock (open-source) RAPIDS jar link against this Spark build, or does it need the 2-class patch?
#
# The answer is in one class, the constructor of org.apache.spark.rdd.MapPartitionsRDD in spark-core:
#   7 args in bytecode (upstream Apache)                      -> stock RAPIDS jar links, no patch
#   8 args, an extra scala.Option (Cloudera's isDeterministic) -> NoSuchMethodError on the first GPU
#                                                                 action; use the patched jar
# Both kinds of build report themselves as Spark 3.5.4, so the version string can't tell them apart.
# See docs/oss-rapids-vs-cloudera-spark.md.
#
# Usage: scripts/check_spark_core.sh SOURCE [SOURCE...]
#   /path/spark-core_2.12-X.jar   a local jar
#   /path/dir                     every spark-core jar under a local directory
#   docker://IMAGE                a CDE runtime image; jars in $IMAGE_JARS_DIR (default /opt/spark/jars)
#   ssh://HOST[/path]             every spark-core jar under /path on a cluster host. The default,
#                                 /opt/cloudera/parcels/, covers every parcel on the host, active or not.
#                                 Extra ssh flags go in $SSH_OPTS, e.g. SSH_OPTS="-F ds1113-ssh.config"
#   https://.../NAME.parcel       a parcel; streamed, keeping only spark-core. A CDH parcel is several
#                                 GB of download, so prefer ssh:// when a cluster is up
#   https://.../parcels/          a parcel directory listing; checks its -$PARCEL_OS.parcel (default el9)
# Paywalled archive.cloudera.com/p/ URLs use CLOUDERA_PAYWALL_USER / CLOUDERA_PAYWALL_PASSWORD, or
# ~/.netrc. The credentials reach curl on stdin, never on its command line.
#
# Needs a JDK's javap locally (set JAVAP to override).
# Exit status: 0 every jar has the upstream constructor, 1 at least one needs the patch,
# 2 a jar or source couldn't be classified.
set -euo pipefail

JAVAP=${JAVAP:-javap}
PARCEL_OS=${PARCEL_OS:-el9}
IMAGE_JARS_DIR=${IMAGE_JARS_DIR:-/opt/spark/jars}
DEFAULT_SSH_PATH=/opt/cloudera/parcels/
JAR_GLOB='spark-core_2.1[23]-*.jar'   # Scala 2.12/2.13 only; skips Spark 2's spark-core_2.11

# Constructor descriptors, as javap -s prints them.
UPSTREAM='(Lorg/apache/spark/rdd/RDD;Lscala/Function3;ZZZLscala/reflect/ClassTag;Lscala/reflect/ClassTag;)V'
CLOUDERA='(Lorg/apache/spark/rdd/RDD;Lscala/Function3;ZZZLscala/Option;Lscala/reflect/ClassTag;Lscala/reflect/ClassTag;)V'

[[ $# -gt 0 ]] || { sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
"$JAVAP" -version >/dev/null 2>&1 || { echo "error: no working javap; install a JDK or set JAVAP" >&2; exit 2; }

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
worst=0
status() { [[ $1 -gt $worst ]] && worst=$1; return 0; }

# URL -> stdout. With CLOUDERA_PAYWALL_USER set, basic auth goes in a curl config read from stdin.
fetch() {
  if [[ -n ${CLOUDERA_PAYWALL_USER:-} ]]; then
    local u=$CLOUDERA_PAYWALL_USER p=${CLOUDERA_PAYWALL_PASSWORD:-}
    u=${u//\\/\\\\}; u=${u//\"/\\\"}; p=${p//\\/\\\\}; p=${p//\"/\\\"}
    printf 'user = "%s:%s"\n' "$u" "$p" | curl -fsSL -K - "$1"
  else
    curl -fsSL --netrc-optional "$1"
  fi
}

# tar stream on stdin -> spark-core jars under DIR. Pass "z" for gzip (GNU tar won't sniff stdin).
untar_spark_core() {
  local dir=$1 z=${2:-}
  if tar --version 2>/dev/null | grep -q 'GNU tar'; then
    tar -x${z}f - -C "$dir" --wildcards --no-anchored "$JAR_GLOB"
  else
    tar -x${z}f - -C "$dir" --include "*$JAR_GLOB"
  fi
}

check_jar() {  # JAR SOURCE-LABEL
  local jar=$1 label=$2 props version built ctors result
  props=$(unzip -p "$jar" spark-version-info.properties 2>/dev/null | tr -d '\r' || true)
  version=$(awk -F' *= *' '$1~/^ *version$/{print $2}' <<<"$props")
  built=$(awk -F' *= *' '$1~/^ *date$/{print substr($2,1,10)}' <<<"$props")
  # Fall back to the version in the file name, e.g. spark-core_2.12-3.5.4.1.26.732.0-45.jar.
  if [[ -z $version && $label =~ spark-core_2\.1[23]-(.+)\.jar$ ]]; then version=${BASH_REMATCH[1]}; fi
  # javap -s prints each member, then its descriptor on the next line.
  ctors=$("$JAVAP" -cp "$jar" -p -s org.apache.spark.rdd.MapPartitionsRDD 2>/dev/null \
    | awk '/ org\.apache\.spark\.rdd\.MapPartitionsRDD\(/ {getline; sub(/^ *descriptor: */, ""); print}' || true)
  if grep -qxF "$UPSTREAM" <<<"$ctors"; then
    result="no patch"; status 0
  elif grep -qxF "$CLOUDERA" <<<"$ctors"; then
    result="PATCH"; status 1
  else
    result="UNKNOWN"; status 2
  fi
  printf '%-9s %-34s %-11s %s\n' "$result" "${version:-?}" "${built:-?}" "$label"
  if [[ $result == UNKNOWN ]]; then echo "          constructors: $(tr '\n' ' ' <<<"${ctors:-none found}")"; fi
}

check_dir() {  # DIR SOURCE-LABEL; every regular spark-core jar under DIR (symlinked copies skipped)
  local dir=$1 label=$2 jars jar
  jars=$(find "$dir" -name "$JAR_GLOB" -type f 2>/dev/null | sort)
  [[ -n $jars ]] || { printf '%-9s %-34s %-11s %s\n' NONE - - "$label (no spark-core jar)"; status 2; return; }
  while IFS= read -r jar; do check_jar "$jar" "$label${jar#"$dir"}"; done <<<"$jars"
}

check_source() {
  local src=$1 d
  d=$(mktemp -d "$work/src.XXXX")
  case $src in
    docker://*)
      local img=${src#docker://} cid
      if cid=$(docker create --platform linux/amd64 "$img" true 2>/dev/null); then
        docker cp "$cid:$IMAGE_JARS_DIR/." - 2>/dev/null | untar_spark_core "$d" || true
        docker rm "$cid" >/dev/null
        check_dir "$d" "$src:$IMAGE_JARS_DIR"
      else
        printf '%-9s %-34s %-11s %s\n' FAILED - - "$src (docker create failed)"; status 2
      fi ;;
    ssh://*)
      local rest=${src#ssh://} host path jars f i=0
      host=${rest%%/*}
      [[ $rest == */* ]] && path=/${rest#*/} || path=$DEFAULT_SSH_PATH
      # shellcheck disable=SC2086
      if ! jars=$(ssh -n $SSH_OPTS "$host" "find '$path' -name '$JAR_GLOB' -type f 2>/dev/null | sort"); then
        printf '%-9s %-34s %-11s %s\n' FAILED - - "$src (ssh failed)"; status 2; return
      fi
      [[ -n $jars ]] || { printf '%-9s %-34s %-11s %s\n' NONE - - "$src (no spark-core jar)"; status 2; return; }
      while IFS= read -r f; do
        i=$((i + 1))
        # shellcheck disable=SC2086
        ssh -n $SSH_OPTS "$host" "cat '$f'" >"$d/$i.jar" && check_jar "$d/$i.jar" "ssh://$host$f"
      done <<<"$jars" ;;
    http://*/ | https://*/)
      local names name
      names=$(fetch "$src" | grep -oE "href=\"[^\"/]*-$PARCEL_OS\\.parcel\"" | sed -E 's/^href="//; s/"$//' | sort -u || true)
      [[ -n $names ]] || { printf '%-9s %-34s %-11s %s\n' NONE - - "$src (no -$PARCEL_OS.parcel listed, or fetch failed)"; status 2; return; }
      while IFS= read -r name; do check_source "$src$name"; done <<<"$names" ;;
    http://* | https://*)
      if fetch "$src" | untar_spark_core "$d" z; then
        check_dir "$d" "$src!"
      else
        printf '%-9s %-34s %-11s %s\n' FAILED - - "$src (download or untar failed)"; status 2
      fi ;;
    *)
      if [[ -f $src ]]; then check_jar "$src" "$src"
      elif [[ -d $src ]]; then check_dir "$src" "${src%/}"
      else printf '%-9s %-34s %-11s %s\n' FAILED - - "$src (not a file, directory, or known scheme)"; status 2
      fi ;;
  esac
}

printf '%-9s %-34s %-11s %s\n' RESULT "SPARK BUILD" BUILT SOURCE
for src in "$@"; do check_source "$src"; done
exit $worst
