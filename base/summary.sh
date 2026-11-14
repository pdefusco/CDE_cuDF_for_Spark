#!/usr/bin/env bash
# Re-read the logs of benchmark.sh (no re-run): per-query results, CPU fallbacks and the GPU vs CPU
# table. Spark's console progress bar ends its lines with \r, so BENCH lines are split out of it first
# (benchmark.sh now turns the bar off, but older logs have it).
# Run on the gateway host that ran benchmark.sh. Env: WORK (default /tmp/rapids).
set -u
dir=${WORK:-/tmp/rapids}
clean() { sed -E 's/\r/\n/g' "$1" | sed -E 's/^\s*\[Stage [^]]*\]\s*//'; }

for label in gpu cpu; do
  echo "########## $label"
  clean $dir/bench-$label.log | grep -E "^(PROBE|BENCH)" | cut -c1-400
  echo "-- CPU fallback reasons (spark.rapids.sql.explain=NOT_ON_GPU)"
  clean $dir/bench-$label.log | grep -E "cannot run on GPU|will not work on GPU" | sed -E 's/^\s+//' \
    | sort | uniq -c | sort -rn | cut -c1-260 | head -15
done

echo; echo "== Summary (warm run, seconds)"
for label in gpu cpu; do clean $dir/bench-$label.log | grep -E "^BENCH query=.*wall=" | sed "s/^/$label /"; done \
  | awk '{ match($0, /query=[a-z]+/); q = substr($0, RSTART + 6, RLENGTH - 6)
           match($0, /wall=[0-9.]+/); w = substr($0, RSTART + 5, RLENGTH - 5)
           if ($1 == "gpu") g[q] = w; else c[q] = w
           if (!(q in seen)) { seen[q] = 1; order[++n] = q } }
         END { printf "%-8s %8s %8s %8s\n", "query", "gpu", "cpu", "speedup"
               for (i = 1; i <= n; i++) { q = order[i]; tg += g[q]; tc += c[q]
                 printf "%-8s %8s %8s %8s\n", q, g[q], c[q], (g[q] && c[q]) ? sprintf("%.1fx", c[q] / g[q]) : "-" }
               printf "%-8s %8.1f %8.1f %7.1fx\n", "total", tg, tc, tg ? tc / tg : 0 }'
