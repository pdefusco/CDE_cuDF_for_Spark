#!/usr/bin/env bash
# For every Spark symbol the RAPIDS jar references, does a member with a matching descriptor exist in
# this cluster's Spark? That is the JVM's link-time question, and the only one that can produce a
# NoSuchMethodError like the MapPartitionsRDD one. This is the scan of docs/linkage-scan.md (done there
# by hand against CDE 1.26), as a script.
#
# Only classes that load on Spark 3.5.4 are scanned: spark354/, spark-shared/ and the unprefixed roots.
# Every Methodref, InterfaceMethodref and Fieldref owned by an org/apache/spark/** class is resolved
# against the Spark 3 jars plus the client classpath.txt, walking superclasses and interfaces, as is
# every class reference. Misses are counted, not listed, when no caller is reachable from spark354/ or
# the unprefixed classes (spark-shared/ code kept for other shims), or when every live caller is Delta,
# Iceberg or Databricks code. The rest need triage by hand: a miss is a prediction, not a defect (see
# the doc). Resolution stops at JDK classes, which are not indexed, so members inherited from one are
# listed separately as unverified rather than as missing.
#
# Run on a Spark 3 gateway host after plugin_load.sh. Scans the stock jar in $WORK and, if present, the
# patched one from patch_rapids_jar.sh in $OUT, then prints what differs between them.
# Env: RAPIDS_VERSION (default 26.10.0), WORK (default /tmp/rapids), OUT (default /tmp/rapids-patched).
set -euo pipefail
# Spark 3 is the SPARK3 (CDS 3.5) parcel on 7.1.9 and part of the CDH parcel on 7.3.x.
spark=/opt/cloudera/parcels/SPARK3; [[ -d $spark ]] || spark=/opt/cloudera/parcels/CDH
spark_conf=/etc/spark3/conf; [[ -d $spark_conf ]] || spark_conf=/etc/spark/conf
name=rapids-4-spark_2.12-${RAPIDS_VERSION:-26.10.0}-cuda12.jar
scan=(${WORK:-/tmp/rapids}/$name)
[[ -s ${OUT:-/tmp/rapids-patched}/$name ]] && scan+=(${OUT:-/tmp/rapids-patched}/$name)
echo "== Spark: $(ls $spark/lib/spark3/jars/spark-core_2.12-*.jar | head -1 | xargs basename)"

python3 - $spark/lib/spark3/jars $spark_conf/classpath.txt "${scan[@]}" <<'EOF'
import collections, os, re, struct, sys, zipfile

jars_dir, cp_txt, targets = sys.argv[1], sys.argv[2], sys.argv[3:]

def parse(data):
    """Constant-pool references, declared members and supertypes of one class file."""
    u2 = lambda p: struct.unpack_from('>H', data, p)[0]
    n, pos, cp, i = u2(8), 10, {}, 1
    while i < n:
        tag = data[pos]
        if tag == 1:
            ln = u2(pos + 1); cp[i] = data[pos + 3:pos + 3 + ln].decode('utf-8', 'replace'); pos += 3 + ln
        elif tag in (3, 4): pos += 5
        elif tag in (5, 6): pos += 9; i += 1
        elif tag in (7, 8, 16, 19, 20): cp[i] = (tag, u2(pos + 1)); pos += 3
        elif tag in (9, 10, 11, 12, 17, 18): cp[i] = (tag, u2(pos + 1), u2(pos + 3)); pos += 5
        elif tag == 15: pos += 4
        else: raise ValueError('constant pool tag %d' % tag)
        i += 1
    cls = lambda k: cp[cp[k][1]]
    this, sup, ni = u2(pos + 2), u2(pos + 4), u2(pos + 6); pos += 8
    ifaces = [cls(u2(pos + 2 * j)) for j in range(ni)]; pos += 2 * ni
    members = set()
    for _ in range(2):  # fields, then methods
        cnt = u2(pos); pos += 2
        for _ in range(cnt):
            members.add((cp[u2(pos + 2)], cp[u2(pos + 4)])); na = u2(pos + 6); pos += 8
            for _ in range(na): pos += 6 + struct.unpack_from('>I', data, pos + 2)[0]
    refs, classrefs, strings = set(), set(), set()
    for e in cp.values():
        if isinstance(e, tuple) and e[0] in (9, 10, 11):
            nt = cp[e[2]]; refs.add((cls(e[1]), cp[nt[1]], cp[nt[2]]))
        elif isinstance(e, tuple) and e[0] == 7:
            classrefs.add(cp[e[1]])
        elif isinstance(e, tuple) and e[0] == 8:
            strings.add(cp[e[1]])
    return {'name': cls(this), 'sup': cls(sup) if sup else None, 'ifaces': ifaces,
            'members': members, 'refs': refs, 'classrefs': classrefs, 'strings': strings}

def elem(t):  # array class names to their element class; None for primitive arrays
    t = t.lstrip('[')
    return t[1:-1] if t.startswith('L') else (None if len(t) == 1 else t)

# Spark side: class name -> jar, the jars dir first (it is first on the classpath).
cp_jars = sorted(os.path.join(jars_dir, f) for f in os.listdir(jars_dir) if f.endswith('.jar'))
if os.path.exists(cp_txt):
    cp_jars += [l.strip() for l in open(cp_txt) if l.strip().endswith('.jar') and os.path.exists(l.strip())]
index = {}
for j in cp_jars:
    try:
        for e in zipfile.ZipFile(j).namelist():
            if e.endswith('.class') and not e.startswith(('META-INF/', 'module-info')):
                index.setdefault(e[:-6], j)
    except (zipfile.BadZipFile, OSError):
        pass
print('Spark side: %d classes in %d jars (%s and %s)' % (len(index), len(set(index.values())), jars_dir, cp_txt))
spark_cache = {}
def spark_class(c):
    if c not in spark_cache:
        spark_cache[c] = parse(zipfile.ZipFile(index[c]).read(c + '.class')) if c in index else None
    return spark_cache[c]
OBJECT = {('<init>', '()V'), ('getClass', '()Ljava/lang/Class;'), ('hashCode', '()I'),
          ('equals', '(Ljava/lang/Object;)Z'), ('clone', '()Ljava/lang/Object;'), ('toString', '()Ljava/lang/String;'),
          ('notify', '()V'), ('notifyAll', '()V'), ('wait', '()V'), ('wait', '(J)V'), ('wait', '(JI)V'),
          ('finalize', '()V')}
JDK = ('java/', 'javax/', 'jdk/', 'sun/')
MARKERS = {'java/io/Serializable', 'java/lang/Cloneable', 'java/util/RandomAccess', 'java/util/EventListener'}
SHIM = re.compile(r'^(spark3\d+[a-z0-9]*|spark-shared)/')

def scan(path):
    z = zipfile.ZipFile(path)
    names = [e for e in z.namelist() if e.endswith('.class')]
    # What a 3.5.4 session loads: spark354/ before spark-shared/ before the unprefixed roots.
    rank = {'spark354/': 0, 'spark-shared/': 1}
    scope = {}
    for e in names:
        m = SHIM.match(e)
        p = m.group(0) if m else ''
        if p and p not in rank: continue
        c = e[len(p):-6]
        if c not in scope or rank.get(p, 2) < rank.get(scope[c][0], 2): scope[c] = (p, e)
    rapids = {c: parse(z.read(e)) for c, (p, e) in scope.items()}
    def lookup(c):  # parent-first: a Spark class shadows a RAPIDS one of the same name
        return spark_class(c) or rapids.get(c)
    def resolve(owner, name, desc):
        seen, stack, jdk = set(), [owner], False
        while stack:
            c = stack.pop()
            if c in seen or c in MARKERS: continue
            seen.add(c)
            if c == 'java/lang/Object':
                if (name, desc) in OBJECT: return 'ok'
                continue
            info = lookup(c)
            if info is None:
                jdk |= c.startswith(JDK); continue
            if (name, desc) in info['members']: return 'ok'
            if name == '<init>': break  # constructors are not inherited
            stack += ([info['sup']] if info['sup'] else []) + info['ifaces']
        return 'jdk' if jdk and name != '<init>' else 'missing'
    # spark-shared/ means byte-identical across *some* shims, which need not include 3.5.4, so a class
    # there counts as live only if a spark354/ or unprefixed class reaches it: through a class
    # reference, a member's owner, a class-name string (ShimLoader and Class.forName go by name) or a
    # META-INF/services entry. Class-level, so it over-counts (a live class's dead method still counts).
    services = {l.strip().replace('.', '/') for e in z.namelist() if e.startswith('META-INF/services/')
                for l in z.read(e).decode('utf-8', 'replace').splitlines() if l.strip() and not l.startswith('#')}
    live = set()
    stack = [c for c, (p, e) in scope.items() if p != 'spark-shared/'] + [c for c in services if c in rapids]
    while stack:
        c = stack.pop()
        if c in live: continue
        live.add(c)
        info = rapids[c]
        out = {elem(o) for o in info['classrefs']} | {elem(o) for o, n, d in info['refs']} \
            | {s.replace('.', '/') for s in info['strings']}
        stack += [o for o in out if o in rapids and o not in live]
    live_paths = {scope[c][1] for c in live}
    callers = collections.defaultdict(set)
    for c, info in rapids.items():
        caller = scope[c][1]
        for o, n, d in info['refs']:
            o = elem(o)
            if o and o.startswith('org/apache/spark/'): callers[('m', o, n, d)].add(caller)
        for o in info['classrefs']:
            o = elem(o)
            if o and o.startswith('org/apache/spark/'): callers[('c', o, '', '')].add(caller)
    result = {}
    for k in callers:
        kind, o, n, d = k
        if lookup(o) is None: result[k] = 'noclass'
        elif kind == 'm': result[k] = resolve(o, n, d)
        else: result[k] = 'ok'
    # External: owned by a class the RAPIDS jar does not define (RAPIDS has its own org.apache.spark ones).
    external = [k for k in callers if k[0] == 'm' and k[1] not in rapids]
    print('\n########## %s' % os.path.basename(os.path.dirname(path)) + '/' + os.path.basename(path))
    print('In scope: %d of %d classes, %d of them reachable from spark354/ and the roots'
          % (len(scope), len(names), len(live)))
    print('External Spark member references: %d unique, %d resolve'
          % (len(external), sum(1 for k in external if result[k] == 'ok')))
    # Callers in Delta Lake (com.databricks...tahoe is Databricks' Delta), Iceberg or Databricks code.
    moot_re = re.compile(r'delta|tahoe|iceberg|databricks', re.I)
    def why_moot(k):
        cs = [c for c in callers[k] if c in live_paths]
        if not cs: return 'unreachable'
        if all(moot_re.search(c) for c in cs): return 'Delta/Iceberg/Databricks'
        return None
    for status, title in (('missing', 'Missing members'), ('noclass', 'Missing classes'),
                          ('jdk', 'Unverified (inherited from a JDK class, which is not indexed)')):
        ks = sorted(k for k in callers if result[k] == status and (status != 'noclass' or k[0] == 'c'
                    or ('c', k[1], '', '') not in callers))
        moot = collections.Counter(why_moot(k) for k in ks if why_moot(k))
        live_ks = [k for k in ks if not why_moot(k)]
        print('-- %s: %d%s; %d to triage' % (title, len(ks), ''.join(', %d %s-only' % (v, a)
              for a, v in sorted(moot.items())), len(live_ks)))
        for k in live_ks:
            sym = k[1].replace('/', '.') + ('.%s%s' % (k[2], k[3]) if k[0] == 'm' else '')
            cs = sorted(c for c in callers[k] if c in live_paths)
            print('   %s\n      <- %s%s' % (sym[:300], cs[0], ' (+%d more)' % (len(cs) - 1) if len(cs) > 1 else ''))
    return {k for k in callers if result[k] != 'ok'}

misses = [scan(t) for t in targets]
if len(misses) == 2:
    print('\n########## stock vs patched')
    for title, ks in (('Missing only in the stock jar', misses[0] - misses[1]),
                      ('Missing only in the patched jar', misses[1] - misses[0])):
        print('-- %s: %d' % (title, len(ks)))
        for k in sorted(ks): print('   %s.%s%s' % (k[1].replace('/', '.'), k[2], k[3]))
EOF
