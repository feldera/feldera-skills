#!/usr/bin/env python3
"""Derive triage indicators from a Feldera support bundle.

Every number printed is a ratio, a share, a dispersion measure, or a
comparison against the pipeline's own configuration. No absolute
thresholds are baked in: the caller judges the values in context.

Usage:
    bundle_probe.py <extracted-bundle-dir> [--section SECTION] [--top N]

Sections: state, errors, trend, time, skew, sql, memory, disk, all
"""
import argparse
import collections
import glob
import gzip
import json
import math
import os
import re
import sys

ANSI = re.compile(r"\x1b\[[0-9;]*m")


def snapshots(root):
    """Collection directories, oldest first. Names are ISO timestamps."""
    dirs = [d for d in glob.glob(os.path.join(root, "*")) if os.path.isdir(d)]
    return sorted(dirs, key=os.path.basename)


def artifact_path(path):
    """-> path, or path + ".gz" when only the gzipped copy exists, or None."""
    for p in (path, path + ".gz"):
        if os.path.exists(p):
            return p
    return None


def load_json(path):
    # bundles may ship any JSON artifact gzipped (circuit_profile.json.gz)
    path = artifact_path(path)
    if not path:
        return None
    opener = gzip.open if path.endswith(".gz") else open
    try:
        with opener(path, "rt") as f:
            return json.load(f)
    except (ValueError, OSError) as e:
        print(f"  ! unreadable {os.path.basename(path)}: {e}")
        return None


def fmt_bytes(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1000:
            return f"{n:.1f}{unit}"
        n /= 1000
    return f"{n:.1f}PB"


# --------------------------------------------------------------------------
# config settings -- read under either name, the way Feldera reads them
# --------------------------------------------------------------------------
# Feldera 0.366.0 moved the unit of each duration setting from its name into
# its value (checkpoint_interval_secs: 60 -> checkpoint_interval: "1m"), and
# 0.367.0 did the same for sizes (memory_mb_max: 16000 -> memory_max: "16G").
# A config saved since then carries only the new names; one saved before, only
# the old ones. The value type picks the parser, not the name: a string
# carries its unit, a bare number counts the unit of the old name.
Setting = collections.namedtuple("Setting", "section current legacy unit kind")
Found = collections.namedtuple("Found", "key written value current")

MB = 1_000_000  # memory_mb_* is decimal: 128000 exports max_rss_bytes=125952000000
MAX_RSS = Setting(None, "max_rss", "max_rss_mb", MB, "size")
DATAFUSION_MEMORY = Setting(None, "datafusion_memory", "datafusion_memory_mb", MB, "size")
MEMORY_MIN = Setting("resources", "memory_min", "memory_mb_min", MB, "size")
MEMORY_MAX = Setting("resources", "memory_max", "memory_mb_max", MB, "size")
STORAGE_MIN = Setting("resources", "storage_min", "storage_mb_min", MB, "size")
STORAGE_MAX = Setting("resources", "storage_max", "storage_mb_max", MB, "size")
CACHE_SIZE = Setting("storage", "cache_size", "cache_mib", 1 << 20, "size")
MIN_SPILL_SIZE = Setting("storage", "min_spill_size", "min_storage_bytes", 1, "size")
MIN_STEP_SPILL_SIZE = Setting("storage", "min_step_spill_size", "min_step_storage_bytes",
                              1, "size")
SIZE_SETTINGS = (MAX_RSS, DATAFUSION_MEMORY, MEMORY_MIN, MEMORY_MAX, STORAGE_MIN,
                 STORAGE_MAX, CACHE_SIZE, MIN_SPILL_SIZE, MIN_STEP_SPILL_SIZE)
SIZE_RENAME_VERSION = (0, 367, 0)
CHECKPOINT_INTERVAL = Setting("fault_tolerance", "checkpoint_interval",
                              "checkpoint_interval_secs", 1, "duration")

U64_MAX = (1 << 64) - 1
DIGITS = frozenset("0123456789")  # ASCII only, as Rust's is_ascii_digit
SIZE_UNITS = {"Ki": 1 << 10, "Mi": 1 << 20, "Gi": 1 << 30, "Ti": 1 << 40,
              "Pi": 1 << 50, "Ei": 1 << 60, "k": 10**3, "K": 10**3, "M": 10**6,
              "G": 10**9, "T": 10**12, "P": 10**15, "E": 10**18}
DURATION_NANOS = {"ns": 1, "us": 10**3, "µs": 10**3, "μs": 10**3,
                  "ms": 10**6, "s": 10**9, "m": 60 * 10**9, "h": 3600 * 10**9,
                  "d": 86400 * 10**9}


def scan_number(text, at):
    """-> (whole, fraction, end): digits, then at most one point and digits."""
    end = at
    while end < len(text) and text[end] in DIGITS:
        end += 1
    whole, fraction = text[at:end], ""
    if end < len(text) and text[end] == ".":
        start = end = end + 1
        while end < len(text) and text[end] in DIGITS:
            end += 1
        fraction = text[start:end]
    return whole, fraction, end


def scaled(whole, fraction, unit):
    """whole.fraction * unit, exact, to the nearest integer (halves up), or None."""
    whole = whole.lstrip("0")
    if len(whole) > 40:  # overflows any unit; int() also refuses huge strings
        return None
    total = int(whole or "0") * unit
    if fraction:
        kept = fraction[:18]
        den = 10 ** len(kept)
        total += (unit * int(kept) + den // 2) // den
    return total


def parse_size(text):
    """Bytes in a quantity such as "16G" or "1.5Gi", as Feldera's size.rs, or None."""
    if text.startswith("-"):
        return None
    body = text[1:] if text.startswith("+") else text
    whole, fraction, end = scan_number(body, 0)
    name = body[end:].removesuffix("B")
    unit = SIZE_UNITS.get(name) if name else 1
    if not (whole or fraction) or unit is None:
        return None
    total = scaled(whole, fraction, unit)
    return total if total is not None and total <= U64_MAX else None


def parse_duration(text):
    """Nanoseconds in a duration such as "30s" or "1h30m", as Feldera's duration.rs, or None."""
    if text.startswith("-"):
        return None
    body = text[1:] if text.startswith("+") else text
    if body == "0":
        return 0  # the one value with no unit
    if not body:
        return None
    total = at = 0
    while at < len(body):
        whole, fraction, at = scan_number(body, at)
        start = at
        while at < len(body) and body[at] not in DIGITS and body[at] != ".":
            at += 1
        unit = DURATION_NANOS.get(body[start:at])
        if not (whole or fraction) or unit is None:
            return None
        term = scaled(whole, fraction, unit)
        if term is None:
            return None
        total += term
    return total if total // 10**9 <= U64_MAX else None


def whole_number(value):
    """A bare JSON number as Feldera reads it: whole, 0 to 2^64 - 1, or None."""
    if isinstance(value, bool):
        return None
    if isinstance(value, float):
        if not (math.isfinite(value) and value >= 0 and value.is_integer()
                and value < 2.0**64):
            return None
        value = int(value)
    return value if isinstance(value, int) and 0 <= value <= U64_MAX else None


def setting_value(raw, setting):
    """Bytes for a size, seconds for a duration, or None where Feldera refuses raw."""
    if isinstance(raw, str):
        if setting.kind == "duration":
            nanos = parse_duration(raw)
            return None if nanos is None else nanos / 1e9
        # "16000" would mean bytes where 16000 means MB, so Feldera refuses it
        digits = raw.removeprefix("+")
        if (setting.unit != 1 and any(c in DIGITS for c in digits)
                and all(c in DIGITS or c == "." for c in digits) and digits.count(".") <= 1):
            return None
        return parse_size(raw)
    n = whole_number(raw)
    if n is None:
        return None
    return min(n * setting.unit, U64_MAX) if setting.kind == "size" else n * setting.unit


def config_layers(cfg):
    """-> [deployment_config, runtime_config], the ones present, in read order.

    deployment_config is what the current or most recent run used, after any
    live resize; it is null until the first start. runtime_config is what the
    user saved, which can be an edit that has not run yet.
    """
    cfg = cfg or {}
    return [layer for layer in (cfg.get("deployment_config"), cfg.get("runtime_config"))
            if isinstance(layer, dict)]


def layer_node(layer, section):
    node = layer if section is None else layer.get(section)
    return node if isinstance(node, dict) else None


def lookup(layers, section, key):
    """The value of key in the first layer that has it, or None.

    A layer that has the key decides, even when it is null: deployment_config
    writes every field, so null there means unset for that run.
    """
    for layer in layers:
        node = layer_node(layer, section)
        if node is not None and key in node:
            return node[key]
    return None


def read_setting(layers, setting):
    """-> Found from the first layer that has either name, or None when unset.

    In one layer the current name wins over the old one; Feldera refuses a
    config with both.
    """
    for layer in layers:
        node = layer_node(layer, setting.section)
        if node is None or (setting.current not in node and setting.legacy not in node):
            continue
        for key, current in ((setting.current, True), (setting.legacy, False)):
            raw = node.get(key)
            if raw is not None:
                return Found(key, raw, setting_value(raw, setting), current)
        return None
    return None


def describe(found, setting):
    """The value as written, then what Feldera reads it as."""
    if found is None:
        return "unset"
    written = json.dumps(found.written)
    if found.value is None:
        return f"{written} (Feldera refuses this value)"
    if setting.kind == "duration":
        return f"{written} ({found.value:g}s)"
    return f"{written} ({fmt_bytes(found.value)})"


def size_key(cfg, setting):
    """The name of a size setting as this pipeline spells it.

    The name it sets; else the spelling of the size keys in the first layer
    that has any (null ones count); else the spelling its platform version reads.
    """
    layers = config_layers(cfg)
    found = read_setting(layers, setting)
    if found:
        return found.key
    for layer in layers:
        nodes = [layer_node(layer, s.section) or {} for s in SIZE_SETTINGS]
        if any(s.current in n for s, n in zip(SIZE_SETTINGS, nodes)):
            return setting.current
        if any(s.legacy in n for s, n in zip(SIZE_SETTINGS, nodes)):
            return setting.legacy
    m = re.match(r"v?(\d+)\.(\d+)\.(\d+)", str((cfg or {}).get("platform_version") or ""))
    current = bool(m) and tuple(map(int, m.groups())) >= SIZE_RENAME_VERSION
    return setting.current if current else setting.legacy


def snapshot_config(root, snap):
    """-> (snapshot, config): the config collected with snap, else the newest one.

    Budgets must come from the run that the profile or metrics describe; a
    later snapshot can hold an edited config or a different run.
    """
    if snap:
        cfg = load_json(os.path.join(snap, "pipeline_config.json"))
        if cfg:
            return snap, cfg
    for s in reversed(snapshots(root)):
        cfg = load_json(os.path.join(s, "pipeline_config.json"))
        if cfg:
            return s, cfg
    return None, None


# Budget fields compared between the two layers. A live resize (Enterprise,
# 0.365.0+) changes only CPU and memory in deployment_config, and an edit of
# CPU or memory overwrites it there, so a CPU or memory gap is a resize. Any
# other gap is an edit saved while stopped, which applies at the next start.
DRIFT_FIELDS = ((None, "workers"), (None, "io_workers"), ("resources", "cpu_cores_min"),
                ("resources", "cpu_cores_max"), ("resources", "storage_class"),
                ("fault_tolerance", "model"))
DRIFT_SETTINGS = (MEMORY_MIN, MEMORY_MAX, MAX_RSS, DATAFUSION_MEMORY, STORAGE_MAX,
                  MIN_SPILL_SIZE, CHECKPOINT_INTERVAL)
LIVE_RESIZE = {"cpu_cores_min", "cpu_cores_max", MEMORY_MIN.current, MEMORY_MAX.current}


def config_drift(cfg):
    """-> [(name, ran, saved, live resize?)] where runtime_config differs from deployment_config.

    Only keys that runtime_config sets are compared: deployment_config fills
    every other one with its default. Sizes and durations compare as bytes and
    seconds, so a respelling after the 0.366.0 or 0.367.0 rename is no gap.
    """
    cfg = cfg or {}
    dc, rc = cfg.get("deployment_config"), cfg.get("runtime_config")
    if not (isinstance(dc, dict) and isinstance(rc, dict)):
        return []
    rows = []
    for section, key in DRIFT_FIELDS:
        node = layer_node(rc, section)
        if node is None or key not in node:
            continue
        ran = lookup([dc], section, key)
        if ran != node[key]:
            rows.append((key, json.dumps(ran), json.dumps(node[key]), key in LIVE_RESIZE))

    def value(found):
        if found is None:
            return None
        return found.value if found.value is not None else ("refused", json.dumps(found.written))
    for s in DRIFT_SETTINGS:
        node = layer_node(rc, s.section)
        if node is None or (s.current not in node and s.legacy not in node):
            continue
        ran, saved = read_setting([dc], s), read_setting([rc], s)
        if value(ran) != value(saved):
            name = (saved or ran).key if (saved or ran) else s.current
            rows.append((name, describe(ran, s), describe(saved, s), s.current in LIVE_RESIZE))
    return rows


# Measurement envelope variants seen in circuit_profile.json. Any type missing
# from this map yields None, which a caller that sums silently reports as 0 --
# a wrong answer that looks like a healthy one. Keep this list exhaustive:
# verify with `--section types` against a new bundle before trusting a total.
NUMERIC_TYPES = ("count", "int", "float", "bytes")


def numeric(value, field=None):
    """Unwrap a circuit-profile measurement into a float, or None.

    `field` selects a sub-measurement for composite types: "cachecounts"
    carries {count, bytes, elapsed}, and the *_stats types carry
    min/max/avg/total record counts.
    """
    if not isinstance(value, dict):
        return None
    t = value.get("type")
    v = value.get("value")
    if t in NUMERIC_TYPES:
        return float(v) if isinstance(v, (int, float)) else None
    if t == "percent" and isinstance(v, dict):
        den = v.get("denominator") or 0
        return 100.0 * v.get("numerator", 0) / den if den else None
    if t == "duration" and isinstance(v, dict):
        return v.get("secs", 0) + v.get("nanos", 0) / 1e9
    if t == "bool":
        return 1.0 if v else 0.0
    if t == "cachecounts" and isinstance(v, dict):
        return numeric(v.get(field or "count")) if isinstance(v.get(field or "count"), dict) \
            else float(v.get(field or "count", 0) or 0)
    if t == "string":
        return None
    # composite stats block: no "type" key, sub-keys are themselves measurements
    if t is None:
        sub = v if isinstance(v, dict) else value
        if isinstance(sub, dict):
            key = field or "total_records_count"
            if key in sub:
                return numeric(sub[key])
    return None


def value_types(prof):
    """-> Counter of measurement type strings, to audit NUMERIC_TYPES coverage."""
    seen = collections.Counter()
    for w in prof.get("worker_profiles", []):
        for _node, ms in (w.get("metadata") or {}).items():
            for m in ms:
                v = m.get("value")
                if isinstance(v, dict):
                    seen[v.get("type")] += 1
                elif isinstance(v, list):
                    seen["LIST"] += 1
                else:
                    seen[type(v).__name__] += 1
    return seen


# --------------------------------------------------------------------------
# section: state -- terminal status, exit code, config that bounds everything
# --------------------------------------------------------------------------
def section_state(root):
    print("== pipeline state and budget ==")
    snaps = snapshots(root)
    if not snaps:
        print("  no collection directories found")
        return
    cfg = load_json(os.path.join(snaps[-1], "pipeline_config.json"))
    if not cfg:
        print("  no pipeline_config.json in newest snapshot")
        return

    print(f"  name              {cfg.get('name')}")
    print(f"  platform          {cfg.get('platform_version')}")
    print(f"  runtime status    {cfg.get('deployment_runtime_status')} "
          f"since {cfg.get('deployment_runtime_status_since')}")
    if cfg.get("deployment_error"):
        print(f"  deployment_error  {json.dumps(cfg['deployment_error'])[:300]}")
    det = cfg.get("deployment_runtime_status_details")
    if det:
        print(f"  status details    {json.dumps(det)[:300]}")

    layers = config_layers(cfg)
    mem_max = read_setting(layers, MEMORY_MAX)
    print("  -- budget (denominators for every memory rule below) --")
    print(f"  {'source':<20} "
          + ("deployment_config (the most recent run)"
             if isinstance(cfg.get("deployment_config"), dict)
             else "runtime_config (never started)"))
    print(f"  {'workers':<20} {lookup(layers, None, 'workers')}"
          f"   io_workers {lookup(layers, None, 'io_workers')}")
    # each key as this pipeline spells it, value as written (what Feldera reads)
    for s in (MEMORY_MAX, MAX_RSS, DATAFUSION_MEMORY, STORAGE_MAX):
        print(f"  {size_key(cfg, s):<20} {describe(read_setting(layers, s), s)}")
    print(f"  {'storage_class':<20} {lookup(layers, 'resources', 'storage_class')}")
    ci = read_setting(layers, CHECKPOINT_INTERVAL)
    print(f"  {'fault_tolerance':<20} {lookup(layers, 'fault_tolerance', 'model') or '-'}, "
          + (f"{ci.key} {describe(ci, CHECKPOINT_INTERVAL)}" if ci
             else "no checkpoint interval set"))
    print(f"  {'env':<20} {lookup(layers, None, 'env')}")

    drift = config_drift(cfg)
    if drift:
        print("  -- runtime_config differs from what ran --")
        print(f"  {'setting':<20} {'ran':<24} {'saved':<24} why")
        for name, ran, saved, live in drift:
            print(f"  {name:<20} {ran:<24} {saved:<24} "
                  + ("live resize" if live else "edit not run yet"))
        if any(live for *_, live in drift):
            print("  live resize: kept until storage is cleared or CPU or memory is edited")
        if not all(live for *_, live in drift):
            print("  edit not run yet: saved while stopped, applies at the next start")

    if read_setting(layers, MAX_RSS) is None and mem_max and mem_max.value:
        print(f"  NOTE {size_key(cfg, MAX_RSS)} unset; controller derives its budget from"
              f" the pod limit, often leaving little headroom under {fmt_bytes(mem_max.value)}")

    # container restarts and OOM live only here, never in logs.txt
    blob = json.dumps(cfg.get("deployment_resources_status_details"))
    for key in ("restartCount", "exitCode", "reason", "finishedAt"):
        m = re.search(r'"%s":\s*("?[^,"}]*"?)' % key, blob)
        if m:
            print(f"  {key:<17} {m.group(1)}")
    if '"OOMKilled"' in blob:
        print("  >> OOMKilled present: treat memory as a primary suspect")

    ckpt = cfg.get("storage_status_details") or {}
    if isinstance(ckpt, dict) and ckpt.get("checkpoints") == []:
        print("  >> no checkpoints recorded: a restart replays the full snapshot")

    # per-snapshot restartCount pins when the first crash happened
    print("  -- restartCount by snapshot --")
    for s in snaps:
        c = load_json(os.path.join(s, "pipeline_config.json"))
        if not c:
            continue
        b = json.dumps(c.get("deployment_resources_status_details"))
        m = re.search(r'"restartCount":\s*(\d+)', b)
        print(f"    {os.path.basename(s):<34} {m.group(1) if m else '?'}")


# --------------------------------------------------------------------------
# section: errors -- what failed, and whether retries are deterministic
# --------------------------------------------------------------------------
def section_errors(root, top):
    print("== errors and retry behaviour ==")
    for s in snapshots(root):
        log = os.path.join(s, "logs.txt")
        if not os.path.exists(log):
            continue
        with open(log, errors="replace") as f:
            raw = f.readlines()
        lines = [ANSI.sub("", ln).rstrip("\n") for ln in raw]
        # bundle logs frequently repeat every line; count unique
        uniq = set(lines)
        bad = [ln for ln in uniq if re.search(r"\bERROR\b|\bWARN\b.*error", ln)]
        dup = len(lines) / max(len(uniq), 1)
        print(f"  {os.path.basename(s)}  lines={len(lines)} unique={len(uniq)}"
              f" dup_factor={dup:.1f} error_lines={len(bad)}")
        # distinct error shapes, with volatile ids folded away
        shapes = collections.Counter()
        for ln in bad:
            core = re.sub(r"[0-9a-f]{8}-[0-9a-f-]{27}", "<uuid>", ln)
            core = re.sub(r"\d+", "<n>", core)
            shapes[core[-200:]] += 1
        for shape, n in shapes.most_common(top):
            print(f"     [{n}] {shape.strip()[:190]}")
        # retry cadence: a cap that never resets means a permanent failure
        delays = re.findall(r"after (\d+) attempts.*?retrying in ([0-9.]+)(m?s)", "\n".join(uniq))
        if delays:
            att = [int(a) for a, _, _ in delays]
            print(f"     retry attempts seen: max={max(att)} distinct={len(set(att))}"
                  f" -> {'saturated, deterministic failure' if max(att) > 8 else 'still backing off'}")


# --------------------------------------------------------------------------
# section: trend -- each snapshot, oldest first (oldest is not always healthy)
# --------------------------------------------------------------------------
def section_trend(root):
    print("== trend across snapshots (oldest first) ==")
    hdr = f"  {'snapshot':<34} {'rss':>9} {'storage':>9} {'pressure':>9} {'in':>14} {'proc':>14}"
    print(hdr)
    prev = None
    for s in snapshots(root):
        st = load_json(os.path.join(s, "stats.json"))
        if not st:
            print(f"  {os.path.basename(s):<34} {'(no stats.json: pipeline already down)':>60}")
            continue
        g = st.get("global_metrics", st)
        rss = g.get("rss_bytes", 0)
        row = (f"  {os.path.basename(s):<34} {fmt_bytes(rss):>9} "
               f"{fmt_bytes(g.get('storage_bytes',0)):>9} "
               f"{str(g.get('memory_pressure')):>9} "
               f"{g.get('total_input_records',0):>14,} "
               f"{g.get('total_processed_records',0):>14,}")
        if prev:
            d = rss - prev
            row += f"   drss={fmt_bytes(d)}"
        print(row)
        prev = rss
        lag = g.get("total_input_records", 0) - g.get("total_processed_records", 0)
        if g.get("total_input_records"):
            print(f"  {'':34} ingest lag {lag:,} records "
                  f"({100.0*lag/g['total_input_records']:.2f}% of input)")


# --------------------------------------------------------------------------
# circuit profile helpers
# --------------------------------------------------------------------------
def newest_profile(root):
    for s in reversed(snapshots(root)):
        prof = load_json(os.path.join(s, "circuit_profile.json"))
        if prof:
            return s, prof
    return None, None


def simple_nodes(prof):
    """-> {node id: dbsp operator label} for leaf nodes only.

    The graph is a cluster tree. Cluster nodes and the root ("n") carry the sum
    of their children, so including them in a per-node ranking double-counts
    and puts the root at a meaningless 50% of every total.
    """
    out = {}

    def walk(entry):
        if not isinstance(entry, dict):
            return
        # tagged variants
        if "Simple" in entry:
            s = entry["Simple"] or {}
            if s.get("id"):
                out[s["id"]] = s.get("label", "")
            return
        if "Cluster" in entry:
            walk(entry["Cluster"] or {})
            return
        # bare cluster object: {id, label, nodes: [...]}; its own id is an
        # aggregate, so record only its descendants
        for child in entry.get("nodes", []) or []:
            walk(child)

    # graph["nodes"] is the root cluster object, not a list of children
    walk((prof.get("graph") or {}).get("nodes") or {})
    return out


def node_metric(prof, metric_id):
    """-> {node: [per-worker values]} for one metric, numeric only."""
    leaves = simple_nodes(prof)
    out = collections.defaultdict(list)
    for w in prof.get("worker_profiles", []):
        for node, ms in (w.get("metadata") or {}).items():
            if leaves and node not in leaves:
                continue  # skip root / cluster aggregates
            for m in ms:
                if m.get("metric_id") != metric_id:
                    continue
                v = numeric(m.get("value"))
                if v is not None:
                    out[node].append(v)
    return out


class MirIndex:
    """Maps a circuit-profile persistent_id hash to its SQL provenance.

    Chain: circuit_profile node -> persistent_id "<hash>[.role]"
        -> dataflow_graph.json mir node -> table / view / operation / SQL line.
    """

    def __init__(self, root):
        self.by_hash = {}
        self.sql_lines = []
        snaps = snapshots(root)
        for s in reversed(snaps):
            dg = load_json(os.path.join(s, "dataflow_graph.json"))
            if dg and isinstance(dg.get("mir"), dict):
                for mid, node in dg["mir"].items():
                    h = node.get("persistent_id")
                    if h:
                        self.by_hash[h] = (mid, node)
                break
        for s in reversed(snaps):
            cfg = load_json(os.path.join(s, "pipeline_config.json"))
            if cfg and cfg.get("program_code"):
                self.sql_lines = cfg["program_code"].splitlines()
                break

    def resolve(self, persistent_id):
        """-> (label, mir_id, node) with the role suffix stripped off the hash."""
        if not persistent_id:
            return None, None, None
        h = persistent_id.split(".", 1)[0]
        role = persistent_id.split(".", 1)[1] if "." in persistent_id else ""
        hit = self.by_hash.get(h)
        if not hit:
            return (f"{h[:12]}..{('.' + role) if role else ''}", None, None)
        mid, node = hit
        what = node.get("table") or node.get("view") or "-"
        op = node.get("operation") or "?"
        pos = (node.get("positions") or [{}])[0]
        line = pos.get("start_line_number")
        label = f"{what} [{op}]" + (f" sql:{line}" if line else "")
        if role:
            label += f" .{role}"
        return label, mid, node

    def sql_at(self, line, context=0):
        if not (self.sql_lines and line):
            return []
        lo = max(0, line - 1 - context)
        hi = min(len(self.sql_lines), line + context)
        return [(i + 1, self.sql_lines[i]) for i in range(lo, hi)]


def node_labels(prof, mir=None):
    """-> {circuit node id: human label}, resolved through the MIR when available."""
    raw = {}
    for w in prof.get("worker_profiles", []):
        for node, ms in (w.get("metadata") or {}).items():
            if node in raw:
                continue
            for key in ("persistent_id", "mir_node"):
                for m in ms:
                    if m.get("metric_id") != key:
                        continue
                    v = m.get("value", {})
                    if isinstance(v, dict) and isinstance(v.get("value"), str):
                        raw[node] = v["value"]
                        break
                if node in raw:
                    break
    dbsp = simple_nodes(prof)
    if not mir:
        return {n: raw.get(n) or dbsp.get(n, "") for n in set(raw) | set(dbsp)}
    out = {}
    for n in set(raw) | set(dbsp):
        pid = raw.get(n)
        label = mir.resolve(pid)[0] if pid else None
        out[n] = label or dbsp.get(n, "") or (pid or "")
    return out


def catalog(prof):
    return {m["name"]: m for m in prof.get("metrics", []) if "name" in m}


# --------------------------------------------------------------------------
# section: time -- where wall clock actually goes
# --------------------------------------------------------------------------
def section_time(root, top):
    snap, prof = newest_profile(root)
    if not prof:
        print("== time ==\n  no circuit_profile.json[.gz] in any snapshot")
        return
    print(f"== time: hot operators (from {os.path.basename(snap)}) ==")
    lab = node_labels(prof, MirIndex(root))
    rt = node_metric(prof, "runtime_seconds")
    total = sum(sum(v) for v in rt.values()) or 1.0
    ranked = sorted(rt.items(), key=lambda kv: -sum(kv[1]))
    print(f"  total operator runtime {total:.1f}s across {len(rt)} nodes")
    print(f"  {'node':<10} {'share':>7} {'sum_s':>10} {'max_worker_s':>13}  label")
    for node, vals in ranked[:top]:
        print(f"  {node:<10} {100*sum(vals)/total:>6.2f}% {sum(vals):>10.2f} "
              f"{max(vals):>13.2f}  {lab.get(node,'')[:70]}")

    for mid in ("circuit_wait_time_seconds", "circuit_idle_time_seconds",
                "circuit_runtime_seconds", "exchange_wait_time_seconds",
                "merge_backpressure_wait_time_seconds"):
        vals = [v for vs in node_metric(prof, mid).values() for v in vs]
        if vals:
            print(f"  {mid:<40} sum={sum(vals):>12.1f}s max={max(vals):>10.1f}s")


# --------------------------------------------------------------------------
# section: skew -- self-normalizing, needs no threshold
# --------------------------------------------------------------------------
def section_skew(root, top):
    snap, prof = newest_profile(root)
    if not prof:
        print("== skew ==\n  no circuit_profile.json[.gz] in any snapshot")
        return
    nworkers = len(prof.get("worker_profiles", []))
    print(f"== skew across {nworkers} workers (from {os.path.basename(snap)}) ==")
    print("  ratio = max/mean. 1.0 is perfect balance; N means one worker does N x its share.")
    lab = node_labels(prof, MirIndex(root))

    # 1. key_distribution is an explicit per-worker histogram
    print("  -- key_distribution (balancer) --")
    rows = []
    leaves = simple_nodes(prof)
    for w in prof.get("worker_profiles", []):
        for node, ms in (w.get("metadata") or {}).items():
            if leaves and node not in leaves:
                continue
            for m in ms:
                if m.get("metric_id") != "key_distribution":
                    continue
                dist = [numeric(x) or 0 for x in m.get("value", [])]
                tot = sum(dist)
                if tot <= 0:
                    continue
                mean = tot / len(dist)
                rows.append((max(dist) - mean, max(dist) / mean if mean else 0,
                             tot, node, len(dist)))
    if rows:
        seen = set()
        # excess keys on the hottest worker is the cost; ratio alone over-ranks tiny nodes
        for excess, ratio, tot, node, buckets in sorted(rows, reverse=True):
            if node in seen:
                continue
            seen.add(node)
            print(f"  {node:<10} excess={excess:>15,.0f} ratio={ratio:>6.2f} "
                  f"keys={tot:>15,.0f}  {lab.get(node,'')[:60]}")
            if len(seen) >= top:
                break
    else:
        print("     all key_distribution histograms are empty (no data flowed yet)")

    # 2. per-worker dispersion, ranked by absolute excess rather than ratio.
    #    A ratio alone is meaningless at low volume: one record on one worker
    #    yields ratio == worker_count while costing nothing. Excess (max - mean)
    #    is in the metric's own units, so it self-weights and needs no floor.
    for mid in ("runtime_seconds", "state_records_count", "used_memory_bytes",
                "local_shard_records_count"):
        per = node_metric(prof, mid)
        rows = []
        grand = sum(sum(v) for v in per.values()) or 1.0
        for node, vals in per.items():
            if len(vals) < 2:
                continue
            tot = sum(vals)
            if tot <= 0:
                continue
            mean = tot / len(vals)
            rows.append((max(vals) - mean, max(vals) / mean if mean else 0, tot, node))
        if not rows:
            continue
        rows.sort(reverse=True)
        print(f"  -- {mid} imbalance (ranked by excess = max - mean) --")
        for excess, ratio, tot, node in rows[:top]:
            print(f"  {node:<10} excess={excess:>16,.1f} ratio={ratio:>6.2f} "
                  f"total={tot:>18,.0f} ({100*tot/grand:>5.2f}% of all)  {lab.get(node,'')[:60]}")

    for mid in ("rebalancings_count", "total_rebalancing_time_seconds",
                "accumulator_records_to_repartition_count"):
        vals = [v for vs in node_metric(prof, mid).values() for v in vs]
        if vals and sum(vals):
            print(f"  {mid:<44} sum={sum(vals):>16,.0f} max={max(vals):>14,.0f}")


# --------------------------------------------------------------------------
# section: sql -- amplification and wasted work
# --------------------------------------------------------------------------
def section_sql(root, top):
    snap, prof = newest_profile(root)
    if not prof:
        print("== sql ==\n  no circuit_profile.json[.gz] in any snapshot")
        return
    print(f"== sql efficiency (from {os.path.basename(snap)}) ==")
    _, mpath = newest_metrics(root)
    if mpath:
        g = prom_gauges(mpath, ["transaction_state", "dbsp_steps_total"])
        if g.get("transaction_state") and (g.get("dbsp_steps_total", 0) or 0) <= 1:
            print("  NOTE profile captured with a transaction still open and <=1 step done.")
            print("       Join/output metrics are zero because that work happens at commit.")
            print("       Re-collect a bundle mid-commit to size joins.")
    mir = MirIndex(root)
    lab = node_labels(prof, mir)

    def total(mid):
        return {k: sum(v) for k, v in node_metric(prof, mid).items()}

    out = total("computed_output_records_count")
    left = total("left_input_records_count")
    right = total("right_input_integral_records_count")

    print("  -- join amplification: output / (left + right) --")
    rows = []
    for node, o in out.items():
        denom = left.get(node, 0) + right.get(node, 0)
        if denom <= 0 or o <= 0:
            continue
        rows.append((o / denom, o, denom, node))
    for amp, o, denom, node in sorted(rows, reverse=True)[:top]:
        print(f"  {node:<10} amp={amp:>9.2f} out={o:>15,.0f} in={denom:>15,.0f}"
              f"  {lab.get(node,'')[:70]}")
    if not rows:
        print("     no join operators produced output in this profile")

    print("  -- output_redundancy_percent (work discarded downstream) --")
    red = node_metric(prof, "output_redundancy_percent")
    rows = [(max(v), node) for node, v in red.items() if v and max(v) > 0]
    for pct, node in sorted(rows, reverse=True)[:top]:
        print(f"  {node:<10} {pct:>6.2f}%  {lab.get(node,'')[:70]}")
    if not rows:
        print("     none reported above zero")

    print("  -- negative_weight_count (retraction churn) --")
    neg = total("negative_weight_count")
    rows = [(v, node) for node, v in neg.items() if v > 0]
    for v, node in sorted(rows, reverse=True)[:top]:
        print(f"  {node:<10} {v:>18,.0f}  {lab.get(node,'')[:70]}")
    if not rows:
        print("     none reported above zero")

    # unbounded state: operators holding records with no retainment bound
    st = total("state_records_count")
    bounded = set()
    for w in prof.get("worker_profiles", []):
        for node, ms in (w.get("metadata") or {}).items():
            for m in ms:
                if m.get("metric_id") == "retainment_bounds":
                    v = m.get("value")
                    if v not in (None, [], {}):
                        bounded.add(node)
    unb = [(v, n) for n, v in st.items() if v > 0 and n not in bounded]
    print("  -- largest state with no retainment bound (candidates for LATENESS / windowing) --")
    for v, node in sorted(unb, reverse=True)[:top]:
        print(f"  {node:<10} {v:>18,.0f} records  {lab.get(node,'')[:70]}")


# --------------------------------------------------------------------------
# section: memory -- always relative to the pod budget
# --------------------------------------------------------------------------
BLOOM_BITS_PER_KEY_PER_DECADE = 4.8
DEFAULT_BLOOM_FALSE_POSITIVE_RATE = 0.0001


def bloom_bits_per_key(false_positive_rate):
    """Bits per key a rate implies, or 0 when the rate disables filters.

    Feldera's docs tabulate 4.8 bits per decade, so the 1e-4 default costs
    19.2 bits per key.
    """
    if not 0.0 < false_positive_rate < 1.0:
        return 0.0
    return BLOOM_BITS_PER_KEY_PER_DECADE * math.log10(1.0 / false_positive_rate)


def find_config_value(doc, key):
    """Depth-first search; bloom_false_positive_rate has no fixed home."""
    if isinstance(doc, dict):
        if isinstance(doc.get(key), (int, float)) and not isinstance(doc.get(key), bool):
            return doc[key]
        for v in doc.values():
            found = find_config_value(v, key)
            if found is not None:
                return found
    elif isinstance(doc, list):
        for v in doc:
            found = find_config_value(v, key)
            if found is not None:
                return found
    return None


def section_memory(root, top):
    snap, prof = newest_profile(root)
    cfg_snap, cfg = snapshot_config(root, snap)
    layers = config_layers(cfg)
    mem, disk = read_setting(layers, MEMORY_MAX), read_setting(layers, STORAGE_MAX)
    budget = mem.value if mem else None
    print("== memory, as a share of the pipeline's own budget ==")
    if snap and cfg_snap != snap:
        print(f"  NOTE {os.path.basename(snap)} has no pipeline_config.json; budget is from"
              f" {os.path.basename(cfg_snap) if cfg_snap else 'nowhere'}, maybe another run")
    print(f"  budget {mem.key} = {describe(mem, MEMORY_MAX)}" if budget
          else "  budget unknown; shares cannot be computed"
          + (f": {mem.key} {describe(mem, MEMORY_MAX)}" if mem else ""))
    if not prof:
        print("  no circuit_profile.json[.gz] in any snapshot")
        return

    store = disk.value if disk else None
    print(f"  budget {disk.key} = {describe(disk, STORAGE_MAX)}" if store
          else "  storage budget unknown"
          + (f": {disk.key} {describe(disk, STORAGE_MAX)}" if disk else ""))

    def share(n, against="mem"):
        b = budget if against == "mem" else store
        if not b:
            return "n/a"
        return f"{100.0*n/b:>7.2f}% of {'memory' if against=='mem' else 'storage'}"

    def total_of(mid):
        return sum(v for vs in node_metric(prof, mid).values() for v in vs)

    # Filters live in RAM, so they belong against the memory budget. Summing
    # only bloom bytes under-reports the real cost.
    print("  -- in RAM (compare against memory budget) --")
    fam_total = 0
    for mid in ("bloom_filter_size_bytes", "range_filter_size_bytes",
                "roaring_filter_size_bytes"):
        t = total_of(mid)
        fam_total += t
        print(f"  {mid:<28} {fmt_bytes(t):>10}  {share(t)}")
    print(f"  {'all filter families':<28} {fmt_bytes(fam_total):>10}  {share(fam_total)}")
    for mid in ("used_memory_bytes", "allocated_memory_bytes", "shared_memory_bytes"):
        t = total_of(mid)
        if t:
            print(f"  {mid:<28} {fmt_bytes(t):>10}  {share(t)}")

    # These are on-disk sizes; charging them to the memory budget would
    # manufacture a fake >100% reading.
    print("  -- on disk (compare against storage budget) --")
    for mid in ("spine_storage_size_bytes", "merging_size_bytes"):
        t = total_of(mid)
        if t:
            print(f"  {mid:<28} {fmt_bytes(t):>10}  {share(t, 'disk')}")

    used = {k: sum(v) for k, v in node_metric(prof, "used_memory_bytes").items()}
    lab = node_labels(prof, MirIndex(root))
    print("  -- heaviest operators by in-memory state --")
    for v, node in sorted(((v, k) for k, v in used.items()), reverse=True)[:top]:
        print(f"  {node:<10} {fmt_bytes(v):>10}  {share(v):<20} {lab.get(node,'')[:70]}")

    # Cost/benefit per filter family. A fixed byte threshold cannot see that a
    # filter is large AND useless; size divided by hit rate can.
    print("  -- filter cost vs benefit (is the RAM earning its keep?) --")
    for fam in ("bloom", "range", "roaring"):
        size = total_of(f"{fam}_filter_size_bytes")
        hits = total_of(f"{fam}_filter_hits_count")
        misses = total_of(f"{fam}_filter_misses_count")
        looks = hits + misses
        if not size and not looks:
            continue
        rate = 100.0 * hits / looks if looks else 0.0
        per_hit = size / hits if hits else float("inf")
        verdict = ""
        if size and rate < 1.0:
            verdict = "  <= large and almost never hits: candidate to shrink or disable"
        print(f"  {fam:<9} size={fmt_bytes(size):>9} {share(size)}  hit_rate={rate:>6.2f}%"
              f"  bytes/hit={'inf' if per_hit == float('inf') else fmt_bytes(per_hit)}{verdict}")

    # Filter sizing: are the filters bigger than the configured rate implies?
    rate = None
    for doc in layers + [cfg]:
        rate = find_config_value(doc, "bloom_false_positive_rate")
        if rate is not None:
            break
    rate_is_default = rate is None
    if rate is None:
        rate = DEFAULT_BLOOM_FALSE_POSITIVE_RATE
    configured_bits = bloom_bits_per_key(rate)

    # Weight the reported bits/key by each reading's own filter size: an
    # unweighted mean lets many tiny idle filters outvote the filter that
    # actually holds the memory.
    bits_by_node = node_metric(prof, "bloom_filter_bits_per_key")
    size_by_node = node_metric(prof, "bloom_filter_size_bytes")
    weighted = weight = 0.0
    for node, bits in bits_by_node.items():
        sizes = size_by_node.get(node, [])
        for i, b in enumerate(bits):
            w = sizes[i] if i < len(sizes) else 0
            if b > 0 and w > 0:
                weighted += b * w
                weight += w
    print("  -- filter sizing --")
    print(f"  bloom_false_positive_rate   {rate:g}"
          f"{' (default: not set by the pipeline)' if rate_is_default else ''}")
    print(f"  configured bits/key         {configured_bits:.2f}")
    if weight > 0 and configured_bits > 0:
        reported = weighted / weight
        overhead = reported / configured_bits
        print(f"  reported bits/key           {reported:.2f} (size weighted)")
        print(f"  overhead                    {overhead:.3f}")
        if overhead >= 1.5:
            print("     >> filters cost far more per key than the rate implies. fastbloom")
            print("        floors every filter at 512 bits, so batches too small to need one")
            print(f"        still pay. Raise {size_key(cfg, MIN_SPILL_SIZE)};"
                  " relaxing the rate will not help.")
        else:
            print("        Filters are sized as configured; their cost is key volume, not")
            print("        per-key overhead. Relaxing the rate is the lever that works here.")
        for target in (0.001, 0.01):
            target_bits = bloom_bits_per_key(target)
            if configured_bits > 0 and target_bits > 0:
                projected = fam_total * (target_bits / configured_bits)
                print(f"  at rate {target:g}: filters would be ~{fmt_bytes(projected)}"
                      f"  (saves {fmt_bytes(fam_total - projected)})")
    else:
        print("  reported bits/key           not reported by any operator")

    # memory vs storage residency: how much state actually spilled
    lm = sum(v for vs in node_metric(prof, "loose_memory_records_count").values() for v in vs)
    ls = sum(v for vs in node_metric(prof, "loose_storage_records_count").values() for v in vs)
    if lm + ls:
        print(f"  loose records: memory {lm:,.0f} vs storage {ls:,.0f}"
              f"  ({100.0*lm/(lm+ls):.1f}% resident)")


# --------------------------------------------------------------------------
# section: disk -- histogram percentiles, cache hit rates, stalls
# --------------------------------------------------------------------------
def parse_prom_histograms(path):
    """-> {family: {le: cumulative_count}} plus {family: (count, sum)}."""
    buckets = collections.defaultdict(dict)
    totals = {}
    with open(path, errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            m = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)$", line)
            if not m:
                continue
            name, labels, val = m.group(1), m.group(2) or "", m.group(3)
            try:
                v = float(val)
            except ValueError:
                continue
            if name.endswith("_bucket"):
                le = re.search(r'le="([^"]*)"', labels)
                if le:
                    key = float("inf") if le.group(1) in ("inf", "+Inf") else float(le.group(1))
                    fam = name[: -len("_bucket")]
                    buckets[fam][key] = buckets[fam].get(key, 0) + v
            elif name.endswith("_count"):
                fam = name[: -len("_count")]
                c, s = totals.get(fam, (0.0, 0.0))
                totals[fam] = (c + v, s)
            elif name.endswith("_sum"):
                fam = name[: -len("_sum")]
                c, s = totals.get(fam, (0.0, 0.0))
                totals[fam] = (c, s + v)
    return buckets, totals


def quantile_bounds(bkt, q):
    """Bucket containing quantile q -> (lower, upper). Histograms give bounds, not points."""
    if not bkt:
        return None
    items = sorted(bkt.items())
    total = items[-1][1]
    if total <= 0:
        return None
    target = q * total
    prev = 0.0
    for le, cum in items:
        if cum >= target:
            return (prev, le)
        prev = le
    return (prev, float("inf"))


def prom_gauges(path, names):
    """-> {name: float} first sample per family, ignoring labels."""
    out = {}
    with open(path, errors="replace") as f:
        text = f.read()
    for n in names:
        m = re.search(r"^%s(?:\{[^}]*\})?\s+(\S+)$" % re.escape(n), text, re.M)
        if m:
            try:
                out[n] = float(m.group(1))
            except ValueError:
                pass
    return out


def newest_metrics(root):
    for s in reversed(snapshots(root)):
        p = os.path.join(s, "metrics.txt")
        if os.path.exists(p):
            return s, p
    return None, None


def section_heap(root, top):
    """Live-heap attribution from heap_profile.pb.gz (jemalloc pprof).

    Independent of circuit_profile.json, so it corroborates or contradicts the
    per-operator State metrics. Coverage against RSS says how much of the
    process the profile actually explains.
    """
    print("== heap: live allocation attribution ==")
    path = None
    for s in reversed(snapshots(root)):
        for name in ("heap_profile.pb.gz", "heap_profile.pb"):
            cand = os.path.join(s, name)
            if os.path.exists(cand):
                path = cand
                break
        if path:
            break
    if not path:
        print("  no heap_profile.pb.gz in any snapshot")
        print("  (Linux-only, needs the pipeline built with heap profiling enabled)")
        return
    try:
        from pprof_min import parse_pprof, total, by_alloc_site, by_operator_hash
    except ImportError:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from pprof_min import parse_pprof, total, by_alloc_site, by_operator_hash

    snap = os.path.basename(os.path.dirname(path))
    print(f"  from {snap}/{os.path.basename(path)}")
    types, samples = parse_pprof(path)
    if not types:
        print("  profile carries no sample types")
        return
    print(f"  sample types: {', '.join(f'{t} ({u})' for t, u in types)}   samples: {len(samples)}")
    idx = 0
    tot = total(samples, idx)
    tname, tunit = types[idx]
    print(f"  {tname} total       {fmt_bytes(tot)}")

    # Coverage: what share of the process the profile explains.
    st = load_json(os.path.join(os.path.dirname(path), "stats.json"))
    rss = None
    if st:
        rss = (st.get("global_metrics", st) or {}).get("rss_bytes")
    if rss:
        print(f"  RSS at collection      {fmt_bytes(rss)}   profile covers "
              f"{100.0*tot/rss:.1f}% of it")
        if tot / rss < 0.6:
            print("     >> profile explains well under two thirds of RSS: much of the")
            print("        footprint is outside the tracked allocator (mmap, page cache,")
            print("        fragmentation). Do not size the pipeline from this profile alone.")

    print(f"  -- allocation sites, exclusive, shim skipped (top {top}) --")
    sites = by_alloc_site(samples, idx)
    ranked = sorted(sites.items(), key=lambda kv: -kv[1])
    shown = 0
    for site, v in ranked[:top]:
        shown += v
        print(f"  {fmt_bytes(v):>9} {100.0*v/tot:>6.2f}%  {site[:96]}")
    print(f"  {'':9} {100.0*shown/tot:>6.2f}%  of profiled bytes shown above")

    # Attribute to SQL through the operator hash embedded in generated symbols.
    mir = MirIndex(root)
    byh = by_operator_hash(samples, idx)
    if byh:
        print("  -- attributed to SQL via generated operator symbols --")
        hash_index = {h[:16]: v for h, v in
                      ((pid, node) for pid, node in mir.by_hash.items())}
        rows = []
        for h, v in byh.items():
            node = hash_index.get(h)
            if node:
                what = node[1].get("table") or node[1].get("view") or "-"
                op = node[1].get("operation") or "?"
                line = (node[1].get("positions") or [{}])[0].get("start_line_number")
                label = f"{what} [{op}]" + (f" sql:{line}" if line else "")
            else:
                label = f"{h} (no MIR match)"
            rows.append((v, label))
        for v, label in sorted(rows, reverse=True)[:top]:
            if v / tot < 0.0005:
                continue
            print(f"  {fmt_bytes(v):>9} {100.0*v/tot:>6.2f}%  {label[:80]}")

    # Cross-check against the circuit profile's own filter accounting.
    _snap, prof = newest_profile(root)
    if prof:
        filt = 0.0
        for mid in ("bloom_filter_size_bytes", "range_filter_size_bytes",
                    "roaring_filter_size_bytes"):
            filt += sum(v for vs in node_metric(prof, mid).values() for v in vs)
        heap_filter = sum(v for site, v in sites.items()
                          if re.search(r"[Bb]loom|fastbloom|filter", site))
        if filt or heap_filter:
            print("  -- cross-check: filters, two independent sources --")
            print(f"  circuit_profile filter metrics  {fmt_bytes(filt)}")
            print(f"  heap sites matching /filter/     {fmt_bytes(heap_filter)}")
            print("     Two sources agreeing raises confidence; disagreement means one")
            print("     of them is not measuring what you assume.")


def section_types(root):
    """Audit that every measurement type in this bundle is understood."""
    print("== measurement type coverage (guards against silent zeros) ==")
    snap, prof = newest_profile(root)
    if not prof:
        print("  no circuit_profile.json[.gz] in any snapshot")
        return
    known = set(NUMERIC_TYPES) | {"percent", "duration", "bool", "cachecounts", "string", None}
    for t, c in value_types(prof).most_common():
        mark = "ok " if t in known or t == "LIST" else "UNHANDLED"
        print(f"  {mark:<10} {str(t):<14} {c:>8}")
    print("  UNHANDLED types are dropped by numeric(); any sum over them under-reports.")


def section_pace(root):
    """Execution mode and cadence, derived from the pipeline's own history.

    An absolute per-step latency threshold is meaningless until you know the
    mode: in transaction mode the whole workload is one step, so a healthy
    looking mean step latency can hide a very long transaction.
    """
    print("== pace: mode, cadence, and what a 'slow' step would mean here ==")
    snap, path = newest_metrics(root)
    if not path:
        print("  no metrics.txt in any snapshot")
        return
    print(f"  from {os.path.basename(snap)}/metrics.txt")
    g = prom_gauges(path, [
        "dbsp_steps_total", "dbsp_runtime_elapsed_seconds_total",
        "transaction_state", "transaction_completed_operators",
        "transaction_in_progress_operators", "transaction_remaining_operators",
        "pipeline_start_time_seconds", "records_processed_total",
        "records_input_total", "records_late_total",
    ])
    bkt, tot = parse_prom_histograms(path)

    steps = g.get("dbsp_steps_total", 0)
    print(f"  dbsp_steps_total          {steps:,.0f}   (transaction-level steps)")

    # Wall clock and circuit step count must come from per-worker maxima.
    # dbsp_runtime_elapsed_seconds_total is summed across workers, so dividing
    # it by steps overstates wall clock by roughly the worker count.
    wall = circuit_steps = None
    _, prof = newest_profile(root)
    if prof:
        def worker_max(mid):
            vals = []
            for w in prof.get("worker_profiles", []):
                for _node, ms in (w.get("metadata") or {}).items():
                    for m in ms:
                        if m.get("metric_id") == mid:
                            v = numeric(m.get("value"))
                            if v is not None:
                                vals.append(v)
            return max(vals) if vals else None
        wall = worker_max("circuit_runtime_seconds")
        circuit_steps = worker_max("steps_count")
    if wall:
        print(f"  circuit wall clock        {wall:,.1f}s  (max circuit_runtime_seconds)")
    if circuit_steps:
        print(f"  circuit steps_count       {circuit_steps:,.0f}   (internal circuit steps)")
    if wall and circuit_steps:
        print(f"  seconds per circuit step  {wall/circuit_steps:,.3f}s "
              "(derived: wall clock / circuit steps)")
    if steps and circuit_steps and steps < circuit_steps:
        print(f"  NOTE {circuit_steps:,.0f} circuit steps map to {steps:,.0f} transaction step(s):"
              " results only become visible at commit")
    for fam in ("dbsp_step_latency_seconds", "transaction_ingest_seconds",
                "transaction_commit_seconds", "feldera_checkpoint_runtime_seconds",
                "feldera_checkpoint_delay_seconds"):
        c, s = tot.get(fam, (0.0, 0.0))
        if c:
            print(f"  {fam:<34} n={c:>10,.0f} mean={s/c:>12.3f}s total={s:>12.1f}s")

    txn = g.get("transaction_state")
    if txn:
        print(f"  transaction_state         {txn:.0f} (a transaction is open)")
    if steps and steps <= 10:
        print(f"  >> only {steps:,.0f} transaction step(s) recorded: this pipeline batches its")
        print( "     work into transactions, so dbsp_step_latency thresholds do NOT describe")
        print( "     it. Judge it by transaction ingest/commit duration and wall clock.")
    if g.get("records_late_total"):
        print(f"  records_late_total        {g['records_late_total']:,.0f}"
              " (arriving behind LATENESS: dropped from results)")
    # the interval of the run these metrics came from
    cfg_snap, cfg = snapshot_config(root, snap)
    if cfg_snap and cfg_snap != snap:
        print(f"  NOTE {os.path.basename(snap)} has no pipeline_config.json; interval is from"
              f" {os.path.basename(cfg_snap)}, maybe another run")
    found = read_setting(config_layers(cfg), CHECKPOINT_INTERVAL)
    ci = found.value if found else None  # seconds
    if ci:
        print(f"  {found.key:<25} {describe(found, CHECKPOINT_INTERVAL)}: "
              "the pipeline's own target cadence; compare step/commit time to this")
        c, s = tot.get("feldera_checkpoint_runtime_seconds", (0.0, 0.0))
        if c and s / c > ci:
            print(f"  >> mean checkpoint runtime ({s/c:,.1f}s) exceeds the interval ({ci:g}s):"
                  " checkpoints cannot keep their schedule")
        if wall and steps and wall > ci and steps <= 1:
            print(f"  >> the open transaction has run {wall:,.0f}s against a {ci:g}s checkpoint"
                  " interval: no checkpoint can complete until it commits")


def section_disk(root):
    print("== disk and cache ==")
    snaps = snapshots(root)
    path = None
    for s in reversed(snaps):
        p = os.path.join(s, "metrics.txt")
        if os.path.exists(p):
            path = p
            break
    if not path:
        print("  no metrics.txt in any snapshot")
    else:
        print(f"  from {os.path.basename(os.path.dirname(path))}/metrics.txt")
        bkt, tot = parse_prom_histograms(path)
        fams = [f for f in bkt if f.startswith("storage_") or "checkpoint" in f
                or f.startswith("dbsp_step_latency") or f.startswith("transaction_")]
        print(f"  {'family':<46} {'count':>12} {'mean':>12}   p50 / p99 bucket")
        for fam in sorted(fams):
            c, s = tot.get(fam, (0.0, 0.0))
            mean = s / c if c else 0.0
            p50 = quantile_bounds(bkt[fam], 0.50)
            p99 = quantile_bounds(bkt[fam], 0.99)

            def show(b):
                if not b:
                    return "n/a"
                lo, hi = b
                return f">{lo:g}" if hi == float("inf") else f"{lo:g}..{hi:g}"
            unit = "s" if fam.endswith("_seconds") else ""
            print(f"  {fam:<46} {c:>12,.0f} {mean:>11.4f}{unit or ' '}   "
                  f"{show(p50)} / {show(p99)}")

        # derived saturation: usage against the pipeline's own configured limit
        g = prom_gauges(path, ["storage_cache_usage_bytes",
                               "storage_cache_usage_limit_bytes_total",
                               "storage_usage_bytes", "process_resident_memory_bytes",
                               "max_rss_bytes"])
        cu, cl = g.get("storage_cache_usage_bytes"), g.get("storage_cache_usage_limit_bytes_total")
        if cu and cl:
            print(f"  cache saturation           {fmt_bytes(cu)} / {fmt_bytes(cl)} "
                  f"= {100.0*cu/cl:.1f}% full")
            if cu / cl > 0.95:
                print("     >> cache is effectively full: further reads must go to disk")
        rss, cap = g.get("process_resident_memory_bytes"), g.get("max_rss_bytes")
        if rss and cap:
            print(f"  rss headroom               {fmt_bytes(rss)} / {fmt_bytes(cap)} "
                  f"= {100.0*rss/cap:.1f}% of the controller's own budget")

        # gauges worth reading directly
        with open(path, errors="replace") as f:
            text = f.read()
        for name in ("compaction_stall_duration_seconds", "output_stall_seconds_total",
                     "storage_cache_usage_bytes", "storage_cache_usage_limit_bytes_total",
                     "storage_usage_bytes", "files_created_total", "files_deleted_total",
                     "memory_pressure", "memory_pressure_epoch", "max_rss_bytes",
                     "process_resident_memory_bytes", "process_swap_bytes",
                     "dbsp_steps_total", "records_late_total"):
            for m in re.finditer(r"^%s(?:\{[^}]*\})?\s+(\S+)$" % re.escape(name), text, re.M):
                print(f"  {name:<46} {m.group(1)}")
                break

    snap, prof = newest_profile(root)
    if prof:
        print("  -- cache hit rates (low rate + high read latency = disk-bound) --")
        for mid in ("background_cache_hit_rate_percent", "foreground_cache_hit_rate_percent",
                    "bloom_filter_hit_rate_percent", "range_filter_hit_rate_percent",
                    "roaring_filter_hit_rate_percent", "merge_reduction_percent"):
            vals = [v for vs in node_metric(prof, mid).values() for v in vs]
            if vals:
                print(f"  {mid:<46} mean={sum(vals)/len(vals):>7.2f}%  min={min(vals):>7.2f}%")


SECTIONS = {
    "state": lambda a: section_state(a.bundle),
    "errors": lambda a: section_errors(a.bundle, a.top),
    "trend": lambda a: section_trend(a.bundle),
    "time": lambda a: section_time(a.bundle, a.top),
    "skew": lambda a: section_skew(a.bundle, a.top),
    "sql": lambda a: section_sql(a.bundle, a.top),
    "memory": lambda a: section_memory(a.bundle, a.top),
    "heap": lambda a: section_heap(a.bundle, a.top),
    "types": lambda a: section_types(a.bundle),
    "pace": lambda a: section_pace(a.bundle),
    "disk": lambda a: section_disk(a.bundle),
}
ORDER = ["state", "errors", "trend", "pace", "time", "skew", "sql", "memory",
         "heap", "disk", "types"]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("bundle", help="extracted bundle directory")
    ap.add_argument("--section", default="all", choices=ORDER + ["all"])
    ap.add_argument("--top", type=int, default=10)
    a = ap.parse_args()
    if not os.path.isdir(a.bundle):
        sys.exit(f"not a directory: {a.bundle}")
    todo = ORDER if a.section == "all" else [a.section]
    for name in todo:
        SECTIONS[name](a)
        print()


if __name__ == "__main__":
    main()
