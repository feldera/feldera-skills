#!/usr/bin/env python3
"""Derive triage indicators from a Feldera support bundle.

Every number printed is a ratio, a share, a dispersion measure, or a
comparison against the pipeline's own configuration. No absolute
thresholds are baked in: the caller judges the values in context.

Usage:
    bundle_probe.py <extracted-bundle-dir> [--section SECTION[,SECTION...]]... [--top N]

Sections: state, sidecars, errors, inputs, trend, pace, time, skew, sql,
materialized, memory, heap, disk, types, all. Repeat --section or give a
comma list; the sections run in the order above.
"""
import argparse
import collections
import datetime
import functools
import glob
import gzip
import json
import math
import os
import re
import statistics
import sys
import traceback

ANSI = re.compile(r"\x1b\[[0-9;]*m")
# "2026-06-09T17-38-34.679715382Z", "2026-08-27T02:17:02"; not "<timestamp>_x.zip"
SNAPSHOT_NAME = re.compile(r"^\d{4}-\d{2}-\d{2}T[0-9:.+\-Z]+$")
# a hand-collected snapshot can carry a prefix: "last-snapshot-2026-10-29T18-31Z"
SNAPSHOT_STAMP = re.compile(r"\d{4}-\d{2}-\d{2}T[0-9:.+\-Z]+$")
SNAPSHOT_ARTIFACTS = ("pipeline_config.json", "metrics.txt", "stats.json", "circuit_profile.json")


@functools.lru_cache(maxsize=None)
def snapshots(root):
    """Collection directories, oldest first, as a tuple. Names are ISO timestamps.

    A bundle zipped again holds them one directory down, so a single
    wrapper directory is opened. A name that ends in a timestamp after a
    prefix counts when the directory holds a snapshot artifact, also next to
    ISO-named ones, so a log directory such as "coordinator-restart-<timestamp>"
    does not.
    """
    subdirs = [d for d in glob.glob(os.path.join(root, "*")) if os.path.isdir(d)]
    dirs = [d for d in subdirs if SNAPSHOT_NAME.match(os.path.basename(d))
            or (SNAPSHOT_STAMP.search(os.path.basename(d))
                and any(artifact_path(os.path.join(d, a)) for a in SNAPSHOT_ARTIFACTS))]
    wrappers = [d for d in subdirs if os.path.basename(d) != "__MACOSX"]
    if not dirs and len(wrappers) == 1:
        return snapshots(wrappers[0])
    return tuple(sorted(dirs, key=snapshot_time))


def snapshot_time(snap):
    """-> the time in a snapshot directory name, ISO with colons ("2026-06-09T17:38:34.68Z").

    Names write the time with dashes ("T17-38-34.68Z"); Kubernetes writes it
    with colons, and the two must sort together.
    """
    stamp = SNAPSHOT_STAMP.search(os.path.basename(snap)).group()
    date, _, time = stamp.partition("T")
    return f"{date}T" + re.sub(r"^(\d{2})-(\d{2})(?:-(\d{2}))?",
                               lambda m: ":".join(g for g in m.groups() if g), time)


def artifact_path(path):
    """-> path, or path + ".gz" when only the gzipped copy exists, or None."""
    for p in (path, path + ".gz"):
        if os.path.exists(p):
            return p
    return None


def load_json(path):
    # bundles may ship any JSON artifact gzipped (circuit_profile.json.gz)
    path = artifact_path(path)
    return read_json(os.path.abspath(path)) if path else None


@functools.lru_cache(maxsize=None)
def read_json(path):
    """Parsed once per run: every section reads the same profile and configs."""
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


def keep_end(name, width):
    """An endpoint name ("<table>.<connector>") cut to width from the left: the connector is at the end."""
    name = str(name)
    return name if len(name) <= width else "..." + name[-(width - 3):]


# "2026-11-10T22:34:35.215854Z" in a log line; the zone is optional
ISO_TIME = re.compile(r"(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(\.\d+)?(Z|[+-]\d{2}:?\d{2})?")


def iso_epoch(text):
    """-> epoch seconds of the first ISO time in text (UTC when it has no zone), or None."""
    m = ISO_TIME.search(str(text or ""))
    if not m:
        return None
    date, clock, frac, zone = m.groups()
    zone = "+00:00" if zone in (None, "Z") else zone if ":" in zone else zone[:3] + ":" + zone[3:]
    try:
        return datetime.datetime.fromisoformat(f"{date}T{clock}{zone}").timestamp() + float(frac or 0)
    except ValueError:
        return None


def epoch_iso(seconds):
    """-> "2026-11-10T22:34:59Z" for epoch seconds."""
    return datetime.datetime.fromtimestamp(seconds, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# key groups snapshots of one process; uuid and start can be None; uptime is in seconds
Incarnation = collections.namedtuple("Incarnation", "key uuid start uptime")


@functools.lru_cache(maxsize=None)
def incarnation(snap):
    """-> the Incarnation (pipeline process) that served snap, from stats.json, or None.

    A restart starts a new process: counters, histograms and the circuit
    profile start again at zero, so values from two incarnations do not compare.
    incarnation_uuid is in newer versions; start_time (epoch seconds) in all.
    """
    st = load_json(os.path.join(snap, "stats.json"))
    g = (st.get("global_metrics", st) if isinstance(st, dict) else None) or {}
    uuid = g.get("incarnation_uuid") if isinstance(g.get("incarnation_uuid"), str) else None
    start = g.get("start_time")
    start = start if isinstance(start, (int, float)) and not isinstance(start, bool) and start > 0 else None
    uptime = g.get("uptime_msecs")
    uptime = uptime / 1000 if isinstance(uptime, (int, float)) and not isinstance(uptime, bool) else None
    if not (uuid or start):
        return None
    return Incarnation(uuid or f"start {start}", uuid, start, uptime)


def incarnation_name(inc):
    """-> a short name: the uuid's time part (13 chars), else the start time."""
    if inc is None:
        return "unknown"
    return inc.uuid[:13] if inc.uuid else epoch_iso(inc.start)


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
    version = version_tuple(cfg)
    current = bool(version) and version >= SIZE_RENAME_VERSION
    return setting.current if current else setting.legacy


def version_tuple(cfg):
    """(major, minor, patch) of the config's platform_version, or None."""
    m = re.match(r"v?(\d+)\.(\d+)\.(\d+)", str((cfg or {}).get("platform_version") or ""))
    return tuple(map(int, m.groups())) if m else None


def snapshot_config(root, snap):
    """-> (snapshot, config): the config collected with snap, else the newest one.

    Budgets must come from the run that the profile or metrics describe; a
    later snapshot can hold an edited config or a different run. With no
    pipeline_config.json anywhere, a runtime config file stands in for it.
    """
    if snap:
        cfg = load_json(os.path.join(snap, "pipeline_config.json"))
        if cfg:
            return snap, cfg
    for s in reversed(snapshots(root)):
        cfg = load_json(os.path.join(s, "pipeline_config.json"))
        if cfg:
            return s, cfg
    # a hand-collected bundle: the snapshot's own directory, then the one above it
    for s in ([snap] if snap else []) + list(reversed(snapshots(root))):
        cfg = stand_in_config(s)
        if cfg:
            return s, cfg
    return None, None


CONFIG_FILE = "_config_file"  # the file a stand-in config came from; absent in pipeline_config.json
RUNTIME_CONFIG_NAMES = ("config.json", "runtime_config.json")


@functools.lru_cache(maxsize=None)
def stand_in_config(snap):
    """-> a pipeline_config.json-shaped dict built from snap's runtime config file, or None.

    The file is in snap or in the directory above it. The config runs as
    deployment_config. Its program_ir holds the MIR and the compiled schema
    of program_info, and its outputs are the output connectors. It has no
    status, no restarts and no SQL (program_code).
    """
    for d in (snap, os.path.dirname(snap)):
        path, runtime = runtime_config_file(d)
        if runtime:
            ir = runtime.get("program_ir") if isinstance(runtime.get("program_ir"), dict) else {}
            info = {"input_connectors": {},
                    "output_connectors": runtime.get("outputs") or {}}
            if isinstance(ir.get("program_schema"), dict):
                info["schema"] = ir["program_schema"]
            if isinstance(ir.get("mir"), dict) and ir["mir"]:
                info["dataflow"] = {"mir": ir["mir"]}
            return {"deployment_config": runtime, CONFIG_FILE: path, "program_info": info,
                    "name": runtime.get("given_name") or runtime.get("name")}
    return None


@functools.lru_cache(maxsize=None)
def runtime_config_file(directory):
    """-> (path, runtime config) of a hand-collected bundle, or (None, None).

    A bundle copied from Kubernetes can hold the pipeline's ConfigMap
    ("<name>.configmap.json"), whose data["config.json"] is the config the
    runner gave the pipeline, as a JSON string; or that config as a plain file.
    """
    for path in sorted(glob.glob(os.path.join(directory, "*.json"))):
        name = os.path.basename(path)
        if not (name.endswith(".configmap.json") or name in RUNTIME_CONFIG_NAMES
                or ("runtime" in name and "config" in name)):
            continue
        doc = load_json(path)
        data = doc.get("data") if isinstance(doc, dict) else None
        text = data.get("config.json") if isinstance(data, dict) else None
        if isinstance(text, str):
            try:
                doc = json.loads(text)
            except ValueError:
                continue
        if isinstance(doc, dict) and whole_number(doc.get("workers")):
            return path, doc
    return None, None


def config_file(snap, cfg):
    """-> "<snapshot>/<file>" that cfg came from."""
    path = (cfg or {}).get(CONFIG_FILE) or os.path.join(snap or "", "pipeline_config.json")
    return os.path.join(os.path.basename(os.path.dirname(path)), os.path.basename(path))


def config_hosts(cfg):
    """-> the host count a config sets (hosts, or multihost.hosts), or None."""
    layers = config_layers(cfg)
    found = [whole_number(lookup(layers, None, "hosts")),
             whole_number(lookup(layers, "multihost", "hosts"))]
    return max((h for h in found if h), default=None)


# value in bytes; key names it in shares ("of memory_max"); why and where give its source
Budget = collections.namedtuple("Budget", "value key why where config")


def memory_budget(root, snap):
    """-> the per-pod memory Budget of the run snap describes, or None. Every section uses this.

    resources.memory_max when the config sets it. Else the memory_max the
    runtime reports in deployment_runtime_status_details: with neither
    max_rss nor memory_max set, each host takes its own available memory
    (server.rs). Else max_rss_bytes in snap's own metrics.txt, the RSS limit
    of the controller, else max_rss in the config, the same limit as saved.
    Each is per pod, also on a multi-host pipeline.
    """
    _s, cfg = snapshot_config(root, snap)
    layers = config_layers(cfg)
    found = read_setting(layers, MEMORY_MAX)
    if found and found.value:
        return Budget(found.value, found.key, describe(found, MEMORY_MAX), "config", True)
    det = (cfg or {}).get("deployment_runtime_status_details")
    raw = det.get("memory_max") if isinstance(det, dict) else None
    value = setting_value(raw, MEMORY_MAX) if raw is not None else None
    if value:
        why = ("derived by the runtime from host memory" if read_setting(layers, MAX_RSS) is None
               else "reported by the runtime")
        return Budget(value, MEMORY_MAX.current, why,
                      f"deployment_runtime_status_details memory_max {json.dumps(raw)}", False)
    mpath = os.path.join(snap, "metrics.txt") if snap else None
    if mpath and os.path.exists(mpath):
        caps = [v for v in (prom_gauges(mpath, ["max_rss_bytes"]).get("max_rss_bytes") or {}).values()
                if v > 0]
        if caps:  # the tightest pod when hosts differ
            return Budget(min(caps), "max_rss_bytes", "the controller's RSS limit",
                          f"{os.path.basename(snap)}/metrics.txt", False)
    # a pipeline that is down has no metrics.txt, but its config can still name the RSS limit
    found = read_setting(layers, MAX_RSS)
    if found and found.value:
        return Budget(found.value, found.key, describe(found, MAX_RSS), "config", True)
    return None


def budget_text(b):
    """'64.0GB, derived by the runtime from host memory (where)' for a budget the config does not set."""
    return f"{fmt_bytes(b.value)}, {b.why} ({b.where})"


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
        _s, cfg = snapshot_config(root, snaps[-1])
        if not (cfg and CONFIG_FILE in cfg):
            print("  no pipeline_config.json in newest snapshot")
            return
    stand_in = CONFIG_FILE in cfg

    print(f"  name              {cfg.get('name')}")
    if stand_in:
        print("  platform          unknown: no pipeline_config.json, so no status, restarts or SQL")
    else:
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
          + (f"{config_file(snaps[-1], cfg)} (the runtime config the pipeline was given)"
             if stand_in else "deployment_config (the most recent run)"
             if isinstance(cfg.get("deployment_config"), dict)
             else "runtime_config (never started)"))
    workers, hosts = lookup(layers, None, "workers"), config_hosts(cfg)
    print(f"  {'workers':<20} {workers}"
          f"   io_workers {lookup(layers, None, 'io_workers')}"
          + (f"   hosts {hosts} ({workers // hosts} workers each; every budget is per pod)"
             if hosts and hosts > 1 and whole_number(workers) else ""))
    # each key as this pipeline spells it, value as written (what Feldera reads)
    for s in (MEMORY_MAX, MAX_RSS, DATAFUSION_MEMORY, STORAGE_MAX):
        print(f"  {size_key(cfg, s):<20} {describe(read_setting(layers, s), s)}")
    budget = memory_budget(root, snaps[-1])
    if budget and not budget.config:
        print(f"  {'memory budget':<20} {budget_text(budget)}; the config sets none")
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
    details = cfg.get("deployment_resources_status_details")
    found = pod_containers(details)
    pods = len({o for o, _, _ in found})
    for ordinal, name, c in found:
        t = last_termination(c)
        w = ((c.get("state") or {}).get("waiting") or {}).get("reason")
        print(f"  {container_who(ordinal, name, pods):<17} restartCount {c.get('restartCount', '?')}"
              + (f", waiting {w}" if w else "")
              + (f", last terminated {t.get('reason')} (exit {t.get('exitCode')})"
                 f" at {t.get('finishedAt')}" if t else ""))
    if '"OOMKilled"' in json.dumps(details):
        print("  >> OOMKilled present: treat memory as a primary suspect")

    ckpt = cfg.get("storage_status_details") or {}
    if isinstance(ckpt, dict) and ckpt.get("checkpoints") == []:
        print("  >> no checkpoints recorded: a restart replays the full snapshot")

    print_incarnations(snaps)
    if stand_in:
        return
    # per-snapshot restartCount pins when the first crash happened
    print("  -- restartCount by snapshot (main per pod, other containers when > 0) --")
    for s in snaps:
        c = load_json(os.path.join(s, "pipeline_config.json"))
        if not c:
            continue
        found = pod_containers(c.get("deployment_resources_status_details"))
        pods = len({o for o, _, _ in found})
        counts = [(container_who(o, name, pods), x.get("restartCount", "?"))
                  for o, name, x in found if name == "main" or x.get("restartCount")]
        shown = (str(counts[0][1]) if len(counts) == 1 and counts[0][0] == "main"
                 else ", ".join(f"{who} {n}" for who, n in counts) or "?")
        print(f"    {os.path.basename(s):<34} {shown}")


def print_incarnations(snaps):
    """Which snapshots each pipeline process served, oldest first."""
    groups = {}  # key -> (Incarnation, [snapshots]); dicts keep first-seen order
    for s in snaps:
        inc = incarnation(s)
        groups.setdefault(inc.key if inc else None, (inc, []))[1].append(os.path.basename(s))
    if not any(groups):
        return
    print("  -- incarnations (stats.json): a restart resets counters, histograms and the profile --")
    newest = incarnation(snaps[-1])
    for key, (inc, names) in groups.items():
        started = f"started {epoch_iso(inc.start)}" if inc and inc.start else "start unknown"
        what = "no stats.json" if key is None else started
        mark = "  (newest)" if inc and newest and key == newest.key else ""
        print(f"    {incarnation_name(inc):<14} {what:<29} {', '.join(names)}{mark}")


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
        # sorted: a set of strings iterates in a different order on every run
        bad = sorted(ln for ln in uniq if re.search(r"\bERROR\b|\bWARN\b.*error", ln))
        dup = len(lines) / max(len(uniq), 1)
        print(f"  {os.path.basename(s)}  lines={len(lines)} unique={len(uniq)}"
              f" dup_factor={dup:.1f} error_lines={len(bad)}")
        # logs.txt spans restarts: split off lines older than this snapshot's process
        start, since = incarnation_start(s)
        older = {ln for ln in bad if start and (iso_epoch(ln[:120]) or start) < start}
        groups = [("", [ln for ln in bad if ln not in older])]
        if older and since == STATS_START:
            # start_time is set when the controller starts, after the process's own startup
            print(f"     {len(older)} of them are older than this incarnation's start"
                  f" ({epoch_iso(start)}, {since}): an earlier process, or this one's startup")
            groups = [(f"since {epoch_iso(start)}", groups[0][1]),
                      (f"before {epoch_iso(start)} (an earlier process, or the startup of this"
                       " one before its controller began)", sorted(older))]
        elif older:
            # no stats.json: the pipeline is down, and the status change is no process start
            print(f"     {len(older)} of them are older than the last status change"
                  f" ({epoch_iso(start)}, {since}); with no stats.json the process start is"
                  " unknown, so they can be the errors of the process that just stopped")
            groups = [(f"since the status change at {epoch_iso(start)}", groups[0][1]),
                      (f"before the status change at {epoch_iso(start)} (the process that ran"
                       " until then, or an earlier one)", sorted(older))]
        for title, group in groups:
            if title and group:
                print(f"     -- {title} --")
            # distinct error shapes, with volatile ids folded away
            shapes = collections.Counter()
            for ln in group:
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


STATS_START = "stats.json start_time"


def incarnation_start(snap):
    """-> (epoch seconds, source) of when snap's pipeline process started, or (None, None).

    stats.json start_time, else deployment_runtime_status_since, which is no
    process start: it moves at every status change, also when the process stops.
    """
    inc = incarnation(snap)
    if inc and inc.start:
        return inc.start, STATS_START
    cfg = load_json(os.path.join(snap, "pipeline_config.json")) or {}
    since = iso_epoch(cfg.get("deployment_runtime_status_since"))
    return (since, "deployment_runtime_status_since") if since else (None, None)


def pipeline_events(root):
    """The runner's per-minute status events, oldest first, deduped by event_id.

    Each snapshot carries the events of the window before it, so windows
    overlap; the union spans more time than any one snapshot.
    """
    events = {}
    for s in snapshots(root):
        for e in load_json(os.path.join(s, "pipeline_events.json")) or []:
            if isinstance(e, dict) and e.get("event_id"):
                events[e["event_id"]] = e
    return sorted(events.values(), key=lambda e: str(e.get("recorded_at")))


# --------------------------------------------------------------------------
# section: sidecars -- what ran in the pod, from the runner's status history
# --------------------------------------------------------------------------
# "[2026-10-22T10:01:27Z] (<pod uid>) Normal :: Killing (kubelet): Stopping container x (#1)"
POD_EVENT = re.compile(r"^\[(?P<time>[^\]]+)\] \((?P<uid>[^)]*)\) (?P<type>\w+) :: "
                       r"(?P<reason>\w+) \((?P<source>[^)]*)\): (?P<message>.*?)(?: \(#\d+\))?$")
# Normal-type pod events that still stop or move a container
POD_EVENT_REASONS = {"Killing", "Evicted", "Preempting", "OOMKilling", "Unhealthy", "BackOff"}
# waits every pod start goes through
POD_START_WAITS = {None, "PodInitializing", "ContainerCreating"}


def status_samples(root):
    """-> ([(time, deployment_resources_status_details)], source), oldest first.

    pipeline_events.json samples the pod about once a minute; without it,
    each snapshot's pipeline_config.json gives one sample.
    """
    events = pipeline_events(root)
    if events:
        return ([(str(e.get("recorded_at")), e.get("deployment_resources_status_details"))
                 for e in events], "pipeline_events.json")
    samples = []
    for s in snapshots(root):
        cfg = load_json(os.path.join(s, "pipeline_config.json")) or {}
        if cfg.get("deployment_resources_status_details"):
            samples.append((snapshot_time(s), cfg["deployment_resources_status_details"]))
    return samples, "pipeline_config.json"


def pod_statuses(details):
    """-> [(pod ordinal or None, pod status)] for every Kubernetes pod status in a blob.

    The runner lists the pods of the statefulset in "pods" by ordinal, and keeps
    a pod's slot when the pod is missing, so the index is the ordinal.
    """
    found = []

    def walk(node, ordinal):
        if isinstance(node, dict):
            if "containerStatuses" in node or "initContainerStatuses" in node:
                found.append((ordinal, node))
                return
            for k, v in node.items():
                if k == "pods" and isinstance(v, list):
                    for i, pod in enumerate(v):
                        walk(pod, i)
                else:
                    walk(v, ordinal)
        elif isinstance(node, list):
            for v in node:
                walk(v, ordinal)
    walk(details, None)
    return found


# the runner names the pipeline container after its statefulset: pipeline-<pipeline id>
PIPELINE_CONTAINER = re.compile(r"^pipeline-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def pod_containers(details):
    """-> [(pod ordinal or None, name, container status)] in a status details blob.

    name is "main" for the pipeline container, else the container's own name
    (an init container, a native sidecar, or a second regular container such
    as istio-proxy).
    """
    out = []
    for ordinal, pod in pod_statuses(details):
        for group in ("containerStatuses", "initContainerStatuses"):
            members = [c for c in pod.get(group) or [] if isinstance(c, dict)]
            for c in members:
                name = c.get("name") or "?"
                if group == "containerStatuses" and (PIPELINE_CONTAINER.match(name)
                                                     or len(members) == 1):
                    name = "main"
                out.append((ordinal, name, c))
    return out


def container_who(ordinal, name, pods):
    """The container name, prefixed with its pod when the pipeline runs more than one."""
    return f"pod{ordinal}/{name}" if pods > 1 and ordinal is not None else name


def last_termination(c):
    """-> the container's newest terminated state, or None."""
    for state in ("state", "lastState"):
        t = (c.get(state) or {}).get("terminated")
        if t:
            return t
    return None


def pod_events(details):
    """-> [(pod ordinal or None, parsed event)] for the Kubernetes pod event strings in a blob."""
    found = []

    def walk(node, ordinal):
        if isinstance(node, dict):
            for k, v in node.items():
                if k == "pods" and isinstance(v, list):
                    for i, pod in enumerate(v):
                        walk(pod, i)
                else:
                    walk(v, ordinal)
        elif isinstance(node, list):
            for v in node:
                walk(v, ordinal)
        elif isinstance(node, str) and node.startswith("["):
            m = POD_EVENT.match(node)
            if m:
                found.append((ordinal, m.groupdict()))
    walk(details, None)
    return found


def image_label(container):
    """'repo:tag @digest12' with the registry path dropped."""
    image = str(container.get("image") or "?").rsplit("/", 1)[-1]
    m = re.search(r"sha256:([0-9a-f]{12})", str(container.get("imageID") or ""))
    return f"{image} @{m.group(1)}" if m else image


def mutable_tag(image):
    """True when an image reference can resolve to a new image at the next pull."""
    if "@sha256:" in image:
        return False
    name = image.rsplit("/", 1)[-1]
    return ":" not in name or name.rsplit(":", 1)[1] == "latest"


def section_sidecars(root, top):
    print("== sidecars and pod containers ==")
    samples, source = status_samples(root)
    if not samples:
        print("  no pod status in pipeline_events.json or pipeline_config.json")
        return
    print(f"  {len(samples)} pod status samples from {source}, {samples[0][0]} .. {samples[-1][0]}")

    _, cfg = snapshot_config(root, None)
    specs = {c["name"]: c for c in lookup(config_layers(cfg), None, "init_containers") or []
             if isinstance(c, dict) and c.get("name")}
    if specs:
        print("  -- configured init and sidecar containers (init_containers) --")
        print(f"  {'name':<20} {'kind':<8} {'probes':<26} {'memory limit':<13} image")
        for name, c in specs.items():
            kind = "sidecar" if c.get("restartPolicy") == "Always" else "init"
            probes = ",".join(p for p in ("startup", "liveness", "readiness")
                              if c.get(f"{p}Probe")) or "none"
            limit = ((c.get("resources") or {}).get("limits") or {}).get("memory", "-")
            print(f"  {name:<20} {kind:<8} {probes:<26} {limit:<13} {c.get('image')}")
            if kind == "sidecar" and not c.get("livenessProbe"):
                print(f"  >> {name}: no livenessProbe; Kubernetes keeps a hung sidecar ready and"
                      " never restarts it, so readiness and restarts cannot show a hang")
            if mutable_tag(str(c.get("image") or "")):
                print(f"  >> {name}: mutable image tag; each pod start can pull a different"
                      " image (watch the digest below)")

    # walk the samples: image changes per pod start, and per-container events,
    # each container keyed by its pod so the pods of a multi-host pipeline stay apart
    pods = len({o for _, d in samples for o, _ in pod_statuses(d)})
    timeline = set()  # (time, who, text)
    images, waiting = {}, {}
    was_ready, unready = set(), set()  # container ids that were ready / that lost it
    image_sets = []  # (main, started, first seen, {who: label}, [whos that changed])
    for when, details in samples:
        by_pod = collections.defaultdict(list)
        for ordinal, name, c in pod_containers(details):
            by_pod[ordinal].append((container_who(ordinal, name, pods), c))
        for ordinal, members in by_pod.items():
            current = {}
            for who, c in members:
                cid = c.get("containerID")
                if c.get("imageID"):  # a container still starting has no digest yet
                    current[who] = c
                for state in ("lastState", "state"):
                    t = (c.get(state) or {}).get("terminated")
                    if t and t.get("finishedAt"):
                        timeline.add((t["finishedAt"], who,
                                      f"terminated {t.get('reason')} (exit {t.get('exitCode')}),"
                                      f" ran from {t.get('startedAt')}"))
                w = ((c.get("state") or {}).get("waiting") or {}).get("reason")
                if w not in POD_START_WAITS and waiting.get(who) != w:
                    timeline.add((when, who, f"waiting: {w}"))
                waiting[who] = w
                # readiness lost by a running container, not the wait at its start
                if c.get("ready"):
                    if cid in unready:
                        timeline.add((when, who, "ready again"))
                        unready.discard(cid)
                    was_ready.add(cid)
                elif cid in was_ready and cid not in unready:
                    timeline.add((when, who, "not ready (it was, and did not restart)"))
                    unready.add(cid)
            labels = {n: image_label(c) for n, c in current.items()}
            changed = [n for n, lab in labels.items() if images.get(n) not in (None, lab)]
            if labels and (not image_sets or changed or set(labels) - set(images)):
                main = container_who(ordinal, "main", pods)
                running = ((current.get(main) or {}).get("state") or {}).get("running") or {}
                image_sets.append((main, running.get("startedAt"), when, labels, changed))
            images.update(labels)
        for ordinal, ev in pod_events(details):
            if ev["type"] != "Normal" or ev["reason"] in POD_EVENT_REASONS:
                timeline.add((ev["time"], container_who(ordinal, ev["source"], pods),
                              f"{ev['type']} {ev['reason']}: {ev['message'][:140]}"))

    print("  -- container images, at the first sample and at every change --")
    for main, started, when, labels, changed in image_sets:
        print(f"  {main} started {started or '?'} (first seen {when[:19]})")
        for who, lab in labels.items():
            mark = "  << changed" if who in changed else ""
            print(f"     {who[:24]:<24} {lab}{mark}")
        if len(changed) > 1:
            print(f"  >> {', '.join(changed)} changed image on the same pod start; either change"
                  " can explain what follows, so do not credit one without ruling out the other")

    print("  -- container and pod events (terminations, readiness, waits, kills, warnings) --")
    rows = sorted(timeline)
    limit = max(3 * top, 30)
    if len(rows) > limit:
        print(f"  ({len(rows) - limit} earlier events not shown; raise --top)")
    for when, who, text in rows[-limit:]:
        print(f"  {when[:19]:<19} {who[:24]:<24} {text}")
    if not rows:
        print("  none")


# --------------------------------------------------------------------------
# section: inputs -- connector buffer limits against the budget, and errors
# --------------------------------------------------------------------------
DEFAULT_MAX_QUEUED_RECORDS = 1_000_000  # feldera-types default_max_queued_records
QUEUED_BYTES_PER_RECORD = 1000  # an unset byte limit is 1000 x max_queued_records
QUEUED_BYTES_VERSION = (0, 305, 0)  # first release with a byte limit
MAX_QUEUED_SIZE = Setting(None, "max_queued_size", "max_queued_bytes", 1, "size")


def input_connectors(cfg):
    """-> {endpoint: connector config}: the SQL connectors plus any added by the API."""
    cfg = cfg or {}
    out = dict((cfg.get("program_info") or {}).get("input_connectors") or {})
    out.update(lookup(config_layers(cfg), None, "inputs") or {})
    return {k: v for k, v in out.items() if isinstance(v, dict)}


def queue_limits(conn, version):
    """-> (records, bytes or None, records set?, bytes set?) that pause this connector."""
    records = whole_number(conn.get("max_queued_records"))
    found = read_setting([conn], MAX_QUEUED_SIZE)
    size = found.value if found else None
    limit = DEFAULT_MAX_QUEUED_RECORDS if records is None else records  # 0 is a real limit
    if size is None and (version is None or version >= QUEUED_BYTES_VERSION):
        size = limit * QUEUED_BYTES_PER_RECORD
    return limit, size, records is not None, found is not None


def error_shape(message):
    message = re.sub(r"[0-9a-f]{8}-[0-9a-f-]{27}", "<uuid>", str(message))
    return re.sub(r"\d+", "<n>", message).strip()


ARROW_TRANSPORTS = ("delta_table_input", "iceberg_input")  # count bytes as Arrow batch memory


def arrow_bytes_per_record(conns, inputs, top):
    """Bytes per record of Arrow-based readers, with why it is not the row width.

    These readers add each batch's get_array_memory_size()
    (crates/static-connectors/src/deinput.rs), the capacity of every buffer the
    batch's arrays hold. The Delta reader decodes strings as Utf8View, which
    points into shared Parquet buffers, so one buffer can count in many batches.
    """
    rows = []
    for i in inputs:
        name = i.get("endpoint_name")
        transport = ((conns.get(name) or {}).get("transport") or {}).get("name")
        m = i.get("metrics") or {}
        if transport in ARROW_TRANSPORTS and m.get("total_records"):
            rows.append((name, transport, (m.get("total_bytes") or 0) / m["total_records"]))
    if not rows:
        return
    print("  -- bytes per record, Arrow-based readers (stats.json total_bytes / total_records) --")
    if len(rows) > top:
        print(f"  ({len(rows) - top} more not shown; raise --top)")
    for name, transport, per in sorted(rows, key=lambda r: -r[2])[:top]:
        print(f"  {fmt_bytes(per):>9}  {transport:<18} {keep_end(name, 70)}")
    print("  >> an upper bound, not the row width: it is Arrow buffer memory per batch, and a")
    print("     Utf8View string column (the Delta reader's default) can count one shared Parquet")
    print("     buffer in many batches. For the width, divide the table's size by its row count.")


HOST_INPUT_GAUGES = ("input_connector_records_total", "input_connector_end_of_input",
                     "input_connector_delta_phase",
                     "input_connector_delta_snapshot_completed_seconds",
                     "input_connector_delta_snapshot_records_total")
DELTA_LOADING = 0  # DeltaPhase::LoadingSnapshot in delta_table/input.rs


def inputs_per_host(root, top):
    """Multi-host input progress per host.

    Each host reads its own share of a distributed input, and a transaction
    commits only when every host is done, so the slowest host sets the pace.
    A split that is even by files can be uneven by rows.
    """
    snap, path = newest_metrics(root)
    if not path:
        return
    per = collections.defaultdict(lambda: collections.defaultdict(dict))  # endpoint -> host -> gauges
    for name, labels, v in prom_samples(path):
        if (name in HOST_INPUT_GAUGES and "endpoint" in labels
                and labels.get("ordinal", "").isdigit()):
            per[labels["endpoint"]][int(labels["ordinal"])][name] = v
    hosts = sorted({h for by_host in per.values() for h in by_host})
    if len(hosts) < 2:
        return
    inc = incarnation(snap)
    start = inc.start if inc else None
    now = start + inc.uptime if start and inc.uptime else None

    records = {h: sum(b.get(h, {}).get("input_connector_records_total", 0) for b in per.values())
               for h in hosts}
    # Behind: this host has not reached the end of an input that another host finished.
    behind = collections.defaultdict(list)
    for ep, by_host in per.items():
        eoi = {h: g.get("input_connector_end_of_input") for h, g in by_host.items()}
        if 1 in eoi.values():
            for h, v in eoi.items():
                if v == 0:
                    behind[h].append(ep)
    # Delta snapshot per host: loading, or the time its last snapshot finished.
    delta = {}
    for h in hosts:
        gs = [b[h] for b in per.values() if h in b and "input_connector_delta_phase" in b[h]]
        if gs and any(g["input_connector_delta_phase"] == DELTA_LOADING for g in gs):
            delta[h] = ("loading", now)
        elif gs:
            delta[h] = ("done", max(g.get("input_connector_delta_snapshot_completed_seconds", 0)
                                    for g in gs) or None)

    mean = sum(records.values()) / len(hosts)
    print(f"  -- per host ({len(hosts)} hosts, {os.path.basename(snap)}/metrics.txt):"
          " a commit waits for the slowest host --")
    print("     (snapshot minutes count from the pipeline start in stats.json; rec/s is every record"
          " since the start, snap rec/s the Delta snapshot records over the snapshot time)")
    print(f"  {'host':>4} {'records':>15} {'vs mean':>8} {'behind':>7} {'delta snapshot':>20}"
          f" {'rec/s':>9} {'snap rec/s':>10}")
    uptime = now - start if now and start else None
    for h in hosts:
        state, until = delta.get(h, (None, None))
        secs = until - start if until and start else None
        snap_col = "" if state is None else (
            f"{state} {secs / 60:.1f} min" + ("+" if state == "loading" else "") if secs else state)
        loaded = sum(b.get(h, {}).get("input_connector_delta_snapshot_records_total", 0)
                     for b in per.values())
        snap_rate = f"{loaded / secs:,.0f}" if state and secs else "-"
        rate = f"{records[h] / uptime:,.0f}" if uptime else "-"
        print(f"  {h:>4} {records[h]:>15,.0f} {100.0 * (records[h] - mean) / mean if mean else 0:>+7.1f}%"
              f" {len(behind.get(h, [])):>7} {snap_col:>20} {rate:>9} {snap_rate:>10}")
    if mean:
        top_host = max(hosts, key=lambda h: records[h])
        print(f"  records per host: mean {mean:,.0f}; max {records[top_host]:,.0f} on host {top_host}"
              f" (excess {records[top_host] - mean:,.0f}, {records[top_host] / mean:.2f}x mean)")
    done = [delta[h][1] - start for h in hosts if delta.get(h, ("",))[0] == "done"
            and delta[h][1] and start]
    for h in sorted(behind, key=lambda h: -len(behind[h]))[:top]:
        eps = ", ".join(e.rsplit(".", 1)[-1] for e in behind[h][:3])
        print(f"  >> host {h} has not reached the end of {len(behind[h])} input(s) that other hosts"
              f" finished ({eps}); the open transaction cannot commit until it does")
    loading = [h for h in hosts if delta.get(h, ("",))[0] == "loading"]
    if loading and done and now and start:
        print(f"  >> Delta snapshot: host(s) {', '.join(map(str, loading))} still loading after"
              f" {(now - start) / 60:.1f} min; the other hosts finished in"
              f" {min(done) / 60:.1f} .. {max(done) / 60:.1f} min")


def section_inputs(root, top):
    print("== inputs: buffer limits against the budget, and errors ==")
    snap, cfg = snapshot_config(root, None)
    conns = input_connectors(cfg)
    if not cfg:
        print("  no pipeline_config.json or runtime config in any snapshot: buffer limits unknown")
    elif conns:
        print(f"  from {config_file(snap, cfg)}")
        connector_limits(cfg, conns, top, memory_budget(root, snap))
    else:
        # HTTP ingress has no connector config, but stats.json still reports its buffers and errors
        print(f"  no input connectors in {config_file(snap, cfg)}")
    input_progress(root, conns, top)


def connector_limits(cfg, conns, top, mem_budget):
    """Connector mix and the sum of their buffer limits against the budget (memory_budget())."""
    transports = collections.Counter((c.get("transport") or {}).get("name") for c in conns.values())
    tables = collections.Counter(c.get("stream") for c in conns.values())
    pre = collections.Counter(p.get("name") for c in conns.values()
                              for p in c.get("preprocessor") or [] if isinstance(p, dict))
    print(f"  {'connectors':<18} {len(conns)}: "
          + ", ".join(f"{t} {n}" for t, n in transports.most_common()))
    print(f"  {'per table':<18} " + ", ".join(f"{t} {n}" for t, n in tables.most_common(top)))
    if pre:
        print(f"  {'preprocessors':<18} " + ", ".join(f"{p} on {n}" for p, n in pre.most_common())
              + "; a preprocessor failure counts as a parse error, and that message is dropped")

    # Feldera pauses a connector when its own buffer is full; nothing caps the sum
    version = version_tuple(cfg)
    size_name = next((read_setting([c], MAX_QUEUED_SIZE).key for c in conns.values()
                      if read_setting([c], MAX_QUEUED_SIZE)), None) \
        or (MAX_QUEUED_SIZE.current if version and version >= SIZE_RENAME_VERSION
            else MAX_QUEUED_SIZE.legacy)
    groups = collections.Counter(queue_limits(c, version) for c in conns.values())
    print("  -- buffer limits (each connector pauses only when its own buffer is full) --")
    for (records, size, rec_set, size_set), n in groups.most_common():
        print(f"  {n:>4} connectors  max_queued_records {records:,}"
              f"{'' if rec_set else ' (default)'}  {size_name} "
              + ("none in this version" if size is None
                 else fmt_bytes(size) + ("" if size_set else
                                         f" (default: {QUEUED_BYTES_PER_RECORD} x records)")))
    sum_records = sum(r * n for (r, _s, _a, _b), n in groups.items())
    sizes = [s * n for (_r, s, _a, _b), n in groups.items() if s is not None]
    print(f"  {'sum of limits':<18} {sum_records:,} records"
          + (f", {fmt_bytes(sum(sizes))}" if sizes else ""))
    layers = config_layers(cfg)
    for s in (MEMORY_MAX, MAX_RSS):
        budget = mem_budget if s is MEMORY_MAX else read_setting(layers, s)
        if sizes and budget and budget.value:
            share = 100.0 * sum(sizes) / budget.value
            print(f"  {'vs ' + budget.key:<18} {fmt_bytes(sum(sizes))} / {fmt_bytes(budget.value)}"
                  f" = {share:.0f}%" + ("" if s is MAX_RSS or budget.config
                                         else f"  ({budget.why})"))
            if share >= 100 and s is MEMORY_MAX:
                print(f"  >> the connectors may together buffer {share / 100:.1f}x {budget.key}"
                      " before any one of them pauses")


def input_progress(root, conns, top):
    """Buffered input and errors per snapshot (stats.json), then per host (metrics.txt)."""
    print("  -- buffered and errors per snapshot (stats.json; counters reset at each restart) --")
    print(f"  {'snapshot':<34} {'buffered':>12} {'bytes':>9} {'of mem':>7} "
          f"{'parse err':>10} {'transport':>10} {'records in':>14}")
    worst, shapes, fatal, last_inputs = None, {}, collections.Counter(), None
    spread = set()  # host counts of the multi-host snapshots
    derived = set()  # budgets not set in the config
    for s in snapshots(root):
        st = load_json(os.path.join(s, "stats.json"))
        if not st or not isinstance(st.get("inputs"), list):
            continue
        inputs = st["inputs"]

        def total(key):
            return sum((i.get("metrics") or {}).get(key) or 0 for i in inputs)
        budget = memory_budget(root, s)
        if budget and not budget.config:
            derived.add(f"{budget.key} {fmt_bytes(budget.value)}, {budget.why}")
        # stats.json sums every host, but memory_max is per pod: share of all the pods
        hosts = pipeline_hosts(root, s)
        share = (f"{100.0 * total('buffered_bytes') / (budget.value * hosts):.1f}%"
                 + ("*" if hosts > 1 else "") if budget and budget.value else "-")
        if hosts > 1:
            spread.add(hosts)
        print(f"  {os.path.basename(s):<34} {total('buffered_records'):>12,} "
              f"{fmt_bytes(total('buffered_bytes')):>9} {share:>7} {total('num_parse_errors'):>10,} "
              f"{total('num_transport_errors'):>10,} {total('total_records'):>14,}")
        if total("num_parse_errors") + total("num_transport_errors"):
            worst = (s, inputs)
        last_inputs = inputs
        for i in inputs:
            if i.get("fatal_error"):
                fatal[f"{i.get('endpoint_name')}: {str(i['fatal_error'])[:150]}"] += 1
            for kind in ("parse_errors", "transport_errors"):
                for e in i.get(kind) or []:
                    if isinstance(e, dict):
                        key = (i.get("endpoint_name"), e.get("timestamp"), e.get("message"))
                        shapes[key] = (kind, error_shape(e.get("message")), str(e.get("timestamp")))

    for text in sorted(derived):
        print(f"  'of mem' is of {text}")
    if spread:
        n = "/".join(map(str, sorted(spread)))
        print(f"  * {n} hosts: stats.json sums every host, so 'of mem' is of {n} x the per-pod budget,"
              " the mean pod; one pod can buffer more")
    if last_inputs:
        arrow_bytes_per_record(conns, last_inputs, top)
    inputs_per_host(root, top)

    if worst:
        s, inputs = worst
        print(f"  -- connectors with the most errors ({os.path.basename(s)}) --")
        ranked = sorted(inputs, key=lambda i: -((i.get("metrics") or {}).get("num_parse_errors", 0)
                                                + (i.get("metrics") or {}).get("num_transport_errors", 0)))
        for i in ranked[:top]:
            m = i.get("metrics") or {}
            errs = m.get("num_parse_errors", 0) + m.get("num_transport_errors", 0)
            if not errs:
                break
            print(f"  {str(i.get('endpoint_name'))[:60]:<60} parse={m.get('num_parse_errors', 0):,}"
                  f" transport={m.get('num_transport_errors', 0):,}"
                  f" ({100.0 * errs / max(m.get('total_records') or 0, 1):.2f}% of its records)")
    if shapes:
        print("  -- recent error messages (stats.json keeps the last few per connector), folded --")
        folded = {}
        for kind, shape, when in shapes.values():
            n, first, last = folded.get((kind, shape), (0, when, when))
            folded[(kind, shape)] = (n + 1, min(first, when), max(last, when))
        for (kind, shape), (n, first, last) in sorted(folded.items(), key=lambda x: -x[1][0])[:top]:
            print(f"  [{n}] {kind:<16} {first[:19]} .. {last[:19]}  {shape[:150]}")
    for text, n in fatal.most_common(top):
        print(f"  >> fatal [{n} snapshot(s)] {text}")

    changes, last = [], object()
    for e in pipeline_events(root):
        # some platform versions write an error text here, not an object
        det = e.get("deployment_runtime_status_details")
        stats = det.get("connector_stats") if isinstance(det, dict) else None
        n = stats.get("num_errors") if isinstance(stats, dict) else None
        if n is not None and n != last:
            changes.append((str(e.get("recorded_at")), n))
            last = n
    if changes:
        print("  -- connector error count over time (pipeline_events.json; resets at restart) --")
        limit = max(2 * top, 20)
        if len(changes) > limit:
            print(f"  ({len(changes) - limit} earlier changes not shown; raise --top)")
        for when, n in changes[-limit:]:
            print(f"  {when[:19]:<20} {n:,}")


# --------------------------------------------------------------------------
# section: trend -- each snapshot, oldest first (oldest is not always healthy)
# --------------------------------------------------------------------------
def section_trend(root):
    print("== trend across snapshots (oldest first) ==")
    hdr = f"  {'snapshot':<34} {'rss':>9} {'storage':>9} {'pressure':>9} {'in':>14} {'proc':>14}"
    print(hdr)
    prev = prev_inc = None
    for s in snapshots(root):
        st = load_json(os.path.join(s, "stats.json"))
        if not st:
            print(f"  {os.path.basename(s):<34} {'(no stats.json: pipeline already down)':>60}")
            continue
        inc = incarnation(s)
        if prev_inc and inc and inc.key != prev_inc.key:
            # a restart resets every counter: a delta across it is meaningless
            started = f" started {epoch_iso(inc.start)}" if inc.start else ""
            print(f"  -- new incarnation {incarnation_name(inc)}{started}:"
                  " counters restart, no deltas across this line --")
            prev = None
        prev_inc = inc or prev_inc
        g = st.get("global_metrics", st)
        rss = g.get("rss_bytes") or 0
        row = (f"  {os.path.basename(s):<34} {fmt_bytes(rss):>9} "
               f"{fmt_bytes(g.get('storage_bytes',0)):>9} "
               f"{str(g.get('memory_pressure')):>9} "
               f"{g.get('total_input_records',0):>14,} "
               f"{g.get('total_processed_records',0):>14,}")
        if prev is not None:
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


def leaf_readings(prof, metric_id):
    """Yield (worker index, node, value) of one metric on leaf nodes, numeric only."""
    leaves = simple_nodes(prof)
    for i, w in enumerate(prof.get("worker_profiles", [])):
        for node, ms in (w.get("metadata") or {}).items():
            if leaves and node not in leaves:
                continue  # skip root / cluster aggregates
            for m in ms:
                if m.get("metric_id") == metric_id:
                    v = numeric(m.get("value"))
                    if v is not None:
                        yield i, node, v


def node_metric(prof, metric_id):
    """-> {node: [values of the workers that report it]} for one metric."""
    out = collections.defaultdict(list)
    for _i, node, v in leaf_readings(prof, metric_id):
        out[node].append(v)
    return out


VIEW_DEF = re.compile(r'^\s*CREATE\s+(?:MATERIALIZED\s+|LOCAL\s+)?VIEW\s+"?([^"\s(]+)', re.I)


def dataflow_doc(snap):
    """-> snap's dataflow graph ({mir, calcite_plan}), or None.

    dataflow_graph.json, else pipeline_config.json program_info.dataflow,
    which holds the same mir and calcite_plan, else the program_ir.mir of a
    runtime config (no calcite_plan).
    """
    dg = load_json(os.path.join(snap, "dataflow_graph.json"))
    if not (isinstance(dg, dict) and isinstance(dg.get("mir"), dict)):
        cfg = load_json(os.path.join(snap, "pipeline_config.json")) or stand_in_config(snap)
        dg = ((cfg.get("program_info") or {}) if isinstance(cfg, dict) else {}).get("dataflow")
    return dg if isinstance(dg, dict) and isinstance(dg.get("mir"), dict) else None


class MirIndex:
    """Maps a circuit-profile persistent_id hash to its SQL provenance.

    Chain: circuit_profile node -> persistent_id "<hash>[.role]"
        -> dataflow_graph.json mir node -> table / view / operation / SQL line.
    Operators of a recursive view are children of a MIR node whose operation
    is "nested". They name no table or view, so the view is the CREATE VIEW
    statement above their SQL line.
    """

    def __init__(self, root, snap=None):
        """Read the MIR and SQL of snap, else of the newest snapshot of root that has them."""
        self.by_hash = {}
        self.sql_lines = []
        order = ([snap] if snap else []) + (list(reversed(snapshots(root))) if root else [])
        for s in order:
            dg = dataflow_doc(s)
            if dg:
                nested = []
                for mid, node in dg["mir"].items():
                    if node.get("persistent_id"):
                        self.by_hash[node["persistent_id"]] = (mid, node)
                    if node.get("operation") == "nested":
                        nested += [(cid, c) for cid, c in node.items()
                                   if isinstance(c, dict) and c.get("persistent_id")]
                for cid, child in nested:
                    self.by_hash.setdefault(child["persistent_id"], (cid, child))
                break
        for s in order:
            cfg = load_json(os.path.join(s, "pipeline_config.json"))
            if cfg and cfg.get("program_code"):
                self.sql_lines = cfg["program_code"].splitlines()
                break

    def resolve(self, persistent_id):
        """-> (label, mir_id, node) with the role suffix stripped off the hash.

        All None when the MIR has no entry; the caller falls back to the DBSP label.
        """
        h, _, role = (persistent_id or "").partition(".")
        hit = self.by_hash.get(h)
        if not hit:
            return None, None, None
        mid, node = hit
        line = ((node.get("positions") or [{}])[0]).get("start_line_number")
        what = node.get("table") or node.get("view") or (line and self.owning_view(line)) or "-"
        label = f"{what} [{node.get('operation') or '?'}]" + (f" sql:{line}" if line else "")
        if role:
            label += f" .{role}"
        return label, mid, node

    def line_of(self, persistent_id):
        """-> the SQL line that created the operator, or None."""
        _label, _mid, node = self.resolve(persistent_id)
        return ((node or {}).get("positions") or [{}])[0].get("start_line_number")

    def owning_view(self, line):
        """-> the view whose CREATE VIEW statement is the last one at or above line."""
        for i in range(min(line, len(self.sql_lines)) - 1, -1, -1):
            m = VIEW_DEF.match(self.sql_lines[i])
            if m:
                return m.group(1)
        return None

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
    labels = {n: mir.resolve(raw.get(n))[0] or dbsp.get(n, "") or raw.get(n, "")
              for n in set(raw) | set(dbsp)}
    near = nearest_sql(prof, labels, dbsp)
    return {n: f"{lab} via {near[n]} {labels[near[n]]}" if n in near else lab
            for n, lab in labels.items()}


_LABELS = {}  # (root, snapshot, id of the profile) -> node_labels(): every section asks for the same ones


def profile_labels(root, snap, prof):
    """-> node_labels() of prof through the MIR of snap, computed once per run."""
    key = (root, snap, id(prof))
    if key not in _LABELS:
        _LABELS[key] = node_labels(prof, MirIndex(root, snap))
    return _LABELS[key]


SQL_LINE = re.compile(r" sql:\d+")


def clip(label, width):
    """label cut to width; a "via" label loses its DBSP part first, so the SQL line stays."""
    if len(label) <= width:
        return label
    head, sep, tail = label.partition(" via ")
    if sep:
        head = head.split(" @ ", 1)[0]  # "JoinTrace @ s/o/d/join.rs:722:21" -> "JoinTrace"
        return (head + sep + tail)[:width]
    return label[:width]


def nearest_sql(prof, labels, dbsp, hops=6):
    """-> {node with no SQL line: nearest node that has one}, by graph.edges.

    DBSP adds operators that no MIR node names (JoinTrace, row_number, Z1),
    so their SQL is the nearest labeled node. At equal distance a path that
    only goes downstream wins (an MIR node labels the last operator it
    creates), then one that only goes upstream, then a mixed one: that finds
    the owner of a helper whose own walks end at a Z1, such as the trace of
    row_number's output. No walk follows an edge out of a Z1 (delay) node,
    so none crosses feedback.
    """
    ups, downs = collections.defaultdict(list), collections.defaultdict(list)
    for e in (prof.get("graph") or {}).get("edges") or []:
        a, b = e.get("from_node"), e.get("to_node")
        if e.get("from_cluster") or e.get("to_cluster") or dbsp.get(a, "").startswith("Z1"):
            continue
        downs[a].append(b)
        ups[b].append(a)
    out = {}
    has_sql = {n for n, lab in labels.items() if SQL_LINE.search(lab)}
    for node in labels:
        if node in has_sql:
            continue
        kind, frontier = {node: None}, [node]  # path kind: 0 down only, 1 up only, 2 mixed
        for _ in range(hops):
            nxt = {}
            for f in frontier:
                for k, edges in ((0, downs), (1, ups)):
                    for m in edges[f]:
                        if m not in kind:
                            nxt[m] = min(nxt.get(m, 2), k if kind[f] in (None, k) else 2)
            kind.update(nxt)
            frontier = sorted(nxt)
            hits = sorted((k, m) for m, k in nxt.items() if m in has_sql)
            if hits:
                out[node] = hits[0][1]
                break
            if not frontier:
                break
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
    lab = profile_labels(root, snap, prof)
    rt = node_metric(prof, "runtime_seconds")
    total = sum(sum(v) for v in rt.values()) or 1.0
    ranked = sorted(rt.items(), key=lambda kv: -sum(kv[1]))
    print(f"  total operator runtime {total:.1f}s across {len(rt)} nodes")
    print(f"  {'node':<10} {'share':>7} {'sum_s':>10} {'max_worker_s':>13}  label")
    for node, vals in ranked[:top]:
        print(f"  {node:<10} {100*sum(vals)/total:>6.2f}% {sum(vals):>10.2f} "
              f"{max(vals):>13.2f}  {clip(lab.get(node, ''), 100)}")

    for mid in ("circuit_wait_time_seconds", "circuit_idle_time_seconds",
                "circuit_runtime_seconds", "exchange_wait_time_seconds",
                "merge_backpressure_wait_time_seconds"):
        vals = [v for vs in node_metric(prof, mid).values() for v in vs]
        if vals:
            print(f"  {mid:<40} sum={sum(vals):>12.1f}s max={max(vals):>10.1f}s")
    single_worker_report(prof, lab, top)


def worker_max(prof, metric_id):
    """-> the largest value on any worker of one metric of the root circuit, or None.

    Circuit-wide metrics (circuit_runtime_seconds, steps_count) are on the
    root node, which leaf_readings() skips. Every nested sub-circuit (a
    recursive view) reports them too, and its iterations outnumber the root's
    steps, so only the root counts.
    """
    root = ((prof.get("graph") or {}).get("nodes") or {}).get("id") or "n"
    vals = [numeric(m.get("value")) for w in prof.get("worker_profiles", [])
            for m in (w.get("metadata") or {}).get(root) or []
            if m.get("metric_id") == metric_id]
    vals = [v for v in vals if v is not None]
    return max(vals) if vals else None


def circuit_wall(prof):
    """-> wall clock of the circuit in seconds: the max circuit_runtime_seconds of a worker."""
    return worker_max(prof, "circuit_runtime_seconds")


SINGLE_WORKER_SHARE = 0.9  # one worker does more than this share of an operator's runtime


def single_worker_ops(prof):
    """-> [(seconds on the busiest worker, node, worker, its share)] of single-worker operators.

    An operator that gathers its input to one worker (ROW_NUMBER() OVER ()
    with no PARTITION BY, a global aggregate, a sort) runs serially there,
    while the other workers wait for it at the next exchange. Only those
    with at least 1% of wall clock (of the busiest one, without a wall clock)
    count: below that the serial time does not matter.
    """
    out = []
    for node, vals in worker_values(prof, "runtime_seconds").items():
        tot = sum(vals)
        if len(vals) > 1 and tot > 0 and max(vals) / tot > SINGLE_WORKER_SHARE:
            w = max(range(len(vals)), key=vals.__getitem__)
            out.append((vals[w], node, w, vals[w] / tot))
    floor = (circuit_wall(prof) or max((s for s, *_ in out), default=0)) / 100
    return sorted((r for r in out if r[0] >= floor), reverse=True)


def single_worker_report(prof, lab, top):
    """Single-worker operators and the serial time they put on their worker."""
    shown = single_worker_ops(prof)
    wall = circuit_wall(prof)
    if not shown:
        return
    print(f"  -- single-worker operators (one worker does > {SINGLE_WORKER_SHARE:.0%} of the"
          " runtime; shown when >= 1% of wall clock) --")
    print(f"  {'node':<10} {'worker':>6} {'worker_s':>9} {'of op':>6} {'of wall':>8}  label")
    for secs, node, w, share in shown[:top]:
        of_wall = f"{100 * secs / wall:.1f}%" if wall else "n/a"
        print(f"  {node:<10} {w:>6} {secs:>9.2f} {100 * share:>5.1f}% {of_wall:>8}"
              f"  {clip(lab.get(node, ''), 100)}")
    if len(shown) > top:
        print(f"  ... {len(shown) - top} more (--top)")
    per = collections.defaultdict(list)
    for secs, node, w, _s in shown:
        per[w].append(secs)
    for w, secs in sorted(per.items(), key=lambda kv: -sum(kv[1])):
        print(f"  >> worker {w} runs {len(secs)} of them serially: {sum(secs):,.1f}s"
              + (f" = {100 * sum(secs) / wall:.1f}% of wall clock ({wall:,.1f}s)" if wall else "")
              + "; the other workers wait for it at the next exchange")


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
    lab = profile_labels(root, snap, prof)

    # 1. key_distribution is an explicit per-worker histogram
    print("  -- key_distribution (balancer) --")
    rows, readings = [], 0
    leaves = simple_nodes(prof)
    for w in prof.get("worker_profiles", []):
        for node, ms in (w.get("metadata") or {}).items():
            if leaves and node not in leaves:
                continue
            for m in ms:
                if m.get("metric_id") != "key_distribution":
                    continue
                readings += 1
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
                  f"keys={tot:>15,.0f}  {clip(lab.get(node, ''), 100)}")
            if len(seen) >= top:
                break
    else:
        no_key_distribution(root, snap, prof, readings)

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
                  f"total={tot:>18,.0f} ({100*tot/grand:>5.2f}% of all)  {clip(lab.get(node, ''), 100)}")
        if mid == "runtime_seconds":
            single = single_worker_ops(prof)
            if single:
                workers = sorted({w for *_x, w, _s in single})
                print(f"  >> {len(single)} operator(s) with >= 1% of wall clock run on one worker"
                      f" (> {SINGLE_WORKER_SHARE:.0%} of their runtime on worker"
                      f" {', '.join(map(str, workers))}): a ratio near {nworkers} is a gather,"
                      " not key skew; see the time section")

    for mid in ("rebalancings_count", "total_rebalancing_time_seconds",
                "accumulator_records_to_repartition_count"):
        vals = [v for vs in node_metric(prof, mid).values() for v in vs]
        if vals and sum(vals):
            print(f"  {mid:<44} sum={sum(vals):>16,.0f} max={max(vals):>14,.0f}")


def no_key_distribution(root, snap, prof, readings):
    """Why the profile has no key_distribution data: adaptive joins off, or no data yet.

    Only the balancer of an adaptive join collects key distributions, and
    dev_tweaks.adaptive_joins defaults to false (DevTweaks::adaptive_joins()).
    """
    what = (f"all {readings} key_distribution histograms are empty" if readings
            else "no operator reports key_distribution")
    layers = config_layers(snapshot_config(root, snap)[1])
    raw = lookup(layers, "dev_tweaks", "adaptive_joins")
    adaptive = raw is True
    st = load_json(os.path.join(snap, "stats.json")) or {}
    processed = (st.get("global_metrics", st) or {}).get("total_processed_records")
    if not isinstance(processed, (int, float)):  # no stats.json: count the profile's own inputs
        processed = sum(v for vs in node_metric(prof, "input_batches_stats").values() for v in vs) or None
    flowed = ("records processed unknown" if processed is None
              else f"{processed:,.0f} records processed")
    if not layers:
        print(f"     {what}: no config in the bundle, so dev_tweaks.adaptive_joins is unknown"
              f" (default false; only a join balancer collects key distributions) ({flowed})")
        print("     judge key skew by the per-worker imbalance below, not by this histogram")
    elif not adaptive:
        setting = "unset (default false)" if raw is None else json.dumps(raw)
        print(f"     {what}: dev_tweaks.adaptive_joins is {setting}, so no join balancer"
              f" collects key distributions ({flowed})")
        print("     judge key skew by the per-worker imbalance below, not by this histogram")
    elif not processed:
        print(f"     {what}: no data flowed yet ({flowed})")
    else:
        print(f"     {what} although dev_tweaks.adaptive_joins is true and {flowed}:"
              " no balanced join has reported keys")


# --------------------------------------------------------------------------
# section: sql -- amplification and wasted work
# --------------------------------------------------------------------------
def truncates(bounds):
    """True when a retainment_bounds measurement lets the trace drop records.

    Feldera writes {"key": [...], "value": [...]} (TraceBoundsInner::metadata
    in dbsp's operator/dynamic/trace.rs). Each list holds the Debug text of
    Option bounds and the lowest one applies, so one "None" (no bound, or no
    waterline yet) keeps every record. A side that is a retain filter, not a
    list, drops records.
    """
    if not (isinstance(bounds, dict) and "key" in bounds and "value" in bounds):
        return bounds not in (None, [], {})  # an older shape: any content is a bound

    def side(pred):
        if not isinstance(pred, list):
            return True
        texts = [p.get("value") if isinstance(p, dict) else p for p in pred]
        return bool(texts) and "None" not in texts
    return side(bounds["key"]) or side(bounds["value"])


def section_sql(root, top):
    snap, prof = newest_profile(root)
    if not prof:
        print("== sql ==\n  no circuit_profile.json[.gz] in any snapshot")
        return
    print(f"== sql efficiency (from {os.path.basename(snap)}) ==")
    lab = profile_labels(root, snap, prof)

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
              f"  {clip(lab.get(node, ''), 100)}")
    if not rows:
        print("     no join operators produced output in this profile")
        _, mpath = snapshot_metrics(root, snap)
        txn_open, done = transactions(mpath) if mpath else (False, 0)
        if txn_open and done <= 1:
            print("  NOTE profile captured with a transaction still open and <=1 step done.")
            print("       Join/output metrics are zero because that work happens at commit.")
            print("       Re-collect a bundle mid-commit to size joins.")

    # non-join operators: a renumbering (ROW_NUMBER() OVER ()) or an explode can emit
    # far more changes than it reads, and every operator after it pays for them
    ins, outs = total("input_batches_stats"), total("output_batches_stats")
    rows = [(o, o / ins[n], ins[n], n) for n, o in outs.items()
            if n not in left and n not in right and ins.get(n, 0) > 0 and o > ins[n]]
    print("  -- operator amplification, non-join: records out / records in (ranked by records out) --")
    for o, amp, i, node in sorted(rows, reverse=True)[:top]:
        print(f"  {node:<10} amp={amp:>9.2f} out={o:>15,.0f} in={i:>15,.0f}"
              f"  {clip(lab.get(node, ''), 100)}")
    if not rows:
        print("     no non-join operator emits more records than it reads")

    print("  -- output_redundancy_percent (work discarded downstream) --")
    red = node_metric(prof, "output_redundancy_percent")
    rows = [(max(v), node) for node, v in red.items() if v and max(v) > 0]
    for pct, node in sorted(rows, reverse=True)[:top]:
        print(f"  {node:<10} {pct:>6.2f}%  {clip(lab.get(node, ''), 100)}")
    if not rows:
        print("     none reported above zero")

    print("  -- negative_weight_count (retraction churn) --")
    neg = total("negative_weight_count")
    rows = [(v, node) for node, v in neg.items() if v > 0]
    for v, node in sorted(rows, reverse=True)[:top]:
        print(f"  {node:<10} {v:>18,.0f}  {clip(lab.get(node, ''), 100)}")
    if not rows:
        print("     none reported above zero")

    # unbounded state: operators holding records with no retainment bound
    st = total("state_records_count")
    bounded = set()
    for w in prof.get("worker_profiles", []):
        for node, ms in (w.get("metadata") or {}).items():
            for m in ms:
                if m.get("metric_id") == "retainment_bounds" and truncates(m.get("value")):
                    bounded.add(node)
    unb = [(v, n) for n, v in st.items() if v > 0 and n not in bounded]
    print("  -- largest state with no retainment bound (candidates for LATENESS / windowing) --")
    for v, node in sorted(unb, reverse=True)[:top]:
        print(f"  {node:<10} {v:>18,.0f} records  {clip(lab.get(node, ''), 100)}")


# --------------------------------------------------------------------------
# section: materialized -- stored views that no output connector reads
# --------------------------------------------------------------------------
def relation_key(name):
    """One spelling for a table or view: quotes off, case folded."""
    return (name or "").strip('"').lower()


def newest_with(root, name, key, prefer=None):
    """-> (snapshot, artifact) for prefer, else the newest snapshot, whose artifact has key."""
    for s in ([prefer] if prefer else []) + list(reversed(snapshots(root))):
        doc = load_json(os.path.join(s, name))
        if isinstance(doc, dict) and doc.get(key):
            return s, doc
    return None, None


def materialized_views(cfg):
    """-> ({view: has output connector}, source) for every MATERIALIZED view.

    program_info (compiled schema) is exact. Without it, fall back to the SQL:
    a connector is declared in the WITH clause before the view's AS.
    """
    info = cfg.get("program_info") or {}
    outputs = (info.get("schema") or {}).get("outputs")
    if outputs is not None:
        fed = {relation_key(c.get("stream")) for c in (info.get("output_connectors") or {}).values()}
        return ({relation_key(o.get("name")): relation_key(o.get("name")) in fed
                 for o in outputs if o.get("materialized")}, "program_info")
    out = {}
    for stmt in (cfg.get("program_code") or "").split(";"):
        m = re.match(r'\s*CREATE\s+MATERIALIZED\s+VIEW\s+"?(\w+)"?', stmt, re.I)
        if m:
            head = re.split(r"\sAS\s", stmt, maxsplit=1, flags=re.I)[0]
            out[relation_key(m.group(1))] = "'connectors'" in head
    return out, "program_code"


def view_readers(dataflow):
    """-> {relation: {views that scan it}} from the Calcite plan."""
    readers = collections.defaultdict(set)
    for view, plan in ((dataflow or {}).get("calcite_plan") or {}).items():
        for rel in (plan or {}).get("rels") or []:
            if rel.get("relOp") == "LogicalTableScan" and rel.get("table"):
                readers[relation_key(rel["table"][-1])].add(relation_key(view))
    return readers


MATERIALIZED_METRICS = ("state_records_count", "spine_storage_size_bytes", "used_memory_bytes")


def materialized_traces(prof, dataflow):
    """-> {view: {trace node ids}} that hold the copy MATERIALIZED keeps.

    The copy feeds an Output node whose persistent_id is
    "<hash of the view's MIR inspect node>.integral". The records live in the
    Z1 (trace) next to the AccumulateUntimedTraceAppend upstream of that Output.
    """
    inspect = {}
    for node in ((dataflow or {}).get("mir") or {}).values():
        if node.get("operation") == "inspect" and node.get("view") and node.get("persistent_id"):
            inspect[node["persistent_id"]] = relation_key(node["view"])
    leaves = simple_nodes(prof)
    pids = {}
    for node, ms in ((prof.get("worker_profiles") or [{}])[0].get("metadata") or {}).items():
        for m in ms:
            if m.get("metric_id") == "persistent_id":
                pids[node] = (m.get("value") or {}).get("value") or ""
    ups, downs = collections.defaultdict(list), collections.defaultdict(list)
    for e in (prof.get("graph") or {}).get("edges") or []:
        ups[e.get("to_node")].append(e.get("from_node"))
        downs[e.get("from_node")].append(e.get("to_node"))
    out = {}
    for node, pid in pids.items():
        view = inspect.get(pid.split(".", 1)[0])
        if not view or not pid.endswith(".integral") or leaves.get(node) != "Output":
            continue
        near, frontier = set(), [node]
        for _ in range(3):  # Output <- Apply <- AccumulateUntimedTraceAppend
            frontier = [u for f in frontier for u in ups[f]]
            near.update(frontier)
        out[view] = {d for n in near if leaves.get(n, "").startswith("AccumulateUntimedTraceAppend")
                     for d in downs[n] if leaves.get(d) == "Z1 (trace)"}
    return out


def section_materialized(root, top):
    snap, cfg = newest_with(root, "pipeline_config.json", "program_code")
    if not cfg:
        # a runtime config carries the compiled schema in program_ir, but no SQL
        snap, cfg = snapshot_config(root, None)
        if not (cfg and CONFIG_FILE in cfg and "schema" in cfg["program_info"]):
            print("== materialized ==\n  no pipeline_config.json with program_code, and no runtime"
                  " config with program_ir, in any snapshot")
            return
    views, source = materialized_views(cfg)
    lonely = sorted(v for v, fed in views.items() if not fed)
    origin, source = ((config_file(snap, cfg), "program_ir") if CONFIG_FILE in cfg
                      else (os.path.basename(snap), source))
    print(f"== materialized views with no output connector (from {origin}, {source}) ==")
    print(f"  {len(views)} materialized views; {len(views) - len(lonely)} feed an output connector; "
          f"{len(lonely)} do not")
    if not lonely:
        return
    # the MIR, stats and budget of the run that the profile describes
    psnap, prof = newest_profile(root)
    dataflow = next(filter(None, map(dataflow_doc, ([psnap] if psnap else [])
                                     + list(reversed(snapshots(root))))), None)
    readers = view_readers(dataflow)
    traces = materialized_traces(prof, dataflow) if prof and dataflow else {}
    sums = {mid: {n: sum(v) for n, v in node_metric(prof, mid).items()}
            for mid in MATERIALIZED_METRICS} if traces else {}
    state = {v: tuple(sum(sums[mid].get(t, 0) for t in ts) for mid in MATERIALIZED_METRICS)
             for v, ts in traces.items()}
    _, stats = newest_with(root, "stats.json", "global_metrics", prefer=psnap)
    http = collections.Counter(relation_key((o.get("config") or {}).get("stream"))
                               for o in (stats or {}).get("outputs") or []
                               if ".api-" in (o.get("endpoint_name") or ""))
    # the profile's own spine total: stats.json storage_bytes counts the files differently
    # (fine-grained-auth: 12.8MB of spines, 4.2MB of storage_bytes), so shares of it pass 100%
    storage = sum(sum(v) for v in node_metric(prof, "spine_storage_size_bytes").values()) if prof else 0
    budget = memory_budget(root, psnap)
    lines = {}
    for i, text in enumerate((cfg.get("program_code") or "").splitlines(), 1):
        m = re.match(r'\s*CREATE\s+MATERIALIZED\s+VIEW\s+"?(\w+)"?', text, re.I)
        if m:
            lines[relation_key(m.group(1))] = i

    # a copy costs RAM until it spills, so rank by memory and disk together
    rows = sorted(lonely, key=lambda v: -sum((state.get(v) or (0, 0, 0))[1:]))
    print(f"  {'view':<48} {'sql':>5} {'records':>12} {'in memory':>9} {'on disk':>9} {'of disk':>8}"
          f" {'http':>5}  read by views")
    for v in rows[:top]:
        rec, disk, mem = state.get(v) or (None, None, None)
        share = f"{100 * disk / storage:.1f}%" if disk is not None and storage else "n/a"
        recs = f"{rec:,.0f}" if rec is not None else "n/a"
        size = fmt_bytes(disk) if disk is not None else "n/a"
        ram = fmt_bytes(mem) if mem is not None else "n/a"
        names = sorted(readers.get(v, ()))
        shown = ", ".join(names[:2]) + (f" +{len(names) - 2}" if len(names) > 2 else "")
        print(f"  {v[:48]:<48} {lines.get(v, ''):>5} {recs:>12} {ram:>9} {size:>9} {share:>8}"
              f" {http.get(v, 0):>5}  {len(names)}: {shown or '-'}")
    if len(rows) > top:
        print(f"  ... {len(rows) - top} more (--top)")
    known = [state[v] for v in lonely if v in state]
    if known:
        disk, mem = sum(s[1] for s in known), sum(s[2] for s in known)
        # memory_max is per pod: on several hosts, charge the host that holds the most
        hosts, per, why = host_layout(root, psnap, prof)
        pod, where = mem, ""
        if per:
            nodes = set().union(*(traces[v] for v in lonely if v in traces))
            on_host = host_totals(prof, "used_memory_bytes", per, nodes)
            h = max(range(len(on_host)), key=on_host.__getitem__)
            pod, where = on_host[h], f"; host {h} holds {fmt_bytes(on_host[h])}"
        mem_share = ""
        if budget and budget.value and (hosts == 1 or per):
            mem_share = f" ({100 * pod / budget.value:.1f}% of {budget.key}{' on that host' if per else ''})"
        elif hosts > 1:
            mem_share = f" (all {hosts} hosts; {why}, so no per-pod share)"
        print(f"  stored copies: {sum(s[0] for s in known):,.0f} records, {fmt_bytes(mem)} in memory"
              + where + mem_share
              + f", {fmt_bytes(disk)} on disk"
              + (f" ({100 * disk / storage:.1f}% of all spine storage)" if storage else ""))
    print("  ASK for each view: who needs its stored copy? MATERIALIZED serves ad-hoc SELECT")
    print("  queries, send_snapshot (on a connector or an HTTP egress request) and debugging.")
    print("  Being read by another view does not need it. http > 0 means clients stream it over")
    print("  HTTP egress, which needs the copy only with send_snapshot=true. If nothing needs it,")
    print("  CREATE VIEW keeps it streamable; CREATE LOCAL VIEW also drops it as an output.")


# --------------------------------------------------------------------------
# section: memory -- always relative to the pod budget
# --------------------------------------------------------------------------
# Bits per key of Feldera's modular Bloom filter by module count; each module
# adds one decade to the rate (crates/dbsp/src/storage/file/filter/bloom.rs)
BLOOM_MODULE_BITS = (0.0, 4.81, 9.62, 14.42, 19.23)
DEFAULT_BLOOM_FALSE_POSITIVE_RATE = 0.0001  # DevTweaks::bloom_false_positive_rate()
BLOOM_TARGETS = (0.001, 0.01)  # coarser rates to project savings for


def bloom_modules(false_positive_rate):
    """Modules a rate gets, as modules_for_rate() in bloom.rs: the nearest decade, 0 to 4.

    0 for a rate at or outside (0, 1), which disables filters.
    """
    if not 0.0 < false_positive_rate < 1.0:
        return 0
    decades = math.floor(math.log10(1.0 / false_positive_rate) + 0.5)  # Rust rounds half up
    return max(0, min(len(BLOOM_MODULE_BITS) - 1, decades))


def bloom_bits_per_key(false_positive_rate):
    """Bits per key a rate implies, or 0 when the rate disables filters."""
    return BLOOM_MODULE_BITS[bloom_modules(false_positive_rate)]


def bloom_rate(cfg):
    """-> (rate, the key that set it or None for the default), or (None, None) with no config.

    storage.bloom_false_positive_rate wins over the deprecated
    dev_tweaks.bloom_false_positive_rate (Runtime::bloom_false_positive_rate).
    Only the first layer counts: both keys are left out when unset, so a key
    that deployment_config lacks was unset for that run.
    """
    layers = config_layers(cfg)
    if not layers:
        return None, None
    for section in ("storage", "dev_tweaks"):
        v = (layer_node(layers[0], section) or {}).get("bloom_false_positive_rate")
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return float(v), f"{section}.bloom_false_positive_rate"
    return DEFAULT_BLOOM_FALSE_POSITIVE_RATE, None


def worker_values(prof, metric_id):
    """-> {node: [value per worker_profiles entry]} for one metric, leaves only, 0 where absent."""
    workers = len(prof.get("worker_profiles", []))
    out = {}
    for i, node, v in leaf_readings(prof, metric_id):
        out.setdefault(node, [0.0] * workers)[i] += v
    return out


def host_layout(root, snap, prof):
    """-> (hosts, workers per host or None, why there is no split or None) for prof.

    A multi-host profile concatenates the hosts' worker_profiles in host order,
    so worker i runs on host i // (workers / hosts). That holds only when the
    profile has every worker the config of its run names.
    """
    hosts = pipeline_hosts(root, snap)
    n = len(prof.get("worker_profiles", []))
    if hosts <= 1:
        return hosts, None, None
    workers = whole_number(lookup(config_layers(snapshot_config(root, snap)[1]), None, "workers"))
    if not n or n % hosts:
        return hosts, None, f"{n} profile workers do not split evenly over {hosts} hosts"
    if workers and n != workers:
        return hosts, None, f"the profile has {n} workers but the config names {workers}"
    return hosts, n // hosts, None


def per_host(values, per):
    """-> [sum per host] of per-worker values, per workers to a host."""
    return [sum(values[i:i + per]) for i in range(0, len(values), per)]


def host_totals(prof, metric_id, per, nodes=None):
    """-> [total of one metric per host] over nodes (every leaf when None)."""
    by_worker = [0.0] * len(prof.get("worker_profiles", []))
    for i, node, v in leaf_readings(prof, metric_id):
        if nodes is None or node in nodes:
            by_worker[i] += v
    return per_host(by_worker, per)


def section_memory(root, top):
    snap, prof = newest_profile(root)
    cfg_snap, cfg = snapshot_config(root, snap)
    layers = config_layers(cfg)
    mem, disk = read_setting(layers, MEMORY_MAX), read_setting(layers, STORAGE_MAX)
    found = memory_budget(root, snap)
    budget = found.value if found else None
    print("== memory, as a share of the pipeline's own budget ==")
    if snap and cfg_snap != snap:
        print(f"  NOTE {os.path.basename(snap)} has no pipeline_config.json; budget is from"
              f" {os.path.basename(cfg_snap) if cfg_snap else 'nowhere'}, maybe another run")
    if cfg:
        print(f"  config from {config_file(cfg_snap, cfg)}")
    if found and found.config:
        print(f"  budget {found.key} = {found.why}")
    elif found:
        print(f"  budget {found.key} = {budget_text(found)}"
              + (f"; config {mem.key} {describe(mem, MEMORY_MAX)}" if mem else ""))
    else:
        print("  budget unknown; shares cannot be computed"
              + (f": {mem.key} {describe(mem, MEMORY_MAX)}" if mem else ""))
    if not prof:
        print("  no circuit_profile.json[.gz] in any snapshot")
        return

    store = disk.value if disk else None
    print(f"  budget {disk.key} = {describe(disk, STORAGE_MAX)}" if store
          else "  storage budget unknown"
          + (f": {disk.key} {describe(disk, STORAGE_MAX)}" if disk else ""))

    # Both budgets are per pod; a multi-host profile sums every host's workers.
    hosts, per, why = host_layout(root, snap, prof)
    if per:
        print(f"  {hosts} hosts x {per} workers: each budget is per pod, so every share is of one"
              " host's total; median and max are")
        print("  hosts ranked by their own total; cluster sums all hosts and mean is cluster / hosts")
    elif hosts > 1:
        print(f"  {hosts} hosts; {why}: totals are cluster-wide, so shares overstate one pod")

    def share(n, against="mem"):
        b = budget if against == "mem" else store
        if not b:
            return "n/a"
        return f"{100.0*n/b:>7.2f}% of {'memory' if against=='mem' else 'storage'}"

    def pct(n, against="mem"):
        b = budget if against == "mem" else store
        return f"{100.0 * n / b:.1f}%" if b else "n/a"

    def total_of(mid):
        return sum(v for vs in node_metric(prof, mid).values() for v in vs)

    def totals(mid):
        """-> (cluster total, [total per host] or None on a single host)."""
        if not per:
            return total_of(mid), None
        ph = host_totals(prof, mid, per)
        return sum(ph), ph

    def biggest(ph):
        """-> the host with the largest total."""
        return max(range(len(ph)), key=ph.__getitem__)

    def row(name, t, ph, against="mem"):
        if ph is None:
            print(f"  {name:<28} {fmt_bytes(t):>10}  {share(t, against)}")
            return
        med, h = statistics.median(ph), biggest(ph)
        print(f"  {name:<28} {fmt_bytes(t):>10} {fmt_bytes(t / len(ph)):>9}"
              f"  {fmt_bytes(med):>9} {pct(med, against):>7}"
              f"  {fmt_bytes(ph[h]):>9} {pct(ph[h], against):>7}  host {h}")

    def header():
        if per:
            print(f"  {'':<28} {'cluster':>10} {'mean':>9}  {'median host':>17}  {'max host':>17}")

    # Filters live in RAM, so they belong against the memory budget. Summing
    # only bloom bytes under-reports the real cost.
    print("  -- in RAM (compare against memory budget) --")
    header()
    fam_total, fam_hosts = 0, [0.0] * hosts if per else None
    for mid in FILTER_METRICS:
        t, ph = totals(mid)
        fam_total += t
        fam_hosts = [a + b for a, b in zip(fam_hosts, ph)] if ph else fam_hosts
        row(mid, t, ph)
    row("all filter families", fam_total, fam_hosts)
    for mid in ("used_memory_bytes", "allocated_memory_bytes", "shared_memory_bytes"):
        t, ph = totals(mid)
        if t:
            row(mid, t, ph)

    # These are on-disk sizes; charging them to the memory budget would
    # manufacture a fake >100% reading.
    print("  -- on disk (compare against storage budget) --")
    header()
    for mid in ("spine_storage_size_bytes", "merging_size_bytes"):
        t, ph = totals(mid)
        if t:
            row(mid, t, ph, "disk")

    lab = profile_labels(root, snap, prof)
    if per:
        print("  -- heaviest operators by in-memory state, on the host where each is largest --")
        rows = []
        for node, vals in worker_values(prof, "used_memory_bytes").items():
            ph = per_host(vals, per)
            h = biggest(ph)
            rows.append((ph[h], node, h, sum(ph)))
        for v, node, h, t in sorted(rows, reverse=True)[:top]:
            print(f"  {node:<10} {fmt_bytes(v):>10} {pct(v):>7} on host {h:<4} cluster"
                  f" {fmt_bytes(t):>9}  {clip(lab.get(node, ''), 100)}")
    else:
        used = {k: sum(v) for k, v in node_metric(prof, "used_memory_bytes").items()}
        print("  -- heaviest operators by in-memory state --")
        for v, node in sorted(((v, k) for k, v in used.items()), reverse=True)[:top]:
            print(f"  {node:<10} {fmt_bytes(v):>10}  {share(v):<20} {clip(lab.get(node, ''), 100)}")

    # Cost/benefit per filter family. A fixed byte threshold cannot see that a
    # filter is large AND useless; size divided by hit rate can.
    print("  -- filter cost vs benefit (is the RAM earning its keep?) --")
    for fam in ("bloom", "range", "roaring"):
        size, size_hosts = totals(f"{fam}_filter_size_bytes")
        hits = total_of(f"{fam}_filter_hits_count")
        misses = total_of(f"{fam}_filter_misses_count")
        looks = hits + misses
        if not size and not looks:
            continue
        pod = max(size_hosts) if size_hosts else size  # what one pod pays
        rate = 100.0 * hits / looks if looks else None
        per_hit = size / hits if hits else float("inf")
        verdict = ""
        if size and not looks:
            verdict = "  <= no lookups yet: its hit rate is unknown"
        elif size and rate < 1.0 and (not budget or pod >= budget / 100):
            # below 1% of the memory budget a useless filter costs too little to tune
            verdict = "  <= almost never hits: candidate to shrink or disable"
        cost = (share(size) if not size_hosts
                else f"max host {fmt_bytes(pod)} {pct(pod)} of memory")
        print(f"  {fam:<9} size={fmt_bytes(size):>9} {cost}  hit_rate="
              + (f"{rate:>6.2f}%" if rate is not None else f"{'n/a':>7}")
              + f"  bytes/hit={'inf' if per_hit == float('inf') else fmt_bytes(per_hit)}{verdict}")

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
    reported = weighted / weight if weight > 0 else None

    # Filter sizing: are the filters bigger than the configured rate implies?
    print("  -- filter sizing --")
    print("  reported bits/key           " + (f"{reported:.2f} (size weighted)" if reported is not None
                                             else "not reported by any operator"))

    def small_batches():
        print("     >> filters cost far more per key than the rate implies. A filter has at least")
        print("        one 64-bit word per module (crates/modular-bloom/src/layout.rs), so batches")
        print(f"        with few keys pay for bits they do not use. Raise {size_key(cfg, MIN_SPILL_SIZE)};"
              " relaxing the rate will not help.")

    rate, rate_key = bloom_rate(cfg)
    finest = BLOOM_MODULE_BITS[-1]  # the most bits per key that any rate costs
    if rate is None:
        # With no config the rate is unknown. A rate taken from the reported
        # bits/key would make the overhead 1 by construction and hide it.
        print("  bloom_false_positive_rate   unknown: no config in the bundle")
        if reported is not None and reported / finest >= 1.5:
            print(f"  overhead                    at least {reported / finest:.3f}"
                  f" (against the finest rate, {finest} bits/key)")
            small_batches()
        elif reported is not None:
            print("     overhead and savings need the rate: read bloom_false_positive_rate in the"
                  " pipeline config")
    else:
        modules = bloom_modules(rate)
        configured_bits = BLOOM_MODULE_BITS[modules]
        print(f"  bloom_false_positive_rate   {rate:g}"
              f" ({rate_key or 'default: not set by the pipeline'})")
        print(f"  configured bits/key         {configured_bits:.2f} ({modules} modules)")
        if not configured_bits:
            print("  the rate disables Bloom filters")
        elif reported is not None:
            overhead = reported / configured_bits
            # only rates coarser than the one in use can save memory
            targets = [t for t in BLOOM_TARGETS if bloom_modules(t) < modules]
            print(f"  overhead                    {overhead:.3f}")
            if overhead >= 1.5:
                small_batches()
                targets = []  # the floor, not the rate, sets the size: a coarser rate saves little
            elif targets:
                print("        Filters are sized as configured; their cost is key volume, not")
                print("        per-key overhead. Relaxing the rate is the lever that works here.")
            else:
                print(f"        Filters are sized as configured, and the rate is already {rate:g}:")
                print("        their cost is key volume, so fewer keys (less state) is the lever.")
            bloom, bloom_hosts = totals("bloom_filter_size_bytes")
            for target in targets:
                factor = bloom_bits_per_key(target) / configured_bits
                on_host = ""
                if bloom_hosts:
                    h = biggest(bloom_hosts)
                    on_host = f"; {fmt_bytes(bloom_hosts[h] * (1 - factor))} on host {h}"
                print(f"  at rate {target:g}: filters would be ~{fmt_bytes(bloom * factor)}"
                      f"  (saves {fmt_bytes(bloom * (1 - factor))}{on_host})")

    # memory vs storage residency: how much state actually spilled
    lm = sum(v for vs in node_metric(prof, "loose_memory_records_count").values() for v in vs)
    ls = sum(v for vs in node_metric(prof, "loose_storage_records_count").values() for v in vs)
    if lm + ls:
        print(f"  loose records: memory {lm:,.0f} vs storage {ls:,.0f}"
              f"  ({100.0*lm/(lm+ls):.1f}% resident)")


# --------------------------------------------------------------------------
# section: disk -- histogram percentiles, cache hit rates, stalls
# --------------------------------------------------------------------------
@functools.lru_cache(maxsize=None)
def prom_samples(path):
    """-> ((name, {label: value}, value), ...) for every sample in metrics.txt.

    A multi-host pipeline writes one series per host, labeled ordinal="<n>".
    """
    out = []
    with open(path, errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            m = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)$", line)
            if not m:
                continue
            try:
                v = float(m.group(3))
            except ValueError:
                continue
            out.append((m.group(1), dict(re.findall(r'(\w+)="([^"]*)"', m.group(2) or "")), v))
    return tuple(out)


@functools.lru_cache(maxsize=None)
def parse_prom_histograms(path):
    """-> {family: {le: cumulative_count}} plus {family: (count, sum)}.

    Buckets add up every series of every host. Count and sum are those of the
    host with the largest sum: each host observes every step and commit, so
    adding the hosts up would multiply the count by the host count.
    """
    buckets = collections.defaultdict(dict)
    per_host = collections.defaultdict(dict)  # family -> host -> [count, sum]
    for name, labels, v in prom_samples(path):
        if name.endswith("_bucket"):
            if "le" in labels:
                key = float("inf") if labels["le"] in ("inf", "+Inf") else float(labels["le"])
                fam = name[: -len("_bucket")]
                buckets[fam][key] = buckets[fam].get(key, 0) + v
        elif name.endswith(("_count", "_sum")):
            fam, _, kind = name.rpartition("_")
            per_host[fam].setdefault(labels.get("ordinal"), [0.0, 0.0])[kind == "sum"] += v
    totals = {fam: tuple(max(hosts.values(), key=lambda cs: cs[1]))
              for fam, hosts in per_host.items()}
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
    """-> {name: {host: float}}, the first sample per host; host is the ordinal label or None."""
    out = {}
    names = set(names)
    for name, labels, v in prom_samples(path):
        if name in names:
            out.setdefault(name, {}).setdefault(labels.get("ordinal"), v)
    return out


def hosts_max(gauges, name):
    """-> the largest value of a gauge on any host, or None."""
    return max(gauges[name].values()) if gauges.get(name) else None


def newest_metrics(root):
    for s in reversed(snapshots(root)):
        p = os.path.join(s, "metrics.txt")
        if os.path.exists(p):
            return s, p
    return None, None


def snapshot_metrics(root, snap):
    """-> (snapshot, metrics.txt path): the one collected with snap, else the newest."""
    if snap and os.path.exists(os.path.join(snap, "metrics.txt")):
        return snap, os.path.join(snap, "metrics.txt")
    return newest_metrics(root)


def transactions(path):
    """-> (transaction_state, highest on any host; nonzero while one is open, transactions done).

    dbsp_steps_total counts transaction steps, but some versions leave it at
    0 after many commits, so the commit count of transaction_commit_seconds
    counts too.
    """
    g = prom_gauges(path, ["transaction_state", "dbsp_steps_total"])
    _, totals = parse_prom_histograms(path)
    done = max(hosts_max(g, "dbsp_steps_total") or 0,
               totals.get("transaction_commit_seconds", (0.0, 0.0))[0])
    return hosts_max(g, "transaction_state") or 0, done


FILTER_METRICS = ("bloom_filter_size_bytes", "range_filter_size_bytes", "roaring_filter_size_bytes")


def pipeline_hosts(root, snap):
    """-> host count of the run snap describes: its config, else the ordinal labels in metrics.txt."""
    _s, cfg = snapshot_config(root, snap)
    hosts = config_hosts(cfg) if cfg else None
    if hosts:
        return hosts
    _s, mpath = snapshot_metrics(root, snap)
    ordinals = {labels["ordinal"] for _n, labels, _v in prom_samples(mpath)
                if "ordinal" in labels} if mpath else set()
    return max(len(ordinals), 1)


def heap_hosts(root, snap):
    """-> (host count, host 0's RSS bytes or None) for the snapshot a heap profile came from.

    Only that snapshot's metrics.txt counts: RSS from another snapshot is
    another moment, and maybe another run.
    """
    hosts = pipeline_hosts(root, snap)
    mpath = os.path.join(snap, "metrics.txt")
    if hosts > 1 and os.path.exists(mpath):
        rss = prom_gauges(mpath, ["process_resident_memory_bytes"]).get(
            "process_resident_memory_bytes", {})
        return hosts, rss.get("0")
    return hosts, None


def filter_bytes(root, snap, prof):
    """-> (filter bytes, scope label) over the workers that host 0's heap profile covers."""
    hosts, per, why = host_layout(root, snap, prof)
    if per:
        return (sum(host_totals(prof, mid, per)[0] for mid in FILTER_METRICS),
                f"   (host 0: workers 0..{per - 1} of {hosts * per})")
    total = sum(v for mid in FILTER_METRICS for vs in node_metric(prof, mid).values() for v in vs)
    return total, (f"   (all {hosts} hosts: {why})" if hosts > 1 else "")


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
    if tot <= 0:
        print("  no bytes sampled yet: nothing to attribute")
        return

    # Coverage: what share of the process the profile explains.
    st = load_json(os.path.join(os.path.dirname(path), "stats.json"))
    rss = None
    if st:
        rss = (st.get("global_metrics", st) or {}).get("rss_bytes")
    hosts, host0_rss = heap_hosts(root, os.path.dirname(path))
    rss_label = "RSS at collection     "
    if hosts > 1:
        # The coordinator serves ordinal 0 when no ordinal is given, and the
        # bundle collector gives none; stats.json rss_bytes sums every host.
        print(f"  multi-host ({hosts} hosts): this is host 0's profile, not the pipeline's")
        if rss:
            print(f"  RSS, sum of all hosts  {fmt_bytes(rss)}   (stats.json; not comparable)")
        rss, rss_label = (host0_rss, "RSS of host 0         ") if host0_rss else (
            rss / hosts if rss else None, "RSS, mean per host    ")
    if rss:
        print(f"  {rss_label} {fmt_bytes(rss)}   profile covers "
              f"{100.0*tot/rss:.1f}% of it")
        if tot / rss < 0.6:
            print("     >> profile explains well under two thirds of RSS: much of the")
            print("        footprint is outside the tracked allocator (mmap, page cache,")
            print("        fragmentation). Do not size the pipeline from this profile alone.")
        elif tot / rss > 1.05:
            print("     >> profile exceeds RSS: it counts allocated bytes, some of which are not")
            print("        resident (untouched or reclaimed pages), and the two were read at")
            print("        slightly different times. Read its shares, not its total.")

    print(f"  -- allocation sites, exclusive, shim skipped (top {top}) --")
    sites = by_alloc_site(samples, idx)
    ranked = sorted(sites.items(), key=lambda kv: -kv[1])
    shown = 0
    for site, v in ranked[:top]:
        shown += v
        print(f"  {fmt_bytes(v):>9} {100.0*v/tot:>6.2f}%  {site[:96]}")
    print(f"  {'':9} {100.0*shown/tot:>6.2f}%  of profiled bytes shown above")

    # Attribute to SQL through the operator hash embedded in generated symbols.
    mir = MirIndex(root, os.path.dirname(path))
    byh = by_operator_hash(samples, idx)
    if byh:
        print("  -- attributed to SQL via generated operator symbols --")
        full = {pid[:16]: pid for pid in mir.by_hash}
        # a hash with no MIR node can still be a circuit node's persistent_id: label it by the graph
        hprof = load_json(os.path.join(os.path.dirname(path), "circuit_profile.json"))
        graph_labels, by_pid = {}, {}
        if hprof:
            graph_labels = profile_labels(root, os.path.dirname(path), hprof)
            for node, ms in ((hprof.get("worker_profiles") or [{}])[0].get("metadata") or {}).items():
                for m in ms:
                    if m.get("metric_id") == "persistent_id":
                        by_pid.setdefault(str((m.get("value") or {}).get("value"))[:16], node)

        def heap_label(h):
            if mir.resolve(full.get(h))[0]:
                return mir.resolve(full.get(h))[0]
            if h in by_pid:
                return f"{by_pid[h]} {graph_labels.get(by_pid[h], '')}"
            return f"{h} (no MIR match" + (", and no circuit node has this hash)" if hprof else ")")
        rows = [(v, heap_label(h)) for h, v in byh.items()]
        for v, label in sorted(rows, reverse=True)[:top]:
            if v / tot < 0.0005:
                continue
            print(f"  {fmt_bytes(v):>9} {100.0*v/tot:>6.2f}%  {clip(label, 100)}")

    # Cross-check against the circuit profile's own filter accounting.
    psnap, prof = newest_profile(root)
    if prof:
        filt, scope = filter_bytes(root, psnap, prof)
        heap_filter = sum(v for site, v in sites.items()
                          if re.search(r"[Bb]loom|fastbloom|filter", site))
        if filt or heap_filter:
            print("  -- cross-check: filters, two independent sources --")
            print(f"  circuit_profile filter metrics  {fmt_bytes(filt)}{scope}")
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


def section_pace(root, top):
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
    g = prom_gauges(path, ["dbsp_steps_total", "records_late_total"])
    bkt, tot = parse_prom_histograms(path)
    hosts = len(g.get("dbsp_steps_total") or {})
    if hosts > 1:
        print(f"  {hosts} hosts step together: step counts are per host; n and total"
              " are of the slowest host")

    steps = hosts_max(g, "dbsp_steps_total") or 0
    txn, done = transactions(path)  # done: also counts commits where dbsp_steps_total stays 0
    print(f"  dbsp_steps_total          {steps:,.0f}   (transaction-level steps)")

    # Wall clock and circuit step count must come from per-worker maxima.
    # dbsp_runtime_elapsed_seconds_total is summed across workers, so dividing
    # it by steps overstates wall clock by roughly the worker count.
    wall = circuit_steps = None
    _, prof = newest_profile(root)
    if prof:
        wall = circuit_wall(prof)
        circuit_steps = worker_max(prof, "steps_count")
    if wall:
        print(f"  circuit wall clock        {wall:,.1f}s  (max circuit_runtime_seconds)")
    if circuit_steps:
        print(f"  circuit steps_count       {circuit_steps:,.0f}   (internal circuit steps)")
    if wall and circuit_steps:
        print(f"  seconds per circuit step  {wall/circuit_steps:,.3f}s "
              "(derived: wall clock / circuit steps)")
    if done and circuit_steps and done < circuit_steps:
        print(f"  NOTE {circuit_steps:,.0f} circuit steps map to {done:,.0f} transaction step(s):"
              " results only become visible at commit")
    for fam in ("dbsp_step_latency_seconds", "transaction_ingest_seconds",
                "transaction_commit_seconds", "feldera_checkpoint_runtime_seconds",
                "feldera_checkpoint_delay_seconds"):
        c, s = tot.get(fam, (0.0, 0.0))
        top_bucket = max_bucket(bkt.get(fam))
        if c:
            print(f"  {fam:<34} n={c:>10,.0f} mean={s/c:>12.3f}s total={s:>12.1f}s"
                  + (f"  max {fmt_range(top_bucket)}" if top_bucket else "")
                  + (f"  ({HISTOGRAM_SCOPE[fam]})" if fam in HISTOGRAM_SCOPE else ""))
        elif fam == "transaction_ingest_seconds" and fam in tot:
            print(f"  {fam:<34} n=0: no multi-step transactions (each commit is one step)")

    if txn:
        print(f"  transaction_state         {txn:.0f} (a transaction is open)")
    # 0 done counts too: an open first transaction, or circuit steps with no commit yet
    if done <= 10 and (txn or (circuit_steps or 0) > done):
        print(f"  >> only {done:,.0f} transaction step(s) recorded: this pipeline batches its")
        print( "     work into transactions, so dbsp_step_latency thresholds do NOT describe")
        print( "     it. Judge it by transaction ingest/commit duration and wall clock.")
    late = sum((g.get("records_late_total") or {}).values())  # each host drops its own
    if late:
        print(f"  records_late_total        {late:,.0f}"
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
        if wall and wall > ci and done <= 1 and (txn or done):
            print(f"  >> the open transaction has run {wall:,.0f}s against a {ci:g}s checkpoint"
                  " interval: no checkpoint can complete until it commits")
    pace_by_snapshot(root, top)


# what each latency histogram covers (controller.rs metric help, controller/stats.rs)
HISTOGRAM_SCOPE = {
    "dbsp_step_latency_seconds": "window: last 60 s or 1000 steps",
    "transaction_commit_seconds": "whole run: its max bucket bounds every commit",
    "transaction_ingest_seconds": "whole run",
}
INPUT_LATENCY = ("input_connector_processing_latency_seconds",
                 "input_connector_completion_latency_seconds")
# InputEndpointMetrics::*_latency_histogram(): SlidingHistogram::new(10_000, 600 s)
INPUT_LATENCY_WINDOW = "600 s or 10,000 samples"


def max_bucket(bkt):
    """-> (lower, upper) of the highest histogram bucket that holds a sample, or None."""
    items = sorted((bkt or {}).items())
    prev_le, prev_cum, top_bucket = 0.0, 0.0, None
    for le, cum in items:
        if cum > prev_cum:
            top_bucket = (prev_le, le)
        prev_le, prev_cum = le, cum
    return top_bucket


def bucket_tail(bkt, k=2):
    """-> [(lower, upper, samples)] of the k highest buckets that hold samples, highest last."""
    items, out = sorted((bkt or {}).items()), []
    prev_le, prev_cum = 0.0, 0.0
    for le, cum in items:
        if cum > prev_cum:
            out.append((prev_le, le, cum - prev_cum))
        prev_le, prev_cum = le, cum
    return out[-k:]


def fmt_range(b):
    """'0.1..0.2s' for a bucket (Feldera's bounds are 0.199999 and the like), '>30s' when open."""
    if not b:
        return "n/a"
    lo, hi = b
    return f">{lo:.3g}s" if hi == float("inf") else f"{lo:.3g}..{hi:.3g}s"


def histogram_series(path, family, label):
    """-> {label value: ({le: cumulative count}, count)} with the series of every host added up.

    Each host reads its own share of an input, so its samples add to the others.
    """
    out = {}
    for name, labels, v in prom_samples(path):
        if label not in labels or not name.startswith(family):
            continue
        bkt, n = out.get(labels[label], ({}, 0.0))
        if name == family + "_bucket" and "le" in labels:
            le = float("inf") if labels["le"] in ("inf", "+Inf") else float(labels["le"])
            bkt[le] = bkt.get(le, 0.0) + v
        elif name == family + "_count":
            n += v
        out[labels[label]] = (bkt, n)
    return out


def pace_by_snapshot(root, top):
    """Commit duty cycle and input latency per snapshot: each snapshot has its own windows."""
    rows = [(s, os.path.join(s, "metrics.txt")) for s in snapshots(root)
            if os.path.exists(os.path.join(s, "metrics.txt"))]
    if not rows:
        return
    newest = next(filter(None, (incarnation(s) for s, _p in reversed(rows))), None)
    commits, ingests = {}, {}  # snapshot -> max commit bucket, max ingest bucket
    print("  -- commits per snapshot (transaction_commit_seconds is cumulative for its run) --")
    print(f"  {'snapshot':<34} {'incarnation':<30} {'uptime':>8} {'commit sum':>10} {'duty':>6}"
          f" {'commits':>8} {'max commit':>12} {'ingest n':>8}")
    for s, path in rows:
        bkt, tot = parse_prom_histograms(path)
        c, total_s = tot.get("transaction_commit_seconds", (0.0, 0.0))
        inc = incarnation(s)
        uptime = inc.uptime if inc else None
        commits[s] = max_bucket(bkt.get("transaction_commit_seconds"))
        ingests[s] = max_bucket(bkt.get("transaction_ingest_seconds"))
        name = incarnation_name(inc) + (" (earlier)" if inc and newest and inc.key != newest.key
                                        else "")
        print(f"  {os.path.basename(s):<34} {name:<30} "
              + (f"{uptime:>7.1f}s" if uptime else f"{'?':>8}")
              + f" {total_s:>9.1f}s "
              + (f"{100 * total_s / uptime:>5.1f}%" if uptime else f"{'?':>6}")
              + f" {c:>8,.0f} {fmt_range(commits[s]):>12}"
              + f" {tot.get('transaction_ingest_seconds', (0.0, 0.0))[0]:>8,.0f}")
    print("  duty = commit sum / uptime (stats.json): near 100% the circuit ran commits back to back"
          " (saturated); ingest n = 0: no multi-step transactions")

    series = {s: {fam: histogram_series(path, fam, "endpoint") for fam in INPUT_LATENCY}
              for s, path in rows}
    if not any(sr for by_fam in series.values() for sr in by_fam.values()):
        return
    hosts = max(pipeline_hosts(root, s) for s, _p in rows)
    print(f"  -- input latency per endpoint: sliding windows of the last {INPUT_LATENCY_WINDOW}"
          " (controller/stats.rs), so a tail can age out of a later snapshot --")
    if hosts > 1:
        print(f"     {hosts} hosts: each endpoint adds up the series of every host")
    print(f"  {'snapshot':<34} {'endpoint':<32} {'kind':<10} {'n':>7} {'p50':>13} {'p99':>13}"
          "  tail (highest buckets)")
    verdicts = []
    for s, _path in rows:
        proc, comp = (series[s][fam] for fam in INPUT_LATENCY)
        # the slowest endpoints first: their tails decide a latency complaint
        eps = sorted(set(proc) | set(comp), key=lambda e: (
            -(max_bucket(comp.get(e, ({}, 0))[0]) or (0, 0))[1], e))
        first = True
        for ep in eps[:top]:
            for kind, by_ep in (("processing", proc), ("completion", comp)):
                if ep not in by_ep:
                    continue
                bkt, n = by_ep[ep]
                tail = ", ".join(f"{int(k):,} in {fmt_range((lo, hi))}" for lo, hi, k in bucket_tail(bkt))
                print(f"  {os.path.basename(s) if first else '':<34} {keep_end(ep, 32):<32} {kind:<10}"
                      f" {n:>7,.0f} {fmt_range(quantile_bounds(bkt, 0.5)):>13}"
                      f" {fmt_range(quantile_bounds(bkt, 0.99)):>13}  {tail or '-'}")
                first = False
        if len(eps) > top:
            print(f"  {'':<34} ... {len(eps) - top} more endpoints (--top)")
        worst = max((max_bucket(b) for b, _n in comp.values()), key=lambda b: b[0] if b else -1,
                    default=None)
        # the circuit holds a record for one transaction at most: its ingest phase, then its commit
        circuit = (commits[s][1] + (ingests[s][1] if ingests.get(s) else 0)) if commits.get(s) else None
        if worst and circuit and worst[0] >= 10 * circuit:
            verdicts.append((s, worst, circuit, bool(ingests.get(s))))
    for s, worst, circuit, multi_step in verdicts:
        print(f"  >> {os.path.basename(s)}: completion latency reaches {fmt_range(worst)}, while no"
              f" transaction of this run took over {circuit:.3g}s"
              + (" (longest ingest + longest commit):" if multi_step else " (longest commit):"))
        print("     the delay is not in the circuit. An output connector held the completed frontier"
              " (completion is the min over all outputs), or the input waited in a queue")
        backlog = input_backlog(s)
        if backlog:
            print(f"     input buffered at collection: {backlog}")
    if any(series[s][INPUT_LATENCY[0]] for s, _p in rows):
        print("  processing latency cannot clear the output side: it copies completion latency whenever"
              " an output lags")
        print("  (update_processed in crates/connectors/src/controller/stats.rs stops at the first"
              " processed but not completed record)")


def input_backlog(snap):
    """-> 'N records, about K commits of input' buffered at collection (stats.json), or None."""
    st = load_json(os.path.join(snap, "stats.json")) or {}
    g = st.get("global_metrics", st) or {}
    buffered, total = g.get("buffered_input_records"), g.get("total_input_records")
    path = os.path.join(snap, "metrics.txt")
    commits = parse_prom_histograms(path)[1].get("transaction_commit_seconds", (0.0, 0.0))[0] \
        if os.path.exists(path) else 0
    if not isinstance(buffered, (int, float)):
        return None
    per = total / commits if isinstance(total, (int, float)) and commits else None
    if not per:
        return f"{buffered:,.0f} records"
    queued = buffered / per
    return (f"{buffered:,.0f} records, about {queued:.2g} commits of input: "
            + ("less than one commit, so no queue before the circuit at that moment" if queued < 1
               else "a queue before the circuit, which also adds to completion latency"))


DISK_GAUGES = ("compaction_stall_duration_seconds", "output_stall_seconds_total",
               "storage_cache_usage_bytes", "storage_cache_usage_limit_bytes_total",
               "storage_usage_bytes", "files_created_total", "files_deleted_total",
               "memory_pressure", "memory_pressure_epoch", "max_rss_bytes",
               "process_resident_memory_bytes", "process_swap_bytes",
               "dbsp_steps_total", "records_late_total")


def section_disk(root):
    print("== disk and cache ==")
    snap, path = newest_metrics(root)
    if not path:
        print("  no metrics.txt in any snapshot")
    else:
        print(f"  from {os.path.basename(snap)}/metrics.txt")
        bkt, tot = parse_prom_histograms(path)
        g = prom_gauges(path, DISK_GAUGES)
        hosts = max((len(v) for v in g.values()), default=1)
        if hosts > 1:
            print(f"  {hosts} hosts: count and mean are of the slowest host;"
                  " the p50 / p99 buckets cover every host")
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

        # derived saturation: usage against the pipeline's own configured limit,
        # on the host nearest its limit
        def nearest(used, limit):
            pairs = [(h, u, (g.get(limit) or {}).get(h)) for h, u in (g.get(used) or {}).items()]
            return max(((h, u, c) for h, u, c in pairs if u and c),
                       key=lambda p: p[1] / p[2], default=None)

        def on_host(text, h):
            return f"{text} (host {h})" if hosts > 1 else text
        hit = nearest("storage_cache_usage_bytes", "storage_cache_usage_limit_bytes_total")
        if hit:
            h, cu, cl = hit
            print(f"  {on_host('cache saturation', h):<26} {fmt_bytes(cu)} / {fmt_bytes(cl)} "
                  f"= {100.0*cu/cl:.1f}% full")
            if cu / cl > 0.95:
                print("     >> cache is effectively full: further reads must go to disk")
        hit = nearest("process_resident_memory_bytes", "max_rss_bytes")
        if hit:
            h, rss, cap = hit
            print(f"  {on_host('rss headroom', h):<26} {fmt_bytes(rss)} / {fmt_bytes(cap)} "
                  f"= {100.0*rss/cap:.1f}% of the controller's own budget")

        # gauges worth reading directly; a spread across hosts shows as min .. max
        def num(v):
            return f"{v:.15g}"  # as written: 106798.567806227, 263303412855
        for name in DISK_GAUGES:
            per = g.get(name)
            if not per:
                continue
            lo, hi = min(per.values()), max(per.values())
            print(f"  {name:<46} " + (num(hi) if lo == hi else
                                      f"{num(lo)} .. {num(hi)} (max on host {max(per, key=per.get)})"))

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
    "sidecars": lambda a: section_sidecars(a.bundle, a.top),
    "inputs": lambda a: section_inputs(a.bundle, a.top),
    "trend": lambda a: section_trend(a.bundle),
    "time": lambda a: section_time(a.bundle, a.top),
    "skew": lambda a: section_skew(a.bundle, a.top),
    "sql": lambda a: section_sql(a.bundle, a.top),
    "materialized": lambda a: section_materialized(a.bundle, a.top),
    "memory": lambda a: section_memory(a.bundle, a.top),
    "heap": lambda a: section_heap(a.bundle, a.top),
    "types": lambda a: section_types(a.bundle),
    "pace": lambda a: section_pace(a.bundle, a.top),
    "disk": lambda a: section_disk(a.bundle),
}
ORDER = ["state", "sidecars", "errors", "inputs", "trend", "pace", "time", "skew", "sql",
         "materialized", "memory", "heap", "disk", "types"]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("bundle", help="extracted bundle directory")
    ap.add_argument("--section", action="append", metavar="NAME[,NAME...]",
                    help="repeat it or give a comma list; sections run in the order above"
                         f" (default all): {', '.join(ORDER)}")
    ap.add_argument("--top", type=int, default=10)
    a = ap.parse_args()
    asked = {s.strip() for arg in a.section or ["all"] for s in arg.split(",") if s.strip()}
    unknown = sorted(asked - set(ORDER) - {"all"})
    if unknown:
        ap.error(f"unknown section(s): {', '.join(unknown)}; choose from {', '.join(ORDER)}, all")
    if not os.path.isdir(a.bundle):
        sys.exit(f"not a directory: {a.bundle}")
    if not snapshots(a.bundle):
        sys.exit(f"no snapshot directories (named by ISO timestamp) in {a.bundle}; read a flat"
                 " bundle (<timestamp>_metrics.txt files) by hand")
    todo = [s for s in ORDER if "all" in asked or s in asked]
    failed = []
    for name in todo:
        try:
            SECTIONS[name](a)
        except Exception as e:  # one bad artifact must not hide the sections after it
            frames = traceback.extract_tb(e.__traceback__)
            here = os.path.dirname(os.path.abspath(__file__))
            where = next((f for f in reversed(frames)
                          if os.path.dirname(os.path.abspath(f.filename)) == here), frames[-1])
            print(f"  !! section {name} stopped: {type(e).__name__}: {e}"
                  f" ({os.path.basename(where.filename)}:{where.lineno} in {where.name})")
            failed.append(name)
        print()
    if failed:
        sys.exit(f"sections that stopped early: {', '.join(failed)}")


if __name__ == "__main__":
    main()
