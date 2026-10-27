#!/usr/bin/env python3
"""Diff two circuit profiles to get the cost of the work done between them.

Profile counters are cumulative since the pipeline started, so one profile
cannot tell what a single transaction cost. Take a bundle, run one
transaction, take a second bundle, and diff the two.

Usage:
    profile_diff.py <before> <after> [--top N]

<before> and <after> are each an extracted bundle (its newest snapshot with a
profile is used), one snapshot directory, or a circuit_profile.json[.gz].
"""
import argparse
import collections
import os
import sys

try:
    from bundle_probe import (MirIndex, artifact_path, load_json, numeric,
                              parse_prom_histograms, snapshots)
except ImportError:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from bundle_probe import (MirIndex, artifact_path, load_json, numeric,
                              parse_prom_histograms, snapshots)

ROOT = "n"


def locate(path):
    """-> (bundle or None, snapshot dir, profile path) for a bundle, a snapshot or a profile file.

    Only a bundle has sibling snapshots to read the MIR and SQL from when the
    profile's own snapshot lacks them.
    """
    if os.path.isfile(path):
        return None, os.path.dirname(os.path.abspath(path)), path
    here = artifact_path(os.path.join(path, "circuit_profile.json"))
    if here:
        return None, path, here
    for s in reversed(snapshots(path)):
        p = artifact_path(os.path.join(s, "circuit_profile.json"))
        if p:
            return path, s, p
    return None, None, None


def tree(prof):
    """-> ({node: parent cluster}, {cluster ids}, {leaf id: dbsp label})."""
    parent, clusters, leaves = {}, set(), {}

    def walk(entry, up):
        if "Simple" in entry:
            s = entry["Simple"] or {}
            parent[s.get("id")] = up
            leaves[s.get("id")] = s.get("label", "")
            return
        # tagged {"Cluster": {...}} or the bare root cluster object
        c = entry.get("Cluster", entry) or {}
        parent[c.get("id")] = up
        clusters.add(c.get("id"))
        for child in c.get("nodes", []) or []:
            walk(child, c.get("id"))

    walk((prof.get("graph") or {}).get("nodes") or {}, None)
    return parent, clusters, leaves


def readings(prof):
    """-> {node: {metric_id: [per-worker measurement]}}."""
    out = collections.defaultdict(lambda: collections.defaultdict(list))
    for w in prof.get("worker_profiles", []):
        for node, ms in (w.get("metadata") or {}).items():
            for m in ms:
                out[node][m.get("metric_id")].append(m.get("value"))
    return out


def amount(value, field=None):
    """numeric(), plus the elapsed time inside cachecounts, which numeric() cannot read."""
    if field == "elapsed" and isinstance(value, dict) and value.get("type") == "cachecounts":
        e = (value.get("value") or {}).get("elapsed") or {}
        return e.get("secs", 0) + e.get("nanos", 0) / 1e9
    return numeric(value, field)


class Delta:
    """Per-worker difference of cumulative counters between two profiles."""

    def __init__(self, before, after):
        self.b, self.a = readings(before), readings(after)

    def reports(self, node, metric):
        return metric in self.a.get(node, {})

    def workers(self, node, metric, field=None):
        a = [amount(v, field) for v in self.a.get(node, {}).get(metric, [])]
        b = [amount(v, field) for v in self.b.get(node, {}).get(metric, [])]
        if not a or None in a:
            return []
        b += [0.0] * (len(a) - len(b))  # node absent before: it started at 0
        return [x - (y or 0.0) for x, y in zip(a, b)]

    def total(self, node, metric, field=None):
        return sum(self.workers(node, metric, field))

    def peak(self, node, metric, field=None):
        return max(self.workers(node, metric, field), default=0.0)

    def mean(self, node, metric, field=None):
        """Mean per worker, or None when the profile does not report the metric."""
        w = self.workers(node, metric, field)
        return sum(w) / len(w) if w else None

    def went_down(self, nodes):
        """True when a cumulative counter is lower in <after> on some worker."""
        return any(x < 0 for n in nodes for metric in ("steps_count", "runtime_seconds")
                   for x in self.workers(n, metric))


def secs(x, form="{:.2f}"):
    """Seconds, or n/a when the profile does not report the metric: never a silent 0."""
    return "n/a" if x is None else form.format(x)


def pid_of(rd, node):
    for v in rd.get(node, {}).get("persistent_id", []):
        if isinstance(v, dict) and isinstance(v.get("value"), str):
            return v["value"]
    return None


def inside(parent, node, cluster):
    while node:
        if node == cluster:
            return True
        node = parent.get(node)
    return False


def window_facts(snap):
    """-> {name: value} from stats.json and metrics.txt next to a profile, if present."""
    out = {}
    stats = load_json(os.path.join(snap, "stats.json")) or {}
    g = stats.get("global_metrics") or {}
    for k in ("incarnation_uuid", "total_processed_records"):
        if k in g:
            out[k] = g[k]
    metrics = os.path.join(snap, "metrics.txt")
    if os.path.exists(metrics):
        _buckets, totals = parse_prom_histograms(metrics)
        if "transaction_commit_seconds" in totals:
            out["commits"], out["commit_seconds"] = totals["transaction_commit_seconds"]
    return out


def section_window(d, before, after, fb, fa):
    print("== window ==")
    print(f"  before  {before}\n  after   {after}")
    steps = d.peak(ROOT, "steps_count")
    wall = d.peak(ROOT, "circuit_runtime_seconds")
    print(f"  root circuit steps          +{steps:,.0f}")
    print(f"  wall clock (slowest worker) +{wall:.2f}s"
          f"   cpu {secs(d.mean(ROOT, 'circuit_cpu_time_seconds'), '+{:.2f}s')}"
          f"   wait {secs(d.mean(ROOT, 'circuit_wait_time_seconds'), '+{:.2f}s')}  (mean per worker)")
    if "commits" in fa and "commits" in fb:
        n = fa["commits"] - fb["commits"]
        s = fa["commit_seconds"] - fb["commit_seconds"]
        print(f"  transactions committed      +{n:,.0f}" + (f"   commit time +{s:.2f}s" if n else ""))
    if "total_processed_records" in fa and "total_processed_records" in fb:
        print(f"  input records processed     +{fa['total_processed_records'] - fb['total_processed_records']:,}")
    return steps, wall


def section_recursive(d, parent, clusters, leaves, mir, wall):
    nested = sorted(c for c in clusters if c != ROOT and d.reports(c, "steps_count"))
    print("== recursive sub-circuits (each runs inside a root step) ==")
    if not nested:
        print("  none in this circuit")
        return
    print(f"  {'cluster':<9} {'runs':>6} {'iters':>7} {'it/run':>6} {'wall_s':>8} {'share':>6} "
          f"{'cpu_s':>7} {'wait_s':>7} {'op_s':>7} {'rec_in':>9} {'data_it<=':>9}  views")
    idle_wall = 0.0
    idle = 0
    for c in nested:
        runs = d.peak(c, "invocations_count")
        iters = d.peak(c, "steps_count")
        cwall = d.peak(c, "circuit_runtime_seconds")
        inner = [n for n in leaves if inside(parent, n, c)]
        workers = max(len(d.workers(c, "steps_count")), 1)
        op = sum(d.total(n, "runtime_seconds") for n in inner) / workers
        # busiest inner operator that runs on every iteration: its records and
        # batches bound the iterations that carried data (accumulators run less often)
        every = d.total(c, "steps_count")
        moved, rec, batches = 0.0, 0.0, 0.0
        for n in inner:
            if not d.reports(n, "input_batches_stats"):
                continue
            r = d.total(n, "input_batches_stats", "total_records_count")
            b = d.total(n, "input_batches_stats", "batches_count")
            moved = max(moved, r)
            if b == every and r >= rec:
                rec, batches = r, b
        bound = f"{100 * min(rec, batches) / batches:.1f}%" if batches else "n/a"
        views = collections.Counter()
        for n in inner:
            line = mir.line_of(pid_of(d.a, n))
            view = line and mir.owning_view(line)
            if view:
                views[view] += 1
        names = ", ".join(v for v, _ in views.most_common(2)) or "-"
        share = f"{100 * cwall / wall:.0f}%" if wall else "n/a"
        print(f"  {c:<9} {runs:>6,.0f} {iters:>7,.0f} {iters / runs if runs else 0:>6.1f} {cwall:>8.2f} {share:>6} "
              f"{secs(d.mean(c, 'circuit_cpu_time_seconds')):>7} {secs(d.mean(c, 'circuit_wait_time_seconds')):>7} "
              f"{op:>7.2f} {moved:>9,.0f} {bound:>9}  {names[:60]}")
        if runs and moved == 0:
            idle += 1
            idle_wall += cwall
    print("  wall_s, share: slowest worker. cpu_s, wait_s, op_s: mean per worker. rec_in: input records")
    print("  of the busiest inner operator, all workers. data_it<=: share of iterations in which the busiest")
    print("  operator that runs every iteration saw data, assuming one record per batch (an upper limit).")
    if idle:
        print(f"  >> {idle} of {len(nested)} sub-circuits ran but moved no records: "
              f"{idle_wall:.2f}s of wall clock (slowest worker, summed)")


def section_operators(d, leaves, mir, top):
    print("== operators by runtime in the window (leaves only) ==")
    rt = {n: d.total(n, "runtime_seconds") for n in leaves}
    total = sum(rt.values())
    counted = [n for n in leaves if d.reports(n, "input_batches_stats")]
    no_input = sum(rt[n] for n in counted if d.total(n, "input_batches_stats", "total_records_count") == 0)
    with_counts = sum(rt[n] for n in counted)
    print(f"  total operator runtime +{total:.2f}s (all workers)")
    if with_counts:
        print(f"  operators that report record counts: {with_counts:.2f}s; of that with no input records: "
              f"{no_input:.2f}s ({100 * no_input / with_counts:.0f}%)")
    print(f"  {'node':<14} {'sum_s':>8} {'max_w_s':>8} {'runs':>7} {'rec_in':>9} {'rec_out':>9} {'miss_s':>7}  label")
    for n in sorted(leaves, key=lambda n: -rt[n])[:top]:
        if rt[n] <= 0:
            break
        rin = d.total(n, "input_batches_stats", "total_records_count") if d.reports(n, "input_batches_stats") else None
        rout = d.total(n, "output_batches_stats", "total_records_count") if d.reports(n, "output_batches_stats") else None
        miss = d.total(n, "foreground_cache_misses", "elapsed")
        fmt = lambda x: "-" if x is None else f"{x:,.0f}"
        print(f"  {n:<14} {rt[n]:>8.2f} {d.peak(n, 'runtime_seconds'):>8.2f} {d.peak(n, 'invocations_count'):>7,.0f} "
              f"{fmt(rin):>9} {fmt(rout):>9} {miss:>7.2f}  {(mir.resolve(pid_of(d.a, n))[0] or leaves[n])[:70]}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("before", help="bundle, snapshot directory or circuit_profile.json[.gz]")
    ap.add_argument("after", help="bundle, snapshot directory or circuit_profile.json[.gz]")
    ap.add_argument("--top", type=int, default=15)
    a = ap.parse_args()

    (_, sb, pb), (ra, sa, pa) = locate(a.before), locate(a.after)
    for arg, p in ((a.before, pb), (a.after, pa)):
        if not p:
            sys.exit(f"no circuit_profile.json[.gz] found in {arg}")
    before, after = load_json(pb), load_json(pa)
    if not (before and after):
        sys.exit("could not read both profiles")

    fb, fa = window_facts(sb), window_facts(sa)
    if fb.get("incarnation_uuid") and fa.get("incarnation_uuid") \
            and fb["incarnation_uuid"] != fa["incarnation_uuid"]:
        sys.exit("the pipeline restarted between the two profiles (incarnation_uuid differs): "
                 "its counters started again from 0, so a diff means nothing")
    # node ids are positions in the circuit, so compare what sits at each one
    parent, clusters, leaves = tree(after)
    d = Delta(before, after)
    if leaves != tree(before)[2] or any(pid_of(d.b, n) != pid_of(d.a, n) for n in leaves):
        sys.exit("the two profiles come from different circuits: the program changed between them")
    if d.went_down([ROOT, *leaves]):
        sys.exit("a cumulative counter is lower in <after> than in <before>: the arguments are"
                 " swapped, or the pipeline restarted between the two profiles")
    steps, wall = section_window(d, pb, pa, fb, fa)
    if not (fb.get("incarnation_uuid") and fa.get("incarnation_uuid")):
        print("  NOTE no stats.json next to a profile: a restart between the two cannot be ruled"
              " out, and after one the diff mixes two runs")
    print()
    if steps == 0:
        print("  no root circuit steps between the two profiles: nothing ran in this window")
        return
    mir = MirIndex(ra, sa)
    section_recursive(d, parent, clusters, leaves, mir, wall)
    print()
    section_operators(d, leaves, mir, a.top)


if __name__ == "__main__":
    main()
