---
name: support-bundle-triage
description: >-
  Diagnose a Feldera support bundle across all failure classes: crashes and
  OOM, slow pipelines, skewed joins and shards, memory pressure, slow disk and
  cache thrash, and inefficient SQL. Derives every threshold from the bundle
  itself (share of the pipeline's own budget, per-worker excess, cost per
  benefit, trend across snapshots) instead of hardcoded constants, and
  resolves hot operators back to the SQL line that created them. Use when
  handed a *-support-bundle.zip or asked why a pipeline is slow, crashed, is
  using too much memory, or is skewed.
allowed-tools: Bash Read
argument-hint: "SUPPORT_BUNDLE_ZIP"
license: MIT
compatibility: Requires Python 3 (standard library only) and unzip.
metadata:
  author: Feldera
  version: "1.0.0"
---

# Support bundle triage

## The rule that matters

Never judge a bundle against an absolute number. Every useful threshold is
already in the bundle:

| Instead of a fixed limit on | Derive |
| --- | --- |
| filter bytes | filter bytes / `resources.memory_mb_max`, and bytes per filter hit |
| step latency | latency vs `checkpoint_interval_secs`, and wall clock / step count |
| skew ratio (max / mean) | `max - mean` in the metric's own units, ranked by excess |
| state record count | share of that metric's grand total across all operators |
| RSS | trend across snapshots, plus RSS / the controller's own `max_rss_bytes` |

A fixed limit is wrong in both directions. A 64 GB filter limit is 25% of RAM
on a 256 GB pipeline and can never fire on a 32 GB one, so the same limit
over-fires on one and never fires on the other.

## Start here

    unzip -q BUNDLE.zip -d bundle
    python3 scripts/bundle_probe.py bundle              # every section
    python3 scripts/bundle_probe.py bundle --section skew --top 20

Sections, in the order the probe runs them: `state`, `errors`, `trend`,
`pace`, `time`, `skew`, `sql`, `memory`, `heap`, `disk`, `types`. Read `state`
and `pace` before anything else; they decide which of the later sections even
mean anything.

Bundles run to hundreds of MB. Never `cat logs.txt`.

## What lives where

A bundle holds several timestamped collection directories plus `metadata.txt`.
Directory names are ISO timestamps, so they sort chronologically. Do not
assume the oldest snapshot is healthy. Collection can start after the problem
did, so every snapshot can show it. Test each snapshot against the pipeline's
own budgets (`memory_mb_max`, `checkpoint_interval_secs`, `max_rss_bytes`)
before you call it a baseline:

| Snapshots show | Compare against | Reading |
| --- | --- | --- |
| older ones in budget, newer ones not | the last in-budget snapshot | regression; the first breach starts the failure window |
| all out of budget, flat trend | budgets, and operators and workers against each other | steady state; cause is SQL, config, or data shape, not a change over time |
| all out of budget, rising trend | budgets, plus the rate of change | growth that started before collection; the bundle has no baseline |

Older bundles use a flat layout (`<timestamp>_metrics.txt`) in place of one
directory per snapshot. `bundle_probe.py` reads only the directory layout, so
read a flat bundle by hand. Any JSON artifact can also ship gzipped
(`circuit_profile.json.gz`).

Artifacts missing from the newest snapshot ("status is currently
'unavailable'") mean the pipeline was already dead when the bundle was taken.

| File | Carries | Blind spot |
| --- | --- | --- |
| `pipeline_config.json` | status, `runtime_config` budgets, `program_code` (the SQL), K8s `restartCount` / `exitCode` / `OOMKilled` | none; read it first |
| `metrics.txt` | Prometheus families, aggregate plus per-`endpoint` | no `worker` or `operator` label |
| `circuit_profile.json` | per-worker, per-operator metrics in categories, plus its own metric catalog under `metrics` | large; only present while the pipeline is alive |
| `heap_profile.pb.gz` | gzipped pprof from `jemalloc_pprof`; live allocation sites, symbolized | Linux only, and a build with heap profiling enabled; whole-process, never per worker |
| `dataflow_graph.json` | `mir` nodes with `table` / `view` / `operation` / SQL `positions`, and `calcite_plan` | none |
| `stats.json` | `rss_bytes`, `storage_bytes`, `memory_pressure`, input vs processed | absent once the pipeline is down |
| `logs.txt` | connector errors, transaction progress | no exit code; lines are duplicated |

## Problem class to signal

Full metric catalog and per-class formulas: `references/signals.md`. The
bundle also ships its own catalog (name, category, description) in
`circuit_profile.json` under `metrics`, so read that rather than trusting a
hardcoded list.

| Symptom | Sections | Decisive signal |
| --- | --- | --- |
| crash, restart loop | `state` | `exitCode` 137 + `OOMKilled` in `lastState`, `restartCount` per snapshot |
| pipeline is slow | `pace`, `time` | mode first, then `runtime_seconds` share per operator, `exchange_wait_time_seconds` |
| skewed join or shard | `skew` | `key_distribution` excess, per-worker spread of `local_shard_records_count` |
| memory pressure | `memory`, `heap`, `trend` | share of `memory_mb_max`; filter bytes per hit; bits-per-key overhead; `loose_memory` vs `loose_storage` |
| where memory actually went | `heap` | allocation site shares from `heap_profile.pb.gz`, and its agreement with the circuit profile |
| slow disk | `disk` | `storage_*_latency_seconds` percentiles, cache saturation, `compaction_stall_duration_seconds` |
| inefficient SQL | `sql`, `time` | join amplification, `output_redundancy_percent`, `negative_weight_count`, state with no `retainment_bounds` |

## Hot operator to SQL line

The chain that makes findings actionable:

    circuit_profile node id
      -> persistent_id "<hash>.<role>"
      -> dataflow_graph.json mir node
      -> table / view / operation + positions.start_line_number
      -> that line of pipeline_config.json program_code

The probe prints resolved labels already, in the form
`<table or view> [<operation>] sql:<line>`. The hash before the dot groups every
operator belonging to one SQL construct, so group by it to get cost per SQL
statement. When the MIR has no entry, fall back to the DBSP operator label
from `graph`.

The heap profile joins the same way: generated symbols are named
`feldera_pipe_operator_<first 16 hex of the persistent_id>`, so live heap bytes
resolve to a table and a SQL line too. That is the only route from a Rust frame
to a SQL statement, and it is what makes a heap finding actionable rather than
merely descriptive.

## Traps that produce confident wrong answers

- **Silent zeros.** Measurement values come in many type envelopes
  (`count`, `bytes`, `percent`, `duration`, `cachecounts`, composite stats,
  ...; see `references/signals.md`). An extractor that misses `bytes` reports memory as 0, which reads as
  healthy rather than as an error. Run `--section types` on any new bundle.
- **Cluster double-counting.** `graph["nodes"]` is the root cluster object,
  not a list. Root and cluster nodes carry the sum of their children, so
  including them puts the root at exactly 50% of every total. Rank leaves only.
- **Worker-summed metrics.** `dbsp_runtime_elapsed_seconds_total` and the
  profile's `*_seconds` sums span all workers. Wall clock is the per-worker
  *max* (`circuit_runtime_seconds`), not the sum.
- **Ratios at low volume.** `max/mean` equals the worker count whenever one
  record lands on one worker. Rank by `max - mean`, and report the ratio only
  alongside the volume.
- **Mid-transaction profiles.** In transaction mode the work happens at
  commit, so a profile taken while a transaction is open shows zero join and
  output metrics. Check `transaction_state` and `dbsp_steps_total` before
  concluding a join is idle.
- **Two step counters.** `dbsp_steps_total` counts transaction steps;
  `steps_count` in the profile counts circuit steps. They can differ by a
  large factor. Per-step latency thresholds describe neither on their own.
- **Duplicated log lines.** Bundle logs can repeat each line several times.
  Count unique lines, not lines.
- **pprof's flat column is useless here.** `heap_profile.pb.gz` comes from
  `jemalloc_pprof`, and every stack begins with a four-frame allocator shim
  (`prof_backtrace_impl`, `_rjem_je_*`, `do_rallocx`). So `go tool pprof -top`
  charges 100% flat to `prof_backtrace_impl` and every root frame reads 100%
  cumulative. Skip the shim and the next frame is the real allocation site;
  `scripts/pprof_min.py` does this via `alloc_site`.
- **Truncated log windows.** The manager's buffer discards lines
  ("N prior log lines were discarded"), so an older snapshot can be missing
  the middle of its window. Absence of an error there is not evidence.

## Bloom filter sizing

The `memory` section of the probe uses this maths:

- `bloom_bits_per_key(fpr) = 4.8 * log10(1 / fpr)`, so the default 1e-4 costs
  19.2 bits per key. `resize factor = bits(target) / bits(current)` gives what
  relaxing the rate would save.
- `bits per key overhead = reported / configured`. At 1.5 or above, the filters
  are bigger than the rate implies because `fastbloom` floors every filter at
  512 bits, so batches too small to need one still pay. The knob is
  `min_storage_bytes`, not the false-positive rate. Getting this backwards is
  the common mistake.
- Heap bloom bytes are the headline figure and circuit filter metrics the
  fallback; their ratio should sit slightly above 1. Far outside roughly
  0.8 to 1.35 means one side is counting something else.
- Weight the mean reported bits per key by filter size. An unweighted mean
  lets many tiny idle filters outvote the one filter that holds the memory.

## Output

Lead with the one-sentence cause, then:

- What the signal means mechanically, citing `file:line` for code claims and
  the metric name for data claims.
- Why this pipeline hit it, naming the config that made it fatal.
- A UTC timeline table from first symptom to terminal event.
- Remediation ranked by time to unblock: pasteable config first, diagnostic
  queries second, product fixes last.

Separate what the evidence proves from what it merely suggests. When two
causes ran concurrently, rank them and say which dominates rather than
merging them. Name any bundle artifact that was missing, truncated, or
duplicated, because whoever checks your work will hit the same gap.
