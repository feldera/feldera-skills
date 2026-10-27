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
| filter bytes | filter bytes / `resources.memory_max`, and bytes per filter hit |
| step latency | latency vs `checkpoint_interval`, and wall clock / step count |
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
    python3 scripts/profile_diff.py before after         # cost of the work between two bundles

Sections, in the order the probe runs them: `state`, `sidecars`, `errors`,
`inputs`, `trend`, `pace`, `time`, `skew`, `sql`, `materialized`, `memory`,
`heap`, `disk`, `types`. Read `state`, `sidecars` and `pace` before anything
else; they decide which of the later sections even mean anything.

Bundles run to hundreds of MB. Never `cat logs.txt`.

## What lives where

A bundle holds several timestamped collection directories plus `metadata.txt`.
Directory names are ISO timestamps, so they sort chronologically. Do not
assume the oldest snapshot is healthy. Collection can start after the problem
did, so every snapshot can show it. Test each snapshot against the pipeline's
own budgets (`memory_max`, `checkpoint_interval`, `max_rss_bytes`)
before you call it a baseline:

| Snapshots show | Compare against | Reading |
| --- | --- | --- |
| older ones in budget, newer ones not | the last in-budget snapshot | regression; the first breach starts the failure window |
| all out of budget, flat trend | budgets, and operators and workers against each other | steady state; cause is SQL, config, or data shape, not a change over time |
| all out of budget, rising trend | budgets, plus the rate of change | growth that started before collection; the bundle has no baseline |

Older bundles use a flat layout (`<timestamp>_metrics.txt`) in place of one
directory per snapshot. `bundle_probe.py` reads only the directory layout and
stops on a flat bundle, so read a flat bundle by hand. A bundle that was
zipped again has one more directory around the snapshots; the probe opens it.
Any JSON artifact can also ship gzipped (`circuit_profile.json.gz`).

Artifacts missing from the newest snapshot ("status is currently
'unavailable'") mean the pipeline was already dead when the bundle was taken.

| File | Carries | Blind spot |
| --- | --- | --- |
| `pipeline_config.json` | status, budgets in `deployment_config` (the most recent run) and `runtime_config` (as saved), `program_code` (the SQL), `program_info` (compiled schema with a `materialized` flag per view, and the output connectors), K8s `restartCount` / `exitCode` / `OOMKilled` | `deployment_config` is null until the first start; read it first |
| `metrics.txt` | Prometheus families, aggregate plus per-`endpoint` | no `worker` or `operator` label |
| `circuit_profile.json` | per-worker, per-operator metrics in categories, plus its own metric catalog under `metrics` | large; only present while the pipeline is alive |
| `heap_profile.pb.gz` | gzipped pprof from `jemalloc_pprof`; live allocation sites, symbolized | Linux only, and a build with heap profiling enabled; whole-process, never per worker |
| `dataflow_graph.json` | `mir` nodes with `table` / `view` / `operation` / SQL `positions`, and `calcite_plan` | none |
| `stats.json` | `rss_bytes`, `storage_bytes`, `memory_pressure`, input vs processed; per input connector: buffered records and bytes, error counts, the last error messages | absent once the pipeline is down |
| `pipeline_events.json` | the runner's pod status about once a minute: each container's image digest, readiness, restarts, `lastState` (`OOMKilled`), Kubernetes pod events (kills, evictions, warnings), `connector_stats.num_errors` | only the window before each snapshot; windows overlap, so dedupe by `event_id` |
| `logs.txt` | connector errors, transaction progress | no exit code; lines are duplicated |

Feldera 0.366.0 renamed the duration settings and 0.367.0 renamed the size
settings. The unit moved from the name into the value:

| Before | After |
| --- | --- |
| `fault_tolerance.checkpoint_interval_secs: 60` | `fault_tolerance.checkpoint_interval: "1m"` |
| `resources.memory_mb_max: 16000` (MB) | `resources.memory_max: "16G"` |
| `resources.storage_mb_max`, `max_rss_mb`, `datafusion_memory_mb` (MB) | `storage_max`, `max_rss`, `datafusion_memory` |
| `storage.min_storage_bytes`, `storage.cache_mib` | `storage.min_spill_size`, `storage.cache_size` |

A config not saved since the upgrade keeps the old names, so a bundle can
carry either. The probe reads both and prints each key as the config spells
it. Use that spelling when you name a key to the user.

The probe reads each budget from `deployment_config` first, because that is
what ran. `runtime_config` can differ from it: a live resize changes only
`deployment_config`, and an edit made while the pipeline is stopped changes
`runtime_config` before the next run. When the two differ, the `state`
section prints each gap with the value that ran, the value saved, and the
cause: live resize or edit not run yet. An edit not run yet can be a fix that
the user already saved, so read it before you recommend a change. The
`memory` and `pace` sections take the config from the snapshot that their
profile or metrics came from, not from the newest snapshot.

## Problem class to signal

Full metric catalog and per-class formulas: `references/signals.md`. The
bundle also ships its own catalog (name, category, description) in
`circuit_profile.json` under `metrics`, so read that rather than trusting a
hardcoded list.

| Symptom | Sections | Decisive signal |
| --- | --- | --- |
| crash, restart loop | `state`, `sidecars` | `exitCode` 137 + `OOMKilled` in `lastState`, `restartCount` per snapshot, and the container and pod events around it |
| regression right after an upgrade | `sidecars` | any other container whose image digest changed on the same pod start; a mutable tag (`:latest`) can change it unseen |
| sidecar or UDF dependency failing | `sidecars`, `inputs` | sidecar `OOMKilled` or not ready; no `livenessProbe`; parse error messages from a preprocessor that cannot reach its sidecar (`Connection refused`) and error count steps at the same time |
| input piling up in memory | `inputs` | sum of every connector's buffer limit against `memory_max`; buffered bytes per snapshot as a share of `memory_max` |
| pipeline is slow | `pace`, `time` | mode first, then `runtime_seconds` share per operator, `exchange_wait_time_seconds` |
| skewed join or shard | `skew` | `key_distribution` excess, per-worker spread of `local_shard_records_count` |
| memory pressure | `memory`, `heap`, `trend` | share of `memory_max`; filter bytes per hit; bits-per-key overhead; `loose_memory` vs `loose_storage` |
| where memory actually went | `heap` | allocation site shares from `heap_profile.pb.gz`, and its agreement with the circuit profile |
| slow disk | `disk` | `storage_*_latency_seconds` percentiles, cache saturation, `compaction_stall_duration_seconds` |
| inefficient SQL | `sql`, `time` | join amplification, `output_redundancy_percent`, `negative_weight_count`, state with no `retainment_bounds` |
| stored copies that nothing may need | `materialized` | MATERIALIZED view with no output connector: size of its copy, HTTP listeners, views that read it |

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
statement. When the MIR has no entry, the probe shows the DBSP operator label
from `graph`.

The heap profile joins the same way: generated symbols are named
`feldera_pipe_operator_<first 16 hex of the persistent_id>`, so live heap bytes
resolve to a table and a SQL line too. That is the only route from a Rust frame
to a SQL statement, and it is what makes a heap finding actionable rather than
merely descriptive.

## Cost of one transaction

Profile counters are cumulative since the pipeline started. When every
snapshot comes after the last transaction, the bundle cannot tell what one
transaction cost or which transaction moved the records. Measure it:

1. With the pipeline idle, take a support bundle.
2. Send one transaction. To measure work done with no data, change a table
   that the suspect view does not read.
3. Take a second support bundle.
4. `python3 scripts/profile_diff.py before after`

Each argument is an extracted bundle, one snapshot directory, or a
`circuit_profile.json[.gz]`.

| Section | Shows |
| --- | --- |
| `window` | root circuit steps, wall clock of the slowest worker, commits and input records in the window |
| `recursive sub-circuits` | per recursive view: runs, iterations, wall clock, CPU, wait, operator work, records moved, upper limit on the iterations that carried data |
| `operators` | top leaves by runtime with records in and out, and the share of runtime spent by operators that got no input |

A recursive sub-circuit runs at every root step, also with no input. A row
with `rec_in` 0 and a nonzero `wall_s` is a fixed cost per step: more memory,
cores or workers do not reduce it; fewer recursive views or fewer steps do.

The script stops when the pipeline restarted between the two bundles
(`incarnation_uuid` differs, or a cumulative counter went down) or the program
changed (different operators or persistent ids). Without `stats.json` next to
both profiles, it cannot see a restart after which every counter grew past
its old value, and it prints a note.

Operators of a recursive view are children of a MIR node with operation
`nested`. Both scripts index them and name the owning view from the SQL line.
`bundle_probe.py` never ranks the sub-circuit as a whole; `profile_diff.py`
does.

Cache-miss time (`foreground_cache_misses` elapsed) is charged to the trace
node (`Z1 (trace)`) that owns the blocks, but the time passes in the operator
that reads them. A slow operator with no input next to a trace with high miss
time is a read problem: compare the block size on a miss with the cache shard
(`cache_size` / workers / 256 on the S3-FIFO cache; `cache_mib` before
0.367.0).

## Materialized views with no output connector

`CREATE MATERIALIZED VIEW` keeps a full copy of the view. Only these uses need
the copy:

| Use | What the bundle shows |
| --- | --- |
| Ad-hoc `SELECT` queries | nothing; ask |
| `send_snapshot` on an output connector | the connector in `program_info.output_connectors` |
| `send_snapshot=true` on an HTTP egress request | `<view>.api-<uuid>` outputs in `stats.json` show a listener, not the flag; ask |
| Debugging | nothing; ask |

Another view that reads it does not need the copy. The `materialized` section
lists each materialized view with no output connector, the size of its copy
in memory and on disk, its HTTP listeners and the views that read it. For each one, ask whether it
must stay materialized. If not, `CREATE VIEW` keeps it available to HTTP
egress, and `CREATE LOCAL VIEW` also removes it as an output.

The copy is the `Z1 (trace)` behind an `Output` node whose `persistent_id` is
`<hash of the view's MIR inspect node>.integral`. The traces of the view's own
joins and aggregates are separate, so the size shown is what MATERIALIZED adds.

## Traps that produce confident wrong answers

- Silent zeros. Measurement values come in many type envelopes
  (`count`, `bytes`, `percent`, `duration`, `cachecounts`, composite stats,
  ...; see `references/signals.md`). An extractor that misses `bytes` reports memory as 0, which reads as
  healthy rather than as an error. Run `--section types` on any new bundle.
- Cluster double-counting. `graph["nodes"]` is the root cluster object,
  not a list. Root and cluster nodes carry the sum of their children, so
  including them puts the root at exactly 50% of every total. Rank leaves only.
- Worker-summed metrics. `dbsp_runtime_elapsed_seconds_total` and the
  profile's `*_seconds` sums span all workers. Wall clock is the per-worker
  max (`circuit_runtime_seconds`), not the sum.
- Host-summed metrics. A multi-host pipeline writes one `metrics.txt` series
  per host (label `ordinal`), and each host counts every step and commit.
  Adding the hosts multiplies those counts by the host count. The probe shows
  a gauge as min .. max across hosts, and a histogram count from the slowest
  host.
- Ratios at low volume. `max/mean` equals the worker count whenever one
  record lands on one worker. Rank by `max - mean`, and report the ratio only
  alongside the volume.
- Mid-transaction profiles. In transaction mode the work happens at
  commit, so a profile taken while a transaction is open shows zero join and
  output metrics. Check `transaction_state`, `dbsp_steps_total` and the count
  of `transaction_commit_seconds` before concluding a join is idle. Some
  versions keep `dbsp_steps_total` at 0 after many commits.
- Two step counters. `dbsp_steps_total` counts transaction steps;
  `steps_count` in the profile counts circuit steps. They can differ by a
  large factor. Per-step latency thresholds describe neither on their own.
- Retainment bounds that are not set. `retainment_bounds` shows each bound
  as `None` until a waterline arrives, and the lowest bound applies. A trace
  with any `None` bound keeps every record, so the `sql` section lists it as
  having no bound.
- Duplicated log lines. Bundle logs can repeat each line several times.
  Count unique lines, not lines.
- pprof's flat column is useless here. `heap_profile.pb.gz` comes from
  `jemalloc_pprof`, and every stack begins with a four-frame allocator shim
  (`prof_backtrace_impl`, `_rjem_je_*`, `do_rallocx`). So `go tool pprof -top`
  charges 100% flat to `prof_backtrace_impl` and every root frame reads 100%
  cumulative. Skip the shim and the next frame is the real allocation site;
  `scripts/pprof_min.py` does this via `alloc_site`.
- Truncated log windows. The manager's buffer discards lines
  ("N prior log lines were discarded"), so an older snapshot can be missing
  the middle of its window. Absence of an error there is not evidence.
- Confounded upgrades. A pod start pulls every image again. A sidecar on a
  mutable tag can change on the same restart as the Feldera upgrade, and then
  the upgrade gets the blame. Before you credit a version change, read the
  `sidecars` image table for every container that changed.
- Per-connector buffer limits. Feldera pauses a connector only when its own
  buffer is full (`max_queued_records`, default 1,000,000, and
  `max_queued_size`, `max_queued_bytes` before 0.367.0, default 1000 x
  records). Nothing caps the sum, so 40 connectors at the defaults may buffer
  40 GB. The `inputs` section compares that sum with `memory_max`.
- A healthy-looking sidecar. With only a `startupProbe`, a sidecar that hangs
  stays `ready` with 0 restarts. Readiness cannot rule out a hang; parse errors
  and buffered input can.

## Bloom filter sizing

The `memory` section of the probe uses this maths:

- `bloom_bits_per_key(fpr) = 4.8 * log10(1 / fpr)`, so the default 1e-4 costs
  19.2 bits per key. `resize factor = bits(target) / bits(current)` gives what
  relaxing the rate would save.
- `bits per key overhead = reported / configured`. At 1.5 or above, the filters
  are bigger than the rate implies because `fastbloom` floors every filter at
  512 bits, so batches too small to need one still pay. The knob is
  `min_spill_size` (`min_storage_bytes` before 0.367.0), not the
  false-positive rate. Getting this backwards is
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
