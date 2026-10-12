# Signal catalog and derived formulas

Two sources. `metrics.txt` is Prometheus text, aggregate plus per-`endpoint`,
with no worker or operator label. `circuit_profile.json` is per-worker and
per-operator: `worker_profiles` is a list of one entry per worker, each with
`metadata: {node_id: [{metric_id, value}, ...]}`.

The profile ships its own catalog under `metrics` as
`{name, category, advanced, description}`. Read it instead of hardcoding
names; it is the authority for what a given platform version emits.

## Value envelopes

`value` is one of:

| type | shape | numeric reading |
| --- | --- | --- |
| `count`, `int`, `bytes` | `{value: N}` | N |
| `float` | `{value: N}` | N |
| `percent` | `{value: {numerator, denominator}}` | 100 * num / den, guard den == 0 |
| `duration` | `{value: {secs, nanos}}` | secs + nanos / 1e9 |
| `bool` | `{value: true|false}` | 1 or 0 |
| `cachecounts` | `{value: {count, bytes, elapsed}}` | pick a sub-field |
| composite `*_stats` | no `type`; sub-keys are measurements | pick `total_records_count` etc. |
| `string` | `{value: "..."}` | not numeric; used for `persistent_id`, `mir_node`, `balancer_policy` |
| LIST | array of measurements | `key_distribution`, one entry per worker |

Anything unhandled returns nothing and a caller that sums it reports 0.

## Categories in the profile

`Time`, `State`, `Balancer`, `Cache`, `Inputs`, `Outputs`, `Multihost`.
The set can change between platform versions; the catalog under `metrics`
is the authority.

## Per class

### Slow pipeline

Establish the mode first. `transaction_state` non-zero plus a tiny
`dbsp_steps_total` means the workload batches into transactions, and per-step
latency describes nothing.

| Quantity | Formula |
| --- | --- |
| wall clock | `max` over workers of `circuit_runtime_seconds` |
| circuit steps | `max` over workers of `steps_count` |
| seconds per circuit step | wall clock / circuit steps |
| operator share of runtime | node `runtime_seconds` sum / grand total |
| blocked on exchange | `exchange_wait_time_seconds` sum / `circuit_runtime_seconds` sum |
| checkpoint keeping up | mean `feldera_checkpoint_runtime_seconds` vs `checkpoint_interval_secs` |
| ingest keeping up | `total_input_records - total_processed_records`, as a share of input |

Related: `circuit_idle_time_seconds`, `circuit_wait_time_seconds`,
`nonblocking_percent`, `merge_backpressure_wait_time_seconds`,
`output_stall_seconds_total`, `compaction_stall_duration_seconds`,
`transaction_ingest_seconds`, `transaction_commit_seconds`.

### Skewed join or shard

`key_distribution` is an array with one bucket per worker, so it is a
histogram of the shard split and needs no external baseline.

| Quantity | Formula |
| --- | --- |
| excess (rank by this) | `max(dist) - mean(dist)`, in keys or records |
| ratio (report alongside) | `max(dist) / mean(dist)`; equals worker count at trivial volume |
| per-worker spread | same two figures over one node's values across `worker_profiles` |

Apply to `local_shard_records_count`, `state_records_count`,
`used_memory_bytes`, `runtime_seconds`. Then read `balancer_policy`,
`rebalancings_count`, `total_rebalancing_time_seconds`,
`rebalancing_in_progress_bool`, `accumulator_records_to_repartition_count`,
`integral_records_to_repartition_count`.

A `shard` policy with a high excess on one operator points at a join or
`GROUP BY` key with a dominant value. Resolve the node to its SQL line and
read the key expression.

### Memory pressure

Denominators: `resources.memory_mb_max`, `resources.storage_mb_max`,
`max_rss_bytes` (what the controller actually budgeted), and
`datafusion_memory_mb` for connector-side sorts.

| Quantity | Formula |
| --- | --- |
| filter cost | (`bloom` + `range` + `roaring`)`_filter_size_bytes` / memory budget |
| filter benefit | `*_filter_hits_count` / (hits + misses); and size / hits as bytes per hit |
| operator state share | node `used_memory_bytes` / memory budget |
| residency | `loose_memory_records_count` / (memory + `loose_storage_records_count`) |
| on-disk state | `spine_storage_size_bytes`, `merging_size_bytes` / storage budget |
| RSS headroom | `process_resident_memory_bytes` / `max_rss_bytes` |

A filter that is large and rarely hits is pure waste; neither a size
threshold nor a hit-rate threshold finds it alone. Charge on-disk metrics to
the storage budget, or they manufacture readings above 100%.

When `max_rss_mb` is unset the controller derives its budget from the pod
limit, which can leave little headroom. A single large
untracked allocation then kills the pod.

### Slow disk and cache thrash

Histograms give bucket bounds, not point values. Report the bucket that
contains the quantile.

| Quantity | Formula |
| --- | --- |
| read / write / sync latency | `storage_{read,write,sync}_latency_seconds` p50 and p99 buckets |
| block sizes | `storage_{read,write}_block_bytes` mean = sum / count |
| cache saturation | `storage_cache_usage_bytes` / `storage_cache_usage_limit_bytes_total` |
| cache effectiveness | `background_cache_hit_rate_percent`, `foreground_cache_hit_rate_percent` |
| merge churn | `files_created_total`, `files_deleted_total`, `completed_merges`, `merge_reduction_percent` |
| stalls | `compaction_stall_duration_seconds`, `merge_backpressure_wait_time_seconds` |

Fast per-operation latency with a saturated cache and a low background hit
rate means the volume of I/O is the problem, not the device. Say which.

### Inefficient SQL

| Quantity | Formula |
| --- | --- |
| join amplification | `computed_output_records_count` / (`left_input_records_count` + `right_input_integral_records_count`) |
| wasted downstream work | `output_redundancy_percent` |
| retraction churn | `negative_weight_count` |
| unbounded state | large `state_records_count` on nodes with no `retainment_bounds`; candidates for `LATENESS` or a window |
| dropped input | `records_late_total` |
| batch shape | `input_batches_stats` / `output_batches_stats` min, max, avg, total |

Resolve every finding to its SQL line through `persistent_id` and the MIR, and
quote the statement. An amplification figure without the join predicate is
not actionable.

### Multihost

`exchange_serialized_bytes`, `exchange_deserialized_bytes`,
`exchange_serialization_time_seconds`,
`exchange_deserialization_time_seconds`,
`multihost_exchange_messages_received_total`,
`multihost_duplicate_exchange_messages_received_total`. Compare exchange time
to total runtime before blaming the network.

## Connector health

Per-`endpoint` in `metrics.txt`: `input_connector_records_total`,
`input_connector_bytes_total`, `input_connector_buffered_records`,
`input_connector_errors_{parse,transport}_total`,
`input_connector_{processing,completion}_latency_seconds`,
`input_connector_extra_memory_bytes`, `input_connector_barrier`,
`input_connector_running`, `input_connector_end_of_input`, and for Delta
`input_connector_delta_{phase,last_ingested_version,catchup_target_version,snapshot_records_total}`.

Buffered records pinned at a connector's `max_queued_records` means the
circuit, not the connector, is the bottleneck. Output side mirrors this with
`output_connector_queued_records` and `output_buffered_batches`.
