---
name: feldera-docs
description: >
  Search Feldera documentation. Fetches relevant doc pages directly from
  docs.feldera.com for a given question or keyword. Use when user asks
  "how do I use TUMBLE", "what connectors does Feldera support", "show me
  the Kafka connector docs", or any question about Feldera SQL functions,
  connectors, pipelines, or configuration.
allowed-tools: WebFetch
argument-hint: "QUESTION_OR_KEYWORD"
license: MIT
compatibility: Requires network access to fetch docs.feldera.com.
metadata:
  author: Feldera
  version: "1.0.0"
---

You are helping the user find relevant Feldera documentation.

Query: `$@`

If `$@` is empty, ask the user what they want to look up before fetching anything.

## Step 1 — Identify relevant pages

Based on the query, pick **2–4 pages** from the table below that are most
likely to contain the answer. If unsure, start with the most specific match.

### SQL

| Topic | URL |
|-------|-----|
| SQL intro & streaming extensions | https://docs.feldera.com/sql/intro/ |
| Types (INT, VARCHAR, TIMESTAMP…) | https://docs.feldera.com/sql/types/ |
| DDL grammar (CREATE TABLE/VIEW) | https://docs.feldera.com/sql/grammar/ |
| Aggregates (SUM, COUNT, TUMBLE…) | https://docs.feldera.com/sql/aggregates/ |
| Date & time functions | https://docs.feldera.com/sql/datetime/ |
| String functions | https://docs.feldera.com/sql/string/ |
| Array functions | https://docs.feldera.com/sql/array/ |
| Map functions | https://docs.feldera.com/sql/map/ |
| JSON functions | https://docs.feldera.com/sql/json/ |
| Streaming (LATENESS, WATERMARK) | https://docs.feldera.com/sql/streaming/ |
| Tables | https://docs.feldera.com/sql/table/ |
| Materialized views | https://docs.feldera.com/sql/materialized/ |
| Recursion | https://docs.feldera.com/sql/recursion/ |
| User-defined functions (UDFs) | https://docs.feldera.com/sql/udf/ |
| Ad-hoc queries | https://docs.feldera.com/sql/ad-hoc/ |
| Casts & conversions | https://docs.feldera.com/sql/casts/ |
| Comparison & boolean operators | https://docs.feldera.com/sql/comparisons/ |
| Numeric operators | https://docs.feldera.com/sql/operators/ |
| Decimal type & precision | https://docs.feldera.com/sql/decimal/ |
| Unsupported operations | https://docs.feldera.com/sql/unsupported-operations/ |

### Connectors — Sources

| Topic | URL |
|-------|-----|
| Connectors overview | https://docs.feldera.com/connectors/ |
| Connector unique keys (primary key) | https://docs.feldera.com/connectors/unique_keys/ |
| Kafka source | https://docs.feldera.com/connectors/sources/kafka/ |
| Delta Lake source | https://docs.feldera.com/connectors/sources/delta/ |
| Debezium (CDC) source | https://docs.feldera.com/connectors/sources/debezium/ |
| PostgreSQL source | https://docs.feldera.com/connectors/sources/postgresql/ |
| HTTP input source | https://docs.feldera.com/connectors/sources/http/ |
| HTTP GET (poll) source | https://docs.feldera.com/connectors/sources/http-get/ |
| S3 source | https://docs.feldera.com/connectors/sources/s3/ |
| Iceberg source | https://docs.feldera.com/connectors/sources/iceberg/ |
| NATS source | https://docs.feldera.com/connectors/sources/nats/ |
| Pub/Sub source | https://docs.feldera.com/connectors/sources/pubsub/ |
| Datagen source | https://docs.feldera.com/connectors/sources/datagen/ |
| File source | https://docs.feldera.com/connectors/sources/file/ |

### Connectors — Sinks

| Topic | URL |
|-------|-----|
| Kafka sink | https://docs.feldera.com/connectors/sinks/kafka/ |
| Delta Lake sink | https://docs.feldera.com/connectors/sinks/delta/ |
| Snowflake sink | https://docs.feldera.com/connectors/sinks/snowflake/ |
| PostgreSQL sink | https://docs.feldera.com/connectors/sinks/postgresql/ |
| Redis sink | https://docs.feldera.com/connectors/sinks/redis/ |
| Iceberg sink | https://docs.feldera.com/connectors/sinks/iceberg/ |
| HTTP sink | https://docs.feldera.com/connectors/sinks/http/ |
| File sink | https://docs.feldera.com/connectors/sinks/file/ |
| Confluent JDBC sink | https://docs.feldera.com/connectors/sinks/confluent-jdbc/ |
| Completion tokens | https://docs.feldera.com/connectors/completion-tokens/ |
| Secret references | https://docs.feldera.com/connectors/secret-references/ |
| Connector orchestration | https://docs.feldera.com/connectors/orchestration/ |

### Data Formats

| Topic | URL |
|-------|-----|
| JSON format | https://docs.feldera.com/formats/json/ |
| Avro format | https://docs.feldera.com/formats/avro/ |
| CSV format | https://docs.feldera.com/formats/csv/ |
| Parquet format | https://docs.feldera.com/formats/parquet/ |
| Raw format | https://docs.feldera.com/formats/raw/ |

### Pipelines & Operations

| Topic | URL |
|-------|-----|
| Pipeline lifecycle | https://docs.feldera.com/pipelines/lifecycle/ |
| Pipeline settings (workers, memory, compilation) | https://docs.feldera.com/pipelines/configuration/ |
| Fault tolerance & DR overview | https://docs.feldera.com/pipelines/fault-tolerance-overview/ |
| Fault tolerance (checkpoints) | https://docs.feldera.com/pipelines/fault-tolerance/ |
| Checkpoint sync to object store | https://docs.feldera.com/pipelines/checkpoint-sync/ |
| Transactions | https://docs.feldera.com/pipelines/transactions/ |
| Latency | https://docs.feldera.com/pipelines/latency/ |
| Modifying a running pipeline | https://docs.feldera.com/pipelines/modifying/ |
| Metrics | https://docs.feldera.com/operations/metrics/ |
| Memory | https://docs.feldera.com/operations/memory/ |
| Operations guide | https://docs.feldera.com/operations/guide/ |

### Interface & API

| Topic | URL |
|-------|-----|
| CLI (fda) | https://docs.feldera.com/interface/cli/ |
| Web Console | https://docs.feldera.com/interface/web-console/ |
| Python SDK | https://docs.feldera.com/python/ |
| REST API | https://docs.feldera.com/api/ |

### Setup & Tutorials

| Topic | URL |
|-------|-----|
| Docker setup | https://docs.feldera.com/get-started/docker/ |
| Sandbox (try.feldera.com) | https://docs.feldera.com/get-started/sandbox/ |
| Helm chart guide | https://docs.feldera.com/get-started/enterprise/helm-guide/ |
| Tutorial: basics part 1 | https://docs.feldera.com/tutorials/basics/part1/ |
| Tutorial: basics part 2 | https://docs.feldera.com/tutorials/basics/part2/ |
| Tutorial: time-series | https://docs.feldera.com/tutorials/time-series/ |
| Tutorial: writing SQL | https://docs.feldera.com/tutorials/writing-sql/ |

### Use Cases

| Topic | URL |
|-------|-----|
| Fraud detection | https://docs.feldera.com/use_cases/fraud_detection/ |
| Fine-grained authorization (intro) | https://docs.feldera.com/use_cases/fine_grained_authorization/intro/ |
| Fine-grained authorization (static) | https://docs.feldera.com/use_cases/fine_grained_authorization/static/ |
| Fine-grained authorization (dynamic) | https://docs.feldera.com/use_cases/fine_grained_authorization/dynamic/ |
| Batch processing | https://docs.feldera.com/use_cases/batch/intro/ |
| Real-time apps | https://docs.feldera.com/use_cases/real_time_apps/part1/ |

## Step 2 — Fetch and extract

Use WebFetch on each selected URL. Focus on sections that directly answer
the query — quote code blocks and config options verbatim.

If the page is long, look for headings that match the query terms and
extract just those sections.

## Step 3 — Present the answer

Lead with a concise answer to `$@`, then show the supporting doc excerpts:

```
### {Section heading} — {Page title}
{url}

{Relevant excerpt}

---
```

## Step 4 — If not found

If none of the fetched pages answer the query:
- Try one broader page (e.g. the connectors overview or SQL intro)
- If still not found, tell the user and direct them to https://docs.feldera.com
