---
name: deploy-pipeline
description: >
  Create or update a Feldera pipeline from a SQL file. If the pipeline is new,
  creates and starts it. If it already exists, stops it, updates the program,
  and restarts it. Polls until Running. Use when user says "deploy pipeline",
  "create pipeline from SQL", "run my pipeline", or "update my Feldera program".
allowed-tools: Bash
argument-hint: "PIPELINE_NAME SQL_FILE"
license: MIT
compatibility: Requires the fda CLI and a reachable Feldera instance.
metadata:
  author: Feldera
  version: "1.0.0"
---

You are deploying a Feldera pipeline from a SQL file.

Arguments: `$@`

Resolve `<pipeline-name>` and `<sql-file>` from the arguments. If either is
missing, ask the user for it before proceeding.

Resolve the connection. Prefer environment variables already exported in the shell — `fda` reads
`FELDERA_HOST` / `FELDERA_API_KEY` natively — and fall back to `.env` only when they are unset:

```bash
[ -z "$FELDERA_HOST" ]    && FELDERA_HOST=$(grep -E '^FELDERA_HOST=' .env 2>/dev/null | tail -1 | cut -d'=' -f2-)
[ -z "$FELDERA_API_KEY" ] && FELDERA_API_KEY=$(grep -E '^FELDERA_API_KEY=' .env 2>/dev/null | tail -1 | cut -d'=' -f2-)
FELDERA_HOST=${FELDERA_HOST:-http://localhost:8080}

# fda requires the API key to start with "apikey:" — normalize if a raw key was stored.
if [ -n "$FELDERA_API_KEY" ]; then
  case "$FELDERA_API_KEY" in apikey:*) ;; *) FELDERA_API_KEY="apikey:$FELDERA_API_KEY" ;; esac
fi
```

Build the base `fda` command once — with `--auth` only when an API key is present (not needed for Docker):

```bash
if [ -n "$FELDERA_API_KEY" ]; then
  FDA="fda --host $FELDERA_HOST --auth $FELDERA_API_KEY"
else
  FDA="fda --host $FELDERA_HOST"
fi
```

Use `$FDA` for every `fda` call below.

If `fda` is not installed, run `/feldera-skills:install-fda` first.
If Feldera is not reachable, run `/feldera-skills:install-feldera` first.

---

## Step 1 — Check if pipeline exists

```bash
$FDA status <pipeline-name> >/dev/null 2>&1 && echo "EXISTS" || echo "NEW"
```

---

## Step 2 — Deploy

`fda start` waits for compilation and **exits non-zero — printing the compiler error — if the SQL
fails to compile**, so its exit status is the primary failure signal. Capture it.

### If NEW

Tell the user: "⏳ Creating pipeline `<pipeline-name>`..."

```bash
$FDA create <pipeline-name> <sql-file>
```

Tell the user: "⏳ Starting..."

```bash
$FDA start <pipeline-name>; echo "start_rc=$?"
```

### If EXISTS

Tell the user: "⏳ Updating pipeline `<pipeline-name>`..."

```bash
$FDA stop <pipeline-name>
$FDA program set <pipeline-name> <sql-file>
```

Tell the user: "⏳ Restarting..."

```bash
$FDA start <pipeline-name>; echo "start_rc=$?"
```

If `create`, `program set`, or `start` exits non-zero, the SQL almost certainly failed to compile
(the error is already printed) — skip the poll and use the interpretation in Step 3 to triage.

---

## Step 3 — Confirm the real state

Read the state from JSON and match the `deployment_status` field **exactly** — the default
`--format text` is an ASCII table that is hard to parse, and a substring grep is unsafe because
`deployment_desired_status` can read `Running` while the pipeline is still provisioning. (Parsing
below uses `jq`.)

```bash
for i in $(seq 1 60); do
  J=$($FDA status <pipeline-name> --format json 2>/dev/null)
  DS=$(echo "$J" | jq -r '.deployment_status // empty')
  PS=$(echo "$J" | jq -r '.program_status // empty')
  [ "$DS" = "Running" ] && break
  case "$PS" in SqlError|RustError|SystemError) break ;; esac
  sleep 1
done
$FDA status <pipeline-name> --format json | jq '{deployment_status, program_status, deployment_error, program_error}'
```

Interpret the final JSON:

- `deployment_status` = **Running** — tell the user "✅ Pipeline `<pipeline-name>` is running."
- `program_status` = **SqlError** — a SQL compile error. Read `program_error` for the offending
  line/message (`deployment_error` carries only the summary), fix the SQL, and go back to Step 2.
- `program_status` = **RustError** / **SystemError** — rare internal error. Report `program_error` /
  `deployment_error` to the user verbatim and stop.
- Otherwise (no `Running` within the loop) — report the final JSON and stop.
