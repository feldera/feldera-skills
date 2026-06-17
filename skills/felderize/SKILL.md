---
name: felderize
description: >
  Translate Spark SQL into valid Feldera SQL. Rewrites DDL, type syntax, and
  functions using the master Spark-to-Feldera reference, annotates behavioral
  differences with NOTE comments, and marks unsupported constructs. Use when
  user says "translate Spark SQL", "convert Spark to Feldera", "migrate from
  Spark", "felderize", or provides a .sql file from a Spark or Databricks
  project.
allowed-tools: Bash Read Write
argument-hint: "SPARK_SQL_FILE [OUTPUT_FILE]"
license: MIT
compatibility: Requires the fda CLI, or Java 19–21 to run the sql2dbsp compiler, plus network access.
metadata:
  author: Feldera
  version: "1.0.0"
---

You are translating Spark SQL into valid Feldera SQL.

Arguments: `$@`

- `<spark-sql-file>` — path to the Spark SQL to translate (schema DDL and/or
  queries/views). If omitted, ask the user for the file path or for the SQL
  pasted inline.
- `[output-file]` — where to write the translated Feldera SQL. If omitted,
  default to the input filename with a `.feldera.sql` suffix (e.g.
  `pipeline.sql` → `pipeline.feldera.sql`).

---

## Step 1 — Load the translation rules (MANDATORY)

Before translating anything, read the full reference. It is the authority for
every rewrite — do not translate from memory.

Use the **Read tool** on the bundled reference — `references/spark-skills.md`, in
this skill's own directory — and read it in its entirety:

```
references/spark-skills.md
```

It covers: type mappings, DDL rewrites, the full function reference (direct
equivalents, rewritable patterns, and the unsupported list), and the general
behavioral differences (the `[GBD-*]` codes).

## Step 2 — Read the input

Read the Spark SQL file (or take the inline SQL the user pasted). Identify:

- **DDL** — `CREATE TABLE` / `CREATE VIEW` statements and the column types.
  The types drive several rewrites (e.g. `AVG` on integer columns, `size()` on
  `NOT NULL` arrays), so extract the schema first.
- **Queries / views** — the statements to translate.

## Step 3 — Translate

Apply the reference in this order:

1. **DDL first.** Fix types (`ARRAY<T>` → `T ARRAY`, `FLOAT` → `REAL`, etc.),
   strip catalog/schema qualifiers, drop unsupported clauses (`USING`,
   `PARTITIONED BY`, `COMMENT`, `CREATE INDEX`), normalize `PRIMARY KEY`, and
   quote reserved-word column names. **Never reorder columns.**
2. **Then queries.** Rewrite functions and syntax per the reference. Prefer a
   documented rewrite over marking something unsupported — consult the
   reference before concluding anything is unsupported.

While translating, follow these rules from the reference:

- **Behavioral differences** — whenever a `[GBD-*]` rule applies to the query,
  add a `-- NOTE: [GBD-CODE] ...` comment in the output SQL explaining how
  Feldera may differ from Spark, so the user knows where results can diverge.
- **Unsupported constructs** — never silently drop or approximate them with
  different semantics. Replace each unsupported expression with
  `CAST(NULL AS <type>)`, translate everything else normally (an empty view is
  never acceptable), and add an `-- UNSUPPORTED: ...` comment. Collect the list
  of unsupported constructs to report to the user at the end.

> Note: the reference's "Rules for unsupported constructs" mentions an
> `unsupported` array and a `feldera_query` field — that is legacy JSON-output
> wording. This skill's actual contract is the `.feldera.sql` output file plus
> inline `-- UNSUPPORTED:` comments and the Step 4 summary; do not emit JSON.

## Step 4 — Write the output

Write the translated SQL to the output file. Preserve statement order and the
overall structure of the input. Keep all `-- NOTE:` and `-- UNSUPPORTED:`
comments inline above the relevant statement.

Then summarize for the user:

- ✅ what translated cleanly
- ⚠️ behavioral differences flagged (the `[GBD-*]` notes)
- ❌ anything marked unsupported, with the reason

## Step 5 — Validate (optional, recommended)

Compilation is the only reliable check that the translated SQL is valid. Offer
the user a choice of two ways to compile it:

### Option A — Standalone compiler (no running Feldera needed)

Compile locally with the `sql2dbsp` SQL compiler JAR. This needs **Java 19–21**
on the `PATH` (`java -version`) but no running instance.

Locate the compiler JAR (this skill keeps it under `~/.feldera/compiler/`). Use
**v0.304.0 or newer** — earlier releases lack SQL features the reference relies
on (e.g. `div_null`, `MAKE_DATE`) — so ignore any stale cached jar below that:

```bash
COMPILER=$(ls -1 ~/.feldera/compiler/sql2dbsp-jar-with-dependencies-*.jar 2>/dev/null | sort -V | tail -1)
if [ -n "$COMPILER" ]; then
  VER=$(basename "$COMPILER" | grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+')
  [ "$(printf 'v0.304.0\n%s\n' "$VER" | sort -V | head -1)" = "v0.304.0" ] || \
    { echo "cached $VER is older than v0.304.0 — ignoring"; COMPILER=""; }
fi
echo "${COMPILER:-NONE}"
```

If none is found (or the cached one was too old), **offer to download** the
latest from Feldera's GitHub releases (`fda` cannot fetch a compiler — it only
talks to a running instance):

```bash
mkdir -p ~/.feldera/compiler
ASSET_URL=$(curl -fsSL https://api.github.com/repos/feldera/feldera/releases/latest \
  | grep -oE '"browser_download_url": *"[^"]*sql2dbsp-jar-with-dependencies-[^"]*\.jar"' \
  | head -1 | cut -d'"' -f4)
if [ -z "$ASSET_URL" ]; then
  echo "Could not find the sql2dbsp asset (GitHub rate limit, or the asset was renamed)." >&2
  echo "Use Option B (deploy to a running instance), or download the JAR manually." >&2
  exit 1
fi
echo "Downloading $ASSET_URL"
curl -fL "$ASSET_URL" -o ~/.feldera/compiler/"$(basename "$ASSET_URL")" || { echo "download failed" >&2; exit 1; }
COMPILER=$(ls -1 ~/.feldera/compiler/sql2dbsp-jar-with-dependencies-*.jar | sort -V | tail -1)
```

The latest release always satisfies the v0.304.0 floor. Then compile the output
(SQL-only, no Rust codegen, so it is fast):

```bash
java -jar "$COMPILER" -i --ignoreOrder --alltables --noRust <output-file>
```

Exit code 0 means the SQL is valid. A non-zero exit prints the compiler errors
on stderr — triage them per the reference (below).

### Option B — Running Feldera instance

If the user already has a running instance (or prefers Docker), deploy with
`/feldera-skills:deploy-pipeline <pipeline-name> <output-file>` — this
creates/updates the pipeline and surfaces any SQL compile error. Run
`/feldera-skills:install-feldera` first if there is no instance.

### Triage compile failures

If compilation fails (either option), use the **"Compiler errors: syntax errors
vs. unsupported features"** section of the reference to triage:

- **Syntax / type error** (points to a line with a clear parse or type message,
  e.g. `Table 'X' not found`, `No match found for function signature`) — this
  is a bug in the translation. Fix the SQL and re-validate. Do **not** mark the
  construct unsupported. **Cap this at ~3 attempts per statement**: if the same
  statement still fails after three fixes, stop iterating on it — mark it
  `-- UNSUPPORTED: <last compiler error>`, replace the offending expression with
  `CAST(NULL AS <type>)` per Step 3, and report it rather than looping further.
- **Unsupported feature** (`Feature not yet implemented`, or `No match found`
  for a function absent from Feldera) — apply the unsupported rule from Step 3
  (`CAST(NULL AS <type>)` + comment) and re-validate. Do **not** keep retrying
  with different syntax.

Repeat until it compiles or every remaining failure is a documented-unsupported
construct or has hit the 3-attempt cap above — never loop indefinitely.
