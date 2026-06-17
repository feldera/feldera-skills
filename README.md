# feldera-skills

Official Feldera skills — set up Feldera, deploy pipelines, translate Spark SQL, and search Feldera docs. Built on the [Agent Skills Open Standard](https://agentskills.io), so they work in Claude Code and other compatible agents (Cursor, GitHub Copilot, …) — see [Compatibility](#compatibility).

## Installation

**Any Agent Skills–compatible agent** (vendor-neutral):
```bash
npx skills add feldera/feldera-skills
```

**Claude Code** (plugin marketplace):
```bash
claude plugin marketplace add feldera/feldera-skills
claude plugin install feldera-skills
```

## Skills

| Skill | Description | Status |
|-------|-------------|--------|
| `install-fda` | Install or update the Feldera CLI (`fda`) | ✅ |
| `install-feldera` | Start Feldera locally with Docker or connect to a remote instance | ✅ |
| `deploy-pipeline` | Create or update a Feldera pipeline from a SQL file | ✅ |
| `felderize` | Translate Spark SQL into valid Feldera SQL | ✅ |
| `feldera-docs` | Search Feldera documentation | ✅ |

## Usage

Replace the `<…>` placeholders with your own names and file paths (the file arguments
are read relative to your current directory):

```
/feldera-skills:install-fda
/feldera-skills:install-fda --update
/feldera-skills:install-fda --version v0.304.0

/feldera-skills:install-feldera

/feldera-skills:deploy-pipeline <pipeline-name> <sql-file>

/feldera-skills:felderize <spark-sql-file>
/feldera-skills:felderize <spark-sql-file> <output-file>

/feldera-skills:feldera-docs TUMBLE window function
/feldera-skills:feldera-docs Kafka source connector
```

### `felderize` example

`felderize` reads a Spark SQL file and writes the translated Feldera SQL. For example, this Spark input:
```sql
CREATE TABLE orders (
  order_id BIGINT, customer_id BIGINT, region STRING,
  amount DECIMAL(12,2), status STRING, created_at TIMESTAMP
) USING parquet;

CREATE OR REPLACE TEMP VIEW revenue_by_region_month AS
SELECT region,
       date_trunc('MONTH', created_at) AS order_month,
       COUNT(*) AS order_count,
       SUM(amount) AS total_amount
FROM orders
WHERE status IN ('PAID', 'SHIPPED')
GROUP BY region, date_trunc('MONTH', created_at);
```

This input ships with the plugin at [`skills/felderize/examples/orders.spark.sql`](skills/felderize/examples/orders.spark.sql) — run `felderize` on it (or your own `.sql`):
```
/feldera-skills:felderize skills/felderize/examples/orders.spark.sql orders.feldera.sql
```


## Compatibility

These skills follow the [Agent Skills Open Standard](https://agentskills.io) — the `SKILL.md` files are vendor-neutral, so any compatible agent can load them. A few skills need specific agent capabilities:

| Skill | Needs |
|-------|-------|
| `felderize` | shell + file read/write (the optional compile-check needs Java, or a running Feldera) |
| `deploy-pipeline` | shell + the `fda` CLI + a reachable Feldera instance |
| `feldera-docs` | a **web-fetch** capability (reads docs.feldera.com) |
| `install-fda`, `install-feldera` | shell + a **web-fetch** capability (read the docs / GitHub releases) |

In Claude Code you invoke a skill with its plugin namespace (`/feldera-skills:felderize …`); other agents invoke by skill name (`/felderize …`) or load it automatically when your prompt matches.


## Requirements

- Claude Code (CLI, [VS Code extension](https://marketplace.visualstudio.com/items?itemName=Anthropic.claude-code), or JetBrains plugin)
- Docker (for local Feldera — installed by `install-feldera`)
- `fda` CLI (installed by `install-fda`)

## License

MIT
