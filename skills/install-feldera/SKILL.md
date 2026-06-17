---
name: install-feldera
description: >
  Set up a Feldera instance — run it locally with Docker or use the free online sandbox.
  Use this when the user needs a running Feldera instance before doing any pipeline work.
allowed-tools: Bash WebFetch
license: MIT
compatibility: Requires Docker for the local option, or network access to reach a remote Feldera instance.
metadata:
  author: Feldera
  version: "1.0.0"
---

You are helping the user get a Feldera instance running.

## Step 1 — Choose how to run Feldera

Ask the user:

> How would you like to run Feldera?
>
> | | |
> |---|---|
> | **1 — Docker** | Runs locally, no account needed. Requires Docker. |
> | **2 — Sandbox / Remote** | Any Feldera deployment (try.feldera.com, company instance, etc.). No install needed. |

Wait for their answer.

---

## Option 1 — Docker

### Check Docker is available

```bash
docker info 2>/dev/null && echo "OK" || echo "MISSING"
```

If `MISSING`: tell the user "⚠️ Docker is not installed or not running. Install it from https://docs.docker.com/get-docker/ then re-run." and stop.

On Apple Silicon Macs, Rosetta must be enabled in Docker Desktop for x86/amd64 emulation (Docker Desktop → Settings → General → "Use Rosetta for x86/amd64 emulation").

### Check if port 8080 is already in use

```bash
lsof -ti:8080 2>/dev/null || ss -tlnH 2>/dev/null | grep -E 'LISTEN.*:8080(\s|$)'
```

If something is using port 8080: tell the user "⚠️ Port 8080 is already in use. Stop that process (find its PID with `lsof -ti:8080`, or `sudo ss -tlnp | grep :8080`), then re-run." and stop.

### Get the Docker image and start Feldera

Fetch the current Docker setup instructions to get the image name:

WebFetch https://docs.feldera.com/get-started/docker

Extract the image name from the docs (if the WebFetch fails, fall back to the known image
`images.feldera.com/feldera/pipeline-manager:latest`). Run it detached (`-d`) and **named** so
Claude Code can continue while Feldera starts — and so the container can be found later for
logs/stop. (The docs show an interactive `-it` command; detached is intentional here so the agent
can poll for readiness.)

```bash
docker run --pull always -p 8080:8080 --rm -d --name feldera <image-from-docs> \
  && echo "STARTED" || echo "RUN_FAILED"
```

If `RUN_FAILED`: report the `docker run` error to the user and stop. Common causes: the image is
still being pulled on a slow connection, or — on Apple Silicon — Rosetta is not enabled (see the
note above).

> Note: `--rm` makes this a throwaway instance — stopping the container deletes all pipelines and
> state. That's fine for local development; for a persistent instance, drop `--rm` and add a volume
> mount per the Docker docs.

### Wait until ready

Poll until the web console responds (up to 60 seconds), capturing whether it actually came up:

```bash
READY=no
for i in $(seq 1 60); do
  curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8080 | grep -q 200 && { READY=yes; break; }
  sleep 1
done
echo "$READY"
```

- If `READY=yes`, tell the user:
  > ✅ Feldera is running. Open the web console at: http://127.0.0.1:8080
- If `READY=no`, **do not report success** — Feldera did not become ready in time. Show the logs and stop:
  ```bash
  docker logs feldera 2>&1 | tail -30 || echo "(no 'feldera' container — it may have exited on startup)"
  ```
  Tell the user: "⚠️ Feldera did not become ready within 60s. See the logs above (common cause: the image is still pulling on a slow connection, or a startup error)."

---

## Option 2 — Sandbox or remote instance

Ask the user for their Feldera host URL:

> Which Feldera instance would you like to connect to?
> The public sandbox is **https://try.feldera.com**, but any Feldera deployment works
> (your company's instance, a private cloud deployment, etc.).

Wait for their host URL, then tell the user:

> 1. Open `<host>` in your browser and log in.
> 2. Create an API key: open your account/user menu and look for an **API Keys** (or **Settings**) section, then generate and copy your key.
> 3. Add these two lines to your `.env` file. The API key **must begin with `apikey:`** — that prefix is part of the value `fda --auth` expects, and authentication fails without it. If the key you copied already starts with `apikey:`, don't add it twice:
>    ```
>    FELDERA_HOST=<host>
>    FELDERA_API_KEY=apikey:<your-api-key>
>    ```
>
> Note: try.feldera.com shuts down each pipeline after 24 hours of runtime, and occasional refreshes may delete all pipelines.
