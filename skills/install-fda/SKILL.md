---
name: install-fda
description: >
  Install or update the Feldera CLI (fda).
  Use when fda is missing or the user wants to upgrade it.
allowed-tools: Bash WebFetch
argument-hint: "[--update] [--version VERSION]"
license: MIT
compatibility: Requires network access. Falls back to Cargo (Rust) when no prebuilt binary is available.
metadata:
  author: Feldera
  version: "1.0.0"
---

You are helping the user install or update the `fda` CLI.

Arguments: `$@`

Parse the arguments:
- `--update` — upgrade to the latest version.
- `--version <version>` — install a specific version. The token immediately after `--version`
  is the version string (e.g. `v0.304.0`); call it `VERSION`. If `--version` is given with no
  following token, ask the user which version before continuing.

## Step 1 — Check for fda

```bash
fda --version 2>/dev/null || echo "NOT_FOUND"
```

**If found** and neither `--update` nor `--version` is in `$@`:

Compare the installed version with the latest GitHub release. The formats differ — `fda --version`
prints `fda 0.306.0` while the GitHub tag is `v0.311.0` — so reduce both to the bare `x.y.z` before
comparing, or they will never match and you will prompt to update even when already current:

```bash
INSTALLED=$(fda --version 2>/dev/null | awk '{print $2}')
LATEST=$(curl -s https://api.github.com/repos/feldera/feldera/releases/latest \
  | grep '"tag_name"' | head -1 | cut -d'"' -f4 | sed 's/^v//')
echo "installed=$INSTALLED latest=$LATEST"
```

- If `$INSTALLED` equals `$LATEST`: report "✅ fda `<version>` is already installed and up to date." and stop.
- Otherwise (a newer version is available): ask the user:
  > fda `<installed>` is installed. Latest is `<latest>`. Would you like to update?
  - If yes → continue to Step 2 (install latest).
  - If no → stop.

## Step 2 — Install

Fetch the current install instructions from the docs (treat them as authoritative if they differ
from the baseline commands below):

WebFetch https://docs.feldera.com/interface/cli/

Detect OS and architecture together — the installer choice depends on both:

```bash
echo "os=$(uname -s 2>/dev/null || echo Windows) arch=$(uname -m 2>/dev/null || echo unknown)"
```

Pick the installer:
- **Linux** (`x86_64` / `aarch64`) — curl installer. The prebuilt binary needs **glibc ≥ 2.39**
  (Ubuntu 24.04+, Debian 13+, Fedora 40+, RHEL 10+); on older systems use Cargo instead.
- **macOS Apple Silicon** (`Darwin` + `arm64` / `aarch64`) — curl installer.
- **macOS Intel** (`Darwin` + `x86_64`) — curl installer is **not** available; use `cargo install fda`.
- **Windows** (`uname` reports `MINGW*` / `MSYS*`, or is absent) — PowerShell installer.

Baseline commands (prefer the fetched docs if they differ):

```bash
# Linux / macOS — latest:
curl -fsSL https://docs.feldera.com/install-fda | bash
# Linux / macOS — specific version (use the VERSION parsed above):
curl -fsSL https://docs.feldera.com/install-fda | FDA_VERSION=<version> bash
```

```powershell
# Windows — latest:
powershell -ExecutionPolicy Bypass -NoProfile -c "irm https://docs.feldera.com/install-fda.ps1 | iex"
# Windows — specific version:
powershell -c "$env:FDA_VERSION='<version>'; irm https://docs.feldera.com/install-fda.ps1 | iex"
```

If `--version` was given, install `VERSION` via the `FDA_VERSION` prefix; otherwise install the latest.

If the curl/PowerShell installer fails (or glibc is too old for the prebuilt Linux binary), fall
back to `cargo install fda`.

If Cargo is also unavailable, tell the user:
> **Could not install fda.** Install Rust from https://rustup.rs/ and run `cargo install fda`, or see https://docs.feldera.com/interface/cli/ for other options.

## Step 3 — Verify

```bash
fda --version
```

Report "✅ fda `<version>` installed." or stop with an error if fda is still not found.
