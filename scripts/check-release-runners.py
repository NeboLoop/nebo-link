#!/usr/bin/env python3
"""Release builds run on NeboAI's own machines, never on GitHub's hosted
runners: macOS on the house Mac mini ("stadium"), Linux arm64 on its Lima VM,
Linux amd64 on a DigitalOcean droplet created for the run and deleted after it,
Windows on the house Windows box. Nebo's releases follow the same rule, and
this check exists because it has been undone there before (Aug 15-16 2026:
release builds moved to macos-latest/ubuntu-latest and took 70+ minutes on
4-core rented machines). ubuntu-latest is allowed only for jobs that
orchestrate (key checks, droplet lifecycle, checksums, uploads).

Run locally: python3 scripts/check-release-runners.py
"""
import sys
import yaml

WF = ".github/workflows/release.yml"
MAC_JOBS = ("build-macos", "notarize-macos")
WINDOWS_JOBS = ("build-windows", "sign-windows")


def labels(v):
    if isinstance(v, list):
        return [str(x) for x in v]
    return [str(v)]


def main():
    with open(WF) as f:
        jobs = yaml.safe_load(f)["jobs"]
    errors = []
    for name in MAC_JOBS:
        runs_on = labels(jobs[name].get("runs-on"))
        if "stadium-mac" not in runs_on:
            errors.append(f"{name}: runs-on {runs_on} — must be the house Mac mini ([self-hosted, macOS, stadium-mac])")
    for leg in jobs["build-linux"]["strategy"]["matrix"]["include"]:
        runs_on = labels(leg.get("runner"))
        arch = leg.get("arch")
        if arch == "arm64" and "neboloop" not in runs_on:
            errors.append(f"build-linux arm64: runner {runs_on} — must be the Mac mini's Lima VM ([self-hosted, Linux, ARM64, neboloop])")
        # amd64 builds on a DigitalOcean droplet that exists only for the run
        # (linux-amd64-runner provisions it, linux-amd64-runner-down deletes
        # it). Still ours, still self-hosted; never a GitHub-hosted image.
        if arch == "amd64" and not any(l.startswith("nebo-amd64-") for l in runs_on):
            errors.append(f"build-linux amd64: runner {runs_on} — must be the per-run droplet ([self-hosted, Linux, X64, nebo-amd64-<run id>])")
        if "self-hosted" not in runs_on:
            errors.append(f"build-linux {arch}: runner {runs_on} — must be self-hosted")
    for name in ("linux-amd64-runner", "linux-amd64-runner-down"):
        if name not in jobs:
            errors.append(f"{name}: the per-run droplet jobs must exist")
    for name in WINDOWS_JOBS:
        runs_on = labels(jobs[name].get("runs-on"))
        if "stadium-win" not in runs_on:
            errors.append(f"{name}: runs-on {runs_on} — must be the house Windows box ([self-hosted, Windows, X64, stadium-win])")
    for name, job in jobs.items():
        runs_on = " ".join(labels(job.get("runs-on")))
        if "macos-" in runs_on or "windows-" in runs_on:
            errors.append(f"{name}: uses a GitHub-hosted runner ({runs_on})")
        # Anything that compiles runs on a house machine.
        if "self-hosted" not in runs_on and "${{ matrix.runner }}" not in runs_on:
            for step in job.get("steps", []):
                if "cargo " in str(step.get("run", "")):
                    errors.append(f"{name}: runs cargo on a GitHub-hosted runner ({runs_on})")
                    break
    if errors:
        print("Release builds must run on NeboAI's own machines (see nebo CLAUDE.md, 'Release builds'):")
        for e in errors:
            print("  -", e)
        return 1
    print("release runners OK: Mac + Linux arm64 on the house Mac mini, Linux amd64 on a per-run droplet, Windows on the house Windows box")
    return 0


if __name__ == "__main__":
    sys.exit(main())
