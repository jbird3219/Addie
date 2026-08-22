# Addie — Project Notes

## What's here
- `scripts/addie_pipeline.py` — real, runnable data pipeline (stocks/ETFs via
  yfinance, crypto via CoinGecko's free public API, no keys required) plus
  Addie's v0 rule-based commentary logic. **Does not run inside this Claude
  Cowork cloud sandbox** — the sandbox's network access is allowlisted to
  package registries only, not general internet hosts like Yahoo Finance or
  CoinGecko. It's written to run correctly the moment it has a normal
  internet connection.
- `data/snapshot_20260821_manual.json` — today's decision-log entry. Because
  the script couldn't run here, this one was hand-built from real numbers
  pulled via Claude's web-fetch tool. Every future entry should come from
  `addie_pipeline.py` instead — this file's `source` field says as much, so
  the log is honest about its own provenance.

## Hosting decision (resolved)
This sandbox is a good place to *build and edit* Addie's code and to *design*
the dashboard, but it is not a good permanent home for a live, scheduled
pipeline — no outbound access to market data or broker APIs, and that's true
even reaching your own machine through Claude's device bridge (it shares the
same network allowlist as this cloud sandbox).

**Chosen approach for this stage: GitHub Actions.** It's free for this
workload, fully managed (no server to maintain or leave running), has normal
internet access, and runs on a real schedule. `.github/workflows/addie_pipeline.yml`
runs `scripts/addie_pipeline.py` every 4 hours and commits the fresh
snapshot to `data/latest.json` in the repo.

To make the dashboard actually refresh from that: Claude can't fetch from
GitHub inside a published Artifact (the CSP blocks it, deliberately — true of
most sandboxed pages), but a Claude **scheduled task** *can* fetch a public
URL and republish the artifact with fresh data baked in. So the loop is:
GitHub Actions refreshes the data → a Claude scheduled task reads it and
republishes the dashboard. Not literally real-time-in-your-browser, but
genuinely living and unattended.

This is the stage-1 answer. Once Addie is placing trades on a schedule
(stage 4+), a small always-on server (~$5–6/mo VPS) becomes worth the cost —
for faster reaction time, and so execution isn't gated on a scheduled task's
cadence. Not needed yet.

## Next steps
1. Create a GitHub account if you don't have one (free), and a new repository
   — public is simplest for now, since nothing in this repo is sensitive yet.
2. Push everything in this folder to that repo, including the hidden
   `.github/` directory (that's what makes the schedule real).
3. Confirm the Action runs: repo → Actions tab → "Addie data pipeline" → Run
   workflow, and check that `data/latest.json` updates.
4. Send Claude the repo's raw URL for `data/latest.json` — from there, set up
   the scheduled task that keeps the dashboard refreshed automatically.
5. Set up an Alpaca account (paper first — see the setup steps already in
   chat), then swap the data source over to Alpaca's market data API so
   pricing comes from the same place trades will eventually execute.
