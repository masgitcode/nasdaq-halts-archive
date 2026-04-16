# Nasdaq Halts Archive

Standalone Node CLI for archiving Nasdaq trade halt data into a local SQLite database that can be committed back into the repo on a daily schedule.

## What it does

- Fetches the Nasdaq RSS halt feed for a target ET date using both `haltdate` and `resumedate`
- Normalizes rows into an analytics-friendly `halt_events` SQLite table
- Tracks ingestion metadata in `ingestion_runs`
- Supports daily ingest, historical backfill, and CSV export
- Includes a GitHub Actions workflow that can run automatically on weekdays

## Requirements

- Node 20+
- `sqlite3` CLI on your PATH

## Commands

Ingest a single ET date:

```bash
npm run ingest-day -- --date 2026-03-26
```

If `--date` is omitted, the CLI uses the current day in `America/New_York`.

Backfill a date range:

```bash
npm run backfill -- --from 2026-03-01 --to 2026-03-26
```

Export analytics-ready CSV to stdout:

```bash
npm run export-csv > data/halts-export.csv
```

Run tests:

```bash
npm test
```

## Database

The SQLite database lives at `data/halts.sqlite`.

Main tables:

- `halt_events`
- `ingestion_runs`

`event_id` is a stable SHA-256 hash of:

```text
symbol|halt_date_et|halt_time_et|reason_code|market
```

That key is used to upsert the same halt across `haltdate` and `resumedate` ingests, including multi-day resumptions.

## GitHub Actions

The workflow in `.github/workflows/daily-ingest.yml`:

- runs automatically on weekdays at `30 1 * * 2-6` UTC
- supports manual `workflow_dispatch` with an optional `date` input
- commits `data/halts.sqlite` back into the repo only when it changes

## Notes

- `duration_seconds_to_trade` is the canonical halt-duration field
- `duration_seconds_to_quote` is also stored when Nasdaq provides quote resumption time
- `raw_json` preserves source payloads from `haltdate` and `resumedate`
