PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS halt_events (
  event_id TEXT PRIMARY KEY,
  symbol TEXT NOT NULL,
  issue_name TEXT,
  market TEXT,
  reason_code TEXT NOT NULL,
  reason_label TEXT,
  is_volatility_pause INTEGER NOT NULL,
  halt_date_et TEXT NOT NULL,
  halt_time_et TEXT NOT NULL,
  halted_at_utc TEXT NOT NULL,
  resumption_date_et TEXT,
  resumption_quote_time_et TEXT,
  resumption_trade_time_et TEXT,
  resumption_quote_at_utc TEXT,
  resumption_trade_at_utc TEXT,
  duration_seconds_to_quote INTEGER,
  duration_seconds_to_trade INTEGER,
  pause_threshold_price TEXT,
  status TEXT NOT NULL,
  first_seen_at_utc TEXT NOT NULL,
  last_seen_at_utc TEXT NOT NULL,
  last_source_date_et TEXT NOT NULL,
  raw_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
  run_id TEXT PRIMARY KEY,
  run_type TEXT NOT NULL,
  target_date_et TEXT NOT NULL,
  started_at_utc TEXT NOT NULL,
  finished_at_utc TEXT,
  rows_seen INTEGER NOT NULL,
  rows_inserted INTEGER NOT NULL,
  rows_updated INTEGER NOT NULL,
  ok INTEGER NOT NULL,
  error_text TEXT
);

CREATE INDEX IF NOT EXISTS idx_halt_events_symbol ON halt_events(symbol);
CREATE INDEX IF NOT EXISTS idx_halt_events_halted_at_utc ON halt_events(halted_at_utc);
CREATE INDEX IF NOT EXISTS idx_halt_events_reason_code ON halt_events(reason_code);
CREATE INDEX IF NOT EXISTS idx_ingestion_runs_target_date_et ON ingestion_runs(target_date_et);
