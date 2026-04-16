const crypto = require('node:crypto');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');

const { fetchFeedSnapshot, parseEasternDateTime } = require('./nasdaq');
const { runSql, queryJson, queryCsv, toSqlValue, ensureParentDir } = require('./sqlite');

const DB_SCHEMA_PATH = join(__dirname, 'schema.sql');
const STATUS_RANK = {
  UNKNOWN: 0,
  HALTED: 1,
  RESUME_SCHEDULED: 2,
  RESUMED: 3,
};
const EXPORT_COLUMNS = [
  'event_id',
  'symbol',
  'issue_name',
  'market',
  'reason_code',
  'reason_label',
  'is_volatility_pause',
  'halt_date_et',
  'halt_time_et',
  'halted_at_utc',
  'resumption_date_et',
  'resumption_quote_time_et',
  'resumption_trade_time_et',
  'resumption_quote_at_utc',
  'resumption_trade_at_utc',
  'duration_seconds_to_quote',
  'duration_seconds_to_trade',
  'pause_threshold_price',
  'status',
  'first_seen_at_utc',
  'last_seen_at_utc',
  'last_source_date_et',
];

function validateIsoDate(value) {
  const text = String(value || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    throw new Error('Date must use YYYY-MM-DD format.');
  }

  const [year, month, day] = text.split('-').map(Number);
  const dt = new Date(Date.UTC(year, month - 1, day));
  if (
    Number.isNaN(dt.getTime()) ||
    dt.getUTCFullYear() !== year ||
    dt.getUTCMonth() + 1 !== month ||
    dt.getUTCDate() !== day
  ) {
    throw new Error('Date must be a real calendar day in YYYY-MM-DD format.');
  }

  return text;
}

function formatEtDate(ms) {
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });

  const parts = formatter.formatToParts(new Date(ms));
  const map = {};
  for (const part of parts) {
    if (part.type !== 'literal') map[part.type] = part.value;
  }
  return `${map.year}-${map.month}-${map.day}`;
}

function getTodayEtDate(now = Date.now()) {
  return formatEtDate(now);
}

function dateEtToNasdaqQuery(dateEt) {
  const validated = validateIsoDate(dateEt);
  const [year, month, day] = validated.split('-');
  return `${month}${day}${year}`;
}

function isoDateToFeedDate(dateEt) {
  const validated = validateIsoDate(dateEt);
  const [year, month, day] = validated.split('-');
  return `${month}/${day}/${year}`;
}

function nextIsoDate(dateEt) {
  const validated = validateIsoDate(dateEt);
  const [year, month, day] = validated.split('-').map(Number);
  const next = new Date(Date.UTC(year, month - 1, day + 1));
  return next.toISOString().slice(0, 10);
}

function feedDateToIso(dateText) {
  const trimmed = String(dateText || '').trim();
  if (!trimmed) return null;
  const match = trimmed.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!match) return null;
  return `${match[3]}-${match[1]}-${match[2]}`;
}

function toUtcIso(ms) {
  if (!Number.isFinite(ms)) return null;
  return new Date(ms).toISOString();
}

function durationSeconds(startMs, endMs) {
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs < startMs) return null;
  return Math.round((endMs - startMs) / 1000);
}

function buildEventId(item) {
  const key = [
    item.symbol || '',
    feedDateToIso(item.raw?.haltDate) || '',
    String(item.raw?.haltTime || '').trim(),
    item.reasonCode || '',
    item.market || '',
  ].join('|');

  return crypto.createHash('sha256').update(key).digest('hex');
}

function mergeRawJson(existingRaw, candidateRaw) {
  const existing = existingRaw ? JSON.parse(existingRaw) : {};
  const candidate = typeof candidateRaw === 'string' ? JSON.parse(candidateRaw) : candidateRaw;
  return JSON.stringify({
    sources: {
      ...(existing.sources || {}),
      ...(candidate.sources || {}),
    },
  });
}

function rankStatus(value) {
  return STATUS_RANK[String(value || '').trim()] ?? 0;
}

function bestValue(primary, secondary) {
  if (primary === null || primary === undefined || primary === '') return secondary ?? null;
  return primary;
}

function mergeRecord(existing, candidate) {
  if (!existing) return candidate;

  return {
    event_id: existing.event_id,
    symbol: bestValue(existing.symbol, candidate.symbol),
    issue_name: bestValue(candidate.issue_name, existing.issue_name),
    market: bestValue(candidate.market, existing.market),
    reason_code: bestValue(existing.reason_code, candidate.reason_code),
    reason_label: bestValue(candidate.reason_label, existing.reason_label),
    is_volatility_pause: Number(candidate.is_volatility_pause || existing.is_volatility_pause || 0),
    halt_date_et: bestValue(existing.halt_date_et, candidate.halt_date_et),
    halt_time_et: bestValue(existing.halt_time_et, candidate.halt_time_et),
    halted_at_utc: bestValue(existing.halted_at_utc, candidate.halted_at_utc),
    resumption_date_et: bestValue(candidate.resumption_date_et, existing.resumption_date_et),
    resumption_quote_time_et: bestValue(candidate.resumption_quote_time_et, existing.resumption_quote_time_et),
    resumption_trade_time_et: bestValue(candidate.resumption_trade_time_et, existing.resumption_trade_time_et),
    resumption_quote_at_utc: bestValue(candidate.resumption_quote_at_utc, existing.resumption_quote_at_utc),
    resumption_trade_at_utc: bestValue(candidate.resumption_trade_at_utc, existing.resumption_trade_at_utc),
    duration_seconds_to_quote: bestValue(candidate.duration_seconds_to_quote, existing.duration_seconds_to_quote),
    duration_seconds_to_trade: bestValue(candidate.duration_seconds_to_trade, existing.duration_seconds_to_trade),
    pause_threshold_price: bestValue(candidate.pause_threshold_price, existing.pause_threshold_price),
    status: rankStatus(candidate.status) >= rankStatus(existing.status) ? candidate.status : existing.status,
    first_seen_at_utc: existing.first_seen_at_utc,
    last_seen_at_utc: candidate.last_seen_at_utc,
    last_source_date_et: candidate.last_source_date_et,
    raw_json: mergeRawJson(existing.raw_json, candidate.raw_json),
  };
}

function normalizeItemToRecord(item, { sourceName, targetDateEt, seenAtUtc }) {
  const eventId = buildEventId(item);
  const rawPayload = {
    sources: {
      [sourceName]: {
        raw: item.raw,
        timer: item.timer,
      },
    },
  };

  return {
    event_id: eventId,
    symbol: item.symbol,
    issue_name: item.issueName || null,
    market: item.market || null,
    reason_code: item.reasonCode,
    reason_label: item.reasonLabel || null,
    is_volatility_pause: item.isVolatilityPause ? 1 : 0,
    halt_date_et: feedDateToIso(item.raw?.haltDate),
    halt_time_et: String(item.raw?.haltTime || '').trim(),
    halted_at_utc: toUtcIso(item.haltedAt),
    resumption_date_et: feedDateToIso(item.raw?.resumptionDate),
    resumption_quote_time_et: String(item.raw?.resumptionQuoteTime || '').trim() || null,
    resumption_trade_time_et: String(item.raw?.resumptionTradeTime || '').trim() || null,
    resumption_quote_at_utc: toUtcIso(item.resumptionQuoteAt),
    resumption_trade_at_utc: toUtcIso(item.resumptionTradeAt),
    duration_seconds_to_quote: durationSeconds(item.haltedAt, item.resumptionQuoteAt),
    duration_seconds_to_trade: durationSeconds(item.haltedAt, item.resumptionTradeAt),
    pause_threshold_price: item.pauseThresholdPrice || null,
    status: item.timer?.state || 'UNKNOWN',
    first_seen_at_utc: seenAtUtc,
    last_seen_at_utc: seenAtUtc,
    last_source_date_et: targetDateEt,
    raw_json: JSON.stringify(rawPayload),
  };
}

function recordToSqlAssignments(record) {
  return [
    `symbol=${toSqlValue(record.symbol)}`,
    `issue_name=${toSqlValue(record.issue_name)}`,
    `market=${toSqlValue(record.market)}`,
    `reason_code=${toSqlValue(record.reason_code)}`,
    `reason_label=${toSqlValue(record.reason_label)}`,
    `is_volatility_pause=${toSqlValue(record.is_volatility_pause)}`,
    `halt_date_et=${toSqlValue(record.halt_date_et)}`,
    `halt_time_et=${toSqlValue(record.halt_time_et)}`,
    `halted_at_utc=${toSqlValue(record.halted_at_utc)}`,
    `resumption_date_et=${toSqlValue(record.resumption_date_et)}`,
    `resumption_quote_time_et=${toSqlValue(record.resumption_quote_time_et)}`,
    `resumption_trade_time_et=${toSqlValue(record.resumption_trade_time_et)}`,
    `resumption_quote_at_utc=${toSqlValue(record.resumption_quote_at_utc)}`,
    `resumption_trade_at_utc=${toSqlValue(record.resumption_trade_at_utc)}`,
    `duration_seconds_to_quote=${toSqlValue(record.duration_seconds_to_quote)}`,
    `duration_seconds_to_trade=${toSqlValue(record.duration_seconds_to_trade)}`,
    `pause_threshold_price=${toSqlValue(record.pause_threshold_price)}`,
    `status=${toSqlValue(record.status)}`,
    `first_seen_at_utc=${toSqlValue(record.first_seen_at_utc)}`,
    `last_seen_at_utc=${toSqlValue(record.last_seen_at_utc)}`,
    `last_source_date_et=${toSqlValue(record.last_source_date_et)}`,
    `raw_json=${toSqlValue(record.raw_json)}`,
  ];
}

function initializeDatabase(dbPath) {
  ensureParentDir(dbPath);
  const schema = readFileSync(DB_SCHEMA_PATH, 'utf8');
  runSql(dbPath, schema);
}

function readExistingEvent(dbPath, eventId) {
  const rows = queryJson(
    dbPath,
    `SELECT * FROM halt_events WHERE event_id = ${toSqlValue(eventId)} LIMIT 1;`
  );
  return rows[0] || null;
}

function insertEvent(dbPath, record) {
  const columns = Object.keys(record);
  const values = columns.map((key) => toSqlValue(record[key]));
  runSql(
    dbPath,
    `
      INSERT INTO halt_events (${columns.join(', ')})
      VALUES (${values.join(', ')});
    `
  );
}

function updateEvent(dbPath, record) {
  runSql(
    dbPath,
    `
      UPDATE halt_events
      SET ${recordToSqlAssignments(record).join(', ')}
      WHERE event_id = ${toSqlValue(record.event_id)};
    `
  );
}

function insertRunRecord(dbPath, record) {
  const columns = Object.keys(record);
  const values = columns.map((key) => toSqlValue(record[key]));
  runSql(
    dbPath,
    `
      INSERT INTO ingestion_runs (${columns.join(', ')})
      VALUES (${values.join(', ')});
    `
  );
}

async function ingestDate({ dbPath, targetDateEt, fetchImpl = global.fetch, now }) {
  const validatedDate = validateIsoDate(targetDateEt);
  const effectiveNow =
    typeof now === 'number' && Number.isFinite(now)
      ? now
      : parseEasternDateTime(isoDateToFeedDate(validatedDate), '23:59:59');
  initializeDatabase(dbPath);

  const runId = crypto.randomUUID();
  const startedAtUtc = new Date(effectiveNow).toISOString();
  let rowsSeen = 0;
  let rowsInserted = 0;
  let rowsUpdated = 0;

  try {
    const queryDate = dateEtToNasdaqQuery(validatedDate);
    const [haltSnapshot, resumeSnapshot] = await Promise.all([
      fetchFeedSnapshot({ haltdate: queryDate, now: effectiveNow, fetchImpl }),
      fetchFeedSnapshot({ resumedate: queryDate, now: effectiveNow, fetchImpl }),
    ]);

    rowsSeen = haltSnapshot.items.length + resumeSnapshot.items.length;
    const byEventId = new Map();

    for (const item of haltSnapshot.items) {
      const record = normalizeItemToRecord(item, {
        sourceName: 'haltdate',
        targetDateEt: validatedDate,
        seenAtUtc: startedAtUtc,
      });
      const existing = byEventId.get(record.event_id);
      byEventId.set(record.event_id, mergeRecord(existing, record));
    }

    for (const item of resumeSnapshot.items) {
      const record = normalizeItemToRecord(item, {
        sourceName: 'resumedate',
        targetDateEt: validatedDate,
        seenAtUtc: startedAtUtc,
      });
      const existing = byEventId.get(record.event_id);
      byEventId.set(record.event_id, mergeRecord(existing, record));
    }

    for (const record of byEventId.values()) {
      const existing = readExistingEvent(dbPath, record.event_id);
      if (!existing) {
        insertEvent(dbPath, record);
        rowsInserted += 1;
        continue;
      }

      const merged = mergeRecord(existing, record);
      updateEvent(dbPath, merged);
      rowsUpdated += 1;
    }

    const finishedAtUtc = new Date().toISOString();
    insertRunRecord(dbPath, {
      run_id: runId,
      run_type: 'daily_ingest',
      target_date_et: validatedDate,
      started_at_utc: startedAtUtc,
      finished_at_utc: finishedAtUtc,
      rows_seen: rowsSeen,
      rows_inserted: rowsInserted,
      rows_updated: rowsUpdated,
      ok: 1,
      error_text: null,
    });

    return {
      runId,
      targetDateEt: validatedDate,
      rowsSeen,
      rowsInserted,
      rowsUpdated,
      uniqueEvents: byEventId.size,
      dbPath,
      sources: {
        haltdate: haltSnapshot.source,
        resumedate: resumeSnapshot.source,
      },
    };
  } catch (error) {
    const finishedAtUtc = new Date().toISOString();
    insertRunRecord(dbPath, {
      run_id: runId,
      run_type: 'daily_ingest',
      target_date_et: validatedDate,
      started_at_utc: startedAtUtc,
      finished_at_utc: finishedAtUtc,
      rows_seen: rowsSeen,
      rows_inserted: rowsInserted,
      rows_updated: rowsUpdated,
      ok: 0,
      error_text: String(error?.message || error),
    });
    throw error;
  }
}

function exportCsv(dbPath) {
  const csv = queryCsv(
    dbPath,
    `
      SELECT
        ${EXPORT_COLUMNS.join(',\n        ')}
      FROM halt_events
      ORDER BY halted_at_utc, symbol;
    `
  );
  return csv || `${EXPORT_COLUMNS.join(',')}\n`;
}

module.exports = {
  STATUS_RANK,
  validateIsoDate,
  getTodayEtDate,
  dateEtToNasdaqQuery,
  isoDateToFeedDate,
  nextIsoDate,
  feedDateToIso,
  durationSeconds,
  buildEventId,
  normalizeItemToRecord,
  mergeRecord,
  initializeDatabase,
  ingestDate,
  exportCsv,
  _test: {
    formatEtDate,
    toUtcIso,
  },
};
