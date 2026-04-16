const test = require('node:test');
const assert = require('node:assert/strict');
const { mkdtempSync } = require('node:fs');
const { join } = require('node:path');
const { tmpdir } = require('node:os');

const { queryJson } = require('../src/sqlite');
const { ingestDate, initializeDatabase } = require('../src/ingest');
const ingestDayCli = require('../src/ingest-day');

const DAY1_HALT_XML = `<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">
  <channel>
    <title>NASDAQTrader.com</title>
    <pubDate>Thu, 26 Mar 2026 20:00:00 GMT</pubDate>
    <ndaq:numItems>1</ndaq:numItems>
    <item>
      <title>ABCD</title>
      <ndaq:HaltDate>03/26/2026</ndaq:HaltDate>
      <ndaq:HaltTime>15:55:00</ndaq:HaltTime>
      <ndaq:IssueSymbol>ABCD</ndaq:IssueSymbol>
      <ndaq:IssueName>Alpha Beta Co</ndaq:IssueName>
      <ndaq:Market>NASDAQ</ndaq:Market>
      <ndaq:ReasonCode>T1</ndaq:ReasonCode>
      <ndaq:ResumptionDate />
      <ndaq:ResumptionQuoteTime />
      <ndaq:ResumptionTradeTime />
    </item>
  </channel>
</rss>`;

const EMPTY_XML = `<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">
  <channel>
    <title>NASDAQTrader.com</title>
    <pubDate>Thu, 26 Mar 2026 20:00:00 GMT</pubDate>
    <ndaq:numItems>0</ndaq:numItems>
  </channel>
</rss>`;

const DAY2_RESUME_XML = `<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">
  <channel>
    <title>NASDAQTrader.com</title>
    <pubDate>Fri, 27 Mar 2026 20:00:00 GMT</pubDate>
    <ndaq:numItems>1</ndaq:numItems>
    <item>
      <title>ABCD</title>
      <ndaq:HaltDate>03/26/2026</ndaq:HaltDate>
      <ndaq:HaltTime>15:55:00</ndaq:HaltTime>
      <ndaq:IssueSymbol>ABCD</ndaq:IssueSymbol>
      <ndaq:IssueName>Alpha Beta Co</ndaq:IssueName>
      <ndaq:Market>NASDAQ</ndaq:Market>
      <ndaq:ReasonCode>T1</ndaq:ReasonCode>
      <ndaq:ResumptionDate>03/27/2026</ndaq:ResumptionDate>
      <ndaq:ResumptionQuoteTime>09:29:00</ndaq:ResumptionQuoteTime>
      <ndaq:ResumptionTradeTime>09:30:00</ndaq:ResumptionTradeTime>
    </item>
  </channel>
</rss>`;

function tempDbPath() {
  const dir = mkdtempSync(join(tmpdir(), 'nasdaq-halts-archive-'));
  return join(dir, 'halts.sqlite');
}

function makeFetch(fixturesByDate) {
  return async (url) => {
    const parsed = new URL(url);
    const haltdate = parsed.searchParams.get('haltdate') || '';
    const resumedate = parsed.searchParams.get('resumedate') || '';
    const key = haltdate ? `haltdate:${haltdate}` : `resumedate:${resumedate}`;
    const xml = fixturesByDate[key];
    if (!xml) {
      throw new Error(`Missing fixture for ${key}`);
    }
    return {
      ok: true,
      text: async () => xml,
    };
  };
}

test('daily ingest is idempotent and does not duplicate events', async () => {
  const dbPath = tempDbPath();
  const fetchImpl = makeFetch({
    'haltdate:03262026': DAY1_HALT_XML,
    'resumedate:03262026': EMPTY_XML,
  });

  await ingestDate({ dbPath, targetDateEt: '2026-03-26', fetchImpl });
  await ingestDate({ dbPath, targetDateEt: '2026-03-26', fetchImpl });

  const rows = queryJson(dbPath, 'SELECT event_id, symbol, status FROM halt_events;');
  assert.equal(rows.length, 1);
  assert.equal(rows[0].symbol, 'ABCD');
  assert.equal(rows[0].status, 'HALTED');
});

test('later resumedate ingest updates an existing halt with duration fields', async () => {
  const dbPath = tempDbPath();

  await ingestDate({
    dbPath,
    targetDateEt: '2026-03-26',
    fetchImpl: makeFetch({
      'haltdate:03262026': DAY1_HALT_XML,
      'resumedate:03262026': EMPTY_XML,
    }),
  });

  await ingestDate({
    dbPath,
    targetDateEt: '2026-03-27',
    fetchImpl: makeFetch({
      'haltdate:03272026': EMPTY_XML,
      'resumedate:03272026': DAY2_RESUME_XML,
    }),
  });

  const rows = queryJson(
    dbPath,
    `
      SELECT
        symbol,
        halt_date_et,
        resumption_date_et,
        resumption_trade_time_et,
        duration_seconds_to_trade,
        status,
        last_source_date_et
      FROM halt_events;
    `
  );

  assert.equal(rows.length, 1);
  assert.equal(rows[0].symbol, 'ABCD');
  assert.equal(rows[0].halt_date_et, '2026-03-26');
  assert.equal(rows[0].resumption_date_et, '2026-03-27');
  assert.equal(rows[0].resumption_trade_time_et, '09:30:00');
  assert.equal(rows[0].status, 'RESUMED');
  assert.equal(rows[0].last_source_date_et, '2026-03-27');
  assert.ok(rows[0].duration_seconds_to_trade > 0);
});

test('ingest-day CLI rejects invalid dates clearly', async () => {
  const stdout = { write() {} };
  await assert.rejects(
    () =>
      ingestDayCli.main({
        argv: ['--date', '2026/03/26'],
        stdout,
        fetchImpl: async () => {
          throw new Error('should not fetch');
        },
      }),
    /YYYY-MM-DD/
  );
});

test('ingest-day CLI reports inserted and updated counts on success', async () => {
  const dbPath = tempDbPath();
  const chunks = [];
  const stdout = {
    write(value) {
      chunks.push(value);
    },
  };

  const summary = await ingestDayCli.main({
    argv: ['--date', '2026-03-26', '--db', dbPath],
    stdout,
    fetchImpl: makeFetch({
      'haltdate:03262026': DAY1_HALT_XML,
      'resumedate:03262026': EMPTY_XML,
    }),
  });

  const output = chunks.join('');
  assert.equal(summary.rowsInserted, 1);
  assert.match(output, /"rowsInserted": 1/);
  assert.match(output, /"rowsUpdated": 0/);
});

test('database initialization creates both tables', () => {
  const dbPath = tempDbPath();
  initializeDatabase(dbPath);
  const tables = queryJson(
    dbPath,
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('halt_events', 'ingestion_runs') ORDER BY name;"
  );
  assert.deepEqual(
    tables.map((row) => row.name),
    ['halt_events', 'ingestion_runs']
  );
});
