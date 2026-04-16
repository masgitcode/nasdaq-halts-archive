const test = require('node:test');
const assert = require('node:assert/strict');

const nasdaq = require('../src/nasdaq');

const SAMPLE_XML = `<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">
  <channel>
    <title>NASDAQTrader.com</title>
    <pubDate>Thu, 26 Mar 2026 20:00:00 GMT</pubDate>
    <ndaq:numItems>2</ndaq:numItems>
    <item>
      <title>ABCD</title>
      <ndaq:HaltDate>03/26/2026</ndaq:HaltDate>
      <ndaq:HaltTime>09:35:11.321</ndaq:HaltTime>
      <ndaq:IssueSymbol>ABCD</ndaq:IssueSymbol>
      <ndaq:IssueName>Alpha Beta Co</ndaq:IssueName>
      <ndaq:Market>NASDAQ</ndaq:Market>
      <ndaq:ReasonCode>LUDP</ndaq:ReasonCode>
      <ndaq:ResumptionDate>03/26/2026</ndaq:ResumptionDate>
      <ndaq:ResumptionQuoteTime>09:35:11</ndaq:ResumptionQuoteTime>
      <ndaq:ResumptionTradeTime>09:40:11</ndaq:ResumptionTradeTime>
    </item>
    <item>
      <title>WXYZ</title>
      <ndaq:HaltDate>03/25/2026</ndaq:HaltDate>
      <ndaq:HaltTime>15:58:00</ndaq:HaltTime>
      <ndaq:IssueSymbol>WXYZ</ndaq:IssueSymbol>
      <ndaq:IssueName>Window XYZ Inc</ndaq:IssueName>
      <ndaq:Mkt>Q</ndaq:Mkt>
      <ndaq:ReasonCode>T1</ndaq:ReasonCode>
      <ndaq:ResumptionDate />
      <ndaq:ResumptionQuoteTime />
      <ndaq:ResumptionTradeTime />
    </item>
  </channel>
</rss>`;

test('buildFeedUrl supports haltdate and resumedate modes', () => {
  assert.equal(
    nasdaq.buildFeedUrl({ haltdate: '03262026' }),
    'https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts&haltdate=03262026'
  );
  assert.equal(
    nasdaq.buildFeedUrl({ resumedate: '03262026' }),
    'https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts&resumedate=03262026'
  );
});

test('parseHaltFeed normalizes historical and current rows', () => {
  const items = nasdaq.parseHaltFeed(SAMPLE_XML, Date.UTC(2026, 2, 26, 20, 0, 0));
  assert.equal(items.length, 2);

  const volatilityRow = items.find((item) => item.symbol === 'ABCD');
  assert.equal(volatilityRow.reasonCode, 'LUDP');
  assert.equal(volatilityRow.isVolatilityPause, true);
  assert.equal(volatilityRow.timer.state, 'RESUMED');
  assert.equal(volatilityRow.raw.haltTime, '09:35:11.321');

  const currentRow = items.find((item) => item.symbol === 'WXYZ');
  assert.equal(currentRow.market, 'Q');
  assert.equal(currentRow.timer.state, 'HALTED');
  assert.equal(currentRow.currentlyHalted, true);
});

test('parseEasternDateTime preserves fractional seconds in ET', () => {
  const ms = nasdaq.parseEasternDateTime('03/26/2026', '09:35:11.321');
  assert.equal(Number.isFinite(ms), true);
  assert.equal(new Date(ms).toISOString(), '2026-03-26T13:35:11.321Z');
});
