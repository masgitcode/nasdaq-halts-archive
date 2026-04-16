const FEED_BASE_URL = 'https://www.nasdaqtrader.com/rss.aspx';
const FEED_PROVIDER = 'NASDAQ Trader RSS';
const FEED_TIMEZONE = 'America/New_York';
const ROLLOVER_CONFIRM_MS = 60 * 1000;
const VOLATILITY_WINDOW_MS = 5 * 60 * 1000;
const MAX_ITEMS = 600;

const VOLATILITY_CODES = new Set(['LUDP', 'LUDS', 'M', 'T5', 'T7']);

const REASON_LABELS = {
  LUDP: 'Volatility Trading Pause',
  LUDS: 'Volatility Pause (Straddle)',
  M: 'Volatility Trading Pause',
  T5: 'Single Stock Trading Pause',
  T7: 'Quotation-Only Pause',
  T1: 'News Pending',
  T2: 'News Released',
  T3: 'News and Resumption Times',
  T6: 'Extraordinary Market Activity',
  T8: 'ETF Halt',
  T12: 'Additional Information Requested',
  H4: 'Non-compliance Halt',
  H9: 'Not Current Halt',
  H10: 'SEC Trading Suspension',
  H11: 'Regulatory Concern Halt',
  O1: 'Operations Halt',
  IPO1: 'IPO Issue Not Yet Trading',
  R4: 'Qualifications Reviewed/Resolved',
  R9: 'Filings Satisfied/Resolved',
  C3: 'Issuer News Not Forthcoming',
  C4: 'Qualifications Halt Ended',
  C9: 'Qualifications Halt Concluded',
  C11: 'Halt Concluded by Other Authority',
  R1: 'New Issue Available',
  R2: 'Issue Available',
  IPOD: 'IPO Security Released',
  IPOE: 'IPO Positioning Extension',
  D: 'Delisted/Deletion',
};

const formatterCache = new Map();

function secUserAgent() {
  return process.env.NASDAQ_USER_AGENT || process.env.SEC_USER_AGENT || 'NasdaqHaltsArchive support@example.com';
}

function stripCdata(value) {
  return String(value || '')
    .replace(/^<!\[CDATA\[/, '')
    .replace(/\]\]>$/, '')
    .trim();
}

function decodeHtml(value) {
  return String(value || '')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"');
}

function pickTagValue(block, tag) {
  const match = String(block || '').match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, 'i'));
  return match ? decodeHtml(stripCdata(match[1])) : '';
}

function parseRssItems(xml) {
  const rows = [];
  const pattern = /<item[^>]*>([\s\S]*?)<\/item>/gi;
  let match = pattern.exec(String(xml || ''));
  while (match) {
    rows.push(match[1]);
    match = pattern.exec(String(xml || ''));
  }
  return rows;
}

function pickFirstTagValue(block, tags) {
  for (const tag of tags) {
    const value = pickTagValue(block, tag);
    if (String(value || '').trim()) return String(value).trim();
  }
  return '';
}

function normalizeReasonCode(value) {
  return String(value || '').trim().toUpperCase();
}

function buildFeedUrl(params = {}) {
  const url = new URL(FEED_BASE_URL);
  url.searchParams.set('feed', 'tradehalts');

  const haltdate = String(params.haltdate || '').trim();
  const resumedate = String(params.resumedate || '').trim();
  if (haltdate) url.searchParams.set('haltdate', haltdate);
  if (resumedate) url.searchParams.set('resumedate', resumedate);

  return url.toString();
}

function extractFeedPubDate(xml) {
  const match = String(xml || '').match(/<pubDate>([\s\S]*?)<\/pubDate>/i);
  return match ? String(match[1] || '').trim() : '';
}

function extractNumItems(xml) {
  const match = String(xml || '').match(/<ndaq:numItems>([\s\S]*?)<\/ndaq:numItems>/i);
  if (!match) return null;
  const value = Number(String(match[1] || '').trim());
  return Number.isFinite(value) ? value : null;
}

function parseDateParts(dateText) {
  const match = String(dateText || '').trim().match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!match) return null;
  return {
    month: Number(match[1]),
    day: Number(match[2]),
    year: Number(match[3]),
  };
}

function parseTimeParts(timeText) {
  const match = String(timeText || '')
    .trim()
    .match(/^(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,3}))?$/);
  if (!match) return null;
  return {
    hour: Number(match[1]),
    minute: Number(match[2]),
    second: Number(match[3]),
    millisecond: Number(String(match[4] || '').padEnd(3, '0') || 0),
  };
}

function getEtFormatter() {
  const key = 'et_full';
  if (formatterCache.has(key)) return formatterCache.get(key);
  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone: FEED_TIMEZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
  formatterCache.set(key, formatter);
  return formatter;
}

function formatEt(ms) {
  if (!Number.isFinite(ms) || ms <= 0) return '';
  return getEtFormatter().format(new Date(ms));
}

function getTimeZoneOffsetMs(utcMs, timeZone) {
  const wholeSecondUtcMs = Math.floor(utcMs / 1000) * 1000;
  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });

  const parts = formatter.formatToParts(new Date(wholeSecondUtcMs));
  const map = {};
  for (const part of parts) {
    if (part.type !== 'literal') map[part.type] = part.value;
  }

  const asUtc = Date.UTC(
    Number(map.year),
    Number(map.month) - 1,
    Number(map.day),
    Number(map.hour),
    Number(map.minute),
    Number(map.second)
  );

  return asUtc - wholeSecondUtcMs;
}

function zonedLocalToUtcMs(parts, timeZone) {
  const baseUtc =
    Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second) + (parts.millisecond || 0);

  let candidate = baseUtc;
  for (let index = 0; index < 4; index += 1) {
    const offset = getTimeZoneOffsetMs(candidate, timeZone);
    const next = baseUtc - offset;
    if (Math.abs(next - candidate) < 1000) return next;
    candidate = next;
  }
  return candidate;
}

function parseEasternDateTime(dateText, timeText) {
  const date = parseDateParts(dateText);
  const time = parseTimeParts(timeText);
  if (!date || !time) return null;

  const utcMs = zonedLocalToUtcMs(
    {
      year: date.year,
      month: date.month,
      day: date.day,
      hour: time.hour,
      minute: time.minute,
      second: time.second,
      millisecond: time.millisecond,
    },
    FEED_TIMEZONE
  );

  return Number.isFinite(utcMs) ? utcMs : null;
}

function truncateToSecond(ms) {
  if (!Number.isFinite(ms) || ms <= 0) return null;
  return Math.floor(ms / 1000) * 1000;
}

function buildTimer({
  hasOfficialResumeFields,
  hasResumeParseFailure,
  isVolatilityPause,
  haltedAt,
  observedAt,
  resumptionQuoteAt,
  resumptionTradeAt,
  now,
}) {
  if (Number.isFinite(resumptionTradeAt)) {
    const overdue = now >= resumptionTradeAt;
    return {
      model: 'nasdaq-rss-v1',
      state: overdue ? 'RESUMED' : 'RESUME_SCHEDULED',
      nextExpectedResumeAt: resumptionTradeAt,
      secondsRemaining: Math.max(0, Math.floor((resumptionTradeAt - now) / 1000)),
      windowNumber: null,
      rollingWindowMs: null,
      overdue,
    };
  }

  if (isVolatilityPause && Number.isFinite(haltedAt)) {
    const confirmedAt = Number.isFinite(observedAt) ? observedAt : now;
    const anchorAt = resumptionQuoteAt || truncateToSecond(haltedAt) || haltedAt;
    const elapsedMs = Math.max(0, confirmedAt - anchorAt - ROLLOVER_CONFIRM_MS);
    const windowNumber = Math.floor(elapsedMs / VOLATILITY_WINDOW_MS) + 1;
    const targetAt = anchorAt + windowNumber * VOLATILITY_WINDOW_MS;
    const overdue = now >= targetAt;
    return {
      model: 'nasdaq-rss-v1',
      state: 'HALTED',
      nextExpectedResumeAt: targetAt,
      secondsRemaining: Math.max(0, Math.floor((targetAt - now) / 1000)),
      windowNumber,
      rollingWindowMs: VOLATILITY_WINDOW_MS,
      overdue,
    };
  }

  if (Number.isFinite(resumptionQuoteAt)) {
    const overdue = now >= resumptionQuoteAt;
    return {
      model: 'nasdaq-rss-v1',
      state: 'HALTED',
      nextExpectedResumeAt: resumptionQuoteAt,
      secondsRemaining: Math.max(0, Math.floor((resumptionQuoteAt - now) / 1000)),
      windowNumber: null,
      rollingWindowMs: null,
      overdue,
    };
  }

  if (hasOfficialResumeFields && hasResumeParseFailure) {
    return {
      model: 'nasdaq-rss-v1',
      state: 'UNKNOWN',
      nextExpectedResumeAt: null,
      secondsRemaining: 0,
      windowNumber: null,
      rollingWindowMs: null,
      overdue: false,
    };
  }

  return {
    model: 'nasdaq-rss-v1',
    state: 'HALTED',
    nextExpectedResumeAt: null,
    secondsRemaining: 0,
    windowNumber: null,
    rollingWindowMs: null,
    overdue: false,
  };
}

function isCurrentlyHalted(item) {
  if (!item) return false;
  const haltTime = String(item.raw?.haltTime || '').trim();
  const resumptionTradeTime = String(item.raw?.resumptionTradeTime || '').trim();
  return Boolean(haltTime) && !resumptionTradeTime;
}

function buildItemId(item) {
  return [
    item.symbol || '',
    item.raw?.haltDate || '',
    item.raw?.haltTime || '',
    item.reasonCode || '',
    item.raw?.resumptionDate || '',
    item.raw?.resumptionTradeTime || '',
    item.raw?.resumptionQuoteTime || '',
    item.market || '',
  ].join('|');
}

function dedupeExactItems(items) {
  const map = new Map();
  for (const item of items) {
    const id = buildItemId(item);
    if (!map.has(id)) map.set(id, { ...item, id });
  }
  return Array.from(map.values());
}

function parseHaltFeed(xml, now, options = {}) {
  const observedAt = Number.isFinite(options.observedAt) ? options.observedAt : now;
  const blocks = parseRssItems(xml).slice(0, MAX_ITEMS);
  const items = [];

  for (const block of blocks) {
    const symbol = pickFirstTagValue(block, ['ndaq:IssueSymbol', 'IssueSymbol']).toUpperCase();
    if (!symbol) continue;

    const issueName = pickFirstTagValue(block, ['ndaq:IssueName', 'IssueName']);
    const market = pickFirstTagValue(block, ['ndaq:Market', 'ndaq:Mkt', 'Market', 'Mkt']);
    const reasonCode = normalizeReasonCode(pickFirstTagValue(block, ['ndaq:ReasonCode', 'ReasonCode']));
    const pauseThresholdPrice = pickFirstTagValue(block, ['ndaq:PauseThresholdPrice', 'PauseThresholdPrice']);
    const haltDate = pickFirstTagValue(block, ['ndaq:HaltDate', 'HaltDate']);
    const haltTime = pickFirstTagValue(block, ['ndaq:HaltTime', 'HaltTime']);
    const resumptionDate = pickFirstTagValue(block, ['ndaq:ResumptionDate', 'ResumptionDate']);
    const resumptionQuoteTime = pickFirstTagValue(block, ['ndaq:ResumptionQuoteTime', 'ResumptionQuoteTime']);
    const resumptionTradeTime = pickFirstTagValue(block, ['ndaq:ResumptionTradeTime', 'ResumptionTradeTime']);

    const haltedAt = parseEasternDateTime(haltDate, haltTime);
    const parseDate = resumptionDate || haltDate;
    const resumptionQuoteAt = parseEasternDateTime(parseDate, resumptionQuoteTime);
    const resumptionTradeAt = parseEasternDateTime(parseDate, resumptionTradeTime);
    const hasOfficialResumeFields = Boolean(resumptionDate || resumptionQuoteTime || resumptionTradeTime);
    const hasResumeParseFailure =
      Boolean(resumptionQuoteTime || resumptionTradeTime) && !Number.isFinite(resumptionQuoteAt) && !Number.isFinite(resumptionTradeAt);

    const timer = buildTimer({
      hasOfficialResumeFields,
      hasResumeParseFailure,
      isVolatilityPause: VOLATILITY_CODES.has(reasonCode),
      haltedAt,
      observedAt,
      resumptionQuoteAt,
      resumptionTradeAt,
      now,
    });

    items.push({
      id: '',
      symbol,
      issueName,
      market,
      reasonCode,
      reasonLabel: REASON_LABELS[reasonCode] || 'Other halt',
      isVolatilityPause: VOLATILITY_CODES.has(reasonCode),
      pauseThresholdPrice: pauseThresholdPrice || null,
      haltedAt,
      haltedAtEt: formatEt(haltedAt),
      resumptionQuoteAt,
      resumptionQuoteAtEt: formatEt(resumptionQuoteAt),
      resumptionTradeAt,
      resumptionTradeAtEt: formatEt(resumptionTradeAt),
      timer,
      currentlyHalted: false,
      raw: {
        haltDate,
        haltTime,
        resumptionDate,
        resumptionQuoteTime,
        resumptionTradeTime,
        issueName,
        market,
        reasonCode,
        pauseThresholdPrice,
      },
    });
  }

  return dedupeExactItems(items).map((item) => ({
    ...item,
    currentlyHalted: isCurrentlyHalted(item),
  }));
}

async function fetchFeedXml(url, fetchImpl = global.fetch) {
  if (typeof fetchImpl !== 'function') {
    throw new Error('A fetch implementation is required.');
  }

  const response = await fetchImpl(url, {
    headers: {
      'User-Agent': secUserAgent(),
      Accept: 'application/rss+xml, application/xml;q=0.9, text/xml;q=0.8,*/*;q=0.5',
    },
  });

  if (!response.ok) {
    throw new Error(`Nasdaq halt feed failed ${response.status}`);
  }

  return response.text();
}

async function fetchFeedSnapshot({ haltdate = '', resumedate = '', now = Date.now(), fetchImpl = global.fetch }) {
  const feedUrl = buildFeedUrl({ haltdate, resumedate });
  const xml = await fetchFeedXml(feedUrl, fetchImpl);
  const feedPubDate = extractFeedPubDate(xml);
  const feedObservedAt = Date.parse(feedPubDate || '');
  const observedAt = Number.isFinite(feedObservedAt) ? feedObservedAt : now;

  return {
    items: parseHaltFeed(xml, now, { observedAt }),
    totalParsed: parseRssItems(xml).slice(0, MAX_ITEMS).length,
    source: {
      provider: FEED_PROVIDER,
      feedUrl,
      feedPubDate,
      feedItemCount: extractNumItems(xml),
    },
  };
}

module.exports = {
  FEED_TIMEZONE,
  REASON_LABELS,
  VOLATILITY_CODES,
  buildFeedUrl,
  buildTimer,
  parseEasternDateTime,
  parseHaltFeed,
  fetchFeedSnapshot,
  _test: {
    decodeHtml,
    stripCdata,
    pickTagValue,
    parseRssItems,
    extractFeedPubDate,
    extractNumItems,
  },
};
