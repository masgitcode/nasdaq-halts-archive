const { join } = require('node:path');

const { ingestDate, getTodayEtDate } = require('./ingest');

const DEFAULT_DB_PATH = join(__dirname, '..', 'data', 'halts.sqlite');

function parseArgs(argv) {
  const args = {
    date: '',
    dbPath: DEFAULT_DB_PATH,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === '--date') {
      args.date = String(argv[index + 1] || '').trim();
      index += 1;
      continue;
    }
    if (token === '--db') {
      args.dbPath = String(argv[index + 1] || '').trim() || DEFAULT_DB_PATH;
      index += 1;
      continue;
    }
    if (token === '--help' || token === '-h') {
      args.help = true;
    }
  }

  return args;
}

function helpText() {
  return [
    'Usage: node src/ingest-day.js [--date YYYY-MM-DD] [--db path/to/halts.sqlite]',
    '',
    'If --date is omitted, the current America/New_York date is used.',
  ].join('\n');
}

async function main(options = {}) {
  const argv = options.argv || process.argv.slice(2);
  const stdout = options.stdout || process.stdout;
  const fetchImpl = options.fetchImpl || global.fetch;

  const args = parseArgs(argv);
  if (args.help) {
    stdout.write(`${helpText()}\n`);
    return null;
  }

  const targetDateEt = args.date || getTodayEtDate();
  const summary = await ingestDate({
    dbPath: args.dbPath,
    targetDateEt,
    fetchImpl,
  });

  stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
  return summary;
}

if (require.main === module) {
  main().catch((error) => {
    console.error(String(error?.message || error));
    process.exitCode = 1;
  });
}

module.exports = {
  DEFAULT_DB_PATH,
  parseArgs,
  main,
};
