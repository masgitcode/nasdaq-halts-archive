const { join } = require('node:path');

const { ingestDate, nextIsoDate, validateIsoDate } = require('./ingest');

const DEFAULT_DB_PATH = join(__dirname, '..', 'data', 'halts.sqlite');

function parseArgs(argv) {
  const args = {
    from: '',
    to: '',
    dbPath: DEFAULT_DB_PATH,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === '--from') {
      args.from = String(argv[index + 1] || '').trim();
      index += 1;
      continue;
    }
    if (token === '--to') {
      args.to = String(argv[index + 1] || '').trim();
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
    'Usage: node src/backfill.js --from YYYY-MM-DD --to YYYY-MM-DD [--db path/to/halts.sqlite]',
    '',
    'Runs single-day ingest for every date in the inclusive range.',
  ].join('\n');
}

async function main(options = {}) {
  const argv = options.argv || process.argv.slice(2);
  const stdout = options.stdout || process.stdout;
  const fetchImpl = options.fetchImpl || global.fetch;
  const args = parseArgs(argv);

  if (args.help) {
    stdout.write(`${helpText()}\n`);
    return [];
  }

  const from = validateIsoDate(args.from);
  const to = validateIsoDate(args.to);
  if (from > to) {
    throw new Error('--from must be less than or equal to --to.');
  }

  const results = [];
  let current = from;
  while (current <= to) {
    const result = await ingestDate({
      dbPath: args.dbPath,
      targetDateEt: current,
      fetchImpl,
    });
    results.push(result);
    current = nextIsoDate(current);
  }

  stdout.write(`${JSON.stringify(results, null, 2)}\n`);
  return results;
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
