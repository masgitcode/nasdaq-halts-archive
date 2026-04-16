const { join } = require('node:path');

const { exportCsv } = require('./ingest');

const DEFAULT_DB_PATH = join(__dirname, '..', 'data', 'halts.sqlite');

function parseArgs(argv) {
  const args = {
    dbPath: DEFAULT_DB_PATH,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === '--db') {
      args.dbPath = String(argv[index + 1] || '').trim() || DEFAULT_DB_PATH;
      index += 1;
      continue;
    }
  }

  return args;
}

function main(options = {}) {
  const argv = options.argv || process.argv.slice(2);
  const stdout = options.stdout || process.stdout;
  const args = parseArgs(argv);
  stdout.write(exportCsv(args.dbPath));
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(String(error?.message || error));
    process.exitCode = 1;
  }
}

module.exports = {
  DEFAULT_DB_PATH,
  parseArgs,
  main,
};
