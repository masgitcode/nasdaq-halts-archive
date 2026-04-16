const { spawnSync } = require('node:child_process');
const { existsSync, mkdirSync } = require('node:fs');
const { dirname } = require('node:path');

function ensureParentDir(filePath) {
  mkdirSync(dirname(filePath), { recursive: true });
}

function runSqlite(args, input = '') {
  const result = spawnSync('sqlite3', args, {
    input,
    encoding: 'utf8',
  });

  if (result.error) throw result.error;
  if (result.status !== 0) {
    const message = (result.stderr || result.stdout || '').trim() || `sqlite3 exited ${result.status}`;
    throw new Error(message);
  }

  return {
    stdout: result.stdout || '',
    stderr: result.stderr || '',
  };
}

function runSql(dbPath, sql) {
  ensureParentDir(dbPath);
  return runSqlite([dbPath], `${sql.trim()}\n`);
}

function queryJson(dbPath, sql) {
  ensureParentDir(dbPath);
  const { stdout } = runSqlite(['-json', dbPath], `${sql.trim()}\n`);
  const trimmed = stdout.trim();
  return trimmed ? JSON.parse(trimmed) : [];
}

function queryCsv(dbPath, sql) {
  ensureParentDir(dbPath);
  const { stdout } = runSqlite(['-header', '-csv', dbPath], `${sql.trim()}\n`);
  return stdout;
}

function toSqlValue(value) {
  if (value === null || value === undefined || value === '') return 'NULL';
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return 'NULL';
    return String(value);
  }
  if (typeof value === 'boolean') return value ? '1' : '0';
  return `'${String(value).replace(/'/g, "''")}'`;
}

function fileExists(filePath) {
  return existsSync(filePath);
}

module.exports = {
  ensureParentDir,
  runSql,
  queryJson,
  queryCsv,
  toSqlValue,
  fileExists,
};
