#!/usr/bin/env node
/*
 * Build user stats shards from item shards.
 * Output: docs/static-user-stats-shards/user_<sid>.sqlite(.gz)
 * Manifest: docs/static-user-stats-manifest.json
 */

import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import os from 'os';
import zlib from 'zlib';
import Database from 'better-sqlite3';

const BACKUP_STAMP = new Date().toISOString().replace(/[:.]/g, '-');

const DEFAULT_MANIFEST = 'docs/static-manifest.json';
const DEFAULT_SHARDS_DIR = 'docs/static-shards';
const DEFAULT_OUT_DIR = 'docs/static-user-stats-shards';
const DEFAULT_OUT_MANIFEST = 'docs/static-user-stats-manifest.json';
const DEFAULT_TARGET_MB = 15;
const DEFAULT_BATCH = 5000;
const SHARD_SIZE_CHECK_EVERY = 1000;

function usage() {
  const msg = `Usage:
  toool/s/build-user-stats.mjs [--manifest PATH] [--shards-dir PATH]
                               [--out-dir PATH] [--out-manifest PATH]
                               [--target-mb N] [--batch N]
                               [--gzip] [--keep-sqlite]
                               [--manifest-only]

Examples:
  toool/s/build-user-stats.mjs --gzip --target-mb 15
  toool/s/build-user-stats.mjs --manifest-only --gzip
`;
  process.stdout.write(msg);
}

function parseArgs(argv) {
  const out = {
    manifest: DEFAULT_MANIFEST,
    shardsDir: DEFAULT_SHARDS_DIR,
    outDir: DEFAULT_OUT_DIR,
    outManifest: DEFAULT_OUT_MANIFEST,
    targetMb: DEFAULT_TARGET_MB,
    batch: DEFAULT_BATCH,
    gzip: false,
    keepSqlite: false,
    manifestOnly: false
  };

  for (let i = 2; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--help' || a === '-h') {
      usage();
      process.exit(0);
    }
    if (!a.startsWith('--')) continue;
    const key = a.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      out[key] = true;
      continue;
    }
    out[key] = next;
    i += 1;
  }

  if (out['target-mb'] != null) out.targetMb = Number(out['target-mb']);
  if (out['batch'] != null) out.batch = Number(out['batch']);
  if (out['manifest-only'] != null) out.manifestOnly = true;
  return out;
}

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

function gzipFileSync(srcPath, dstPath) {
  const data = fs.readFileSync(srcPath);
  const gz = zlib.gzipSync(data, { level: 9 });
  const tmpPath = `${dstPath}.tmp`;
  fs.writeFileSync(tmpPath, gz);
  fs.renameSync(tmpPath, dstPath);
  return gz.length;
}

function ensureWritableOrBackup(filePath) {
  if (!fs.existsSync(filePath)) return;
  try {
    fs.accessSync(filePath, fs.constants.W_OK);
    return;
  } catch {}
  const dir = path.dirname(filePath);
  const backupDir = path.join(dir, `backups-${BACKUP_STAMP}`);
  fs.mkdirSync(backupDir, { recursive: true });
  const dest = path.join(backupDir, path.basename(filePath));
  fs.renameSync(filePath, dest);
  console.log(`[post] moved protected file to ${dest}`);
}

function validateGzipFileSync(gzPath) {
  zlib.gunzipSync(fs.readFileSync(gzPath));
}

async function gunzipToTemp(srcPath, tmpRoot) {
  const dstPath = path.join(tmpRoot, path.basename(srcPath, '.gz'));
  await new Promise((resolve, reject) => {
    const src = fs.createReadStream(srcPath);
    const gunzip = zlib.createGunzip();
    const dst = fs.createWriteStream(dstPath);
    src.on('error', reject);
    gunzip.on('error', reject);
    dst.on('error', reject);
    dst.on('finish', resolve);
    src.pipe(gunzip).pipe(dst);
  });
  return dstPath;
}

function initUserDb(dbPath) {
  if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);
  const db = new Database(dbPath);
  db.pragma('journal_mode = OFF');
  db.pragma('synchronous = OFF');
  db.exec(`
    CREATE TABLE users (
      username TEXT PRIMARY KEY,
      first_time INTEGER,
      last_time INTEGER,
      items INTEGER,
      comments INTEGER,
      stories INTEGER,
      ask INTEGER,
      show INTEGER,
      launch INTEGER,
      jobs INTEGER,
      polls INTEGER,
      avg_score REAL,
      sum_score INTEGER,
      max_score INTEGER,
      min_score INTEGER,
      max_score_id INTEGER,
      max_score_title TEXT
    );

    CREATE TABLE user_domains (
      username TEXT NOT NULL,
      domain TEXT NOT NULL,
      count INTEGER NOT NULL,
      PRIMARY KEY(username, domain)
    );

    CREATE TABLE user_months (
      username TEXT NOT NULL,
      month TEXT NOT NULL,
      count INTEGER NOT NULL,
      PRIMARY KEY(username, month)
    );
  `);
  return db;
}

function monthKey(ts) {
  if (!ts) return null;
  const d = new Date(ts * 1000);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  return `${y}-${m}`;
}

function domainFromUrl(url) {
  try {
    const u = new URL(url);
    return u.host.replace(/^www\./, '');
  } catch {
    return null;
  }
}

function lowerName(name) {
  return String(name || '').trim().toLowerCase();
}

async function buildManifestFromUserShards({ outDir, outManifest, gzipOut, targetMb }) {
  const files = fs.readdirSync(outDir)
    .map(name => {
      const m = name.match(/^user_(\d+)\.sqlite(\.gz)?$/);
      if (!m) return null;
      return { name, sid: Number(m[1]) };
    })
    .filter(Boolean)
    .sort((a, b) => a.sid - b.sid);

  if (!files.length) {
    console.error(`No user stats shards found in ${outDir}`);
    process.exit(1);
  }

  const tmpRoot = await fsp.mkdtemp(path.join(os.tmpdir(), 'static-news-user-manifest-'));
  const tempFiles = new Set();
  const shardMeta = [];
  const growthCounts = new Map();
  const activeCounts = new Map();
  let totalUsers = 0;

  try {
    for (const entry of files) {
      const shardPath = path.join(outDir, entry.name);
      let dbPath = shardPath;
      if (shardPath.endsWith('.gz')) {
        dbPath = await gunzipToTemp(shardPath, tmpRoot);
        tempFiles.add(dbPath);
      }

      const db = new Database(dbPath, { readonly: true });
      const countRow = db.prepare('SELECT COUNT(*) as c FROM users').get();
      const loRow = db.prepare('SELECT username FROM users ORDER BY username COLLATE NOCASE LIMIT 1').get();
      const hiRow = db.prepare('SELECT username FROM users ORDER BY username COLLATE NOCASE DESC LIMIT 1').get();
      const bytes = fs.statSync(shardPath).size;

      const firstRows = db.prepare('SELECT first_time FROM users WHERE first_time IS NOT NULL').iterate();
      for (const row of firstRows) {
        const m = monthKey(row.first_time);
        if (!m) continue;
        growthCounts.set(m, (growthCounts.get(m) || 0) + 1);
      }

      const activeRows = db.prepare('SELECT month, COUNT(*) as c FROM user_months GROUP BY month').iterate();
      for (const row of activeRows) {
        if (!row.month) continue;
        activeCounts.set(row.month, (activeCounts.get(row.month) || 0) + (row.c || 0));
      }

      db.close();

      const shardUsers = countRow ? countRow.c || 0 : 0;
      totalUsers += shardUsers;

      shardMeta.push({
        sid: entry.sid,
        user_lo: lowerName(loRow ? loRow.username : ''),
        user_hi: lowerName(hiRow ? hiRow.username : ''),
        users: shardUsers,
        file: entry.name,
        bytes
      });
    }

    const out = {
      version: 1,
      created_at: new Date().toISOString(),
      target_mb: Number.isFinite(targetMb) ? targetMb : DEFAULT_TARGET_MB,
      shards: shardMeta,
      totals: {
        users: totalUsers
      },
      collation: 'nocase'
    };

    const growthMonths = Array.from(growthCounts.entries())
      .sort((a, b) => a[0].localeCompare(b[0]));
    let cumulative = 0;
    out.user_growth = growthMonths.map(([month, count]) => {
      cumulative += count;
      return { month, new_users: count, total_users: cumulative };
    });

    const activeMonths = Array.from(activeCounts.entries())
      .sort((a, b) => a[0].localeCompare(b[0]));
    out.user_active = activeMonths.map(([month, active_users]) => ({ month, active_users }));

    ensureWritableOrBackup(outManifest);
    fs.writeFileSync(outManifest, JSON.stringify(out, null, 2));
    console.log(`Wrote ${outManifest}`);
    if (gzipOut) {
      const gzPath = `${outManifest}.gz`;
      gzipFileSync(outManifest, gzPath);
      validateGzipFileSync(gzPath);
      console.log(`Wrote ${gzPath}`);
    }
  } finally {
    for (const p of tempFiles) {
      try { await fsp.unlink(p); } catch {}
    }
    try { await fsp.rmdir(tmpRoot); } catch {}
  }
}

async function main() {
  const args = parseArgs(process.argv);
  const manifestPath = path.resolve(args.manifest);
  const shardsDir = path.resolve(args.shardsDir);
  const outDir = path.resolve(args.outDir);
  const outManifest = path.resolve(args.outManifest);
  const gzipOut = !!args.gzip;
  const keepSqlite = !!args['keep-sqlite'];
  const targetBytes = Math.floor(Number(args.targetMb || DEFAULT_TARGET_MB) * 1024 * 1024);
  const batchSize = Math.max(1000, Number(args.batch || DEFAULT_BATCH));

  if (args.manifestOnly) {
    await fsp.mkdir(outDir, { recursive: true });
    await fsp.mkdir(path.dirname(outManifest), { recursive: true });
    await buildManifestFromUserShards({ outDir, outManifest, gzipOut, targetMb: Number(args.targetMb || DEFAULT_TARGET_MB) });
    return;
  }

  if (!fs.existsSync(manifestPath)) {
    console.error(`Manifest not found: ${manifestPath}`);
    process.exit(1);
  }

  const manifest = readJson(manifestPath);
  const shards = (manifest.shards || []).slice().sort((a, b) => a.sid - b.sid);
  if (!shards.length) {
    console.error('No shards found in manifest.');
    process.exit(1);
  }

  await fsp.mkdir(outDir, { recursive: true });
  await fsp.mkdir(path.dirname(outManifest), { recursive: true });

  const tmpRoot = await fsp.mkdtemp(path.join(os.tmpdir(), 'static-news-user-'));
  const tempFiles = new Set();

  const tempDbPath = path.join(tmpRoot, 'user_stats_all.sqlite');
  const tempDb = initUserDb(tempDbPath);
  tempDb.pragma('cache_size = -200000');

  const upsertUser = tempDb.prepare(`
    INSERT INTO users (username, first_time, last_time, items, comments, stories, ask, show, launch, jobs, polls, avg_score, sum_score, max_score, min_score, max_score_id, max_score_title)
    VALUES (@username, @first_time, @last_time, 1, @comments, @stories, @ask, @show, @launch, @jobs, @polls, @avg_score, @sum_score, @max_score, @min_score, @max_score_id, @max_score_title)
    ON CONFLICT(username) DO UPDATE SET
      first_time = MIN(first_time, excluded.first_time),
      last_time = MAX(last_time, excluded.last_time),
      items = users.items + 1,
      comments = users.comments + excluded.comments,
      stories = users.stories + excluded.stories,
      ask = users.ask + excluded.ask,
      show = users.show + excluded.show,
      launch = users.launch + excluded.launch,
      jobs = users.jobs + excluded.jobs,
      polls = users.polls + excluded.polls,
      sum_score = users.sum_score + excluded.sum_score,
      max_score = MAX(users.max_score, excluded.max_score),
      min_score = MIN(users.min_score, excluded.min_score),
      max_score_id = CASE WHEN excluded.max_score > users.max_score THEN excluded.max_score_id ELSE users.max_score_id END,
      max_score_title = CASE WHEN excluded.max_score > users.max_score THEN excluded.max_score_title ELSE users.max_score_title END
  `);

  const upsertDomain = tempDb.prepare(`
    INSERT INTO user_domains (username, domain, count)
    VALUES (?, ?, 1)
    ON CONFLICT(username, domain) DO UPDATE SET count = count + 1
  `);

  const upsertMonth = tempDb.prepare(`
    INSERT INTO user_months (username, month, count)
    VALUES (?, ?, 1)
    ON CONFLICT(username, month) DO UPDATE SET count = count + 1
  `);

  const txBatch = tempDb.transaction((rows) => {
    for (const r of rows) {
      upsertUser.run(r);
      if (r.domain) upsertDomain.run(r.username, r.domain);
      if (r.month) upsertMonth.run(r.username, r.month);
    }
  });

  let totalItems = 0;
  let shardIndex = 0;
  let batch = [];

  try {
    for (const shard of shards) {
      shardIndex += 1;
      const shardPath = path.join(shardsDir, shard.file);
      if (!fs.existsSync(shardPath)) {
        console.warn(`Missing shard file: ${shardPath}`);
        continue;
      }

      process.stdout.write(`\r[users] shard ${shardIndex}/${shards.length} sid ${shard.sid}... `);
      let dbPath = shardPath;
      if (shardPath.endsWith('.gz')) {
        try {
          dbPath = await gunzipToTemp(shardPath, tmpRoot);
          tempFiles.add(dbPath);
        } catch (err) {
          console.warn(`Failed to gunzip shard ${shard.sid}: ${err.code || err.message}`);
          continue;
        }
      }

      const db = new Database(dbPath, { readonly: true });
      const iter = db.prepare('SELECT id, type, time, by, title, url, score FROM items WHERE by IS NOT NULL').iterate();

      for (const row of iter) {
        const username = String(row.by);
        const isComment = row.type === 'comment' ? 1 : 0;
        const isStory = row.type === 'story' ? 1 : 0;
        const isJob = row.type === 'job' ? 1 : 0;
        const isPoll = row.type === 'poll' ? 1 : 0;
        const title = row.title || '';
        const isAsk = isStory && /^Ask HN:/i.test(title) ? 1 : 0;
        const isShow = isStory && /^Show HN:/i.test(title) ? 1 : 0;
        const isLaunch = isStory && /^Launch HN:/i.test(title) ? 1 : 0;
        const score = Number.isFinite(row.score) ? row.score : 0;

        batch.push({
          username,
          first_time: row.time || null,
          last_time: row.time || null,
          comments: isComment,
          stories: isStory,
          ask: isAsk,
          show: isShow,
          launch: isLaunch,
          jobs: isJob,
          polls: isPoll,
          avg_score: score,
          sum_score: score,
          max_score: score,
          min_score: score,
          max_score_id: row.id || null,
          max_score_title: row.title || null,
          domain: row.url ? domainFromUrl(row.url) : null,
          month: row.time ? monthKey(row.time) : null
        });

        totalItems += 1;
        if (batch.length >= batchSize) {
          txBatch(batch);
          batch = [];
        }

        if (totalItems % 200000 === 0) {
          process.stdout.write(`\r[users] shard ${shardIndex}/${shards.length} sid ${shard.sid} | items ${totalItems.toLocaleString('en-US')}`);
        }
      }
      db.close();

      if (batch.length) {
        txBatch(batch);
        batch = [];
      }

      process.stdout.write(`\r[users] shard ${shardIndex}/${shards.length} sid ${shard.sid} | items ${totalItems.toLocaleString('en-US')} ok`);
    }

    process.stdout.write(`\r[users] items ${totalItems.toLocaleString('en-US')}\n`);

    process.stdout.write('\r[users] finalizing stats (avg_score + indexes)...');
    tempDb.exec('UPDATE users SET avg_score = CAST(sum_score AS REAL) / NULLIF(items, 0)');
    tempDb.exec('CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)');
    tempDb.exec('CREATE INDEX IF NOT EXISTS idx_user_domains_username ON user_domains(username)');
    tempDb.exec('CREATE INDEX IF NOT EXISTS idx_user_months_username ON user_months(username)');
    process.stdout.write('\r[users] finalizing stats (avg_score + indexes) ok\n');

    const growthCounts = new Map();
    const activeCounts = new Map();

    process.stdout.write('\r[users] computing growth + active months...');
    const firstRows = tempDb.prepare('SELECT first_time FROM users WHERE first_time IS NOT NULL').iterate();
    for (const row of firstRows) {
      const m = monthKey(row.first_time);
      if (!m) continue;
      growthCounts.set(m, (growthCounts.get(m) || 0) + 1);
    }

    const activeRows = tempDb.prepare('SELECT month, COUNT(*) as c FROM user_months GROUP BY month').iterate();
    for (const row of activeRows) {
      if (!row.month) continue;
      activeCounts.set(row.month, (activeCounts.get(row.month) || 0) + (row.c || 0));
    }

    const totalUsersRow = tempDb.prepare('SELECT COUNT(*) as c FROM users').get();
    const totalUsers = totalUsersRow ? totalUsersRow.c : 0;
    process.stdout.write('\r[users] computing growth + active months ok\n');

    const userIter = tempDb.prepare('SELECT * FROM users ORDER BY username COLLATE NOCASE').iterate();
    const domainIter = tempDb.prepare('SELECT username, domain, count FROM user_domains ORDER BY username COLLATE NOCASE').iterate();
    const monthIter = tempDb.prepare('SELECT username, month, count FROM user_months ORDER BY username COLLATE NOCASE').iterate();

    const nextDomainRow = () => {
      const r = domainIter.next();
      return r.done ? null : r.value;
    };
    const nextMonthRow = () => {
      const r = monthIter.next();
      return r.done ? null : r.value;
    };

    let domainRow = nextDomainRow();
    let monthRow = nextMonthRow();

    let shardSid = 0;
    let shardDb = null;
    let shardPath = null;
    let shardUsers = 0;
    let shardUserLo = null;
    let shardUserHi = null;
    const shardMeta = [];

    function openShard() {
      shardPath = path.join(outDir, `user_${shardSid}.sqlite`);
      shardDb = initUserDb(shardPath);
      shardDb.pragma('cache_size = -50000');
      shardUsers = 0;
      shardUserLo = null;
      shardUserHi = null;
    }

    function finalizeShard() {
      if (!shardDb) return;
      shardDb.exec('UPDATE users SET avg_score = CAST(sum_score AS REAL) / NULLIF(items, 0)');
      shardDb.exec('CREATE INDEX IF NOT EXISTS idx_users_last_time ON users(last_time)');
      shardDb.exec('CREATE INDEX IF NOT EXISTS idx_users_items ON users(items)');
      shardDb.exec('CREATE INDEX IF NOT EXISTS idx_user_domains ON user_domains(username)');
      shardDb.exec('CREATE INDEX IF NOT EXISTS idx_user_months ON user_months(username)');
      shardDb.close();

      let finalPath = shardPath;
      let bytes = fs.statSync(finalPath).size;
      if (gzipOut) {
        const gzPath = `${finalPath}.gz`;
        const gzBytes = gzipFileSync(finalPath, gzPath);
        try {
          validateGzipFileSync(gzPath);
        } catch (err) {
          console.error(`\n[user] gzip validation failed for shard ${shardSid}: ${err && err.message ? err.message : err}`);
          process.exit(1);
        }
        bytes = gzBytes;
        finalPath = gzPath;
        if (!keepSqlite) fs.unlinkSync(shardPath);
      }

      shardMeta.push({
        sid: shardSid,
        user_lo: shardUserLo,
        user_hi: shardUserHi,
        users: shardUsers,
        file: path.basename(finalPath),
        bytes
      });

      shardSid += 1;
      shardDb = null;
      shardPath = null;
    }

    openShard();

    const insertUser = () => shardDb.prepare(`
      INSERT INTO users (username, first_time, last_time, items, comments, stories, ask, show, launch, jobs, polls, avg_score, sum_score, max_score, min_score, max_score_id, max_score_title)
      VALUES (@username, @first_time, @last_time, @items, @comments, @stories, @ask, @show, @launch, @jobs, @polls, @avg_score, @sum_score, @max_score, @min_score, @max_score_id, @max_score_title)
    `);
    const insertDomain = () => shardDb.prepare('INSERT INTO user_domains (username, domain, count) VALUES (?, ?, ?)');
    const insertMonth = () => shardDb.prepare('INSERT INTO user_months (username, month, count) VALUES (?, ?, ?)');

    let userStmt = insertUser();
    let domainStmt = insertDomain();
    let monthStmt = insertMonth();
    let userCountSinceCheck = 0;

    let userRows = 0;
    for (const user of userIter) {
      const uname = String(user.username || '');
      const unameKey = lowerName(uname);
      if (!shardUserLo) shardUserLo = unameKey;
      shardUserHi = unameKey;

      userStmt.run(user);
      shardUsers += 1;
      userCountSinceCheck += 1;

      // Advance domain/month iterators in case collation differences leave them behind.
      while (domainRow && lowerName(domainRow.username) < unameKey) domainRow = nextDomainRow();
      while (monthRow && lowerName(monthRow.username) < unameKey) monthRow = nextMonthRow();

      while (domainRow && lowerName(domainRow.username) === unameKey) {
        domainStmt.run(domainRow.username, domainRow.domain, domainRow.count);
        domainRow = nextDomainRow();
      }

      while (monthRow && lowerName(monthRow.username) === unameKey) {
        monthStmt.run(monthRow.username, monthRow.month, monthRow.count);
        monthRow = nextMonthRow();
      }

      if (userCountSinceCheck >= SHARD_SIZE_CHECK_EVERY) {
        userCountSinceCheck = 0;
        const size = fs.statSync(shardPath).size;
        if (size >= targetBytes && shardUsers > 0) {
          finalizeShard();
          openShard();
          userStmt = insertUser();
          domainStmt = insertDomain();
          monthStmt = insertMonth();
        }
      }
      userRows += 1;
      if (userRows % 20000 === 0) {
        process.stdout.write(`\r[users] writing shards... ${userRows.toLocaleString('en-US')} users`);
      }
    }

    if (shardUsers > 0) finalizeShard();
    process.stdout.write(`\r[users] writing shards... ${userRows.toLocaleString('en-US')} users ok\n`);

    const out = {
      version: 1,
      created_at: new Date().toISOString(),
      target_mb: Number(args.targetMb || DEFAULT_TARGET_MB),
      shards: shardMeta,
      totals: {
        users: totalUsers
      },
      collation: 'nocase'
    };

    const growthMonths = Array.from(growthCounts.entries())
      .sort((a, b) => a[0].localeCompare(b[0]));
    let cumulative = 0;
    out.user_growth = growthMonths.map(([month, count]) => {
      cumulative += count;
      return { month, new_users: count, total_users: cumulative };
    });

    const activeMonths = Array.from(activeCounts.entries())
      .sort((a, b) => a[0].localeCompare(b[0]));
    out.user_active = activeMonths.map(([month, active_users]) => ({ month, active_users }));

    ensureWritableOrBackup(outManifest);
    fs.writeFileSync(outManifest, JSON.stringify(out, null, 2));
    console.log(`Wrote ${outManifest}`);
    if (gzipOut) {
      const gzPath = `${outManifest}.gz`;
      gzipFileSync(outManifest, gzPath);
      try {
        validateGzipFileSync(gzPath);
      } catch (err) {
        console.error(`\n[user] gzip validation failed for manifest: ${err && err.message ? err.message : err}`);
        process.exit(1);
      }
      console.log(`Wrote ${gzPath}`);
    }
  } finally {
    try { tempDb.close(); } catch {}
    for (const p of tempFiles) {
      try { await fsp.unlink(p); } catch {}
    }
    try { await fsp.unlink(tempDbPath); } catch {}
    try { await fsp.rmdir(tmpRoot); } catch {}
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
