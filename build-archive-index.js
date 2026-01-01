#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const os = require('os');
const zlib = require('zlib');
const Database = require('better-sqlite3');

const BACKUP_STAMP = new Date().toISOString().replace(/[:.]/g, '-');

const manifestPath = path.join('docs', 'static-manifest.json');
const shardsDir = path.join('docs', 'static-shards');
const outPath = path.join('docs', 'archive-index.json');

const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
const shards = manifest.shards || [];
const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'static-news-archive-'));
const tempFiles = new Set();

const out = {
  generated_at: new Date().toISOString(),
  snapshot_time: manifest.snapshot_time || null,
  totals: {
    items: 0,
    posts: 0,
    comments: 0,
    bytes: 0,
    shards: shards.length
  },
  manifests: [],
  shards: []
};

const manifestFiles = [
  { file: 'static-manifest.json', note: 'Shard metadata, ranges, and snapshot time.' },
  { file: 'filter-manifest.json', note: 'Prime filter data for the main view.' }
];

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

for (const entry of manifestFiles) {
  const full = path.join('docs', entry.file);
  if (!fs.existsSync(full)) continue;
  const stat = fs.statSync(full);
  out.manifests.push({
    file: entry.file,
    bytes: stat.size,
    note: entry.note
  });
}

function openShardDb(fullPath, localTempRoot) {
  if (!fullPath.endsWith('.gz')) {
    return { dbPath: fullPath, cleanup: false };
  }
  const gz = fs.readFileSync(fullPath);
  const raw = zlib.gunzipSync(gz);
  const tmpPath = path.join(localTempRoot, path.basename(fullPath, '.gz') + '_' + Math.random().toString(36).slice(2));
  fs.writeFileSync(tmpPath, raw);
  return { dbPath: tmpPath, cleanup: true };
}

const SCAN_CONCURRENCY = Math.max(1, Math.min(8, require('os').cpus().length));

async function runPool(items, limit, worker) {
  const queue = items.slice();
  const workers = Array.from({ length: Math.max(1, limit) }, async () => {
    while (queue.length) {
      const item = queue.shift();
      if (!item) break;
      await worker(item);
    }
  });
  await Promise.all(workers);
}

async function scanShards() {
  const results = [];
  let done = 0;
  const total = shards.length;

  await runPool(shards, SCAN_CONCURRENCY, async (shard) => {
    const file = shard.file;
    const fullPath = path.join(shardsDir, file);
    if (!fs.existsSync(fullPath)) {
      throw new Error(`Shard missing: ${fullPath}`);
    }
    const stat = fs.statSync(fullPath);
    const { dbPath, cleanup } = openShardDb(fullPath, tempRoot);
    if (cleanup) tempFiles.add(dbPath);

    const db = new Database(dbPath, { readonly: true });
    const timeCountRow = db.prepare(`SELECT COUNT(*) as c FROM items WHERE time IS NOT NULL`).get();
    const timeCount = timeCountRow.c || 0;
    let tminEff = shard.tmin || null;
    let tmaxEff = shard.tmax || null;
    let tnull = 0;
    if (timeCount > 0) {
      const p1 = Math.floor((timeCount - 1) * 0.01);
      const p99 = Math.floor((timeCount - 1) * 0.99);
      const rowMin = db.prepare(`SELECT time as t FROM items WHERE time IS NOT NULL ORDER BY time LIMIT 1 OFFSET ?`).get(p1);
      const rowMax = db.prepare(`SELECT time as t FROM items WHERE time IS NOT NULL ORDER BY time LIMIT 1 OFFSET ?`).get(p99);
      tminEff = rowMin ? rowMin.t : tminEff;
      tmaxEff = rowMax ? rowMax.t : tmaxEff;
    }
    const nullRow = db.prepare(`SELECT COUNT(*) as c FROM items WHERE time IS NULL`).get();
    tnull = nullRow.c || 0;
    const row = db.prepare(`
      SELECT
        COUNT(*) as items,
        SUM(CASE WHEN type='comment' THEN 1 ELSE 0 END) as comments,
        SUM(CASE WHEN type!='comment' THEN 1 ELSE 0 END) as posts
      FROM items
    `).get();
    db.close();

    const items = row.items || 0;
    const comments = row.comments || 0;
    const posts = row.posts || 0;

    results.push({
      sid: shard.sid,
      file,
      tmin: shard.tmin || null,
      tmax: shard.tmax || null,
      tmin_eff: tminEff,
      tmax_eff: tmaxEff,
      time_null: tnull,
      id_lo: shard.id_lo || null,
      id_hi: shard.id_hi || null,
      count: items,
      posts,
      comments,
      bytes: stat.size
    });

    done += 1;
    process.stdout.write(`\r[scan] ${done}/${total} shards (concurrency: ${SCAN_CONCURRENCY})`);
  });

  // Sort by sid and compute totals
  results.sort((a, b) => a.sid - b.sid);
  for (const r of results) {
    out.totals.items += r.count;
    out.totals.comments += r.comments;
    out.totals.posts += r.posts;
    out.totals.bytes += r.bytes;
    out.shards.push(r);
  }
}

try {
  scanShards().then(() => {
    process.stdout.write('\n');
    ensureWritableOrBackup(outPath);
    fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
    console.log(`Wrote ${outPath}`);
  }).catch(err => {
    console.error(err);
    process.exit(1);
  }).finally(() => {
    for (const p of tempFiles) {
      try { fs.unlinkSync(p); } catch {}
    }
    try { fs.rmdirSync(tempRoot); } catch {}
  });
} catch (err) {
  console.error(err);
  for (const p of tempFiles) {
    try { fs.unlinkSync(p); } catch {}
  }
  try { fs.rmdirSync(tempRoot); } catch {}
  process.exit(1);
}
