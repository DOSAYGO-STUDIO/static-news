#!/usr/bin/env node
/*
 * Build a cross-shard edge index:
 *   parent_id -> [child_shard_sid...]
 * Only includes edges where parent_id is outside the shard's id_lo..id_hi range.
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
const DEFAULT_OUT = 'docs/cross-shard-index.sqlite';
const DEFAULT_LOG_EVERY = 500000;

function usage() {
  const msg = `Usage:
  toool/s/build-cross-shard-index.mjs [--manifest PATH] [--shards-dir PATH]
                                     [--out PATH] [--log-every N]
                                     [--json | --binary]

Examples:
  toool/s/build-cross-shard-index.mjs
  toool/s/build-cross-shard-index.mjs --out docs/cross-shard-index.sqlite
  toool/s/build-cross-shard-index.mjs --json --out docs/cross-shard-index.json
  toool/s/build-cross-shard-index.mjs --binary --out docs/cross-shard-index.bin
`;
  process.stdout.write(msg);
}

function parseArgs(argv) {
  const out = {
    manifest: DEFAULT_MANIFEST,
    shardsDir: DEFAULT_SHARDS_DIR,
    out: DEFAULT_OUT,
    logEvery: DEFAULT_LOG_EVERY,
    json: false,
    binary: false
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

  if (out['log-every'] != null) out.logEvery = Number(out['log-every']);
  if (out['shards-dir']) out.shardsDir = out['shards-dir'];
  return out;
}

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, 'utf8'));
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

async function main() {
  const args = parseArgs(process.argv);
  if (args.json && args.binary) {
    console.error('Choose only one output format: --json or --binary (default is sqlite).');
    process.exit(1);
  }
  const manifestPath = path.resolve(args.manifest);
  const shardsDir = path.resolve(args.shardsDir);
  let outPath = path.resolve(args.out);
  const hasOutFlag = process.argv.includes('--out');
  if (!hasOutFlag) {
    if (args.json) outPath = path.resolve('docs/cross-shard-index.json');
    else if (args.binary) outPath = path.resolve('docs/cross-shard-index.bin');
    else outPath = path.resolve('docs/cross-shard-index.sqlite');
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

  const tmpRoot = await fsp.mkdtemp(path.join(os.tmpdir(), 'static-news-cross-'));
  const tempDbPath = path.join(tmpRoot, 'cross-shard-index.sqlite');
  const tempDb = new Database(tempDbPath);
  tempDb.exec(`
    PRAGMA journal_mode = OFF;
    PRAGMA synchronous = OFF;
    CREATE TABLE cross (
      parent_id INTEGER NOT NULL,
      shard_sid INTEGER NOT NULL,
      PRIMARY KEY(parent_id, shard_sid)
    );
    CREATE TABLE meta (
      key TEXT PRIMARY KEY,
      value TEXT
    );
  `);

  const insert = tempDb.prepare('INSERT OR IGNORE INTO cross (parent_id, shard_sid) VALUES (?, ?)');
  const insertMany = tempDb.transaction((rows) => {
    for (const r of rows) insert.run(r.parent_id, r.shard_sid);
  });

  const shardTempFiles = new Map();
  let totalEdges = 0;

  try {
    let shardIndex = 0;
    for (const shard of shards) {
      shardIndex += 1;
      const shardPath = path.join(shardsDir, shard.file);
      if (!fs.existsSync(shardPath)) {
        console.warn(`Missing shard file: ${shardPath}`);
        continue;
      }

      let dbPath = shardPath;
      if (shardPath.endsWith('.gz')) {
        try {
          dbPath = await gunzipToTemp(shardPath, tmpRoot);
          shardTempFiles.set(shard.sid, dbPath);
        } catch (err) {
          console.warn(`Failed to gunzip shard ${shard.sid} (${shard.file}): ${err.code || err.message}`);
          continue;
        }
      }

      const db = new Database(dbPath, { readonly: true });
      const iter = db.prepare(
        'SELECT parent_id FROM edges WHERE parent_id < ? OR parent_id > ?'
      ).iterate(shard.id_lo, shard.id_hi);

      let batch = [];
      for (const row of iter) {
        batch.push({ parent_id: row.parent_id, shard_sid: shard.sid });
        totalEdges += 1;
        if (batch.length >= 10000) {
          insertMany(batch);
          batch = [];
          if (args.logEvery && totalEdges % args.logEvery === 0) {
            process.stdout.write(`\r[scan] shard ${shardIndex}/${shards.length} sid ${shard.sid} | edges ${totalEdges.toLocaleString('en-US')}`);
          }
        }
      }
      if (batch.length) insertMany(batch);
      db.close();
      process.stdout.write(`\r[scan] shard ${shardIndex}/${shards.length} sid ${shard.sid} | edges ${totalEdges.toLocaleString('en-US')} ok`);
    }

    if (args.logEvery) process.stdout.write(`\n[scan] edges ${totalEdges.toLocaleString('en-US')}\n`);

    const totals = tempDb.prepare('SELECT COUNT(*) as links, COUNT(DISTINCT parent_id) as parents FROM cross').get();

    if (args.json) {
      await fsp.mkdir(path.dirname(outPath), { recursive: true });
      ensureWritableOrBackup(outPath);
      const out = fs.createWriteStream(outPath, { encoding: 'utf8' });

      out.write('{\n');
      out.write(`  "generated_at": "${new Date().toISOString()}",\n`);
      out.write(`  "source_manifest": "${path.relative(process.cwd(), manifestPath)}",\n`);
      out.write(`  "totals": {"parents": ${totals.parents || 0}, "links": ${totals.links || 0}, "shards": ${shards.length}},\n`);
      out.write('  "parents": [\n');

      const rows = tempDb.prepare('SELECT parent_id, shard_sid FROM cross ORDER BY parent_id, shard_sid').iterate();
      let currentId = null;
      let currentSids = [];
      let firstEntry = true;

      function flushEntry() {
        if (currentId == null) return;
        const payload = `    [${currentId}, [${currentSids.join(',')}]]`;
        out.write((firstEntry ? '' : ',\n') + payload);
        firstEntry = false;
      }

      for (const row of rows) {
        if (currentId == null) {
          currentId = row.parent_id;
          currentSids = [row.shard_sid];
          continue;
        }
        if (row.parent_id !== currentId) {
          flushEntry();
          currentId = row.parent_id;
          currentSids = [row.shard_sid];
        } else {
          currentSids.push(row.shard_sid);
        }
      }
      flushEntry();

      out.write('\n  ]\n');
      out.write('}\n');
      await new Promise((resolve, reject) => {
        out.on('finish', resolve);
        out.on('error', reject);
        out.end();
      });
      console.log(`Wrote ${outPath}`);
      return;
    }

    if (args.binary) {
      const parentsCount = totals.parents || 0;
      const linksCount = totals.links || 0;

      const parentIds = new Uint32Array(parentsCount);
      const offsets = new Uint32Array(parentsCount + 1);
      const shardSids = new Uint16Array(linksCount);

      const rows = tempDb.prepare('SELECT parent_id, shard_sid FROM cross ORDER BY parent_id, shard_sid').iterate();
      let parentIdx = 0;
      let linkIdx = 0;
      let currentId = null;

      for (const row of rows) {
        if (currentId == null) {
          currentId = row.parent_id;
          parentIds[parentIdx] = row.parent_id;
          offsets[parentIdx] = linkIdx;
        }
        if (row.parent_id !== currentId) {
          offsets[parentIdx + 1] = linkIdx;
          parentIdx += 1;
          currentId = row.parent_id;
          parentIds[parentIdx] = row.parent_id;
          offsets[parentIdx] = linkIdx;
        }
        shardSids[linkIdx] = row.shard_sid;
        linkIdx += 1;
      }
      if (parentsCount > 0) offsets[parentIdx + 1] = linkIdx;

      const header = Buffer.alloc(24);
      header.write('CSHX', 0, 4, 'ascii');
      header.writeUInt32LE(1, 4); // version
      header.writeUInt32LE(parentsCount, 8);
      header.writeUInt32LE(linksCount, 12);
      header.writeUInt32LE(shards.length, 16);
      header.writeUInt32LE(0, 20);

      await fsp.mkdir(path.dirname(outPath), { recursive: true });
      ensureWritableOrBackup(outPath);
      const out = fs.createWriteStream(outPath);
      out.write(header);
      out.write(Buffer.from(parentIds.buffer));
      out.write(Buffer.from(offsets.buffer));
      out.write(Buffer.from(shardSids.buffer));
      await new Promise((resolve, reject) => {
        out.on('finish', resolve);
        out.on('error', reject);
        out.end();
      });
      console.log(`Wrote ${outPath}`);
      return;
    }

    const upsertMeta = tempDb.prepare('INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)');
    upsertMeta.run('generated_at', new Date().toISOString());
    upsertMeta.run('source_manifest', path.relative(process.cwd(), manifestPath));
    upsertMeta.run('parents', String(totals.parents || 0));
    upsertMeta.run('links', String(totals.links || 0));
    upsertMeta.run('shards', String(shards.length));
    tempDb.exec('CREATE INDEX IF NOT EXISTS idx_cross_parent ON cross(parent_id);');

    await fsp.mkdir(path.dirname(outPath), { recursive: true });
    ensureWritableOrBackup(outPath);
    tempDb.close();
    await fsp.rename(tempDbPath, outPath);
    console.log(`Wrote ${outPath}`);
  } finally {
    try { tempDb.close(); } catch {}
    for (const p of shardTempFiles.values()) {
      try { await fsp.unlink(p); } catch {}
    }
    try { await fsp.rmdir(tmpRoot); } catch {}
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
