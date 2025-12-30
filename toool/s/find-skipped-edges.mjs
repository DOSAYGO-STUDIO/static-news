#!/usr/bin/env node
/*
 * Find examples where edges live in a different shard than the parent item.
 * Requires: Node.js, better-sqlite3 (already used by this repo).
 */

import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import os from 'os';
import zlib from 'zlib';
import Database from 'better-sqlite3';

const DEFAULT_MANIFEST = 'docs/static-manifest.json';
const DEFAULT_SHARDS_DIR = 'docs/static-shards';
const DEFAULT_LIMIT = 5;
const DEFAULT_MAX_TEXT = 240;

function usage() {
  const msg = `Usage:
  toool/s/find-skipped-edges.mjs [--shard SID|FILENAME] [--limit N] [--max-text N]
                                [--manifest PATH] [--shards-dir PATH]

Examples:
  toool/s/find-skipped-edges.mjs
  toool/s/find-skipped-edges.mjs --shard 42
  toool/s/find-skipped-edges.mjs --shard shard_42.sqlite.gz --limit 8
`;
  process.stdout.write(msg);
}

function parseArgs(argv) {
  const out = {
    shard: null,
    limit: DEFAULT_LIMIT,
    maxText: DEFAULT_MAX_TEXT,
    manifest: DEFAULT_MANIFEST,
    shardsDir: DEFAULT_SHARDS_DIR
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

  if (out.limit !== DEFAULT_LIMIT) out.limit = Number(out.limit);
  if (out['max-text'] != null) out.maxText = Number(out['max-text']);
  if (out['shards-dir']) out.shardsDir = out['shards-dir'];
  return out;
}

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

function isNumeric(val) {
  return /^\d+$/.test(String(val || ''));
}

function pickRandom(arr) {
  if (!arr.length) return null;
  return arr[Math.floor(Math.random() * arr.length)];
}

function normalizeText(text, maxLen) {
  if (!text) return '';
  const flat = String(text).replace(/\s+/g, ' ').trim();
  if (maxLen && flat.length > maxLen) return flat.slice(0, maxLen) + '...';
  return flat;
}

function findShardById(shardsById, itemId) {
  let lo = 0;
  let hi = shardsById.length - 1;
  const x = Number(itemId);
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const s = shardsById[mid];
    if (x < s.id_lo) hi = mid - 1;
    else if (x > s.id_hi) lo = mid + 1;
    else return s;
  }
  return null;
}

async function gunzipToTemp(srcPath) {
  const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'static-news-'));
  const dstPath = path.join(tmpDir, path.basename(srcPath, '.gz'));
  await new Promise((resolve, reject) => {
    const src = fs.createReadStream(srcPath);
    const dst = fs.createWriteStream(dstPath);
    src.pipe(zlib.createGunzip()).pipe(dst);
    dst.on('finish', resolve);
    dst.on('error', reject);
    src.on('error', reject);
  });
  return dstPath;
}

async function main() {
  const args = parseArgs(process.argv);
  const manifestPath = path.resolve(args.manifest);
  const shardsDir = path.resolve(args.shardsDir);

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

  const shardsById = (manifest.shards || []).slice().sort((a, b) => a.id_lo - b.id_lo);

  let shardRec = null;
  if (args.shard) {
    if (isNumeric(args.shard)) {
      shardRec = shards.find(s => s.sid === Number(args.shard));
    } else {
      const base = path.basename(String(args.shard));
      shardRec = shards.find(s => s.file === base || `shard_${s.sid}.sqlite.gz` === base || `shard_${s.sid}.sqlite` === base);
    }
    if (!shardRec) {
      console.error(`Shard not found for: ${args.shard}`);
      process.exit(1);
    }
  } else {
    shardRec = pickRandom(shards);
  }

  const shardPath = path.join(shardsDir, shardRec.file);
  if (!fs.existsSync(shardPath)) {
    console.error(`Shard file missing: ${shardPath}`);
    process.exit(1);
  }

  const tempFiles = new Map(); // sid -> temp path
  const dbCache = new Map(); // sid -> Database

  async function openShardDb(sid) {
    if (dbCache.has(sid)) return dbCache.get(sid);
    const rec = shards.find(s => s.sid === sid);
    if (!rec) return null;
    const filePath = path.join(shardsDir, rec.file);

    let dbPath = filePath;
    if (filePath.endsWith('.gz')) {
      dbPath = await gunzipToTemp(filePath);
      tempFiles.set(sid, dbPath);
    }

    const db = new Database(dbPath, { readonly: true });
    dbCache.set(sid, db);
    return db;
  }

  try {
    const edgeDb = await openShardDb(shardRec.sid);
    const rows = edgeDb.prepare(
      'SELECT parent_id, child_id FROM edges WHERE parent_id < ? OR parent_id > ? ORDER BY RANDOM() LIMIT ?'
    ).all(shardRec.id_lo, shardRec.id_hi, args.limit);

    if (!rows.length) {
      console.log(`No cross-shard parent edges found in shard ${shardRec.sid} (${shardRec.file}).`);
      return;
    }

    console.log(`Shard: ${shardRec.sid} (${shardRec.file})`);
    console.log(`Showing ${rows.length} edge(s) where parent_id is outside this shard range.`);
    console.log(`Text truncated to ${args.maxText} chars. Override with --max-text.`);

    for (const row of rows) {
      const parentShard = findShardById(shardsById, row.parent_id);
      const childShard = findShardById(shardsById, row.child_id);

      const parentDb = parentShard ? await openShardDb(parentShard.sid) : null;
      const childDb = childShard ? await openShardDb(childShard.sid) : null;

      const parent = parentDb
        ? parentDb.prepare('SELECT id, type, time, text, title FROM items WHERE id=? LIMIT 1').get(row.parent_id)
        : null;
      const child = childDb
        ? childDb.prepare('SELECT id, type, time, text, title FROM items WHERE id=? LIMIT 1').get(row.child_id)
        : null;

      const parentText = normalizeText(parent?.text || parent?.title || '', args.maxText);
      const childText = normalizeText(child?.text || child?.title || '', args.maxText);
      const delta = (parent?.time && child?.time) ? (child.time - parent.time) : null;

      console.log('---');
      console.log(`edge_shard:   ${shardRec.sid}`);
      console.log(`parent_id:    ${row.parent_id} (shard ${parentShard ? parentShard.sid : 'n/a'})`);
      console.log(`child_id:     ${row.child_id} (shard ${childShard ? childShard.sid : 'n/a'})`);
      console.log(`parent_time:  ${parent?.time ?? 'n/a'}`);
      console.log(`child_time:   ${child?.time ?? 'n/a'}`);
      console.log(`delta_sec:    ${delta ?? 'n/a'}`);
      console.log(`parent_type:  ${parent?.type ?? 'n/a'}`);
      console.log(`child_type:   ${child?.type ?? 'n/a'}`);
      console.log(`parent_text:  ${parentText || '[empty]'}`);
      console.log(`child_text:   ${childText || '[empty]'}`);
    }
  } finally {
    for (const db of dbCache.values()) {
      try { db.close(); } catch {}
    }
    for (const p of tempFiles.values()) {
      try { await fsp.unlink(p); } catch {}
      try { await fsp.rmdir(path.dirname(p)); } catch {}
    }
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
