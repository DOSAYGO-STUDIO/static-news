#!/usr/bin/env node
/**
 * Download the deployed Hacker Book site (all manifests + shards) over HTTPS.
 * Defaults to https://hackerbook.dosaygo.com and writes to ./downloaded-site.
 *
 * Usage:
 *   node toool/download-site.mjs [--base https://example.com] [--out ./path] [--no-shards]
 *
 * Environment overrides:
 *   BASE_URL, OUT_DIR, SKIP_SHARDS
 */

import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import { pipeline } from 'stream/promises';
import zlib from 'zlib';

const args = process.argv.slice(2);
const argMap = new Map();
for (let i = 0; i < args.length; i++) {
  const a = args[i];
  if (a.startsWith('--')) {
    const key = a.replace(/^--/, '');
    const val = args[i + 1] && !args[i + 1].startsWith('--') ? args[++i] : true;
    argMap.set(key, val);
  }
}

const BASE = String(process.env.BASE_URL || argMap.get('base') || 'https://hackerbook.dosaygo.com').replace(/\/+$/, '');
const OUT_DIR = path.resolve(String(process.env.OUT_DIR || argMap.get('out') || 'downloaded-site'));
const SKIP_SHARDS = process.env.SKIP_SHARDS === '1' || argMap.get('no-shards') === true;
const CONCURRENCY = 4;

if (argMap.has('help') || argMap.has('h')) {
  const help = `Download the hosted Hacker Book site (core assets + shards) over HTTPS.\n\nUsage:\n  node toool/download-site.mjs [--base URL] [--out DIR] [--no-shards]\n\nOptions:\n  --base URL     Base URL to fetch from (default: https://hackerbook.dosaygo.com)\n  --out DIR      Output directory (default: ./downloaded-site)\n  --no-shards    Skip downloading item/user shards (just core assets/manifests)\n\nEnv vars:\n  BASE_URL, OUT_DIR, SKIP_SHARDS=1 mirror the above.\n\nExamples:\n  node toool/download-site.mjs\n  node toool/download-site.mjs --base https://example.com --out ./site\n  SKIP_SHARDS=1 node toool/download-site.mjs\n`;
  process.stdout.write(help);
  process.exit(0);
}

const corePaths = [
  'index.html',
  'static-manifest.json.gz',
  'archive-index.json.gz',
  'cross-shard-index.bin.gz',
  'static-user-stats-manifest.json.gz',
  'cd.png',
  'jswasm/sqlite3.js',
  'jswasm/sqlite3.wasm',
  'vendor/uplot/uPlot.iife.min.js',
  'vendor/uplot/uPlot.min.css'
];

async function ensureDir(p) {
  await fsp.mkdir(path.dirname(p), { recursive: true });
}

async function fetchBytes(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  const buf = await res.arrayBuffer();
  return new Uint8Array(buf);
}

function maybeGunzip(u8) {
  if (u8.length >= 2 && u8[0] === 0x1f && u8[1] === 0x8b) {
    return zlib.gunzipSync(u8);
  }
  return u8;
}

async function fetchJsonMaybeGzip(url) {
  const u8 = await fetchBytes(url);
  const raw = maybeGunzip(u8);
  return JSON.parse(Buffer.from(raw).toString('utf8'));
}

async function downloadFile(relPath) {
  const dest = path.join(OUT_DIR, relPath);
  const url = `${BASE}/${relPath}`;
  await ensureDir(dest);
  if (fs.existsSync(dest)) return;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  const fileStream = fs.createWriteStream(dest);
  await pipeline(res.body, fileStream);
}

async function downloadMany(list, label) {
  const queue = list.slice();
  let done = 0;
  const worker = async () => {
    while (queue.length) {
      const rel = queue.shift();
      try {
        await downloadFile(rel);
      } catch (err) {
        console.error(`Failed ${rel}: ${err.message || err}`);
      }
      done += 1;
      if (done % 10 === 0) {
        process.stdout.write(`\r${label}: ${done}/${list.length}`);
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, list.length || 1) }, worker));
  if (list.length) process.stdout.write(`\r${label}: ${done}/${list.length}\n`);
}

async function main() {
  console.log(`Base: ${BASE}`);
  console.log(`Out:  ${OUT_DIR}`);
  await fsp.mkdir(OUT_DIR, { recursive: true });

  // Core files
  console.log('Downloading core assets...');
  await downloadMany(corePaths, 'core');

  if (SKIP_SHARDS) {
    console.log('Skipping shards (SKIP_SHARDS set).');
    return;
  }

  // Manifests to discover shards
  console.log('Fetching manifests for shard lists...');
  const manifest = await fetchJsonMaybeGzip(`${BASE}/static-manifest.json.gz`);
  const userManifest = await fetchJsonMaybeGzip(`${BASE}/static-user-stats-manifest.json.gz`);

  const shardPaths = (manifest?.shards || []).map(s => `static-shards/${s.file}`);
  const userShardPaths = (userManifest?.shards || []).map(s => `static-user-stats-shards/${s.file}`);

  console.log(`Downloading item shards (${shardPaths.length})...`);
  await downloadMany(shardPaths, 'item shards');

  console.log(`Downloading user shards (${userShardPaths.length})...`);
  await downloadMany(userShardPaths, 'user shards');

  console.log('Done.');
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
