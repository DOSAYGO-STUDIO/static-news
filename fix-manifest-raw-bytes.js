#!/usr/bin/env node
/**
 * fix-manifest-raw-bytes.js
 * 
 * Updates raw_bytes_est in static-manifest.json by decompressing gzipped shards
 * to determine their uncompressed size.
 * 
 * Usage:
 *   node fix-manifest-raw-bytes.js [manifest-path]
 * 
 * Default manifest path: docs/static-manifest.json
 */

import fs from 'fs';
import path from 'path';
import zlib from 'zlib';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DEFAULT_MANIFEST = path.join(__dirname, 'docs', 'static-manifest.json');

async function gunzipSize(gzPath) {
  const gz = fs.readFileSync(gzPath);
  const raw = zlib.gunzipSync(gz);
  return raw.length;
}

async function main() {
  const manifestPath = process.argv[2] || DEFAULT_MANIFEST;
  
  if (!fs.existsSync(manifestPath)) {
    console.error(`Manifest not found: ${manifestPath}`);
    process.exit(1);
  }

  const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
  const shardsDir = path.join(path.dirname(manifestPath), 'static-shards');

  if (!manifest.shards || !Array.isArray(manifest.shards)) {
    console.error('No shards array in manifest');
    process.exit(1);
  }

  console.log(`Processing ${manifest.shards.length} shards...`);

  let updated = 0;
  let totalRaw = 0;
  let totalGz = 0;

  for (let i = 0; i < manifest.shards.length; i++) {
    const shard = manifest.shards[i];
    const file = shard.file;
    const isGz = file.endsWith('.gz');
    const fullPath = path.join(shardsDir, file);

    if (!fs.existsSync(fullPath)) {
      console.warn(`  [${i + 1}/${manifest.shards.length}] Missing: ${file}`);
      continue;
    }

    const gzBytes = fs.statSync(fullPath).size;

    if (isGz) {
      try {
        const rawBytes = await gunzipSize(fullPath);
        if (shard.raw_bytes_est !== rawBytes) {
          shard.raw_bytes_est = rawBytes;
          updated++;
        }
        totalRaw += rawBytes;
        totalGz += gzBytes;
        process.stdout.write(`\r  [${i + 1}/${manifest.shards.length}] ${file} → ${(rawBytes / 1024 / 1024).toFixed(1)}MB raw`);
      } catch (err) {
        console.warn(`\n  [${i + 1}/${manifest.shards.length}] Error decompressing ${file}: ${err.message}`);
      }
    } else {
      // Not gzipped - raw size equals file size
      if (shard.raw_bytes_est !== gzBytes) {
        shard.raw_bytes_est = gzBytes;
        updated++;
      }
      totalRaw += gzBytes;
      process.stdout.write(`\r  [${i + 1}/${manifest.shards.length}] ${file} → ${(gzBytes / 1024 / 1024).toFixed(1)}MB`);
    }
  }

  console.log('\n');
  console.log(`Total raw size: ${(totalRaw / 1024 / 1024 / 1024).toFixed(2)} GB`);
  if (totalGz > 0) {
    console.log(`Total gz size:  ${(totalGz / 1024 / 1024 / 1024).toFixed(2)} GB`);
    console.log(`Compression:    ${((1 - totalGz / totalRaw) * 100).toFixed(1)}%`);
  }

  if (updated > 0) {
    fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));
    console.log(`\nUpdated ${updated} shard(s) in ${manifestPath}`);
  } else {
    console.log('\nNo updates needed.');
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
