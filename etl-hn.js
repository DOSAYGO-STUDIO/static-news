#!/usr/bin/env node
/**
 * ETL: BigQuery HN export (NDJSON .json.gz) -> SQLite shards (ID-range) + interval manifest.
 *
 * Goals:
 *  - Perfect routing by ID interval: id ∈ [id_lo, id_hi] => shard
 *  - Fast thread traversal: edges(parent_id, ord, child_id)
 *  - Iteration knobs: target shard size, max days, max ids, presorted vs stage/sort
 *
 * Output:
 *  - ./docs/shards/shard_<sid>.sqlite        (always created during build)
 *  - (post-pass) ./docs/shards/shard_<sid>_<hash>.sqlite.gz
 *  - ./docs/manifest.json                   (updated to reference gz files if --gzip)
 *
 * NOTE:
 *  - No VACUUM in build loop.
 *  - Optional final VACUUM + gzip pass.
 */

const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const os = require("os");
const crypto = require("crypto");
const { pipeline } = require("stream/promises");
const { execSync } = require("child_process");
const readline = require("readline");
const Database = require("better-sqlite3");

const BACKUP_STAMP = new Date().toISOString().replace(/[:.]/g, "-");

// -------------------- CLI --------------------
function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const key = a.slice(2);
    const next = argv[i + 1];
    if (next && !next.startsWith("--")) {
      out[key] = next;
      i++;
    } else {
      out[key] = true;
    }
  }
  return out;
}

const args = parseArgs(process.argv);

// Paths
const DATA_DIR = args.data || "./data/raw";
const OUT_DIR = args.out || "./docs/static-shards";
const MANIFEST_PATH = args.manifest || "./docs/static-manifest.json";
const STAGING_PATH = args.staging || "./data/static-staging-hn.sqlite";

// Sharding knobs
// CUT TARGET IN HALF (default 6 -> 3)
const TARGET_MB = Number(args["target-mb"] ?? 3);        // desired shard size feel (compressed-ish)
const MAX_MB = Number(args["max-mb"] ?? 6);              // hard cap heuristic (keep proportional)
const MAX_DAYS = Number(args["max-days"] ?? 14);         // guardrail on time span inside one shard
const MAX_IDS = Number(args["max-ids"] ?? 600_000);      // guardrail on count of items in shard
const TIME_SAMPLE_SIZE = Number(args["time-sample"] ?? 2048);
const RUN_ARCHIVE_INDEX = args["archive-index"] !== "0";

// Compression heuristic: estimate gzip ratio from uncompressed bytes.
const GZIP_RATIO = Number(args["gzip-ratio"] ?? 0.25);   // gz ~ 25% of raw as a rule of thumb
const TARGET_RAW_BYTES = Math.floor((TARGET_MB * 1024 * 1024) / Math.max(GZIP_RATIO, 0.05));
const MAX_RAW_BYTES = Math.floor((MAX_MB * 1024 * 1024) / Math.max(GZIP_RATIO, 0.05));

// Modes
const PRESORTED = !!args.presorted;
const FROM_STAGING = !!args["from-staging"];
const DELETE_STAGING = !!args["delete-staging"];
const RESTART_ETL = !!args["restart"];
const REBUILD_MANIFEST = !!args["rebuild-manifest"];
const POST_CONCURRENCY = Number(args["post-concurrency"] ?? Math.max(1, Math.floor(os.cpus().length / 2)));

// Post-pass behaviors
const GZIP_SHARDS = !!args.gzip;                 // do final gz + manifest rewrite
const KEEP_SQLITE = !!args["keep-sqlite"];       // keep .sqlite after gz
const VACUUM_AT_END = args["vacuum"] === false ? false : true; // default true
const CI_MODE = !!process.env.CI;
const CI_EAGER_FINALIZE = CI_MODE && GZIP_SHARDS;
const CI_LOG_INTERVAL_MS = 5000;

// Performance knobs
const WRITE_BATCH = Number(args["write-batch"] ?? 5000); // batch rows per transaction

// Safety
fs.mkdirSync(OUT_DIR, { recursive: true });
fs.mkdirSync(path.dirname(MANIFEST_PATH), { recursive: true });
if (!FROM_STAGING && !fs.existsSync(DATA_DIR)) {
  console.error(`DATA_DIR not found: ${DATA_DIR}`);
  process.exit(1);
}

// -------------------- Helpers --------------------
function listGzFiles(dir) {
  return fs.readdirSync(dir).filter(f => f.endsWith(".json.gz")).sort();
}

const ciLogState = new Map();
function writeProgress(key, line) {
  if (!CI_MODE) {
    process.stdout.write(line);
    return;
  }
  const now = Date.now();
  const last = ciLogState.get(key) || 0;
  if (now - last >= CI_LOG_INTERVAL_MS) {
    ciLogState.set(key, now);
    process.stdout.write(line);
  }
}

function safeInt(x) {
  const n = Number(x);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

// conservative raw byte estimate per item for shard sizing
function estimateRawBytes(item) {
  const by = item.by ? String(item.by).length : 0;
  const title = item.title ? String(item.title).length : 0;
  const text = item.text ? String(item.text).length : 0;
  const url = item.url ? String(item.url).length : 0;
  // note: in our row, kids are not present, but callers add kids*6 separately.
  return 80 + by + title + text + url;
}

function mb(n) { return (n / 1024 / 1024).toFixed(2); }

function isoUTC(sec) {
  if (sec == null) return "n/a";
  return new Date(sec * 1000).toISOString().replace(".000Z", "Z");
}

function spanDaysFloat(tmin, tmax) {
  if (tmin == null || tmax == null) return 0;
  return (tmax - tmin) / 86400;
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

function gzipFileSync(srcPath, dstPath) {
  const data = fs.readFileSync(srcPath);
  const gz = zlib.gzipSync(data, { level: 9 });
  const tmpPath = `${dstPath}.tmp`;
  fs.writeFileSync(tmpPath, gz);
  fs.renameSync(tmpPath, dstPath);
  return gz.length;
}

function validateGzipFileSync(gzPath) {
  execSync(`gzip -t ${JSON.stringify(gzPath)}`, { stdio: "ignore" });
}

async function gzipFile(srcPath, dstPath) {
  const tmpPath = `${dstPath}.tmp`;
  await pipeline(
    fs.createReadStream(srcPath),
    zlib.createGzip({ level: 9 }),
    fs.createWriteStream(tmpPath)
  );
  const stat = fs.statSync(tmpPath);
  fs.renameSync(tmpPath, dstPath);
  return stat.size;
}

async function gzipFileToTemp(srcPath, tmpPath) {
  await pipeline(
    fs.createReadStream(srcPath),
    zlib.createGzip({ level: 9 }),
    fs.createWriteStream(tmpPath)
  );
  return fs.statSync(tmpPath).size;
}

async function hashFileHex(filePath, hexLen = 12) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash("sha256");
    const stream = fs.createReadStream(filePath);
    stream.on("error", reject);
    stream.on("data", chunk => hash.update(chunk));
    stream.on("end", () => {
      resolve(hash.digest("hex").slice(0, hexLen));
    });
  });
}

function isHashedShardName(name) {
  return /^shard_\d+_[0-9a-f]{12}\.sqlite\.gz$/.test(name);
}

function buildShardGzMap() {
  const map = new Map();
  const files = fs.readdirSync(OUT_DIR);
  for (const file of files) {
    const match = /^shard_(\d+)(?:_([0-9a-f]{12}))?\.sqlite\.gz$/.exec(file);
    if (!match) continue;
    const sid = Number(match[1]);
    const hasHash = !!match[2];
    const existing = map.get(sid);
    if (!existing || hasHash) {
      map.set(sid, file);
    }
  }
  return map;
}

function getShardGzInfo(sid, gzMap) {
  const file = gzMap.get(sid);
  if (!file) return null;
  return { file, path: path.join(OUT_DIR, file) };
}

async function normalizeShardGzName(sid, gzInfo, gzMap) {
  if (!gzInfo || isHashedShardName(gzInfo.file)) return gzInfo;
  const hash = await hashFileHex(gzInfo.path, 12);
  const nextFile = `shard_${sid}_${hash}.sqlite.gz`;
  const nextPath = path.join(OUT_DIR, nextFile);
  if (gzInfo.path !== nextPath) {
    if (fs.existsSync(nextPath)) {
      fs.unlinkSync(gzInfo.path);
    } else {
      fs.renameSync(gzInfo.path, nextPath);
    }
  }
  gzMap.set(sid, nextFile);
  return { file: nextFile, path: nextPath };
}

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

function checkSqliteIntegrity(sqlitePath) {
  const db = new Database(sqlitePath, { readonly: true });
  const row = db.prepare("PRAGMA quick_check").get();
  db.close();
  return row && (row.quick_check === "ok" || row["quick_check"] === "ok");
}

async function gunzipToTemp(srcPath, tmpRoot) {
  const dstPath = path.join(tmpRoot, path.basename(srcPath, ".gz"));
  await pipeline(
    fs.createReadStream(srcPath),
    zlib.createGunzip(),
    fs.createWriteStream(dstPath)
  );
  return dstPath;
}

async function rebuildManifestFromShards() {
  const files = fs.readdirSync(OUT_DIR);
  const shardRe = /^shard_(\d+)(?:_[0-9a-f]{12})?\.sqlite(\.gz)?$/;
  const shardMap = new Map();
  for (const file of files) {
    const match = shardRe.exec(file);
    if (!match) continue;
    const sid = Number(match[1]);
    const isGz = !!match[2];
    const isHashed = isHashedShardName(file);
    const existing = shardMap.get(sid);
    if (!existing || (isGz && !existing.isGz) || (isHashed && existing.isGz && !isHashedShardName(existing.file))) {
      shardMap.set(sid, { sid, file, isGz });
    }
  }
  const shards = Array.from(shardMap.values()).sort((a, b) => a.sid - b.sid);
  if (!shards.length) {
    throw new Error(`No shard files found in ${OUT_DIR}`);
  }

  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "static-news-rebuild-"));
  const tempFiles = new Set();
  const out = {
    version: 1,
    created_at: new Date().toISOString(),
    sharding: {
      axis: "id",
      rebuild_from_shards: true
    },
    shards: []
  };

  let globalTmin = null;
  let globalTmax = null;
  let index = 0;

  try {
    for (const shard of shards) {
      index += 1;
      const fullPath = path.join(OUT_DIR, shard.file);
      let dbPath = fullPath;
      if (shard.isGz) {
        dbPath = await gunzipToTemp(fullPath, tmpRoot);
        tempFiles.add(dbPath);
      }

      const db = new Database(dbPath, { readonly: true });
      const row = db.prepare(`
        SELECT
          MIN(id) as id_lo,
          MAX(id) as id_hi,
          MIN(time) as tmin,
          MAX(time) as tmax,
          COUNT(*) as count
        FROM items
      `).get();
      db.close();

      const bytes = fs.statSync(fullPath).size;
      // For gzipped shards, get the uncompressed size from the temp file
      const rawBytesEst = shard.isGz ? fs.statSync(dbPath).size : bytes;
      const record = {
        sid: shard.sid,
        id_lo: row?.id_lo ?? null,
        id_hi: row?.id_hi ?? null,
        tmin: row?.tmin ?? null,
        tmax: row?.tmax ?? null,
        count: row?.count ?? 0,
        raw_bytes_est: rawBytesEst,
        file: shard.file,
        bytes
      };
      out.shards.push(record);

      if (record.tmin != null) {
        globalTmin = globalTmin == null ? record.tmin : Math.min(globalTmin, record.tmin);
      }
      if (record.tmax != null) {
        globalTmax = globalTmax == null ? record.tmax : Math.max(globalTmax, record.tmax);
      }

      writeProgress("rebuild", `\r[rebuild] shard ${index}/${shards.length} sid ${shard.sid}`);
    }
    process.stdout.write("\n");
  } finally {
    for (const p of tempFiles) {
      try { fs.unlinkSync(p); } catch {}
    }
    try { fs.rmdirSync(tmpRoot); } catch {}
  }

  out.snapshot_time = globalTmax;
  ensureWritableOrBackup(MANIFEST_PATH);
  fs.writeFileSync(MANIFEST_PATH, JSON.stringify(out, null, 2));
  console.log(`[rebuild] Wrote manifest: ${MANIFEST_PATH}`);
  return out;
}

// -------------------- Staging (optional) --------------------
function initStagingDb(stagingPath) {
  fs.mkdirSync(path.dirname(stagingPath), { recursive: true });
  if (fs.existsSync(stagingPath)) fs.unlinkSync(stagingPath);
  const db = new Database(stagingPath);
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.exec(`
    CREATE TABLE items_raw (
      id INTEGER PRIMARY KEY,
      time INTEGER,
      type TEXT,
      by TEXT,
      title TEXT,
      text TEXT,
      url TEXT,
      score INTEGER,
      parent INTEGER,
      dead INTEGER,
      deleted INTEGER,
      kids_json TEXT
    );
    CREATE INDEX idx_items_raw_time ON items_raw(time);
  `);
  return db;
}

async function stageAllInput(db, files) {
  const insert = db.prepare(`
    INSERT OR REPLACE INTO items_raw
    (id,time,type,by,title,text,url,score,parent,dead,deleted,kids_json)
    VALUES (@id,@time,@type,@by,@title,@text,@url,@score,@parent,@dead,@deleted,@kids_json)
  `);

  const insertMany = db.transaction((rows) => {
    for (const r of rows) insert.run(r);
  });

  let total = 0;
  for (const filename of files) {
    console.log(`[stage] ${filename}`);
    const fileStream = fs.createReadStream(path.join(DATA_DIR, filename));
    const unzip = zlib.createGunzip();
    const rl = readline.createInterface({ input: fileStream.pipe(unzip), crlfDelay: Infinity });

    let batch = [];
    for await (const line of rl) {
      if (!line) continue;
      let item;
      try { item = JSON.parse(line); } catch { continue; }
      const id = safeInt(item.id);
      if (id == null) continue;

      batch.push({
        id,
        time: safeInt(item.time),
        type: item.type || null,
        by: item.by || null,
        title: item.title || null,
        text: item.text || null,
        url: item.url || null,
        score: safeInt(item.score),
        parent: safeInt(item.parent),
        dead: item.dead ? 1 : 0,
        deleted: item.deleted ? 1 : 0,
        kids_json: Array.isArray(item.kids) ? JSON.stringify(item.kids) : null
      });

      if (batch.length >= 10_000) {
        insertMany(batch);
        total += batch.length;
        batch = [];
        writeProgress("stage-insert", `\r[stage] inserted ${total.toLocaleString()} rows`);
      }
    }
    if (batch.length) {
      insertMany(batch);
      total += batch.length;
      batch = [];
      writeProgress("stage-insert", `\r[stage] inserted ${total.toLocaleString()} rows`);
    }
    process.stdout.write("\n");
  }
  console.log(`[stage] done: ${total.toLocaleString()} items`);
  return total;
}

// -------------------- Shard writer --------------------
function createShardDb(shardPath) {
  if (fs.existsSync(shardPath)) fs.unlinkSync(shardPath);
  const db = new Database(shardPath);

  // Fast build settings (safe; shard DB is write-once)
  db.pragma("journal_mode = OFF");
  db.pragma("synchronous = OFF");
  db.pragma("temp_store = MEMORY");
  db.pragma("cache_size = -200000"); // ~200MB cache if available; SQLite interprets negative as KB

  db.exec(`
    CREATE TABLE items (
      id INTEGER PRIMARY KEY,
      type TEXT,
      time INTEGER,
      by TEXT,
      title TEXT,
      text TEXT,
      url TEXT,
      score INTEGER,
      parent INTEGER
    );

    CREATE TABLE edges (
      parent_id INTEGER NOT NULL,
      ord INTEGER NOT NULL,
      child_id INTEGER NOT NULL,
      PRIMARY KEY(parent_id, ord)
    );
  `);

  return db;
}

function finalizeShardDb(db, indexSet = "v1") {
  // Build edges from parent field (siblings ordered by time)
  db.exec(`
    INSERT INTO edges (parent_id, ord, child_id)
    SELECT parent, ROW_NUMBER() OVER (PARTITION BY parent ORDER BY time, id) - 1, id
    FROM items
    WHERE parent IS NOT NULL AND parent != 0
  `);

  // Create indexes after inserts (much faster)
  if (indexSet === "v1") {
    db.exec(`
      CREATE INDEX idx_items_time ON items(time);
      CREATE INDEX idx_items_type_time ON items(type, time);
      CREATE INDEX idx_items_parent ON items(parent);
      CREATE INDEX idx_edges_parent ON edges(parent_id);
    `);
  } else if (indexSet === "minimal") {
    db.exec(`
      CREATE INDEX idx_items_time ON items(time);
    `);
  }
  db.exec(`ANALYZE;`);
}

function classifyChannel(type, title) {
  if (type === "job") return "jobs";
  if (type !== "story") return null;
  if (!title) return "story";
  const t = String(title);
  if (/^Ask HN:/i.test(t)) return "ask";
  if (/^Show HN:/i.test(t)) return "show";
  if (/^Launch HN:/i.test(t)) return "launch";
  return "story";
}

function computeEffectiveTimeStats(sqlitePath, fallbackMin, fallbackMax) {
  const db = new Database(sqlitePath, { readonly: true });
  const timeCountRow = db.prepare(`SELECT COUNT(*) as c FROM items WHERE time IS NOT NULL`).get();
  const timeCount = timeCountRow.c || 0;
  const nullRow = db.prepare(`SELECT COUNT(*) as c FROM items WHERE time IS NULL`).get();
  const timeNull = nullRow.c || 0;
  let tminEff = fallbackMin || null;
  let tmaxEff = fallbackMax || null;
  if (timeCount > 0) {
    const p1 = Math.floor((timeCount - 1) * 0.01);
    const p99 = Math.floor((timeCount - 1) * 0.99);
    const rowMin = db.prepare(`SELECT time as t FROM items WHERE time IS NOT NULL ORDER BY time LIMIT 1 OFFSET ?`).get(p1);
    const rowMax = db.prepare(`SELECT time as t FROM items WHERE time IS NOT NULL ORDER BY time LIMIT 1 OFFSET ?`).get(p99);
    if (rowMin && rowMin.t) tminEff = rowMin.t;
    if (rowMax && rowMax.t) tmaxEff = rowMax.t;
  }
  db.close();
  return { tminEff, tmaxEff, timeNull };
}

// -------------------- Post-pass: VACUUM + gzip + manifest rewrite --------------------
async function vacuumAndGzipAllShards(manifest, opts = {}) {
  const isRestart = !!opts.restart;
  const checkCount = Math.max(1, POST_CONCURRENCY * 2);
  const gzMap = buildShardGzMap();

  console.log(`\n[post] Finalizing shards...`);
  console.log(`[post] vacuum: ${VACUUM_AT_END ? "yes" : "no"} | gzip: ${GZIP_SHARDS ? "yes" : "no"} | keep-sqlite: ${KEEP_SQLITE ? "yes" : "no"} | concurrency: ${POST_CONCURRENCY}`);

  const updated = { ...manifest, shards: manifest.shards.map(s => ({ ...s })) };
  const gzipQueue = [];

  if (isRestart) {
    const sqliteTodo = updated.shards
      .filter(s => fs.existsSync(path.join(OUT_DIR, `shard_${s.sid}.sqlite`)) && !getShardGzInfo(s.sid, gzMap))
      .sort((a, b) => a.sid - b.sid)
      .slice(0, checkCount);

    const allGz = updated.shards
      .filter(s => getShardGzInfo(s.sid, gzMap))
      .sort((a, b) => a.sid - b.sid);
    const allSqlite = updated.shards
      .filter(s => fs.existsSync(path.join(OUT_DIR, `shard_${s.sid}.sqlite`)))
      .sort((a, b) => a.sid - b.sid);
    const firstMissing = allGz.length ? allGz[allGz.length - 1].sid + 1 : (allSqlite[0]?.sid ?? 0);
    const pauseLine = `[post] restart detected: gz=${allGz.length} sqlite=${allSqlite.length} next_sid=${firstMissing}`;
    console.log(pauseLine);

    if (sqliteTodo.length) {
      writeProgress("post-restart-sqlite", `[post] restart: checking ${sqliteTodo.length} sqlite shards...`);
      for (const s of sqliteTodo) {
        const sqlitePath = path.join(OUT_DIR, `shard_${s.sid}.sqlite`);
        if (!checkSqliteIntegrity(sqlitePath)) {
          console.error(`\n[post] sqlite integrity check failed: shard ${s.sid}`);
          process.exit(1);
        }
      }
      process.stdout.write("ok\n");
    }

    const gzExisting = updated.shards
      .filter(s => getShardGzInfo(s.sid, gzMap))
      .sort((a, b) => a.sid - b.sid);
    const gzTail = gzExisting.slice(Math.max(0, gzExisting.length - checkCount));
    if (gzTail.length) {
      writeProgress("post-restart-gzip", `[post] restart: checking ${gzTail.length} gzip shards...`);
      for (const s of gzTail) {
        const gzInfo = getShardGzInfo(s.sid, gzMap);
        if (!gzInfo) continue;
        try {
          validateGzipFileSync(gzInfo.path);
        } catch (err) {
          console.error(`\n[post] gzip validation failed for shard ${s.sid}: ${err && err.message ? err.message : err}`);
          process.exit(1);
        }
      }
      process.stdout.write("ok\n");
    }
  }

  let shardIndex = 0;

  // First pass: identify shards needing processing and those already done
  const needsProcessing = [];
  for (const s of updated.shards) {
    const sqlitePath = path.join(OUT_DIR, `shard_${s.sid}.sqlite`);
    let gzInfo = getShardGzInfo(s.sid, gzMap);
    const hasSqlite = fs.existsSync(sqlitePath);
    const hasGz = !!gzInfo;
    if (!hasSqlite) {
      if (hasGz && GZIP_SHARDS) {
        gzInfo = await normalizeShardGzName(s.sid, gzInfo, gzMap);
        s.file = gzInfo.file;
        s.bytes = fs.statSync(gzInfo.path).size;
      }
      continue;
    }
    needsProcessing.push({ s, sqlitePath, hasGz, gzInfo });
  }

  // Parallel vacuum + stats computation
  if (needsProcessing.length) {
    let done = 0;
    const total = needsProcessing.length;
    await runPool(needsProcessing, POST_CONCURRENCY, async ({ s, sqlitePath, hasGz, gzInfo }) => {
      if (VACUUM_AT_END) {
        const db = new Database(sqlitePath);
        db.exec(`VACUUM;`);
        db.close();
      }

      const eff = computeEffectiveTimeStats(sqlitePath, s.tmin, s.tmax);
      s.tmin_eff = eff.tminEff;
      s.tmax_eff = eff.tmaxEff;
      s.time_null = eff.timeNull;

      if (GZIP_SHARDS) {
        if (!hasGz) {
          gzipQueue.push({ s, sqlitePath });
        } else {
          const normalizedGzInfo = await normalizeShardGzName(s.sid, gzInfo, gzMap);
          s.file = normalizedGzInfo.file;
          s.bytes = fs.statSync(normalizedGzInfo.path).size;
          if (!KEEP_SQLITE) fs.unlinkSync(sqlitePath);
        }
      } else {
        s.file = path.basename(sqlitePath);
        s.bytes = fs.statSync(sqlitePath).size;
      }

      done += 1;
      writeProgress("post-vacuum", `\r[post] vacuum+stats ${done}/${total} | sid ${s.sid}`);
    });
    process.stdout.write("\n");
  }

  if (GZIP_SHARDS && gzipQueue.length) {
    let done = 0;
    const total = gzipQueue.length;
    await runPool(gzipQueue, POST_CONCURRENCY, async ({ s, sqlitePath }) => {
      const tmpPath = `${sqlitePath}.gz.tmp`;
      await gzipFileToTemp(sqlitePath, tmpPath);
      try {
        validateGzipFileSync(tmpPath);
      } catch (err) {
        console.error(`\n[post] gzip validation failed for shard ${s.sid}: ${err && err.message ? err.message : err}`);
        process.exit(1);
      }
      const hash = await hashFileHex(tmpPath, 12);
      const finalName = `shard_${s.sid}_${hash}.sqlite.gz`;
      const finalPath = path.join(OUT_DIR, finalName);
      if (fs.existsSync(finalPath)) {
        fs.unlinkSync(tmpPath);
      } else {
        fs.renameSync(tmpPath, finalPath);
      }
      gzMap.set(s.sid, finalName);
      s.file = finalName;
      s.bytes = fs.statSync(finalPath).size;
      if (!KEEP_SQLITE) fs.unlinkSync(sqlitePath);
      done += 1;
      writeProgress("post-gzip", `\r[post] gzip ${done}/${total} | last sid ${s.sid} | ${mb(s.bytes)}MB`);
    });
    process.stdout.write("\n");
  }

  return updated;
}

// -------------------- Main build --------------------
async function main() {
  if (REBUILD_MANIFEST) {
    try {
      await rebuildManifestFromShards();
    } catch (err) {
      console.error(`[rebuild] failed: ${err && err.message ? err.message : err}`);
      process.exit(1);
    }
    return;
  }
  if (RESTART_ETL) {
    if (CI_EAGER_FINALIZE) {
      console.log(`[post] CI mode: skipping post-pass on restart; rebuilding manifest from shards`);
      try {
        await rebuildManifestFromShards();
      } catch (err) {
        console.error(`[post] rebuild failed: ${err && err.message ? err.message : err}`);
        process.exit(1);
      }
      if (RUN_ARCHIVE_INDEX) {
        try {
          console.log(`[post] building archive index...`);
          require("./build-archive-index.js");
        } catch (err) {
          console.warn(`[post] archive index failed: ${err && err.message ? err.message : err}`);
        }
      }
      return;
    }
    let manifestPath = MANIFEST_PATH;
    const prepassPath = `${MANIFEST_PATH}.prepass`;
    if (!fs.existsSync(manifestPath)) {
      const gzPath = `${MANIFEST_PATH}.gz`;
      if (fs.existsSync(prepassPath)) {
        manifestPath = prepassPath;
      } else if (fs.existsSync(gzPath)) {
        const gz = fs.readFileSync(gzPath);
        const raw = zlib.gunzipSync(gz);
        const manifest = JSON.parse(raw.toString("utf8"));
        const finalManifest = await vacuumAndGzipAllShards(manifest, { restart: true });
        ensureWritableOrBackup(MANIFEST_PATH);
        fs.writeFileSync(MANIFEST_PATH, JSON.stringify(finalManifest, null, 2));
        console.log(`\n[3/3] Wrote manifest: ${MANIFEST_PATH}`);
        if (RUN_ARCHIVE_INDEX) {
          try {
            console.log(`[post] building archive index...`);
            require("./build-archive-index.js");
          } catch (err) {
            console.warn(`[post] archive index failed: ${err && err.message ? err.message : err}`);
          }
        }
        return;
      } else {
        console.warn(`[post] manifest missing; rebuilding from shards...`);
        await rebuildManifestFromShards();
        manifestPath = MANIFEST_PATH;
      }
    }
    const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
    const finalManifest = await vacuumAndGzipAllShards(manifest, { restart: true });
    ensureWritableOrBackup(MANIFEST_PATH);
    fs.writeFileSync(MANIFEST_PATH, JSON.stringify(finalManifest, null, 2));
    console.log(`\n[3/3] Wrote manifest: ${MANIFEST_PATH}`);
    if (RUN_ARCHIVE_INDEX) {
      try {
        console.log(`[post] building archive index...`);
        require("./build-archive-index.js");
      } catch (err) {
        console.warn(`[post] archive index failed: ${err && err.message ? err.message : err}`);
      }
    }
    return;
  }

  const files = FROM_STAGING ? [] : listGzFiles(DATA_DIR);
  if (!FROM_STAGING && !files.length) {
    console.error(`No .json.gz files found in ${DATA_DIR}`);
    process.exit(1);
  }

  let iter;
  let stagedDb = null;

  if (FROM_STAGING) {
    if (!fs.existsSync(STAGING_PATH)) {
      console.error(`Staging DB not found: ${STAGING_PATH}`);
      process.exit(1);
    }
    console.log(`[1/3] Using existing staging DB: ${STAGING_PATH}`);
    stagedDb = new Database(STAGING_PATH, { readonly: true });
    iter = stagedDb.prepare(`
      SELECT id,time,type,by,title,text,url,score,parent,dead,deleted,kids_json
      FROM items_raw
      ORDER BY id ASC
    `).iterate();
  } else if (!PRESORTED) {
    console.log(`[1/3] Staging + sorting by id into ${STAGING_PATH}`);
    stagedDb = initStagingDb(STAGING_PATH);
    await stageAllInput(stagedDb, files);
    if (CI_MODE) {
      console.log(`[stage] CI mode: removing raw .json.gz inputs to save disk`);
      for (const filename of files) {
        const fullPath = path.join(DATA_DIR, filename);
        try {
          fs.unlinkSync(fullPath);
        } catch (err) {
          console.warn(`[stage] failed to remove ${fullPath}: ${err && err.message ? err.message : err}`);
        }
      }
    }

    console.log(`[2/3] Sharding from staging ORDER BY id`);
    iter = stagedDb.prepare(`
      SELECT id,time,type,by,title,text,url,score,parent,dead,deleted,kids_json
      FROM items_raw
      ORDER BY id ASC
    `).iterate();
  } else {
    console.log(`[1/3] Presorted mode: reading input streams directly`);
    iter = (async function* () {
      for (const filename of files) {
        const fileStream = fs.createReadStream(path.join(DATA_DIR, filename));
        const unzip = zlib.createGunzip();
        const rl = readline.createInterface({ input: fileStream.pipe(unzip), crlfDelay: Infinity });
        for await (const line of rl) {
          if (!line) continue;
          let item;
          try { item = JSON.parse(line); } catch { continue; }
          const id = safeInt(item.id);
          if (id == null) continue;
          yield {
            id,
            time: safeInt(item.time),
            type: item.type || null,
            by: item.by || null,
            title: item.title || null,
            text: item.text || null,
            url: item.url || null,
            score: safeInt(item.score),
            parent: safeInt(item.parent),
            dead: item.dead ? 1 : 0,
            deleted: item.deleted ? 1 : 0,
            kids_json: Array.isArray(item.kids) ? JSON.stringify(item.kids) : null
          };
        }
      }
    })();
  }

  // Shard state
  let sid = 0;
  let shardDb = null;
  let shardPath = null;
  let shardIdLo = null;
  let shardIdHi = null;
  let shardTmin = null;
  let shardTmax = null;
  let shardRawBytes = 0;
  let shardCount = 0;
  let shardTimeSample = [];
  let shardTimeSampleCount = 0;

  // Global timeline info (for "start date")
  let globalTmin = null;
  let globalTmax = null;

  const manifest = {
    version: 1,
    created_at: new Date().toISOString(),
    sharding: {
      axis: "id",
      target_mb: TARGET_MB,
      max_mb: MAX_MB,
      max_days: MAX_DAYS,
      max_ids: MAX_IDS,
      gzip_ratio_assumed: GZIP_RATIO,
      target_raw_bytes: TARGET_RAW_BYTES,
      max_raw_bytes: MAX_RAW_BYTES,
      write_batch: WRITE_BATCH,
      vacuum_at_end: VACUUM_AT_END,
      gzip_at_end: GZIP_SHARDS
    },
    shards: []
  };

  if (CI_EAGER_FINALIZE) {
    console.log(`[build] CI mode: eager vacuum+gzip per shard`);
  }

  function openNewShard() {
    shardPath = path.join(OUT_DIR, `shard_${sid}.sqlite`);
    shardDb = createShardDb(shardPath);
    shardIdLo = null;
    shardIdHi = null;
    shardTmin = null;
    shardTmax = null;
    shardRawBytes = 0;
    shardCount = 0;
    shardTimeSample = [];
    shardTimeSampleCount = 0;

    // prepared statements for this shard
    itemStmt = shardDb.prepare(`
      INSERT INTO items (id,type,time,by,title,text,url,score,parent)
      VALUES (@id,@type,@time,@by,@title,@text,@url,@score,@parent)
    `);
    edgeStmt = shardDb.prepare(`
      INSERT INTO edges (parent_id, ord, child_id)
      VALUES (@parent_id, @ord, @child_id)
    `);

    // batch transaction for speed (edges built in finalizeShardDb)
    shardTxBatch = shardDb.transaction((batch) => {
      for (const row of batch) {
        itemStmt.run(row);
      }
    });
  }

  async function closeShard() {
    if (!shardDb) return;

    // finalize schema-level bits (indexes/analyze), no VACUUM here
    finalizeShardDb(shardDb, args["index-set"] || "v1");
    if (CI_EAGER_FINALIZE && VACUUM_AT_END) {
      shardDb.exec(`VACUUM;`);
    }
    shardDb.close();

    let tminEff = null;
    let tmaxEff = null;
    let timeNull = null;
    if (CI_EAGER_FINALIZE) {
      const eff = computeEffectiveTimeStats(shardPath, shardTmin, shardTmax);
      tminEff = eff.tminEff;
      tmaxEff = eff.tmaxEff;
      timeNull = eff.timeNull;
    }

    const sqliteBytes = fs.statSync(shardPath).size;
    let fileName = path.basename(shardPath);
    let fileBytes = sqliteBytes;
    if (CI_EAGER_FINALIZE) {
      const tmpPath = `${shardPath}.gz.tmp`;
      await gzipFileToTemp(shardPath, tmpPath);
      try {
        validateGzipFileSync(tmpPath);
      } catch (err) {
        console.error(`\n[build] gzip validation failed for shard ${sid}: ${err && err.message ? err.message : err}`);
        process.exit(1);
      }
      const hash = await hashFileHex(tmpPath, 12);
      const finalName = `shard_${sid}_${hash}.sqlite.gz`;
      const finalPath = path.join(OUT_DIR, finalName);
      if (fs.existsSync(finalPath)) {
        fs.unlinkSync(tmpPath);
      } else {
        fs.renameSync(tmpPath, finalPath);
      }
      fileName = finalName;
      fileBytes = fs.statSync(finalPath).size;
      if (!KEEP_SQLITE) fs.unlinkSync(shardPath);
    }

    const days = spanDaysFloat(shardTmin, shardTmax);
    const shardRec = {
      sid,
      id_lo: shardIdLo,
      id_hi: shardIdHi,
      tmin: shardTmin,
      tmax: shardTmax,
      tmin_eff: tminEff,
      tmax_eff: tmaxEff,
      time_null: timeNull,
      count: shardCount,
      raw_bytes_est: shardRawBytes,
      file: fileName,
      bytes: fileBytes
    };

    manifest.shards.push(shardRec);

    console.log(
      `[shard ${sid}] ids ${shardIdLo}..${shardIdHi} | items ${shardCount.toLocaleString()} | ` +
      `t ${shardTmin}..${shardTmax} (${isoUTC(shardTmin)} → ${isoUTC(shardTmax)} | ${(days).toFixed(2)}d) | ` +
      `estRaw ${mb(shardRawBytes)}MB | file ${mb(fileBytes)}MB`
    );

    sid++;
    shardDb = null;
    shardPath = null;
  }

  // Prepared statements + batch txn handles (per shard)
  let itemStmt = null;
  let edgeStmt = null;
  let shardTxBatch = null;

  openNewShard();

  let lastLog = Date.now();
  let totalItems = 0;

  // buffered batch for write transactions
  let writeBatch = [];

  // helper: flush batched inserts
  function flushWrites() {
    if (!writeBatch.length) return;
    shardTxBatch(writeBatch);
    writeBatch = [];
  }

  for await (const r of iter) {
    // Normalization
    const row = {
      id: r.id,
      type: r.type,
      time: r.time,
      by: r.by,
      title: r.title,
      text: r.text,
      url: r.url,
      score: r.score,
      parent: r.parent
    };

    // Note: edges are built from parent field in finalizeShardDb()

    // Update shard bounds
    if (shardIdLo == null) shardIdLo = row.id;
    shardIdHi = row.id;
    if (row.time != null) {
      if (shardTmin == null || row.time < shardTmin) shardTmin = row.time;
      if (shardTmax == null || row.time > shardTmax) shardTmax = row.time;

      if (globalTmin == null || row.time < globalTmin) globalTmin = row.time;
      if (globalTmax == null || row.time > globalTmax) globalTmax = row.time;

      shardTimeSampleCount++;
      if (shardTimeSample.length < TIME_SAMPLE_SIZE) {
        shardTimeSample.push(row.time);
      } else {
        const j = Math.floor(Math.random() * shardTimeSampleCount);
        if (j < TIME_SAMPLE_SIZE) shardTimeSample[j] = row.time;
      }
    }

    shardRawBytes += estimateRawBytes(row);
    shardCount++;
    totalItems++;

    // Buffer for batched transaction write
    writeBatch.push(row);
    if (writeBatch.length >= WRITE_BATCH) flushWrites();

    // Decide whether to cut shard (use float days for progress, int days for guard)
    const daysFloat = spanDaysFloat(shardTmin, shardTmax);
    const spanDaysInt = Math.floor(daysFloat);

    let timeTooWide = spanDaysInt >= MAX_DAYS;
    if (timeTooWide && shardTimeSample.length >= 16) {
      const sorted = shardTimeSample.slice().sort((a,b)=>a-b);
      const p1 = sorted[Math.floor((sorted.length - 1) * 0.01)];
      const p99 = sorted[Math.floor((sorted.length - 1) * 0.99)];
      const effDays = spanDaysFloat(p1, p99);
      timeTooWide = Math.floor(effDays) >= MAX_DAYS;
    }

    const shouldCut =
      shardRawBytes >= TARGET_RAW_BYTES ||
      shardRawBytes >= MAX_RAW_BYTES ||
      shardCount >= MAX_IDS ||
      timeTooWide;

    if (shouldCut) {
      flushWrites();
      await closeShard();
      openNewShard();
    }

    // Progress log (decorate with human-readable shard time span)
    const now = Date.now();
    if (now - lastLog > 1000) {
      const days = spanDaysFloat(shardTmin, shardTmax);
      const from = isoUTC(shardTmin);
      const to = isoUTC(shardTmax);
      writeProgress("build-shard", 
        `\r[build] items ${totalItems.toLocaleString()} | shard ${sid} count ${shardCount.toLocaleString()} | ` +
        `span ${(days).toFixed(2)}d (${from} → ${to}) | estRaw ${mb(shardRawBytes)}MB`
      );
      lastLog = now;
    }
  }

  // finalize last shard
  flushWrites();
  process.stdout.write("\n");
  await closeShard();

  if (stagedDb) stagedDb.close();
  if (DELETE_STAGING) {
    if (fs.existsSync(STAGING_PATH)) {
      fs.unlinkSync(STAGING_PATH);
      console.log(`[post] deleted staging DB: ${STAGING_PATH}`);
    } else {
      console.warn(`[post] staging DB not found for deletion: ${STAGING_PATH}`);
    }
  }

  // Derive snapshot end time, and print global start date
  manifest.snapshot_time = globalTmax;
  console.log(`\n[build] global start: ${globalTmin} (${isoUTC(globalTmin)})`);
  console.log(`[build] global end:   ${globalTmax} (${isoUTC(globalTmax)})`);

  // Write pre-pass manifest immediately so we can restart after interruption.
  ensureWritableOrBackup(`${MANIFEST_PATH}.prepass`);
  fs.writeFileSync(`${MANIFEST_PATH}.prepass`, JSON.stringify(manifest, null, 2));
  console.log(`\n[2/3] Wrote prepass manifest: ${MANIFEST_PATH}.prepass`);

  let finalManifest = manifest;
  if (CI_EAGER_FINALIZE) {
    console.log(`\n[3/3] Skipping post-pass (CI eager finalize enabled)`);
  } else {
    // Post-pass: VACUUM + gzip (and rewrite manifest)
    finalManifest = await vacuumAndGzipAllShards(manifest);
    console.log(`\n[3/3] Wrote manifest: ${MANIFEST_PATH}`);
  }

  ensureWritableOrBackup(MANIFEST_PATH);
  fs.writeFileSync(MANIFEST_PATH, JSON.stringify(finalManifest, null, 2));
  if (CI_EAGER_FINALIZE) {
    console.log(`\n[3/3] Wrote manifest: ${MANIFEST_PATH}`);
  }

  if (RUN_ARCHIVE_INDEX) {
    try {
      console.log(`[post] building archive index...`);
      require("./build-archive-index.js");
    } catch (err) {
      console.warn(`[post] archive index failed: ${err && err.message ? err.message : err}`);
    }
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
