#!/usr/bin/env node
/*
 * Build "HN related discussions" sidecar artifacts from staging DB.
 *
 * Design goals:
 * - additive / non-destructive to existing HackerBook pipeline
 * - resumable across long-running phases
 * - clear progress with phases, spinner, rates, ETA
 * - efficient chunked processing with bounded memory
 *
 * Outputs:
 * - docs/static-related-shards/related_<sid>_<hash>.sqlite.gz (or .sqlite)
 * - docs/static-related-manifest.json(.gz)
 * - docs/related-top.json(.gz)
 *
 * Internal resumable state:
 * - data/related-build/related-work.sqlite
 */

import fs from "fs";
import fsp from "fs/promises";
import path from "path";
import zlib from "zlib";
import crypto from "crypto";
import readline from "readline";
import { Writable } from "stream";
import { pipeline } from "stream/promises";
import Database from "better-sqlite3";

const SPINNER_FRAMES = ["|", "/", "-", "\\"];
const PROGRESS_INTERVAL_MS = 100;
const RELATED_WORK_SCHEMA_VERSION = "v6_total_story_comments_cluster_years";
const RANK_MODELS = Object.freeze({
  points: "v2_cluster_points_sum_comment_2_718_ceil",
  legacy: "v1_legacy_mixed"
});

const DEFAULTS = {
  fromStaging: "data/static-staging-hn.sqlite",
  dataDir: "data/raw",
  outDir: "docs/static-related-shards",
  outManifest: "docs/static-related-manifest.json",
  topIndex: "docs/related-top.json",
  workDb: "data/related-build/related-work.sqlite",
  batch: 4000,
  resolveBatch: 500,
  scoreBatch: 700,
  maxDepth: 40,
  maxLinksPerItem: 12,
  linkDumpPenaltyCap: 8,
  maxStoryTokens: 96,
  minEdgeWeight: 0.18,
  maxEdgesPerSource: 5,
  targetMb: 15,
  componentsPerShard: 3500,
  rankModel: "points",
  gzip: false,
  keepSqlite: false,
  reset: false,
  emitOnly: false,
  forceEmit: false
};

const STOP_WORDS = new Set([
  "a","an","and","are","as","at","be","been","but","by","for","from","had","has","have","he","her","his","i","if","in",
  "into","is","it","its","itself","me","more","most","my","no","not","of","on","or","our","ours","s","she","so","t",
  "than","that","the","their","theirs","them","themselves","then","there","these","they","this","those","to","too","up",
  "us","very","was","we","were","what","when","where","which","who","why","with","you","your","yours","ycombinator","hn",
  "news","item","id","https","http","www","com"
]);

function usage() {
  const msg = `Usage:
  node toool/s/build-related-index.mjs [options]

Options:
  --from-staging PATH      Source staging DB (default: ${DEFAULTS.fromStaging})
  --data PATH              Raw ndjson.gz directory if staging DB missing (default: ${DEFAULTS.dataDir})
  --out-dir PATH           Related shard output dir (default: ${DEFAULTS.outDir})
  --out-manifest PATH      Related manifest path (default: ${DEFAULTS.outManifest})
  --top-index PATH         Related top index path (default: ${DEFAULTS.topIndex})
  --work-db PATH           Resumable work DB path (default: ${DEFAULTS.workDb})
  --batch N                Scan batch size (default: ${DEFAULTS.batch})
  --resolve-batch N        Story resolve batch size (default: ${DEFAULTS.resolveBatch})
  --score-batch N          Evidence scoring batch size (default: ${DEFAULTS.scoreBatch})
  --max-depth N            Parent-chain max depth (default: ${DEFAULTS.maxDepth})
  --max-links-per-item N   Link cap per source item (default: ${DEFAULTS.maxLinksPerItem}, 0 = no cap)
  --link-dump-penalty-cap N Link-dump soft cap for score penalty (default: ${DEFAULTS.linkDumpPenaltyCap}, 0 = no penalty cap)
  --max-story-tokens N     Token cap per story profile (default: ${DEFAULTS.maxStoryTokens})
  --min-edge-weight N      Min aggregate edge weight (default: ${DEFAULTS.minEdgeWeight})
  --max-edges-per-source N Max retained edges per source (default: ${DEFAULTS.maxEdgesPerSource})
  --target-mb N            Desired shard target size (default: ${DEFAULTS.targetMb})
  --components-per-shard N Soft shard rotation count (default: ${DEFAULTS.componentsPerShard})
  --rank-model NAME        Rank model: points|legacy (default: ${DEFAULTS.rankModel})
  --gzip                   Write gzip + hash file names
  --keep-sqlite            Keep .sqlite after gzip
  --reset                  Reset work DB + output artifacts before build
  --emit-only              Skip compute phases and emit from existing work DB
  --force-emit             Re-emit shards even if emit phase marked done
  -h, --help               Show help
`;
  process.stdout.write(msg);
}

function parseArgs(argv) {
  const out = { ...DEFAULTS };

  for (let i = 2; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === "-h" || a === "--help") {
      usage();
      process.exit(0);
    }
    if (!a.startsWith("--")) continue;
    const key = a.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) {
      out[key] = true;
      continue;
    }
    out[key] = next;
    i += 1;
  }

  if (out["from-staging"] != null) out.fromStaging = String(out["from-staging"]);
  if (out["data"] != null) out.dataDir = String(out["data"]);
  if (out["out-dir"] != null) out.outDir = String(out["out-dir"]);
  if (out["out-manifest"] != null) out.outManifest = String(out["out-manifest"]);
  if (out["top-index"] != null) out.topIndex = String(out["top-index"]);
  if (out["work-db"] != null) out.workDb = String(out["work-db"]);
  if (out["batch"] != null) out.batch = Number(out["batch"]);
  if (out["resolve-batch"] != null) out.resolveBatch = Number(out["resolve-batch"]);
  if (out["score-batch"] != null) out.scoreBatch = Number(out["score-batch"]);
  if (out["max-depth"] != null) out.maxDepth = Number(out["max-depth"]);
  if (out["max-links-per-item"] != null) out.maxLinksPerItem = Number(out["max-links-per-item"]);
  if (out["link-dump-penalty-cap"] != null) out.linkDumpPenaltyCap = Number(out["link-dump-penalty-cap"]);
  if (out["max-story-tokens"] != null) out.maxStoryTokens = Number(out["max-story-tokens"]);
  if (out["min-edge-weight"] != null) out.minEdgeWeight = Number(out["min-edge-weight"]);
  if (out["max-edges-per-source"] != null) out.maxEdgesPerSource = Number(out["max-edges-per-source"]);
  if (out["target-mb"] != null) out.targetMb = Number(out["target-mb"]);
  if (out["components-per-shard"] != null) out.componentsPerShard = Number(out["components-per-shard"]);
  if (out["rank-model"] != null) out.rankModel = String(out["rank-model"]);
  if (out["gzip"] != null) out.gzip = Boolean(out["gzip"]);
  if (out["keep-sqlite"] != null) out.keepSqlite = Boolean(out["keep-sqlite"]);
  if (out["reset"] != null) out.reset = Boolean(out["reset"]);
  if (out["emit-only"] != null) out.emitOnly = Boolean(out["emit-only"]);
  if (out["force-emit"] != null) out.forceEmit = Boolean(out["force-emit"]);

  return out;
}

function nowIso() {
  return new Date().toISOString();
}

function fmtNum(n) {
  return Number(n || 0).toLocaleString("en-US");
}

function clamp(x, lo, hi) {
  return Math.max(lo, Math.min(hi, x));
}

function tick() {
  return new Promise((resolve) => setImmediate(resolve));
}

function decodeEntities(html) {
  if (!html) return "";
  return String(html)
    .replace(/&quot;/g, '"')
    .replace(/&#x27;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&");
}

function stripHtml(html) {
  if (!html) return "";
  return decodeEntities(String(html))
    .replace(/<a\b[^>]*>/gi, " ")
    .replace(/<\/a>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function hasRelatedKeyword(text) {
  const s = stripHtml(text);
  return /\b(related|past)\s*:/i.test(s) ? 1 : 0;
}

function extractHnItemIds(text, url, maxLinksPerItem) {
  const out = new Set();
  const hasLimit = Number(maxLinksPerItem) > 0;
  const src = `${text || ""}\n${url || ""}`;
  if (!src || !src.includes("item?id=")) return [];

  const patterns = [
    /https?:\/\/news\.ycombinator\.com\/item\?id=(\d+)/gi,
    /\bnews\.ycombinator\.com\/item\?id=(\d+)/gi,
    /(?:href\s*=\s*["'])item\?id=(\d+)/gi,
    /\bitem\?id=(\d{3,})\b/gi
  ];

  for (const re of patterns) {
    re.lastIndex = 0;
    let m;
    while ((m = re.exec(src)) !== null) {
      const id = Number(m[1]);
      if (!Number.isFinite(id) || id <= 0) continue;
      out.add(id);
      if (hasLimit && out.size >= maxLinksPerItem) return [...out];
    }
  }
  return [...out];
}

function tokenizeStory(title, text, maxTokens) {
  const raw = `${stripHtml(title)} ${stripHtml(text)}`.toLowerCase();
  if (!raw) return [];
  const chunks = raw.split(/[^a-z0-9]+/g);
  const toks = [];
  const seen = new Set();
  for (const c of chunks) {
    if (!c || c.length < 3) continue;
    if (STOP_WORDS.has(c)) continue;
    if (/^\d+$/.test(c)) continue;
    if (seen.has(c)) continue;
    seen.add(c);
    toks.push(c);
    if (toks.length >= maxTokens) break;
  }
  toks.sort();
  return toks;
}

function jaccardTokens(a, b) {
  if (!a.length || !b.length) return 0;
  let i = 0;
  let j = 0;
  let inter = 0;
  while (i < a.length && j < b.length) {
    if (a[i] === b[j]) {
      inter += 1;
      i += 1;
      j += 1;
      continue;
    }
    if (a[i] < b[j]) i += 1;
    else j += 1;
  }
  const union = a.length + b.length - inter;
  if (union <= 0) return 0;
  return inter / union;
}

class PrettyProgress {
  constructor() {
    this.startAt = Date.now();
    this.timer = null;
    this.spin = 0;
    this.currentLine = "";
  }

  start() {
    if (this.timer) return;
    this.timer = setInterval(() => {
      if (!this.currentLine) return;
      this.render(this.currentLine, true);
    }, PROGRESS_INTERVAL_MS);
    this.timer.unref();
  }

  stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  elapsedSec() {
    return (Date.now() - this.startAt) / 1000;
  }

  render(line, spinning = false) {
    const frame = spinning ? SPINNER_FRAMES[this.spin++ % SPINNER_FRAMES.length] : " ";
    process.stdout.write(`\r${frame} ${line}\x1b[K`);
  }

  phase(phaseNum, phaseTotal, title, extra = "") {
    this.currentLine = `Phase ${phaseNum}/${phaseTotal}: ${title}${extra ? ` | ${extra}` : ""}`;
    this.render(this.currentLine);
  }

  updateCounts(phaseNum, phaseTotal, title, current, total, extra = "", startTs = null) {
    const pct = total > 0 ? ((current / total) * 100).toFixed(1) : "0.0";
    const elapsed = startTs ? Math.max(0.001, (Date.now() - startTs) / 1000) : Math.max(0.001, this.elapsedSec());
    const rate = current > 0 ? current / elapsed : 0;
    const rateLabel = rate >= 10 ? Math.round(rate).toLocaleString("en-US") : rate.toFixed(1);
    const rem = total > current && rate > 0 ? Math.round((total - current) / rate) : 0;
    const eta = rem > 60 ? `${Math.floor(rem / 60)}m${rem % 60}s` : `${rem}s`;
    this.currentLine = `Phase ${phaseNum}/${phaseTotal}: ${title} | ${fmtNum(current)}/${fmtNum(total)} (${pct}%) | ${rateLabel}/s | ETA ${eta}${extra ? ` | ${extra}` : ""}`;
    this.render(this.currentLine);
  }

  done(msg) {
    process.stdout.write(`\r✓ ${msg}\x1b[K\n`);
    this.currentLine = "";
  }
}

function ensureDirSync(p) {
  fs.mkdirSync(p, { recursive: true });
}

function removeIfExistsSync(p) {
  if (!fs.existsSync(p)) return;
  fs.rmSync(p, { recursive: true, force: true });
}

function openDb(dbPath, readonly = false) {
  const db = new Database(dbPath, { readonly });
  if (!readonly) {
    db.pragma("journal_mode = WAL");
    db.pragma("synchronous = NORMAL");
    db.pragma("temp_store = MEMORY");
    db.pragma("cache_size = -200000");
  }
  return db;
}

function tableColumns(db, tableName) {
  try {
    const rows = db.prepare(`PRAGMA table_info(${tableName})`).all();
    return new Set(rows.map((r) => String(r.name)));
  } catch {
    return new Set();
  }
}

function ensureColumn(db, tableName, columnName, columnTypeSql) {
  const cols = tableColumns(db, tableName);
  if (cols.has(columnName)) return;
  db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnTypeSql}`);
}

function listGzFiles(dirPath) {
  if (!fs.existsSync(dirPath)) return [];
  return fs.readdirSync(dirPath).filter((f) => f.endsWith(".json.gz")).sort();
}

function safeInt(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

function initStagingDb(stagingPath) {
  ensureDirSync(path.dirname(stagingPath));
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
      kids_json TEXT,
      descendants INTEGER
    );
    CREATE INDEX idx_items_raw_time ON items_raw(time);
  `);
  return db;
}

async function buildStagingFromRaw({ stagingPath, dataDir, progress }) {
  const files = listGzFiles(dataDir);
  if (!files.length) {
    throw new Error(`No .json.gz files found in ${dataDir} and staging DB is missing (${stagingPath})`);
  }

  const tmpPath = `${stagingPath}.tmp`;
  if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath);
  const db = initStagingDb(tmpPath);

  const insert = db.prepare(`
    INSERT OR REPLACE INTO items_raw
    (id,time,type,by,title,text,url,score,parent,dead,deleted,kids_json,descendants)
    VALUES (@id,@time,@type,@by,@title,@text,@url,@score,@parent,@dead,@deleted,@kids_json,@descendants)
  `);
  const insertMany = db.transaction((rows) => {
    for (const r of rows) insert.run(r);
  });

  let total = 0;
  for (let i = 0; i < files.length; i += 1) {
    const name = files[i];
    const fullPath = path.join(dataDir, name);
    progress.phase(0, 6, "Building staging DB from raw files", `${i + 1}/${files.length} ${name} | inserted=${fmtNum(total)}`);

    const fileStream = fs.createReadStream(fullPath);
    const unzip = zlib.createGunzip();
    const rl = readline.createInterface({ input: fileStream.pipe(unzip), crlfDelay: Infinity });

    let batch = [];
    for await (const line of rl) {
      if (!line) continue;
      let item;
      try {
        item = JSON.parse(line);
      } catch {
        continue;
      }
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
        kids_json: Array.isArray(item.kids) ? JSON.stringify(item.kids) : null,
        descendants: safeInt(item.descendants)
      });
      if (batch.length >= 10000) {
        insertMany(batch);
        total += batch.length;
        batch = [];
      }
    }
    if (batch.length) {
      insertMany(batch);
      total += batch.length;
      batch = [];
    }
    await tick();
  }

  db.close();
  if (fs.existsSync(stagingPath)) fs.unlinkSync(stagingPath);
  fs.renameSync(tmpPath, stagingPath);
  progress.done(`Phase 0/6 complete | built staging DB from raw (${fmtNum(total)} rows)`);
}

function initWorkDb(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS meta (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS raw_links (
      evidence_item_id INTEGER NOT NULL,
      source_item_id INTEGER NOT NULL,
      source_type TEXT,
      source_parent INTEGER,
      source_time INTEGER,
      source_score INTEGER,
      has_keyword INTEGER NOT NULL,
      target_item_id INTEGER NOT NULL,
      PRIMARY KEY(evidence_item_id, target_item_id)
    );
    CREATE INDEX IF NOT EXISTS idx_raw_links_source_item ON raw_links(source_item_id);
    CREATE INDEX IF NOT EXISTS idx_raw_links_target_item ON raw_links(target_item_id);
    CREATE INDEX IF NOT EXISTS idx_raw_links_evidence ON raw_links(evidence_item_id);

    CREATE TABLE IF NOT EXISTS item_story_cache (
      item_id INTEGER PRIMARY KEY,
      story_id INTEGER,
      resolved INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_item_story_story ON item_story_cache(story_id);
    CREATE INDEX IF NOT EXISTS idx_item_story_resolved ON item_story_cache(resolved);

    CREATE TABLE IF NOT EXISTS story_profiles (
      story_id INTEGER PRIMARY KEY,
      token_json TEXT NOT NULL,
      token_count INTEGER NOT NULL,
      title TEXT,
      by TEXT,
      time INTEGER,
      score INTEGER,
      url TEXT,
      comment_total INTEGER NOT NULL DEFAULT -1
    );

    CREATE TABLE IF NOT EXISTS edge_scores (
      source_story_id INTEGER NOT NULL,
      target_story_id INTEGER NOT NULL,
      weight REAL NOT NULL,
      source_align REAL NOT NULL,
      self_consistency REAL NOT NULL,
      evidence_count INTEGER NOT NULL DEFAULT 1,
      last_time INTEGER,
      PRIMARY KEY(source_story_id, target_story_id)
    );
    CREATE INDEX IF NOT EXISTS idx_edge_scores_weight ON edge_scores(weight DESC);
  `);

  // Backward-compatible schema migration for pre-url work DBs.
  ensureColumn(db, "story_profiles", "url", "TEXT");
  ensureColumn(db, "story_profiles", "comment_total", "INTEGER NOT NULL DEFAULT -1");
}

function getMeta(db, key, fallback = null) {
  const row = db.prepare("SELECT value FROM meta WHERE key=?").get(key);
  return row ? row.value : fallback;
}

function setMeta(db, key, value) {
  db.prepare(`
    INSERT INTO meta(key, value) VALUES (?, ?)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value
  `).run(key, String(value));
}

function getMetaBool(db, key) {
  return getMeta(db, key, "0") === "1";
}

function markPhaseDone(db, phaseKey) {
  setMeta(db, phaseKey, "1");
}

function resolveStoriesForIds(stagingDb, ids, maxDepth) {
  if (!ids.length) return new Map();
  const placeholders = ids.map(() => "?").join(",");
  const sql = `
    WITH RECURSIVE anc(start_id, id, parent, type, depth) AS (
      SELECT i.id AS start_id, i.id, i.parent, i.type, 0
      FROM items_raw i
      WHERE i.id IN (${placeholders})
      UNION ALL
      SELECT anc.start_id, p.id, p.parent, p.type, anc.depth + 1
      FROM anc
      JOIN items_raw p ON p.id = anc.parent
      WHERE anc.parent IS NOT NULL AND anc.depth < ${Math.max(1, Number(maxDepth) || 40)}
    )
    SELECT start_id, story_id
    FROM (
      SELECT
        start_id,
        id AS story_id,
        depth,
        ROW_NUMBER() OVER (PARTITION BY start_id ORDER BY depth ASC) AS rn
      FROM anc
      WHERE type='story'
    ) ranked
    WHERE rn=1
  `;
  const rows = stagingDb.prepare(sql).all(...ids);
  const out = new Map();
  for (const r of rows) {
    out.set(Number(r.start_id), Number(r.story_id));
  }
  return out;
}

async function phaseExtractRawLinks(ctx) {
  const { stagingDb, workDb, args, progress } = ctx;
  const PHASE_NUM = 1;
  const PHASE_TOTAL = 6;
  const doneKey = "phase_extract_raw_links_done";
  if (getMetaBool(workDb, doneKey)) {
    progress.done("Phase 1/6: raw link extraction already complete");
    return;
  }

  const maxIdRow = stagingDb.prepare("SELECT MAX(id) AS max_id FROM items_raw").get();
  const maxId = Number(maxIdRow?.max_id || 0);
  const selectRows = stagingDb.prepare(`
    SELECT id, type, parent, time, score, text, url
    FROM items_raw
    WHERE id > ?
    ORDER BY id ASC
    LIMIT ?
  `);
  const insertLink = workDb.prepare(`
    INSERT OR IGNORE INTO raw_links (
      evidence_item_id, source_item_id, source_type, source_parent, source_time, source_score, has_keyword, target_item_id
    ) VALUES (
      @evidence_item_id, @source_item_id, @source_type, @source_parent, @source_time, @source_score, @has_keyword, @target_item_id
    )
  `);
  const insertBatch = workDb.transaction((rows) => {
    for (const r of rows) insertLink.run(r);
  });

  let lastId = Number(getMeta(workDb, "extract_last_id", "0")) || 0;
  let processedRows = Number(getMeta(workDb, "extract_processed_rows", "0")) || 0;
  let linksInserted = Number(getMeta(workDb, "extract_links_inserted", "0")) || 0;
  const startTs = Date.now();

  progress.phase(PHASE_NUM, PHASE_TOTAL, "Scanning items for HN links");
  while (true) {
    const rows = selectRows.all(lastId, args.batch);
    if (!rows.length) break;

    const toInsert = [];
    for (const row of rows) {
      const id = Number(row.id);
      const text = row.text || "";
      const url = row.url || "";
      if (!text.includes("item?id=") && !url.includes("item?id=")) continue;
      const targets = extractHnItemIds(text, url, args.maxLinksPerItem);
      if (!targets.length) continue;
      const keyword = hasRelatedKeyword(text);
      const uniq = new Set(targets);
      for (const targetId of uniq) {
        if (!Number.isFinite(targetId) || targetId <= 0) continue;
        if (targetId === id) continue;
        toInsert.push({
          evidence_item_id: id,
          source_item_id: id,
          source_type: row.type || null,
          source_parent: row.parent == null ? null : Number(row.parent),
          source_time: row.time == null ? null : Number(row.time),
          source_score: row.score == null ? null : Number(row.score),
          has_keyword: keyword,
          target_item_id: targetId
        });
      }
    }

    if (toInsert.length) {
      insertBatch(toInsert);
      linksInserted += toInsert.length;
    }
    lastId = Number(rows[rows.length - 1].id);
    processedRows += rows.length;

    setMeta(workDb, "extract_last_id", lastId);
    setMeta(workDb, "extract_processed_rows", processedRows);
    setMeta(workDb, "extract_links_inserted", linksInserted);

    progress.updateCounts(
      PHASE_NUM,
      PHASE_TOTAL,
      "Scanning items for HN links",
      lastId,
      maxId,
      `rows=${fmtNum(processedRows)} links=${fmtNum(linksInserted)}`,
      startTs
    );
    await tick();
  }

  markPhaseDone(workDb, doneKey);
  progress.done(`Phase 1/6 complete | scanned=${fmtNum(processedRows)} | raw links=${fmtNum(linksInserted)}`);
}

async function phaseResolveStoryIds(ctx) {
  const { stagingDb, workDb, args, progress } = ctx;
  const PHASE_NUM = 2;
  const PHASE_TOTAL = 6;
  const doneKey = "phase_resolve_story_ids_done";
  if (getMetaBool(workDb, doneKey)) {
    progress.done("Phase 2/6: item->story resolution already complete");
    return;
  }

  progress.phase(PHASE_NUM, PHASE_TOTAL, "Preparing item/story candidates");
  workDb.exec(`
    INSERT OR IGNORE INTO item_story_cache(item_id, story_id, resolved)
    SELECT source_item_id, NULL, 0 FROM raw_links;
    INSERT OR IGNORE INTO item_story_cache(item_id, story_id, resolved)
    SELECT target_item_id, NULL, 0 FROM raw_links;
  `);

  // Helpful for parent-chain traversal.
  stagingDb.exec(`
    CREATE INDEX IF NOT EXISTS idx_items_raw_parent ON items_raw(parent);
  `);

  const totalRow = workDb.prepare("SELECT COUNT(*) AS c FROM item_story_cache").get();
  const total = Number(totalRow?.c || 0);
  let doneCount = Number(getMeta(workDb, "resolve_done_count", "0")) || 0;

  const selectBatch = workDb.prepare(`
    SELECT item_id
    FROM item_story_cache
    WHERE resolved=0
    LIMIT ?
  `);
  const updateOne = workDb.prepare(`
    UPDATE item_story_cache
    SET story_id=?, resolved=1
    WHERE item_id=?
  `);
  const updateBatch = workDb.transaction((pairs) => {
    for (const p of pairs) updateOne.run(p.story_id, p.item_id);
  });

  const startTs = Date.now();
  while (true) {
    const ids = selectBatch.all(args.resolveBatch).map((r) => Number(r.item_id));
    if (!ids.length) break;

    const resolved = resolveStoriesForIds(stagingDb, ids, args.maxDepth);
    const pairs = ids.map((itemId) => ({
      item_id: itemId,
      story_id: resolved.has(itemId) ? resolved.get(itemId) : null
    }));
    updateBatch(pairs);
    doneCount += pairs.length;
    setMeta(workDb, "resolve_done_count", doneCount);

    progress.updateCounts(
      PHASE_NUM,
      PHASE_TOTAL,
      "Resolving source/target items to story IDs",
      doneCount,
      total,
      `batch=${fmtNum(pairs.length)}`,
      startTs
    );
    await tick();
  }

  markPhaseDone(workDb, doneKey);
  const storiesRow = workDb.prepare("SELECT COUNT(*) AS c FROM item_story_cache WHERE story_id IS NOT NULL").get();
  progress.done(`Phase 2/6 complete | candidates=${fmtNum(total)} | resolved stories=${fmtNum(storiesRow?.c || 0)}`);
}

async function phaseBuildStoryProfiles(ctx) {
  const { stagingDb, workDb, args, progress } = ctx;
  const PHASE_NUM = 3;
  const PHASE_TOTAL = 6;
  const doneKey = "phase_story_profiles_done";
  if (getMetaBool(workDb, doneKey)) {
    progress.done("Phase 3/6: story profile build already complete");
    return;
  }

  progress.phase(PHASE_NUM, PHASE_TOTAL, "Preparing story profiles");
  const stagingCols = tableColumns(stagingDb, "items_raw");
  if (!stagingCols.has("descendants")) {
    throw new Error(
      "staging items_raw is missing descendants; rebuild staging DB from raw (delete data/static-staging-hn.sqlite or run with --reset)"
    );
  }
  workDb.exec(`
    INSERT OR IGNORE INTO story_profiles(story_id, token_json, token_count, title, by, time, score)
    SELECT DISTINCT story_id, '[]', -1, NULL, NULL, NULL, NULL
    FROM item_story_cache
    WHERE story_id IS NOT NULL;
  `);

  const totalRow = workDb.prepare("SELECT COUNT(*) AS c FROM story_profiles").get();
  const total = Number(totalRow?.c || 0);
  const selectBatch = workDb.prepare(`
    SELECT story_id
    FROM story_profiles
    WHERE token_count < 0
    LIMIT ?
  `);
  const updateProfile = workDb.prepare(`
    UPDATE story_profiles
    SET token_json=?, token_count=?, title=?, by=?, time=?, score=?, url=?, comment_total=?
    WHERE story_id=?
  `);
  const updateBatch = workDb.transaction((rows) => {
    for (const r of rows) {
      updateProfile.run(
        r.token_json,
        r.token_count,
        r.title,
        r.by,
        r.time,
        r.score,
        r.url,
        r.comment_total,
        r.story_id
      );
    }
  });

  let doneCount = Number(getMeta(workDb, "story_profiles_done_count", "0")) || 0;
  const startTs = Date.now();
  while (true) {
    const ids = selectBatch.all(args.resolveBatch).map((r) => Number(r.story_id));
    if (!ids.length) break;

    const placeholders = ids.map(() => "?").join(",");
    const srcRows = stagingDb.prepare(`
      SELECT id, title, text, by, time, score, url, descendants
      FROM items_raw
      WHERE id IN (${placeholders})
    `).all(...ids);

    const byId = new Map();
    for (const r of srcRows) byId.set(Number(r.id), r);

    const updates = [];
    for (const sid of ids) {
      const row = byId.get(sid);
      const title = row?.title || null;
      const by = row?.by || null;
      const time = row?.time == null ? null : Number(row.time);
      const score = row?.score == null ? null : Number(row.score);
      const url = row?.url || null;
      const comment_total = row?.descendants == null ? 0 : Number(row.descendants);
      const tokens = tokenizeStory(row?.title || "", row?.text || "", args.maxStoryTokens);
      updates.push({
        story_id: sid,
        token_json: JSON.stringify(tokens),
        token_count: tokens.length,
        title,
        by,
        time,
        score,
        url,
        comment_total
      });
    }

    if (updates.length) updateBatch(updates);
    doneCount += updates.length;
    setMeta(workDb, "story_profiles_done_count", doneCount);

    progress.updateCounts(
      PHASE_NUM,
      PHASE_TOTAL,
      "Building story token profiles",
      doneCount,
      total,
      `batch=${fmtNum(updates.length)}`,
      startTs
    );
    await tick();
  }

  markPhaseDone(workDb, doneKey);
  progress.done(`Phase 3/6 complete | story profiles=${fmtNum(total)}`);
}

async function backfillStoryProfileUrls(ctx) {
  const { stagingDb, workDb, progress, args } = ctx;
  if (!stagingDb) return;
  const hasItemsRaw = Number(
    stagingDb.prepare(`
      SELECT COUNT(*) AS c
      FROM sqlite_master
      WHERE type='table' AND name='items_raw'
    `).get()?.c || 0
  );
  if (!hasItemsRaw) {
    progress.done("Backfill story URLs skipped | staging table items_raw missing");
    return;
  }
  const missingBefore = Number(
    workDb.prepare("SELECT COUNT(*) AS c FROM story_profiles WHERE url IS NULL OR url=''").get()?.c || 0
  );
  if (!missingBefore) return;

  const selectMissing = workDb.prepare(`
    SELECT story_id
    FROM story_profiles
    WHERE (url IS NULL OR url='') AND story_id > ?
    ORDER BY story_id ASC
    LIMIT ?
  `);
  const updateUrl = workDb.prepare(`
    UPDATE story_profiles
    SET url=?
    WHERE story_id=?
  `);
  const updateBatch = workDb.transaction((rows) => {
    for (const r of rows) updateUrl.run(r.url, r.story_id);
  });

  const batchSize = Math.max(100, Number(args.resolveBatch || 500));
  let processed = 0;
  let filled = 0;
  let lastStoryId = 0;
  const startTs = Date.now();
  while (true) {
    const ids = selectMissing.all(lastStoryId, batchSize).map((r) => Number(r.story_id));
    if (!ids.length) break;
    lastStoryId = Number(ids[ids.length - 1]) || lastStoryId;
    const placeholders = ids.map(() => "?").join(",");
    const srcRows = stagingDb.prepare(`
      SELECT id, url
      FROM items_raw
      WHERE id IN (${placeholders}) AND url IS NOT NULL AND url!=''
    `).all(...ids);
    const updates = srcRows.map((r) => ({
      story_id: Number(r.id),
      url: r.url
    }));
    if (updates.length) {
      updateBatch(updates);
      filled += updates.length;
    }
    processed += ids.length;
    progress.updateCounts(
      0,
      6,
      "Backfilling story URLs",
      processed,
      missingBefore,
      `filled=${fmtNum(filled)}`,
      startTs
    );
    await tick();
  }
  const missingAfter = Number(
    workDb.prepare("SELECT COUNT(*) AS c FROM story_profiles WHERE url IS NULL OR url=''").get()?.c || 0
  );
  progress.done(`Backfill story URLs complete | remaining missing=${fmtNum(missingAfter)}`);
}

async function backfillStoryCommentTotals(ctx) {
  const { stagingDb, workDb, args, progress } = ctx;
  if (!stagingDb) return;
  const cols = tableColumns(stagingDb, "items_raw");
  if (!cols.has("descendants")) {
    progress.done("Backfill story comment totals skipped | staging.items_raw.descendants missing");
    return;
  }
  const missingBefore = Number(
    workDb.prepare("SELECT COUNT(*) AS c FROM story_profiles WHERE comment_total < 0").get()?.c || 0
  );
  if (!missingBefore) return;

  const selectMissing = workDb.prepare(`
    SELECT story_id
    FROM story_profiles
    WHERE comment_total < 0 AND story_id > ?
    ORDER BY story_id ASC
    LIMIT ?
  `);
  const updateCommentTotal = workDb.prepare(`
    UPDATE story_profiles
    SET comment_total=?
    WHERE story_id=?
  `);
  const updateBatch = workDb.transaction((rows) => {
    for (const r of rows) updateCommentTotal.run(r.comment_total, r.story_id);
  });

  const batchSize = Math.max(500, Math.min(5000, Number(args.resolveBatch || 500)));
  let lastStoryId = 0;
  let processed = 0;
  let filled = 0;
  const startTs = Date.now();
  while (true) {
    const ids = selectMissing.all(lastStoryId, batchSize).map((r) => Number(r.story_id));
    if (!ids.length) break;
    lastStoryId = Number(ids[ids.length - 1]) || lastStoryId;
    const placeholders = ids.map(() => "?").join(",");
    const srcRows = stagingDb.prepare(`
      SELECT id, descendants
      FROM items_raw
      WHERE id IN (${placeholders})
    `).all(...ids);
    const byId = new Map(srcRows.map((r) => [Number(r.id), r]));
    const updates = ids.map((sid) => ({
      story_id: sid,
      comment_total: byId.get(sid)?.descendants == null ? 0 : Number(byId.get(sid).descendants)
    }));
    updateBatch(updates);
    processed += ids.length;
    filled += updates.length;
    progress.updateCounts(
      3,
      6,
      "Backfilling story comment totals",
      processed,
      missingBefore,
      `filled=${fmtNum(filled)}`,
      startTs
    );
    await tick();
  }
  const missingAfter = Number(
    workDb.prepare("SELECT COUNT(*) AS c FROM story_profiles WHERE comment_total < 0").get()?.c || 0
  );
  progress.done(`Backfill story comment totals complete | remaining missing=${fmtNum(missingAfter)}`);
}

function createTokenCache(workDb, limit = 20000) {
  const selectProfile = workDb.prepare(`
    SELECT token_json
    FROM story_profiles
    WHERE story_id=?
    LIMIT 1
  `);
  const cache = new Map();
  return {
    get(storyId) {
      const sid = Number(storyId);
      if (cache.has(sid)) {
        const val = cache.get(sid);
        cache.delete(sid);
        cache.set(sid, val);
        return val;
      }
      const row = selectProfile.get(sid);
      const tokens = row?.token_json ? JSON.parse(row.token_json) : [];
      cache.set(sid, tokens);
      if (cache.size > limit) {
        const first = cache.keys().next().value;
        cache.delete(first);
      }
      return tokens;
    }
  };
}

async function phaseScoreEdges(ctx) {
  const { workDb, args, progress } = ctx;
  const PHASE_NUM = 4;
  const PHASE_TOTAL = 6;
  const doneKey = "phase_score_edges_done";
  if (getMetaBool(workDb, doneKey)) {
    progress.done("Phase 4/6: evidence scoring already complete");
    return;
  }

  const totalEvidenceRow = workDb.prepare("SELECT COUNT(DISTINCT evidence_item_id) AS c FROM raw_links").get();
  const totalEvidence = Number(totalEvidenceRow?.c || 0);
  if (totalEvidence === 0) {
    markPhaseDone(workDb, doneKey);
    progress.done("Phase 4/6 complete | no evidence rows");
    return;
  }

  const tokenCache = createTokenCache(workDb);
  let lastEvidenceId = Number(getMeta(workDb, "score_last_evidence_id", "0")) || 0;
  let processedEvidence = Number(getMeta(workDb, "score_processed_evidence", "0")) || 0;
  let emittedEdges = Number(getMeta(workDb, "score_emitted_edges", "0")) || 0;
  const startTs = Date.now();

  const selectGrouped = workDb.prepare(`
    SELECT
      rl.evidence_item_id,
      src.story_id AS source_story_id,
      MAX(rl.source_time) AS source_time,
      MAX(rl.source_score) AS source_score,
      MAX(rl.has_keyword) AS has_keyword,
      GROUP_CONCAT(DISTINCT tgt.story_id) AS target_story_csv
    FROM raw_links rl
    JOIN item_story_cache src ON src.item_id = rl.source_item_id AND src.resolved=1
    JOIN item_story_cache tgt ON tgt.item_id = rl.target_item_id AND tgt.resolved=1
    WHERE rl.evidence_item_id > ?
      AND src.story_id IS NOT NULL
      AND tgt.story_id IS NOT NULL
    GROUP BY rl.evidence_item_id, src.story_id
    ORDER BY rl.evidence_item_id ASC
    LIMIT ?
  `);

  const upsertEdge = workDb.prepare(`
    INSERT INTO edge_scores(
      source_story_id, target_story_id, weight, source_align, self_consistency, evidence_count, last_time
    ) VALUES (?, ?, ?, ?, ?, 1, ?)
    ON CONFLICT(source_story_id, target_story_id) DO UPDATE SET
      weight = edge_scores.weight + excluded.weight,
      source_align = CASE WHEN excluded.source_align > edge_scores.source_align THEN excluded.source_align ELSE edge_scores.source_align END,
      self_consistency = CASE WHEN excluded.self_consistency > edge_scores.self_consistency THEN excluded.self_consistency ELSE edge_scores.self_consistency END,
      evidence_count = edge_scores.evidence_count + 1,
      last_time = CASE
        WHEN edge_scores.last_time IS NULL THEN excluded.last_time
        WHEN excluded.last_time IS NULL THEN edge_scores.last_time
        WHEN excluded.last_time > edge_scores.last_time THEN excluded.last_time
        ELSE edge_scores.last_time
      END
  `);
  const upsertBatch = workDb.transaction((rows) => {
    for (const r of rows) {
      upsertEdge.run(
        r.source_story_id,
        r.target_story_id,
        r.weight,
        r.source_align,
        r.self_consistency,
        r.last_time
      );
    }
  });

  progress.phase(PHASE_NUM, PHASE_TOTAL, "Scoring evidence and aggregating edges");
  while (true) {
    const batch = selectGrouped.all(lastEvidenceId, args.scoreBatch);
    if (!batch.length) break;

    const outEdges = [];
    for (const row of batch) {
      const sourceStoryId = Number(row.source_story_id);
      const sourceTokens = tokenCache.get(sourceStoryId);
      if (!sourceTokens.length) continue;

      const targetIds = String(row.target_story_csv || "")
        .split(",")
        .map((x) => Number(x))
        .filter((x) => Number.isFinite(x) && x > 0 && x !== sourceStoryId);
      if (!targetIds.length) continue;

      const uniqAllTargets = [...new Set(targetIds)];
      const uniqTargets = Number(args.maxLinksPerItem) > 0
        ? uniqAllTargets.slice(0, args.maxLinksPerItem)
        : uniqAllTargets;
      if (!uniqTargets.length) continue;

      const targetTokens = uniqTargets.map((tid) => tokenCache.get(tid));
      const sourceSims = targetTokens.map((tok) => jaccardTokens(sourceTokens, tok));
      const sourceAlign = sourceSims.reduce((a, b) => a + b, 0) / sourceSims.length;

      let selfConsistency = sourceAlign;
      if (uniqTargets.length > 1) {
        let pairSum = 0;
        let pairCount = 0;
        for (let i = 0; i < targetTokens.length; i += 1) {
          for (let j = i + 1; j < targetTokens.length; j += 1) {
            pairSum += jaccardTokens(targetTokens[i], targetTokens[j]);
            pairCount += 1;
          }
        }
        selfConsistency = pairCount ? (pairSum / pairCount) : sourceAlign;
      }

      const keywordBoost = Number(row.has_keyword) ? 0.15 : 0;
      const penalty = clamp(0.35 + (2.0 * selfConsistency), 0.35, 1.0);
      const linkDumpCap = Number(args.linkDumpPenaltyCap);
      const listPenalty = (linkDumpCap > 0 && uniqTargets.length > linkDumpCap)
        ? (linkDumpCap / uniqTargets.length)
        : 1.0;
      const baseScore = (0.55 * sourceAlign + 0.45 * selfConsistency + keywordBoost) * penalty * listPenalty;
      if (baseScore <= 0.01) continue;

      for (let i = 0; i < uniqTargets.length; i += 1) {
        const tid = uniqTargets[i];
        const edgeWeight = baseScore * (0.6 + 0.4 * sourceSims[i]);
        if (edgeWeight <= 0.001) continue;
        outEdges.push({
          source_story_id: sourceStoryId,
          target_story_id: tid,
          weight: edgeWeight,
          source_align: sourceAlign,
          self_consistency: selfConsistency,
          last_time: row.source_time == null ? null : Number(row.source_time)
        });
      }
    }

    if (outEdges.length) {
      upsertBatch(outEdges);
      emittedEdges += outEdges.length;
    }
    processedEvidence += batch.length;
    lastEvidenceId = Number(batch[batch.length - 1].evidence_item_id);

    setMeta(workDb, "score_last_evidence_id", lastEvidenceId);
    setMeta(workDb, "score_processed_evidence", processedEvidence);
    setMeta(workDb, "score_emitted_edges", emittedEdges);

    progress.updateCounts(
      PHASE_NUM,
      PHASE_TOTAL,
      "Scoring evidence and aggregating edges",
      processedEvidence,
      totalEvidence,
      `edges=${fmtNum(emittedEdges)}`,
      startTs
    );
    await tick();
  }

  markPhaseDone(workDb, doneKey);
  const edgeCountRow = workDb.prepare("SELECT COUNT(*) AS c FROM edge_scores").get();
  progress.done(`Phase 4/6 complete | scored evidence=${fmtNum(processedEvidence)} | unique edges=${fmtNum(edgeCountRow?.c || 0)}`);
}

function ufFind(parent, x) {
  let cur = x;
  while (parent.get(cur) !== cur) cur = parent.get(cur);
  const root = cur;
  cur = x;
  while (parent.get(cur) !== cur) {
    const nxt = parent.get(cur);
    parent.set(cur, root);
    cur = nxt;
  }
  return root;
}

function ufUnion(parent, rank, a, b) {
  const ra = ufFind(parent, a);
  const rb = ufFind(parent, b);
  if (ra === rb) return;
  const rka = rank.get(ra) || 0;
  const rkb = rank.get(rb) || 0;
  if (rka < rkb) {
    parent.set(ra, rb);
  } else if (rkb < rka) {
    parent.set(rb, ra);
  } else {
    parent.set(rb, ra);
    rank.set(ra, rka + 1);
  }
}

async function phaseBuildComponents(ctx) {
  const { workDb, args, progress, rankModel, rankModelVersion } = ctx;
  const PHASE_NUM = 5;
  const PHASE_TOTAL = 6;
  const doneKey = "phase_components_done";
  if (getMetaBool(workDb, doneKey)) {
    progress.done("Phase 5/6: component build already complete");
    return;
  }

  progress.phase(PHASE_NUM, PHASE_TOTAL, "Pruning edges and building components");
  const isLegacyRank = rankModel === "legacy";
  const rankSelectExpr = isLegacyRank
    ? `ri.legacy_rank`
    : `(
        CAST(ri.raw_rank AS INTEGER) +
        CASE
          WHEN ri.raw_rank > CAST(ri.raw_rank AS INTEGER) THEN 1
          ELSE 0
        END
      )`;

  workDb.exec(`
    DROP TABLE IF EXISTS edge_pruned;
    CREATE TABLE edge_pruned (
      source_story_id INTEGER NOT NULL,
      target_story_id INTEGER NOT NULL,
      weight REAL NOT NULL,
      evidence_count INTEGER NOT NULL,
      last_time INTEGER
    );
  `);

  const pruneInsert = workDb.prepare(`
    INSERT INTO edge_pruned(source_story_id, target_story_id, weight, evidence_count, last_time)
    SELECT source_story_id, target_story_id, weight, evidence_count, last_time
    FROM (
      SELECT
        source_story_id,
        target_story_id,
        weight,
        evidence_count,
        last_time,
        ROW_NUMBER() OVER (
          PARTITION BY source_story_id
          ORDER BY weight DESC, evidence_count DESC, COALESCE(last_time,0) DESC, target_story_id DESC
        ) AS rn
      FROM edge_scores
      WHERE weight >= ?
    ) ranked
    WHERE rn <= ?
  `);
  pruneInsert.run(args.minEdgeWeight, args.maxEdgesPerSource);
  workDb.exec(`
    CREATE INDEX idx_edge_pruned_source ON edge_pruned(source_story_id);
    CREATE INDEX idx_edge_pruned_target ON edge_pruned(target_story_id);
  `);

  const edgeCount = Number(workDb.prepare("SELECT COUNT(*) AS c FROM edge_pruned").get()?.c || 0);
  if (edgeCount === 0) {
    workDb.exec(`
      DROP TABLE IF EXISTS component_story;
      DROP TABLE IF EXISTS component_index;
      DROP TABLE IF EXISTS component_members;
      DROP TABLE IF EXISTS component_links;
      CREATE TABLE component_story(story_id INTEGER PRIMARY KEY, component_id INTEGER NOT NULL);
      CREATE TABLE component_index(
        component_id INTEGER PRIMARY KEY,
        root_story_id INTEGER NOT NULL,
        member_count INTEGER NOT NULL,
        edge_count INTEGER NOT NULL,
        rank_score REAL NOT NULL,
        latest_time INTEGER,
        max_story_score INTEGER,
        story_points_sum REAL NOT NULL DEFAULT 0,
        comment_count INTEGER NOT NULL DEFAULT 0,
        years_csv TEXT
      );
      CREATE TABLE component_members(
        component_id INTEGER NOT NULL,
        story_id INTEGER NOT NULL,
        rel_score REAL NOT NULL,
        title TEXT,
        by TEXT,
        time INTEGER,
        score INTEGER,
        PRIMARY KEY(component_id, story_id)
      );
      CREATE TABLE component_links(
        component_id INTEGER NOT NULL,
        source_story_id INTEGER NOT NULL,
        target_story_id INTEGER NOT NULL,
        weight REAL NOT NULL,
        evidence_count INTEGER NOT NULL,
        last_time INTEGER,
        PRIMARY KEY(component_id, source_story_id, target_story_id)
      );
    `);
    markPhaseDone(workDb, doneKey);
    progress.done("Phase 5/6 complete | no pruned edges");
    return;
  }

  const parent = new Map();
  const rank = new Map();
  let scanned = 0;
  const startTs = Date.now();
  const iterEdges = workDb.prepare(`
    SELECT source_story_id, target_story_id
    FROM edge_pruned
  `).iterate();
  for (const row of iterEdges) {
    const a = Number(row.source_story_id);
    const b = Number(row.target_story_id);
    if (!parent.has(a)) {
      parent.set(a, a);
      rank.set(a, 0);
    }
    if (!parent.has(b)) {
      parent.set(b, b);
      rank.set(b, 0);
    }
    ufUnion(parent, rank, a, b);
    scanned += 1;
    if (scanned % 20000 === 0) {
      progress.updateCounts(
        PHASE_NUM,
        PHASE_TOTAL,
        "Building connected components",
        scanned,
        edgeCount,
        `nodes=${fmtNum(parent.size)}`,
        startTs
      );
      await tick();
    }
  }
  progress.updateCounts(
    PHASE_NUM,
    PHASE_TOTAL,
    "Building connected components",
    edgeCount,
    edgeCount,
    `nodes=${fmtNum(parent.size)}`,
    startTs
  );

  workDb.exec(`
    DROP TABLE IF EXISTS component_story;
    CREATE TABLE component_story(
      story_id INTEGER PRIMARY KEY,
      component_id INTEGER NOT NULL
    );
  `);
  const insertComponentStory = workDb.prepare(`
    INSERT INTO component_story(story_id, component_id) VALUES (?, ?)
  `);
  const insertComponentBatch = workDb.transaction((rows) => {
    for (const r of rows) insertComponentStory.run(r.story_id, r.component_id);
  });

  const rows = [];
  for (const storyId of parent.keys()) {
    rows.push({ story_id: storyId, component_id: ufFind(parent, storyId) });
    if (rows.length >= 25000) {
      insertComponentBatch(rows.splice(0, rows.length));
      await tick();
    }
  }
  if (rows.length) insertComponentBatch(rows);
  workDb.exec(`
    CREATE INDEX idx_component_story_component ON component_story(component_id);
  `);

  workDb.exec(`
    DROP TABLE IF EXISTS story_rel;
    CREATE TABLE story_rel AS
    SELECT story_id, SUM(weight) AS rel_score
    FROM (
      SELECT source_story_id AS story_id, weight FROM edge_pruned
      UNION ALL
      SELECT target_story_id AS story_id, weight FROM edge_pruned
    )
    GROUP BY story_id;
    CREATE INDEX idx_story_rel_story ON story_rel(story_id);

    DROP TABLE IF EXISTS component_edges_count;
    CREATE TABLE component_edges_count AS
    SELECT c1.component_id AS component_id, COUNT(*) AS edge_count
    FROM edge_pruned ep
    JOIN component_story c1 ON c1.story_id = ep.source_story_id
    JOIN component_story c2 ON c2.story_id = ep.target_story_id
    WHERE c1.component_id = c2.component_id
    GROUP BY c1.component_id;
    CREATE INDEX idx_component_edges_count_id ON component_edges_count(component_id);

    DROP TABLE IF EXISTS component_roots;
    CREATE TABLE component_roots AS
    SELECT component_id, story_id AS root_story_id
    FROM (
      SELECT
        cs.component_id,
        cs.story_id,
        COALESCE(sp.score, 0) AS score,
        COALESCE(sp.time, 0) AS time,
        ROW_NUMBER() OVER (
          PARTITION BY cs.component_id
          ORDER BY
            date(COALESCE(sp.time,0), 'unixepoch') DESC,
            COALESCE(sp.score,0) DESC,
            COALESCE(sp.time,0) DESC,
            cs.story_id DESC
        ) AS rn
      FROM component_story cs
      LEFT JOIN story_profiles sp ON sp.story_id = cs.story_id
    ) ranked
    WHERE rn = 1;
    CREATE INDEX idx_component_roots_component ON component_roots(component_id);

    DROP TABLE IF EXISTS component_story_points;
    CREATE TABLE component_story_points AS
    SELECT
      cs.component_id,
      SUM(COALESCE(sp.score, 0)) AS story_points_sum
    FROM component_story cs
    LEFT JOIN story_profiles sp ON sp.story_id = cs.story_id
    GROUP BY cs.component_id;
    CREATE INDEX idx_component_story_points_id ON component_story_points(component_id);

    DROP TABLE IF EXISTS component_comment_counts;
    CREATE TABLE component_comment_counts AS
    SELECT
      cs.component_id,
      SUM(COALESCE(sp.comment_total, 0)) AS comment_count
    FROM component_story cs
    LEFT JOIN story_profiles sp ON sp.story_id = cs.story_id
    GROUP BY cs.component_id;
    CREATE INDEX idx_component_comment_counts_id ON component_comment_counts(component_id);

    DROP TABLE IF EXISTS component_years;
    CREATE TABLE component_years AS
    SELECT
      component_id,
      GROUP_CONCAT(y, ',') AS years_csv
    FROM (
      SELECT DISTINCT
        cs.component_id AS component_id,
        CAST(strftime('%Y', COALESCE(sp.time,0), 'unixepoch') AS INTEGER) AS y
      FROM component_story cs
      LEFT JOIN story_profiles sp ON sp.story_id = cs.story_id
      WHERE COALESCE(sp.time, 0) > 0
      ORDER BY cs.component_id, y DESC
    ) yy
    GROUP BY component_id;
    CREATE INDEX idx_component_years_id ON component_years(component_id);

    DROP TABLE IF EXISTS component_index;
    CREATE TABLE component_index AS
    WITH member_stats AS (
      SELECT
        cs.component_id,
        COUNT(*) AS member_count,
        MAX(COALESCE(sp.time,0)) AS latest_time,
        MAX(COALESCE(sp.score,0)) AS max_story_score
      FROM component_story cs
      LEFT JOIN story_profiles sp ON sp.story_id = cs.story_id
      GROUP BY cs.component_id
    ),
    rank_inputs AS (
      SELECT
        ms.component_id,
        cr.root_story_id,
        ms.member_count,
        COALESCE(ce.edge_count, 0) AS edge_count,
        ms.latest_time,
        ms.max_story_score,
        COALESCE(csp.story_points_sum, 0) AS story_points_sum,
        COALESCE(cc.comment_count, 0) AS comment_count,
        (
          COALESCE(ms.max_story_score,0) * 0.70 +
          COALESCE(ms.latest_time,0) * 0.000001 * 0.30 +
          ms.member_count * 2.00 +
          COALESCE(ce.edge_count,0) * 0.50
        ) AS legacy_rank,
        (COALESCE(csp.story_points_sum, 0) + COALESCE(cc.comment_count, 0) * 2.718) AS raw_rank
      FROM member_stats ms
      JOIN component_roots cr ON cr.component_id = ms.component_id
      LEFT JOIN component_edges_count ce ON ce.component_id = ms.component_id
      LEFT JOIN component_story_points csp ON csp.component_id = ms.component_id
      LEFT JOIN component_comment_counts cc ON cc.component_id = ms.component_id
      WHERE ms.member_count >= 2
    )
    SELECT
      ri.component_id,
      ri.root_story_id,
      ri.member_count,
      ri.edge_count,
      ${rankSelectExpr} AS rank_score,
      ri.latest_time,
      ri.max_story_score,
      ri.story_points_sum,
      ri.comment_count,
      cy.years_csv
    FROM rank_inputs ri
    LEFT JOIN component_years cy ON cy.component_id = ri.component_id;
    CREATE INDEX idx_component_index_rank ON component_index(rank_score DESC, latest_time DESC, component_id DESC);

    DROP TABLE IF EXISTS component_members;
    CREATE TABLE component_members AS
    SELECT
      cs.component_id,
      cs.story_id,
      COALESCE(sr.rel_score, 0) AS rel_score,
      sp.title,
      sp.by,
      sp.time,
      sp.score,
      sp.url
    FROM component_story cs
    JOIN component_index ci ON ci.component_id = cs.component_id
    LEFT JOIN story_rel sr ON sr.story_id = cs.story_id
    LEFT JOIN story_profiles sp ON sp.story_id = cs.story_id;
    CREATE INDEX idx_component_members_component ON component_members(component_id);

    DROP TABLE IF EXISTS component_links;
    CREATE TABLE component_links AS
    SELECT
      c1.component_id,
      ep.source_story_id,
      ep.target_story_id,
      ep.weight,
      ep.evidence_count,
      ep.last_time
    FROM edge_pruned ep
    JOIN component_story c1 ON c1.story_id = ep.source_story_id
    JOIN component_story c2 ON c2.story_id = ep.target_story_id
    JOIN component_index ci ON ci.component_id = c1.component_id
    WHERE c1.component_id = c2.component_id;
    CREATE INDEX idx_component_links_component ON component_links(component_id);
  `);

  const compCount = Number(workDb.prepare("SELECT COUNT(*) AS c FROM component_index").get()?.c || 0);
  setMeta(workDb, "rank_model_version", rankModelVersion);
  markPhaseDone(workDb, doneKey);
  progress.done(`Phase 5/6 complete | components=${fmtNum(compCount)} | nodes=${fmtNum(parent.size)} | edges=${fmtNum(edgeCount)}`);
}

function initRelatedShardDb(dbPath) {
  if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);
  const db = new Database(dbPath);
  db.pragma("journal_mode = OFF");
  db.pragma("synchronous = OFF");
  db.pragma("temp_store = MEMORY");
  db.exec(`
    CREATE TABLE components (
      component_id INTEGER PRIMARY KEY,
      root_story_id INTEGER NOT NULL,
      member_count INTEGER NOT NULL,
      edge_count INTEGER NOT NULL,
      rank_score REAL NOT NULL,
      latest_time INTEGER,
      max_story_score INTEGER,
      story_points_sum REAL NOT NULL DEFAULT 0,
      comment_count INTEGER NOT NULL DEFAULT 0,
      years_csv TEXT,
      root_title TEXT,
      root_by TEXT,
      root_time INTEGER,
      root_score INTEGER,
      root_url TEXT
    );

    CREATE TABLE component_members (
      component_id INTEGER NOT NULL,
      story_id INTEGER NOT NULL,
      rel_score REAL NOT NULL,
      title TEXT,
      by TEXT,
      time INTEGER,
      score INTEGER,
      url TEXT,
      PRIMARY KEY(component_id, story_id)
    );

    CREATE TABLE component_links (
      component_id INTEGER NOT NULL,
      source_story_id INTEGER NOT NULL,
      target_story_id INTEGER NOT NULL,
      weight REAL NOT NULL,
      evidence_count INTEGER NOT NULL,
      last_time INTEGER,
      PRIMARY KEY(component_id, source_story_id, target_story_id)
    );
  `);
  return db;
}

async function gzipAndHashFile(srcPath, outDir, sid, prefix = "related") {
  const tmpGz = `${srcPath}.gz.tmp`;
  await pipeline(
    fs.createReadStream(srcPath),
    zlib.createGzip({ level: 9 }),
    fs.createWriteStream(tmpGz)
  );

  await pipeline(
    fs.createReadStream(tmpGz),
    zlib.createGunzip(),
    new Writable({
      write(_chunk, _enc, cb) {
        cb();
      }
    })
  );

  const hash = await new Promise((resolve, reject) => {
    const h = crypto.createHash("sha256");
    const s = fs.createReadStream(tmpGz);
    s.on("error", reject);
    s.on("data", (c) => h.update(c));
    s.on("end", () => resolve(h.digest("hex").slice(0, 12)));
  });

  const finalName = `${prefix}_${sid}_${hash}.sqlite.gz`;
  const finalPath = path.join(outDir, finalName);
  if (fs.existsSync(finalPath)) fs.unlinkSync(finalPath);
  fs.renameSync(tmpGz, finalPath);
  return { finalPath, finalName };
}

async function writeJsonMaybeGzip(jsonPath, obj, gzipOut) {
  ensureDirSync(path.dirname(jsonPath));
  const raw = JSON.stringify(obj, null, 2);
  await fsp.writeFile(jsonPath, raw);
  if (!gzipOut) return;
  const gzPath = `${jsonPath}.gz`;
  await pipeline(
    fs.createReadStream(jsonPath),
    zlib.createGzip({ level: 9 }),
    fs.createWriteStream(gzPath)
  );
}

async function phaseEmitArtifacts(ctx) {
  const { workDb, args, progress, rankModelVersion } = ctx;
  const PHASE_NUM = 6;
  const PHASE_TOTAL = 6;
  const doneKey = "phase_emit_done";
  if (getMetaBool(workDb, doneKey) && !args.forceEmit) {
    progress.done("Phase 6/6: emit already complete (use --force-emit to rebuild)");
    return;
  }

  ensureDirSync(args.outDir);
  ensureDirSync(path.dirname(args.outManifest));
  ensureDirSync(path.dirname(args.topIndex));

  // Clear prior emitted files only (work DB is separate and preserved).
  for (const f of fs.readdirSync(args.outDir)) {
    if (/^related_\d+.*\.sqlite(\.gz)?$/.test(f)) {
      fs.unlinkSync(path.join(args.outDir, f));
    }
  }

  const totalComponents = Number(workDb.prepare("SELECT COUNT(*) AS c FROM component_index").get()?.c || 0);
  const totalMembers = Number(workDb.prepare("SELECT COUNT(*) AS c FROM component_members").get()?.c || 0);
  const totalLinks = Number(workDb.prepare("SELECT COUNT(*) AS c FROM component_links").get()?.c || 0);
  const snapshot = Number(workDb.prepare("SELECT MAX(latest_time) AS t FROM component_index").get()?.t || 0);

  const manifest = {
    version: 1,
    created_at: nowIso(),
    source_staging: path.relative(process.cwd(), path.resolve(args.fromStaging)),
    snapshot_time: snapshot || null,
    config: {
      rank_model_version: rankModelVersion,
      min_edge_weight: args.minEdgeWeight,
      max_edges_per_source: args.maxEdgesPerSource,
      max_links_per_item: args.maxLinksPerItem,
      link_dump_penalty_cap: args.linkDumpPenaltyCap,
      target_mb: args.targetMb,
      components_per_shard: args.componentsPerShard
    },
    totals: {
      components: totalComponents,
      members: totalMembers,
      links: totalLinks
    },
    shards: []
  };

  const topRows = [];
  if (totalComponents === 0) {
    await writeJsonMaybeGzip(args.outManifest, manifest, true);
    await writeJsonMaybeGzip(args.topIndex, { version: 1, generated_at: nowIso(), rows: [] }, true);
    markPhaseDone(workDb, doneKey);
    progress.done("Phase 6/6 complete | no components to emit");
    return;
  }

  const selectComponents = workDb.prepare(`
    SELECT
      ci.component_id,
      ci.root_story_id,
      ci.member_count,
      ci.edge_count,
      ci.rank_score,
      ci.latest_time,
      ci.max_story_score,
      ci.story_points_sum,
      ci.comment_count,
      ci.years_csv,
      sp.title AS root_title,
      sp.by AS root_by,
      sp.time AS root_time,
      sp.score AS root_score,
      sp.url AS root_url
    FROM component_index ci
    LEFT JOIN story_profiles sp ON sp.story_id = ci.root_story_id
    ORDER BY ci.rank_score DESC, ci.latest_time DESC, ci.component_id DESC
  `);
  const selectMembers = workDb.prepare(`
    SELECT story_id, rel_score, title, by, time, score, url
    FROM component_members
    WHERE component_id=?
    ORDER BY rel_score DESC, COALESCE(score,0) DESC, COALESCE(time,0) DESC, story_id DESC
  `);
  const selectLinks = workDb.prepare(`
    SELECT source_story_id, target_story_id, weight, evidence_count, last_time
    FROM component_links
    WHERE component_id=?
    ORDER BY weight DESC, evidence_count DESC, COALESCE(last_time,0) DESC
  `);

  let shardSid = 0;
  let shardDb = null;
  let shardPath = null;
  let shardCompCount = 0;
  let shardMemberCount = 0;
  let shardLinkCount = 0;
  let shardRankLo = null;
  let shardRankHi = null;
  const targetBytes = Math.max(1, Number(args.targetMb || 15)) * 1024 * 1024;

  let insertComponent = null;
  let insertMember = null;
  let insertLink = null;
  let insertTx = null;

  const openShard = () => {
    shardPath = path.join(args.outDir, `related_${shardSid}.sqlite`);
    shardDb = initRelatedShardDb(shardPath);
    insertComponent = shardDb.prepare(`
      INSERT INTO components(
        component_id, root_story_id, member_count, edge_count, rank_score, latest_time, max_story_score, story_points_sum, comment_count,
        years_csv, root_title, root_by, root_time, root_score, root_url
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    insertMember = shardDb.prepare(`
      INSERT INTO component_members(component_id, story_id, rel_score, title, by, time, score, url)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);
    insertLink = shardDb.prepare(`
      INSERT INTO component_links(component_id, source_story_id, target_story_id, weight, evidence_count, last_time)
      VALUES (?, ?, ?, ?, ?, ?)
    `);
    insertTx = shardDb.transaction((payload) => {
      insertComponent.run(
        payload.component.component_id,
        payload.component.root_story_id,
        payload.component.member_count,
        payload.component.edge_count,
        payload.component.rank_score,
        payload.component.latest_time,
        payload.component.max_story_score,
        payload.component.story_points_sum,
        payload.component.comment_count,
        payload.component.years_csv,
        payload.component.root_title,
        payload.component.root_by,
        payload.component.root_time,
        payload.component.root_score,
        payload.component.root_url
      );
      for (const m of payload.members) {
        insertMember.run(payload.component.component_id, m.story_id, m.rel_score, m.title, m.by, m.time, m.score, m.url);
      }
      for (const l of payload.links) {
        insertLink.run(payload.component.component_id, l.source_story_id, l.target_story_id, l.weight, l.evidence_count, l.last_time);
      }
    });

    shardCompCount = 0;
    shardMemberCount = 0;
    shardLinkCount = 0;
    shardRankLo = null;
    shardRankHi = null;
  };

  const closeShard = () => {
    if (!shardDb) return null;
    shardDb.exec(`
      CREATE INDEX IF NOT EXISTS idx_components_rank ON components(rank_score DESC, latest_time DESC, component_id DESC);
      CREATE INDEX IF NOT EXISTS idx_members_component ON component_members(component_id);
      CREATE INDEX IF NOT EXISTS idx_links_component ON component_links(component_id);
      ANALYZE;
    `);
    shardDb.close();
    const bytes = fs.statSync(shardPath).size;
    const rec = {
      sid: shardSid,
      rank_lo: shardRankLo,
      rank_hi: shardRankHi,
      component_count: shardCompCount,
      member_count: shardMemberCount,
      link_count: shardLinkCount,
      file: path.basename(shardPath),
      bytes,
      _sqlite_path: shardPath
    };
    shardSid += 1;
    shardDb = null;
    shardPath = null;
    return rec;
  };

  progress.phase(PHASE_NUM, PHASE_TOTAL, "Emitting related shards + manifest");
  openShard();

  let emittedComponents = 0;
  const startTs = Date.now();
  for (const comp of selectComponents.iterate()) {
    emittedComponents += 1;
    const members = selectMembers.all(comp.component_id);
    const links = selectLinks.all(comp.component_id);
    insertTx({
      component: comp,
      members,
      links
    });

    shardCompCount += 1;
    shardMemberCount += members.length;
    shardLinkCount += links.length;
    if (shardRankLo == null) shardRankLo = emittedComponents;
    shardRankHi = emittedComponents;

    topRows.push({
      rank: emittedComponents,
      component_id: Number(comp.component_id),
      root_story_id: Number(comp.root_story_id),
      title: comp.root_title || null,
      by: comp.root_by || null,
      time: comp.root_time == null ? null : Number(comp.root_time),
      score: comp.root_score == null ? null : Number(comp.root_score),
      root_url: comp.root_url || null,
      years_csv: comp.years_csv || null,
      member_count: Number(comp.member_count || 0),
      edge_count: Number(comp.edge_count || 0),
      rank_score: Number(comp.rank_score || 0),
      story_points_sum: Number(comp.story_points_sum || 0),
      comment_count: Number(comp.comment_count || 0)
    });

    if (emittedComponents % 100 === 0) {
      const currentBytes = fs.statSync(shardPath).size;
      const shouldRotate =
        shardCompCount >= args.componentsPerShard ||
        (currentBytes >= targetBytes && shardCompCount >= 250);
      if (shouldRotate) {
        const rec = closeShard();
        if (rec) manifest.shards.push(rec);
        openShard();
      }
    }

    if (emittedComponents % 200 === 0 || emittedComponents === totalComponents) {
      progress.updateCounts(
        PHASE_NUM,
        PHASE_TOTAL,
        "Emitting related shards + manifest",
        emittedComponents,
        totalComponents,
        `shards=${fmtNum(manifest.shards.length + (shardDb ? 1 : 0))}`,
        startTs
      );
      await tick();
    }
  }
  const lastRec = closeShard();
  if (lastRec && lastRec.component_count > 0) manifest.shards.push(lastRec);

  // Gzip/hash shards if requested.
  if (args.gzip) {
    for (const shard of manifest.shards) {
      const sqlitePath = shard._sqlite_path;
      const out = await gzipAndHashFile(sqlitePath, args.outDir, shard.sid, "related");
      shard.file = out.finalName;
      shard.bytes = fs.statSync(out.finalPath).size;
      if (!args.keepSqlite && fs.existsSync(sqlitePath)) fs.unlinkSync(sqlitePath);
    }
  } else {
    for (const shard of manifest.shards) {
      const sqlitePath = shard._sqlite_path;
      shard.file = path.basename(sqlitePath);
      shard.bytes = fs.statSync(sqlitePath).size;
    }
  }
  for (const shard of manifest.shards) delete shard._sqlite_path;

  await writeJsonMaybeGzip(args.outManifest, manifest, true);
  await writeJsonMaybeGzip(args.topIndex, {
    version: 1,
    generated_at: nowIso(),
    rank_model_version: rankModelVersion,
    totals: {
      components: totalComponents
    },
    rows: topRows
  }, true);

  markPhaseDone(workDb, doneKey);
  setMeta(workDb, "last_emit_at", nowIso());
  progress.done(`Phase 6/6 complete | components=${fmtNum(totalComponents)} | shards=${fmtNum(manifest.shards.length)}`);
}

function assertFinitePositive(name, value) {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`Invalid ${name}: ${value}`);
  }
}

function assertFiniteNonNegative(name, value) {
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid ${name}: ${value}`);
  }
}

async function main() {
  const args = parseArgs(process.argv);
  assertFinitePositive("batch", args.batch);
  assertFinitePositive("resolve-batch", args.resolveBatch);
  assertFinitePositive("score-batch", args.scoreBatch);
  assertFinitePositive("max-depth", args.maxDepth);
  assertFinitePositive("target-mb", args.targetMb);
  assertFinitePositive("components-per-shard", args.componentsPerShard);
  assertFiniteNonNegative("max-links-per-item", args.maxLinksPerItem);
  assertFiniteNonNegative("link-dump-penalty-cap", args.linkDumpPenaltyCap);

  const rankModel = String(args.rankModel || "points").trim().toLowerCase();
  if (!Object.prototype.hasOwnProperty.call(RANK_MODELS, rankModel)) {
    throw new Error(`Invalid --rank-model '${args.rankModel}'. Valid: ${Object.keys(RANK_MODELS).join("|")}`);
  }
  const rankModelVersion = RANK_MODELS[rankModel];

  const fromStaging = path.resolve(args.fromStaging);
  const dataDir = path.resolve(args.dataDir);
  const outDir = path.resolve(args.outDir);
  const outManifest = path.resolve(args.outManifest);
  const topIndex = path.resolve(args.topIndex);
  const workDbPath = path.resolve(args.workDb);

  if (args.reset) {
    removeIfExistsSync(workDbPath);
    removeIfExistsSync(outDir);
    removeIfExistsSync(outManifest);
    removeIfExistsSync(`${outManifest}.gz`);
    removeIfExistsSync(topIndex);
    removeIfExistsSync(`${topIndex}.gz`);
  }

  ensureDirSync(path.dirname(workDbPath));
  ensureDirSync(outDir);

  const progress = new PrettyProgress();
  progress.start();

  let stagingDb = null;
  let workDb = null;
  try {
    progress.phase(0, 6, "Opening work DB");
    workDb = openDb(workDbPath, false);
    initWorkDb(workDb);

    const existingRankModel = getMeta(workDb, "rank_model_version", "");
    if (getMetaBool(workDb, "phase_components_done") && existingRankModel !== rankModelVersion) {
      setMeta(workDb, "phase_components_done", "0");
      setMeta(workDb, "phase_emit_done", "0");
      const fromLabel = existingRankModel || "unversioned";
      progress.done(`Rank model changed (${fromLabel} -> ${rankModelVersion}); invalidated phases 5/6`);
    }

    const existingSchemaVersion = getMeta(workDb, "related_work_schema_version", "");
    if (getMetaBool(workDb, "phase_components_done") && existingSchemaVersion !== RELATED_WORK_SCHEMA_VERSION) {
      setMeta(workDb, "phase_story_profiles_done", "0");
      setMeta(workDb, "phase_components_done", "0");
      setMeta(workDb, "phase_emit_done", "0");
      const fromSchema = existingSchemaVersion || "unversioned";
      progress.done(`Related schema changed (${fromSchema} -> ${RELATED_WORK_SCHEMA_VERSION}); invalidated phases 3/5/6`);
    }
    const staleCommentTotals = Number(
      workDb.prepare("SELECT COUNT(*) AS c FROM story_profiles WHERE comment_total < 0").get()?.c || 0
    );
    if (staleCommentTotals > 0 && getMetaBool(workDb, "phase_components_done")) {
      setMeta(workDb, "phase_components_done", "0");
      setMeta(workDb, "phase_emit_done", "0");
      progress.done(`Detected ${fmtNum(staleCommentTotals)} stories with missing comment totals; invalidated phases 5/6`);
    }
    setMeta(workDb, "related_work_schema_version", RELATED_WORK_SCHEMA_VERSION);

    if (args.emitOnly && !getMetaBool(workDb, "phase_components_done")) {
      throw new Error("--emit-only requires completed compute phases in work DB (missing phase_components_done)");
    }

    if (!args.emitOnly) {
      if (!fs.existsSync(fromStaging)) {
        await buildStagingFromRaw({ stagingPath: fromStaging, dataDir, progress });
      }
      progress.phase(0, 6, "Opening staging DB");
      stagingDb = openDb(fromStaging, false);
    } else {
      progress.done("Phase 0/6 complete | emit-only mode (staging DB not required)");
    }

    if (stagingDb) {
      const hasItemsRaw = stagingDb.prepare(`
        SELECT COUNT(*) AS c
        FROM sqlite_master
        WHERE type='table' AND name='items_raw'
      `).get();
      if (!Number(hasItemsRaw?.c)) {
        throw new Error(`Table items_raw not found in ${fromStaging}`);
      }
      const cols = tableColumns(stagingDb, "items_raw");
      if (!cols.has("descendants")) {
        try {
          stagingDb.close();
        } catch {}
        stagingDb = null;
        progress.done("Staging DB missing descendants; rebuilding from raw exports");
        await buildStagingFromRaw({ stagingPath: fromStaging, dataDir, progress });
        progress.phase(0, 6, "Re-opening staging DB");
        stagingDb = openDb(fromStaging, false);
      }
    }

    const ctx = {
      args: {
        ...args,
        rankModel,
        fromStaging,
        dataDir,
        outDir,
        outManifest,
        topIndex
      },
      rankModel,
      rankModelVersion,
      stagingDb,
      workDb,
      progress
    };

    if (!args.emitOnly) {
      await phaseExtractRawLinks(ctx);
      await phaseResolveStoryIds(ctx);
      await phaseBuildStoryProfiles(ctx);
      await backfillStoryProfileUrls(ctx);
      await backfillStoryCommentTotals(ctx);
      await phaseScoreEdges(ctx);
      await phaseBuildComponents(ctx);
    } else {
      progress.done("Compute phases skipped (--emit-only)");
    }
    await phaseEmitArtifacts(ctx);

    progress.done(`Build complete | work DB: ${path.relative(process.cwd(), workDbPath)}`);
  } finally {
    try {
      if (stagingDb) stagingDb.close();
    } catch {}
    try {
      if (workDb) workDb.close();
    } catch {}
    progress.stop();
  }
}

main().catch((err) => {
  process.stderr.write(`\n[related] failed: ${err && err.message ? err.message : String(err)}\n`);
  process.exit(1);
});
