# Hacker Book — Archivist/Developer Handbook

Static, offline-friendly Hacker News archive shipped as plain files. Everything runs client-side in your browser via SQLite WASM; the browser only downloads the shards it needs.

- Demo: https://hackerbook.dosaygo.com
- Landing / download: https://dosaygo-studio.github.io/HackerBook/
- Code: https://github.com/DOSAYGO-STUDIO/HackerBook

## Quick start
Always run `npm install` once in the repo root.

**Build + prep everything**
1) `./toool/s/predeploy-checks.sh [--use-staging] [--restart-etl] [--from-shards] [--hash-only]`
2) Serve `docs/` locally: `npx serve docs` or `python3 -m http.server 8000 --directory docs`

**Download the published site (no ETL)**
1) `node toool/download-site.mjs [--base URL] [--out DIR] [--no-shards]`
2) Serve the downloaded folder.

## Concepts
**Shards**
- `docs/static-shards/`: gzipped SQLite shards of items/comments.
- Filenames are hashed: `shard_<sid>_<hash>.sqlite.gz` where `<hash>` is 12 hex chars (SHA-256 truncated).

**Manifests & indexes**
- `docs/static-manifest.json(.gz)`: shard metadata + snapshot time.
- `docs/archive-index.json(.gz)`: per-shard stats + effective time range.
- `docs/cross-shard-index.bin(.gz)`: parent->shard cross index.
- `docs/static-user-stats-manifest.json(.gz)`: user stats shards.

**Cache behavior**
- HTML uses a cache-bust string for manifest/index URLs.
- Shards are immutable by filename; if data changes, the hash changes so clients fetch the new shard.
- Content hashing guarantees cache correctness: the gz shard filename embeds a SHA-256 hash (12 hex chars), so any byte-level change produces a new URL.

## Pipeline overview
1) **Raw data**: BigQuery exports in `data/raw/*.json.gz` (or `toool/data/raw/`).
2) **Staging (optional)**: imports into `data/static-staging-hn.sqlite`.
3) **Shard build**: write `.sqlite` shards in `docs/static-shards/`.
4) **Post-pass**: VACUUM, gzip, hash rename, manifest rewrite.
5) **Indexes**: archive, cross-shard, user stats.
6) **Deploy**: publish `docs/`.

## Orchestration: predeploy
`./toool/s/predeploy-checks.sh` runs the full pipeline with prompts.

Flags:
- `--use-staging` use `data/static-staging-hn.sqlite` instead of raw downloads.
- `--restart-etl` resume post-pass/gzip from existing shards and/or `.prepass`.
- `--from-shards` skip ETL; normalize shard hashes and rebuild from existing shards.
- `--hash-only` with `--from-shards`, only normalize shard hashes (skip post-pass).
- `AUTO_RUN=1` auto-advance prompts.

What it does:
- Downloads raw data if missing.
- Runs ETL (or restarts post-pass).
- Rebuilds indexes and gzips manifests.
- Validates gzip assets and shard presence.

## ETL: `etl-hn.js`
Primary flags:
- `--gzip` enable post-pass gzip + manifest rewrite.
- `--restart` resume post-pass from existing shards/manifest.
- `--rebuild-manifest` rebuild manifest from shards on disk.
- `--from-staging` use `data/static-staging-hn.sqlite`.
- `--data PATH` raw file directory.
- `--post-concurrency N` post-pass concurrency.
- `--keep-sqlite` keep `.sqlite` after gzip.

Restart behavior:
- Uses `docs/static-manifest.json.prepass` if present.
- Validates a tail slice of `.sqlite` + `.gz` shards before resuming.
- Skips already-gzipped shards; only continues missing ones.

Hashing behavior:
- Hashes are computed on the final `.sqlite.gz`.
- Un-hashed `.sqlite.gz` are normalized to `shard_<sid>_<hash>.sqlite.gz`.

## Index builders
- Archive index: `node ./build-archive-index.js`
- Cross-shard index: `node ./toool/s/build-cross-shard-index.mjs --binary`
- User stats: `node ./toool/s/build-user-stats.mjs --gzip --target-mb 15`

## Safe stop / resume
- Kill only after a shard completes (`[shard N] ... file ...MB`).
- If killed mid-gzip, restart with:
  `./toool/s/predeploy-checks.sh --restart-etl`
- Temporary `*.gz.tmp` can be deleted if needed.

## Deploy checklist
- `static-manifest.json(.gz)` updated and points to hashed shards.
- `archive-index.json(.gz)` updated.
- `cross-shard-index.bin(.gz)` updated.
- `static-user-stats-manifest.json(.gz)` updated.
- Cache-bust string bumped in `docs/index.html` and `docs/static.html`.

## Contribute analysis
Have charts or analyses you want to feature? Email a link and a short caption to `hey@browserbox.io`.

## Notes
- Works best on modern browsers (Chrome, Firefox, Safari) with `DecompressionStream`; falls back to pako gzip when needed.
- Mobile: layout is locked to the viewport, and everything runs offline once the needed shards are cached.
- The code for the viewer and ETL pipeline is released under the MIT License.
- The content (Hacker News data) is property of Y Combinator and the respective comment authors.
