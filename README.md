# Hacker Book — Community, All the HN Belong to You! 2006 - 2025 FOREVER

Static, offline-friendly Hacker News archive shipped as plain files. Everything runs client-side in your browser via SQLite WASM; the browser only downloads the shards it needs.

- Demo: https://hackerbook.dosaygo.com
- Landing / download: https://dosaygo-studio.github.io/HackerBook/
- Code: https://github.com/DOSAYGO-STUDIO/HackerBook

## How to get the site
Always run `npm install` once in the repo root.

**Option A: Build everything yourself (one-touch)**
1) `./toool/s/predeploy-checks.sh [--use-staging] [--restart-etl] [--from-shards] [--hash-only]`
   - Downloads raw data (if missing), runs ETL (`etl-hn.js`), gzips shards, regenerates manifests/indexes, and rebuilds user stats.
   - Flags:
     - `--use-staging` use `data/static-staging-hn.sqlite` instead of raw downloads.
     - `--restart-etl` resume post-pass/gzip from existing shards.
     - `AUTO_RUN=1` to auto-advance the prompts.
2) Serve `docs/` locally (see below).

**Option B: Grab the published site (no ETL)**
1) Download over HTTPS:
   - `node toool/download-site.mjs` (defaults to https://hackerbook.dosaygo.com → `./downloaded-site`)
   - `SKIP_SHARDS=1 node toool/download-site.mjs` (core assets/manifests only)
   - Flags: `--base`, `--out`, `--no-shards` or env `BASE_URL`, `OUT_DIR`, `SKIP_SHARDS=1`.
2) Serve the downloaded folder (or use the demo directly).

## Browse locally
- `npx serve docs` **or** `python3 -m http.server 8000 --directory docs`
- Open the reported URL; time-warp with the date picker. Everything queries locally in the browser.

## What’s inside
- `docs/static-shards/`: gzipped SQLite shards of HN items and comments.
- `docs/static-user-stats-shards/`: gzipped SQLite shards with per-user stats and monthly activity.
- `docs/static-manifest.json.gz`, `docs/archive-index.json.gz`, `docs/cross-shard-index.bin.gz`: indexes the app fetches and gunzips on load.
- All assets are static; no backend required.

## Tools and scripts
- `./toool/s/predeploy-checks.sh` — orchestration for raw downloads, ETL, gzip, manifests, archive index, and user stats. `--from-shards` normalizes shard hashes and rebuilds from existing shards; `--hash-only` skips the ETL post-pass. Use `AUTO_RUN=1` for unattended runs.
- `node toool/s/build-user-stats.mjs --gzip --target-mb 15` — rebuild only the user stats shards when item shards already exist.
- `node toool/download-site.mjs --help` — usage for pulling the deployed site.
- ETL entry points: `etl-hn.js` / `etl-hn.sh` (expect BigQuery exports in `data/raw/` or `toool/data/raw/`).

## Notes
- Works best on modern browsers (Chrome, Firefox, Safari) with `DecompressionStream`; falls back to pako gzip when needed.
- Mobile: layout is locked to the viewport, and everything runs offline once the needed shards are cached.
- The code for the viewer and ETL pipeline is released under the MIT License.
- The content (Hacker News data) is property of Y Combinator and the respective comment authors.
