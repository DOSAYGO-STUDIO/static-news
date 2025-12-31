#!/usr/bin/env bash
if [[ -z "${BASH_VERSION:-}" ]]; then
  exec /usr/bin/env bash "${0}" "$@"
fi
set -euo pipefail
shopt -s nullglob

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DOCS_DIR="${REPO_DIR}/docs"
RAW_DIR_PRIMARY="${REPO_DIR}/data/raw"
RAW_DIR_ALT="${REPO_DIR}/toool/data/raw"
USE_STAGING=0
RESTART_ETL=0
FROM_SHARDS=0
HASH_ONLY=0

CUSTOM_DOMAIN="${CUSTOM_DOMAIN:-hnbackuptape.dosaygo.com}"
EXPECTED_CNAME="${EXPECTED_CNAME:-static-news-dtg.pages.dev}"
PAGES_PROJECT_NAME="${PAGES_PROJECT_NAME:-static-news}"
BACKUP_STAMP="$(date -u +%Y-%m-%dT%H-%M-%SZ)"

for arg in "$@"; do
  case "${arg}" in
    --use-staging) USE_STAGING=1 ;;
    --restart-etl) RESTART_ETL=1 ;;
    --from-shards) FROM_SHARDS=1 ;;
    --hash-only) HASH_ONLY=1 ;;
    -h|--help)
      cat <<'EOF'
Usage: toool/s/predeploy-checks.sh [--use-staging] [--restart-etl] [--from-shards] [--hash-only]
  --use-staging  Run ETL from ./data/static-staging-hn.sqlite and skip raw download
  --restart-etl  Resume ETL post-pass (vacuum/gzip) from existing shards/manifest
  --from-shards  Skip ETL; normalize shard filenames and rebuild from existing shards
  --hash-only    With --from-shards, only normalize shard hashes (skip ETL post-pass)
EOF
      exit 0
      ;;
    *) echo "Unknown arg: ${arg}"; exit 1 ;;
  esac
done

log() { printf "%s\n" "$*"; }
pass() { log "✅ $*"; }
fail() { log "❌ $*"; exit 1; }
step() { printf "\n🔎 %s\n" "$*"; }
warn() { log "⚠️  $*"; }

pause() {
  printf "\n➡️  %s" "${1}"
  if [[ -n "${AUTO_RUN:-}" ]]; then
    printf " [auto]\n"
    return 0
  fi
  read -r
}

confirm_step() {
  local msg="${1}"; shift
  printf "\n➡️  %s [Enter=run, s=skip]: " "${msg}"
  local reply=""
  if [[ -n "${AUTO_RUN:-}" ]]; then
    printf " [auto]\n"
  else
    read -r reply || true
  fi
  if [[ "${reply:-}" == "s" || "${reply:-}" == "S" ]]; then
    warn "Skipped: ${msg}"
    return 0
  fi
  "$@"
}

in_repo() {
  (cd "${REPO_DIR}" && "$@")
}

require_cmd() {
  local cmd="${1}"
  command -v "${cmd}" >/dev/null 2>&1 || fail "Missing required command: ${cmd}"
}

ensure_gcloud() {
  if command -v gcloud >/dev/null 2>&1; then
    return 0
  fi
  warn "gcloud not found"
  if command -v brew >/dev/null 2>&1; then
    confirm_step "Install gcloud via brew? (google-cloud-sdk)" brew install --cask google-cloud-sdk
  else
    warn "Homebrew not found. Install gcloud from https://cloud.google.com/sdk/docs/install"
    return 1
  fi
  command -v gcloud >/dev/null 2>&1 || return 1
}

ensure_gcloud_auth() {
  if ! command -v gcloud >/dev/null 2>&1; then
    return 1
  fi
  local acct=""
  acct="$(gcloud auth list --filter=status:ACTIVE --format='value(account)' 2>/dev/null || true)"
  if [[ -n "${acct}" ]]; then
    pass "gcloud auth OK (${acct})"
    return 0
  fi
  warn "gcloud not authenticated"
  confirm_step "Run 'gcloud auth login' now?" gcloud auth login
  acct="$(gcloud auth list --filter=status:ACTIVE --format='value(account)' 2>/dev/null || true)"
  [[ -n "${acct}" ]] || fail "gcloud auth still missing"
  pass "gcloud auth OK (${acct})"
}

require_file() {
  local path="${1}"
  [[ -f "${path}" ]] || fail "Missing file: ${path}"
}

file_mtime() {
  local path="${1}"
  if stat -f %m "${path}" >/dev/null 2>&1; then
    stat -f %m "${path}"
  else
    stat -c %Y "${path}"
  fi
}

latest_mtime_glob() {
  local pattern="${1}"
  local newest=0
  local f
  for f in ${pattern}; do
    [[ -e "${f}" ]] || continue
    local m
    m="$(file_mtime "${f}")"
    if [[ "${m}" -gt "${newest}" ]]; then
      newest="${m}"
    fi
  done
  printf "%s" "${newest}"
}

assert_fresh() {
  local target="${1}"
  local dep_time="${2}"
  if [[ ! -f "${target}" ]]; then
    fail "Missing expected file: ${target}"
  fi
  local target_time
  target_time="$(file_mtime "${target}")"
  if [[ "${target_time}" -lt "${dep_time}" ]]; then
    fail "Stale file detected: ${target} (older than upstream inputs)"
  fi
}

gzip_test() {
  local path="${1}"
  gzip -t "${path}" >/dev/null 2>&1 || fail "gzip failed: ${path}"
}

gzip_replace() {
  local src="${1}"
  local dst="${2}"
  local tmp="${dst}.tmp"
  ensure_writable_or_backup "${dst}"
  gzip -9 -c "${src}" > "${tmp}"
  if ! gzip -t "${tmp}" >/dev/null 2>&1; then
    rm -f "${tmp}"
    fail "gzip failed: ${dst}"
  fi
  mv "${tmp}" "${dst}"
  if ! rm -f "${src}" 2>/dev/null; then
    ensure_writable_or_backup "${src}" || true
  fi
}

lock_file() {
  local path="${1}"
  chmod 444 "${path}" || fail "Failed to lock file: ${path}"
}

ensure_writable_or_backup() {
  local path="${1}"
  [[ -e "${path}" ]] || return 0
  if [[ -w "${path}" ]]; then
    return 0
  fi
  local dir
  dir="$(dirname "${path}")"
  local backup_dir="${dir}/backups-${BACKUP_STAMP}"
  mkdir -p "${backup_dir}"
  local dest="${backup_dir}/$(basename "${path}")"
  mv "${path}" "${dest}"
  log "[post] moved protected file to ${dest}"
}

hash_file_12() {
  local path="${1}"
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "${path}" | awk '{print substr($1,1,12)}'
    return
  fi
  if command -v openssl >/dev/null 2>&1; then
    openssl dgst -sha256 "${path}" | awk '{print substr($NF,1,12)}'
    return
  fi
  fail "Missing hash tool: need shasum or openssl"
}

normalize_shard_hashes() {
  local dir="${1}"
  local backup_dir="${dir}/backups-${BACKUP_STAMP}"
  mkdir -p "${backup_dir}"
  local updated=0
  for f in "${dir}"/shard_*.sqlite.gz; do
    [[ -f "${f}" ]] || continue
    local base
    base="$(basename "${f}")"
    if [[ "${base}" =~ ^shard_[0-9]+_[0-9a-f]{12}\.sqlite\.gz$ ]]; then
      continue
    fi
    local sid
    sid="$(printf "%s" "${base}" | sed -E 's/^shard_([0-9]+)\.sqlite\.gz$/\1/')"
    if [[ -z "${sid}" || "${sid}" == "${base}" ]]; then
      continue
    fi
    local hash
    hash="$(hash_file_12 "${f}")"
    local target="${dir}/shard_${sid}_${hash}.sqlite.gz"
    if [[ -e "${target}" ]]; then
      mv "${f}" "${backup_dir}/"
    else
      mv "${f}" "${target}"
    fi
    updated=$((updated+1))
  done
  if [[ "${updated}" -gt 0 ]]; then
    pass "Normalized ${updated} shard filenames with hashes"
  else
    pass "Shard filenames already hashed"
  fi
}

count_glob() {
  local pattern="${1}"
  local -a arr=()
  # shellcheck disable=SC2206
  arr=(${pattern})
  printf "%s" "${#arr[@]}"
}

validate_manifest_shards() {
  local manifest_path="${1}"
  local shards_dir="${2}"
  local label="${3}"

  local shard_files
  shard_files="$(MANIFEST_PATH="${manifest_path}" node - <<'NODE'
const fs = require('fs');
const zlib = require('zlib');
const p = process.env.MANIFEST_PATH || process.argv[1];
if (!p) throw new Error('Missing manifest path');
let raw = fs.readFileSync(p);
if (p.endsWith('.gz')) raw = zlib.gunzipSync(raw);
const m = JSON.parse(raw.toString('utf8'));
const shards = Array.isArray(m.shards) ? m.shards : [];
for (const s of shards) {
  if (s && s.file) process.stdout.write(String(s.file) + '\n');
}
NODE
)"

  local -a files=()
  while IFS= read -r line; do
    [[ -n "${line}" ]] && files+=("${line}")
  done <<<"${shard_files}"

  [[ "${#files[@]}" -gt 0 ]] || fail "${label} manifest has no shards: ${manifest_path}"

  step "Validating ${label} shards from manifest ($(printf "%s" "${#files[@]}") files)"
  local i=0
  for f in "${files[@]}"; do
    i=$((i+1))
    local full="${shards_dir}/${f}"
    printf "\r🧪 %s %d/%d" "${label}" "${i}" "${#files[@]}"
    [[ -f "${full}" ]] || fail "Missing ${label} shard file: ${full}"
    gzip_test "${full}"
  done
  printf "\r"
  pass "${label} shards OK (${#files[@]})"
}

check_cname() {
  local domain="${1}"
  local expected="${2}"
  local cname=""
  if command -v dig >/dev/null 2>&1; then
    cname="$(dig +short CNAME "${domain}" | head -n 1 | tr -d '\r' | sed 's/\.$//')"
  elif command -v nslookup >/dev/null 2>&1; then
    cname="$(nslookup -type=CNAME "${domain}" 2>/dev/null | awk '/canonical name/ {print $NF}' | head -n 1 | tr -d '\r' | sed 's/\.$//')"
  else
    warn "No dig/nslookup available; skipping CNAME check"
    return 0
  fi

  if [[ -z "${cname}" ]]; then
    fail "CNAME not found for ${domain}"
  fi
  if [[ "${cname}" != "${expected}" ]]; then
    fail "CNAME mismatch for ${domain}: expected ${expected}, got ${cname}"
  fi
  pass "CNAME OK: ${domain} → ${cname}"
}

log "🧭 HN Backup Tape pre‑deploy checklist"
log "---------------------------------"

step "Checking prerequisites"
require_cmd bash
require_cmd gzip
require_cmd node
require_cmd rg
if ! command -v wrangler >/dev/null 2>&1; then
  warn "wrangler not found (deploy step can be skipped)"
fi
pass "Core commands available"

step "Checking raw data"
mkdir -p "${RAW_DIR_PRIMARY}" "${RAW_DIR_ALT}"
RAW_DIR="${RAW_DIR_PRIMARY}"

if [[ "${RESTART_ETL}" -eq 1 ]]; then
  if [[ ! -f "${DOCS_DIR}/static-manifest.json" && ! -f "${DOCS_DIR}/static-manifest.json.gz" && ! -f "${DOCS_DIR}/static-manifest.json.prepass" ]]; then
    warn "Manifest missing; restart will rebuild from shards."
  fi
  shard_sqlite_count="$(count_glob "${DOCS_DIR}/static-shards/*.sqlite")"
  shard_gz_count="$(count_glob "${DOCS_DIR}/static-shards/*.sqlite.gz")"
  if [[ "${shard_sqlite_count}" -eq 0 && "${shard_gz_count}" -eq 0 ]]; then
    fail "No shard files found for restart in ${DOCS_DIR}/static-shards"
  fi
  pass "Restarting ETL post-pass from existing shards"
  post_concurrency="$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)"
  confirm_step "Restart ETL post-pass now? (etl-hn.js --restart --gzip)" in_repo node ./etl-hn.js --restart --gzip --post-concurrency "${post_concurrency}"
elif [[ "${FROM_SHARDS}" -eq 1 ]]; then
  shard_sqlite_count="$(count_glob "${DOCS_DIR}/static-shards/*.sqlite")"
  shard_gz_count="$(count_glob "${DOCS_DIR}/static-shards/*.sqlite.gz")"
  if [[ "${shard_sqlite_count}" -eq 0 && "${shard_gz_count}" -eq 0 ]]; then
    fail "No shard files found for from-shards in ${DOCS_DIR}/static-shards"
  fi
  normalize_shard_hashes "${DOCS_DIR}/static-shards"
  if [[ "${HASH_ONLY}" -eq 1 ]]; then
    pass "Hash-only mode: skipping ETL post-pass"
  else
    pass "Rebuilding from existing shards (post-pass + gzip)"
    post_concurrency="$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)"
    confirm_step "Finalize shards now? (etl-hn.js --restart --gzip)" in_repo node ./etl-hn.js --restart --gzip --post-concurrency "${post_concurrency}"
  fi
elif [[ "${USE_STAGING}" -eq 1 ]]; then
  if [[ -f "${REPO_DIR}/data/static-staging-hn.sqlite" ]]; then
    pass "Using staging DB: ${REPO_DIR}/data/static-staging-hn.sqlite"
  else
    fail "Staging DB not found: ${REPO_DIR}/data/static-staging-hn.sqlite"
  fi
  warn "Skipping raw download; ETL will run with --from-staging"
  confirm_step "Run full ETL now? (etl-hn.js --gzip --from-staging)" in_repo node ./etl-hn.js --gzip --from-staging
else
  raw_primary_count="$(count_glob "${RAW_DIR_PRIMARY}/*.json.gz")"
  raw_alt_count="$(count_glob "${RAW_DIR_ALT}/*.json.gz")"

  if [[ "${raw_primary_count}" -gt 0 ]]; then
    pass "BigQuery extract present: ${raw_primary_count} raw files in data/raw"
  elif [[ "${raw_alt_count}" -gt 0 ]]; then
    RAW_DIR="${RAW_DIR_ALT}"
    pass "BigQuery extract present: ${raw_alt_count} raw files in toool/data/raw"
    warn "Using toool/data/raw; ETL will be run with --data \"${RAW_DIR}\""
  else
    warn "No raw data found in data/raw/*.json.gz (or toool/data/raw/*.json.gz)"
    if ensure_gcloud; then
      ensure_gcloud_auth
    fi
    confirm_step "Run download script now? (download_hn.sh)" bash -lc "cd \"${REPO_DIR}\" && bash ./download_hn.sh"
    raw_primary_count="$(count_glob "${RAW_DIR_PRIMARY}/*.json.gz")"
    if [[ "${raw_primary_count}" -gt 0 ]]; then
      pass "Downloaded ${raw_primary_count} raw files"
    else
      warn "No raw data available; ETL step may fail if you run it."
    fi
  fi

confirm_step "Run full ETL now? (etl-hn.js --gzip)" in_repo node ./etl-hn.js --gzip --data "${RAW_DIR}"
fi

should_rebuild_archive_index() {
  local manifest_json="${DOCS_DIR}/static-manifest.json"
  local manifest_gz="${DOCS_DIR}/static-manifest.json.gz"
  local archive_json="${DOCS_DIR}/archive-index.json"
  if [[ ! -f "${archive_json}" ]]; then
    return 0
  fi
  local manifest_ref=""
  if [[ -f "${manifest_json}" ]]; then
    manifest_ref="${manifest_json}"
  elif [[ -f "${manifest_gz}" ]]; then
    manifest_ref="${manifest_gz}"
  else
    return 0
  fi
  if [[ "${manifest_ref}" -nt "${archive_json}" ]]; then
    return 0
  fi
  return 1
}

if should_rebuild_archive_index; then
  confirm_step "Rebuild archive index now? (build-archive-index.js)" in_repo node ./build-archive-index.js
else
  pass "Archive index newer than manifest; skipping rebuild"
fi
archive_json="${DOCS_DIR}/archive-index.json"
archive_gz="${DOCS_DIR}/archive-index.json.gz"
if [[ -f "${archive_json}" ]]; then
  confirm_step "Gzip archive-index.json (-9)" gzip_replace "${archive_json}" "${archive_gz}"
elif [[ -f "${archive_gz}" ]]; then
  manifest_ref="${DOCS_DIR}/static-manifest.json"
  if [[ ! -f "${manifest_ref}" ]]; then
    manifest_ref="${DOCS_DIR}/static-manifest.json.gz"
  fi
  if [[ -f "${manifest_ref}" ]]; then
    assert_fresh "${archive_gz}" "$(file_mtime "${manifest_ref}")"
  fi
  pass "archive-index.json.gz already present; skipping gzip"
else
  warn "Missing archive-index.json; run build-archive-index.js"
fi

confirm_step "Rebuild cross-shard index now? (build-cross-shard-index.mjs --binary)" in_repo node ./toool/s/build-cross-shard-index.mjs --binary
cross_bin="${DOCS_DIR}/cross-shard-index.bin"
cross_gz="${DOCS_DIR}/cross-shard-index.bin.gz"
if [[ -f "${cross_bin}" ]]; then
  confirm_step "Gzip cross-shard-index.bin (-9)" gzip_replace "${cross_bin}" "${cross_gz}"
elif [[ -f "${cross_gz}" ]]; then
  shard_time="$(latest_mtime_glob "${DOCS_DIR}/static-shards/*.sqlite*")"
  if [[ "${shard_time}" -gt 0 ]]; then
    assert_fresh "${cross_gz}" "${shard_time}"
  fi
  pass "cross-shard-index.bin.gz already present; skipping gzip"
else
  warn "Missing cross-shard-index.bin; run build-cross-shard-index.mjs"
fi

user_stats_manifest_json="${DOCS_DIR}/static-user-stats-manifest.json"
user_stats_manifest_gz="${DOCS_DIR}/static-user-stats-manifest.json.gz"
user_stats_sqlite_count="$(count_glob "${DOCS_DIR}/static-user-stats-shards/*.sqlite")"
user_stats_gz_count="$(count_glob "${DOCS_DIR}/static-user-stats-shards/*.gz")"
if [[ "${user_stats_sqlite_count}" -gt 0 || "${user_stats_gz_count}" -gt 0 ]]; then
  if [[ ! -f "${user_stats_manifest_json}" && ! -f "${user_stats_manifest_gz}" ]]; then
    rm -f "${user_stats_manifest_gz}.tmp" || true
    confirm_step "Rebuild user stats manifest from shards now? (build-user-stats.mjs --manifest-only --gzip --target-mb 15)" in_repo node ./toool/s/build-user-stats.mjs --manifest-only --gzip --target-mb 15
  elif [[ ! -f "${user_stats_manifest_json}" && -f "${user_stats_manifest_gz}" ]]; then
    pass "User stats manifest gz present; skipping rebuild"
  else
    confirm_step "Rebuild user stats now? (build-user-stats.mjs --gzip --target-mb 15)" in_repo node ./toool/s/build-user-stats.mjs --gzip --target-mb 15
  fi
else
  confirm_step "Rebuild user stats now? (build-user-stats.mjs --gzip --target-mb 15)" in_repo node ./toool/s/build-user-stats.mjs --gzip --target-mb 15
fi
if [[ -f "${user_stats_manifest_json}" ]]; then
  rm -f "${user_stats_manifest_gz}.tmp" || true
  confirm_step "Gzip static-user-stats-manifest.json (-9)" gzip_replace "${user_stats_manifest_json}" "${user_stats_manifest_gz}"
elif [[ -f "${user_stats_manifest_gz}" ]]; then
  user_stats_time="$(latest_mtime_glob "${DOCS_DIR}/static-user-stats-shards/*.sqlite*")"
  if [[ "${user_stats_time}" -gt 0 ]]; then
    assert_fresh "${user_stats_manifest_gz}" "${user_stats_time}"
  fi
  pass "static-user-stats-manifest.json.gz already present; skipping gzip"
else
  warn "Missing static-user-stats-manifest.json; run build-user-stats.mjs --manifest-only"
fi

static_manifest_json="${DOCS_DIR}/static-manifest.json"
static_manifest_gz="${DOCS_DIR}/static-manifest.json.gz"
if [[ -f "${static_manifest_json}" ]]; then
  confirm_step "Gzip static-manifest.json (-9)" gzip_replace "${static_manifest_json}" "${static_manifest_gz}"
elif [[ -f "${static_manifest_gz}" ]]; then
  shard_time="$(latest_mtime_glob "${DOCS_DIR}/static-shards/*.sqlite*")"
  if [[ "${shard_time}" -gt 0 ]]; then
    assert_fresh "${static_manifest_gz}" "${shard_time}"
  fi
  pass "static-manifest.json.gz already present; skipping gzip"
else
  warn "Missing static-manifest.json; run etl-hn.js --rebuild-manifest"
fi

step "Checking required files"
require_file "${DOCS_DIR}/index.html"
require_file "${DOCS_DIR}/static.html"
require_file "${DOCS_DIR}/static-manifest.json.gz"
require_file "${DOCS_DIR}/archive-index.json.gz"
require_file "${DOCS_DIR}/static-user-stats-manifest.json.gz"
require_file "${DOCS_DIR}/cross-shard-index.bin.gz"
pass "Core files present"

step "Checking content shards"
mkdir -p "${DOCS_DIR}/static-shards"
shard_count="$(count_glob "${DOCS_DIR}/static-shards/*.gz")"
[[ "${shard_count}" -gt 0 ]] || fail "No content shards found"
pass "Found ${shard_count} content shard files"

step "Checking user stats"
mkdir -p "${DOCS_DIR}/static-user-stats-shards"
user_shard_count="$(count_glob "${DOCS_DIR}/static-user-stats-shards/*.gz")"
[[ "${user_shard_count}" -gt 0 ]] || fail "No user stats shards found"
pass "Found ${user_shard_count} user stats shard files"

step "Validating gzip assets"
gzip_test "${DOCS_DIR}/static-manifest.json.gz"
gzip_test "${DOCS_DIR}/archive-index.json.gz"
gzip_test "${DOCS_DIR}/static-user-stats-manifest.json.gz"
gzip_test "${DOCS_DIR}/cross-shard-index.bin.gz"
pass "Core gzip assets OK"

if [[ -f "${DOCS_DIR}/static-manifest.json" ]]; then
  validate_manifest_shards "${DOCS_DIR}/static-manifest.json" "${DOCS_DIR}/static-shards" "content"
else
  warn "docs/static-manifest.json missing; validating via docs/static-manifest.json.gz"
  validate_manifest_shards "${DOCS_DIR}/static-manifest.json.gz" "${DOCS_DIR}/static-shards" "content"
fi
if [[ -f "${DOCS_DIR}/static-user-stats-manifest.json" ]]; then
  validate_manifest_shards "${DOCS_DIR}/static-user-stats-manifest.json" "${DOCS_DIR}/static-user-stats-shards" "user-stats"
else
  warn "docs/static-user-stats-manifest.json missing; validating via docs/static-user-stats-manifest.json.gz"
  validate_manifest_shards "${DOCS_DIR}/static-user-stats-manifest.json.gz" "${DOCS_DIR}/static-user-stats-shards" "user-stats"
fi

step "Quick sanity checks"
if ! rg -q "cross-shard-index.bin.gz" "${DOCS_DIR}/index.html"; then
  fail "index.html missing cross-shard-index.bin.gz reference"
fi
if ! rg -q "static-manifest.json.gz" "${DOCS_DIR}/index.html"; then
  fail "index.html missing static-manifest.json.gz reference"
fi
if ! rg -q "static-user-stats-manifest.json.gz" "${DOCS_DIR}/index.html"; then
  fail "index.html missing static-user-stats-manifest.json.gz reference"
fi
pass "HTML references OK"

step "Validating JSON manifests"
node - "${DOCS_DIR}/archive-index.json.gz" "${DOCS_DIR}/static-user-stats-manifest.json.gz" <<'NODE'
const fs = require('fs');
const zlib = require('zlib');
const paths = process.argv.slice(1).filter(p => p !== '-');
for (const p of paths) {
  let raw = fs.readFileSync(p);
  if (p.endsWith('.gz')) raw = zlib.gunzipSync(raw);
  const j = JSON.parse(raw.toString('utf8'));
  if (!j || typeof j !== 'object') throw new Error(`Invalid JSON object: ${p}`);
}
NODE
pass "Manifests parse as JSON"

node - "${DOCS_DIR}/static-user-stats-manifest.json.gz" <<'NODE'
const fs = require('fs');
const zlib = require('zlib');
const args = process.argv.slice(1).filter(p => p !== '-');
const path = args[0];
if (!path) throw new Error('Missing manifest path');
let raw = fs.readFileSync(path);
if (path.endsWith('.gz')) raw = zlib.gunzipSync(raw);
const m = JSON.parse(raw.toString('utf8'));
if (!Array.isArray(m.shards) || m.shards.length === 0) throw new Error('user stats manifest missing shards');
if (!Array.isArray(m.user_growth) || m.user_growth.length === 0) throw new Error('user stats manifest missing user_growth');
if (!Array.isArray(m.user_active) || m.user_active.length === 0) throw new Error('user stats manifest missing user_active');
NODE
pass "User stats manifest includes growth + MAU"

step "Manifest gzip summary"
for m in "${DOCS_DIR}/static-manifest.json" "${DOCS_DIR}/archive-index.json" "${DOCS_DIR}/static-user-stats-manifest.json"; do
  if [[ -f "${m}.gz" ]]; then
    pass "Gz present: $(basename "${m}.gz")"
  else
    warn "Missing gz: $(basename "${m}.gz")"
  fi
done

step "Lock core gzip assets (chmod 444)"
for f in "${DOCS_DIR}/static-manifest.json.gz" \
  "${DOCS_DIR}/archive-index.json.gz" \
  "${DOCS_DIR}/static-user-stats-manifest.json.gz" \
  "${DOCS_DIR}/cross-shard-index.bin.gz"; do
  if [[ -f "${f}" ]]; then
    lock_file "${f}"
    pass "Locked: $(basename "${f}")"
  else
    warn "Missing: $(basename "${f}")"
  fi
done

pause "Smoke UI locally (/ , ?view=query, ?view=user&id=pg, ?view=archive). Press Enter to continue..."

step "Checking custom domain CNAME"
confirm_step "Check DNS now? (${CUSTOM_DOMAIN} → ${EXPECTED_CNAME})" check_cname "${CUSTOM_DOMAIN}" "${EXPECTED_CNAME}"

if command -v wrangler >/dev/null 2>&1; then
  confirm_step "Deploy now? (wrangler pages deploy)" in_repo wrangler pages deploy docs --project-name "${PAGES_PROJECT_NAME}" --commit-dirty=true
else
  warn "wrangler not found; skipping deploy step"
fi

log ""
pass "Pre‑deploy checklist complete 🎉"
