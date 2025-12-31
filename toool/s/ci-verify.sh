#!/bin/bash
set -euo pipefail

require_file() {
  local path="$1"
  if [ ! -f "$path" ]; then
    echo "Missing required file: $path" >&2
    exit 1
  fi
}

require_dir() {
  local path="$1"
  if [ ! -d "$path" ]; then
    echo "Missing required directory: $path" >&2
    exit 1
  fi
}

require_file "docs/index.html"
require_file "docs/static.html"
if [ -f "docs/static-manifest.json" ]; then
  require_file "docs/static-manifest.json"
else
  require_file "docs/static-manifest.json.gz"
fi
require_file "docs/archive-index.json.gz"
require_file "docs/cross-shard-index.bin.gz"
require_dir "docs/static-shards"
require_dir "docs/static-user-stats-shards"
require_dir "docs/assets"

# Ensure screenshots referenced in README exist.
require_file "docs/assets/hn-home.png"
require_file "docs/assets/hn-query.png"
require_file "docs/assets/hn-me.png"

echo "CI verify: OK"
