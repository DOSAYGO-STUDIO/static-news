#!/usr/bin/env bash
set -euo pipefail

# Generate YYYYMMDD format
NEW_BUST="$(date -u +%Y%m%d)"

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "🔄 Updating CACHE_BUST to: ${NEW_BUST}"

# Update all three HTML files
for html_file in "${REPO_DIR}/docs/index.html" "${REPO_DIR}/docs/static.html" "${REPO_DIR}/docs/mobile.html"; do
  if [[ -f "${html_file}" ]]; then
    # Use sed to replace the CACHE_BUST value
    if [[ "$(uname)" == "Darwin" ]]; then
      # macOS sed
      sed -i '' "s/const CACHE_BUST = '[^']*';/const CACHE_BUST = '${NEW_BUST}';/" "${html_file}"
    else
      # GNU sed
      sed -i "s/const CACHE_BUST = '[^']*';/const CACHE_BUST = '${NEW_BUST}';/" "${html_file}"
    fi
    echo "✅ Updated: $(basename "${html_file}")"
  else
    echo "⚠️  Not found: ${html_file}"
  fi
done

echo "✨ Cache bust updated successfully"
