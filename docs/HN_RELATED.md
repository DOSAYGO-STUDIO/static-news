# HN Related View + Build Notes

This document summarizes the `hn-related` sidecar pipeline and UI that were added alongside the regular HackerBook build.

## What was added

- New related ETL build script: `toool/s/build-related-index.mjs`
- New related UI route: `docs/hn-related.html` (`docs/hn-related/index.html` shim)
- Predeploy branching in `toool/s/predeploy-checks.sh`:
  - regular branch (existing flow)
  - related branch (exclusive branch)
- Related static artifacts:
  - `docs/static-related-shards/*.sqlite.gz`
  - `docs/static-related-manifest.json(.gz)`
  - `docs/related-top.json(.gz)`

## Build/Run

Related-only build:

```bash
node toool/s/build-related-index.mjs --gzip --target-mb 15
```

Predeploy prompt flow supports picking `related` branch and running only related build.

## Ranking + sorting

Top-list sort options now:

- `total comments` (default)
- `total story points`
- `total discussions`
- `pure recency` (day desc, tie by points desc)

## Year filter behavior

Year multiselect is a source-pool filter for top clusters.
A cluster is included if any cluster-year intersects selected years.

## Comment count semantics

`total comments` is intended to be the aggregate of story comment totals across cluster stories.
Current implementation uses story-level `comment_total` populated from HN `descendants` in staging.

If raw export rows do not include `descendants`, totals can be zero.
In that case, comments require upstream extract changes to include story comment totals (or equivalent data to compute them).

## UX notes

- HN-like styling updates (gray rank numbers, small domain tails)
- Sort + year controls in top bar (left)
- Status/related-cluster stats in bottom footer
- Cluster timeline grouped by day, then points within day

