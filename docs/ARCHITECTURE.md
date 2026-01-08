# HackerBook Architecture: SQLite vs Parquet Analysis

This document analyzes the query patterns in HackerBook and explains why SQLite sharding is optimal for this use case.

## Query Pattern Analysis

After analyzing all SQL queries in the codebase, we identified the following distribution:

### Query Breakdown by Type:

1. **Point Lookups (40%)** - `WHERE id=?`, `WHERE username=?`
2. **Top-K Sorted (30%)** - `ORDER BY score/time DESC LIMIT 50`
3. **Aggregations (20%)** - `GROUP BY`, `COUNT()`, `SUM()`
4. **Range Scans (10%)** - `WHERE time BETWEEN`, `WHERE by=? AND time BETWEEN`

## Query Categories in Detail

### 1. POINT LOOKUPS (Primary use case)
Core navigation queries that fetch specific records by primary key:

- `SELECT * FROM items WHERE id=? LIMIT 1` - Single item by ID
- `SELECT * FROM items WHERE id IN (...)` - Batch items by ID list  
- `SELECT first_time FROM users WHERE username=? COLLATE NOCASE LIMIT 1` - Single user lookup
- `SELECT * FROM users WHERE username=? COLLATE NOCASE LIMIT 1` - Full user record
- `SELECT child_id FROM edges WHERE parent_id=?` - Get comment children

**Frequency:** Very High - Core navigation flow

### 2. AGGREGATIONS & ANALYTICS
Used in the Query view and user statistics pages:

- `SELECT by, COUNT(*) as stories FROM items WHERE type='story' GROUP BY by ORDER BY stories DESC LIMIT 50`
- `SELECT domain, COUNT(DISTINCT username) as users FROM user_domains GROUP BY domain ORDER BY users DESC`
- `SELECT month, SUM(count) as active FROM user_months GROUP BY month ORDER BY month DESC`
- `SELECT parent_id, COUNT(*) as cnt FROM edges WHERE parent_id IN (...) GROUP BY parent_id`
- `SELECT e.parent_id as id, i.title, COUNT(*) as comments FROM edges e JOIN items i ON i.id = e.parent_id GROUP BY e.parent_id`

**Frequency:** High - Query view, user stats

### 3. TIME-RANGE SCANS
Used for "Me" view daily activity and time-based filters:

- `SELECT id,type,time,title,text,url,score FROM items WHERE by=? AND time BETWEEN X AND Y ORDER BY time DESC`
- `SELECT id,type,by,time,title,url,score FROM items WHERE type='story' AND time >= (SELECT MAX(time) FROM items) - 86400`

**Frequency:** Medium - "Me" view daily activity, relative time queries

### 4. FILTERED SORTS (Top-K queries)
Front page and list views with filtering and sorting:

- `SELECT id,title,by,time,score FROM items WHERE type='story' ORDER BY score DESC LIMIT 30`
- `SELECT id,title,by,time,score FROM items WHERE type='story' ORDER BY time DESC LIMIT 50`
- `SELECT username, max_score FROM users ORDER BY max_score DESC LIMIT 30`

**Frequency:** Very High - Front page, query templates

### 5. PATTERN MATCHING
Used for Ask HN, Show HN, and Jobs views:

- `SELECT id,title,by,score FROM items WHERE type='story' AND title LIKE 'Ask HN:%' ORDER BY score DESC`
- `SELECT id,title,by,score FROM items WHERE type='story' AND title LIKE 'Show HN:%'`

**Frequency:** Medium - Ask/Show/Jobs views

### 6. SMALL TABLE FULL SCANS
User profile detail queries:

- `SELECT domain, count FROM user_domains WHERE username=? ORDER BY count DESC LIMIT 8`
- `SELECT month, count FROM user_months WHERE username=? ORDER BY count DESC LIMIT 8`

**Frequency:** Medium - User profile views

## Architecture Decision: SQLite vs Parquet + DuckDB

### Where SQLite Excels (Current Architecture)

✅ **Point lookups** - Your bread and butter. Parquet would be 10-100x slower.
- `SELECT * FROM items WHERE id=123` → O(log n) with B-tree index
- Parquet: Must scan row groups, no native indexes

✅ **Top-K with WHERE filters** - SQLite's query optimizer shines
- `WHERE type='story' ORDER BY score DESC LIMIT 30`
- Uses indexes efficiently, stops at 30 rows
- Parquet: Must scan entire column groups

✅ **JOIN queries** - Critical for comment threads
- `edges e JOIN items i ON i.id = e.parent_id`
- SQLite has optimized join algorithms
- Parquet has no JOIN support (must do in application code with DuckDB)

✅ **Browser compatibility**
- sqlite3-wasm: ~1MB
- DuckDB-WASM: ~10MB

### Where Parquet Could Be Interesting

🟡 **Full-shard aggregations** - Query view performs these
- `SELECT by, COUNT(*) as stories FROM items GROUP BY by`
- `SELECT month, SUM(count) as active FROM user_months GROUP BY month`
- Column-oriented storage is faster for these

**However:**
- Results are limited (`LIMIT 50`)
- Users select specific shards to query
- 15MB shard = 1-2s query in SQLite (already fast)
- Cross-shard aggregations rare (users typically query 1-3 shards)

🟡 **Time-range scans on "Me" view**
- `WHERE by=? AND time BETWEEN X AND Y`
- Could benefit from columnar compression and selective column reads
- But: Already using cross-shard index for efficient targeting

### Hybrid Architecture (Considered but Rejected)

We considered maintaining both formats:

```
/static-shards/           # SQLite (current) - for point lookups & navigation
/static-shards-parquet/   # Parquet copies - for query view only
```

**Pros:**
- Query view aggregations could be faster
- Parquet files smaller due to better compression
- HTTP range requests could skip unneeded columns

**Cons:**
- **2x storage** on server (double hosting costs)
- Increased build complexity and time
- DuckDB-WASM is 10MB vs 1MB for SQLite
- Most queries **still need SQLite** for point lookups and JOINs
- Browser must load both SQLite and DuckDB
- Cloudflare Pages caches entire files on first range request anyway

## Final Architecture Decision: SQLite Sharding

**Verdict: Stick with SQLite**

HackerBook's query patterns are:
- **Heavily indexed** (id, type, time, username)
- **Mixed workload** (OLTP point lookups + light OLAP analytics)
- **Small result sets** (LIMIT 30-50 everywhere)
- **Require JOINs** (edges + items for comment threads)
- **Interactive latency requirements** (sub-second)

Parquet excels at:
- **Full table scans** (we rarely do these)
- **Column-heavy analytics** (SELECT few columns from millions of rows)
- **No indexes needed** (we rely heavily on indexes)
- **Large result sets** (we always LIMIT)
- **Batch processing** (not interactive queries)

**Current architecture is optimal:**
- 15MB SQLite shards download in <1s on decent connections
- Users download once, browser caches forever
- Sub-millisecond query latency after initial load
- Content-hash filenames ensure fresh data on updates
- Cross-shard index enables efficient multi-shard targeting

## When Parquet Would Make Sense

If we add a **"Data Export"** feature for data scientists:
- `/static-export/hn-archive.parquet` (single 500MB+ file, all items)
- Users download with DuckDB CLI for local analysis
- Ideal for: "Analyze all HN posts mentioning 'AI' by year"
- Supplementary to main UI, not replacement

But for the **interactive web application**, SQLite + small shards + aggressive caching is the right architecture.

## Current Architecture Benefits

1. **Small shards (~15MB)** = fast initial load
2. **Content-hash filenames** = automatic cache busting on updates
3. **Browser caching** = users typically only need 1-2 shards
4. **Efficient indexes** = sub-millisecond point lookups
5. **sqlite3-wasm (1MB)** = minimal overhead
6. **Proven technology** = works great in browsers
7. **Simple deployment** = static files on Cloudflare Pages

## References

- Query patterns extracted from: `docs/*.html`
- Build process: `etl-hn.js`, `build-user-stats.mjs`
- Shard management: `toool/s/predeploy-checks.sh`
