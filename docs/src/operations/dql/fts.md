# Full-Text Search

Query Lance tables with full-text search (FTS) using the `lance_match` SQL function.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

!!! note "FTS Index Required for Acceleration"
    `lance_match` uses the Lance inverted index when the queried column has one. Without an index, it falls back to plain substring matching — correct but slow. See [CREATE INDEX — FTS Options](../ddl/create-index.md#fts-options) for how to build one.

## Overview

`lance_match(column, 'query')` is a scalar predicate function that translates to a native Lance full-text query at scan time. The optimizer rewrites matching filter expressions into `ScanOptions.fullTextQuery(...)`, so BM25 relevance ranking and tokenizer rules (language, stemming, lower-casing, etc.) from the index definition apply during the scan — not as a post-scan filter in Spark.

## Basic Usage

=== "SQL"
    ```sql
    SELECT id, content
    FROM lance.db.docs
    WHERE lance_match(content, 'apache spark');
    ```

The default operator between query terms is OR (multiple terms → documents containing at least one term match).

## Combining with Scalar Filters

FTS and scalar predicates combine naturally with `AND`. The scalar part stays as a regular Spark filter; the FTS part goes into the index scan.

=== "SQL"
    ```sql
    SELECT id, content
    FROM lance.db.docs
    WHERE lance_match(content, 'python') AND year = 2026;
    ```

## Counting Matches

`count(*) WHERE lance_match(...)` uses the scan-based count path — the inverted index filters rows, and the scanner counts without materializing them. The metadata-only count fast path is bypassed automatically whenever an FTS predicate is active so the count reflects matches, not total rows.

=== "SQL"
    ```sql
    SELECT count(*) FROM lance.db.docs WHERE lance_match(content, 'python');
    ```

## Requirements and Fallback Behavior

- The queried column must have an FTS index for the query to use the index. See [CREATE INDEX](../ddl/create-index.md) for the `USING fts` options (`base_tokenizer`, `language`, `stem`, etc.).
- Without an index, `lance_match` evaluates as a plain substring match in Spark — correct results, but no inverted-index acceleration.
- `lance_match` on a non-string column is rejected at analysis time.

## How It Works

1. `lance_match` is registered as a Spark SQL function via `SparkSessionExtensions.injectFunction`, producing a Catalyst `LanceMatch` expression at analysis time.
2. `LanceFtsPushdownRule` (an optimizer rule) detects `Filter(..., DataSourceV2Relation(LanceTable, ...))` patterns containing `LanceMatch` and moves the column/query text into the table's scan options before the V2 pushdown batch builds the physical scan.
3. `LanceDataset.newScanBuilder` reads those options and configures the `LanceScanBuilder` with an `FtsQuerySpec`.
4. `LanceFragmentScanner` (regular read path) and `LanceCountStarPartitionReader` (count path) call `ScanOptions.Builder.fullTextQuery(FullTextQuery.match(queryText, column))`, so each fragment scan executes an FTS query natively.

## Notes and Limitations

- **Single FTS predicate per query**: `lance_match(col, 'x') AND lance_match(col, 'y')`, `OR` combinations, and nested `NOT lance_match(...)` fall back to Catalyst evaluation of each `LanceMatch` separately (correct but no index use). Multi-term / phrase / boolean variants are on the roadmap.
- **Default operator is `OR`**: query `'apache spark'` matches documents containing either term; explicit `AND` operator, fuzziness, and boost are not yet exposed through the SQL function.
- **Statistics**: FTS selectivity is not currently reported to Spark's cost-based optimizer, so `JoinSelection` may overestimate post-FTS row count when joining FTS-filtered tables.
