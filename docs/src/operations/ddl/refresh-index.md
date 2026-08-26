# REFRESH INDEX

Index the fragments an existing index does not yet cover, without rebuilding the ones it does.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

## Overview

Appending to a table creates new fragments, and an existing index does not cover them until it is
maintained. `REFRESH INDEX` builds index data only for those uncovered fragments and adds it to the
index, so cost scales with the newly appended data rather than with the table.

This is the incremental counterpart to re-running [CREATE INDEX](./create-index.md), which rebuilds
every fragment. Both run distributed across Spark executors.

## Syntax

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users REFRESH INDEX user_id_idx;
    ```

## Options

| Option         | Type    | Description                                                                                                                                                                                                                        |
|----------------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `num_segments` | Integer | Target number of parallel build tasks (upper bound; clamped to the number of uncovered fragments when larger). Fragments are assigned by row count to balance estimated task workloads. Defaults to `min(uncovered_fragments, spark.default.parallelism)`. |

Index method options — such as `rows_per_zone` for `zonemap` or the tokenizer settings for `fts` —
are accepted as well, and are applied to the segments this command builds. See
[Index Method Options](#index-method-options) for when you need to pass them.

## Examples

### Refresh after appending data

=== "SQL"
    ```sql
    INSERT INTO lance.db.users VALUES (100, 'new row');

    ALTER TABLE lance.db.users REFRESH INDEX user_id_idx;
    ```

### Refresh after compacting

[OPTIMIZE](./optimize.md) replaces compacted fragments with new ones that the index does not cover,
so refresh after compacting to bring the index back to full coverage:

=== "SQL"
    ```sql
    OPTIMIZE lance.db.users;

    ALTER TABLE lance.db.users REFRESH INDEX user_id_idx;
    ```

### Control build parallelism

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users REFRESH INDEX user_id_idx WITH (num_segments = 8);
    ```

### Populate a deferred index

An index created with `train = false` covers no fragments, so refreshing it builds it over the whole
table through the normal distributed path:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE INDEX idx_id USING zonemap (id) WITH (train = false);

    ALTER TABLE lance.db.users REFRESH INDEX idx_id;
    ```

### Check coverage before and after

[SHOW INDEXES](./show-indexes.md) reports how much of the table each index covers:

=== "SQL"
    ```sql
    SHOW INDEXES FROM lance.db.users;
    ```

A `num_unindexed_fragments` above zero means a refresh has work to do.

## Output

| Column              | Type   | Description                                                        |
|---------------------|--------|--------------------------------------------------------------------|
| `fragments_indexed` | Long   | Number of previously uncovered fragments that were indexed.        |
| `segments_added`    | Long   | Number of new physical index segments committed.                    |
| `index_name`        | String | Name of the refreshed index.                                       |

A refresh with nothing to do returns zeros and commits no new table version.

## How It Works

1.  **Planning**: the driver resolves the named index, subtracts its fragment coverage from the
    table's fragments, and splits the remainder into batches balanced by row count.
2.  **Distributed Build**: each batch becomes a Spark task that builds one uncommitted index
    segment. Uncommitted segments are invisible to readers, so a failed build cannot affect query
    results.
3.  **Transactional Commit**: the driver commits the new segments as part of the same logical index.
    Segments covering already-indexed fragments are retained, so existing coverage is preserved and
    the change is atomic.

Planning and building both run against a single pinned table version, so every task sees the
fragment set the driver planned over.

## Index Method Options

Lance does not expose an already-built index's parameters as table metadata, so `REFRESH INDEX`
cannot recover them from the existing segments. Options you do not pass fall back to the index
type's defaults.

If the index was created with non-default method options, pass the same options to the refresh so
the new segments match the existing ones:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE INDEX idx_id USING zonemap (id) WITH (rows_per_zone = 2048);

    ALTER TABLE lance.db.users REFRESH INDEX idx_id WITH (rows_per_zone = 2048);
    ```

Segments are queried independently and each one records its own configuration, so a mismatch
changes performance characteristics rather than results.

## Notes and Limitations

- **Index Methods**: supported for the same methods as
  [CREATE INDEX](./create-index.md#index-methods): `zonemap`, `btree`, `bitmap`, `label_list`,
  `ngram`, `bloomfilter`, `rtree`, and `fts` (or `inverted`).
- **Full-Build Options**: `train`, `build_mode`, and `rows_per_range` are rejected. Range-mode
  `btree` redistributes and sorts the whole table, which an incremental refresh does not do — use
  `CREATE INDEX` to rebuild that way.
- **Indexes Created Before Coverage Tracking**: a segment that predates fragment coverage tracking
  reports no coverage, so refreshing such an index rebuilds the whole table rather than a subset.
  If the index mixes such a segment with a tracked one, the refresh fails rather than commit partial
  coverage it cannot place; rebuild it with `CREATE INDEX`.
- **Multi-Field Indexes**: an index keyed on more than one field, or carrying columns beyond its key,
  is rejected — a refreshed segment would declare only the key. Rebuild it with `CREATE INDEX`.
- **System Indexes**: Lance-maintained indexes, including fragment-reuse and MemWAL indexes, cannot
  be refreshed.
- **Compaction**: [OPTIMIZE](./optimize.md) replaces the fragments it compacts with new ones, which
  an existing index does not cover. Refresh afterwards to restore coverage. If a concurrent
  `OPTIMIZE` retires a fragment while a refresh is building it, the refresh fails without committing
  rather than silently leaving it unindexed.
- **Segment Growth**: each refresh adds segments to the index. Rebuild with `CREATE INDEX` when you
  want to consolidate them back into a single segment.

## See Also

- [CREATE INDEX](./create-index.md)
- [SHOW INDEXES](./show-indexes.md)
- [DROP INDEX](./drop-index.md)
- [OPTIMIZE](./optimize.md)
