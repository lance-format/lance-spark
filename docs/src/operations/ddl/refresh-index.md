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
| `num_segments` | Integer | Number of parallel build tasks, and so of index segments added (clamped to the number of uncovered fragments when larger). Each task takes a contiguous run of fragments, sized to balance estimated workloads by row count. Defaults to `min(uncovered_fragments, spark.default.parallelism)`. |

Option names are case-insensitive: `WITH (NUM_SEGMENTS = 8)` and `WITH (num_segments = 8)` are the same option.

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

[OPTIMIZE](./optimize.md) replaces compacted fragments with new ones. A `zonemap` or `bloomfilter`
index records physical row addresses, which a rewrite invalidates, so its coverage drops and a
refresh restores it:

=== "SQL"
    ```sql
    OPTIMIZE lance.db.users;

    ALTER TABLE lance.db.users REFRESH INDEX zone_idx;
    ```

The other methods record row ids, which follow their rows through a rewrite, so compaction carries
their coverage over and a refresh afterwards has nothing to do. See
[Compaction](#notes-and-limitations) below.

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

The deferred index holds no built data to inherit method options from, so pass them to the refresh
that populates it — see [Index Method Options](#index-method-options).

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
| `fragments_indexed` | Long   | Number of previously uncovered fragments the commit covers.        |
| `segments_added`    | Long   | Number of new physical index segments committed.                    |
| `index_name`        | String | Name of the refreshed index, as stored in the table metadata.      |

A refresh with nothing to do returns zeros and commits no new table version.

`fragments_indexed` counts the fragments actually covered, not the fragments planned: if a concurrent
operation retires one while the build runs, it is excluded and a warning names it.

## How It Works

1.  **Planning**: the driver resolves the named index, subtracts its fragment coverage from the
    table's fragments, and splits the remainder into batches, each a contiguous run of fragments
    sized to balance row counts. Contiguity keeps [OPTIMIZE](./optimize.md) able to group the
    fragments a segment covers.
2.  **Distributed Build**: each batch becomes a Spark task that builds one uncommitted index
    segment. Uncommitted segments are invisible to readers, so a failed build cannot affect query
    results.
3.  **Transactional Commit**: the driver commits the new segments as part of the same logical index.
    Segments covering already-indexed fragments are retained, so existing coverage is preserved and
    the change is atomic.

Planning and building both run against a single pinned table version, so every task sees the
fragment set the driver planned over.

## Index Method Options

`REFRESH INDEX` builds its segments from the options in its own `WITH` clause; options you leave out
fall back to the index type's defaults rather than to whatever the index was built with. Lance
records a built index's parameters inside the index itself, not as table metadata a command can read
back, so pass the same options to the refresh that you passed to `CREATE INDEX`:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE INDEX idx_id USING zonemap (id) WITH (rows_per_zone = 2048);

    ALTER TABLE lance.db.users REFRESH INDEX idx_id WITH (rows_per_zone = 2048);
    ```

For most methods each segment is queried on its own and records its own configuration, so a mismatch
changes performance characteristics rather than results.

`fts` (or `inverted`) is the exception: a full-text index is read with one configuration for all of
its segments, and a set that disagrees cannot be queried. `REFRESH INDEX` therefore compares what it
built against the segments it would join and **fails without committing** when they differ, leaving
the index exactly as it was:

```
Index 'idx_text' uses the fts method, whose segments must all share one configuration, and the
segments this build produced are configured differently from the ones they would join. Nothing was
committed and the index is unchanged. Re-run with the options the index was created with, or rebuild
it in full with ALTER TABLE ... CREATE INDEX.
```

Re-run with the original options, or rebuild with [CREATE INDEX](./create-index.md), which replaces
every segment and so has nothing to agree with. An `fts` index built by an older Lance version can
also record a configuration the current one does not reproduce even from the same options; a full
rebuild is the fix there.

## Notes and Limitations

- **Index Methods**: supported for the same methods as
  [CREATE INDEX](./create-index.md#index-methods): `zonemap`, `btree`, `bitmap`, `label_list`,
  `ngram`, `bloomfilter`, `rtree`, and `fts` (or `inverted`). A vector index cannot be created from
  Spark SQL either, so refreshing one is rejected and pointed at the Lance SDK rather than at
  `CREATE INDEX`, which could not build it.
- **Full-Build Options**: `train`, `build_mode`, and `rows_per_range` are rejected. Range-mode
  `btree` redistributes and sorts the whole table, which an incremental refresh does not do — use
  `CREATE INDEX` to rebuild that way.
- **Method Options Are Not Inherited**: new segments are built from the `WITH` clause and the index
  type's defaults, not from the existing index's configuration. For `fts` a mismatch is rejected;
  for the other methods it changes performance only. See
  [Index Method Options](#index-method-options).
- **Indexes Created Before Coverage Tracking**: a segment that predates fragment coverage tracking
  reports no coverage, so refreshing such an index rebuilds the whole table rather than a subset.
  If the index mixes such a segment with a tracked one, the refresh fails rather than commit partial
  coverage it cannot place; rebuild it with `CREATE INDEX`.
- **Multi-Field Indexes**: an index keyed on more than one field, or carrying columns beyond its key,
  is rejected — a refreshed segment would declare only the key. Rebuild it with `CREATE INDEX`.
- **System Indexes**: Lance-maintained indexes, including fragment-reuse and MemWAL indexes, cannot
  be refreshed.
- **Compaction**: [OPTIMIZE](./optimize.md) replaces the fragments it compacts with new ones. Whether
  that costs the index its coverage depends on what the index stores. `zonemap` and `bloomfilter`
  record physical row addresses, which the rewrite invalidates, so they lose coverage and need a
  refresh afterwards. `btree`, `bitmap`, `label_list`, `ngram`, `rtree` and `fts` record row ids,
  which follow their rows, so compaction carries their coverage over and a refresh reports
  `fragments_indexed = 0`.
- **Stale `zonemap` Coverage Can Hide Rows**: on the Lance version this connector builds against, a
  partially covered `zonemap` index prunes the fragments it does not cover, so a predicate on the
  indexed column can return fewer rows than the table holds (`COUNT(*)` over the whole table stays
  correct). Refresh a `zonemap` index after appending or compacting before relying on filters over
  it. The other methods return complete results while partially covered.
- **Concurrent Retirement**: if a concurrent operation retires a fragment while a refresh is
  building it, that fragment is left out of the commit and named in a warning, and
  `fragments_indexed` counts only what was covered. Re-run the refresh to pick up whatever replaced
  it. A refresh whose fragments were *all* retired commits nothing and fails instead.
- **Concurrent `DROP INDEX`**: do not drop an index while a refresh of it is in flight. The refresh
  re-resolves its target immediately before committing and fails if it is gone, but the check and the
  commit are separate transactions and Lance does not currently treat a concurrent same-name drop as
  a conflict. A `DROP INDEX` landing in that window is undone: the index reappears covering only the
  fragments the refresh built. Tracked upstream in
  [lance#6806](https://github.com/lance-format/lance/pull/6806).
- **Segment Growth**: each refresh adds segments to the index, and Lance can only compact fragments
  that are covered by the identical set of index segments. So accumulated refreshes progressively
  narrow what [OPTIMIZE](./optimize.md) can group: run `OPTIMIZE` before `REFRESH INDEX` rather than
  after, and rebuild with [CREATE INDEX](./create-index.md) to consolidate the segments back into one
  when the count grows. [SHOW INDEXES](./show-indexes.md) reports `num_segments`.

## See Also

- [CREATE INDEX](./create-index.md)
- [SHOW INDEXES](./show-indexes.md)
- [DROP INDEX](./drop-index.md)
- [OPTIMIZE](./optimize.md)
