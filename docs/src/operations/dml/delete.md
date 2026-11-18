# DELETE FROM

Currently, delete only supports for Spark 3.5+.

Delete with condition:

```sql
DELETE FROM users WHERE id = 4;
```

Delete all rows:

```sql
DELETE FROM users;
```

On Spark 3.5+, deletion files are written by Spark tasks, with each affected fragment
handled by one task. Tasks return fragment metadata to the driver, which publishes
the deletions in one atomic table commit. Uncommitted task attempts do not change
the table visible to readers.

Each task limits deletion-file write concurrency to `spark.executor.cores`, or two
concurrent writes when that setting is unset.
