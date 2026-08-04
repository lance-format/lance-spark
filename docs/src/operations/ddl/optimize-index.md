# OPTIMIZE INDEX

Incrementally maintains an existing named Lance index.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

## Syntax

```sql
ALTER TABLE table_name OPTIMIZE INDEX index_name
[WITH (
    num_indices_to_merge = non_negative_integer,
    retrain = boolean
)];
```

The index must already exist. Lance builds index data for fragments not currently covered by the
named index and may merge existing index segments according to the supplied options.

## Options

| Option | Type | Description |
|--------|------|-------------|
| `num_indices_to_merge` | Integer | Number of existing index segments Lance should merge during maintenance. When omitted, Lance chooses its default. Set to `0` to add coverage without requesting a merge of existing segments. |
| `retrain` | Boolean | Whether Lance should retrain the index. Default `false`. Support depends on the index type and format; unsupported combinations are rejected by Lance. |

Both options are passed directly to Lance's `OptimizeOptions`. The target index name is passed as
the sole entry in `indexNames`, so other indexes on the table are not maintained by this command.

## Examples

Maintain a scalar index after new data is appended:

```sql
ALTER TABLE lance.db.users OPTIMIZE INDEX idx_user_id;
```

Build coverage for new fragments without requesting a merge of existing segments:

```sql
ALTER TABLE lance.db.users OPTIMIZE INDEX idx_user_id WITH (
    num_indices_to_merge = 0,
    retrain = false
);
```

## Output

| Column | Type | Description |
|--------|------|-------------|
| `index_name` | String | Name of the maintained index. |
| `fragments_indexed` | Long | Number of previously uncovered live fragments indexed by the operation. |
| `segments_before` | Long | Number of physical segments for the named index before maintenance. |
| `segments_after` | Long | Number of physical segments for the named index after maintenance. |

## Execution

This command currently invokes Lance index maintenance on the Spark driver. Its SQL contract is
independent of execution strategy, so a future distributed implementation can retain the same
syntax and options.

`ALTER TABLE ... OPTIMIZE INDEX` maintains index coverage. The table-level [`OPTIMIZE`](optimize.md)
command compacts data fragments and is a separate operation.
