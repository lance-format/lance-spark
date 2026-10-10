# VECTOR_SEARCH

Run vector similarity search from Spark SQL using Lance namespace execution.

!!! warning "Spark Extension Required"
    `VECTOR_SEARCH` requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

!!! note "Namespace Tables Required"
    `VECTOR_SEARCH` resolves the `table` argument through a Spark catalog and executes through the Lance namespace `queryTable` API. Use a Lance namespace catalog table such as `lance.default.items`, not a raw Lance dataset path.

!!! note "Named Arguments"
    Named arguments require Spark 3.5 or later. On Spark 3.4, use the positional form.

!!! note "Replacing `nearest` Read Option"
    The previous DataFrame `nearest` read option has been removed. Use `VECTOR_SEARCH` for vector similarity search so execution goes through the Lance namespace API.

## Basic Usage

`VECTOR_SEARCH` returns the selected table columns plus `_distance`.

=== "SQL"
    ```sql
    SELECT id, title, _distance
    FROM VECTOR_SEARCH(
        table => 'lance.default.items',
        query_vector => array(0.12, 0.34, 0.56, 0.78),
        vector_column => 'embedding',
        num_results => 10,
        distance_type => 'l2',
        columns => array('id', 'title')
    )
    ORDER BY _distance;
    ```

## Positional Form

Use positional arguments for simple calls and Spark 3.4 compatibility.

=== "SQL"
    ```sql
    SELECT *
    FROM VECTOR_SEARCH('lance.default.items', array(0.12, 0.34, 0.56), 5);
    ```

## Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `table` | String | Yes | Catalog table name to search. |
| `query_vector` | Array numeric literal | Yes | Query vector. |
| `vector_column` | String | No | Vector column name. Lance defaults to `vector` when omitted. |
| `num_results`, `limit`, or `k` | Integer | No | Number of results. Defaults to `10`. |
| `distance_type` | String | No | Distance metric: `l2` (`euclidean`), `cosine`, `dot` (`ip`, `inner_product`), or `hamming`. Aliases are normalized to the canonical name. |
| `columns` | Array string literal | No | Output table columns. `_distance` is always included. Use `array('*')` or omit this argument for all table columns. |
| `filter` | String | No | SQL filter expression evaluated by Lance. |
| `offset` | Integer | No | Number of results to skip. Lance Spark requests `num_results + offset` rows from Lance before applying the offset. |
| `version` | Long | No | Lance table version to search. |
| `nprobes`, `ef` | Integer | No | Vector index search tuning parameters. Raise `nprobes` to probe more IVF partitions, which is what recovers recall lost to an approximate search. |
| `refine_factor` | Integer | No | Over-fetch `num_results * refine_factor` candidates from the index, then re-score them against the original vectors and keep the best `num_results`. Improves accuracy for quantized indexes such as IVF_PQ, at the cost of reading the vectors back. |
| `lower_bound`, `upper_bound` | Float | No | Distance bounds. |
| `bypass_vector_index`, `fast_search`, `prefilter`, `with_row_id` | Boolean | No | Lance query options. `with_row_id` adds `_rowid` to the output. `bypass_vector_index` and `fast_search` cannot both be true. |

## Output

The result includes the requested table columns and a nullable `_distance` float column. If `with_row_id => true`, or if `_rowid` is listed in `columns`, the result also includes Lance row ids.

## Execution

By default, Spark plans `VECTOR_SEARCH` with one input partition and calls the Lance namespace
`queryTable` API. When `spark.sql.lance.search.distributed.enabled=true`, Spark instead opens the
dataset from its executors, searches vector-index segments and uncovered fragments in parallel,
and globally merges their candidates. The merge sorts by the distance each task reported, so each
task returns only its own top `num_results + offset` rows: a row outside a task's local top k
cannot enter the global top k. `nprobes` and `refine_factor` apply per task.

Distributed execution treats `nprobes` as an exact probe count. A namespace server can instead
forward it as a lower bound and let the search probe further, in which case the two paths reach
different recall for the same tuning. Re-check recall after enabling the flag; raise `nprobes`, or
use `bypass_vector_index => true` for an exact baseline, if the results need to be comparable.

A filtered distributed search must pass `prefilter=true`, which returns the true filtered top k.
Namespace execution defaults to `prefilter=false` and applies the filter after choosing the top k,
which usually returns fewer rows; a distributed plan cannot reproduce that, so omitting `prefilter`
with a `filter` fails during planning instead of silently changing the result. `lower_bound` and
`upper_bound` are not supported either. Disable distributed execution to run any of these through
the namespace.

## Validation

The Docker integration suite covers `VECTOR_SEARCH` against the directory namespace and a REST namespace backed by a directory namespace. The `Spark Search Docker` GitHub Actions workflow runs both backends for pull requests.
