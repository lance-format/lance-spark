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
| `nprobes`, `ef`, `refine_factor` | Integer | No | Vector index search tuning parameters. |
| `oversample_factor` | Float | No | Distributed search asks each vector-index task for `ceil((num_results + offset) * oversample_factor)` candidates. Defaults to `1.0`; larger values can improve global recall at the cost of more executor work and shuffle traffic. Flat-search tasks are exact and are not oversampled. |
| `lower_bound`, `upper_bound` | Float | No | Distance bounds. |
| `bypass_vector_index`, `fast_search`, `prefilter`, `with_row_id` | Boolean | No | Lance query options. `with_row_id` adds `_rowid` to the output. `bypass_vector_index` and `fast_search` cannot both be true. |

## Output

The result includes the requested table columns and a nullable `_distance` float column. If `with_row_id => true`, or if `_rowid` is listed in `columns`, the result also includes Lance row ids.

## Execution

By default, Spark plans `VECTOR_SEARCH` with one input partition and calls the Lance namespace
`queryTable` API. When `spark.sql.lance.search.distributed.enabled=true`, Spark instead opens the
dataset from its executors, searches vector-index segments and uncovered fragments in parallel,
and globally merges their candidates. `oversample_factor` applies only to this distributed path.
Distributed execution does not support `lower_bound`, `upper_bound`, or an explicit
`prefilter=false`; these combinations fail during planning. Disable distributed execution to run
them through the namespace.

## Validation

The Docker integration suite covers `VECTOR_SEARCH` against the directory namespace and a REST namespace backed by a directory namespace. The `Spark Search Docker` GitHub Actions workflow runs both backends for pull requests.
