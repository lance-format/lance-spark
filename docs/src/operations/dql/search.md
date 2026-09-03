# SEARCH

Run Lance full-text search from Spark SQL using Lance namespace execution.

!!! warning "Spark Extension Required"
    `SEARCH` requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

!!! note "Namespace Tables Required"
    `SEARCH` resolves the `table` argument through a Spark catalog. Use a Lance namespace catalog table such as `lance.default.documents`, not a raw Lance dataset path.

!!! note "Named Arguments Required"
    `search_columns` is required and has no positional slot, so `SEARCH` must be called with named arguments. Named arguments require Spark 3.5 or later, so `SEARCH` is not available on Spark 3.4.

## Basic Usage

`SEARCH` returns the selected table columns plus `_score`. Create an FTS index before querying text columns.

=== "SQL"
    ```sql
    ALTER TABLE lance.default.documents
    CREATE INDEX body_fts USING fts (body) WITH (
        base_tokenizer = 'simple',
        language = 'English',
        max_token_length = 40,
        lower_case = true,
        stem = false,
        remove_stop_words = false,
        ascii_folding = false,
        with_position = true
    );

    SELECT id, body, _score
    FROM SEARCH(
        table => 'lance.default.documents',
        query => 'vector database',
        search_columns => array('body'),
        columns => array('id', 'body'),
        limit => 10
    )
    ORDER BY _score DESC;
    ```

See [CREATE INDEX](../ddl/create-index.md#full-text-search-index) for FTS index options.

## Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `table` | String | Yes | Catalog table name to search. |
| `query` or `search_query` | String | Yes | Full-text query string. |
| `search_columns` | Array string literal | Yes | Text columns to search. |
| `num_results`, `limit`, or `k` | Integer | No | Number of results. Defaults to `10`. |
| `columns` | Array string literal | No | Output table columns. `_score` is always included. Use `array('*')` or omit this argument for all table columns. |
| `filter` | String | No | SQL filter expression evaluated by Lance. |
| `version` | Long | No | Lance table version to search. |
| `with_row_id` | Boolean | No | Include Lance row ids in the result as `_rowid`. |

## Output

The result includes the requested table columns and a nullable `_score` float column. If `with_row_id => true`, or if `_rowid` is listed in `columns`, the result also includes Lance row ids.

## Execution

Spark plans `SEARCH` as a batch read carrying the full-text query as a scan option, wrapped in an optional filter, a projection, `ORDER BY _score DESC`, and `LIMIT k`. The scan then runs either as a single-partition server-side read through the Lance namespace `queryTable` API, or otherwise as a distributed per-fragment scan. The server-side route requires a namespace that implements `queryTable`, a read that does not target a branch or tag, and no pushed-down aggregation.

## Validation

`SEARCH` has no passing end-to-end coverage today: the Docker integration test is marked `xfail` and the JVM test is `@Disabled`, both pending structured full-text query support in lance-core. [`VECTOR_SEARCH`](vector-search.md) and [`HYBRID_SEARCH`](hybrid-search.md) remain covered by the `Spark Search Docker` workflow.
