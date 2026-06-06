# SEARCH

Run Lance full-text search from Spark SQL using Lance namespace execution.

!!! warning "Spark Extension Required"
    `SEARCH` requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

!!! note "Namespace Tables Required"
    `SEARCH` resolves the `table` argument through a Spark catalog and executes through the Lance namespace `queryTable` API. Use a Lance namespace catalog table such as `lance.default.documents`, not a raw Lance dataset path.

!!! note "Named Arguments"
    Named arguments require Spark 3.5 or later. On Spark 3.4, use the positional form.

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

## Positional Form

Use positional arguments for simple calls and Spark 3.4 compatibility.

=== "SQL"
    ```sql
    SELECT *
    FROM SEARCH('lance.default.documents', 'lance', 5);
    ```

## Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `table` | String | Yes | Catalog table name to search. |
| `query` or `search_query` | String | Yes | Full-text query string. |
| `search_columns` | Array string literal | No | Text columns to search. When omitted, Lance uses the indexed columns configured for the FTS index. |
| `num_results`, `limit`, or `k` | Integer | No | Number of results. Defaults to `10`. |
| `columns` | Array string literal | No | Output table columns. `_score` is always included. Use `array('*')` or omit this argument for all table columns. |
| `filter` | String | No | SQL filter expression evaluated by Lance. |
| `offset` | Integer | No | Number of results to skip. |
| `version` | Long | No | Lance table version to search. |
| `with_row_id` | Boolean | No | Include Lance row ids in the result as `_rowid`. |

## Output

The result includes the requested table columns and a nullable `_score` float column. If `with_row_id => true`, or if `_rowid` is listed in `columns`, the result also includes Lance row ids.

## Execution

Spark plans `SEARCH` as a DataSource V2 batch read with one input partition. The partition reader calls the Lance namespace `queryTable` API. With a directory namespace the search runs in the Spark process executing that reader; with a REST namespace the REST server handles the namespace request.

## Validation

The Docker integration suite covers `SEARCH` against the directory namespace and a REST namespace backed by a directory namespace. The `Spark Search Docker` GitHub Actions workflow runs both backends for pull requests.
