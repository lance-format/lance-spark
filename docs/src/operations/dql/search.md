# Search

Run vector similarity search and full-text search from Spark SQL using Lance namespace execution.

!!! warning "Spark Extension Required"
    These table functions require the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

!!! note "Namespace Tables Required"
    `VECTOR_SEARCH` and `SEARCH` resolve the `table` argument through a Spark catalog and execute through the Lance namespace `queryTable` API. Use a Lance namespace catalog table such as `lance.default.items`, not a raw Lance dataset path.

!!! note "Named Arguments"
    Named arguments require Spark 3.5 or later. On Spark 3.4, use the positional form shown below.

!!! note "Replacing `nearest` Read Option"
    The previous DataFrame `nearest` read option has been removed. Use `VECTOR_SEARCH` for vector similarity search so execution goes through the Lance namespace API.

## VECTOR_SEARCH

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

### Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `table` | String | Yes | Catalog table name to search. |
| `query_vector` | Array numeric literal | Yes | Query vector. |
| `vector_column` | String | No | Vector column name. Lance defaults to `vector` when omitted. |
| `num_results`, `limit`, or `k` | Integer | No | Number of results. Defaults to `10`. |
| `distance_type` | String | No | Distance metric such as `l2`, `cosine`, or `dot`. |
| `columns` | Array string literal | No | Output table columns. `_distance` is always included. |
| `filter` | String | No | SQL filter expression evaluated by Lance. |
| `offset` | Integer | No | Number of results to skip. |
| `version` | Long | No | Lance table version to search. |
| `nprobes`, `ef`, `refine_factor` | Integer | No | Vector index search tuning parameters. |
| `lower_bound`, `upper_bound` | Float | No | Distance bounds. |
| `bypass_vector_index`, `fast_search`, `prefilter`, `with_row_id` | Boolean | No | Lance query options. |

## SEARCH

`SEARCH` runs Lance full-text search and returns the selected table columns plus `_score`.
Create an FTS index before querying text columns.

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

### Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `table` | String | Yes | Catalog table name to search. |
| `query` or `search_query` | String | Yes | Full-text query string. |
| `search_columns` | Array string literal | No | Text columns to search. |
| `num_results`, `limit`, or `k` | Integer | No | Number of results. Defaults to `10`. |
| `columns` | Array string literal | No | Output table columns. `_score` is always included. |
| `filter` | String | No | SQL filter expression evaluated by Lance. |
| `offset` | Integer | No | Number of results to skip. |
| `version` | Long | No | Lance table version to search. |
| `with_row_id` | Boolean | No | Include Lance row ids in the result. |

## Positional Form

Named arguments are recommended when supported. For simple calls and Spark 3.4 compatibility, positional arguments are also accepted.

=== "SQL"
    ```sql
    SELECT *
    FROM VECTOR_SEARCH('lance.default.items', array(0.12, 0.34, 0.56), 5);

    SELECT *
    FROM SEARCH('lance.default.documents', 'lance', 5);
    ```
