# Search

Run vector similarity search, full-text search, and hybrid search from Spark SQL using Lance namespace execution.

!!! warning "Spark Extension Required"
    These table functions require the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

!!! note "Namespace Tables Required"
    `VECTOR_SEARCH`, `SEARCH`, and `HYBRID_SEARCH` resolve the `table` argument through a Spark catalog and execute through the Lance namespace `queryTable` API. Use a Lance namespace catalog table such as `lance.default.items`, not a raw Lance dataset path.

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

## HYBRID_SEARCH

`HYBRID_SEARCH` runs vector search and full-text search through the Lance namespace API, then merges the two result sets in Spark using reciprocal rank fusion. It returns the selected table columns plus `_distance`, `_score`, and `_relevance_score`.

Rows that only match one side have null for the other side's metric.

=== "SQL"
    ```sql
    SELECT id, body, _distance, _score, _relevance_score
    FROM HYBRID_SEARCH(
        table => 'lance.default.documents',
        query_vector => array(0.12, 0.34, 0.56, 0.78),
        query => 'vector database',
        vector_column => 'embedding',
        search_columns => array('body'),
        columns => array('id', 'body'),
        num_results => 10,
        candidates => 50,
        rrf_k => 60.0
    )
    ORDER BY _relevance_score DESC;
    ```

### Reranking

Hybrid search currently performs client-side reciprocal rank fusion:

```text
_relevance_score = sum(1.0 / (rank + rrf_k))
```

Ranks are zero-based in each side's result set. `candidates` controls how many rows are fetched from each side before reranking. When omitted, it defaults to `num_results + offset`.

### Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `table` | String | Yes | Catalog table name to search. |
| `query_vector` | Array numeric literal | Yes | Query vector. |
| `query` or `search_query` | String | Yes | Full-text query string. |
| `vector_column` | String | No | Vector column name. Lance defaults to `vector` when omitted. |
| `search_columns` | Array string literal | No | Text columns to search. |
| `num_results`, `limit`, or `k` | Integer | No | Number of final reranked results. Defaults to `10`. |
| `candidates`, `num_candidates`, or `candidate_count` | Integer | No | Number of rows to fetch from each side before reranking. Defaults to `num_results + offset`. |
| `rrf_k` | Float | No | Reciprocal rank fusion constant. Defaults to `60.0`. |
| `columns` | Array string literal | No | Output table columns. `_distance`, `_score`, and `_relevance_score` are always included. |
| `filter` | String | No | SQL filter expression evaluated by Lance on both side queries. |
| `offset` | Integer | No | Number of reranked results to skip after fusion. |
| `version` | Long | No | Lance table version to search. |
| `distance_type` | String | No | Distance metric such as `l2`, `cosine`, or `dot`. |
| `nprobes`, `ef`, `refine_factor` | Integer | No | Vector index search tuning parameters. |
| `lower_bound`, `upper_bound` | Float | No | Distance bounds. |
| `bypass_vector_index`, `fast_search`, `prefilter`, `with_row_id` | Boolean | No | Lance query options. |

## Positional Form

Named arguments are recommended when supported. For simple calls and Spark 3.4 compatibility, positional arguments are also accepted.

=== "SQL"
    ```sql
    SELECT *
    FROM VECTOR_SEARCH('lance.default.items', array(0.12, 0.34, 0.56), 5);

    SELECT *
    FROM SEARCH('lance.default.documents', 'lance', 5);

    SELECT *
    FROM HYBRID_SEARCH('lance.default.documents', array(0.12, 0.34, 0.56), 'lance', 5);
    ```

## Validation

The Docker integration suite covers `VECTOR_SEARCH`, `SEARCH`, and `HYBRID_SEARCH` against the directory namespace and a REST namespace backed by a directory namespace. The `Spark Search Docker` GitHub Actions workflow runs both backends for pull requests.
