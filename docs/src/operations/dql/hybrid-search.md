# HYBRID_SEARCH

Run vector search and full-text search together from Spark SQL, then rerank the combined results with reciprocal rank fusion.

!!! warning "Spark Extension Required"
    `HYBRID_SEARCH` requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

!!! note "Namespace Tables Required"
    `HYBRID_SEARCH` resolves the `table` argument through a Spark catalog and executes both side queries through the Lance namespace `queryTable` API. Use a Lance namespace catalog table such as `lance.default.documents`, not a raw Lance dataset path.

!!! note "Named Arguments"
    Named arguments require Spark 3.5 or later. On Spark 3.4, use the positional form.

## Basic Usage

`HYBRID_SEARCH` returns the selected table columns plus `_distance`, `_score`, and `_relevance_score`. Rows that only match one side have null for the other side's metric.

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

## Positional Form

Use positional arguments for simple calls and Spark 3.4 compatibility.

=== "SQL"
    ```sql
    SELECT *
    FROM HYBRID_SEARCH('lance.default.documents', array(0.12, 0.34, 0.56), 'lance', 5);
    ```

## Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `table` | String | Yes | Catalog table name to search. |
| `query_vector` | Array numeric literal | Yes | Query vector. |
| `query` or `search_query` | String | Yes | Full-text query string. |
| `vector_column` | String | No | Vector column name. Lance defaults to `vector` when omitted. |
| `search_columns` | Array string literal | No | Text columns to search. When omitted, Lance uses the indexed columns configured for the FTS index. |
| `num_results`, `limit`, or `k` | Integer | No | Number of final reranked results. Defaults to `10`. |
| `candidates`, `num_candidates`, or `candidate_count` | Integer | No | Number of rows to fetch from each side before reranking. Defaults to `num_results + offset`. Values below `num_results + offset` are raised to that minimum. |
| `rrf_k` | Float | No | Reciprocal rank fusion constant. Defaults to `60.0`. |
| `columns` | Array string literal | No | Output table columns. `_distance`, `_score`, and `_relevance_score` are always included. Use `array('*')` or omit this argument for all table columns. |
| `filter` | String | No | SQL filter expression evaluated by Lance on both side queries. |
| `offset` | Integer | No | Number of reranked results to skip after fusion. Defaults to `0`. |
| `version` | Long | No | Lance table version to search. |
| `distance_type` | String | No | Distance metric such as `l2`, `cosine`, or `dot`. |
| `nprobes`, `ef`, `refine_factor` | Integer | No | Vector index search tuning parameters. |
| `lower_bound`, `upper_bound` | Float | No | Distance bounds. |
| `bypass_vector_index`, `fast_search`, `prefilter`, `with_row_id` | Boolean | No | Lance query options. `with_row_id` adds `_rowid` to the output. |

## Reranking

Hybrid search performs reciprocal rank fusion in Spark:

```text
_relevance_score = sum(1.0 / (rank + rrf_k))
```

Ranks are zero-based in each side's result set. `candidates` controls how many rows are fetched from each side before reranking.

## Output

The result includes the requested table columns plus nullable `_distance` and `_score` float columns and a non-null `_relevance_score` float column. If `with_row_id => true`, or if `_rowid` is listed in `columns`, the result also includes Lance row ids.

## Execution

Spark plans `HYBRID_SEARCH` as a DataSource V2 batch read with one input partition. The partition reader issues one vector `queryTable` request and one full-text `queryTable` request through the Lance namespace API, merges the two result sets in Spark with reciprocal rank fusion, and returns the final rows. With a REST namespace the two side searches can be handled by the REST server, while the final fusion currently happens in the Spark task.

## Validation

The Docker integration suite covers `HYBRID_SEARCH` against the directory namespace and a REST namespace backed by a directory namespace. The `Spark Search Docker` GitHub Actions workflow runs both backends for pull requests.
