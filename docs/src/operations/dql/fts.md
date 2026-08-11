# Full-Text Search

Query Lance tables using full-text search (FTS) with the `lance_match`, `lance_match_phrase`, and `lance_multi_match` SQL functions. These functions push FTS predicates down to the Lance inverted index for efficient text search.

!!! warning "Prerequisites"
    - The Lance Spark SQL extension must be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions).
    - The Lance catalog must be the session's default catalog (`spark.sql.defaultCatalog`). Spark resolves unqualified function names against the default catalog; if it points elsewhere, calls to `lance_match`, `lance_match_phrase`, and `lance_multi_match` fail with a "function not found" error.
    - An FTS index must exist on the target column(s). See [CREATE INDEX — FTS Options](../ddl/create-index.md#fts-options).
    - `lance_match_phrase` requires the FTS index to be built with `with_position = true`.

## lance_match

Keyword search against a single FTS-indexed column.

```sql
lance_match(column, query)
lance_match(column, query, 'key=value,...')
```

**Arguments:**

| Position | Name    | Type   | Description                        |
|----------|---------|--------|------------------------------------|
| 1        | column  | String | Column name with an FTS index.     |
| 2        | query   | String | Search query text.                 |
| 3        | options | String | Optional comma-separated key=value options. |

**Options:**

| Key              | Type    | Default | Constraints      | Description                                                    |
|------------------|---------|---------|------------------|----------------------------------------------------------------|
| `fuzziness`      | Integer | not set (auto) | ≥ 0       | Edit distance for fuzzy matching. When omitted, the engine determines fuzziness by token length. |
| `operator`       | String  | `OR`    | `AND` or `OR`    | Whether all terms must match (`AND`) or any term (`OR`).       |
| `boost`          | Float   | `1.0`   | any number       | Scoring multiplier (affects ranking, not row selection).        |
| `prefix_length`  | Integer | `0`     | ≥ 0              | Number of leading characters that must match exactly for fuzzy. |
| `max_expansions` | Integer | `50`    | ≥ 1              | Maximum fuzzy term expansions.                                 |

**Examples:**

```sql
-- Basic keyword search
SELECT * FROM lance.db.documents WHERE lance_match(body, 'machine learning');

-- Fuzzy search with AND operator
SELECT * FROM lance.db.documents
WHERE lance_match(body, 'machin lerning', 'fuzziness=1,operator=AND');

-- Boosted search with prefix length
SELECT * FROM lance.db.documents
WHERE lance_match(body, 'spark', 'boost=2.0,prefix_length=2');
```

## lance_match_phrase

Phrase search against a single FTS-indexed column. Returns rows where the query terms appear consecutively (or within a `slop` distance).

!!! note
    The FTS index must be built with `with_position = true` for phrase queries.

```sql
lance_match_phrase(column, query)
lance_match_phrase(column, query, slop)
```

**Arguments:**

| Position | Name   | Type    | Description                                              |
|----------|--------|---------|----------------------------------------------------------|
| 1        | column | String  | Column name with a positional FTS index.                 |
| 2        | query  | String  | Phrase to search for.                                    |
| 3        | slop   | Integer | Optional. Max positional distance between terms (default: 0 = exact). Must be ≥ 0. |

**Examples:**

```sql
-- Exact phrase match
SELECT * FROM lance.db.documents WHERE lance_match_phrase(body, 'machine learning');

-- Phrase match allowing 2 words between terms
SELECT * FROM lance.db.documents WHERE lance_match_phrase(body, 'deep networks', 2);
```

## lance_multi_match

Search across multiple FTS-indexed columns with a single query. A row matches if **any** column matches (OR, default) or **all** columns match (AND).

```sql
lance_multi_match(query, col1, col2, ...)
lance_multi_match(query, 'operator=AND|OR', col1, col2, ...)
```

**Arguments:**

| Position | Name     | Type   | Description                                                              |
|----------|----------|--------|--------------------------------------------------------------------------|
| 1        | query    | String | Search query text.                                                       |
| 2        | options  | String | Optional. `'operator=AND'` or `'operator=OR'`. If omitted, default is OR. |
| 2+       | columns  | String | Two or more column names, each with an FTS index.                        |

When the second argument is a string literal containing `=` with a recognized option key, it is treated as an options string. Otherwise it is treated as the first column name.

**Examples:**

```sql
-- Search across title and body (OR — row matches if either column matches)
SELECT * FROM lance.db.documents
WHERE lance_multi_match('machine learning', title, body);

-- AND operator — row must match in all columns
SELECT * FROM lance.db.documents
WHERE lance_multi_match('machine learning', 'operator=AND', title, body);
```

## Known Limitations

- **No global relevance ordering.** FTS predicates act as WHERE filters and return all matching rows. There is no `lance_score()` function or relevance-based `ORDER BY` — rows are returned in storage order. In particular, `SELECT * FROM t WHERE lance_match(...) LIMIT N` returns N matching rows determined by Spark task scheduling, not by BM25 rank — it is not a "top-N by relevance" query.
- **Column names must match the schema exactly.** Column references are resolved at planning time; aliases or expressions are not supported.
- **`lance_match_phrase` requires positional index.** The FTS index must be built with `with_position = true`. Without it, phrase queries will fail.
- **WHERE-filter uses full BM25 scoring (no WAND early stopping).** Every row matching the query is evaluated — `wand_factor` is not exposed because SQL WHERE semantics require returning all matching rows.
- **One FTS predicate per query.** Only a single FTS function call is allowed per `WHERE` clause. For multi-column search, use `lance_multi_match` instead of combining multiple `lance_match` calls with OR.
- **FTS predicates cannot appear inside OR.** `WHERE lance_match(a, 'x') OR other_condition` is not supported — OR semantics cannot be preserved when pushing a single FTS query to the scanner.

## Cost Model

Each Spark task scans one fragment and independently opens all committed FTS index segments to build a global BM25 scorer. Total segment-open cost per query is proportional to `N_fragments × M_index_segments`. For example, a 500-fragment dataset with 20 index segments causes 10,000 segment opens per query. Running `OPTIMIZE` periodically compacts data fragments, reducing the fragment count and therefore the total number of segment opens — this is the primary user-facing mitigation for latency-sensitive workloads.
