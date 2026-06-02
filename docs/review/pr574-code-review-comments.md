# PR #574 Code Review Comments

Source PR: https://github.com/lance-format/lance-spark/pull/574

This document records confirmed handling decisions for review comments on PR #574.
Unconfirmed comments are intentionally not recorded as decisions yet.

## 1. Deduplicate `_distance` Constant Definitions

- Review comment: https://github.com/lance-format/lance-spark/pull/574#discussion_r3341472391
- Reviewer: `LuciferYang`
- File: `lance-spark-base_2.12/src/main/java/org/lance/spark/LanceConstant.java`
- Comment summary: `_distance` is defined in `LanceConstant.java`, `LanceFragmentScanner.java`, and `LanceTableValuedFunctions.scala`.
- Local verification:
  - `LanceConstant.VECTOR_DISTANCE` already defines the shared `_distance` column name.
  - `LanceFragmentScanner` still has a duplicate private `VECTOR_DISTANCE_COLUMN`.
  - `LanceTableValuedFunctions` still has a duplicate private `VECTOR_DISTANCE`.
- Decision: Accepted.
- Proposed solution:
  - Remove local `_distance` constants from `LanceFragmentScanner` and `LanceTableValuedFunctions`.
  - Use `LanceConstant.VECTOR_DISTANCE` everywhere.
  - In Scala, import `org.lance.spark.LanceConstant` and reference the shared constant.

## 2. Use Spark Catalog Resolution Instead of a Hand-Written Matcher

- Review comment: https://github.com/lance-format/lance-spark/pull/574#discussion_r3341493321
- Reviewer: `LuciferYang`
- File: `lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/LanceTableValuedFunctions.scala`
- Comment summary: Replace the custom multipart table-name matcher with Spark's `LookupCatalog.CatalogAndIdentifier` so `vector_search` follows Spark's current catalog and current namespace resolution rules.
- Local verification:
  - `resolveCatalogTable` still manually handles `Seq(table)`, `Seq(namespaceOrCatalog, table)`, and longer multipart identifiers.
  - The current helper probes catalog names through `tableCatalog`, which duplicates Spark catalog resolution behavior.
  - `LookupCatalog.CatalogAndIdentifier` exists in the local Spark 3.4, 3.5, 4.0, and 4.1 catalyst artifacts.
  - `LookupCatalog` is `private[sql]`, and this Scala file is under `org.apache.spark.sql`, so it can access the trait.
- Decision: Accepted.
- Proposed solution:
  - Replace the manual multipart matching in `resolveCatalogTable` with a local `LookupCatalog` instance bound to `spark.sessionState.catalogManager`.
  - Use `lookup.CatalogAndIdentifier(catalog, ident)` to obtain Spark's standard `(CatalogPlugin, Identifier)` resolution.
  - Require the resolved catalog to be a `TableCatalog`; otherwise fail with a clear error.
  - Remove the now-unneeded `tableCatalog` helper and `NonFatal` import.

## 3. Expose `useIndex` and `distanceType` for SQL Vector Search

- Review comment: https://github.com/lance-format/lance-spark/pull/574#discussion_r3341501852
- Reviewer: `LuciferYang`
- File: `lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/LanceTableValuedFunctions.scala`
- Comment summary: `vector_search` currently forces `useIndex=true` and leaves `distanceType` unset, which can silently return approximate results or use the wrong distance metric.
- Local verification:
  - `createNearestQuery` always calls `builder.setUseIndex(true)`.
  - `createNearestQuery` never calls `builder.setDistanceType(...)`.
  - Existing `nearest` JSON serialization already supports both `useIndex` and `distanceType`.
  - `DistanceType` supports `L2`, `Cosine`, `Dot`, and `Hamming`.
- Decision: Accepted.
- Proposed solution:
  - Extend SQL TVF signatures to support optional arguments:
    - `vector_search(table, column, query_vector, limit)`
    - `vector_search(table, column, query_vector, limit, distance_type)`
    - `vector_search(table, column, query_vector, limit, distance_type, use_index)`
  - Default `distance_type` to `L2` and set it explicitly on `Query.Builder`.
  - Default `use_index` to `false` so SQL vector search returns exact results unless ANN is explicitly requested.
  - Parse `distance_type` case-insensitively and fail with a clear error for unsupported values.
  - Update SQL documentation with the new signature, defaults, and examples.
  - Add tests for exact-search default, explicit ANN search, supported distance types, and invalid distance type handling.

## 4. Validate Vector Column Type During Resolution

- Review comment: https://github.com/lance-format/lance-spark/pull/574#discussion_r3341516853
- Reviewer: `LuciferYang`
- File: `lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/LanceTableValuedFunctions.scala`
- Comment summary: `vector_search` checks that the requested column exists but does not validate that it is a Lance vector column, nor that the query vector length matches the vector dimension.
- Local verification:
  - `resolveVectorSearch` currently checks only column existence.
  - `VectorUtils.isVectorField` and `VectorUtils.getVectorDimension` are available helpers.
- Decision: Rejected.
- Proposed solution:
  - Ignore this review comment for now.
  - Do not add `VectorUtils.isVectorField` validation.
  - Do not add query-vector dimension validation.
  - Do not add the related negative tests.

## 5. Reject Non-Integral Limit Literals

- Review comment: https://github.com/lance-format/lance-spark/pull/574#discussion_r3341550163
- Reviewer: `LuciferYang`
- File: `lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/LanceTableValuedFunctions.scala`
- Comment summary: `extractLimit` accepts any `java.lang.Number`, so floating-point limits such as `10.5f` or `10.5d` are truncated through `longValue()`.
- Local verification:
  - `extractLimit` accepts `Byte`, `Short`, `Integer`, `Long`, and then all remaining `java.lang.Number` values.
  - The current tests cover only a non-positive limit, not a non-integral limit.
- Decision: Rejected.
- Proposed solution:
  - Ignore this review comment for now.
  - Do not change `extractLimit`.
  - Do not add negative tests for floating-point or otherwise non-integral limit literals.

## 6. Add Validation Test Coverage

- Review comment: https://github.com/lance-format/lance-spark/pull/574#discussion_r3341536423
- Reviewer: `LuciferYang`
- File: `lance-spark-base_2.12/src/test/java/org/lance/spark/read/BaseSparkConnectorReadWithVectorSearchTest.java`
- Comment summary: The SQL vector search validation logic has only one negative test and should cover more error cases, with assertions based on message substrings rather than exact exception classes.
- Local verification:
  - The review comment mentions `BaseSparkConnectorSqlVectorSearchTest.java`, but the current PR head uses `BaseSparkConnectorReadWithVectorSearchTest.java`.
  - Current negative coverage only checks `limit = 0`.
  - Several suggested tests overlap with review comments that were rejected separately, including vector-column validation and non-integral limit rejection.
- Decision: Record only; no implementation changes.
- Proposed solution:
  - Do not add or modify tests for this review comment at this stage.
  - Keep this comment recorded as pending context only.

## 7. Analyzer Resolution Performs Table I/O

- Review comment: https://github.com/lance-format/lance-spark/pull/574#discussion_r3341563692
- Reviewer: `LuciferYang`
- File: `lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/LanceTableValuedFunctions.scala`
- Comment summary: Vector-search resolution can perform real I/O while loading table schema during analyzer resolution.
- Local verification:
  - The path-specific `resolvePathTable` branch mentioned in the review comment is no longer present in the current PR head.
  - `resolveCatalogTable` still calls `catalog.loadTable(ident)` during analyzer resolution.
- Decision: Rejected.
- Proposed solution:
  - Ignore this review comment.
  - Do not change analyzer resolution or add caching/lazy schema behavior for this PR.

## 8. Avoid Path Heuristics for Table Identifier Resolution

- Review comment: https://github.com/lance-format/lance-spark/pull/574#discussion_r3341577514
- Reviewer: `LuciferYang`
- File: `lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/LanceTableValuedFunctions.scala`
- Comment summary: The earlier `looksLikePath` heuristic could misclassify quoted catalog identifiers containing `/` as paths, and could treat relative dataset names without `/` as catalog identifiers.
- Local verification:
  - `looksLikePath` and `resolvePathTable` are no longer present in the current PR head.
  - `vector_search` currently resolves the table argument through catalog identifier parsing.
  - The docs still mention direct path usage, but this review comment is being ignored for now.
- Decision: Rejected.
- Proposed solution:
  - Ignore this review comment.
  - Do not change identifier/path handling.
  - Do not update the direct-path documentation as part of this review-response pass.
