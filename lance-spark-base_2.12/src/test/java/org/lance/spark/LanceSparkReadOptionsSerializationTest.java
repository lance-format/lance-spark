/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark;

import org.lance.ipc.FullTextQuery;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class LanceSparkReadOptionsSerializationTest {

  @Test
  public void testFullTextQuerySerializationRoundTrip() throws IOException, ClassNotFoundException {
    FullTextQuery fts =
        FullTextQuery.match(
            "search term", "body", 1.0f, Optional.of(1), 50, FullTextQuery.Operator.OR, 0);

    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").fullTextQuery(fts).build();

    Assertions.assertNotNull(options.getFullTextQuery());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(options);
    oos.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    LanceSparkReadOptions deserialized = (LanceSparkReadOptions) ois.readObject();

    Assertions.assertNotNull(
        deserialized.getFullTextQuery(), "fullTextQuery should not be null after deserialization");
    FullTextQuery rtFts = deserialized.getFullTextQuery();
    Assertions.assertEquals(FullTextQuery.Type.MATCH, rtFts.getType());
    FullTextQuery.MatchQuery mq = (FullTextQuery.MatchQuery) rtFts;
    Assertions.assertEquals("search term", mq.getQueryText());
    Assertions.assertEquals("body", mq.getColumn());
  }

  @Test
  public void testWithRefPropagatesFullTextQuery() {
    FullTextQuery fts = FullTextQuery.phrase("quick brown fox", "body", 1);
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").fullTextQuery(fts).build();

    LanceSparkReadOptions versioned = options.withRef(LanceRef.ofMain(42));
    Assertions.assertNotNull(
        versioned.getFullTextQuery(), "fullTextQuery must not be dropped by withRef()");
    Assertions.assertEquals(42, versioned.getRef().getVersionNumber().get());
    Assertions.assertTrue(
        org.lance.spark.utils.FullTextQueryUtils.equals(
            options.getFullTextQuery(), versioned.getFullTextQuery()));
  }

  @Test
  public void testFromOptionsParsesBranch() {
    for (String key : Arrays.asList("branch", "BrAnCh")) {
      LanceSparkReadOptions options =
          LanceSparkReadOptions.from(Collections.singletonMap(key, "audit"), "s3://bucket/path");
      Assertions.assertEquals(LanceRef.ofBranch("audit"), options.getRef());
    }
  }

  @Test
  public void testFromOptionsParsesTag() {
    for (String key : Arrays.asList("tag", "TaG", "TAG")) {
      Map<String, String> properties = new HashMap<>();
      properties.put(key, "stable");
      properties.put("CustomStorageKey", "value");
      LanceSparkReadOptions options = LanceSparkReadOptions.from(properties, "s3://bucket/path");
      Assertions.assertEquals(LanceRef.ofTag("stable"), options.getRef());
      Assertions.assertEquals(properties, options.getStorageOptions());
    }
  }

  @Test
  public void testFromOptionsParsesMixedCaseVersion() {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.from(Collections.singletonMap("VeRsIoN", "2"), "s3://bucket/path");
    Assertions.assertEquals(LanceRef.ofMain(2), options.getRef());
  }

  @Test
  public void testFromOptionsRejectsEmptyOrConflictingTag() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            LanceSparkReadOptions.from(
                Collections.singletonMap(LanceSparkReadOptions.CONFIG_TAG, ""),
                "s3://bucket/path"));
    for (String tag : Arrays.asList("tag", "TaG")) {
      for (String other : Arrays.asList("branch", "BrAnCh", "version", "VERSION")) {
        Map<String, String> options = new HashMap<>();
        options.put(tag, "stable");
        options.put(other, "2");
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> LanceSparkReadOptions.from(options, "s3://bucket/path"));
      }
    }
  }

  @Test
  public void testFromOptionsRejectsBranchAndVersion() {
    Map<String, String> options = new HashMap<>();
    options.put(LanceSparkReadOptions.CONFIG_BRANCH, "audit");
    options.put(LanceSparkReadOptions.CONFIG_VERSION, "2");

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> LanceSparkReadOptions.from(options, "s3://bucket/path"));
    Assertions.assertTrue(exception.getMessage().contains("branch"));
    Assertions.assertTrue(exception.getMessage().contains("version"));
  }

  @Test
  public void testFromOptionsParsesFtsSubtype() {
    FullTextQuery original = FullTextQuery.phrase("hello world", "body", 0);
    String json = org.lance.spark.utils.FullTextQueryUtils.fullTextQueryToString(original);

    Map<String, String> props = new HashMap<>();
    props.put("path", "s3://bucket/path");
    props.put(LanceSparkReadOptions.CONFIG_FULL_TEXT_QUERY, json);

    LanceSparkReadOptions opts = LanceSparkReadOptions.from(props);
    Assertions.assertNotNull(opts.getFullTextQuery());
    Assertions.assertEquals(FullTextQuery.Type.MATCH_PHRASE, opts.getFullTextQuery().getType());
    FullTextQuery.PhraseQuery pq = (FullTextQuery.PhraseQuery) opts.getFullTextQuery();
    Assertions.assertEquals("hello world", pq.getQueryText());
    Assertions.assertEquals("body", pq.getColumn());
  }

  @Test
  public void testMultiMatchQuerySerializationRoundTrip()
      throws IOException, ClassNotFoundException {
    FullTextQuery fts = FullTextQuery.multiMatch("hello world", Arrays.asList("body", "title"));

    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").fullTextQuery(fts).build();

    Assertions.assertNotNull(options.getFullTextQuery());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(options);
    oos.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    LanceSparkReadOptions deserialized = (LanceSparkReadOptions) ois.readObject();

    Assertions.assertNotNull(
        deserialized.getFullTextQuery(), "fullTextQuery must not be null after deserialization");
    Assertions.assertEquals(
        FullTextQuery.Type.MULTI_MATCH, deserialized.getFullTextQuery().getType());
    FullTextQuery.MultiMatchQuery mmq =
        (FullTextQuery.MultiMatchQuery) deserialized.getFullTextQuery();
    Assertions.assertEquals("hello world", mmq.getQueryText());
    Assertions.assertEquals(Arrays.asList("body", "title"), mmq.getColumns());
    Assertions.assertEquals(FullTextQuery.Operator.OR, mmq.getOperator());
    Assertions.assertFalse(
        mmq.getBoosts().isPresent(), "boosts must be absent for default multiMatch");
  }

  @Test
  public void testWithRefPropagatesMultiMatchQuery() {
    FullTextQuery fts = FullTextQuery.multiMatch("quick fox", Arrays.asList("title", "body"));
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").fullTextQuery(fts).build();

    LanceSparkReadOptions versioned = options.withRef(LanceRef.ofMain(7));
    Assertions.assertNotNull(
        versioned.getFullTextQuery(), "fullTextQuery must not be dropped by withRef()");
    Assertions.assertEquals(7, versioned.getRef().getVersionNumber().get());
    Assertions.assertTrue(
        org.lance.spark.utils.FullTextQueryUtils.equals(
            options.getFullTextQuery(), versioned.getFullTextQuery()));
  }

  @Test
  public void testFromOptionsParsesFtsMultiMatchSubtype() {
    FullTextQuery original = FullTextQuery.multiMatch("search term", Arrays.asList("col1", "col2"));
    String json = org.lance.spark.utils.FullTextQueryUtils.fullTextQueryToString(original);

    Map<String, String> props = new HashMap<>();
    props.put("path", "s3://bucket/path");
    props.put(LanceSparkReadOptions.CONFIG_FULL_TEXT_QUERY, json);

    LanceSparkReadOptions opts = LanceSparkReadOptions.from(props);
    Assertions.assertNotNull(opts.getFullTextQuery());
    Assertions.assertEquals(FullTextQuery.Type.MULTI_MATCH, opts.getFullTextQuery().getType());
    FullTextQuery.MultiMatchQuery mmq = (FullTextQuery.MultiMatchQuery) opts.getFullTextQuery();
    Assertions.assertEquals("search term", mmq.getQueryText());
    Assertions.assertEquals(Arrays.asList("col1", "col2"), mmq.getColumns());
    Assertions.assertEquals(FullTextQuery.Operator.OR, mmq.getOperator());
  }

  @Test
  public void testFtsEqualsAndHashCodeStabilityAcrossConstructionPaths() {
    FullTextQuery fts1 =
        FullTextQuery.match(
            "hello", "body", 1.5f, Optional.of(1), 50, FullTextQuery.Operator.AND, 2);
    FullTextQuery fts2 =
        FullTextQuery.match(
            "hello", "body", 1.5f, Optional.of(1), 50, FullTextQuery.Operator.AND, 2);

    LanceSparkReadOptions opts1 =
        LanceSparkReadOptions.builder().datasetUri("s3://b/p").fullTextQuery(fts1).build();
    LanceSparkReadOptions opts2 =
        LanceSparkReadOptions.builder().datasetUri("s3://b/p").fullTextQuery(fts2).build();

    Assertions.assertEquals(opts1, opts2, "Two instances with identical MatchQuery must be equal");
    Assertions.assertEquals(
        opts1.hashCode(), opts2.hashCode(), "Equal instances must have equal hashCodes");

    FullTextQuery fts3 = FullTextQuery.match("world", "body");
    LanceSparkReadOptions opts3 =
        LanceSparkReadOptions.builder().datasetUri("s3://b/p").fullTextQuery(fts3).build();
    Assertions.assertNotEquals(opts1, opts3);
  }

  @Test
  public void testExecutorCredentialRefreshDefaultsToTrue() {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").build();
    Assertions.assertTrue(
        options.isExecutorCredentialRefresh(),
        "executor_credential_refresh must default to true to preserve existing behavior");
  }

  @Test
  public void testRuntimeFilterGuardrailDefaultsAndOverrides() {
    LanceSparkReadOptions defaults =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").build();
    Assertions.assertEquals(100_000, defaults.getRuntimeFilterMaxKeys());
    Assertions.assertEquals(4_194_304, defaults.getRuntimeFilterMaxBytes());

    Map<String, String> configured = new HashMap<>();
    configured.put(LanceSparkReadOptions.CONFIG_RUNTIME_FILTER_MAX_KEYS, "25");
    configured.put(LanceSparkReadOptions.CONFIG_RUNTIME_FILTER_MAX_BYTES, "4096");
    LanceSparkReadOptions overrides = LanceSparkReadOptions.from(configured, "s3://bucket/path");
    Assertions.assertEquals(25, overrides.getRuntimeFilterMaxKeys());
    Assertions.assertEquals(4096, overrides.getRuntimeFilterMaxBytes());

    LanceSparkReadOptions pinned = overrides.withRef(LanceRef.ofMain(3));
    Assertions.assertEquals(25, pinned.getRuntimeFilterMaxKeys());
    Assertions.assertEquals(4096, pinned.getRuntimeFilterMaxBytes());
  }

  @Test
  public void testRuntimeFilterGuardrailsMustBePositive() {
    Map<String, String> invalidKeys =
        Collections.singletonMap(LanceSparkReadOptions.CONFIG_RUNTIME_FILTER_MAX_KEYS, "0");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> LanceSparkReadOptions.from(invalidKeys, "s3://bucket/path"));

    Map<String, String> invalidBytes =
        Collections.singletonMap(LanceSparkReadOptions.CONFIG_RUNTIME_FILTER_MAX_BYTES, "-1");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> LanceSparkReadOptions.from(invalidBytes, "s3://bucket/path"));
  }

  @Test
  public void testExecutorCredentialRefreshParsedFromOptions() {
    LanceSparkReadOptions optionsFalse =
        LanceSparkReadOptions.from(
            Collections.singletonMap(
                LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH, "false"),
            "s3://bucket/path");
    Assertions.assertFalse(optionsFalse.isExecutorCredentialRefresh());

    LanceSparkReadOptions optionsTrue =
        LanceSparkReadOptions.from(
            Collections.singletonMap(
                LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH, "true"),
            "s3://bucket/path");
    Assertions.assertTrue(optionsTrue.isExecutorCredentialRefresh());
  }

  @Test
  public void testDeprecatedNearestReadOptionFailsFast() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                LanceSparkReadOptions.from(
                    Collections.singletonMap("nearest", "{\"column\":\"vector\"}"),
                    "s3://bucket/path"));

    Assertions.assertTrue(exception.getMessage().contains("nearest"));
    Assertions.assertTrue(exception.getMessage().contains("VECTOR_SEARCH"));
  }

  @Test
  public void testExecutorCredentialRefreshSurvivesSerialization()
      throws IOException, ClassNotFoundException {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path")
            .executorCredentialRefresh(false)
            .build();
    Assertions.assertFalse(options.isExecutorCredentialRefresh());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(options);
    }
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    LanceSparkReadOptions deserialized;
    try (ObjectInputStream ois = new ObjectInputStream(bais)) {
      deserialized = (LanceSparkReadOptions) ois.readObject();
    }

    Assertions.assertFalse(
        deserialized.isExecutorCredentialRefresh(),
        "executor_credential_refresh must survive Java serialization (driver -> executor handoff)");
  }

  @Test
  public void testCacheBackendConfigurationSurvivesSerialization()
      throws IOException, ClassNotFoundException {
    Map<String, String> catalogOpts = new HashMap<>();
    catalogOpts.put("index_cache_backend", "moka://?capacity=1048576");
    catalogOpts.put("metadata_cache_backend", "moka://?capacity=524288");
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path")
            .catalogName("cache-catalog")
            .withCatalogDefaults(LanceSparkCatalogConfig.from(catalogOpts))
            .build();

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (ObjectOutputStream objectOutput = new ObjectOutputStream(output)) {
      objectOutput.writeObject(options);
    }

    LanceSparkReadOptions deserialized;
    try (ObjectInputStream objectInput =
        new ObjectInputStream(new ByteArrayInputStream(output.toByteArray()))) {
      deserialized = (LanceSparkReadOptions) objectInput.readObject();
    }

    Assertions.assertEquals("cache-catalog", deserialized.getCatalogName());
    Assertions.assertEquals("moka://?capacity=1048576", deserialized.getIndexCacheBackend());
    Assertions.assertEquals("moka://?capacity=524288", deserialized.getMetadataCacheBackend());
  }

  @Test
  public void testWithRefPreservesCacheBackendConfiguration() {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path")
            .catalogName("cache-catalog")
            .indexCacheBackend("moka://?capacity=1048576")
            .metadataCacheBackend("moka://?capacity=524288")
            .build();

    LanceSparkReadOptions pinned = options.withRef(LanceRef.ofMain(7));

    Assertions.assertEquals("cache-catalog", pinned.getCatalogName());
    Assertions.assertEquals("moka://?capacity=1048576", pinned.getIndexCacheBackend());
    Assertions.assertEquals("moka://?capacity=524288", pinned.getMetadataCacheBackend());
  }

  @Test
  public void testExecutorCredentialRefreshPreservedByWithRef() {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path")
            .executorCredentialRefresh(false)
            .build();

    LanceSparkReadOptions pinned = options.withRef(LanceRef.ofMain(7));
    Assertions.assertFalse(
        pinned.isExecutorCredentialRefresh(),
        "withRef() must propagate the executor_credential_refresh flag");
  }

  @Test
  public void testExecutorCredentialRefreshFromCatalogDefaults() {
    Map<String, String> catalogOpts = new HashMap<>();
    catalogOpts.put(LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH, "false");
    LanceSparkCatalogConfig catalogConfig = LanceSparkCatalogConfig.from(catalogOpts);

    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path")
            .withCatalogDefaults(catalogConfig)
            .build();

    Assertions.assertFalse(
        options.isExecutorCredentialRefresh(),
        "executor_credential_refresh set at catalog level must land in the typed field "
            + "so it takes effect for SELECT without .option(...) and for SQL DML");
  }

  @Test
  public void testPerReadOptionOverridesCatalogDefaults() {
    Map<String, String> catalogOpts = new HashMap<>();
    catalogOpts.put(LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH, "false");
    LanceSparkCatalogConfig catalogConfig = LanceSparkCatalogConfig.from(catalogOpts);

    Map<String, String> merged = new HashMap<>(catalogConfig.getStorageOptions());
    merged.put(LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH, "true");
    merged.put(LanceSparkReadOptions.CONFIG_USE_SCALAR_INDEX, "false");

    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path")
            .withCatalogDefaults(catalogConfig)
            .fromOptions(merged)
            .build();

    Assertions.assertTrue(
        options.isExecutorCredentialRefresh(),
        "per-read .option(...) must override the catalog-level default");

    Assertions.assertFalse(
        options.isUseScalarIndex(),
        "per-read .option(...) must override the catalog-level default");
  }

  @Test
  public void testUseScalarIndexFromCatalogDefaults() {
    Map<String, String> catalogOpts = new HashMap<>();
    LanceSparkCatalogConfig catalogConfig = LanceSparkCatalogConfig.from(catalogOpts);

    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path")
            .withCatalogDefaults(catalogConfig)
            .build();

    Assertions.assertTrue(
        options.isUseScalarIndex(), "use_scalar_index default value must be true");
  }

  @Test
  public void testUseScalarIndexFromOptions() {
    Map<String, String> properties = new HashMap<>();
    properties.put(LanceSparkReadOptions.CONFIG_DATASET_URI, "s3://bucket/path");
    properties.put(LanceSparkReadOptions.CONFIG_USE_SCALAR_INDEX, "false");

    LanceSparkReadOptions options = LanceSparkReadOptions.from(properties);
    Assertions.assertFalse(options.isUseScalarIndex());
  }

  @Test
  public void testUseScalarIndexSerialization() throws IOException, ClassNotFoundException {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path")
            .useScalarIndex(false)
            .build();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(options);
    oos.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    LanceSparkReadOptions deserializedOptions = (LanceSparkReadOptions) ois.readObject();

    Assertions.assertFalse(deserializedOptions.isUseScalarIndex());
  }

  @Test
  public void testStripSurroundingQuotes() {
    Assertions.assertNull(LanceSparkReadOptions.stripSurroundingQuotes(null));
    Assertions.assertEquals("", LanceSparkReadOptions.stripSurroundingQuotes(""));
    Assertions.assertEquals("", LanceSparkReadOptions.stripSurroundingQuotes("''"));
    Assertions.assertEquals("", LanceSparkReadOptions.stripSurroundingQuotes("\"\""));
    Assertions.assertEquals("false", LanceSparkReadOptions.stripSurroundingQuotes("'false'"));
    Assertions.assertEquals("false", LanceSparkReadOptions.stripSurroundingQuotes("\"false\""));
    Assertions.assertEquals("4096", LanceSparkReadOptions.stripSurroundingQuotes("  '4096'  "));
    Assertions.assertEquals(
        "'unmatched", LanceSparkReadOptions.stripSurroundingQuotes("'unmatched"));
  }

  @Test
  public void testQuotedOptionValuesAreParsedCleanly() {
    Map<String, String> readProps = new HashMap<>();
    readProps.put(LanceSparkReadOptions.CONFIG_DATASET_URI, "s3://bucket/path");
    readProps.put(LanceSparkReadOptions.CONFIG_BATCH_SIZE, "'4096'");
    readProps.put(LanceSparkReadOptions.CONFIG_BLOCK_SIZE, "\"65536\"");
    readProps.put(LanceSparkReadOptions.CONFIG_USE_SCALAR_INDEX, "'false'");
    readProps.put(LanceSparkReadOptions.CONFIG_PUSH_DOWN_FILTERS, "\"false\"");
    readProps.put(LanceSparkReadOptions.CONFIG_TOP_N_PUSH_DOWN, "'false'");
    readProps.put(LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH, "'false'");
    readProps.put(LanceSparkReadOptions.CONFIG_ARROW_BATCH_READS, "'true'");
    readProps.put(LanceSparkReadOptions.CONFIG_VERSION, "'7'");

    LanceSparkReadOptions readOptions = LanceSparkReadOptions.from(readProps);
    Assertions.assertEquals(4096, readOptions.getBatchSize());
    Assertions.assertEquals(Integer.valueOf(65536), readOptions.getBlockSize());
    Assertions.assertFalse(readOptions.isUseScalarIndex());
    Assertions.assertFalse(readOptions.isPushDownFilters());
    Assertions.assertFalse(readOptions.isTopNPushDown());
    Assertions.assertFalse(readOptions.isExecutorCredentialRefresh());
    Assertions.assertTrue(readOptions.isArrowBatchReadsEnabled());
    Assertions.assertEquals(LanceRef.ofMain(7L), readOptions.getRef());

    Map<String, String> writeProps = new HashMap<>();
    writeProps.put(LanceSparkWriteOptions.CONFIG_WRITE_MODE, "'OVERWRITE'");
    writeProps.put(LanceSparkWriteOptions.CONFIG_BATCH_SIZE, "'2048'");
    writeProps.put(LanceSparkWriteOptions.CONFIG_MAX_ROWS_PER_FILE, "\"10000\"");
    writeProps.put(LanceSparkWriteOptions.CONFIG_ENABLE_STABLE_ROW_IDS, "'true'");
    writeProps.put(LanceSparkWriteOptions.CONFIG_FILE_FORMAT_VERSION, "'2.1'");

    LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.from(writeProps, "s3://bucket/path");
    Assertions.assertTrue(writeOptions.isOverwrite());
    Assertions.assertEquals(2048, writeOptions.getBatchSize());
    Assertions.assertEquals(Integer.valueOf(10000), writeOptions.getMaxRowsPerFile());
    Assertions.assertEquals(Boolean.TRUE, writeOptions.getEnableStableRowIds());
    Assertions.assertEquals("2.1", writeOptions.getFileFormatVersion());
  }

  @Test
  public void testSparkSqlSetSessionConfigsWithQuotes() {
    SparkSession spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("lance-session-config-quote-test")
            .getOrCreate();
    try {
      spark.sql("SET spark.sql.lance.batch_size = '4096'");
      spark.sql("SET spark.sql.lance.use_scalar_index = 'false'");
      spark.sql("SET spark.sql.lance.pushDownFilters = \"false\"");
      spark.sql("SET spark.sql.lance.topN_push_down = 'false'");
      spark.sql("SET spark.sql.lance.max_row_per_file = '5000'");
      // Non-tuning options must be ignored at session level
      spark.sql("SET spark.sql.lance.version = '2'");
      spark.sql("SET spark.sql.lance.branch = 'dev'");
      spark.sql("SET spark.sql.lance.write_mode = 'overwrite'");
      spark.sql("SET spark.sql.lance.file_format_version = '2.0'");
      spark.sql("SET spark.sql.lance.enable_stable_row_ids = 'true'");

      // 1. Session tuning configs override catalog defaults, while non-tuning session keys are
      // ignored
      Map<String, String> catalogOpts = new HashMap<>();
      catalogOpts.put(LanceSparkReadOptions.CONFIG_BATCH_SIZE, "1024");
      catalogOpts.put(LanceSparkReadOptions.CONFIG_USE_SCALAR_INDEX, "true");
      catalogOpts.put(LanceSparkWriteOptions.CONFIG_MAX_ROWS_PER_FILE, "1000");
      LanceSparkCatalogConfig catalogConfig = LanceSparkCatalogConfig.from(catalogOpts);

      LanceSparkReadOptions fromCatalog =
          LanceSparkReadOptions.builder()
              .datasetUri("s3://bucket/path")
              .withCatalogDefaults(catalogConfig)
              .build();
      Assertions.assertEquals(4096, fromCatalog.getBatchSize());
      Assertions.assertFalse(fromCatalog.isUseScalarIndex());
      Assertions.assertFalse(fromCatalog.isPushDownFilters());
      Assertions.assertFalse(fromCatalog.isTopNPushDown());
      Assertions.assertNull(fromCatalog.getRef());

      LanceSparkWriteOptions writeFromCatalog =
          LanceSparkWriteOptions.builder()
              .datasetUri("s3://bucket/path")
              .withCatalogDefaults(catalogConfig)
              .build();
      Assertions.assertEquals(4096, writeFromCatalog.getBatchSize());
      Assertions.assertEquals(Integer.valueOf(5000), writeFromCatalog.getMaxRowsPerFile());
      Assertions.assertFalse(writeFromCatalog.isOverwrite());
      Assertions.assertNull(writeFromCatalog.getFileFormatVersion());
      Assertions.assertNull(writeFromCatalog.getEnableStableRowIds());

      // 2. Explicit per-read / per-write options override session configs (including mixed case)
      Map<String, String> perReadOpts = new HashMap<>();
      perReadOpts.put("BATCH_SIZE", "2048");
      perReadOpts.put("USE_SCALAR_INDEX", "true");
      LanceSparkReadOptions overridden =
          LanceSparkReadOptions.builder()
              .datasetUri("s3://bucket/path")
              .fromOptions(perReadOpts)
              .withCatalogDefaults(catalogConfig)
              .build();
      Assertions.assertEquals(2048, overridden.getBatchSize());
      Assertions.assertTrue(overridden.isUseScalarIndex());
      Assertions.assertFalse(overridden.isPushDownFilters());

      Map<String, String> perWriteOpts = new HashMap<>();
      perWriteOpts.put("BATCH_SIZE", "512");
      perWriteOpts.put("MAX_ROW_PER_FILE", "2500");
      LanceSparkWriteOptions overriddenWrite =
          LanceSparkWriteOptions.builder()
              .datasetUri("s3://bucket/path")
              .fromOptions(perWriteOpts)
              .withCatalogDefaults(catalogConfig)
              .build();
      Assertions.assertEquals(512, overriddenWrite.getBatchSize());
      Assertions.assertEquals(Integer.valueOf(2500), overriddenWrite.getMaxRowsPerFile());
    } finally {
      spark.conf().unset("spark.sql.lance.batch_size");
      spark.conf().unset("spark.sql.lance.use_scalar_index");
      spark.conf().unset("spark.sql.lance.pushDownFilters");
      spark.conf().unset("spark.sql.lance.topN_push_down");
      spark.conf().unset("spark.sql.lance.max_row_per_file");
      spark.conf().unset("spark.sql.lance.version");
      spark.conf().unset("spark.sql.lance.branch");
      spark.conf().unset("spark.sql.lance.write_mode");
      spark.conf().unset("spark.sql.lance.file_format_version");
      spark.conf().unset("spark.sql.lance.enable_stable_row_ids");
      spark.stop();
    }
  }
}
