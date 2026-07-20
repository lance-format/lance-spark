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
  public void testWithVersionPropagatesFullTextQuery() {
    FullTextQuery fts = FullTextQuery.phrase("quick brown fox", "body", 1);
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").fullTextQuery(fts).build();

    LanceSparkReadOptions versioned = options.withVersion(42);
    Assertions.assertNotNull(
        versioned.getFullTextQuery(), "fullTextQuery must not be dropped by withVersion()");
    Assertions.assertEquals(42, versioned.getVersion());
    Assertions.assertTrue(
        org.lance.spark.utils.FullTextQueryUtils.equals(
            options.getFullTextQuery(), versioned.getFullTextQuery()));
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
  public void testWithVersionPropagatesMultiMatchQuery() {
    FullTextQuery fts = FullTextQuery.multiMatch("quick fox", Arrays.asList("title", "body"));
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").fullTextQuery(fts).build();

    LanceSparkReadOptions versioned = options.withVersion(7);
    Assertions.assertNotNull(
        versioned.getFullTextQuery(), "fullTextQuery must not be dropped by withVersion()");
    Assertions.assertEquals(7, versioned.getVersion());
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
  public void testExecutorCredentialRefreshPreservedByWithVersion() {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path")
            .executorCredentialRefresh(false)
            .build();

    LanceSparkReadOptions pinned = options.withVersion(7);
    Assertions.assertFalse(
        pinned.isExecutorCredentialRefresh(),
        "withVersion() must propagate the executor_credential_refresh flag");
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
}
