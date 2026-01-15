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
package org.lance.spark.internal;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ReadOptions;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.namespace.LanceNamespaceStorageOptionsProvider;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.read.LanceInputPartition;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LanceFragmentScanner implements AutoCloseable {
  private static LoadingCache<CacheKey, Map<Integer, Fragment>> LOADING_CACHE =
      CacheBuilder.newBuilder()
          .maximumSize(100)
          .expireAfterAccess(1, TimeUnit.HOURS)
          .build(
              new CacheLoader<CacheKey, Map<Integer, Fragment>>() {
                @Override
                public Map<Integer, Fragment> load(CacheKey key) throws Exception {
                  LanceSparkReadOptions readOptions = key.getReadOptions();

                  // Build ReadOptions with merged storage options and credential refresh provider
                  Map<String, String> merged =
                      LanceRuntime.mergeStorageOptions(
                          readOptions.getStorageOptions(), key.getInitialStorageOptions());
                  LanceNamespaceStorageOptionsProvider provider =
                      LanceRuntime.getOrCreateStorageOptionsProvider(
                          key.getNamespaceImpl(),
                          key.getNamespaceProperties(),
                          readOptions.getTableId());

                  ReadOptions.Builder builder = new ReadOptions.Builder().setStorageOptions(merged);
                  if (provider != null) {
                    builder.setStorageOptionsProvider(provider);
                  }

                  Dataset dataset =
                      Dataset.open()
                          .allocator(LanceRuntime.allocator())
                          .uri(readOptions.getDatasetUri())
                          .readOptions(builder.build())
                          .build();
                  return dataset.getFragments().stream()
                      .collect(Collectors.toMap(Fragment::getId, f -> f));
                }
              });
  private final LanceScanner scanner;
  private final int fragmentId;
  private final boolean withFragemtId;
  private final LanceInputPartition inputPartition;

  private LanceFragmentScanner(
      LanceScanner scanner,
      int fragmentId,
      boolean withFragmentId,
      LanceInputPartition inputPartition) {
    this.scanner = scanner;
    this.fragmentId = fragmentId;
    this.withFragemtId = withFragmentId;
    this.inputPartition = inputPartition;
  }

  public static LanceFragmentScanner create(int fragmentId, LanceInputPartition inputPartition) {
    try {
      LanceSparkReadOptions readOptions = inputPartition.getReadOptions();
      CacheKey key =
          new CacheKey(
              readOptions,
              inputPartition.getScanId(),
              inputPartition.getInitialStorageOptions(),
              inputPartition.getNamespaceImpl(),
              inputPartition.getNamespaceProperties());
      Map<Integer, Fragment> cachedFragments = LOADING_CACHE.get(key);
      Fragment fragment = cachedFragments.get(fragmentId);
      ScanOptions.Builder scanOptions = new ScanOptions.Builder();
      scanOptions.columns(getColumnNames(inputPartition.getSchema()));
      if (inputPartition.getWhereCondition().isPresent()) {
        scanOptions.filter(inputPartition.getWhereCondition().get());
      }
      scanOptions.batchSize(readOptions.getBatchSize());
      scanOptions.withRowId(getWithRowId(inputPartition.getSchema()));
      scanOptions.withRowAddress(getWithRowAddress(inputPartition.getSchema()));
      if (inputPartition.getLimit().isPresent()) {
        scanOptions.limit(inputPartition.getLimit().get());
      }
      if (inputPartition.getOffset().isPresent()) {
        scanOptions.offset(inputPartition.getOffset().get());
      }
      if (inputPartition.getTopNSortOrders().isPresent()) {
        scanOptions.setColumnOrderings(inputPartition.getTopNSortOrders().get());
      }
      boolean withFragmentId =
          inputPartition.getSchema().getFieldIndex(LanceConstant.FRAGMENT_ID).nonEmpty();
      return new LanceFragmentScanner(
          fragment.newScan(scanOptions.build()), fragmentId, withFragmentId, inputPartition);
    } catch (Throwable throwable) {
      throw new RuntimeException(throwable);
    }
  }

  /** @return the arrow reader. The caller is responsible for closing the reader */
  public ArrowReader getArrowReader() {
    return scanner.scanBatches();
  }

  @Override
  public void close() throws IOException {
    if (scanner != null) {
      try {
        scanner.close();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  public int fragmentId() {
    return fragmentId;
  }

  public boolean withFragemtId() {
    return withFragemtId;
  }

  public LanceInputPartition getInputPartition() {
    return inputPartition;
  }

  private static List<String> getColumnNames(StructType schema) {
    return Arrays.stream(schema.fields())
        .map(StructField::name)
        .filter(
            name ->
                !name.equals(LanceConstant.FRAGMENT_ID)
                    && !name.equals(LanceConstant.ROW_ID)
                    && !name.equals(LanceConstant.ROW_ADDRESS)
                    && !name.endsWith(LanceConstant.BLOB_POSITION_SUFFIX)
                    && !name.endsWith(LanceConstant.BLOB_SIZE_SUFFIX))
        .collect(Collectors.toList());
  }

  private static boolean getWithRowId(StructType schema) {
    return Arrays.stream(schema.fields())
        .map(StructField::name)
        .anyMatch(name -> name.equals(LanceConstant.ROW_ID));
  }

  private static boolean getWithRowAddress(StructType schema) {
    return Arrays.stream(schema.fields())
        .map(StructField::name)
        .anyMatch(name -> name.equals(LanceConstant.ROW_ADDRESS));
  }

  private static class CacheKey {
    private final LanceSparkReadOptions readOptions;
    private final String scanId;
    private final Map<String, String> initialStorageOptions;
    private final String namespaceImpl;
    private final Map<String, String> namespaceProperties;

    CacheKey(
        LanceSparkReadOptions readOptions,
        String scanId,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties) {
      this.readOptions = readOptions;
      this.scanId = scanId;
      this.initialStorageOptions = initialStorageOptions;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
    }

    public LanceSparkReadOptions getReadOptions() {
      return readOptions;
    }

    public Map<String, String> getInitialStorageOptions() {
      return initialStorageOptions;
    }

    public String getNamespaceImpl() {
      return namespaceImpl;
    }

    public Map<String, String> getNamespaceProperties() {
      return namespaceProperties;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      CacheKey cacheKey = (CacheKey) o;
      return Objects.equals(readOptions, cacheKey.readOptions)
          && Objects.equals(scanId, cacheKey.scanId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(readOptions, scanId);
    }
  }
}
