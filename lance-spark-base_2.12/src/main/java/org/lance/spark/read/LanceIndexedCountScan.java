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
package org.lance.spark.read;

import org.lance.Dataset;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.internal.ExecutorNamespace;
import org.lance.spark.utils.Utils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * LocalScan copies its rows into the physical plan, so it would run the count during planning. The
 * filtered scanner can build a row-address mask on this one task.
 */
public class LanceIndexedCountScan implements Scan, Batch {
  static final String PLAN_MARKER = "LanceIndexedCount";

  private final LanceSparkReadOptions readOptions;
  private final String indexName;
  private final String filter;
  private final Map<String, String> initialStorageOptions;
  private final String namespaceImpl;
  private final Map<String, String> namespaceProperties;

  LanceIndexedCountScan(
      LanceSparkReadOptions readOptions,
      String indexName,
      String filter,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties) {
    this.readOptions = readOptions;
    this.indexName = indexName;
    this.filter = filter;
    this.initialStorageOptions =
        initialStorageOptions == null ? Collections.emptyMap() : initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties =
        namespaceProperties == null ? Collections.emptyMap() : namespaceProperties;
  }

  @Override
  public StructType readSchema() {
    return new StructType().add("count", DataTypes.LongType);
  }

  @Override
  public String description() {
    return PLAN_MARKER + "[" + readOptions.getDatasetUri() + "]";
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    return new InputPartition[] {
      new IndexedCountPartition(
          readOptions, indexName, filter, initialStorageOptions, namespaceImpl, namespaceProperties)
    };
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new IndexedCountReaderFactory();
  }

  private static final class IndexedCountPartition implements InputPartition {
    private static final long serialVersionUID = 1L;

    private final LanceSparkReadOptions readOptions;
    private final String indexName;
    private final String filter;
    private final Map<String, String> initialStorageOptions;
    private final String namespaceImpl;
    private final Map<String, String> namespaceProperties;

    private IndexedCountPartition(
        LanceSparkReadOptions readOptions,
        String indexName,
        String filter,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties) {
      this.readOptions = readOptions;
      this.indexName = indexName;
      this.filter = filter;
      this.initialStorageOptions = initialStorageOptions;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
    }
  }

  private static final class IndexedCountReaderFactory implements PartitionReaderFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      return new IndexedCountReader((IndexedCountPartition) partition);
    }
  }

  private static final class IndexedCountReader implements PartitionReader<InternalRow> {
    private final IndexedCountPartition partition;
    private boolean produced = false;
    private InternalRow row;

    private IndexedCountReader(IndexedCountPartition partition) {
      this.partition = partition;
    }

    @Override
    public boolean next() throws IOException {
      if (produced) {
        return false;
      }
      produced = true;
      row = new GenericInternalRow(new Object[] {countIndexedRows()});
      return true;
    }

    @Override
    public InternalRow get() {
      return row;
    }

    @Override
    public void close() {
      // The dataset is closed by try-with-resources before next() returns.
    }

    private long countIndexedRows() throws IOException {
      // A negative count is the JNI error sentinel.
      try (ExecutorNamespace ignored =
              ExecutorNamespace.acquire(
                  partition.readOptions, partition.namespaceImpl, partition.namespaceProperties);
          Dataset dataset =
              Utils.openDatasetBuilder(partition.readOptions)
                  .initialStorageOptions(partition.initialStorageOptions)
                  .build()) {
        long indexedCount =
            dataset.countIndexedRows(
                partition.indexName, partition.filter, java.util.Optional.empty());
        if (indexedCount < 0) {
          throw new IOException(
              "Scalar index '"
                  + partition.indexName
                  + "' returned a negative row count: "
                  + indexedCount);
        }
        return indexedCount;
      } catch (RuntimeException e) {
        throw new IOException(
            "Failed to count rows with scalar index '" + partition.indexName + "'", e);
      }
    }
  }
}
