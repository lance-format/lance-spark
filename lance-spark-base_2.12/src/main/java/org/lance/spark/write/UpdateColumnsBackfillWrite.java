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
package org.lance.spark.write;

import org.lance.spark.LanceConstant;
import org.lance.spark.LanceSparkWriteOptions;

import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.SortValue;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

/**
 * Spark write for UPDATE COLUMNS FROM command.
 *
 * <p>This updates existing columns in a Lance table by computing their values from a source
 * TABLE/VIEW. Only rows that match (by _rowaddr) are updated; rows in source that don't exist in
 * target are ignored.
 */
public class UpdateColumnsBackfillWrite implements Write, RequiresDistributionAndOrdering {
  private final LanceSparkWriteOptions writeOptions;
  private final StructType schema;
  private final List<String> updateColumns;

  /**
   * Initial storage options fetched from namespace.describeTable() on the driver. These are passed
   * to workers so they can reuse the credentials without calling describeTable again.
   */
  private final Map<String, String> initialStorageOptions;

  /** Namespace configuration for credential refresh on workers. */
  private final String namespaceImpl;

  private final Map<String, String> namespaceProperties;
  private final List<String> tableId;

  UpdateColumnsBackfillWrite(
      StructType schema,
      LanceSparkWriteOptions writeOptions,
      List<String> updateColumns,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId) {
    this.schema = schema;
    this.writeOptions = writeOptions;
    this.updateColumns = updateColumns;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;
  }

  @Override
  public BatchWrite toBatch() {
    return new UpdateColumnsBackfillBatchWrite(
        schema,
        writeOptions,
        updateColumns,
        initialStorageOptions,
        namespaceImpl,
        namespaceProperties,
        tableId);
  }

  @Override
  public StreamingWrite toStreaming() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Distribution requiredDistribution() {
    NamedReference segmentId = Expressions.column(LanceConstant.FRAGMENT_ID);
    return Distributions.clustered(new NamedReference[] {segmentId});
  }

  @Override
  public SortOrder[] requiredOrdering() {
    NamedReference segmentId = Expressions.column(LanceConstant.ROW_ADDRESS);
    SortValue sortValue =
        new SortValue(segmentId, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST);
    return new SortValue[] {sortValue};
  }

  /** Write builder for UPDATE COLUMNS FROM command. */
  public static class UpdateColumnsWriteBuilder implements WriteBuilder {
    private final LanceSparkWriteOptions writeOptions;
    private final StructType schema;
    private final List<String> updateColumns;

    /**
     * Initial storage options fetched from namespace.describeTable() on the driver. These are
     * passed to workers so they can reuse the credentials without calling describeTable again.
     */
    private final Map<String, String> initialStorageOptions;

    /** Namespace configuration for credential refresh on workers. */
    private final String namespaceImpl;

    private final Map<String, String> namespaceProperties;
    private final List<String> tableId;

    public UpdateColumnsWriteBuilder(
        StructType schema,
        LanceSparkWriteOptions writeOptions,
        List<String> updateColumns,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        List<String> tableId) {
      this.schema = schema;
      this.writeOptions = writeOptions;
      this.updateColumns = updateColumns;
      this.initialStorageOptions = initialStorageOptions;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
      this.tableId = tableId;
    }

    @Override
    public Write build() {
      return new UpdateColumnsBackfillWrite(
          schema,
          writeOptions,
          updateColumns,
          initialStorageOptions,
          namespaceImpl,
          namespaceProperties,
          tableId);
    }
  }
}
