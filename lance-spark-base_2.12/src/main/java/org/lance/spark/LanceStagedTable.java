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

import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.DeregisterTableRequest;
import org.lance.operation.Overwrite;
import org.lance.spark.write.SparkWrite;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.lance.spark.utils.Utils.openDataset;

/**
 * A staged table that supports atomic create, replace, and create-or-replace operations.
 *
 * <p>This class extends {@link LanceDataset} and implements Spark's {@link StagedTable} interface
 * to provide atomic table operations. When Spark calls {@code commitStagedChanges()}, the table is
 * finalized; when {@code abortStagedChanges()} is called, any partial changes are cleaned up.
 */
public class LanceStagedTable extends LanceDataset implements StagedTable {

  /** The type of staging operation. */
  public enum Operation {
    CREATE,
    REPLACE,
    CREATE_OR_REPLACE
  }

  private final Operation operation;
  private final LanceNamespace namespace;
  private final List<String> tableIdList;
  private final Schema arrowSchema;
  private final Map<String, String> storageOptions;
  private final boolean tableExisted;
  private final AtomicBoolean commitFlag = new AtomicBoolean(false);

  /**
   * Creates a new staged table.
   *
   * @param readOptions read options for the underlying dataset
   * @param sparkSchema the Spark schema
   * @param initialStorageOptions initial storage options from namespace
   * @param namespaceImpl namespace implementation type
   * @param namespaceProperties namespace connection properties
   * @param operation the staging operation type
   * @param namespace the Lance namespace for table operations
   * @param tableIdList the table identifier path
   * @param arrowSchema the Arrow schema for the table
   * @param storageOptions storage options for table creation
   * @param tableExisted whether the table existed at staging time
   */
  public LanceStagedTable(
      LanceSparkReadOptions readOptions,
      StructType sparkSchema,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      Operation operation,
      LanceNamespace namespace,
      List<String> tableIdList,
      Schema arrowSchema,
      Map<String, String> storageOptions,
      boolean tableExisted) {
    super(readOptions, sparkSchema, initialStorageOptions, namespaceImpl, namespaceProperties);
    this.operation = operation;
    this.namespace = namespace;
    this.tableIdList = tableIdList;
    this.arrowSchema = arrowSchema;
    this.storageOptions = storageOptions;
    this.tableExisted = tableExisted;
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    WriteBuilder builder = super.newWriteBuilder(info);
    if (builder instanceof SparkWrite.SparkWriteBuilder) {
      SparkWrite.SparkWriteBuilder sparkBuilder = (SparkWrite.SparkWriteBuilder) builder;
      sparkBuilder.setCommitFlag(commitFlag);
      if (!tableExisted) {
        sparkBuilder.setNewTable(true);
      }
      if (operation == Operation.REPLACE || operation == Operation.CREATE_OR_REPLACE) {
        sparkBuilder.truncate();
      }
    }
    return builder;
  }

  @Override
  public void commitStagedChanges() {
    if (commitFlag.get()) {
      return;
    }

    if (!tableExisted) {
      try (Dataset dataset =
          Dataset.write()
              .allocator(LanceRuntime.allocator())
              .namespace(namespace)
              .tableId(tableIdList)
              .schema(arrowSchema)
              .mode(WriteParams.WriteMode.CREATE)
              .storageOptions(storageOptions)
              .execute()) {
        // Table created on commit
      }
    } else {
      try (Dataset dataset = openDataset(readOptions)) {
        dataset
            .newTransactionBuilder()
            .operation(
                Overwrite.builder().fragments(Collections.emptyList()).schema(arrowSchema).build())
            .build()
            .commit();
      }
    }
  }

  @Override
  public void abortStagedChanges() {
    if (!tableExisted) {
      DeregisterTableRequest deregisterRequest = new DeregisterTableRequest();
      tableIdList.forEach(deregisterRequest::addIdItem);
      namespace.deregisterTable(deregisterRequest);
    }
  }
}
