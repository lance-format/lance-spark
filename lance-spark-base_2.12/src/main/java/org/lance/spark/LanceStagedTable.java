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

import org.lance.spark.write.SparkWrite;

import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

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
  private final Runnable commitAction;
  private final Runnable abortAction;
  private final AtomicBoolean dataCommitted = new AtomicBoolean(false);

  /**
   * Creates a new staged table.
   *
   * @param readOptions read options for the underlying dataset
   * @param sparkSchema the Spark schema
   * @param initialStorageOptions initial storage options from namespace
   * @param namespaceImpl namespace implementation type
   * @param namespaceProperties namespace connection properties
   * @param operation the staging operation type
   * @param commitAction action to run on commitStagedChanges when no data was written (schema-only)
   * @param abortAction action to run on abortStagedChanges for cleanup
   */
  public LanceStagedTable(
      LanceSparkReadOptions readOptions,
      StructType sparkSchema,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      Operation operation,
      Runnable commitAction,
      Runnable abortAction) {
    super(readOptions, sparkSchema, initialStorageOptions, namespaceImpl, namespaceProperties);
    this.operation = operation;
    this.commitAction = commitAction;
    this.abortAction = abortAction;
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    WriteBuilder builder = super.newWriteBuilder(info);
    if (builder instanceof SparkWrite.SparkWriteBuilder) {
      SparkWrite.SparkWriteBuilder sparkBuilder = (SparkWrite.SparkWriteBuilder) builder;
      sparkBuilder.setOnCommit(() -> dataCommitted.set(true));
      if (operation == Operation.REPLACE || operation == Operation.CREATE_OR_REPLACE) {
        sparkBuilder.truncate();
      }
    }
    return builder;
  }

  @Override
  public void commitStagedChanges() {
    if (!dataCommitted.get() && commitAction != null) {
      commitAction.run();
    }
  }

  @Override
  public void abortStagedChanges() {
    if (abortAction != null) {
      abortAction.run();
    }
  }
}
