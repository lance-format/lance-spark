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
import org.lance.schema.ColumnAlteration;
import org.lance.spark.utils.FieldPathUtils;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.catalog.TableChange.AddColumn;
import org.apache.spark.sql.connector.catalog.TableChange.ColumnChange;
import org.apache.spark.sql.connector.catalog.TableChange.DeleteColumn;
import org.apache.spark.sql.connector.catalog.TableChange.RenameColumn;
import org.apache.spark.sql.connector.catalog.TableChange.UpdateColumnNullability;
import org.apache.spark.sql.connector.catalog.TableChange.UpdateColumnType;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Translates Spark {@link ColumnChange} schema evolution requests (produced by {@code ALTER TABLE
 * ADD/DROP/RENAME/ALTER COLUMN}) into the corresponding {@link Dataset} operations. Changes are
 * applied to the open dataset in the order Spark supplies them.
 */
final class LanceSchemaEvolution {

  private LanceSchemaEvolution() {}

  static void apply(Dataset dataset, List<ColumnChange> changes) {
    for (ColumnChange change : changes) {
      if (change instanceof AddColumn) {
        addColumn(dataset, (AddColumn) change);
      } else if (change instanceof DeleteColumn) {
        DeleteColumn delete = (DeleteColumn) change;
        dataset.dropColumns(Collections.singletonList(path(delete.fieldNames())));
      } else if (change instanceof RenameColumn) {
        RenameColumn rename = (RenameColumn) change;
        dataset.alterColumns(
            Collections.singletonList(
                new ColumnAlteration.Builder(path(rename.fieldNames()))
                    .rename(rename.newName())
                    .build()));
      } else if (change instanceof UpdateColumnType) {
        UpdateColumnType updateType = (UpdateColumnType) change;
        // The current lance-core JNI drops the cast target type on the way to Rust, silently
        // turning a type change into a no-op. Reject it explicitly rather than lying about it.
        throw new UnsupportedOperationException(
            "Changing the type of column '"
                + path(updateType.fieldNames())
                + "' is not supported by the current Lance version.");
      } else if (change instanceof UpdateColumnNullability) {
        UpdateColumnNullability updateNull = (UpdateColumnNullability) change;
        dataset.alterColumns(
            Collections.singletonList(
                new ColumnAlteration.Builder(path(updateNull.fieldNames()))
                    .nullable(updateNull.nullable())
                    .build()));
      } else {
        throw new UnsupportedOperationException(
            "Unsupported column change type: " + change.getClass().getSimpleName());
      }
    }
  }

  private static void addColumn(Dataset dataset, AddColumn add) {
    String[] fieldNames = add.fieldNames();
    if (fieldNames.length != 1) {
      throw new UnsupportedOperationException(
          "Adding nested columns is not supported: " + path(fieldNames));
    }
    if (add.position() != null) {
      throw new UnsupportedOperationException(
          "ADD COLUMN with FIRST/AFTER position is not supported; columns are appended.");
    }

    MetadataBuilder metadataBuilder = new MetadataBuilder();
    if (add.comment() != null) {
      metadataBuilder.putString("comment", add.comment());
    }
    StructField field =
        new StructField(fieldNames[0], add.dataType(), add.isNullable(), metadataBuilder.build());
    Schema arrowSchema =
        LanceArrowUtils.toArrowSchema(new StructType(new StructField[] {field}), "UTC", true);
    dataset.addColumns(arrowSchema.getFields());
  }

  private static String path(String[] fieldNames) {
    return FieldPathUtils.canonicalPath(Arrays.asList(fieldNames));
  }
}
