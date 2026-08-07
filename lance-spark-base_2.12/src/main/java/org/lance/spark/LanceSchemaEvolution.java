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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Translates Spark {@link ColumnChange} schema evolution requests (produced by {@code ALTER TABLE
 * ADD/DROP/RENAME/ALTER COLUMN}) into the corresponding {@link Dataset} operations.
 *
 * <p>The whole ordered request is validated against the current schema first and any unsupported
 * option is rejected <em>before</em> any change is written, so a rejected request never leaves the
 * table partially mutated. Validated changes are then applied in the order Spark supplies them.
 */
final class LanceSchemaEvolution {

  /** Lance file format version string for the legacy ("0.1") format. */
  private static final String LEGACY_FILE_FORMAT_VERSION = "0.1";

  private LanceSchemaEvolution() {}

  static void apply(Dataset dataset, List<ColumnChange> changes) {
    boolean legacyFormat = LEGACY_FILE_FORMAT_VERSION.equals(dataset.getLanceFileFormatVersion());

    // Validate the entire request before mutating anything, so a rejected change never leaves the
    // table partially mutated. Validation runs against a simulated copy of the field set that
    // tracks the names each change introduces or removes, so ordered requests are checked against
    // the evolving schema.
    Set<String> simulated = topLevelFieldNames(dataset);
    for (ColumnChange change : changes) {
      validate(change, simulated, legacyFormat);
    }

    // Apply against a fresh live copy so per-change decisions (e.g. DROP COLUMN IF EXISTS) reflect
    // the schema state at the point each change is applied.
    Set<String> current = topLevelFieldNames(dataset);
    for (ColumnChange change : changes) {
      applyOne(dataset, change, current);
    }
  }

  private static void validate(
      ColumnChange change, Set<String> topLevelFields, boolean legacyFormat) {
    if (change instanceof AddColumn) {
      AddColumn add = (AddColumn) change;
      if (add.fieldNames().length != 1) {
        throw new UnsupportedOperationException(
            "Adding nested columns is not supported: " + path(add.fieldNames()));
      }
      if (add.position() != null) {
        throw new UnsupportedOperationException(
            "ADD COLUMN with FIRST/AFTER position is not supported; columns are appended.");
      }
      if (add.defaultValue() != null) {
        throw new UnsupportedOperationException(
            "ADD COLUMN with a DEFAULT value is not supported; new columns are filled with NULL.");
      }
      if (legacyFormat) {
        throw new UnsupportedOperationException(
            "ADD COLUMN is not supported on legacy-format ("
                + LEGACY_FILE_FORMAT_VERSION
                + ") tables.");
      }
      topLevelFields.add(add.fieldNames()[0]);
    } else if (change instanceof DeleteColumn) {
      DeleteColumn delete = (DeleteColumn) change;
      if (topLevelFields.contains(topLevelName(delete.fieldNames())) || delete.ifExists()) {
        topLevelFields.remove(topLevelName(delete.fieldNames()));
      } else {
        throw new UnsupportedOperationException(
            "Cannot drop missing column: " + path(delete.fieldNames()));
      }
    } else if (change instanceof RenameColumn) {
      RenameColumn rename = (RenameColumn) change;
      requireExists(topLevelFields, rename.fieldNames());
      topLevelFields.remove(topLevelName(rename.fieldNames()));
      topLevelFields.add(rename.newName());
    } else if (change instanceof UpdateColumnNullability) {
      UpdateColumnNullability updateNull = (UpdateColumnNullability) change;
      requireExists(topLevelFields, updateNull.fieldNames());
    } else if (change instanceof UpdateColumnType) {
      UpdateColumnType updateType = (UpdateColumnType) change;
      // The current lance-core JNI drops the cast target type on the way to Rust, silently
      // turning a type change into a no-op. Reject it explicitly rather than lying about it.
      throw new UnsupportedOperationException(
          "Changing the type of column '"
              + path(updateType.fieldNames())
              + "' is not supported by the current Lance version.");
    } else {
      throw new UnsupportedOperationException(
          "Unsupported column change type: " + change.getClass().getSimpleName());
    }
  }

  private static void applyOne(Dataset dataset, ColumnChange change, Set<String> current) {
    if (change instanceof AddColumn) {
      AddColumn add = (AddColumn) change;
      addColumn(dataset, add);
      current.add(add.fieldNames()[0]);
    } else if (change instanceof DeleteColumn) {
      DeleteColumn delete = (DeleteColumn) change;
      if (delete.ifExists() && !current.contains(topLevelName(delete.fieldNames()))) {
        return;
      }
      current.remove(topLevelName(delete.fieldNames()));
      dataset.dropColumns(Collections.singletonList(path(delete.fieldNames())));
    } else if (change instanceof RenameColumn) {
      RenameColumn rename = (RenameColumn) change;
      dataset.alterColumns(
          Collections.singletonList(
              new ColumnAlteration.Builder(path(rename.fieldNames()))
                  .rename(rename.newName())
                  .build()));
      current.remove(topLevelName(rename.fieldNames()));
      current.add(rename.newName());
    } else if (change instanceof UpdateColumnNullability) {
      UpdateColumnNullability updateNull = (UpdateColumnNullability) change;
      dataset.alterColumns(
          Collections.singletonList(
              new ColumnAlteration.Builder(path(updateNull.fieldNames()))
                  .nullable(updateNull.nullable())
                  .build()));
    }
  }

  private static void addColumn(Dataset dataset, AddColumn add) {
    MetadataBuilder metadataBuilder = new MetadataBuilder();
    if (add.comment() != null) {
      metadataBuilder.putString("comment", add.comment());
    }
    StructField field =
        new StructField(
            add.fieldNames()[0], add.dataType(), add.isNullable(), metadataBuilder.build());
    Schema arrowSchema =
        LanceArrowUtils.toArrowSchema(new StructType(new StructField[] {field}), "UTC", true);
    dataset.addColumns(arrowSchema.getFields());
  }

  private static void requireExists(Set<String> topLevelFields, String[] fieldNames) {
    if (!topLevelFields.contains(topLevelName(fieldNames))) {
      throw new UnsupportedOperationException("Cannot alter missing column: " + path(fieldNames));
    }
  }

  private static Set<String> topLevelFieldNames(Dataset dataset) {
    Set<String> names = new LinkedHashSet<>();
    dataset.getSchema().getFields().forEach(f -> names.add(f.getName()));
    return names;
  }

  private static String topLevelName(String[] fieldNames) {
    return fieldNames[0];
  }

  private static String path(String[] fieldNames) {
    return FieldPathUtils.canonicalPath(Arrays.asList(fieldNames));
  }
}
