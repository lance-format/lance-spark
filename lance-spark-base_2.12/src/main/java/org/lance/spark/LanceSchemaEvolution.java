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

import org.apache.arrow.vector.types.pojo.Field;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Translates a Spark {@link ColumnChange} schema-evolution request (produced by {@code ALTER TABLE
 * ADD/DROP/RENAME/ALTER COLUMN}) into a single atomic {@link Dataset} mutation.
 *
 * <p>To preserve Spark's all-or-nothing {@code alterTable} contract, an accepted request is
 * committed through exactly one core operation: {@code addColumns}, {@code dropColumns}, or {@code
 * alterColumns} (each of which commits its whole batch atomically). The request is fully validated
 * against the current schema, and requests that would require more than one core mutation are
 * rejected, <em>before</em> anything is written — so a rejected request never leaves the table
 * partially mutated. Heterogeneous batching (mixing additions, drops, and alterations in one
 * commit) is not supported by the core yet, so such requests must be issued as separate statements.
 */
final class LanceSchemaEvolution {

  /** Lance file format version string for the legacy ("0.1") format. */
  private static final String LEGACY_FILE_FORMAT_VERSION = "0.1";

  /** The single core mutation an accepted request compiles down to. */
  private enum Kind {
    ADD,
    DROP,
    ALTER
  }

  private LanceSchemaEvolution() {}

  static void apply(Dataset dataset, List<ColumnChange> changes) {
    if (changes.isEmpty()) {
      return;
    }

    boolean legacyFormat = LEGACY_FILE_FORMAT_VERSION.equals(dataset.getLanceFileFormatVersion());
    Set<String> fields = topLevelFieldNames(dataset);

    // Validate the whole request and confirm it compiles to a single core mutation before writing
    // anything. `fields` tracks the names each change introduces or removes so an ordered request
    // is checked against the evolving schema.
    Kind kind = null;
    for (ColumnChange change : changes) {
      Kind changeKind = validate(change, fields, legacyFormat);
      if (kind == null) {
        kind = changeKind;
      } else if (kind != changeKind) {
        throw new UnsupportedOperationException(
            "A single ALTER TABLE cannot mix column additions, drops, and alterations; "
                + "issue them as separate statements.");
      }
    }

    switch (kind) {
      case ADD:
        applyAdds(dataset, changes);
        break;
      case DROP:
        applyDrops(dataset, changes);
        break;
      case ALTER:
        applyAlters(dataset, changes);
        break;
      default:
        throw new IllegalStateException("Unexpected change kind: " + kind);
    }
  }

  private static Kind validate(
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
      return Kind.ADD;
    } else if (change instanceof DeleteColumn) {
      DeleteColumn delete = (DeleteColumn) change;
      if (topLevelFields.contains(topLevelName(delete.fieldNames()))) {
        topLevelFields.remove(topLevelName(delete.fieldNames()));
      } else if (!delete.ifExists()) {
        throw new UnsupportedOperationException(
            "Cannot drop missing column: " + path(delete.fieldNames()));
      }
      return Kind.DROP;
    } else if (change instanceof RenameColumn) {
      RenameColumn rename = (RenameColumn) change;
      requireExists(topLevelFields, rename.fieldNames());
      topLevelFields.remove(topLevelName(rename.fieldNames()));
      topLevelFields.add(rename.newName());
      return Kind.ALTER;
    } else if (change instanceof UpdateColumnNullability) {
      UpdateColumnNullability updateNull = (UpdateColumnNullability) change;
      requireExists(topLevelFields, updateNull.fieldNames());
      return Kind.ALTER;
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

  private static void applyAdds(Dataset dataset, List<ColumnChange> changes) {
    List<StructField> fields = new ArrayList<>(changes.size());
    for (ColumnChange change : changes) {
      AddColumn add = (AddColumn) change;
      MetadataBuilder metadataBuilder = new MetadataBuilder();
      if (add.comment() != null) {
        metadataBuilder.putString("comment", add.comment());
      }
      fields.add(
          new StructField(
              add.fieldNames()[0], add.dataType(), add.isNullable(), metadataBuilder.build()));
    }
    Schema arrowSchema =
        LanceArrowUtils.toArrowSchema(
            new StructType(fields.toArray(new StructField[0])), "UTC", true);
    dataset.addColumns(arrowSchema.getFields());
  }

  private static void applyDrops(Dataset dataset, List<ColumnChange> changes) {
    Set<String> current = topLevelFieldNames(dataset);
    List<String> toDrop = new ArrayList<>(changes.size());
    for (ColumnChange change : changes) {
      DeleteColumn delete = (DeleteColumn) change;
      if (delete.ifExists() && !current.contains(topLevelName(delete.fieldNames()))) {
        continue;
      }
      toDrop.add(path(delete.fieldNames()));
    }
    if (!toDrop.isEmpty()) {
      dataset.dropColumns(toDrop);
    }
  }

  private static void applyAlters(Dataset dataset, List<ColumnChange> changes) {
    // Merge rename and nullability edits that target the same column into one alteration so the
    // whole request stays a single alterColumns commit.
    Map<String, ColumnAlteration.Builder> builders = new LinkedHashMap<>();
    for (ColumnChange change : changes) {
      if (change instanceof RenameColumn) {
        RenameColumn rename = (RenameColumn) change;
        builders
            .computeIfAbsent(path(rename.fieldNames()), ColumnAlteration.Builder::new)
            .rename(rename.newName());
      } else if (change instanceof UpdateColumnNullability) {
        UpdateColumnNullability updateNull = (UpdateColumnNullability) change;
        builders
            .computeIfAbsent(path(updateNull.fieldNames()), ColumnAlteration.Builder::new)
            .nullable(updateNull.nullable());
      }
    }
    List<ColumnAlteration> alterations = new ArrayList<>(builders.size());
    for (ColumnAlteration.Builder builder : builders.values()) {
      alterations.add(builder.build());
    }
    dataset.alterColumns(alterations);
  }

  private static void requireExists(Set<String> topLevelFields, String[] fieldNames) {
    if (!topLevelFields.contains(topLevelName(fieldNames))) {
      throw new UnsupportedOperationException("Cannot alter missing column: " + path(fieldNames));
    }
  }

  private static Set<String> topLevelFieldNames(Dataset dataset) {
    Set<String> names = new LinkedHashSet<>();
    for (Field field : dataset.getSchema().getFields()) {
      names.add(field.getName());
    }
    return names;
  }

  private static String topLevelName(String[] fieldNames) {
    return fieldNames[0];
  }

  private static String path(String[] fieldNames) {
    return FieldPathUtils.canonicalPath(Arrays.asList(fieldNames));
  }
}
