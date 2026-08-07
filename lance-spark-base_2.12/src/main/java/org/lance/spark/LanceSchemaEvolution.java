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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Translates a Spark {@link ColumnChange} schema-evolution request (produced by {@code ALTER TABLE
 * ADD/DROP/RENAME/ALTER COLUMN}) into a single atomic {@link Dataset} mutation.
 *
 * <p>To preserve Spark's ordered, all-or-nothing {@code alterTable} contract, an accepted request
 * is committed through exactly one core operation ({@code addColumns}, {@code dropColumns}, or
 * {@code alterColumns} — each commits its whole batch atomically <em>against the current
 * schema</em> with no ordering between the batched entries). Because the batch cannot express
 * changes that depend on an earlier change in the same request, the request is validated against
 * the current schema and the following are rejected <em>before</em> anything is written, so a
 * rejected request never leaves the table partially mutated:
 *
 * <ul>
 *   <li>requests that would require more than one core mutation (mixing additions, drops, and
 *       alterations, since heterogeneous batching is not supported by the core yet);
 *   <li>requests where two changes target the same column (their order would be lost when collapsed
 *       into one batch);
 *   <li>nested (multi-part) column paths, since validation is top-level only.
 * </ul>
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
    Set<String> currentFields = topLevelFieldNames(dataset);

    // Validate the whole request against the current schema and confirm it compiles to a single
    // core mutation before writing anything. Because the batched core operation applies to the
    // current schema with no ordering between entries, a column may be targeted at most once and
    // every referenced column is checked against the current schema (not an evolving one).
    Kind kind = null;
    Set<String> touched = new HashSet<>();
    for (ColumnChange change : changes) {
      Kind changeKind = validate(change, currentFields, touched, legacyFormat);
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
        applyDrops(dataset, changes, currentFields);
        break;
      case ALTER:
        applyAlters(dataset, changes);
        break;
      default:
        throw new IllegalStateException("Unexpected change kind: " + kind);
    }
  }

  private static Kind validate(
      ColumnChange change, Set<String> currentFields, Set<String> touched, boolean legacyFormat) {
    if (change instanceof AddColumn) {
      AddColumn add = (AddColumn) change;
      requireTopLevel(add.fieldNames(), "Adding nested columns");
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
      String name = add.fieldNames()[0];
      if (currentFields.contains(name)) {
        throw new UnsupportedOperationException("Cannot add existing column: " + name);
      }
      requireDistinct(touched, name);
      return Kind.ADD;
    } else if (change instanceof DeleteColumn) {
      DeleteColumn delete = (DeleteColumn) change;
      requireTopLevel(delete.fieldNames(), "Dropping nested columns");
      String name = delete.fieldNames()[0];
      if (!currentFields.contains(name) && !delete.ifExists()) {
        throw new UnsupportedOperationException("Cannot drop missing column: " + name);
      }
      requireDistinct(touched, name);
      return Kind.DROP;
    } else if (change instanceof RenameColumn) {
      RenameColumn rename = (RenameColumn) change;
      requireTopLevel(rename.fieldNames(), "Renaming nested columns");
      requireExists(currentFields, rename.fieldNames()[0]);
      requireDistinct(touched, rename.fieldNames()[0]);
      return Kind.ALTER;
    } else if (change instanceof UpdateColumnNullability) {
      UpdateColumnNullability updateNull = (UpdateColumnNullability) change;
      requireTopLevel(updateNull.fieldNames(), "Altering nested columns");
      requireExists(currentFields, updateNull.fieldNames()[0]);
      requireDistinct(touched, updateNull.fieldNames()[0]);
      return Kind.ALTER;
    } else if (change instanceof UpdateColumnType) {
      UpdateColumnType updateType = (UpdateColumnType) change;
      // The current lance-core JNI drops the cast target type on the way to Rust, silently
      // turning a type change into a no-op. Reject it explicitly rather than lying about it.
      throw new UnsupportedOperationException(
          "Changing the type of column '"
              + String.join(".", updateType.fieldNames())
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

  private static void applyDrops(
      Dataset dataset, List<ColumnChange> changes, Set<String> currentFields) {
    List<String> toDrop = new ArrayList<>(changes.size());
    for (ColumnChange change : changes) {
      DeleteColumn delete = (DeleteColumn) change;
      String name = delete.fieldNames()[0];
      // Validation already guaranteed a non-ifExists drop targets an existing column; skip a
      // missing IF EXISTS target so the core is never asked to drop a nonexistent column.
      if (delete.ifExists() && !currentFields.contains(name)) {
        continue;
      }
      toDrop.add(name);
    }
    if (!toDrop.isEmpty()) {
      dataset.dropColumns(toDrop);
    }
  }

  private static void applyAlters(Dataset dataset, List<ColumnChange> changes) {
    // Each change targets a distinct existing column (enforced during validation), so one
    // ColumnAlteration per change keeps the request a single alterColumns commit.
    List<ColumnAlteration> alterations = new ArrayList<>(changes.size());
    for (ColumnChange change : changes) {
      if (change instanceof RenameColumn) {
        RenameColumn rename = (RenameColumn) change;
        alterations.add(
            new ColumnAlteration.Builder(rename.fieldNames()[0]).rename(rename.newName()).build());
      } else if (change instanceof UpdateColumnNullability) {
        UpdateColumnNullability updateNull = (UpdateColumnNullability) change;
        alterations.add(
            new ColumnAlteration.Builder(updateNull.fieldNames()[0])
                .nullable(updateNull.nullable())
                .build());
      }
    }
    dataset.alterColumns(alterations);
  }

  private static void requireTopLevel(String[] fieldNames, String action) {
    if (fieldNames.length != 1) {
      throw new UnsupportedOperationException(
          action + " is not supported: " + String.join(".", fieldNames));
    }
  }

  private static void requireExists(Set<String> currentFields, String name) {
    if (!currentFields.contains(name)) {
      throw new UnsupportedOperationException("Cannot alter missing column: " + name);
    }
  }

  private static void requireDistinct(Set<String> touched, String name) {
    if (!touched.add(name)) {
      throw new UnsupportedOperationException(
          "Column '"
              + name
              + "' is targeted by more than one change in the same ALTER TABLE; "
              + "issue the changes as separate statements.");
    }
  }

  private static Set<String> topLevelFieldNames(Dataset dataset) {
    Set<String> names = new LinkedHashSet<>();
    for (Field field : dataset.getSchema().getFields()) {
      names.add(field.getName());
    }
    return names;
  }
}
