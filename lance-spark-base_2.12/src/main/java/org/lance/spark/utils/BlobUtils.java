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
package org.lance.spark.utils;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.fragment.DataFile;
import org.lance.operation.Append;
import org.lance.operation.Merge;
import org.lance.operation.Operation;
import org.lance.operation.Overwrite;
import org.lance.operation.Update;
import org.lance.schema.LanceField;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class BlobUtils {

  public static final String LANCE_ENCODING_BLOB_KEY = "lance-encoding:blob";
  public static final String LANCE_ENCODING_BLOB_VALUE = "true";

  public static final String ARROW_EXTENSION_NAME_KEY = "ARROW:extension:name";
  public static final String ARROW_EXTENSION_BLOB_V2 = "lance.blob.v2";

  /** Lowest Lance file format version that can store blob v2 columns. */
  public static final String MIN_BLOB_V2_FILE_FORMAT_VERSION = "2.2";

  /**
   * Highest version that stores the legacy (v1) blob encoding. From {@link
   * #MIN_BLOB_V2_FILE_FORMAT_VERSION} on, Lance turns a column marked {@link
   * #LANCE_ENCODING_BLOB_KEY} into blob v2.
   */
  public static final String MAX_BLOB_V1_FILE_FORMAT_VERSION = "2.1";

  /**
   * Lance file format release selectors that resolve to a concrete version supporting blob v2.
   * Lance never persists these; it resolves them when the file is written.
   */
  private static final Set<String> BLOB_V2_CAPABLE_SELECTORS =
      new HashSet<>(Arrays.asList("stable", "next"));

  /**
   * Versions Lance recognizes that cannot store the legacy (v1) blob encoding. Lance parses
   * versions against a closed set, so an unrecognized string such as {@code 2.4} is not a newer
   * format but a parse failure, and is left for Lance to report rather than rejected here. Only
   * this side needs listing: it grows with each Lance release, while the v1-capable side is closed
   * at {@link #MAX_BLOB_V1_FILE_FORMAT_VERSION}.
   */
  private static final Set<String> KNOWN_BLOB_V2_VERSIONS =
      new HashSet<>(Arrays.asList("2.2", "2.3", "stable", "next"));

  /** Table property that requests a blob encoding: {@code <column>.lance.encoding = 'blob'}. */
  public static final String BLOB_ENCODING_PROPERTY_SUFFIX = ".lance.encoding";

  public static final String BLOB_ENCODING_PROPERTY_VALUE = "blob";

  /**
   * Spark struct type for a Lance blob v2 descriptor: {@code kind, position, size, blob_id,
   * blob_uri}.
   */
  public static final StructType BLOB_DESCRIPTOR_STRUCT =
      new StructType()
          .add("kind", DataTypes.ShortType)
          .add("position", DataTypes.LongType)
          .add("size", DataTypes.LongType)
          .add("blob_id", DataTypes.LongType)
          .add("blob_uri", DataTypes.StringType);

  /** Ordinal of the {@code size} field within {@link #BLOB_DESCRIPTOR_STRUCT}. */
  public static final int BLOB_DESCRIPTOR_SIZE_ORDINAL = BLOB_DESCRIPTOR_STRUCT.fieldIndex("size");

  private static final int BLOB_DESCRIPTOR_KIND_ORDINAL = BLOB_DESCRIPTOR_STRUCT.fieldIndex("kind");
  private static final int BLOB_DESCRIPTOR_POSITION_ORDINAL =
      BLOB_DESCRIPTOR_STRUCT.fieldIndex("position");
  private static final int BLOB_DESCRIPTOR_BLOB_ID_ORDINAL =
      BLOB_DESCRIPTOR_STRUCT.fieldIndex("blob_id");
  private static final int BLOB_DESCRIPTOR_BLOB_URI_ORDINAL =
      BLOB_DESCRIPTOR_STRUCT.fieldIndex("blob_uri");

  /**
   * True when a blob v2 descriptor row is Lance's null sentinel: inline kind with zero
   * position/size, zero blob_id, and empty uri. Matches lance-core {@code collect_blob_entries_v2}
   * and Python {@code _is_null_blob_description}. A SQL-null descriptor also counts as null.
   */
  public static boolean isNullBlobV2Descriptor(InternalRow descriptor) {
    if (descriptor == null) {
      return true;
    }
    if (descriptor.getShort(BLOB_DESCRIPTOR_KIND_ORDINAL) != 0) {
      return false;
    }
    if (descriptor.getLong(BLOB_DESCRIPTOR_POSITION_ORDINAL) != 0L) {
      return false;
    }
    if (descriptor.getLong(BLOB_DESCRIPTOR_SIZE_ORDINAL) != 0L) {
      return false;
    }
    if (descriptor.getLong(BLOB_DESCRIPTOR_BLOB_ID_ORDINAL) != 0L) {
      return false;
    }
    if (descriptor.isNullAt(BLOB_DESCRIPTOR_BLOB_URI_ORDINAL)) {
      return true;
    }
    UTF8String uri = descriptor.getUTF8String(BLOB_DESCRIPTOR_BLOB_URI_ORDINAL);
    return uri == null || uri.numBytes() == 0;
  }

  /**
   * Check if a Spark field is a blob field based on its metadata.
   *
   * @param field the Spark struct field to check
   * @return true if the field is a blob field, false otherwise
   */
  public static boolean isBlobSparkField(StructField field) {
    return field != null && isBlobSparkMetadata(field.metadata());
  }

  /** Returns true when {@code metadata} carries the legacy (v1) blob marker. */
  public static boolean isBlobSparkMetadata(Metadata metadata) {
    if (metadata == null || !metadata.contains(LANCE_ENCODING_BLOB_KEY)) {
      return false;
    }

    return LANCE_ENCODING_BLOB_VALUE.equalsIgnoreCase(metadata.getString(LANCE_ENCODING_BLOB_KEY));
  }

  /**
   * Check if an Arrow field is a blob field based on its metadata.
   *
   * @param field the Arrow field to check
   * @return true if the field is a blob field, false otherwise
   */
  public static boolean isBlobArrowField(org.apache.arrow.vector.types.pojo.Field field) {
    if (field == null) {
      return false;
    }

    java.util.Map<String, String> metadata = field.getMetadata();
    if (metadata == null) {
      return false;
    }

    if (!metadata.containsKey(LANCE_ENCODING_BLOB_KEY)) {
      return false;
    }

    String value = metadata.get(LANCE_ENCODING_BLOB_KEY);
    return LANCE_ENCODING_BLOB_VALUE.equalsIgnoreCase(value);
  }

  /** Returns true when a Spark field carries the lance-core blob v2 Arrow extension. */
  public static boolean isBlobV2SparkField(StructField field) {
    return field != null && isBlobV2SparkMetadata(field.metadata());
  }

  public static boolean isBlobV2SparkMetadata(Metadata metadata) {
    if (metadata == null) {
      return false;
    }

    return metadata.contains(ARROW_EXTENSION_NAME_KEY)
        && ARROW_EXTENSION_BLOB_V2.equals(metadata.getString(ARROW_EXTENSION_NAME_KEY));
  }

  /** Names of the blob v2 columns in {@code schema}, identified by the lance.blob.v2 extension. */
  public static Set<String> blobV2ColumnNames(StructType schema) {
    Set<String> names = new HashSet<>();
    for (StructField field : schema.fields()) {
      if (isBlobV2SparkField(field)) {
        names.add(field.name());
      }
    }
    return names;
  }

  /** Returns true if any field in {@code schema} is a blob v2 column. */
  public static boolean hasBlobV2Fields(StructType schema) {
    return !blobV2ColumnNames(schema).isEmpty();
  }

  /**
   * Paths of the legacy (v1) blob fields in {@code schema}, identified by {@link
   * #LANCE_ENCODING_BLOB_KEY}. Fields nested in structs, arrays and maps count too: Lance converts
   * them to blob v2 like top-level columns.
   */
  public static Set<String> blobV1ColumnNames(StructType schema) {
    Set<String> paths = new LinkedHashSet<>();
    collectBlobV1Paths("", schema, Metadata.empty(), paths);
    return paths;
  }

  private static void collectBlobV1Paths(
      String path, DataType type, Metadata metadata, Set<String> paths) {
    if (isBlobSparkMetadata(metadata)) {
      paths.add(path);
    }
    if (type instanceof StructType) {
      for (StructField field : ((StructType) type).fields()) {
        String fieldPath = path.isEmpty() ? field.name() : path + "." + field.name();
        collectBlobV1Paths(fieldPath, field.dataType(), field.metadata(), paths);
      }
    } else if (type instanceof ArrayType) {
      // Spark has no metadata slot for array elements or map entries, so the connector keeps
      // theirs on the parent field (see LanceArrowUtils).
      collectBlobV1Paths(
          path,
          ((ArrayType) type).elementType(),
          LanceArrowUtils.parseEmbeddedMetadata(
              metadata, LanceArrowUtils.LANCE_ELEMENT_METADATA_KEY()),
          paths);
    } else if (type instanceof MapType) {
      MapType map = (MapType) type;
      collectBlobV1Paths(
          path + ".key",
          map.keyType(),
          LanceArrowUtils.parseEmbeddedMetadata(
              metadata, LanceArrowUtils.LANCE_MAP_KEY_METADATA_KEY()),
          paths);
      collectBlobV1Paths(
          path + ".value",
          map.valueType(),
          LanceArrowUtils.parseEmbeddedMetadata(
              metadata, LanceArrowUtils.LANCE_MAP_VALUE_METADATA_KEY()),
          paths);
    }
  }

  /** Returns true if any field in {@code schema} carries legacy (v1) blob metadata. */
  public static boolean hasBlobV1Fields(StructType schema) {
    return !blobV1ColumnNames(schema).isEmpty();
  }

  /**
   * Returns true for blob columns in the Spark read schema, v1 or v2. Drives {@code _rowaddr} and
   * the unloaded-blob read path in the scan.
   */
  public static boolean isBlobReadColumn(StructField field) {
    return isBlobSparkField(field) || isBlobV2SparkField(field);
  }

  /** Rewrites blob v2 columns to the descriptor struct returned by Lance. */
  public static StructType applyBlobV2DescriptorSchema(StructType schema) {
    StructField[] fields = new StructField[schema.fields().length];
    boolean changed = false;
    for (int i = 0; i < schema.fields().length; i++) {
      StructField field = schema.fields()[i];
      if (!isBlobV2SparkField(field)) {
        fields[i] = field;
        continue;
      }

      fields[i] =
          new StructField(field.name(), BLOB_DESCRIPTOR_STRUCT, field.nullable(), field.metadata());
      changed = true;
    }

    return changed ? new StructType(fields) : schema;
  }

  /**
   * True when {@code fileFormatVersion} is known not to store legacy (v1) blob columns, so the
   * connector refuses to create or write them at it. Deliberately narrower than {@link
   * #fileFormatSupportsBlobV2(String)}, which picks the metadata to write and so errs towards v2;
   * this one gates an up-front failure.
   */
  public static boolean knownToRejectBlobV1(String fileFormatVersion) {
    if (fileFormatVersion == null) {
      return false;
    }
    return KNOWN_BLOB_V2_VERSIONS.contains(fileFormatVersion.trim().toLowerCase(Locale.ROOT));
  }

  /**
   * Rejects writing {@code schema}'s legacy (v1) blob fields at a {@code fileFormatVersion} that
   * cannot store them. A commit that adds a 2.2+ file turns those fields into blob v2, while {@code
   * Fragment.create} still writes the v1 layout into the file, so the new version cannot be read.
   */
  public static void checkBlobV1WriteVersion(StructType schema, String fileFormatVersion) {
    if (!knownToRejectBlobV1(fileFormatVersion)) {
      return;
    }
    Set<String> blobV1Fields = blobV1ColumnNames(schema);
    if (!blobV1Fields.isEmpty()) {
      throw new IllegalArgumentException(
          "Legacy (v1) blob columns "
              + blobV1Fields
              + " cannot be written with file_format_version '"
              + fileFormatVersion
              + "'. Write with "
              + MAX_BLOB_V1_FILE_FORMAT_VERSION
              + " or older, or rebuild the columns as blob v2.");
    }
  }

  /**
   * Rejects committing {@code operation} on {@code dataset} when the files it keeps from {@code
   * dataset}, or the fragments it adds, include a data file at file format {@value
   * #MIN_BLOB_V2_FILE_FORMAT_VERSION} or newer that holds a legacy (v1) blob field. Lance 13 turns
   * such a field into blob v2 on that commit, while files that Lance 12 or a fragment writer wrote
   * keep the v1 layout, so the new version cannot be read. An overwrite only references the
   * fragments it commits; any other operation also keeps the files of {@code dataset}. Lance builds
   * the new version on its latest one, so a file another client commits after the version of {@code
   * dataset} is not checked. {@link LanceCommits} runs this before each connector commit. Remove
   * once the connector uses a Lance release that reads that layout.
   *
   * @param dataset the version {@code operation} is committed on; unused for an overwrite
   */
  static void checkBlobV1Commit(Dataset dataset, Operation operation) {
    if (operation instanceof Overwrite) {
      Overwrite overwrite = (Overwrite) operation;
      checkAddedBlobV1Fields(
          arrowBlobV1Fields(overwrite.schema()), overwrite.fragments(), Collections.emptySet());
      return;
    }
    checkBlobV1DataFiles(dataset, addedFragments(operation));
    if (operation instanceof Merge) {
      // A merge can add columns, such as ADD COLUMNS does, and Lance takes the new schema from it.
      Merge merge = (Merge) operation;
      Set<String> added = arrowBlobV1Fields(merge.schema());
      Map<Integer, String> existing = new HashMap<>();
      collectBlobV1Fields("", dataset.getLanceSchema().fields(), existing);
      added.removeAll(existing.values());
      if (!added.isEmpty()) {
        checkAddedBlobV1Fields(added, merge.fragments(), dataFilePaths(dataset));
      }
    }
  }

  /**
   * Like {@link #checkBlobV1Commit}, for a Lance call that commits on {@code dataset}, such as
   * {@code createIndex}. The data files such a call writes itself, as compaction does, come from
   * Lance's dataset writer, which stores v1 blob fields as blob v2 from 2.2 on, so only the files
   * kept from {@code dataset} are checked.
   */
  static void checkBlobV1DataFiles(Dataset dataset) {
    checkBlobV1DataFiles(dataset, Collections.emptyList());
  }

  private static void checkBlobV1DataFiles(Dataset dataset, List<FragmentMetadata> added) {
    Map<Integer, String> blobV1Fields = new HashMap<>();
    collectBlobV1Fields("", dataset.getLanceSchema().fields(), blobV1Fields);
    if (blobV1Fields.isEmpty()) {
      return;
    }
    List<DataFile> kept = new ArrayList<>();
    for (Fragment fragment : dataset.getFragments()) {
      kept.addAll(fragment.metadata().getReferencedLanceFiles());
    }
    Set<String> held = blobV1FieldsInBlobV2Files(blobV1Fields, kept);
    if (!held.isEmpty()) {
      throw new UnsupportedOperationException(
          "Data files at file format "
              + MIN_BLOB_V2_FILE_FORMAT_VERSION
              + " or newer hold the legacy (v1) blob columns "
              + held
              + ", so a commit would turn them into blob v2 and leave the table unreadable."
              + " Repair the table first; see Blob v2 Writes in the configuration docs.");
    }
    List<DataFile> written = new ArrayList<>();
    for (FragmentMetadata fragment : added) {
      written.addAll(fragment.getReferencedLanceFiles());
    }
    Set<String> writtenFields = blobV1FieldsInBlobV2Files(blobV1Fields, written);
    if (!writtenFields.isEmpty()) {
      throw writtenAtBlobV2Version(writtenFields);
    }
  }

  /** Fragments {@code operation} commits with new or rewritten data files. */
  private static List<FragmentMetadata> addedFragments(Operation operation) {
    if (operation instanceof Append) {
      return ((Append) operation).fragments();
    }
    if (operation instanceof Merge) {
      return ((Merge) operation).fragments();
    }
    if (operation instanceof Update) {
      Update update = (Update) operation;
      List<FragmentMetadata> fragments = new ArrayList<>(update.updatedFragments());
      fragments.addAll(update.newFragments());
      return fragments;
    }
    return Collections.emptyList();
  }

  /** Paths of {@code blobV1Fields}, keyed by field id, that a 2.2+ file in {@code files} holds. */
  static Set<String> blobV1FieldsInBlobV2Files(
      Map<Integer, String> blobV1Fields, List<DataFile> files) {
    Set<String> held = new TreeSet<>();
    for (DataFile file : files) {
      if (isBlobV2FileFormat(file)) {
        for (int id : file.getFields()) {
          String path = blobV1Fields.get(id);
          if (path != null) {
            held.add(path);
          }
        }
      }
    }
    return held;
  }

  /**
   * Rejects writing {@code fields}, legacy (v1) blob fields that get their ids only when the
   * overwrite or merge adding them commits, into a data file at file format {@value
   * #MIN_BLOB_V2_FILE_FORMAT_VERSION} or newer. Without ids they are matched by path: every data
   * file a write adds holds all of the columns it writes, so any such file of {@code fragments}
   * outside {@code kept} holds them.
   */
  private static void checkAddedBlobV1Fields(
      Set<String> fields, List<FragmentMetadata> fragments, Set<String> kept) {
    if (fields.isEmpty()) {
      return;
    }
    for (FragmentMetadata fragment : fragments) {
      for (DataFile file : fragment.getReferencedLanceFiles()) {
        if (isBlobV2FileFormat(file) && !kept.contains(file.getPath())) {
          throw writtenAtBlobV2Version(fields);
        }
      }
    }
  }

  private static Set<String> arrowBlobV1Fields(Schema schema) {
    Set<String> paths = new TreeSet<>();
    collectArrowBlobV1Fields("", schema.getFields(), paths);
    return paths;
  }

  private static Set<String> dataFilePaths(Dataset dataset) {
    Set<String> paths = new HashSet<>();
    for (Fragment fragment : dataset.getFragments()) {
      for (DataFile file : fragment.metadata().getReferencedLanceFiles()) {
        paths.add(file.getPath());
      }
    }
    return paths;
  }

  private static IllegalArgumentException writtenAtBlobV2Version(Set<String> blobV1Fields) {
    return new IllegalArgumentException(
        "Legacy (v1) blob columns "
            + blobV1Fields
            + " were written at file format "
            + MIN_BLOB_V2_FILE_FORMAT_VERSION
            + " or newer, which cannot store them. Write with file_format_version "
            + MAX_BLOB_V1_FILE_FORMAT_VERSION
            + " or older.");
  }

  private static boolean isBlobV2FileFormat(DataFile file) {
    return file.getFileMajorVersion() > 2
        || (file.getFileMajorVersion() == 2 && file.getFileMinorVersion() >= 2);
  }

  // Lance's legacy blob test: the blob key without blob v2. Blob v2 fields are structs in both of
  // their schema forms, so a legacy blob is a leaf that carries the key.
  private static boolean isLegacyBlob(Map<String, String> metadata, boolean leaf) {
    return leaf
        && metadata != null
        && metadata.containsKey(LANCE_ENCODING_BLOB_KEY)
        && !ARROW_EXTENSION_BLOB_V2.equals(metadata.get(ARROW_EXTENSION_NAME_KEY));
  }

  private static void collectBlobV1Fields(
      String parent, List<LanceField> fields, Map<Integer, String> blobV1Fields) {
    for (LanceField field : fields) {
      String path = parent.isEmpty() ? field.getName() : parent + "." + field.getName();
      if (isLegacyBlob(field.getMetadata(), field.getChildren().isEmpty())) {
        blobV1Fields.put(field.getId(), path);
      }
      collectBlobV1Fields(path, field.getChildren(), blobV1Fields);
    }
  }

  private static void collectArrowBlobV1Fields(
      String parent, List<Field> fields, Set<String> paths) {
    for (Field field : fields) {
      String path = parent.isEmpty() ? field.getName() : parent + "." + field.getName();
      if (isLegacyBlob(field.getMetadata(), field.getChildren().isEmpty())) {
        paths.add(path);
      }
      collectArrowBlobV1Fields(path, field.getChildren(), paths);
    }
  }

  /**
   * True when {@code tableProperties} asks for a blob column via {@code <column>.lance.encoding =
   * 'blob'}. Checked before the schema is processed, because the blob metadata that {@code
   * SchemaConverter} attaches depends on the version resolved from this.
   */
  public static boolean requestsBlobEncoding(Map<String, String> tableProperties) {
    if (tableProperties == null) {
      return false;
    }
    for (Map.Entry<String, String> entry : tableProperties.entrySet()) {
      if (entry.getKey() != null
          && entry.getKey().endsWith(BLOB_ENCODING_PROPERTY_SUFFIX)
          && BLOB_ENCODING_PROPERTY_VALUE.equalsIgnoreCase(entry.getValue())) {
        return true;
      }
    }
    return false;
  }

  /**
   * True when {@code fileFormatVersion} resolves to {@value #MIN_BLOB_V2_FILE_FORMAT_VERSION} or
   * newer, either as a numeric {@code major[.minor]} or as one of Lance's release selectors.
   *
   * <p>Null, {@code legacy}, and malformed strings return false. Lance validates version strings at
   * dataset creation.
   *
   * <p>TODO: delegate to {@code LanceFileFormatVersion.isAtLeast()} in lance-core once version
   * aliases are exposed to Java.
   */
  public static boolean fileFormatSupportsBlobV2(String fileFormatVersion) {
    if (fileFormatVersion == null) {
      return false;
    }
    String trimmed = fileFormatVersion.trim();
    if (BLOB_V2_CAPABLE_SELECTORS.contains(trimmed.toLowerCase(Locale.ROOT))) {
      return true;
    }
    String[] parts = trimmed.split("\\.");
    try {
      int major = Integer.parseInt(parts.length > 0 ? parts[0] : "");
      int minor = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
      return major > 2 || (major == 2 && minor >= 2);
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
