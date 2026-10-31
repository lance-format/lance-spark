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

import org.lance.spark.LanceConstant;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.BlobUtils;
import org.lance.spark.utils.JsonUtils;
import org.lance.spark.utils.Utils;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Plans and builds Arrow batches for {@link LanceSparkReadOptions#CONFIG_ARROW_BATCH_READS}: every
 * column is a Spark {@link ArrowColumnVector} that Spark reads as the column's declared type.
 * Vector and row metadata columns are adapted without copying values; a scan with a column that
 * cannot be adapted has no Arrow batch form ({@link #plan}).
 */
public final class ArrowBatchAdapter {

  private ArrowBatchAdapter() {}

  /**
   * Plans the Arrow batches of a scan of {@code scanSchema}, the read schema, over a dataset with
   * Arrow schema {@code datasetSchema}. Never throws: a failure makes the scan read as usual.
   */
  public static Plan plan(StructType scanSchema, Schema datasetSchema) {
    try {
      List<Field> fields = new ArrayList<>(scanSchema.size());
      for (StructField field : scanSchema.fields()) {
        Optional<String> unsupported = unsupportedReason(field);
        if (unsupported.isPresent()) {
          return Plan.unsupported(unsupported.get());
        }
        Field arrowField = batchField(field, datasetSchema);
        if (arrowField == null) {
          return Plan.unsupported(
              "column " + field.name() + " is not in the dataset schema " + datasetSchema);
        }
        Optional<String> mismatch = readMismatch(arrowField, field.dataType());
        if (mismatch.isPresent()) {
          return Plan.unsupported("column " + field.name() + ": " + mismatch.get());
        }
        fields.add(arrowField);
      }
      return Plan.supported(new Schema(fields));
    } catch (RuntimeException e) {
      return Plan.unsupported("planning Arrow batches failed: " + e);
    }
  }

  /**
   * Builds the columns of an Arrow batch over {@code root}, a batch of the native scan of fragment
   * {@code fragmentId}, in {@code scanSchema}'s order. Vectors the batch needs besides the root's
   * are added to {@code owned}, also when this method throws; the caller closes them once the batch
   * is no longer valid.
   */
  public static ColumnVector[] buildColumns(
      VectorSchemaRoot root,
      StructType scanSchema,
      int fragmentId,
      BufferAllocator allocator,
      List<FieldVector> owned) {
    Map<String, FieldVector> nativeVectors = new HashMap<>();
    for (FieldVector vector : root.getFieldVectors()) {
      nativeVectors.put(vector.getField().getName(), vector);
    }
    int rowCount = root.getRowCount();
    StructField[] fields = scanSchema.fields();
    ColumnVector[] columns = new ColumnVector[fields.length];
    for (int i = 0; i < fields.length; i++) {
      String name = fields[i].name();
      FieldVector vector;
      if (name.equals(LanceConstant.FRAGMENT_ID)) {
        IntVector fragmentIds = new IntVector(fragmentIdField(), allocator);
        owned.add(fragmentIds);
        fragmentIds.allocateNew(rowCount);
        for (int row = 0; row < rowCount; row++) {
          fragmentIds.set(row, fragmentId);
        }
        fragmentIds.setValueCount(rowCount);
        vector = fragmentIds;
      } else {
        FieldVector nativeVector = nativeVectors.get(name);
        if (nativeVector == null) {
          throw new IllegalStateException(
              "Lance scan did not return expected field '" + name + "'");
        }
        if (isRowMetadataColumn(name) && isUnsigned64(nativeVector.getField())) {
          vector = new BigIntVector(signed64Field(name), allocator);
          owned.add(vector);
          load(nativeVector, vector, allocator);
        } else if (containsFixedSizeList(nativeVector.getField())) {
          vector = toListField(nativeVector.getField()).createVector(allocator);
          owned.add(vector);
          load(nativeVector, vector, allocator);
        } else {
          vector = nativeVector;
        }
      }
      columns[i] = new ArrowColumnVector(vector);
      // The plan checked the dataset's Arrow types; fail rather than read a different scan type.
      if (!sameShape(columns[i].dataType(), fields[i].dataType())) {
        throw new IllegalStateException(
            "Arrow batch column "
                + name
                + " reads as "
                + columns[i].dataType().simpleString()
                + ", not as "
                + fields[i].dataType().simpleString());
      }
    }
    return columns;
  }

  /** Why {@code field} has no Arrow batch form whatever its Arrow type, if it has none. */
  private static Optional<String> unsupportedReason(StructField field) {
    String name = field.name();
    if (name.endsWith(LanceConstant.BLOB_POSITION_SUFFIX)
        || name.endsWith(LanceConstant.BLOB_SIZE_SUFFIX)
        || BlobUtils.isBlobReadColumn(field)) {
      return Optional.of(
          "column " + name + " is a blob column, which the Spark reader builds on the JVM");
    }
    if (JsonUtils.hasJsonMetadata(field.metadata())) {
      return Optional.of("column " + name + " is a JSON column, which the Spark reader decodes");
    }
    if (Utils.isScoreMetadataColumn(field)) {
      return Optional.of("column " + name + " is the full-text score");
    }
    return Optional.empty();
  }

  /** The Arrow field of the batch column for {@code field}, or null if the dataset has none. */
  private static Field batchField(StructField field, Schema datasetSchema) {
    String name = field.name();
    if (name.equals(LanceConstant.FRAGMENT_ID)) {
      return fragmentIdField();
    }
    if (isRowMetadataColumn(name)) {
      return signed64Field(name);
    }
    Field datasetField =
        datasetSchema.getFields().stream()
            .filter(f -> f.getName().equals(name))
            .findFirst()
            .orElse(null);
    if (datasetField == null || !containsFixedSizeList(datasetField)) {
      return datasetField;
    }
    // The field of the vector buildColumns() builds, as Arrow names list children itself.
    try (FieldVector vector = toListField(datasetField).createVector(LanceRuntime.allocator())) {
      return vector.getField();
    }
  }

  /**
   * Checks how the running Spark version reads an empty Arrow vector of {@code arrowField}: empty
   * if as {@code declared}, else why not.
   */
  private static Optional<String> readMismatch(Field arrowField, DataType declared) {
    FieldVector vector = arrowField.createVector(LanceRuntime.allocator());
    try (ArrowColumnVector column = new ArrowColumnVector(vector)) {
      if (!sameShape(column.dataType(), declared)) {
        return Optional.of(
            "Spark reads Arrow type "
                + typeTree(arrowField)
                + " as "
                + column.dataType().simpleString()
                + ", not as "
                + declared.simpleString());
      }
      return Optional.empty();
    } catch (RuntimeException e) {
      vector.close();
      return Optional.of(
          "Spark does not read Arrow type " + typeTree(arrowField) + ": " + e.getMessage());
    }
  }

  /**
   * Whether {@code actual} has the shape of {@code declared}: the same types, with struct fields in
   * the same order, ignoring field names and nullability, which do not change how Spark reads a
   * value.
   */
  private static boolean sameShape(DataType actual, DataType declared) {
    if (actual instanceof ArrayType && declared instanceof ArrayType) {
      return sameShape(((ArrayType) actual).elementType(), ((ArrayType) declared).elementType());
    }
    if (actual instanceof MapType && declared instanceof MapType) {
      MapType a = (MapType) actual;
      MapType d = (MapType) declared;
      return sameShape(a.keyType(), d.keyType()) && sameShape(a.valueType(), d.valueType());
    }
    if (actual instanceof StructType && declared instanceof StructType) {
      StructField[] a = ((StructType) actual).fields();
      StructField[] d = ((StructType) declared).fields();
      if (a.length != d.length) {
        return false;
      }
      for (int i = 0; i < a.length; i++) {
        if (!sameShape(a[i].dataType(), d[i].dataType())) {
          return false;
        }
      }
      return true;
    }
    return actual.equals(declared);
  }

  private static boolean isRowMetadataColumn(String name) {
    return name.equals(LanceConstant.ROW_ID)
        || name.equals(LanceConstant.ROW_ADDRESS)
        || name.equals(LanceConstant.ROW_CREATED_AT_VERSION)
        || name.equals(LanceConstant.ROW_LAST_UPDATED_AT_VERSION);
  }

  private static boolean isUnsigned64(Field field) {
    ArrowType type = field.getType();
    return type instanceof ArrowType.Int
        && !((ArrowType.Int) type).getIsSigned()
        && ((ArrowType.Int) type).getBitWidth() == 64;
  }

  private static Field fragmentIdField() {
    return new Field(
        LanceConstant.FRAGMENT_ID, FieldType.nullable(new ArrowType.Int(32, true)), null);
  }

  private static Field signed64Field(String name) {
    return new Field(name, FieldType.nullable(new ArrowType.Int(64, true)), null);
  }

  private static boolean containsFixedSizeList(Field field) {
    return field.getType() instanceof ArrowType.FixedSizeList
        || field.getChildren().stream().anyMatch(ArrowBatchAdapter::containsFixedSizeList);
  }

  /** {@code field} with every FixedSizeList, at any depth, replaced by a List. */
  private static Field toListField(Field field) {
    if (!containsFixedSizeList(field)) {
      return field;
    }
    FieldType fieldType =
        field.getType() instanceof ArrowType.FixedSizeList
            ? new FieldType(
                field.isNullable(),
                ArrowType.List.INSTANCE,
                field.getDictionary(),
                field.getMetadata())
            : field.getFieldType();
    return new Field(
        field.getName(),
        fieldType,
        field.getChildren().stream()
            .map(ArrowBatchAdapter::toListField)
            .collect(Collectors.toList()));
  }

  /**
   * Loads {@code source} into {@code target}, a vector of {@code toListField(source.getField())} or
   * a signed 64-bit vector over an unsigned one, sharing or moving the buffers of {@code source}
   * rather than copying values. A FixedSizeList becomes a List over the same child values: element
   * {@code i} of a FixedSizeList of size {@code n} is child range {@code [i * n, (i + 1) * n)},
   * also for null entries, so only an offsets buffer is new.
   */
  private static void load(FieldVector source, FieldVector target, BufferAllocator allocator) {
    if (!containsFixedSizeList(source.getField()) && !isUnsigned64(source.getField())) {
      // Same type on both sides: move the buffers.
      source.makeTransferPair(target).transfer();
      return;
    }
    ArrowFieldNode node = new ArrowFieldNode(source.getValueCount(), source.getNullCount());
    if (source instanceof FixedSizeListVector) {
      FixedSizeListVector fixed = (FixedSizeListVector) source;
      long width = BaseRepeatedValueVector.OFFSET_WIDTH;
      try (ArrowBuf offsets = allocator.buffer((fixed.getValueCount() + 1L) * width)) {
        for (int i = 0; i <= fixed.getValueCount(); i++) {
          offsets.setInt(i * width, i * fixed.getListSize());
        }
        // loadFieldBuffers takes its own reference to the offsets.
        target.loadFieldBuffers(node, Arrays.asList(fixed.getValidityBuffer(), offsets));
      }
    } else {
      target.loadFieldBuffers(node, source.getFieldBuffers());
    }
    List<FieldVector> sourceChildren = source.getChildrenFromFields();
    List<FieldVector> targetChildren = target.getChildrenFromFields();
    for (int i = 0; i < sourceChildren.size(); i++) {
      load(sourceChildren.get(i), targetChildren.get(i), allocator);
    }
  }

  /** The type of {@code field} and of its descendants, e.g. {@code List<Int(8, true)>}. */
  private static String typeTree(Field field) {
    if (field.getChildren().isEmpty()) {
      return field.getType().toString();
    }
    return field.getType()
        + field.getChildren().stream()
            .map(ArrowBatchAdapter::typeTree)
            .collect(Collectors.joining(",", "<", ">"));
  }

  /** The Arrow schema of a scan's Arrow batches, or why the scan has no Arrow batch form. */
  public static final class Plan implements Serializable {
    private static final long serialVersionUID = 1L;

    // Arrow JSON, as Arrow's Schema is not serializable.
    private final String schemaJson;
    private final String unsupportedReason;

    private Plan(String schemaJson, String unsupportedReason) {
      this.schemaJson = schemaJson;
      this.unsupportedReason = unsupportedReason;
    }

    static Plan supported(Schema schema) {
      return new Plan(schema.toJson(), null);
    }

    public static Plan unsupported(String reason) {
      return new Plan(null, reason);
    }

    /** Whether the scan can emit Arrow batches. */
    public boolean isSupported() {
      return unsupportedReason == null;
    }

    /**
     * The Arrow schema of the batches.
     *
     * @throws IllegalStateException if the scan cannot emit Arrow batches
     */
    public Schema schema() {
      if (!isSupported()) {
        throw new IllegalStateException("No Arrow batches: " + unsupportedReason);
      }
      try {
        return Schema.fromJSON(schemaJson);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    /** Why the scan cannot emit Arrow batches, or null if it can. */
    public String unsupportedReason() {
      return unsupportedReason;
    }
  }
}
