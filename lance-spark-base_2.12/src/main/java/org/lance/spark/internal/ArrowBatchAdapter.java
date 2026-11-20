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
import org.lance.spark.utils.Float16Utils;
import org.lance.spark.utils.JsonUtils;
import org.lance.spark.utils.Utils;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
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
      Map<String, Field> datasetFields = new HashMap<>();
      for (Field datasetField : datasetSchema.getFields()) {
        datasetFields.putIfAbsent(datasetField.getName(), datasetField);
      }
      List<Field> fields = new ArrayList<>(scanSchema.size());
      for (StructField field : scanSchema.fields()) {
        Field datasetField = datasetFields.get(field.name());
        Optional<String> unsupported = unsupportedReason(field, datasetField);
        if (unsupported.isPresent()) {
          return Plan.unsupported(unsupported.get());
        }
        Field arrowField = batchField(field, datasetField);
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
        } else if (needsConversion(nativeVector.getField())) {
          vector = toBatchField(nativeVector.getField()).createVector(allocator);
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

  /**
   * Why {@code field}, with dataset field {@code datasetField} (null if none), has no Arrow batch
   * form whatever its Arrow type, if it has none.
   */
  private static Optional<String> unsupportedReason(StructField field, Field datasetField) {
    String name = field.name();
    if (name.endsWith(LanceConstant.BLOB_POSITION_SUFFIX)
        || name.endsWith(LanceConstant.BLOB_SIZE_SUFFIX)
        || BlobUtils.isBlobReadColumn(field)) {
      return Optional.of(
          "column " + name + " is a blob column, which the Spark reader builds on the JVM");
    }
    // Lance reads a blob at any depth as its descriptor, which the declared type does not show.
    Optional<String> blob =
        datasetField == null ? Optional.empty() : blobPath(datasetField, datasetField.getName());
    if (blob.isPresent()) {
      return Optional.of(
          "column "
              + name
              + " holds blob "
              + blob.get()
              + ", which Lance reads as a blob descriptor");
    }
    if (JsonUtils.hasJsonMetadata(field.metadata())) {
      return Optional.of("column " + name + " is a JSON column, which the Spark reader decodes");
    }
    if (Utils.isScoreMetadataColumn(field)) {
      return Optional.of("column " + name + " is the full-text score");
    }
    return Optional.empty();
  }

  /**
   * The Arrow field of the batch column for {@code field}, with dataset field {@code datasetField},
   * or null if it has none.
   */
  private static Field batchField(StructField field, Field datasetField) {
    String name = field.name();
    if (name.equals(LanceConstant.FRAGMENT_ID)) {
      return fragmentIdField();
    }
    if (isRowMetadataColumn(name)) {
      return signed64Field(name);
    }
    if (datasetField == null || !needsConversion(datasetField)) {
      return datasetField;
    }
    // The field of the vector buildColumns() builds, as Arrow names list children itself.
    try (FieldVector vector = toBatchField(datasetField).createVector(LanceRuntime.allocator())) {
      return vector.getField();
    }
  }

  /**
   * The path of the first blob in {@code field}, whose path is {@code path}, if it is or has one.
   */
  private static Optional<String> blobPath(Field field, String path) {
    if (BlobUtils.isLanceBlobArrowField(field)) {
      return Optional.of(path);
    }
    for (Field child : field.getChildren()) {
      Optional<String> blob = blobPath(child, path + "." + child.getName());
      if (blob.isPresent()) {
        return blob;
      }
    }
    return Optional.empty();
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

  /**
   * The Arrow type of the batch column for a value of Arrow type {@code type}: the type that
   * Spark's {@code ArrowColumnVector} reads as the Spark type the scan declares for it ({@code
   * LanceArrowUtils}), which is {@code type} itself unless Spark does not read it. Lists of any
   * size become Lists, fixed-size binaries become binaries, unsigned integers become the signed
   * integers that hold them (64-bit ones keep their bits), Float16 becomes Float32, and dates and
   * timestamps in other units become days and microseconds.
   */
  private static ArrowType batchType(ArrowType type) {
    if (type instanceof ArrowType.FixedSizeList || type instanceof ArrowType.LargeList) {
      return ArrowType.List.INSTANCE;
    }
    if (type instanceof ArrowType.FixedSizeBinary) {
      return ArrowType.Binary.INSTANCE;
    }
    if (type instanceof ArrowType.Int && !((ArrowType.Int) type).getIsSigned()) {
      int bitWidth = ((ArrowType.Int) type).getBitWidth();
      return new ArrowType.Int(bitWidth == 64 ? 64 : bitWidth * 2, true);
    }
    if (type instanceof ArrowType.FloatingPoint
        && ((ArrowType.FloatingPoint) type).getPrecision() == FloatingPointPrecision.HALF) {
      return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    }
    if (type instanceof ArrowType.Date
        && ((ArrowType.Date) type).getUnit() == DateUnit.MILLISECOND) {
      return new ArrowType.Date(DateUnit.DAY);
    }
    if (type instanceof ArrowType.Timestamp
        && ((ArrowType.Timestamp) type).getUnit() != TimeUnit.MICROSECOND) {
      return new ArrowType.Timestamp(
          TimeUnit.MICROSECOND, ((ArrowType.Timestamp) type).getTimezone());
    }
    return type;
  }

  /**
   * Whether {@code field} or one of its descendants has a different type in the batch. A
   * dictionary-encoded field holds indices rather than values, so it is not converted.
   */
  private static boolean needsConversion(Field field) {
    return field.getDictionary() == null
        && (!batchType(field.getType()).equals(field.getType())
            || field.getChildren().stream().anyMatch(ArrowBatchAdapter::needsConversion));
  }

  /** {@code field} with the batch type ({@link #batchType}) at every depth. */
  private static Field toBatchField(Field field) {
    if (!needsConversion(field)) {
      return field;
    }
    ArrowType type = batchType(field.getType());
    FieldType fieldType =
        new FieldType(field.isNullable(), type, field.getDictionary(), field.getMetadata());
    return new Field(
        field.getName(),
        fieldType,
        field.getChildren().stream()
            .map(ArrowBatchAdapter::toBatchField)
            .collect(Collectors.toList()));
  }

  /**
   * Loads {@code source} into {@code target}, a vector of {@code toBatchField(source.getField())}
   * or a signed 64-bit vector over an unsigned one. Where the layouts agree, the buffers of {@code
   * source} are shared or moved rather than copied:
   *
   * <ul>
   *   <li>A FixedSizeList becomes a List over the same child values: element {@code i} of a
   *       FixedSizeList of size {@code n} is child range {@code [i * n, (i + 1) * n)}, also for
   *       null entries, so only an offsets buffer is new. A fixed-size binary becomes a binary over
   *       the same bytes the same way.
   *   <li>A LargeList becomes a List over the same child values with 32-bit offsets.
   *   <li>An unsigned 64-bit integer keeps its bits as a signed one, as the Spark reader reads it.
   * </ul>
   *
   * Other converted values are copied, rescaled as the Spark reader rescales them.
   */
  private static void load(FieldVector source, FieldVector target, BufferAllocator allocator) {
    Field field = source.getField();
    if (!needsConversion(field) && !isUnsigned64(field)) {
      // Same type on both sides: move the buffers.
      source.makeTransferPair(target).transfer();
      return;
    }
    int rows = source.getValueCount();
    ArrowFieldNode node = new ArrowFieldNode(rows, source.getNullCount());
    long width = BaseRepeatedValueVector.OFFSET_WIDTH;
    if (source instanceof FixedSizeListVector) {
      FixedSizeListVector fixed = (FixedSizeListVector) source;
      try (ArrowBuf offsets = allocator.buffer((rows + 1L) * width)) {
        for (int i = 0; i <= rows; i++) {
          offsets.setInt(i * width, Math.multiplyExact(i, fixed.getListSize()));
        }
        // loadFieldBuffers takes its own reference to the offsets.
        target.loadFieldBuffers(node, Arrays.asList(fixed.getValidityBuffer(), offsets));
      }
    } else if (source instanceof LargeListVector) {
      ArrowBuf largeOffsets = ((LargeListVector) source).getOffsetBuffer();
      try (ArrowBuf offsets = allocator.buffer((rows + 1L) * width)) {
        for (int i = 0; i <= rows; i++) {
          offsets.setInt(
              i * width,
              Math.toIntExact(largeOffsets.getLong((long) i * LargeListVector.OFFSET_WIDTH)));
        }
        target.loadFieldBuffers(node, Arrays.asList(source.getValidityBuffer(), offsets));
      }
    } else if (source instanceof FixedSizeBinaryVector) {
      int byteWidth = ((FixedSizeBinaryVector) source).getByteWidth();
      try (ArrowBuf offsets = allocator.buffer((rows + 1L) * width)) {
        for (int i = 0; i <= rows; i++) {
          offsets.setInt(i * width, Math.multiplyExact(i, byteWidth));
        }
        target.loadFieldBuffers(
            node, Arrays.asList(source.getValidityBuffer(), offsets, source.getDataBuffer()));
      }
      return;
    } else if (isUnsigned64(field)) {
      target.loadFieldBuffers(node, source.getFieldBuffers());
      return;
    } else if (!batchType(field.getType()).equals(field.getType())) {
      convertValues(source, target);
      return;
    } else {
      target.loadFieldBuffers(node, source.getFieldBuffers());
    }
    List<FieldVector> sourceChildren = source.getChildrenFromFields();
    List<FieldVector> targetChildren = target.getChildrenFromFields();
    for (int i = 0; i < sourceChildren.size(); i++) {
      load(sourceChildren.get(i), targetChildren.get(i), allocator);
    }
  }

  /**
   * Copies the values of {@code source}, a scalar vector whose batch type differs from its own,
   * into {@code target}, converted as the Spark reader converts them: unsigned integers widened,
   * Float16 to Float32, milliseconds since the epoch to days by floor division, and timestamps to
   * microseconds, multiplied exactly or divided by floor division.
   */
  private static void convertValues(FieldVector source, FieldVector target) {
    int rows = source.getValueCount();
    target.setInitialCapacity(rows);
    target.allocateNew();
    ArrowType type = source.getField().getType();
    for (int i = 0; i < rows; i++) {
      if (source.isNull(i)) {
        continue;
      }
      if (source instanceof UInt1Vector) {
        ((SmallIntVector) target).set(i, (short) Byte.toUnsignedInt(((UInt1Vector) source).get(i)));
      } else if (source instanceof UInt2Vector) {
        ((IntVector) target).set(i, ((UInt2Vector) source).get(i));
      } else if (source instanceof UInt4Vector) {
        ((BigIntVector) target).set(i, Integer.toUnsignedLong(((UInt4Vector) source).get(i)));
      } else if (type instanceof ArrowType.FloatingPoint
          && ((ArrowType.FloatingPoint) type).getPrecision() == FloatingPointPrecision.HALF) {
        short halfBits = source.getDataBuffer().getShort((long) i * 2);
        ((Float4Vector) target).set(i, Float16Utils.halfToFloat(halfBits));
      } else if (source instanceof DateMilliVector) {
        long millis = ((DateMilliVector) source).get(i);
        ((DateDayVector) target).set(i, (int) Math.floorDiv(millis, MILLIS_PER_DAY));
      } else if (source instanceof TimeStampVector) {
        long raw = ((TimeStampVector) source).get(i);
        ((TimeStampVector) target).set(i, toMicros(raw, ((ArrowType.Timestamp) type).getUnit()));
      } else {
        throw new IllegalStateException("No Arrow batch conversion for " + type);
      }
    }
    target.setValueCount(rows);
  }

  private static final long MILLIS_PER_DAY = 86_400_000L;

  private static long toMicros(long raw, TimeUnit unit) {
    switch (unit) {
      case SECOND:
        return Math.multiplyExact(raw, 1_000_000L);
      case MILLISECOND:
        return Math.multiplyExact(raw, 1_000L);
      case NANOSECOND:
        return Math.floorDiv(raw, 1_000L);
      default:
        return raw;
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
