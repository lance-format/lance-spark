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
import org.lance.spark.utils.BlobUtils;
import org.lance.spark.utils.JsonUtils;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.expressions.MetadataAttribute;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ArrowBatchAdapter#plan}, which decides at planning time whether a scan emits Arrow
 * batches.
 */
public class ArrowBatchAdapterTest {

  private static Field field(String name, ArrowType type, Field... children) {
    return new Field(name, FieldType.nullable(type), Arrays.asList(children));
  }

  private static Field vector(String name, int size, ArrowType element) {
    return field(name, new ArrowType.FixedSizeList(size), field("item", element));
  }

  private static final ArrowType FLOAT = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);

  /** Plans a scan of the whole dataset, read as the Spark reader reads it. */
  private static ArrowBatchAdapter.Plan planWholeDataset(Field... datasetFields) {
    Schema dataset = new Schema(Arrays.asList(datasetFields));
    return ArrowBatchAdapter.plan(LanceArrowUtils.fromArrowSchema(dataset), dataset);
  }

  @Test
  public void plainColumnsKeepTheirArrowTypes() {
    Field id = field("id", new ArrowType.Int(64, true));
    Field name = field("name", ArrowType.Utf8.INSTANCE);
    Field tags = field("tags", ArrowType.List.INSTANCE, field("item", ArrowType.Utf8.INSTANCE));
    ArrowBatchAdapter.Plan plan = planWholeDataset(id, name, tags);
    assertTrue(plan.isSupported(), plan.unsupportedReason());
    assertEquals(new Schema(Arrays.asList(id, name, tags)), plan.schema());
  }

  @Test
  public void vectorsBecomeListsAtAnyDepth() {
    ArrowBatchAdapter.Plan plan =
        planWholeDataset(
            vector("vec", 4, FLOAT),
            vector("i8", 2, new ArrowType.Int(8, true)),
            field("doc", ArrowType.Struct.INSTANCE, vector("emb", 3, FLOAT)),
            field("multi", ArrowType.List.INSTANCE, vector("item", 2, FLOAT)),
            field(
                "nested",
                new ArrowType.FixedSizeList(2),
                vector("item", 2, new ArrowType.Int(32, true))));
    assertTrue(plan.isSupported(), plan.unsupportedReason());
    Schema schema = plan.schema();
    assertEquals(ArrowType.List.INSTANCE, schema.findField("vec").getType());
    assertEquals(ArrowType.List.INSTANCE, schema.findField("i8").getType());
    assertEquals(ArrowType.List.INSTANCE, schema.findField("doc").getChildren().get(0).getType());
    assertEquals(ArrowType.List.INSTANCE, schema.findField("multi").getChildren().get(0).getType());
    Field nested = schema.findField("nested");
    assertEquals(ArrowType.List.INSTANCE, nested.getType());
    assertEquals(ArrowType.List.INSTANCE, nested.getChildren().get(0).getType());
  }

  @Test
  public void metadataColumnsBecomeSignedIntegers() {
    Schema dataset = new Schema(Collections.singletonList(field("x", new ArrowType.Int(64, true))));
    StructType scan =
        new StructType()
            .add("x", DataTypes.LongType)
            .add(LanceConstant.ROW_ID, DataTypes.LongType)
            .add(LanceConstant.ROW_ADDRESS, DataTypes.LongType)
            .add(LanceConstant.ROW_CREATED_AT_VERSION, DataTypes.LongType)
            .add(LanceConstant.ROW_LAST_UPDATED_AT_VERSION, DataTypes.LongType)
            .add(LanceConstant.FRAGMENT_ID, DataTypes.IntegerType);
    ArrowBatchAdapter.Plan plan = ArrowBatchAdapter.plan(scan, dataset);
    assertTrue(plan.isSupported(), plan.unsupportedReason());
    Schema schema = plan.schema();
    for (int i = 0; i < 5; i++) {
      assertEquals(new ArrowType.Int(64, true), schema.getFields().get(i).getType());
    }
    assertEquals(
        new ArrowType.Int(32, true), schema.findField(LanceConstant.FRAGMENT_ID).getType());
  }

  /** An empty projection reads only row counts, so its batches have no column. */
  @Test
  public void emptyProjectionHasNoColumns() {
    ArrowBatchAdapter.Plan plan =
        ArrowBatchAdapter.plan(
            new StructType(),
            new Schema(Collections.singletonList(field("x", new ArrowType.Int(64, true)))));
    assertTrue(plan.isSupported(), plan.unsupportedReason());
    assertTrue(plan.schema().getFields().isEmpty());
  }

  @Test
  public void typesSparkDoesNotReadAsDeclaredHaveNoArrowBatchForm() {
    for (Field unsupported :
        Arrays.asList(
            field("u", new ArrowType.Int(32, false)),
            field("fsb", new ArrowType.FixedSizeBinary(16)),
            field("ns", new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)),
            field("doc", ArrowType.Struct.INSTANCE, field("u", new ArrowType.Int(64, false))))) {
      ArrowBatchAdapter.Plan plan = planWholeDataset(unsupported);
      assertFalse(plan.isSupported(), unsupported.toString());
      assertTrue(
          plan.unsupportedReason().startsWith("column " + unsupported.getName() + ": "),
          plan.unsupportedReason());
      assertThrows(IllegalStateException.class, plan::schema);
    }
  }

  /**
   * Spark reads struct fields by position, so a struct whose Arrow children do not line up with the
   * declared struct, with fields missing on either side, has no Arrow batch form.
   */
  @Test
  public void structsMustMatchTheDeclaredFields() {
    Schema dataset =
        new Schema(
            Collections.singletonList(
                field(
                    "doc",
                    ArrowType.Struct.INSTANCE,
                    field("label", ArrowType.Utf8.INSTANCE),
                    field("score", FLOAT))));
    StructType fewer =
        new StructType().add("doc", new StructType().add("label", DataTypes.StringType));
    StructType more =
        new StructType()
            .add(
                "doc",
                new StructType()
                    .add("label", DataTypes.StringType)
                    .add("score", DataTypes.FloatType)
                    .add("extra", DataTypes.StringType));
    for (StructType scan : Arrays.asList(fewer, more)) {
      ArrowBatchAdapter.Plan plan = ArrowBatchAdapter.plan(scan, dataset);
      assertFalse(plan.isSupported(), scan.simpleString());
      assertTrue(plan.unsupportedReason().startsWith("column doc: "), plan.unsupportedReason());
    }
  }

  @Test
  public void blobJsonAndScoreColumnsHaveNoArrowBatchForm() {
    Schema dataset = new Schema(Collections.singletonList(field("x", ArrowType.Utf8.INSTANCE)));
    Metadata blob =
        new MetadataBuilder()
            .putString(BlobUtils.LANCE_ENCODING_BLOB_KEY, BlobUtils.LANCE_ENCODING_BLOB_VALUE)
            .build();
    Metadata json =
        new MetadataBuilder()
            .putString(BlobUtils.ARROW_EXTENSION_NAME_KEY, JsonUtils.ARROW_JSON_EXTENSION_NAME)
            .build();
    DataType score = DataTypes.FloatType;
    Metadata scoreMetadata = MetadataAttribute.apply(LanceConstant.SCORE, score, true).metadata();
    for (StructType scan :
        Arrays.asList(
            new StructType().add("x", DataTypes.BinaryType, true, blob),
            new StructType().add("x", DataTypes.StringType, true, json),
            new StructType().add("x" + LanceConstant.BLOB_SIZE_SUFFIX, DataTypes.LongType),
            new StructType().add(LanceConstant.SCORE, score, true, scoreMetadata))) {
      ArrowBatchAdapter.Plan plan = ArrowBatchAdapter.plan(scan, dataset);
      assertFalse(plan.isSupported(), scan.simpleString());
    }
  }

  @Test
  public void columnsMissingFromTheDatasetHaveNoArrowBatchForm() {
    ArrowBatchAdapter.Plan plan =
        ArrowBatchAdapter.plan(
            new StructType().add("y", DataTypes.LongType),
            new Schema(Collections.singletonList(field("x", new ArrowType.Int(64, true)))));
    assertFalse(plan.isSupported());
    assertTrue(plan.unsupportedReason().contains("column y"), plan.unsupportedReason());
  }
}
