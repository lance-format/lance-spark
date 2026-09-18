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

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for JSON column detection and the Arrow/Spark type mapping it drives. */
public class JsonUtilsTest {

  private static final String EXT_KEY = "ARROW:extension:name";

  private static Field arrowField(ArrowType type, String extensionName) {
    Map<String, String> metadata = new HashMap<>();
    if (extensionName != null) {
      metadata.put(EXT_KEY, extensionName);
    }
    return new Field("payload", new FieldType(true, type, null, metadata), null);
  }

  private static StructField sparkField(String extensionName) {
    Metadata metadata =
        extensionName == null
            ? Metadata.empty()
            : new MetadataBuilder().putString(EXT_KEY, extensionName).build();
    return new StructField("payload", DataTypes.StringType, true, metadata);
  }

  @Test
  public void testRecognizesBothExtensionNames() {
    assertTrue(JsonUtils.isJsonExtensionName("arrow.json"), "the canonical declared name");
    assertTrue(
        JsonUtils.isJsonExtensionName("lance.json"),
        "the internal name lance-core reports when a dataset is reopened");
    assertFalse(JsonUtils.isJsonExtensionName("lance.blob.v2"));
    assertFalse(JsonUtils.isJsonExtensionName(null));
  }

  /**
   * Lance reports JSON columns as LargeBinary because Arrow Java does not register the extension,
   * so detection must not depend on the storage type.
   */
  @Test
  public void testDetectsJsonRegardlessOfStorageType() {
    assertTrue(JsonUtils.hasJsonArrowExtension(arrowField(ArrowType.Utf8.INSTANCE, "arrow.json")));
    assertTrue(JsonUtils.hasJsonArrowExtension(arrowField(ArrowType.LargeUtf8.INSTANCE, "arrow.json")));
    assertTrue(
        JsonUtils.hasJsonArrowExtension(arrowField(ArrowType.LargeBinary.INSTANCE, "lance.json")));
    assertFalse(JsonUtils.hasJsonArrowExtension(arrowField(ArrowType.Utf8.INSTANCE, null)));
    assertFalse(JsonUtils.hasJsonArrowExtension(null));
  }

  @Test
  public void testDetectsPhysicalLanceJsonFields() {
    assertTrue(
        JsonUtils.isLanceJsonField(arrowField(ArrowType.LargeBinary.INSTANCE, "lance.json")));
    assertFalse(JsonUtils.isLanceJsonField(arrowField(ArrowType.Utf8.INSTANCE, "arrow.json")));
    assertFalse(JsonUtils.isLanceJsonField(null));
  }

  @Test
  public void testSparkFieldDetectionRequiresStringType() {
    assertTrue(JsonUtils.isJsonSparkField(sparkField("arrow.json")));
    assertFalse(JsonUtils.isJsonSparkField(sparkField(null)));
    assertFalse(
        JsonUtils.isJsonSparkField(
            new StructField(
                "payload",
                DataTypes.BinaryType,
                true,
                new MetadataBuilder().putString(EXT_KEY, "arrow.json").build())),
        "the marker alone must not make a binary column JSON");
  }

  @Test
  public void testJsonArrowFieldMapsToStringType() {
    assertEquals(
        DataTypes.StringType,
        LanceArrowUtils.fromArrowField(arrowField(ArrowType.LargeBinary.INSTANCE, "lance.json")),
        "a JSON column reported as LargeBinary must still surface as a string");
  }

  /** A LargeBinary column with no JSON marker keeps its existing mapping. */
  @Test
  public void testPlainLargeBinaryStillMapsToBinaryType() {
    assertEquals(
        DataTypes.BinaryType,
        LanceArrowUtils.fromArrowField(arrowField(ArrowType.LargeBinary.INSTANCE, null)));
  }

  /**
   * The write path must emit the canonical name over UTF-8 storage. Echoing back the internal
   * {@code lance.json} spelling is silently ignored by lance-core on a UTF-8 field, producing a
   * plain string column and an append rejected as {@code should have type json}.
   */
  @Test
  public void testWriteTranslatesInternalNameToCanonicalName() {
    Metadata readMetadata = new MetadataBuilder().putString(EXT_KEY, "lance.json").build();

    Field written =
        LanceArrowUtils.toArrowField(
            "payload", DataTypes.StringType, true, null, readMetadata, false);

    assertEquals(ArrowType.Utf8.INSTANCE, written.getType(), "JSON storage must be UTF-8");
    assertEquals(
        JsonUtils.ARROW_JSON_EXTENSION_NAME,
        written.getMetadata().get(EXT_KEY),
        "the internal name must be translated to the canonical one on write");
  }

  @Test
  public void testWritePreservesCanonicalName() {
    Metadata metadata = new MetadataBuilder().putString(EXT_KEY, "arrow.json").build();

    Field written =
        LanceArrowUtils.toArrowField("payload", DataTypes.StringType, true, null, metadata, false);

    assertEquals(ArrowType.Utf8.INSTANCE, written.getType());
    assertEquals(JsonUtils.ARROW_JSON_EXTENSION_NAME, written.getMetadata().get(EXT_KEY));
  }

  /** A JSON column read back and written again must not pick up a large-binary marker. */
  @Test
  public void testJsonFieldDoesNotGetLargeBinaryMarker() {
    StructType sparkSchema =
        LanceArrowUtils.fromArrowSchema(
            new Schema(
                Collections.singletonList(
                    arrowField(ArrowType.LargeBinary.INSTANCE, "lance.json"))));
    StructField sparkField = sparkSchema.apply("payload");

    assertEquals(DataTypes.StringType, sparkField.dataType());
    assertEquals(
        JsonUtils.ARROW_JSON_EXTENSION_NAME,
        sparkField.metadata().getString(EXT_KEY),
        "Spark metadata must use the canonical Arrow extension name");
    assertFalse(
        sparkField.metadata().contains(LargeVarBinaryUtils.ARROW_LARGE_VAR_BINARY_KEY),
        "a large-binary marker would contradict the StringType mapping and steer writeback "
            + "back to LargeBinary");

    Field written =
        LanceArrowUtils.toArrowField(
            "payload", DataTypes.StringType, true, null, sparkField.metadata(), false);
    assertEquals(ArrowType.Utf8.INSTANCE, written.getType());
    assertEquals(JsonUtils.ARROW_JSON_EXTENSION_NAME, written.getMetadata().get(EXT_KEY));
  }
}
