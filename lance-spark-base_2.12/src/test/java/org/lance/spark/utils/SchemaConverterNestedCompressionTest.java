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
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Coverage for nested-column compression metadata on TBLPROPERTIES (issue #434). All cases call
 * {@link SchemaConverter#processSchemaWithProperties} then {@link LanceArrowUtils#toArrowSchema}
 * and walk the resulting Arrow {@link Schema} to assert metadata lands on the right Arrow {@link
 * Field}.
 */
public class SchemaConverterNestedCompressionTest {

  @Test
  public void testNestedStructNewFormat() {
    StructType schema =
        topStruct(
            structField(
                "events",
                DataTypes.createStructType(
                    new StructField[] {
                      DataTypes.createStructField("id", DataTypes.IntegerType, false),
                      DataTypes.createStructField("payload", DataTypes.StringType, true),
                    })));
    Map<String, String> props = new HashMap<>();
    props.put("lance.compression.column.events.payload", "zstd");

    Schema arrow = process(schema, props);
    Field payload = walk(arrow, "events", "payload");
    assertEquals("zstd", arrowMetadata(payload).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
    assertOuterClean(walk(arrow, "events"));
  }

  @Test
  public void testLegacyFormatTopLevelStillWorks() {
    StructType schema = topStruct(structField("payload", DataTypes.StringType));
    Map<String, String> props = new HashMap<>();
    props.put("payload.lance.compression", "zstd");

    Schema arrow = process(schema, props);
    Field payload = walk(arrow, "payload");
    assertEquals("zstd", arrowMetadata(payload).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testLegacyFormatWinsForTopLevelColumnNameThatLooksLikeNewFormat() {
    String columnName = "lance.compression.column.a..b";
    StructType schema = topStruct(structField(columnName, DataTypes.StringType));
    Map<String, String> props = new HashMap<>();
    props.put(columnName + ".lance.compression", "zstd");

    Schema arrow = process(schema, props);
    Field field = walk(arrow, columnName);
    assertEquals("zstd", arrowMetadata(field).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testStaleLegacyFormatThatLooksLikeMalformedNewFormatIsIgnored() {
    StructType schema = topStruct(structField("payload", DataTypes.StringType));
    Map<String, String> props = new HashMap<>();
    props.put("lance.compression.column.a..b.lance.compression", "zstd");

    Schema arrow = process(schema, props);
    Field field = walk(arrow, "payload");
    assertFalse(arrowMetadata(field).containsKey(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testLegacyFormatDoesNotStampNestedPathWhenColumnNameLooksLikeNewFormat() {
    String columnName = "lance.compression.column.payload";
    StructType schema =
        new StructType(
            new StructField[] {
              structField(
                  "payload",
                  DataTypes.createStructType(
                      new StructField[] {
                        structField(
                            "lance",
                            DataTypes.createStructType(
                                new StructField[] {
                                  structField("compression", DataTypes.StringType)
                                }))
                      })),
              structField(columnName, DataTypes.StringType)
            });
    Map<String, String> props = new HashMap<>();
    props.put(columnName + ".lance.compression", "zstd");

    Schema arrow = process(schema, props);
    Field legacyField = walk(arrow, columnName);
    Field nestedField = walk(arrow, "payload", "lance", "compression");
    assertEquals(
        "zstd", arrowMetadata(legacyField).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
    assertFalse(
        arrowMetadata(nestedField).containsKey(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testLegacyShapedKeyDoesNotTargetNestedPathWhenTopLevelColumnIsStale() {
    StructType schema =
        topStruct(
            structField(
                "payload",
                DataTypes.createStructType(
                    new StructField[] {
                      structField(
                          "lance",
                          DataTypes.createStructType(
                              new StructField[] {structField("compression", DataTypes.StringType)}))
                    })));
    Map<String, String> props = new HashMap<>();
    props.put("lance.compression.column.payload.lance.compression", "zstd");

    Schema arrow = process(schema, props);
    Field field = walk(arrow, "payload", "lance", "compression");
    assertFalse(arrowMetadata(field).containsKey(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testLegacyFormatDoesNotReachNested() {
    StructType schema =
        topStruct(
            structField(
                "events",
                DataTypes.createStructType(
                    new StructField[] {
                      DataTypes.createStructField("payload", DataTypes.StringType, true),
                    })));
    Map<String, String> props = new HashMap<>();
    props.put("events.payload.lance.compression", "zstd");

    Schema arrow = process(schema, props);
    Field payload = walk(arrow, "events", "payload");
    assertNull(arrowMetadata(payload).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testNewFormatOverridesLegacyAtTopLevel() {
    StructType schema = topStruct(structField("payload", DataTypes.StringType));
    Map<String, String> props = new HashMap<>();
    props.put("payload.lance.compression", "zstd");
    props.put("lance.compression.column.payload", "lz4");

    Schema arrow = process(schema, props);
    Field payload = walk(arrow, "payload");
    assertEquals("lz4", arrowMetadata(payload).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testArrayElementListCompression() {
    StructType schema =
        topStruct(structField("tags", DataTypes.createArrayType(DataTypes.StringType, true)));
    Map<String, String> props = new HashMap<>();
    props.put("lance.compression.column.tags.element", "lz4");

    Schema arrow = process(schema, props);
    Field outer = walk(arrow, "tags");
    assertTrue(outer.getType() instanceof ArrowType.List, "Expected List, got " + outer.getType());
    Field element = walk(arrow, "tags", "element");
    assertEquals("lz4", arrowMetadata(element).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
    assertOuterClean(outer);
  }

  @Test
  public void testArrayElementFixedSizeListCompression() {
    StructType schema =
        topStruct(structField("embeddings", DataTypes.createArrayType(DataTypes.FloatType, false)));
    Map<String, String> props = new HashMap<>();
    props.put("embeddings.arrow.fixed-size-list.size", "128");
    props.put("lance.compression.column.embeddings.element", "zstd");

    Schema arrow = process(schema, props);
    Field outer = walk(arrow, "embeddings");
    assertTrue(
        outer.getType() instanceof ArrowType.FixedSizeList,
        "Expected FixedSizeList, got " + outer.getType());
    Field element = walk(arrow, "embeddings", "element");
    assertEquals("zstd", arrowMetadata(element).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
    assertOuterClean(outer);
  }

  @Test
  public void testMapKeyCompression() {
    StructType schema =
        topStruct(
            structField(
                "props",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true)));
    Map<String, String> props = new HashMap<>();
    props.put("lance.compression.column.props.key", "zstd");

    Schema arrow = process(schema, props);
    Field key = walk(arrow, "props", "key");
    assertEquals("zstd", arrowMetadata(key).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
    assertOuterClean(walk(arrow, "props"));
  }

  @Test
  public void testMapValueCompression() {
    StructType schema =
        topStruct(
            structField(
                "props",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true)));
    Map<String, String> props = new HashMap<>();
    props.put("lance.compression.column.props.value", "zstd");

    Schema arrow = process(schema, props);
    Field value = walk(arrow, "props", "value");
    assertEquals("zstd", arrowMetadata(value).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
    assertOuterClean(walk(arrow, "props"));
  }

  @Test
  public void testDeepNestedStruct() {
    StructType schema =
        topStruct(
            structField(
                "a",
                DataTypes.createStructType(
                    new StructField[] {
                      DataTypes.createStructField(
                          "b",
                          DataTypes.createStructType(
                              new StructField[] {
                                DataTypes.createStructField("c", DataTypes.StringType, true),
                              }),
                          true),
                    })));
    Map<String, String> props = new HashMap<>();
    props.put("lance.compression.column.a.b.c", "zstd");

    Schema arrow = process(schema, props);
    Field c = walk(arrow, "a", "b", "c");
    assertEquals("zstd", arrowMetadata(c).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
    assertOuterClean(walk(arrow, "a"));
    assertOuterClean(walk(arrow, "a", "b"));
  }

  @Test
  public void testMixedStructInArrayInStruct() {
    StructType inner =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("body", DataTypes.StringType, true),
            });
    StructType schema =
        topStruct(
            structField(
                "outer",
                DataTypes.createStructType(
                    new StructField[] {
                      DataTypes.createStructField(
                          "items", DataTypes.createArrayType(inner, true), true),
                    })));
    Map<String, String> props = new HashMap<>();
    props.put("lance.compression.column.outer.items.element.body", "zstd");

    Schema arrow = process(schema, props);
    Field body = walk(arrow, "outer", "items", "element", "body");
    assertEquals("zstd", arrowMetadata(body).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testMultipleRulesOnSamePath() {
    StructType schema =
        topStruct(
            structField(
                "events",
                DataTypes.createStructType(
                    new StructField[] {
                      DataTypes.createStructField("payload", DataTypes.StringType, true),
                    })));
    Map<String, String> props = new HashMap<>();
    props.put("lance.compression.column.events.payload", "zstd");
    props.put("lance.compression-level.column.events.payload", "3");

    Schema arrow = process(schema, props);
    Field payload = walk(arrow, "events", "payload");
    Map<String, String> meta = arrowMetadata(payload);
    assertEquals("zstd", meta.get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
    assertEquals("3", meta.get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION_LEVEL));
  }

  @Test
  public void testPathNotFoundIsSilent() {
    StructType schema = topStruct(structField("payload", DataTypes.StringType));
    Map<String, String> props = new HashMap<>();
    props.put("lance.compression.column.nonexistent.sub", "zstd");

    Schema arrow = process(schema, props);
    Field payload = walk(arrow, "payload");
    assertNull(arrowMetadata(payload).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testPathNotFoundLegacyIsSilent() {
    StructType schema = topStruct(structField("payload", DataTypes.StringType));
    Map<String, String> props = new HashMap<>();
    props.put("missing.lance.compression", "zstd");

    Schema arrow = process(schema, props);
    Field payload = walk(arrow, "payload");
    assertNull(arrowMetadata(payload).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testInvalidValueThrowsWithFullPath() {
    StructType schema =
        topStruct(
            structField(
                "events",
                DataTypes.createStructType(
                    new StructField[] {
                      DataTypes.createStructField("payload", DataTypes.StringType, true),
                    })));
    Map<String, String> props = new HashMap<>();
    props.put("lance.compression.column.events.payload", "gzip");

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> SchemaConverter.processSchemaWithProperties(schema, props));
    assertTrue(
        ex.getMessage().contains("events.payload"),
        "Expected error to mention full path 'events.payload', got: " + ex.getMessage());
  }

  @Test
  public void testUnknownEncodingKeyIsSilent() {
    StructType schema = topStruct(structField("payload", DataTypes.StringType));
    Map<String, String> props = new HashMap<>();
    // Shape matches new format (lance prefix + .column.) but rule prefix "lance" matches no rule.
    props.put("lance.column.foo", "zstd");

    Schema arrow = process(schema, props);
    Field payload = walk(arrow, "payload");
    assertNull(arrowMetadata(payload).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testMalformedKeyEmptyPath() {
    StructType schema = topStruct(structField("payload", DataTypes.StringType));
    Map<String, String> props = new HashMap<>();
    props.put("lance.compression.column.", "zstd");

    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaConverter.processSchemaWithProperties(schema, props));
  }

  @Test
  public void testKeyMissingColumnSeparatorIsSilent() {
    StructType schema = topStruct(structField("payload", DataTypes.StringType));
    Map<String, String> props = new HashMap<>();
    // No trailing dot — '.column.' shape predicate fails, no legacy suffix match either.
    props.put("lance.compression.column", "zstd");

    Schema arrow = process(schema, props);
    Field payload = walk(arrow, "payload");
    assertNull(arrowMetadata(payload).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testPathDepthLimitThrows() {
    StructType schema = topStruct(structField("payload", DataTypes.StringType));
    Map<String, String> props = new HashMap<>();
    StringBuilder sb = new StringBuilder("lance.compression.column");
    for (int i = 1; i <= LanceEncodingUtils.MAX_PATH_DEPTH + 1; i++) {
      sb.append(".a").append(i);
    }
    props.put(sb.toString(), "zstd");

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> SchemaConverter.processSchemaWithProperties(schema, props));
    String msg = ex.getMessage();
    assertTrue(
        msg.contains(String.valueOf(LanceEncodingUtils.MAX_PATH_DEPTH)),
        "Expected depth limit value in error, got: " + msg);
    assertTrue(
        msg.contains("segments"),
        "Expected user-friendly 'segments' wording in error, got: " + msg);
  }

  @Test
  public void testStaleLegacyFormatThatLooksLikeOverDepthNewFormatIsIgnored() {
    StructType schema = topStruct(structField("payload", DataTypes.StringType));
    Map<String, String> props = new HashMap<>();
    StringBuilder sb = new StringBuilder("lance.compression.column");
    for (int i = 1; i <= LanceEncodingUtils.MAX_PATH_DEPTH + 1; i++) {
      sb.append(".a").append(i);
    }
    sb.append(".lance.compression");
    props.put(sb.toString(), "zstd");

    Schema arrow = process(schema, props);
    Field payload = walk(arrow, "payload");
    assertFalse(arrowMetadata(payload).containsKey(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testStructChildNamedElementInsideArrayIsUnambiguous() {
    StructType inner =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("element", DataTypes.StringType, true),
            });
    StructType schema =
        topStruct(
            structField(
                "outer",
                DataTypes.createStructType(
                    new StructField[] {
                      DataTypes.createStructField(
                          "items", DataTypes.createArrayType(inner, true), true),
                    })));
    Map<String, String> props = new HashMap<>();
    // outer.items (array) -> element role -> struct -> child literally named "element"
    props.put("lance.compression.column.outer.items.element.element", "zstd");

    Schema arrow = process(schema, props);
    Field el = walk(arrow, "outer", "items", "element", "element");
    assertEquals("zstd", arrowMetadata(el).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  // Float16 path is gated by Arrow 18+ (Spark 4.0+); the base module uses an older Arrow that
  // lacks Float2Vector. Exercise prior-pass coexistence with plain FixedSizeList only here —
  // float16-specific coverage lives in `testPreservesFloat16AndNestedCompression` below.
  @Test
  public void testPreservesEarlierMetadataPasses() {
    StructType schema =
        topStruct(structField("embeddings", DataTypes.createArrayType(DataTypes.FloatType, false)));
    Map<String, String> props = new HashMap<>();
    props.put("embeddings.arrow.fixed-size-list.size", "128");
    props.put("lance.compression.column.embeddings.element", "zstd");

    Schema arrow = process(schema, props);
    Field outer = walk(arrow, "embeddings");
    assertTrue(outer.getType() instanceof ArrowType.FixedSizeList);
    Map<String, String> outerMeta = arrowMetadata(outer);
    assertTrue(
        outerMeta.containsKey("arrow.fixed-size-list.size"),
        "Outer metadata should retain fixed-size-list size: " + outerMeta);
    assertOuterClean(outer);

    Field element = walk(arrow, "embeddings", "element");
    assertEquals("zstd", arrowMetadata(element).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  // float16 + nested-compression coexistence. Skipped on Arrow <18 (Spark 3.4/3.5)
  // and exercised on Spark 4.0+. Pinned here so the float16 inline-element FieldType path in
  // toArrowField does pick up applyAtElement metadata.
  @Test
  public void testPreservesFloat16AndNestedCompression() {
    assumeTrue(
        Float16Utils.isFloat2VectorAvailable(),
        "Float16 vectors require Arrow 18+ (Spark 4.0+); skipping on this runtime.");

    StructType schema =
        topStruct(structField("embeddings", DataTypes.createArrayType(DataTypes.FloatType, false)));
    Map<String, String> props = new HashMap<>();
    props.put("embeddings.arrow.fixed-size-list.size", "128");
    props.put("embeddings.arrow.float16", "true");
    props.put("lance.compression.column.embeddings.element", "zstd");

    Schema arrow = process(schema, props);
    Field outer = walk(arrow, "embeddings");
    assertTrue(outer.getType() instanceof ArrowType.FixedSizeList);
    Map<String, String> outerMeta = arrowMetadata(outer);
    assertTrue(
        outerMeta.containsKey("arrow.fixed-size-list.size"),
        "Outer should retain fixed-size-list size: " + outerMeta);
    assertOuterClean(outer);

    Field element = walk(arrow, "embeddings", "element");
    assertEquals("zstd", arrowMetadata(element).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
    assertTrue(
        element.getType() instanceof ArrowType.FloatingPoint,
        "Expected float16 element, got " + element.getType());
  }

  @Test
  public void testNestedMetadataKeysStrippedFromOuterField() {
    StructType schema =
        topStruct(structField("tags", DataTypes.createArrayType(DataTypes.StringType, true)));
    Map<String, String> props = new HashMap<>();
    props.put("lance.compression.column.tags.element", "lz4");

    Schema arrow = process(schema, props);
    Field outer = walk(arrow, "tags");
    Map<String, String> outerMeta = arrowMetadata(outer);
    for (String k : outerMeta.keySet()) {
      assertFalse(
          k.startsWith(LanceEncodingUtils.LANCE_NESTED_PREFIX),
          "lance-nested.* key leaked onto outer Arrow Field: " + k);
    }
  }

  // Pin: legacy `col.lance.compression-level` routes to compression-level, not compression.
  // (No actual ambiguity today since neither suffix ends-with the other, but pin the behavior.)
  @Test
  public void testLegacyCompressionLevelKeyRoutesToCompressionLevelRule() {
    StructType schema = topStruct(structField("col", DataTypes.StringType));
    Map<String, String> props = new HashMap<>();
    props.put("col.lance.compression-level", "3");

    Schema arrow = process(schema, props);
    Field col = walk(arrow, "col");
    Map<String, String> meta = arrowMetadata(col);
    assertEquals("3", meta.get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION_LEVEL));
    assertNull(
        meta.get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION),
        "compression-level key should NOT match the compression rule");
  }

  @Test
  public void testLegacyDottedTopLevelColumnStillWorks() {
    StructType schema = topStruct(structField("a.b", DataTypes.StringType));
    Map<String, String> props = new HashMap<>();
    props.put("a.b.lance.compression", "zstd");

    Schema arrow = process(schema, props);
    Field f = walk(arrow, "a.b");
    assertEquals("zstd", arrowMetadata(f).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testPathDepthAtLimitSucceeds() {
    final int depth = LanceEncodingUtils.MAX_PATH_DEPTH;
    DataType inner = DataTypes.StringType;
    for (int i = depth; i >= 1; i--) {
      inner =
          DataTypes.createStructType(
              new StructField[] {
                DataTypes.createStructField("l" + i, inner, true),
              });
    }
    // `inner` is now top-level wrapper STRUCT<l1: STRUCT<l2: ... >>; unwrap once for top struct.
    StructType wrap = (StructType) inner;
    StructField only = wrap.fields()[0];
    StructType schema = topStruct(only);

    StringBuilder pathKey = new StringBuilder("lance.compression.column");
    for (int i = 1; i <= depth; i++) {
      pathKey.append(".l").append(i);
    }
    Map<String, String> props = new HashMap<>();
    props.put(pathKey.toString(), "zstd");

    Schema arrow = process(schema, props);
    String[] segs = new String[depth];
    for (int i = 0; i < depth; i++) {
      segs[i] = "l" + (i + 1);
    }
    Field deepest = walk(arrow, segs);
    assertEquals("zstd", arrowMetadata(deepest).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testMapInsideArrayElement() {
    StructType schema =
        topStruct(
            structField(
                "items",
                DataTypes.createArrayType(
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true),
                    true)));
    Map<String, String> props = new HashMap<>();
    props.put("lance.compression.column.items.element.value", "zstd");

    StructType processed = SchemaConverter.processSchemaWithProperties(schema, props);
    StructField items = processed.apply("items");
    String expectedKey =
        LanceEncodingUtils.LANCE_NESTED_PREFIX
            + "element.value."
            + LanceEncodingUtils.LANCE_ENCODING_COMPRESSION;
    assertTrue(
        items.metadata().contains(expectedKey),
        "Expected smuggling key '" + expectedKey + "' on items metadata, got: " + items.metadata());
    assertEquals("zstd", items.metadata().getString(expectedKey));

    Schema arrow = LanceArrowUtils.toArrowSchema(processed, "UTC", false);
    Field value = walk(arrow, "items", "element", "value");
    assertEquals("zstd", arrowMetadata(value).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testArrayInsideMapValue() {
    StructType schema =
        topStruct(
            structField(
                "m",
                DataTypes.createMapType(
                    DataTypes.StringType,
                    DataTypes.createArrayType(DataTypes.StringType, true),
                    true)));
    Map<String, String> props = new HashMap<>();
    props.put("lance.compression.column.m.value.element", "lz4");

    StructType processed = SchemaConverter.processSchemaWithProperties(schema, props);
    StructField m = processed.apply("m");
    String expectedKey =
        LanceEncodingUtils.LANCE_NESTED_PREFIX
            + "value.element."
            + LanceEncodingUtils.LANCE_ENCODING_COMPRESSION;
    assertTrue(
        m.metadata().contains(expectedKey),
        "Expected smuggling key '" + expectedKey + "' on m metadata, got: " + m.metadata());

    Schema arrow = LanceArrowUtils.toArrowSchema(processed, "UTC", false);
    Field elementOfArray = walk(arrow, "m", "value", "element");
    assertEquals(
        "lz4", arrowMetadata(elementOfArray).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  // regression: schema field literally named `column` must not collide with the
  // `.column.` separator. New format: parser uses indexOf which finds the first occurrence.
  @Test
  public void testFieldNamedColumnReachableViaNewFormat() {
    StructType schema = topStruct(structField("column", DataTypes.StringType));
    Map<String, String> props = new HashMap<>();
    props.put("lance.compression.column.column", "zstd");

    Schema arrow = process(schema, props);
    Field f = walk(arrow, "column");
    assertEquals("zstd", arrowMetadata(f).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  // regression: a struct child literally named `column` is reachable.
  @Test
  public void testStructChildLiterallyNamedColumn() {
    StructType schema =
        topStruct(
            structField(
                "outer",
                DataTypes.createStructType(
                    new StructField[] {
                      DataTypes.createStructField("column", DataTypes.StringType, true),
                    })));
    Map<String, String> props = new HashMap<>();
    props.put("lance.compression.column.outer.column", "zstd");

    Schema arrow = process(schema, props);
    Field child = walk(arrow, "outer", "column");
    assertEquals("zstd", arrowMetadata(child).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testInvalidValueOnArrayElementErrorMessageContainsRoleSegmentedPath() {
    StructType schema =
        topStruct(structField("tags", DataTypes.createArrayType(DataTypes.StringType, true)));
    Map<String, String> props = new HashMap<>();
    props.put("lance.compression.column.tags.element", "gzip");

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> SchemaConverter.processSchemaWithProperties(schema, props));
    assertTrue(
        ex.getMessage().contains("tags.element"),
        "Expected role-segmented path 'tags.element' in error, got: " + ex.getMessage());
  }

  @Test
  public void testInvalidValueOnMapKeyErrorMessageContainsRoleSegmentedPath() {
    StructType schema =
        topStruct(
            structField(
                "props",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true)));
    Map<String, String> p = new HashMap<>();
    p.put("lance.compression.column.props.key", "gzip");

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> SchemaConverter.processSchemaWithProperties(schema, p));
    assertTrue(
        ex.getMessage().contains("props.key"),
        "Expected role-segmented path 'props.key' in error, got: " + ex.getMessage());
  }

  @Test
  public void testInvalidValueOnMapValueErrorMessageContainsRoleSegmentedPath() {
    StructType schema =
        topStruct(
            structField(
                "props",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true)));
    Map<String, String> p = new HashMap<>();
    p.put("lance.compression.column.props.value", "gzip");

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> SchemaConverter.processSchemaWithProperties(schema, p));
    assertTrue(
        ex.getMessage().contains("props.value"),
        "Expected role-segmented path 'props.value' in error, got: " + ex.getMessage());
  }

  @Test
  public void testNewFormatSpansMultipleFieldsIndependently() {
    StructType schema =
        new StructType(
            new StructField[] {
              structField("a", DataTypes.StringType),
              structField("b", DataTypes.StringType),
              structField("c", DataTypes.StringType),
            });
    Map<String, String> props = new HashMap<>();
    props.put("lance.compression.column.a", "zstd");
    props.put("lance.compression.column.b", "lz4");
    props.put("lance.bss.column.c", "on");

    Schema arrow = process(schema, props);
    assertEquals(
        "zstd", arrowMetadata(walk(arrow, "a")).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
    assertEquals(
        "lz4", arrowMetadata(walk(arrow, "b")).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
    assertEquals("on", arrowMetadata(walk(arrow, "c")).get(LanceEncodingUtils.LANCE_ENCODING_BSS));
  }

  @Test
  public void testNewFormatOverridesLegacyAvoidsValidationOnLegacyValue() {
    // Stale legacy value 'gzip' (invalid) coexists with valid new-format 'lz4' on the same path.
    // The walker must drop legacy before validation, so this should NOT throw.
    StructType schema = topStruct(structField("payload", DataTypes.StringType));
    Map<String, String> props = new HashMap<>();
    props.put("payload.lance.compression", "gzip");
    props.put("lance.compression.column.payload", "lz4");

    Schema arrow = process(schema, props);
    Field payload = walk(arrow, "payload");
    assertEquals("lz4", arrowMetadata(payload).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testEmptyMiddlePathSegmentThrows() {
    StructType schema = topStruct(structField("payload", DataTypes.StringType));
    Map<String, String> props = new HashMap<>();
    props.put("lance.compression.column.a..b", "zstd");

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> SchemaConverter.processSchemaWithProperties(schema, props));
    assertTrue(
        ex.getMessage().contains("empty path segment at index"),
        "Expected 'empty path segment at index' message, got: " + ex.getMessage());
  }

  @Test
  public void testInvalidStructuralEncodingThrows() {
    StructType schema = topStruct(structField("col", DataTypes.StringType));
    Map<String, String> p = new HashMap<>();
    p.put("lance.structural-encoding.column.col", "bogus");

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> SchemaConverter.processSchemaWithProperties(schema, p));
    assertTrue(
        ex.getMessage().contains("invalid structural-encoding"),
        "Expected 'invalid structural-encoding' message, got: " + ex.getMessage());
  }

  @Test
  public void testInvalidBssModeThrows() {
    StructType schema = topStruct(structField("col", DataTypes.StringType));
    Map<String, String> p = new HashMap<>();
    p.put("lance.bss.column.col", "yes");

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> SchemaConverter.processSchemaWithProperties(schema, p));
    assertTrue(
        ex.getMessage().contains("invalid bss"),
        "Expected 'invalid bss' message, got: " + ex.getMessage());
  }

  @Test
  public void testInvalidRleThresholdNonNumericThrows() {
    StructType schema = topStruct(structField("col", DataTypes.StringType));
    Map<String, String> p = new HashMap<>();
    p.put("lance.rle-threshold.column.col", "abc");

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> SchemaConverter.processSchemaWithProperties(schema, p));
    assertTrue(
        ex.getMessage().contains("invalid rle-threshold"),
        "Expected 'invalid rle-threshold' message, got: " + ex.getMessage());
  }

  @Test
  public void testInvalidRleThresholdNaNAndInfinityRejected() {
    // Regression: `Float.parseFloat("NaN")` returns NaN, for which every numeric comparison is
    // false; the previous range check `threshold <= 0 || threshold > 1` failed open. The fixed
    // positive predicate `!(threshold > 0 && threshold <= 1)` catches NaN and ±Infinity.
    StructType schema = topStruct(structField("col", DataTypes.StringType));
    String[] badValues = {"NaN", "Infinity", "-Infinity"};
    for (String v : badValues) {
      Map<String, String> p = new HashMap<>();
      p.put("lance.rle-threshold.column.col", v);
      IllegalArgumentException ex =
          assertThrows(
              IllegalArgumentException.class,
              () -> SchemaConverter.processSchemaWithProperties(schema, p),
              "Expected IAE for rle-threshold value '" + v + "'");
      assertTrue(
          ex.getMessage().contains("out of range"),
          "Expected 'out of range' message for value '" + v + "', got: " + ex.getMessage());
    }
  }

  @Test
  public void testInvalidRleThresholdOutOfRangeThrows() {
    StructType schema = topStruct(structField("col", DataTypes.StringType));
    Map<String, String> p = new HashMap<>();
    p.put("lance.rle-threshold.column.col", "1.5");

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> SchemaConverter.processSchemaWithProperties(schema, p));
    assertTrue(
        ex.getMessage().contains("out of range"),
        "Expected 'out of range' message, got: " + ex.getMessage());
  }

  @Test
  public void testErrorMessageEscapesNewlinesInColumnName() {
    StructType schema = topStruct(structField("a\nb", DataTypes.StringType));
    Map<String, String> p = new HashMap<>();
    p.put("lance.compression.column.a\nb", "gzip");

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> SchemaConverter.processSchemaWithProperties(schema, p));
    assertTrue(
        ex.getMessage().contains("\\n"),
        "Expected newline in column name to be escaped in message, got: " + ex.getMessage());
    assertFalse(
        ex.getMessage().contains("a\nb"),
        "Raw newline must not leak into error message: " + ex.getMessage());
  }

  @Test
  public void testStrayNonRoleSegmentAfterArrayIsSilentlyIgnored() {
    StructType schema =
        topStruct(structField("tags", DataTypes.createArrayType(DataTypes.StringType, true)));
    Map<String, String> p = new HashMap<>();
    p.put("lance.compression.column.tags.bogus", "zstd");

    Schema arrow = process(schema, p);
    Field tagsElement = walk(arrow, "tags", "element");
    assertNull(
        arrowMetadata(tagsElement).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION),
        "Non-element segment after array must not stamp metadata on the element");
    assertOuterClean(walk(arrow, "tags"));
  }

  @Test
  public void testNullValueOnEachRuleThrowsIllegalArgumentException() {
    // Regression: Float.parseFloat(null) throws NPE, not NumberFormatException — so
    // validateRleThreshold previously leaked NPE through its try/catch. All validators must
    // pre-check null and throw IAE with the standard "Column 'X':" message.
    StructType schema = topStruct(structField("col", DataTypes.StringType));
    String[] keys = {
      "lance.compression.column.col",
      "lance.compression-level.column.col",
      "lance.structural-encoding.column.col",
      "lance.rle-threshold.column.col",
      "lance.bss.column.col",
    };
    for (String k : keys) {
      Map<String, String> p = new HashMap<>();
      p.put(k, null);
      IllegalArgumentException ex =
          assertThrows(
              IllegalArgumentException.class,
              () -> SchemaConverter.processSchemaWithProperties(schema, p),
              "Expected IAE for null value on key '" + k + "'");
      assertTrue(
          ex.getMessage().contains("Column 'col'"),
          "Expected sanitized column message for key '" + k + "', got: " + ex.getMessage());
    }
  }

  @Test
  public void testLegacyOverrideIdentityIsCollisionFreeAcrossControlChars() {
    // Regression: a column name containing U+0000 must not collide with an unrelated path's
    // identityKey. Two separate fields, two separate legacy keys, no new-format overrides — both
    // legacy entries must be applied.
    String nameA = "a\u0000b";
    String nameC = "c";
    StructType schema =
        new StructType(
            new StructField[] {
              structField(nameA, DataTypes.StringType), structField(nameC, DataTypes.StringType),
            });
    Map<String, String> props = new HashMap<>();
    props.put(nameA + ".lance.compression", "zstd");
    props.put(nameC + ".lance.compression", "lz4");

    Schema arrow = process(schema, props);
    assertEquals(
        "zstd",
        arrowMetadata(walk(arrow, nameA)).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
    assertEquals(
        "lz4",
        arrowMetadata(walk(arrow, nameC)).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testMapKeyStructChildLiterallyNamedValueIsUnambiguous() {
    StructType inner =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("value", DataTypes.StringType, true)});
    StructType schema =
        topStruct(structField("m", DataTypes.createMapType(inner, DataTypes.StringType, true)));
    Map<String, String> props = new HashMap<>();
    // m -> key (map role) -> struct -> child literally named "value"
    props.put("lance.compression.column.m.key.value", "zstd");

    Schema arrow = process(schema, props);
    Field value = walk(arrow, "m", "key", "value");
    assertEquals("zstd", arrowMetadata(value).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testMapValueStructChildLiterallyNamedKeyIsUnambiguous() {
    StructType inner =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("key", DataTypes.StringType, true)});
    StructType schema =
        topStruct(structField("m", DataTypes.createMapType(DataTypes.StringType, inner, true)));
    Map<String, String> props = new HashMap<>();
    // m -> value (map role) -> struct -> child literally named "key"
    props.put("lance.compression.column.m.value.key", "zstd");

    Schema arrow = process(schema, props);
    Field key = walk(arrow, "m", "value", "key");
    assertEquals("zstd", arrowMetadata(key).get(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  // ---------- helpers ----------

  private static StructField structField(String name, DataType dt) {
    return DataTypes.createStructField(name, dt, true);
  }

  private static StructType topStruct(StructField only) {
    return new StructType(new StructField[] {only});
  }

  private static Schema process(StructType schema, Map<String, String> props) {
    StructType processed = SchemaConverter.processSchemaWithProperties(schema, props);
    return LanceArrowUtils.toArrowSchema(processed, "UTC", false);
  }

  private static Field walk(Schema schema, String... path) {
    Field current = null;
    for (Field f : schema.getFields()) {
      if (f.getName().equals(path[0])) {
        current = f;
        break;
      }
    }
    if (current == null) {
      throw new AssertionError("No top-level field: " + path[0]);
    }
    for (int i = 1; i < path.length; i++) {
      current = walkChild(current, path[i]);
    }
    return current;
  }

  private static Field walkChild(Field parent, String segment) {
    ArrowType type = parent.getType();
    List<Field> children = parent.getChildren();
    if (type instanceof ArrowType.List || type instanceof ArrowType.FixedSizeList) {
      if (!"element".equals(segment)) {
        throw new AssertionError("Expected 'element' under list, got " + segment);
      }
      return children.get(0);
    }
    if (type instanceof ArrowType.Map) {
      Field entries = children.get(0);
      List<Field> entryChildren = entries.getChildren();
      if ("key".equals(segment)) return entryChildren.get(0);
      if ("value".equals(segment)) return entryChildren.get(1);
      throw new AssertionError("Expected key/value under map, got " + segment);
    }
    if (type instanceof ArrowType.Struct) {
      for (Field f : children) {
        if (f.getName().equals(segment)) return f;
      }
      throw new AssertionError(
          "No struct child '"
              + segment
              + "' under "
              + parent.getName()
              + " (children="
              + childrenNames(children)
              + ")");
    }
    throw new AssertionError("Cannot walk into type " + type);
  }

  private static String childrenNames(List<Field> children) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < children.size(); i++) {
      if (i > 0) sb.append(", ");
      sb.append(children.get(i).getName());
    }
    return sb.append("]").toString();
  }

  private static Map<String, String> arrowMetadata(Field f) {
    return f.getMetadata() == null ? new HashMap<>() : f.getMetadata();
  }

  /** Asserts that no `lance-nested.*` keys leaked onto an Arrow Field's own metadata. */
  private static void assertOuterClean(Field f) {
    Map<String, String> meta = arrowMetadata(f);
    for (String k : meta.keySet()) {
      assertFalse(
          k.startsWith(LanceEncodingUtils.LANCE_NESTED_PREFIX),
          "lance-nested.* key leaked onto Field '" + f.getName() + "': " + k);
    }
  }
}
