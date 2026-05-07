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

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.lance.spark.utils.BlobUtils.LANCE_ENCODING_BLOB_KEY;
import static org.lance.spark.utils.BlobUtils.LANCE_ENCODING_BLOB_VALUE;
import static org.lance.spark.utils.Float16Utils.ARROW_FLOAT16_KEY;
import static org.lance.spark.utils.Float16Utils.ARROW_FLOAT16_VALUE;
import static org.lance.spark.utils.LargeVarCharUtils.ARROW_LARGE_VAR_CHAR_KEY;
import static org.lance.spark.utils.LargeVarCharUtils.ARROW_LARGE_VAR_CHAR_VALUE;
import static org.lance.spark.utils.VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY;

/**
 * Utility class that augments a Spark {@link StructType} with Lance-specific column metadata
 * (vector fixed-size-list, Float16, blob, large varchar, compression) derived from table
 * properties.
 */
public class SchemaConverter {

  private SchemaConverter() {
    // Utility class
  }

  /**
   * Processes a Spark schema with table properties to add metadata for vector, blob, large varchar,
   * and compression columns.
   *
   * @param sparkSchema the original Spark StructType
   * @param properties table properties that may contain column metadata
   * @return StructType with metadata added for matching columns
   */
  public static StructType processSchemaWithProperties(
      StructType sparkSchema, Map<String, String> properties) {
    StructType schemaWithVectors = addVectorMetadata(sparkSchema, properties);
    StructType schemaWithFloat16 = addFloat16Metadata(schemaWithVectors, properties);
    StructType schemaWithBlobs = addBlobMetadata(schemaWithFloat16, properties);
    StructType schemaWithLargeVarChar = addLargeVarCharMetadata(schemaWithBlobs, properties);
    return addCompressionMetadata(schemaWithLargeVarChar, properties);
  }

  /**
   * Adds metadata to ArrayType fields based on table properties for vector columns. Properties with
   * pattern "<column_name>.arrow.fixed-size-list.size" are applied to matching columns.
   *
   * @param sparkSchema the original Spark StructType
   * @param properties table properties that may contain vector column metadata
   * @return StructType with metadata added for vector columns
   */
  private static StructType addVectorMetadata(
      StructType sparkSchema, Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return sparkSchema;
    }

    StructField[] newFields = new StructField[sparkSchema.fields().length];
    for (int i = 0; i < sparkSchema.fields().length; i++) {
      StructField field = sparkSchema.fields()[i];
      String vectorSizeProperty = VectorUtils.createVectorSizePropertyKey(field.name());

      if (properties.containsKey(vectorSizeProperty)) {
        // This field should be a vector column
        if (field.dataType() instanceof ArrayType) {
          ArrayType arrayType = (ArrayType) field.dataType();
          DataType elementType = arrayType.elementType();

          // Validate element type is FloatType or DoubleType
          if (elementType instanceof FloatType || elementType instanceof DoubleType) {
            // Add metadata for FixedSizeList
            long vectorSize = Long.parseLong(properties.get(vectorSizeProperty));
            Metadata newMetadata =
                new MetadataBuilder()
                    .withMetadata(field.metadata())
                    .putLong(ARROW_FIXED_SIZE_LIST_SIZE_KEY, vectorSize)
                    .build();
            newFields[i] =
                new StructField(field.name(), field.dataType(), field.nullable(), newMetadata);
          } else {
            throw new IllegalArgumentException(
                "Vector column '"
                    + field.name()
                    + "' must have element type FLOAT or DOUBLE, found: "
                    + elementType);
          }
        } else {
          throw new IllegalArgumentException(
              "Column '"
                  + field.name()
                  + "' has vector property but is not an ARRAY type: "
                  + field.dataType());
        }
      } else {
        // Keep field as-is
        newFields[i] = field;
      }
    }

    return new StructType(newFields);
  }

  /**
   * Adds float16 metadata to vector fields based on table properties. Properties with pattern
   * "<column_name>.arrow.float16" = "true" are applied to matching columns. The field must already
   * have fixed-size-list metadata and be ArrayType(FloatType).
   *
   * @param sparkSchema the Spark StructType (already processed by addVectorMetadata)
   * @param properties table properties that may contain float16 column metadata
   * @return StructType with float16 metadata added
   */
  private static StructType addFloat16Metadata(
      StructType sparkSchema, Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return sparkSchema;
    }

    StructField[] newFields = new StructField[sparkSchema.fields().length];
    for (int i = 0; i < sparkSchema.fields().length; i++) {
      StructField field = sparkSchema.fields()[i];
      String float16Property = Float16Utils.createPropertyKey(field.name());

      if (properties.containsKey(float16Property)) {
        String value = properties.get(float16Property);
        if ("true".equalsIgnoreCase(value)) {
          // Validate: must be ArrayType(FloatType) with fixed-size-list metadata
          if (!(field.dataType() instanceof ArrayType)) {
            throw new IllegalArgumentException(
                "Float16 column '"
                    + field.name()
                    + "' must be an ARRAY type, found: "
                    + field.dataType());
          }
          ArrayType arrayType = (ArrayType) field.dataType();
          if (!(arrayType.elementType() instanceof FloatType)) {
            throw new IllegalArgumentException(
                "Float16 column '"
                    + field.name()
                    + "' must have element type FLOAT, found: "
                    + arrayType.elementType());
          }
          if (!field.metadata().contains(ARROW_FIXED_SIZE_LIST_SIZE_KEY)) {
            throw new IllegalArgumentException(
                "Float16 column '"
                    + field.name()
                    + "' must also have '"
                    + ARROW_FIXED_SIZE_LIST_SIZE_KEY
                    + "' property set");
          }
          Metadata newMetadata =
              new MetadataBuilder()
                  .withMetadata(field.metadata())
                  .putString(ARROW_FLOAT16_KEY, ARROW_FLOAT16_VALUE)
                  .build();
          newFields[i] =
              new StructField(field.name(), field.dataType(), field.nullable(), newMetadata);
        } else {
          newFields[i] = field;
        }
      } else {
        newFields[i] = field;
      }
    }

    return new StructType(newFields);
  }

  /**
   * Adds metadata to BinaryType fields based on table properties for blob columns. Properties with
   * pattern "<column_name>.lance.encoding" = "blob" are applied to matching columns.
   *
   * @param sparkSchema the original Spark StructType
   * @param properties table properties that may contain blob column metadata
   * @return StructType with metadata added for blob columns
   */
  private static StructType addBlobMetadata(
      StructType sparkSchema, Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return sparkSchema;
    }

    StructField[] newFields = new StructField[sparkSchema.fields().length];
    for (int i = 0; i < sparkSchema.fields().length; i++) {
      StructField field = sparkSchema.fields()[i];
      String blobEncodingProperty = field.name() + ".lance.encoding";

      if (properties.containsKey(blobEncodingProperty)) {
        // This field should be a blob column
        String encodingValue = properties.get(blobEncodingProperty);
        if ("blob".equalsIgnoreCase(encodingValue)) {
          if (field.dataType() instanceof BinaryType) {
            // Add metadata for blob encoding
            Metadata newMetadata =
                new MetadataBuilder()
                    .withMetadata(field.metadata())
                    .putString(LANCE_ENCODING_BLOB_KEY, LANCE_ENCODING_BLOB_VALUE)
                    .build();
            newFields[i] =
                new StructField(field.name(), field.dataType(), field.nullable(), newMetadata);
          } else {
            throw new IllegalArgumentException(
                "Blob column '"
                    + field.name()
                    + "' must have BINARY type, found: "
                    + field.dataType());
          }
        } else {
          // Keep field as-is if encoding value is not blob
          newFields[i] = field;
        }
      } else {
        // Keep field as-is
        newFields[i] = field;
      }
    }

    return new StructType(newFields);
  }

  /**
   * Adds metadata to StringType fields based on table properties for large varchar columns.
   * Properties with pattern "<column_name>.arrow.large_var_char" = "true" are applied to matching
   * columns.
   *
   * @param sparkSchema the original Spark StructType
   * @param properties table properties that may contain large varchar column metadata
   * @return StructType with metadata added for large varchar columns
   */
  private static StructType addLargeVarCharMetadata(
      StructType sparkSchema, Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return sparkSchema;
    }

    StructField[] newFields = new StructField[sparkSchema.fields().length];
    for (int i = 0; i < sparkSchema.fields().length; i++) {
      StructField field = sparkSchema.fields()[i];
      String largeVarCharProperty = LargeVarCharUtils.createPropertyKey(field.name());

      if (properties.containsKey(largeVarCharProperty)) {
        // This field should be a large varchar column
        String encodingValue = properties.get(largeVarCharProperty);
        if ("true".equalsIgnoreCase(encodingValue)) {
          if (field.dataType() instanceof StringType) {
            // Add metadata for large varchar
            Metadata newMetadata =
                new MetadataBuilder()
                    .withMetadata(field.metadata())
                    .putString(ARROW_LARGE_VAR_CHAR_KEY, ARROW_LARGE_VAR_CHAR_VALUE)
                    .build();
            newFields[i] =
                new StructField(field.name(), field.dataType(), field.nullable(), newMetadata);
          } else {
            throw new IllegalArgumentException(
                "Large varchar column '"
                    + field.name()
                    + "' must have STRING type, found: "
                    + field.dataType());
          }
        } else {
          // Keep field as-is if value is not "true"
          newFields[i] = field;
        }
      } else {
        // Keep field as-is
        newFields[i] = field;
      }
    }

    return new StructType(newFields);
  }

  /**
   * Attaches Lance compression metadata at any field depth, parsed from connector-supported
   * TBLPROPERTIES (both the legacy {@code <column>.lance.<key>} and the new {@code
   * lance.<key>.column.<path>} formats — see {@link LanceEncodingUtils} for the format spec and
   * smuggling protocol).
   *
   * <p>Throws {@link IllegalArgumentException} for:
   *
   * <ul>
   *   <li>An invalid value on a key whose path resolves in the schema. Validation runs only after
   *       path resolution, so a bad value on an unresolved path is silently ignored.
   *   <li>A new-format key whose path is empty, contains an empty segment, or exceeds {@link
   *       LanceEncodingUtils#MAX_PATH_DEPTH} segments. (Keys ending in a legacy suffix are reserved
   *       for legacy parsing and never reach this throw branch.)
   * </ul>
   *
   * <p>Silently ignored:
   *
   * <ul>
   *   <li>Paths that don't resolve in the schema — matches the other {@code addX} methods.
   *   <li>Unrecognised {@code lance.*} suffixes (e.g. deferred dict/minichunk keys).
   *   <li>Unknown role tokens after an array/map (not {@code element}/{@code key}/{@code value}).
   *   <li>Type-incompatible combinations (e.g. {@code fsst} on numeric) — left to the Rust encoder.
   * </ul>
   *
   * @param sparkSchema the Spark StructType (already processed by earlier steps)
   * @param properties table properties that may contain compression metadata
   * @return StructType with compression metadata added for matching columns
   */
  private static StructType addCompressionMetadata(
      StructType sparkSchema, Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return sparkSchema;
    }

    List<LanceEncodingUtils.ParsedEncodingKey> legacy = LanceEncodingUtils.parseLegacy(properties);
    List<LanceEncodingUtils.ParsedEncodingKey> newFormat =
        LanceEncodingUtils.parseNewFormat(properties);

    // New-format wins on (path, rule) collisions. Drop the colliding legacy entry BEFORE
    // validation, so a stale invalid legacy value doesn't throw after migration.
    List<LanceEncodingUtils.ParsedEncodingKey> parsed =
        new ArrayList<>(legacy.size() + newFormat.size());
    if (!legacy.isEmpty()) {
      Set<List<Object>> overrides = new HashSet<>(newFormat.size() * 2);
      for (LanceEncodingUtils.ParsedEncodingKey pek : newFormat) {
        overrides.add(pek.identityKey());
      }
      for (LanceEncodingUtils.ParsedEncodingKey pek : legacy) {
        if (!overrides.contains(pek.identityKey())) {
          parsed.add(pek);
        }
      }
    }
    parsed.addAll(newFormat);
    if (parsed.isEmpty()) {
      return sparkSchema;
    }

    AttachResult result = attachMetadataAtPaths(sparkSchema, parsed, Collections.emptyList());
    return (StructType) result.type;
  }

  /**
   * Recursive walker that attaches encoding metadata at the paths described by {@code keys}.
   * Struct-child targets get metadata written directly to {@link StructField#metadata()}. Targets
   * reached only through {@link ArrayType} / {@link MapType} (which have no per-element {@code
   * StructField}) bubble up as {@code lance-nested.*} keys to the nearest enclosing {@code
   * StructField}, with a sub-path of role names ({@code element} / {@code key} / {@code value}).
   * Top-level returns always have empty {@link AttachResult#bubbleUp}.
   */
  private static AttachResult attachMetadataAtPaths(
      DataType dt, List<LanceEncodingUtils.ParsedEncodingKey> keys, List<String> currentPath) {
    if (keys.isEmpty()) {
      return new AttachResult(dt, Collections.emptyList());
    }

    if (dt instanceof StructType) {
      return attachOnStruct((StructType) dt, keys, currentPath);
    }
    if (dt instanceof ArrayType) {
      return attachOnArray((ArrayType) dt, keys, currentPath);
    }
    if (dt instanceof MapType) {
      return attachOnMap((MapType) dt, keys, currentPath);
    }
    // Scalar / unsupported leaf — any keys reaching here with more segments are silently
    // dropped (path-not-found policy).
    return new AttachResult(dt, Collections.emptyList());
  }

  private static AttachResult attachOnStruct(
      StructType st, List<LanceEncodingUtils.ParsedEncodingKey> keys, List<String> currentPath) {
    StructField[] children = st.fields();
    StructField[] newChildren = new StructField[children.length];
    boolean anyChange = false;

    for (int i = 0; i < children.length; i++) {
      StructField f = children[i];
      List<String> childPath = appendSegment(currentPath, f.name());

      List<LanceEncodingUtils.ParsedEncodingKey> directKeys = new ArrayList<>();
      List<LanceEncodingUtils.ParsedEncodingKey> deeperKeys = new ArrayList<>();
      for (LanceEncodingUtils.ParsedEncodingKey pek : keys) {
        List<String> p = pek.getPathSegments();
        if (p.equals(childPath)) {
          directKeys.add(pek);
        } else if (startsWith(p, childPath)) {
          deeperKeys.add(pek);
        }
      }

      MetadataBuilder builder = new MetadataBuilder().withMetadata(f.metadata());
      boolean fieldChanged = false;

      for (LanceEncodingUtils.ParsedEncodingKey pek : directKeys) {
        String dotted = String.join(".", childPath);
        pek.getRule().validate(dotted, pek.getValue());
        builder.putString(pek.getRule().getArrowMetadataKey(), pek.getValue());
        fieldChanged = true;
      }

      DataType newChildType = f.dataType();
      if (!deeperKeys.isEmpty()) {
        AttachResult r = attachMetadataAtPaths(f.dataType(), deeperKeys, childPath);
        if (r.type != f.dataType()) {
          newChildType = r.type;
          fieldChanged = true;
        }
        for (NestedAssignment na : r.bubbleUp) {
          builder.putString(buildNestedMetadataKey(na.subPath, na.arrowKey), na.value);
          fieldChanged = true;
        }
      }

      newChildren[i] =
          fieldChanged ? new StructField(f.name(), newChildType, f.nullable(), builder.build()) : f;
      if (fieldChanged) {
        anyChange = true;
      }
    }

    StructType newType = anyChange ? new StructType(newChildren) : st;
    return new AttachResult(newType, Collections.emptyList());
  }

  private static AttachResult attachOnArray(
      ArrayType at, List<LanceEncodingUtils.ParsedEncodingKey> keys, List<String> currentPath) {
    List<String> elementPath = appendSegment(currentPath, "element");

    List<NestedAssignment> bubbleUp = new ArrayList<>();
    List<LanceEncodingUtils.ParsedEncodingKey> deeperKeys = new ArrayList<>();

    for (LanceEncodingUtils.ParsedEncodingKey pek : keys) {
      List<String> p = pek.getPathSegments();
      if (!startsWith(p, elementPath)) {
        // Silent ignore — segment after currentPath is not the array's role.
        continue;
      }
      if (p.equals(elementPath)) {
        bubbleUp.add(validateAndBuildLeafBubble("element", pek));
      } else {
        deeperKeys.add(pek);
      }
    }

    DataType newElType = at.elementType();
    if (!deeperKeys.isEmpty()) {
      AttachResult r = attachMetadataAtPaths(at.elementType(), deeperKeys, elementPath);
      newElType = r.type;
      bubbleUp.addAll(prependRole("element", r.bubbleUp));
    }

    DataType newType =
        newElType == at.elementType() ? at : new ArrayType(newElType, at.containsNull());
    return new AttachResult(newType, bubbleUp);
  }

  private static AttachResult attachOnMap(
      MapType mt, List<LanceEncodingUtils.ParsedEncodingKey> keys, List<String> currentPath) {
    List<String> keyPath = appendSegment(currentPath, "key");
    List<String> valuePath = appendSegment(currentPath, "value");

    List<NestedAssignment> bubbleUp = new ArrayList<>();
    List<LanceEncodingUtils.ParsedEncodingKey> keyDeeper = new ArrayList<>();
    List<LanceEncodingUtils.ParsedEncodingKey> valueDeeper = new ArrayList<>();

    for (LanceEncodingUtils.ParsedEncodingKey pek : keys) {
      List<String> p = pek.getPathSegments();
      if (p.equals(keyPath)) {
        bubbleUp.add(validateAndBuildLeafBubble("key", pek));
      } else if (p.equals(valuePath)) {
        bubbleUp.add(validateAndBuildLeafBubble("value", pek));
      } else if (startsWith(p, keyPath)) {
        keyDeeper.add(pek);
      } else if (startsWith(p, valuePath)) {
        valueDeeper.add(pek);
      }
      // else: silent ignore (next segment is not key/value).
    }

    DataType newKeyType = mt.keyType();
    DataType newValueType = mt.valueType();

    if (!keyDeeper.isEmpty()) {
      AttachResult r = attachMetadataAtPaths(mt.keyType(), keyDeeper, keyPath);
      newKeyType = r.type;
      bubbleUp.addAll(prependRole("key", r.bubbleUp));
    }
    if (!valueDeeper.isEmpty()) {
      AttachResult r = attachMetadataAtPaths(mt.valueType(), valueDeeper, valuePath);
      newValueType = r.type;
      bubbleUp.addAll(prependRole("value", r.bubbleUp));
    }

    DataType newType =
        (newKeyType == mt.keyType() && newValueType == mt.valueType())
            ? mt
            : new MapType(newKeyType, newValueType, mt.valueContainsNull());
    return new AttachResult(newType, bubbleUp);
  }

  private static String buildNestedMetadataKey(List<String> subPath, String arrowKey) {
    return LanceEncodingUtils.LANCE_NESTED_PREFIX + String.join(".", subPath) + "." + arrowKey;
  }

  /**
   * Validate the value of a parsed key whose target lands at an Array element / Map key / Map value
   * at the current level, and return the leaf bubble-up assignment. {@code role} is one of {@code
   * element} / {@code key} / {@code value}.
   */
  private static NestedAssignment validateAndBuildLeafBubble(
      String role, LanceEncodingUtils.ParsedEncodingKey pek) {
    String dotted = String.join(".", pek.getPathSegments());
    pek.getRule().validate(dotted, pek.getValue());
    return new NestedAssignment(
        Collections.singletonList(role), pek.getRule().getArrowMetadataKey(), pek.getValue());
  }

  /**
   * Returns a copy of {@code bubbles} with each assignment's {@code subPath} prefixed by {@code
   * role}. Used by {@code attachOnArray} / {@code attachOnMap} to compose role names as assignments
   * climb to the nearest enclosing {@link StructField}.
   */
  private static List<NestedAssignment> prependRole(String role, List<NestedAssignment> bubbles) {
    if (bubbles.isEmpty()) {
      return Collections.emptyList();
    }
    List<NestedAssignment> out = new ArrayList<>(bubbles.size());
    for (NestedAssignment na : bubbles) {
      List<String> newSub = new ArrayList<>(na.subPath.size() + 1);
      newSub.add(role);
      newSub.addAll(na.subPath);
      out.add(new NestedAssignment(newSub, na.arrowKey, na.value));
    }
    return out;
  }

  private static List<String> appendSegment(List<String> base, String seg) {
    List<String> r = new ArrayList<>(base.size() + 1);
    r.addAll(base);
    r.add(seg);
    return r;
  }

  /**
   * Returns true if {@code a} has {@code b} as a prefix (segment-wise). Equal-length lists with
   * equal elements return true — call sites that need <i>strict</i> prefix-of check {@code
   * a.equals(b)} separately.
   */
  private static boolean startsWith(List<String> a, List<String> b) {
    if (a.size() < b.size()) {
      return false;
    }
    for (int i = 0; i < b.size(); i++) {
      if (!a.get(i).equals(b.get(i))) {
        return false;
      }
    }
    return true;
  }

  /** A metadata assignment bubbling up to be stamped on the nearest enclosing StructField. */
  private static final class NestedAssignment {
    private final List<String> subPath;
    private final String arrowKey;
    private final String value;

    NestedAssignment(List<String> subPath, String arrowKey, String value) {
      this.subPath = List.copyOf(subPath);
      this.arrowKey = arrowKey;
      this.value = value;
    }
  }

  /** Result of {@link #attachMetadataAtPaths}: the rewritten type, plus pending bubble-ups. */
  private static final class AttachResult {
    private final DataType type;
    private final List<NestedAssignment> bubbleUp;

    AttachResult(DataType type, List<NestedAssignment> bubbleUp) {
      this.type = type;
      this.bubbleUp = List.copyOf(bubbleUp);
    }
  }
}
