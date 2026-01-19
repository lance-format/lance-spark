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

import org.lance.namespace.model.JsonArrowDataType;
import org.lance.namespace.model.JsonArrowField;
import org.lance.namespace.model.JsonArrowSchema;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonArrowSchemaConverter {

  public static Schema convertToArrowSchema(JsonArrowSchema jsonSchema) {
    if (jsonSchema == null) {
      return null;
    }

    List<Field> fields = new ArrayList<>();
    if (jsonSchema.getFields() != null) {
      for (JsonArrowField jsonField : jsonSchema.getFields()) {
        fields.add(convertToArrowField(jsonField));
      }
    }

    Map<String, String> metadata =
        jsonSchema.getMetadata() != null ? jsonSchema.getMetadata() : new HashMap<>();

    return new Schema(fields, metadata);
  }

  private static Field convertToArrowField(JsonArrowField jsonField) {
    if (jsonField == null) {
      throw new IllegalArgumentException("JsonArrowField cannot be null");
    }

    String name = jsonField.getName();
    boolean nullable = jsonField.getNullable() != null ? jsonField.getNullable() : true;
    ArrowType arrowType = convertToArrowType(jsonField.getType());

    Map<String, String> metadata =
        jsonField.getMetadata() != null ? jsonField.getMetadata() : new HashMap<>();

    FieldType fieldType = new FieldType(nullable, arrowType, null, metadata);

    // Convert child fields if they exist (needed for list, struct, map, etc.)
    List<Field> children = null;
    boolean hasChildFields = jsonField.getType() != null && jsonField.getType().getFields() != null;
    if (hasChildFields) {
      children = new ArrayList<>();
      for (JsonArrowField childField : jsonField.getType().getFields()) {
        children.add(convertToArrowField(childField));
      }
    }

    return new Field(name, fieldType, children);
  }

  private static ArrowType convertToArrowType(JsonArrowDataType jsonType) {
    if (jsonType == null) {
      throw new IllegalArgumentException("JsonArrowDataType cannot be null");
    }

    String typeName = jsonType.getType();
    if (typeName == null) {
      throw new IllegalArgumentException("Type name cannot be null");
    }

    switch (typeName.toLowerCase()) {
      case "null":
        return ArrowType.Null.INSTANCE;
      case "bool":
      case "boolean":
        return ArrowType.Bool.INSTANCE;
      case "int8":
        return new ArrowType.Int(8, true);
      case "uint8":
        return new ArrowType.Int(8, false);
      case "int16":
        return new ArrowType.Int(16, true);
      case "uint16":
        return new ArrowType.Int(16, false);
      case "int32":
        return new ArrowType.Int(32, true);
      case "uint32":
        return new ArrowType.Int(32, false);
      case "int64":
        return new ArrowType.Int(64, true);
      case "uint64":
        return new ArrowType.Int(64, false);
      case "float16":
        return new ArrowType.FloatingPoint(FloatingPointPrecision.HALF);
      case "float32":
        return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case "float64":
        return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case "utf8":
      case "string":
        return ArrowType.Utf8.INSTANCE;
      case "binary":
        return ArrowType.Binary.INSTANCE;
      case "fixedsizebinary":
        Long length = jsonType.getLength();
        if (length == null) {
          throw new IllegalArgumentException("FixedSizeBinary type requires length field");
        }
        return new ArrowType.FixedSizeBinary(length.intValue());
      case "decimal128":
        // Default precision and scale for decimal, should be enhanced based on actual requirements
        return new ArrowType.Decimal(38, 18, 128);
      case "decimal256":
        return new ArrowType.Decimal(76, 18, 256);
      case "date32":
        return new ArrowType.Date(DateUnit.DAY);
      case "date64":
        return new ArrowType.Date(DateUnit.MILLISECOND);
      case "time32":
        return new ArrowType.Time(TimeUnit.SECOND, 32);
      case "time64":
        return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
      case "timestamp":
        return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
      case "interval":
        return new ArrowType.Interval(IntervalUnit.DAY_TIME);
      case "duration":
        return new ArrowType.Duration(TimeUnit.MICROSECOND);
      case "list":
        if (jsonType.getFields() == null || jsonType.getFields().isEmpty()) {
          throw new IllegalArgumentException("List type requires field definition");
        }
        return ArrowType.List.INSTANCE;
      case "struct":
        return ArrowType.Struct.INSTANCE;
      case "union":
        return new ArrowType.Union(UnionMode.Sparse, new int[0]);
      case "fixedsizelist":
        Long listSize = jsonType.getLength();
        if (listSize == null) {
          throw new IllegalArgumentException("FixedSizeList type requires length field");
        }
        return new ArrowType.FixedSizeList(listSize.intValue());
      case "map":
        return new ArrowType.Map(false);
      default:
        throw new IllegalArgumentException("Unsupported Arrow type: " + typeName);
    }
  }

  public static JsonArrowSchema convertToJsonArrowSchema(Schema arrowSchema) {
    if (arrowSchema == null) {
      return null;
    }

    JsonArrowSchema jsonSchema = new JsonArrowSchema();

    // Convert fields
    List<JsonArrowField> jsonFields = new ArrayList<>();
    for (Field field : arrowSchema.getFields()) {
      jsonFields.add(convertToJsonArrowField(field));
    }
    jsonSchema.setFields(jsonFields);

    // Convert metadata
    if (arrowSchema.getCustomMetadata() != null) {
      jsonSchema.setMetadata(arrowSchema.getCustomMetadata());
    }

    return jsonSchema;
  }

  private static JsonArrowField convertToJsonArrowField(Field field) {
    JsonArrowField jsonField = new JsonArrowField();
    jsonField.setName(field.getName());
    jsonField.setNullable(field.isNullable());
    jsonField.setType(convertToJsonArrowDataType(field.getType(), field.getChildren()));

    if (field.getMetadata() != null && !field.getMetadata().isEmpty()) {
      jsonField.setMetadata(field.getMetadata());
    }

    return jsonField;
  }

  private static JsonArrowDataType convertToJsonArrowDataType(
      ArrowType arrowType, List<Field> children) {
    JsonArrowDataType jsonType = new JsonArrowDataType();

    // Convert type name - for now we just set the basic type
    String typeName = arrowTypeToString(arrowType);
    jsonType.setType(typeName);

    // Handle children for complex types
    if (children != null && !children.isEmpty()) {
      List<JsonArrowField> jsonChildren = new ArrayList<>();
      for (Field child : children) {
        jsonChildren.add(convertToJsonArrowField(child));
      }
      jsonType.setFields(jsonChildren);
    }

    return jsonType;
  }

  private static String arrowTypeToString(ArrowType arrowType) {
    if (arrowType instanceof ArrowType.Null) {
      return "null";
    } else if (arrowType instanceof ArrowType.Bool) {
      return "bool";
    } else if (arrowType instanceof ArrowType.Int) {
      ArrowType.Int intType = (ArrowType.Int) arrowType;
      return "int" + intType.getBitWidth();
    } else if (arrowType instanceof ArrowType.FloatingPoint) {
      ArrowType.FloatingPoint fp = (ArrowType.FloatingPoint) arrowType;
      return fp.getPrecision() == FloatingPointPrecision.SINGLE ? "float32" : "float64";
    } else if (arrowType instanceof ArrowType.Utf8) {
      return "utf8";
    } else if (arrowType instanceof ArrowType.LargeUtf8) {
      return "largeutf8";
    } else if (arrowType instanceof ArrowType.Binary) {
      return "binary";
    } else if (arrowType instanceof ArrowType.LargeBinary) {
      return "largebinary";
    } else if (arrowType instanceof ArrowType.FixedSizeBinary) {
      return "fixedsizebinary";
    } else if (arrowType instanceof ArrowType.Date) {
      return "date";
    } else if (arrowType instanceof ArrowType.Time) {
      return "time";
    } else if (arrowType instanceof ArrowType.Timestamp) {
      return "timestamp";
    } else if (arrowType instanceof ArrowType.Duration) {
      return "duration";
    } else if (arrowType instanceof ArrowType.Interval) {
      return "interval";
    } else if (arrowType instanceof ArrowType.Decimal) {
      return "decimal";
    } else if (arrowType instanceof ArrowType.List) {
      return "list";
    } else if (arrowType instanceof ArrowType.LargeList) {
      return "largelist";
    } else if (arrowType instanceof ArrowType.FixedSizeList) {
      return "fixedsizelist";
    } else if (arrowType instanceof ArrowType.Struct) {
      return "struct";
    } else if (arrowType instanceof ArrowType.Map) {
      return "map";
    } else if (arrowType instanceof ArrowType.Union) {
      return "union";
    } else {
      return arrowType.getTypeID().toString().toLowerCase();
    }
  }
}
