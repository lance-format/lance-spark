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
package org.lance.spark.substrait;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import io.substrait.proto.Type.Nullability;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TypeEncoderTest {

  @Test
  void primitivesEncodeWithCorrectNullability() {
    assertEquals(Type.KindCase.BOOL, TypeEncoder.encode(DataTypes.BooleanType, true).getKindCase());
    assertEquals(Type.KindCase.I8, TypeEncoder.encode(DataTypes.ByteType, false).getKindCase());
    assertEquals(Type.KindCase.I16, TypeEncoder.encode(DataTypes.ShortType, true).getKindCase());
    assertEquals(Type.KindCase.I32, TypeEncoder.encode(DataTypes.IntegerType, false).getKindCase());
    assertEquals(Type.KindCase.I64, TypeEncoder.encode(DataTypes.LongType, true).getKindCase());
    assertEquals(Type.KindCase.FP32, TypeEncoder.encode(DataTypes.FloatType, false).getKindCase());
    assertEquals(Type.KindCase.FP64, TypeEncoder.encode(DataTypes.DoubleType, true).getKindCase());
    assertEquals(
        Type.KindCase.STRING, TypeEncoder.encode(DataTypes.StringType, false).getKindCase());
    assertEquals(
        Type.KindCase.BINARY, TypeEncoder.encode(DataTypes.BinaryType, true).getKindCase());

    Type nullableInt = TypeEncoder.encode(DataTypes.IntegerType, true);
    assertEquals(Nullability.NULLABILITY_NULLABLE, nullableInt.getI32().getNullability());

    Type requiredLong = TypeEncoder.encode(DataTypes.LongType, false);
    assertEquals(Nullability.NULLABILITY_REQUIRED, requiredLong.getI64().getNullability());
  }

  @Test
  void dateAndTimestampVariants() {
    assertEquals(Type.KindCase.DATE, TypeEncoder.encode(DataTypes.DateType, true).getKindCase());
    assertEquals(
        Type.KindCase.TIMESTAMP_TZ,
        TypeEncoder.encode(DataTypes.TimestampType, true).getKindCase());
    assertEquals(
        Type.KindCase.TIMESTAMP,
        TypeEncoder.encode(DataTypes.TimestampNTZType, true).getKindCase());
  }

  @Test
  void decimalCarriesPrecisionAndScale() {
    Type t = TypeEncoder.encode(DataTypes.createDecimalType(38, 10), true);
    assertEquals(Type.KindCase.DECIMAL, t.getKindCase());
    assertEquals(38, t.getDecimal().getPrecision());
    assertEquals(10, t.getDecimal().getScale());
  }

  // No negative-scale test: Spark's DecimalType constructor rejects scale < 0 upstream, so
  // our encoder's defensive check is unreachable in practice. The check stays as a guard
  // against spark.sql.legacy.allowNegativeScaleOfDecimal.

  @Test
  void unsupportedTypesReturnNull() {
    assertNull(TypeEncoder.encode(new ArrayType(DataTypes.IntegerType, true), true));
    assertNull(
        TypeEncoder.encode(new MapType(DataTypes.StringType, DataTypes.IntegerType, true), true));
    assertNull(TypeEncoder.encode(NullType$.MODULE$, true));
    StructType nested = new StructType().add("inner", DataTypes.IntegerType);
    assertNull(TypeEncoder.encode(nested, true));
  }

  @Test
  void isEncodableMatchesEncode() {
    assertTrue(TypeEncoder.isEncodable(DataTypes.IntegerType));
    assertTrue(TypeEncoder.isEncodable(DataTypes.StringType));
    assertTrue(TypeEncoder.isEncodable(DataTypes.createDecimalType(10, 2)));
    assertFalse(TypeEncoder.isEncodable(new ArrayType(DataTypes.IntegerType, true)));
    assertFalse(TypeEncoder.isEncodable(NullType$.MODULE$));
  }

  @Test
  void datasetSchemaPreservesFieldCountAndOrder() {
    StructType schema =
        new StructType()
            .add("a", DataTypes.IntegerType, true)
            .add("b", DataTypes.StringType, false)
            .add("c", DataTypes.DoubleType, true);

    NamedStruct ns = TypeEncoder.encodeDatasetSchema(schema);

    assertEquals(3, ns.getNamesCount());
    assertEquals("a", ns.getNames(0));
    assertEquals("b", ns.getNames(1));
    assertEquals("c", ns.getNames(2));

    assertEquals(3, ns.getStruct().getTypesCount());
    assertEquals(Type.KindCase.I32, ns.getStruct().getTypes(0).getKindCase());
    assertEquals(Type.KindCase.STRING, ns.getStruct().getTypes(1).getKindCase());
    assertEquals(Type.KindCase.FP64, ns.getStruct().getTypes(2).getKindCase());

    assertEquals(
        Nullability.NULLABILITY_NULLABLE, ns.getStruct().getTypes(0).getI32().getNullability());
    assertEquals(
        Nullability.NULLABILITY_REQUIRED, ns.getStruct().getTypes(1).getString().getNullability());
  }

  @Test
  void datasetSchemaEmitsPlaceholderForUnencodableColumns() {
    StructType schema =
        new StructType()
            .add("a", DataTypes.IntegerType)
            .add("vec", new ArrayType(DataTypes.FloatType, true))
            .add("c", DataTypes.StringType);

    NamedStruct ns = TypeEncoder.encodeDatasetSchema(schema);

    // Field count must match the original schema.
    assertEquals(3, ns.getNamesCount());
    assertEquals(3, ns.getStruct().getTypesCount());

    // Real columns keep their names.
    assertEquals("a", ns.getNames(0));
    assertEquals("c", ns.getNames(2));

    // The placeholder column gets a __unlikely_name_placeholder_<i> name and a stand-in type.
    assertTrue(
        ns.getNames(1).startsWith(TypeEncoder.PLACEHOLDER_NAME_PREFIX),
        "expected placeholder name, got: " + ns.getNames(1));
  }

  // Spark's NullType is a Scala object; reach it via MODULE$ to avoid importing scala.
  private static final class NullType$ {
    static final NullType MODULE$ = NullType$.module();

    private static NullType module() {
      try {
        return (NullType)
            Class.forName("org.apache.spark.sql.types.NullType$").getField("MODULE$").get(null);
      } catch (ReflectiveOperationException e) {
        throw new AssertionError(e);
      }
    }
  }
}
