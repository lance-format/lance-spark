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
package org.lance.spark.write;

import org.lance.spark.utils.BlobUtils;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The name+order contract pinned here is load-bearing for blob v2 copy-through:
 * LanceBlobV2CopyThroughRule gates rewrites by output column NAME, which is only sound because this
 * validator admits exclusively projections matching the target columns in name and order. Weakening
 * these rejections requires changing that gating first.
 */
public class LanceWriteSchemaValidatorTest {

  @Test
  public void testValidateAcceptsBinaryInputForBlobV2Column() {
    assertDoesNotThrow(
        () ->
            LanceWriteSchemaValidator.validate(
                writeSchemaWithIdAndBlob(), inputSchemaWithIdAndBinaryContent()));
  }

  @Test
  public void testValidateRejectsColumnCountMismatch() {
    StructType input =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
            });

    assertValidationFailure(
        "column count is different",
        () -> LanceWriteSchemaValidator.validate(writeSchemaWithIdAndBlob(), input));
  }

  @Test
  public void testValidateRejectsNonBinaryInputForBlobV2Column() {
    StructType writeSchema = new StructType(new StructField[] {blobV2Field("content")});
    StructType input =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("content", DataTypes.StringType, true),
            });

    assertValidationFailure(
        "accept binary", () -> LanceWriteSchemaValidator.validate(writeSchema, input));
  }

  @Test
  public void testValidateRejectsOutOfOrderColumns() {
    StructType reordered =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("content", DataTypes.BinaryType, true),
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
            });

    assertValidationFailure(
        "order is different",
        () -> LanceWriteSchemaValidator.validate(writeSchemaWithIdAndBlob(), reordered));
  }

  @Test
  public void testValidateRejectsUnknownColumnNames() {
    StructType foreignNames =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("identifier", DataTypes.IntegerType, false),
              DataTypes.createStructField("payload", DataTypes.BinaryType, true),
            });

    assertValidationFailure(
        "'identifier'",
        () -> LanceWriteSchemaValidator.validate(writeSchemaWithIdAndBlob(), foreignNames));
  }

  @Test
  public void testValidateAcceptsSqlValuesColumnNames() {
    StructType sqlValuesNames =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("col1", DataTypes.IntegerType, false),
              DataTypes.createStructField("col2", DataTypes.BinaryType, true),
            });

    assertDoesNotThrow(
        () -> LanceWriteSchemaValidator.validate(writeSchemaWithIdAndBlob(), sqlValuesNames));
  }

  @Test
  public void testValidateRejectsPartiallyMatchingColumnNames() {
    StructType partial =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("payload", DataTypes.BinaryType, true),
            });

    assertValidationFailure(
        "'payload'", () -> LanceWriteSchemaValidator.validate(writeSchemaWithIdAndBlob(), partial));
  }

  @Test
  public void testValidateRejectsNullableInputForNonNullableColumn() {
    StructType writeSchema =
        new StructType(
            new StructField[] {DataTypes.createStructField("id", DataTypes.IntegerType, false)});
    StructType input =
        new StructType(
            new StructField[] {DataTypes.createStructField("id", DataTypes.IntegerType, true)});

    assertValidationFailure(
        "does not allow nulls", () -> LanceWriteSchemaValidator.validate(writeSchema, input));
  }

  @Test
  public void testValidateAcceptsNonNullableInputForNullableColumn() {
    StructType writeSchema =
        new StructType(
            new StructField[] {DataTypes.createStructField("id", DataTypes.IntegerType, true)});
    StructType input =
        new StructType(
            new StructField[] {DataTypes.createStructField("id", DataTypes.IntegerType, false)});

    assertDoesNotThrow(() -> LanceWriteSchemaValidator.validate(writeSchema, input));
  }

  @Test
  public void testValidateAcceptsMatchingNestedStruct() {
    StructType nested =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("city", DataTypes.StringType, true),
              DataTypes.createStructField("zip", DataTypes.IntegerType, false),
            });
    StructType writeSchema =
        new StructType(new StructField[] {DataTypes.createStructField("address", nested, false)});
    StructType input =
        new StructType(
            new StructField[] {
              DataTypes.createStructField(
                  "address",
                  new StructType(
                      new StructField[] {
                        DataTypes.createStructField("city", DataTypes.StringType, true),
                        DataTypes.createStructField("zip", DataTypes.IntegerType, false),
                      }),
                  false)
            });

    assertDoesNotThrow(() -> LanceWriteSchemaValidator.validate(writeSchema, input));
  }

  @Test
  public void testValidateRejectsNestedStructFieldTypeMismatch() {
    StructType nested =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("city", DataTypes.StringType, true),
              DataTypes.createStructField("zip", DataTypes.IntegerType, false),
            });
    StructType writeSchema =
        new StructType(new StructField[] {DataTypes.createStructField("address", nested, true)});
    StructType input =
        new StructType(
            new StructField[] {
              DataTypes.createStructField(
                  "address",
                  new StructType(
                      new StructField[] {
                        DataTypes.createStructField("city", DataTypes.StringType, true),
                        DataTypes.createStructField("zip", DataTypes.StringType, true),
                      }),
                  true)
            });

    assertValidationFailure(
        "address.zip", () -> LanceWriteSchemaValidator.validate(writeSchema, input));
  }

  @Test
  public void testValidateRejectsNestedStructNullabilityMismatch() {
    StructType nested =
        new StructType(
            new StructField[] {DataTypes.createStructField("zip", DataTypes.IntegerType, false)});
    StructType writeSchema =
        new StructType(new StructField[] {DataTypes.createStructField("address", nested, true)});
    StructType input =
        new StructType(
            new StructField[] {
              DataTypes.createStructField(
                  "address",
                  new StructType(
                      new StructField[] {
                        DataTypes.createStructField("zip", DataTypes.IntegerType, true)
                      }),
                  true)
            });

    assertValidationFailure(
        "address.zip", () -> LanceWriteSchemaValidator.validate(writeSchema, input));
  }

  @Test
  public void testValidateRejectsNestedStructFieldCountMismatch() {
    StructType nested =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("city", DataTypes.StringType, true),
              DataTypes.createStructField("zip", DataTypes.IntegerType, false),
            });
    StructType writeSchema =
        new StructType(new StructField[] {DataTypes.createStructField("address", nested, true)});
    StructType input =
        new StructType(
            new StructField[] {
              DataTypes.createStructField(
                  "address",
                  new StructType(
                      new StructField[] {
                        DataTypes.createStructField("city", DataTypes.StringType, true),
                      }),
                  true)
            });

    assertValidationFailure(
        "fields in the table", () -> LanceWriteSchemaValidator.validate(writeSchema, input));
  }

  private static StructField blobV2Field(String name) {
    Metadata metadata =
        new MetadataBuilder()
            .putString(BlobUtils.ARROW_EXTENSION_NAME_KEY, BlobUtils.ARROW_EXTENSION_BLOB_V2)
            .build();
    return DataTypes.createStructField(name, DataTypes.BinaryType, true, metadata);
  }

  private static StructType writeSchemaWithIdAndBlob() {
    return new StructType(
        new StructField[] {
          DataTypes.createStructField("id", DataTypes.IntegerType, false), blobV2Field("content"),
        });
  }

  private static StructType inputSchemaWithIdAndBinaryContent() {
    return new StructType(
        new StructField[] {
          DataTypes.createStructField("id", DataTypes.IntegerType, false),
          DataTypes.createStructField("content", DataTypes.BinaryType, true),
        });
  }

  private static void assertValidationFailure(String expectedFragment, Runnable action) {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, action::run);
    assertTrue(
        e.getMessage().contains(expectedFragment),
        () -> "expected message to contain '" + expectedFragment + "': " + e.getMessage());
  }
}
