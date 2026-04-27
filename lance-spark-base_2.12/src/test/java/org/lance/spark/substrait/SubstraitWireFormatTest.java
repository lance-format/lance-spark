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

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.WriteParams;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.spark.TestUtils;

import io.substrait.proto.Expression;
import io.substrait.proto.Expression.FieldReference;
import io.substrait.proto.Expression.Literal;
import io.substrait.proto.Expression.ReferenceSegment;
import io.substrait.proto.Expression.ReferenceSegment.StructField;
import io.substrait.proto.Expression.ScalarFunction;
import io.substrait.proto.ExpressionReference;
import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.FunctionArgument;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionDeclaration.ExtensionFunction;
import io.substrait.proto.SimpleExtensionURI;
import io.substrait.proto.Type;
import io.substrait.proto.Type.Boolean;
import io.substrait.proto.Type.I32;
import io.substrait.proto.Type.Nullability;
import io.substrait.proto.Type.Struct;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pins the wire format between substrait-java and Lance's datafusion-substrait consumer.
 *
 * <p>Deliberately bypasses every abstraction in {@code org.lance.spark.substrait} and hand-builds
 * an {@link ExtendedExpression} for {@code value < 5} directly from substrait-java's protobuf
 * builders, so this test catches wire-format drift independently of any encoder code. The dataset
 * has ten int32 rows 0..9; the filter should match rows 0..4.
 */
public class SubstraitWireFormatTest {
  @TempDir Path tempDir;

  private static final String COMPARISON_URI =
      "https://github.com/substrait-io/substrait/blob/main/extensions/functions_comparison.yaml";

  private static final Schema ARROW_SCHEMA =
      new Schema(
          Collections.singletonList(
              new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));

  @Test
  public void substraitLessThanFilterRoundtrip(TestInfo testInfo) throws Exception {
    String datasetUri =
        TestUtils.getDatasetUri(tempDir.toString(), testInfo.getTestMethod().get().getName());

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      // 1. Create empty dataset, capture its version, write one fragment with values 0..9.
      long readVersion;
      try (Dataset created =
          Dataset.create(allocator, datasetUri, ARROW_SCHEMA, new WriteParams.Builder().build())) {
        readVersion = created.version();
      }

      List<FragmentMetadata> fragments;
      try (VectorSchemaRoot root = VectorSchemaRoot.create(ARROW_SCHEMA, allocator)) {
        IntVector values = (IntVector) root.getVector("value");
        values.allocateNew(10);
        for (int i = 0; i < 10; i++) {
          values.set(i, i);
        }
        values.setValueCount(10);
        root.setRowCount(10);
        fragments = Fragment.create(datasetUri, allocator, root, new WriteParams.Builder().build());
      }

      @SuppressWarnings("deprecation")
      Dataset appended =
          Dataset.commitAppend(
              datasetUri, Optional.of(readVersion), fragments, Collections.emptyMap());
      appended.close();

      // 2. Hand-build ExtendedExpression for: value < 5
      byte[] substraitBytes = buildLessThanFilter(5);

      // 3. Open, scan with substraitFilter, assert matching row count.
      try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
        assertEquals(10, dataset.countRows(), "dataset should have 10 rows");

        // Lance's JNI uses GetDirectBufferAddress, which requires a direct ByteBuffer.
        ByteBuffer directBuf = ByteBuffer.allocateDirect(substraitBytes.length);
        directBuf.put(substraitBytes).flip();
        ScanOptions options = new ScanOptions.Builder().substraitFilter(directBuf).build();
        try (LanceScanner scanner = dataset.newScan(options)) {
          assertEquals(5, scanner.countRows(), "filter 'value < 5' should match rows 0..4");
        }
      }
    }
  }

  /**
   * Hand-rolled ExtendedExpression for {@code value < literal}. Keeps substrait-java abstraction
   * surface to the bare protobuf builders so this test has nothing to do with our encoder layer.
   */
  private static byte[] buildLessThanFilter(int literal) {
    Type nullableI32 =
        Type.newBuilder()
            .setI32(I32.newBuilder().setNullability(Nullability.NULLABILITY_NULLABLE))
            .build();
    Type requiredBool =
        Type.newBuilder()
            .setBool(Boolean.newBuilder().setNullability(Nullability.NULLABILITY_REQUIRED))
            .build();

    // value (field index 0)
    Expression fieldRef =
        Expression.newBuilder()
            .setSelection(
                FieldReference.newBuilder()
                    .setDirectReference(
                        ReferenceSegment.newBuilder()
                            .setStructField(StructField.newBuilder().setField(0)))
                    .setRootReference(FieldReference.RootReference.getDefaultInstance()))
            .build();

    // i32 literal
    Expression literalExpr =
        Expression.newBuilder()
            .setLiteral(Literal.newBuilder().setI32(literal).setNullable(false))
            .build();

    // lt(value, literal) — function_anchor=1, bare name "lt".
    Expression lt =
        Expression.newBuilder()
            .setScalarFunction(
                ScalarFunction.newBuilder()
                    .setFunctionReference(1)
                    .addArguments(FunctionArgument.newBuilder().setValue(fieldRef))
                    .addArguments(FunctionArgument.newBuilder().setValue(literalExpr))
                    .setOutputType(requiredBool))
            .build();

    NamedStruct baseSchema =
        NamedStruct.newBuilder()
            .addNames("value")
            .setStruct(
                Struct.newBuilder()
                    .addTypes(nullableI32)
                    .setNullability(Nullability.NULLABILITY_REQUIRED))
            .build();

    ExtendedExpression extended =
        ExtendedExpression.newBuilder()
            .addExtensionUris(
                SimpleExtensionURI.newBuilder().setExtensionUriAnchor(1).setUri(COMPARISON_URI))
            .addExtensions(
                SimpleExtensionDeclaration.newBuilder()
                    .setExtensionFunction(
                        ExtensionFunction.newBuilder()
                            .setExtensionUriReference(1)
                            .setFunctionAnchor(1)
                            .setName("lt")))
            .addReferredExpr(
                ExpressionReference.newBuilder().addOutputNames("filter").setExpression(lt))
            .setBaseSchema(baseSchema)
            .build();

    return extended.toByteArray();
  }
}
