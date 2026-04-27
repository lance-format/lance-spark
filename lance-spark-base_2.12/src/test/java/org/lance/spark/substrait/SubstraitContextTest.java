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

import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionURI;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class SubstraitContextTest {

  private static final String CMP = FunctionNames.COMPARISON_URI;
  private static final String ARI = FunctionNames.ARITHMETIC_URI;

  @Test
  void resolveFieldOrdinalReturnsZeroBasedIndex() {
    StructType schema =
        new StructType()
            .add("a", DataTypes.IntegerType)
            .add("b", DataTypes.StringType)
            .add("c", DataTypes.DoubleType);
    SubstraitContext ctx = new SubstraitContext(schema);

    assertEquals(0, ctx.resolveFieldOrdinal("a"));
    assertEquals(1, ctx.resolveFieldOrdinal("b"));
    assertEquals(2, ctx.resolveFieldOrdinal("c"));
  }

  @Test
  void resolveFieldOrdinalReturnsNegativeOneForUnknownColumn() {
    StructType schema = new StructType().add("a", DataTypes.IntegerType);
    SubstraitContext ctx = new SubstraitContext(schema);
    assertEquals(-1, ctx.resolveFieldOrdinal("missing"));
  }

  @Test
  void resolveFieldOrdinalReturnsNegativeOneForPlaceholderColumn() {
    StructType schema =
        new StructType()
            .add("a", DataTypes.IntegerType)
            .add("vec", new ArrayType(DataTypes.FloatType, true))
            .add("c", DataTypes.StringType);
    SubstraitContext ctx = new SubstraitContext(schema);

    // Encodable columns resolve to their dataset ordinal (NOT a post-pruning ordinal).
    assertEquals(0, ctx.resolveFieldOrdinal("a"));
    assertEquals(2, ctx.resolveFieldOrdinal("c"));

    // The un-encodable ArrayType column is reachable as a placeholder in the NamedStruct, but
    // expressions cannot reference it — resolveFieldOrdinal returns -1 so encoders fail-soft.
    assertEquals(-1, ctx.resolveFieldOrdinal("vec"));
  }

  @Test
  void registerFunctionAllocatesAnchorsStartingFromOne() {
    SubstraitContext ctx = new SubstraitContext(new StructType().add("a", DataTypes.IntegerType));

    assertEquals(1, ctx.registerFunction("equal", CMP));
    assertEquals(2, ctx.registerFunction("lt", CMP));
    assertEquals(3, ctx.registerFunction("add", ARI));
  }

  @Test
  void registerFunctionReusesAnchorForSamePair() {
    SubstraitContext ctx = new SubstraitContext(new StructType().add("a", DataTypes.IntegerType));

    int first = ctx.registerFunction("equal", CMP);
    int second = ctx.registerFunction("equal", CMP);
    assertEquals(first, second);

    int third = ctx.registerFunction("equal", CMP);
    assertEquals(first, third);
  }

  @Test
  void registerFunctionTreatsSameNameDifferentUriAsDifferentFunction() {
    SubstraitContext ctx = new SubstraitContext(new StructType().add("a", DataTypes.IntegerType));

    int cmpEqual = ctx.registerFunction("equal", CMP);
    // Hypothetical: same bare name in a different extension namespace.
    int ariEqual = ctx.registerFunction("equal", ARI);
    assertNotEquals(cmpEqual, ariEqual);
  }

  @Test
  void uriDeclarationsAreInRegistrationOrder() {
    SubstraitContext ctx = new SubstraitContext(new StructType().add("a", DataTypes.IntegerType));
    ctx.registerFunction("equal", CMP);
    ctx.registerFunction("add", ARI);

    List<SimpleExtensionURI> uris = ctx.uriDeclarations();
    assertEquals(2, uris.size());
    assertEquals(1, uris.get(0).getExtensionUriAnchor());
    assertEquals(CMP, uris.get(0).getUri());
    assertEquals(2, uris.get(1).getExtensionUriAnchor());
    assertEquals(ARI, uris.get(1).getUri());
  }

  @Test
  void uriDeclarationsDeduplicateRepeatedUris() {
    SubstraitContext ctx = new SubstraitContext(new StructType().add("a", DataTypes.IntegerType));
    ctx.registerFunction("equal", CMP);
    ctx.registerFunction("lt", CMP);
    ctx.registerFunction("gt", CMP);
    // Three functions, one URI.
    assertEquals(1, ctx.uriDeclarations().size());
  }

  @Test
  void functionDeclarationsCarryAnchorsAndUriReferences() {
    SubstraitContext ctx = new SubstraitContext(new StructType().add("a", DataTypes.IntegerType));
    int eqAnchor = ctx.registerFunction("equal", CMP);
    int addAnchor = ctx.registerFunction("add", ARI);

    List<SimpleExtensionDeclaration> decls = ctx.functionDeclarations();
    assertEquals(2, decls.size());

    SimpleExtensionDeclaration.ExtensionFunction eq = decls.get(0).getExtensionFunction();
    assertEquals("equal", eq.getName());
    assertEquals(eqAnchor, eq.getFunctionAnchor());
    assertEquals(1, eq.getExtensionUriReference()); // URI anchor for CMP

    SimpleExtensionDeclaration.ExtensionFunction add = decls.get(1).getExtensionFunction();
    assertEquals("add", add.getName());
    assertEquals(addAnchor, add.getFunctionAnchor());
    assertEquals(2, add.getExtensionUriReference()); // URI anchor for ARI
  }

  @Test
  void toNamedStructDelegatesToTypeEncoder() {
    StructType schema =
        new StructType().add("x", DataTypes.IntegerType).add("y", DataTypes.StringType);
    SubstraitContext ctx = new SubstraitContext(schema);
    assertEquals(2, ctx.toNamedStruct().getNamesCount());
    assertEquals("x", ctx.toNamedStruct().getNames(0));
    assertEquals("y", ctx.toNamedStruct().getNames(1));
  }
}
