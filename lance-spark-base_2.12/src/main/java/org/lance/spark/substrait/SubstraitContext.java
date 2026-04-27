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
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionURI;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Mutable per-encode state shared by the substrait encoder classes.
 *
 * <p>A new {@code SubstraitContext} is created for every encode operation (driver side), used to
 * walk one expression / aggregate tree, and discarded once {@code .toByteArray()} has been called
 * on the resulting envelope. It is not thread-safe and not intended to live across encodes.
 *
 * <p>Holds three pieces of state:
 *
 * <ol>
 *   <li>The full Lance dataset {@link StructType}, used to resolve {@link
 *       org.apache.spark.sql.connector.expressions.NamedReference} columns to positional ordinals
 *       and to emit the envelope's {@code base_schema} {@link NamedStruct}.
 *   <li>An anchor registry mapping each extension URI seen during encoding to a small integer
 *       anchor (used by {@code SimpleExtensionURI.extension_uri_anchor}).
 *   <li>An anchor registry mapping each (bare function name, URI) pair to a function anchor (used
 *       by {@code ExtensionFunction.function_anchor}).
 * </ol>
 *
 * <p>The dataset schema must contain every column in the underlying Lance dataset (in dataset
 * order), not the post-pruning Spark scan schema. Lance's {@code parse_substrait} consumer enforces
 * a length match between {@code envelope.base_schema} and its own input schema; columns the encoder
 * cannot represent (FixedSizeList, UDT, etc.) become placeholder entries that Lance drops via
 * {@code remove_extension_types}.
 */
public final class SubstraitContext {

  private final StructType datasetSchema;
  private final Map<String, Integer> nameToOrdinal;
  private final NamedStruct namedStruct;

  /** URI → anchor (1-based; anchor 0 is reserved for "unset" in protobuf). */
  private final LinkedHashMap<String, Integer> uriToAnchor = new LinkedHashMap<>();

  /** (bareName, uri) joint key → function anchor. */
  private final LinkedHashMap<FunctionKey, Integer> functionAnchors = new LinkedHashMap<>();

  public SubstraitContext(StructType datasetSchema) {
    this.datasetSchema = datasetSchema;
    this.namedStruct = TypeEncoder.encodeDatasetSchema(datasetSchema);
    this.nameToOrdinal = buildNameToOrdinal(datasetSchema);
  }

  /** Returns the dataset schema this context was constructed with. */
  public StructType datasetSchema() {
    return datasetSchema;
  }

  /**
   * Resolves a top-level column name to its 0-based ordinal in the dataset schema.
   *
   * @return the ordinal, or {@code -1} if the column is unknown or has been replaced with a
   *     placeholder by {@link TypeEncoder} (i.e. its Spark type is not encodable into Substrait).
   */
  public int resolveFieldOrdinal(String name) {
    Integer ord = nameToOrdinal.get(name);
    return ord == null ? -1 : ord;
  }

  /**
   * Registers a (bare function name, extension URI) pair, allocating a function anchor on first use
   * of the pair and a URI anchor on first use of the URI. Subsequent calls with the same pair
   * return the same anchor.
   *
   * @return the function anchor to embed in {@code ScalarFunction.function_reference} or {@code
   *     AggregateFunction.function_reference}.
   */
  public int registerFunction(String bareName, String extensionUri) {
    Objects.requireNonNull(bareName, "bareName");
    Objects.requireNonNull(extensionUri, "extensionUri");
    uriToAnchor.computeIfAbsent(extensionUri, uri -> uriToAnchor.size() + 1);
    FunctionKey key = new FunctionKey(bareName, extensionUri);
    Integer existing = functionAnchors.get(key);
    if (existing != null) {
      return existing;
    }
    int anchor = functionAnchors.size() + 1;
    functionAnchors.put(key, anchor);
    return anchor;
  }

  /** Returns the {@link NamedStruct} for the encoded envelope's {@code base_schema} field. */
  public NamedStruct toNamedStruct() {
    return namedStruct;
  }

  /**
   * Returns the {@link SimpleExtensionURI} declarations the registry has accumulated, in anchor
   * order, for inclusion in {@code ExtendedExpression.extension_uris} or {@code
   * Plan.extension_uris}.
   */
  public List<SimpleExtensionURI> uriDeclarations() {
    List<SimpleExtensionURI> out = new ArrayList<>(uriToAnchor.size());
    for (Map.Entry<String, Integer> e : uriToAnchor.entrySet()) {
      out.add(
          SimpleExtensionURI.newBuilder()
              .setExtensionUriAnchor(e.getValue())
              .setUri(e.getKey())
              .build());
    }
    return out;
  }

  /**
   * Returns the {@link SimpleExtensionDeclaration} entries the registry has accumulated, in
   * function-anchor order, for inclusion in {@code ExtendedExpression.extensions} or {@code
   * Plan.extensions}.
   *
   * <p>Sets only {@code extension_uri_reference}, not {@code extension_urn_reference}, even though
   * substrait-java 0.84.1 supports both. Lance's current {@code datafusion-substrait} consumer
   * accepts URI-only (verified by {@code SubstraitWireFormatTest} on all four target Spark
   * versions). Substrait-java's <em>own</em> producer at {@code
   * io.substrait.extension.ExtensionCollector} sets URN exclusively, so a future Lance upgrade that
   * follows substrait-java's lead may require populating both. Adding URN emission is a deliberate
   * Phase 1 follow-up with its own roundtrip test against Lance — do not change here without
   * re-running {@code SubstraitWireFormatTest} on Spark 3.4 / 3.5 / 4.0 / 4.1.
   */
  public List<SimpleExtensionDeclaration> functionDeclarations() {
    List<SimpleExtensionDeclaration> out = new ArrayList<>(functionAnchors.size());
    for (Map.Entry<FunctionKey, Integer> e : functionAnchors.entrySet()) {
      Integer uriAnchor = uriToAnchor.get(e.getKey().uri);
      out.add(
          SimpleExtensionDeclaration.newBuilder()
              .setExtensionFunction(
                  SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                      .setExtensionUriReference(uriAnchor)
                      .setFunctionAnchor(e.getValue())
                      .setName(e.getKey().bareName))
              .build());
    }
    return out;
  }

  private static Map<String, Integer> buildNameToOrdinal(StructType schema) {
    StructField[] fields = schema.fields();
    Map<String, Integer> map = new HashMap<>(fields.length * 2);
    for (int i = 0; i < fields.length; i++) {
      // A field whose Spark type is not encodable becomes a placeholder in the NamedStruct,
      // and Substrait expressions cannot reference it. Skip it from the lookup map so
      // resolveFieldOrdinal returns -1.
      if (TypeEncoder.isEncodable(fields[i].dataType())) {
        map.put(fields[i].name(), i);
      }
    }
    return map;
  }

  /** Joint key for the function anchor registry. */
  private static final class FunctionKey {
    final String bareName;
    final String uri;

    FunctionKey(String bareName, String uri) {
      this.bareName = bareName;
      this.uri = uri;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof FunctionKey)) return false;
      FunctionKey that = (FunctionKey) o;
      return bareName.equals(that.bareName) && uri.equals(that.uri);
    }

    @Override
    public int hashCode() {
      return 31 * bareName.hashCode() + uri.hashCode();
    }
  }
}
