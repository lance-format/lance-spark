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

/**
 * Substrait protobuf encoders for Lance scan pushdown.
 *
 * <p>Translates Spark V2 connector expressions into the protobuf format Lance's scanner consumes
 * via {@code ScanOptions.substraitFilter} and {@code ScanOptions.substraitAggregate}:
 *
 * <ul>
 *   <li>{@link org.lance.spark.substrait.SubstraitContext} — per-encode mutable state: dataset
 *       schema binding, URI + function anchor registries.
 *   <li>{@link org.lance.spark.substrait.TypeEncoder} — Spark {@code DataType} → Substrait {@code
 *       Type}, plus whole-dataset {@code NamedStruct} construction.
 *   <li>{@link org.lance.spark.substrait.LiteralEncoder} — Spark V2 {@code Literal&lt;?&gt;} →
 *       Substrait {@code Expression.Literal}.
 *   <li>{@link org.lance.spark.substrait.FunctionNames} — static map from Spark V2 op names to bare
 *       Substrait function names and their extension URIs.
 *   <li>{@link org.lance.spark.substrait.PredicateEncoder} / {@link
 *       org.lance.spark.substrait.AggregateEncoder} — Phase 1 / Phase 2 markers, implemented in
 *       follow-up PRs.
 * </ul>
 *
 * <p>Driver-side only. {@link org.lance.spark.substrait.SubstraitContext} is mutable and lives for
 * a single encode; encoded bytes are shipped to workers via {@code LanceInputPartition}.
 *
 * <p>The published {@code lance-spark-bundle-*} uber-jars relocate {@code io.substrait} and {@code
 * com.google.protobuf} under {@code org.lance.spark.shaded.*}, so substrait-java's bundled protobuf
 * cannot collide with the version Spark ships at runtime.
 */
package org.lance.spark.substrait;
