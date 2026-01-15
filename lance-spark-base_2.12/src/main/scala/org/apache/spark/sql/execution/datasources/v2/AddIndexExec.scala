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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.{AddIndexOutputType, NamedArgument}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.util.LanceSerializeUtil.{decode, encode}
import org.apache.spark.unsafe.types.UTF8String
import org.json4s.JsonAST.{JBool, JDouble, JField, JInt, JNull, JObject, JString}
import org.json4s.jackson.JsonMethods.{compact, render}
import org.lance.Dataset
import org.lance.ReadOptions
import org.lance.index.{Index, IndexOptions, IndexParams, IndexType}
import org.lance.index.scalar.ScalarIndexParams
import org.lance.operation.{CreateIndex => AddIndexOperation}
import org.lance.spark.{BaseLanceNamespaceSparkCatalog, LanceDataset, LanceRuntime, LanceSparkReadOptions}

import java.util.{Collections, Optional, UUID}

import scala.jdk.CollectionConverters._

/**
 * Physical execution of distributed CREATE INDEX (ALTER TABLE ... CREATE INDEX ...) for Lance datasets.
 *
 * This builds per-fragment indexes with the provided options, merges index metadata
 * and commits an index-creation transaction.
 */
case class AddIndexExec(
    catalog: TableCatalog,
    ident: Identifier,
    indexName: String,
    method: String,
    columns: Seq[String],
    args: Seq[NamedArgument]) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = AddIndexOutputType.SCHEMA

  private def toJson(args: Seq[NamedArgument]): String = {
    if (args.isEmpty) {
      "{}"
    } else {
      val fields = args.map { a =>
        val jv = a.value match {
          case null => JNull
          case s: java.lang.String =>
            val trimmed = s.stripPrefix("\"").stripSuffix("\"").stripPrefix("'").stripSuffix("'")
            JString(trimmed)
          case b: java.lang.Boolean => JBool(b.booleanValue())
          case c: java.lang.Character => JString(String.valueOf(c))
          case by: java.lang.Byte => JInt(BigInt(by.intValue()))
          case sh: java.lang.Short => JInt(BigInt(sh.intValue()))
          case i: java.lang.Integer => JInt(BigInt(i.intValue()))
          case l: java.lang.Long => JInt(BigInt(l.longValue()))
          case f: java.lang.Float => JDouble(f.doubleValue())
          case d: java.lang.Double => JDouble(d.doubleValue())
          case other => JString(String.valueOf(other))
        }
        JField(a.name, jv)
      }
      compact(render(JObject(fields.toList)))
    }
  }

  override protected def run(): Seq[InternalRow] = {
    val lanceDataset = catalog.loadTable(ident) match {
      case d: LanceDataset => d
      case _ => throw new UnsupportedOperationException("AddIndex only supports LanceDataset")
    }

    val readOptions = lanceDataset.readOptions()

    // Get all fragment id list from dataset
    val fragmentIds = {
      val ds = openDataset(readOptions)
      try {
        ds.getFragments.asScala.map(_.getId).map(Integer.valueOf).toList
      } finally {
        ds.close()
      }
    }

    if (fragmentIds.isEmpty) {
      // No fragments to index
      return Seq(new GenericInternalRow(Array[Any](0L, UTF8String.fromString(indexName))))
    }

    val uuid = UUID.randomUUID()
    val indexType = IndexTypeUtils.buildIndexType(method)

    // Get namespace info from catalog if available (for credential vending on workers)
    val (nsImpl, nsProps, tableId, initialStorageOpts): (
        Option[String],
        Option[Map[String, String]],
        Option[List[String]],
        Option[Map[String, String]]) = catalog match {
      case nsCatalog: BaseLanceNamespaceSparkCatalog =>
        (
          Option(nsCatalog.getNamespaceImpl),
          Option(nsCatalog.getNamespaceProperties).map(_.asScala.toMap),
          Option(readOptions.getTableId).map(_.asScala.toList),
          Option(lanceDataset.getInitialStorageOptions).map(_.asScala.toMap))
      case _ => (None, None, None, None)
    }

    // Build per-fragment tasks
    val tasks = fragmentIds.map { fid =>
      IndexTaskExecutor.create(
        readOptions,
        columns,
        method.toLowerCase,
        toJson(args),
        indexName,
        uuid.toString,
        fid,
        nsImpl,
        nsProps,
        tableId,
        initialStorageOpts)
    }.toSeq

    val rdd = session.sparkContext.parallelize(tasks, tasks.size)
    rdd.map(t => t.execute()).collect() // ensure execution

    val dataset = openDataset(readOptions)
    try {
      // Merge index metadata after all fragments are indexed
      dataset.mergeIndexMetadata(uuid.toString, indexType, Optional.empty())

      val fieldIds = dataset.getLanceSchema.fields().asScala
        .filter(f => columns.contains(f.getName))
        .map(_.getId)
        .toList

      val datasetVersion = dataset.version()

      val index = Index
        .builder()
        .uuid(uuid)
        .name(indexName)
        .fields(fieldIds.map(java.lang.Integer.valueOf).asJava)
        .datasetVersion(datasetVersion)
        .indexVersion(0)
        .fragments(fragmentIds.asJava)
        .build()

      val op = AddIndexOperation.builder().withNewIndices(Collections.singletonList(index)).build()
      val newDataset = dataset.newTransactionBuilder().operation(op).build().commit()

      // close the committed new dataset to release resources
      newDataset.close()
    } finally {
      dataset.close()
    }

    Seq(new GenericInternalRow(Array[Any](
      fragmentIds.size.toLong,
      UTF8String.fromString(indexName))))
  }

  private def openDataset(readOptions: LanceSparkReadOptions): Dataset = {
    if (readOptions.hasNamespace) {
      Dataset.open()
        .allocator(LanceRuntime.allocator())
        .namespace(readOptions.getNamespace)
        .tableId(readOptions.getTableId)
        .build()
    } else {
      Dataset.open()
        .allocator(LanceRuntime.allocator())
        .uri(readOptions.getDatasetUri)
        .readOptions(readOptions.toReadOptions)
        .build()
    }
  }
}

case class IndexTaskExecutor(
    lanceConf: String,
    columnsEnc: String,
    method: String,
    json: String,
    indexName: String,
    uuid: String,
    fragmentId: Int,
    namespaceImpl: Option[String],
    namespaceProperties: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOptions: Option[Map[String, String]]) extends Serializable {

  def execute(): String = {
    val readOptions = decode[LanceSparkReadOptions](lanceConf)
    val columns = decode[Array[String]](columnsEnc).toSeq
    val indexType = IndexTypeUtils.buildIndexType(method)
    val params = IndexParams.builder()
      .setScalarIndexParams(ScalarIndexParams.create(method, json))
      .build()

    val indexOptions = IndexOptions
      .builder(java.util.Arrays.asList(columns: _*), indexType, params)
      .replace(true)
      .withIndexName(indexName)
      .withIndexUUID(uuid)
      .withFragmentIds(Collections.singletonList(fragmentId))
      .build()

    // Build ReadOptions with merged storage options and credential refresh provider
    val merged = LanceRuntime.mergeStorageOptions(
      readOptions.getStorageOptions,
      initialStorageOptions.map(_.asJava).orNull)
    val provider = LanceRuntime.getOrCreateStorageOptionsProvider(
      namespaceImpl.orNull,
      namespaceProperties.map(_.asJava).orNull,
      tableId.map(_.asJava).orNull)

    val builder = new ReadOptions.Builder().setStorageOptions(merged)
    if (provider != null) {
      builder.setStorageOptionsProvider(provider)
    }

    val dataset = Dataset.open()
      .allocator(LanceRuntime.allocator())
      .uri(readOptions.getDatasetUri)
      .readOptions(builder.build())
      .build()

    try {
      dataset.createIndex(indexOptions)
    } finally {
      dataset.close()
    }

    encode("OK")
  }
}

object IndexTaskExecutor {
  def create(
      readOptions: LanceSparkReadOptions,
      cols: Seq[String],
      method: String,
      json: String,
      indexName: String,
      uuid: String,
      fragmentId: Int,
      namespaceImpl: Option[String],
      namespaceProperties: Option[Map[String, String]],
      tableId: Option[List[String]],
      initialStorageOptions: Option[Map[String, String]]): IndexTaskExecutor = {
    IndexTaskExecutor(
      encode(readOptions),
      encode(cols.toArray),
      method,
      json,
      indexName,
      uuid,
      fragmentId,
      namespaceImpl,
      namespaceProperties,
      tableId,
      initialStorageOptions)
  }
}

/**
 * Utility methods for working with index types.
 */
object IndexTypeUtils {

  /**
   * Build an [[IndexType]] from the given index method string.
   *
   * @param method the index method name
   * @return the corresponding [[IndexType]]
   * @throws UnsupportedOperationException if the method is not supported
   */
  def buildIndexType(method: String): IndexType = {
    method.toLowerCase match {
      case "btree" => IndexType.BTREE
      case "fts" => IndexType.INVERTED
      case other => throw new UnsupportedOperationException(s"Unsupported index method: $other")
    }
  }
}
