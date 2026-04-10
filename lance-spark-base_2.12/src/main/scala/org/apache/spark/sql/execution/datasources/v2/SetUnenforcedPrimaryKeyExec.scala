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
import org.apache.spark.sql.catalyst.plans.logical.SetUnenforcedPrimaryKeyOutputType
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.unsafe.types.UTF8String
import org.lance.{CommitBuilder, Transaction}
import org.lance.operation.{UpdateConfig, UpdateMap}
import org.lance.spark.LanceDataset
import org.lance.spark.utils.Utils

import scala.collection.JavaConverters._

object SetUnenforcedPrimaryKeyExec {
  val METADATA_KEY_PK = "lance-schema:unenforced-primary-key"
  val METADATA_KEY_PK_POSITION =
    "lance-schema:unenforced-primary-key:position"
}

case class SetUnenforcedPrimaryKeyExec(
    catalog: TableCatalog,
    ident: Identifier,
    columns: Seq[String]) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = SetUnenforcedPrimaryKeyOutputType.SCHEMA

  override protected def run(): Seq[InternalRow] = {
    val lanceDataset = catalog.loadTable(ident) match {
      case ds: LanceDataset => ds
      case _ =>
        throw new UnsupportedOperationException(
          "SET UNENFORCED PRIMARY KEY only supports LanceDataset")
    }

    val readOptions = lanceDataset.readOptions()
    val dataset = Utils.openDatasetBuilder(readOptions).build()
    try {
      val lanceSchema = dataset.getLanceSchema
      val allFields = lanceSchema.fields().asScala.toSeq

      // Validate no duplicate columns
      val duplicates = columns.groupBy(identity).collect {
        case (name, occurrences) if occurrences.size > 1 => name
      }
      if (duplicates.nonEmpty) {
        throw new IllegalArgumentException(
          s"Duplicate columns in primary key: ${duplicates.mkString(", ")}")
      }

      // Validate columns exist
      val fieldsByName = allFields.map(f => (f.getName, f)).toMap
      val resolvedFields = columns.map { colName =>
        fieldsByName.getOrElse(
          colName,
          throw new IllegalArgumentException(
            s"Column '$colName' not found in table schema. " +
              s"Available columns: ${allFields.map(_.getName).mkString(", ")}"))
      }

      // Validate no existing unenforced primary key
      val existingPkFields = allFields.filter(f =>
        f.isUnenforcedPrimaryKey || hasPrimaryKeyMetadata(f))
      if (existingPkFields.nonEmpty) {
        throw new IllegalStateException(
          "Table already has unenforced primary key defined on columns: " +
            s"${existingPkFields.map(_.getName).mkString(", ")}. " +
            "Unenforced primary key cannot be changed once set.")
      }

      // Validate fields are non-nullable leaf fields
      resolvedFields.foreach { field =>
        if (field.isNullable) {
          throw new IllegalArgumentException(
            s"Column '${field.getName}' is nullable. " +
              "Unenforced primary key columns must be non-nullable.")
        }
        if (!field.getChildren.isEmpty) {
          throw new IllegalArgumentException(
            s"Column '${field.getName}' is not a leaf field. " +
              "Unenforced primary key columns must be primitive types.")
        }
      }

      // Build field metadata updates
      val fieldMetadataUpdates =
        new java.util.HashMap[java.lang.Integer, UpdateMap]()
      resolvedFields.zipWithIndex.foreach { case (field, idx) =>
        val metadataMap = new java.util.HashMap[String, String]()
        metadataMap.put(SetUnenforcedPrimaryKeyExec.METADATA_KEY_PK, "true")
        metadataMap.put(
          SetUnenforcedPrimaryKeyExec.METADATA_KEY_PK_POSITION,
          (idx + 1).toString)
        fieldMetadataUpdates.put(
          java.lang.Integer.valueOf(field.getId),
          UpdateMap.builder().updates(metadataMap).replace(false).build())
      }

      // Build and commit transaction
      val updateConfig = UpdateConfig.builder()
        .fieldMetadataUpdates(fieldMetadataUpdates)
        .build()
      val txn = new Transaction.Builder()
        .readVersion(dataset.version())
        .operation(updateConfig)
        .build()
      try {
        val newDataset = new CommitBuilder(dataset)
          .writeParams(readOptions.getStorageOptions)
          .execute(txn)
        newDataset.close()
      } finally {
        txn.close()
      }
    } finally {
      dataset.close()
    }

    val columnsStr = columns.mkString(", ")
    Seq(new GenericInternalRow(Array[Any](
      UTF8String.fromString("OK"),
      UTF8String.fromString(columnsStr))))
  }

  private def hasPrimaryKeyMetadata(
      field: org.lance.schema.LanceField): Boolean = {
    val metadata = field.getMetadata
    metadata != null && "true".equalsIgnoreCase(
      metadata.get(SetUnenforcedPrimaryKeyExec.METADATA_KEY_PK))
  }
}
