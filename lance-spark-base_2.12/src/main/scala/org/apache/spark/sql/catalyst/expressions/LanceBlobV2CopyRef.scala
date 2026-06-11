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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.lance.spark.utils.{BlobReference, BlobUtils}

/**
 * [[LanceBlobV2CopyRef]] turns a blob v2 descriptor struct into a [[org.lance.spark.utils.BlobReference]]
 * BINARY token for the write path.
 */
case class LanceBlobV2CopyRef(
    blobDescriptor: Expression,
    rowAddress: Expression,
    datasetUri: String,
    columnName: String)
  extends Expression
  with CodegenFallback {

  override def children: Seq[Expression] = Seq(blobDescriptor, rowAddress)

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = true

  // datasetUri and columnName are constant for the whole scan, so encode the reference prefix once.
  @transient private lazy val prefix: Array[Byte] =
    BlobReference.serializePrefix(datasetUri, columnName)

  override def eval(input: InternalRow): Any = {
    val descriptor = blobDescriptor.eval(input)
    if (descriptor == null) {
      null
    } else {
      val desc = descriptor.asInstanceOf[InternalRow]
      if (BlobUtils.isNullBlobV2Descriptor(desc)) {
        null
      } else {
        val size = desc.getLong(BlobUtils.BLOB_DESCRIPTOR_SIZE_ORDINAL)
        val rowAddr = rowAddress.eval(input)
        // A null row address would unbox to 0L and silently copy row 0's blob into this row.
        if (rowAddr == null) {
          throw new IllegalStateException(
            s"Row address is null while copying blob column '$columnName' from $datasetUri. " +
              "Cannot locate the source blob")
        }
        BlobReference.appendRowAddressAndSize(prefix, rowAddr.asInstanceOf[Long], size)
      }
    }
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): LanceBlobV2CopyRef =
    copy(blobDescriptor = newChildren(0), rowAddress = newChildren(1))
}
