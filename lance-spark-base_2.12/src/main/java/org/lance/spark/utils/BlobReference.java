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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A compact serializable reference to a blob stored in a Lance dataset.
 *
 * <p>When a blob column is read from a Lance table and flows through Spark's shuffle (e.g. during a
 * JOIN + INSERT INTO), the actual blob bytes are NOT materialized. Instead, a small BlobReference
 * (~200 bytes) is serialized as the binary value. The write side detects these references, opens
 * the source dataset, fetches the actual blob bytes via {@code Dataset.takeBlobs()}, and writes
 * them to the target table.
 *
 * <p>Wire format:
 *
 * <pre>
 *   [8 bytes] magic header (LANCEREF)
 *   [1 byte]  version
 *   [2+N bytes] datasetUri (length-prefixed UTF-8)
 *   [2+N bytes] columnName (length-prefixed UTF-8)
 *   [8 bytes] rowAddress
 * </pre>
 */
public class BlobReference {

  /** 8-byte magic header to identify a serialized BlobReference. */
  public static final byte[] MAGIC = {'L', 'A', 'N', 'C', 'E', 'R', 'E', 'F'};

  /** Min byte length: magic(8) + version(1) + two empty strings(2+2) + rowAddress(8). */
  private static final int MIN_SIZE = MAGIC.length + 1 + 2 + 2 + 8;

  private static final byte VERSION = 1;

  private final String datasetUri;
  private final String columnName;
  private final long rowAddress;

  public BlobReference(String datasetUri, String columnName, long rowAddress) {
    this.datasetUri = datasetUri;
    this.columnName = columnName;
    this.rowAddress = rowAddress;
  }

  /**
   * Checks whether a byte array is a valid serialized BlobReference by verifying the magic header,
   * version, and that the encoded string lengths are consistent with the total size.
   */
  public static boolean isBlobReference(byte[] bytes) {
    if (bytes == null || bytes.length < MIN_SIZE) {
      return false;
    }
    for (int i = 0; i < MAGIC.length; i++) {
      if (bytes[i] != MAGIC[i]) {
        return false;
      }
    }
    if (bytes[MAGIC.length] != VERSION) {
      return false;
    }
    try {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
      in.skipBytes(MAGIC.length + 1);
      int uriLen = in.readUnsignedShort();
      int remaining = bytes.length - MAGIC.length - 1 - 2;
      if (uriLen < 0 || uriLen > remaining) {
        return false;
      }
      in.skipBytes(uriLen);
      int colLen = in.readUnsignedShort();
      remaining = remaining - uriLen - 2;
      if (colLen < 0 || colLen > remaining) {
        return false;
      }
      int expectedRemaining = colLen + 8;
      return remaining == expectedRemaining;
    } catch (IOException e) {
      return false;
    }
  }

  /** Serialize this reference to a compact byte array. */
  public byte[] serialize() {
    return appendRowAddress(serializePrefix(datasetUri, columnName), rowAddress);
  }

  /**
   * Serializes the constant portion of a reference: everything except the trailing 8-byte
   * rowAddress (i.e. magic + version + datasetUri + columnName).
   *
   * <p>{@code datasetUri} and {@code columnName} are constant for an entire scan batch, so callers
   * on the per-row read hot path should compute this prefix once and then call {@link
   * #appendRowAddress(byte[], long)} per row instead of re-encoding the strings every time.
   */
  public static byte[] serializePrefix(String datasetUri, String columnName) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
      DataOutputStream out = new DataOutputStream(baos);
      out.write(MAGIC);
      out.writeByte(VERSION);
      writeString(out, datasetUri);
      writeString(out, columnName);
      out.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize BlobReference", e);
    }
  }

  /**
   * Returns a full serialized reference: {@code prefix} (from {@link #serializePrefix}) followed by
   * the 8-byte big-endian {@code rowAddress}, matching {@link DataOutputStream#writeLong}.
   */
  public static byte[] appendRowAddress(byte[] prefix, long rowAddress) {
    byte[] out = Arrays.copyOf(prefix, prefix.length + 8);
    int off = prefix.length;
    out[off] = (byte) (rowAddress >>> 56);
    out[off + 1] = (byte) (rowAddress >>> 48);
    out[off + 2] = (byte) (rowAddress >>> 40);
    out[off + 3] = (byte) (rowAddress >>> 32);
    out[off + 4] = (byte) (rowAddress >>> 24);
    out[off + 5] = (byte) (rowAddress >>> 16);
    out[off + 6] = (byte) (rowAddress >>> 8);
    out[off + 7] = (byte) rowAddress;
    return out;
  }

  /** Deserialize a BlobReference from bytes. */
  public static BlobReference deserialize(byte[] bytes) {
    if (!isBlobReference(bytes)) {
      throw new IllegalArgumentException("Not a valid BlobReference");
    }
    try {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
      in.skipBytes(MAGIC.length);
      in.readByte(); // version, already validated
      String datasetUri = readString(in);
      String columnName = readString(in);
      long rowAddress = in.readLong();
      return new BlobReference(datasetUri, columnName, rowAddress);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize BlobReference", e);
    }
  }

  private static void writeString(DataOutputStream out, String s) throws IOException {
    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    out.writeShort(bytes.length);
    out.write(bytes);
  }

  private static String readString(DataInputStream in) throws IOException {
    int len = in.readUnsignedShort();
    byte[] bytes = new byte[len];
    in.readFully(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  // ========== Getters ==========

  public String getDatasetUri() {
    return datasetUri;
  }

  public String getColumnName() {
    return columnName;
  }

  public long getRowAddress() {
    return rowAddress;
  }

  @Override
  public String toString() {
    return String.format(
        "BlobReference{dataset=%s, column=%s, rowAddr=0x%016X}",
        datasetUri, columnName, rowAddress);
  }
}
