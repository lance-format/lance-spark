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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Reads Lance v2 file footer and column metadata to extract the compression schemes actually
 * applied to each column.
 *
 * <p>This is a test-only utility for verifying that TBLPROPERTIES compression settings are
 * propagated through the write path into the on-disk Lance file format. It parses the binary
 * protobuf encoding tree without requiring the protobuf-java runtime by implementing minimal varint
 * and tag-length-value parsing.
 *
 * <h3>Lance v2 file layout (from EOF):</h3>
 *
 * <pre>
 * [Footer - 40 bytes at EOF]
 *   u64: column_meta_start
 *   u64: column_meta_offsets_start  (CMO table)
 *   u64: global_buff_offsets_start  (GBO table)
 *   u32: num_global_buffers
 *   u32: num_columns
 *   u16: major_version
 *   u16: minor_version
 *   "LANC" (4 bytes magic)
 * </pre>
 *
 * <p>The CMO table contains {@code num_columns} entries of 16 bytes each (u64 offset, u64 length),
 * pointing to protobuf-encoded {@code ColumnMetadata} messages.
 */
public final class LanceFileCompressionReader {

  /** Compression schemes defined in Lance's encodings_v2_1.proto. */
  public enum CompressionScheme {
    UNSPECIFIED,
    LZ4,
    ZSTD;

    static CompressionScheme fromProtoValue(int value) {
      switch (value) {
        case 1:
          return LZ4;
        case 2:
          return ZSTD;
        default:
          return UNSPECIFIED;
      }
    }
  }

  private static final byte[] MAGIC = {0x4C, 0x41, 0x4E, 0x43}; // "LANC"
  private static final int FOOTER_LEN = 40;

  /** Protobuf wire types. */
  private static final int WIRE_VARINT = 0;

  private static final int WIRE_FIXED64 = 1;
  private static final int WIRE_LENGTH_DELIMITED = 2;
  private static final int WIRE_FIXED32 = 5;

  private LanceFileCompressionReader() {}

  /**
   * Reads compression schemes for all columns in a single Lance data file.
   *
   * @param lanceFile path to a .lance data file
   * @return list of sets, one per column; each set contains the distinct compression schemes found
   *     in that column's page encodings
   */
  public static List<Set<CompressionScheme>> readColumnCompressions(Path lanceFile)
      throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(lanceFile.toFile(), "r")) {
      long fileLen = raf.length();
      if (fileLen < FOOTER_LEN) {
        throw new IOException("File too small to contain a Lance footer: " + fileLen);
      }

      // Read 40-byte footer
      raf.seek(fileLen - FOOTER_LEN);
      byte[] footer = new byte[FOOTER_LEN];
      raf.readFully(footer);

      // Verify magic
      for (int i = 0; i < 4; i++) {
        if (footer[36 + i] != MAGIC[i]) {
          throw new IOException("Not a Lance file (bad magic)");
        }
      }

      ByteBuffer fb = ByteBuffer.wrap(footer).order(ByteOrder.LITTLE_ENDIAN);
      long cmoStart = fb.getLong(8); // column_meta_offsets_start
      int numColumns = fb.getInt(28);

      // Read CMO table: num_columns * 16 bytes
      raf.seek(cmoStart);
      byte[] cmoTable = new byte[numColumns * 16];
      raf.readFully(cmoTable);
      ByteBuffer cmoBuf = ByteBuffer.wrap(cmoTable).order(ByteOrder.LITTLE_ENDIAN);

      List<Set<CompressionScheme>> result = new ArrayList<>(numColumns);

      for (int col = 0; col < numColumns; col++) {
        long pos = cmoBuf.getLong(col * 16);
        long len = cmoBuf.getLong(col * 16 + 8);

        raf.seek(pos);
        byte[] colMeta = new byte[(int) len];
        raf.readFully(colMeta);

        result.add(extractCompressionSchemes(colMeta));
      }

      return result;
    }
  }

  /**
   * Finds all Lance data files (*.lance regular files) under the given dataset directory.
   *
   * @param datasetDir the .lance dataset root (e.g., {@code /tmp/table_name.lance})
   * @return list of paths to individual data files
   */
  public static List<Path> findDataFiles(Path datasetDir) throws IOException {
    Path dataDir = datasetDir.resolve("data");
    if (!Files.isDirectory(dataDir)) {
      return Collections.emptyList();
    }

    List<Path> files = new ArrayList<>();
    Files.walkFileTree(
        dataDir,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            if (file.getFileName().toString().endsWith(".lance")) {
              files.add(file);
            }
            return FileVisitResult.CONTINUE;
          }
        });
    return files;
  }

  /**
   * Searches recursively under {@code root} for a Lance dataset directory that belongs to the given
   * table and returns the Lance data files within it.
   *
   * <p>Handles two naming conventions used by the DirectoryNamespace:
   *
   * <ul>
   *   <li>Root namespace with dir_listing: {@code {tableName}.lance}
   *   <li>Manifest mode with child namespace: {@code {8hex}_{namespace}${tableName}} (e.g., {@code
   *       a1b2c3d4_default$my_table})
   * </ul>
   *
   * A directory is considered a Lance dataset if it contains a {@code data/} subdirectory.
   *
   * @param root the search root (e.g., the catalog root or temp directory)
   * @param tableName the table name (without the .lance suffix)
   * @return list of paths to individual data files
   */
  public static List<Path> findDataFilesForTable(Path root, String tableName) throws IOException {
    String dirListingName = tableName + ".lance";
    String manifestSuffix = "$" + tableName;
    List<Path> datasetDirs = new ArrayList<>();

    Files.walkFileTree(
        root,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
            if (dir.getFileName() == null) {
              return FileVisitResult.CONTINUE;
            }
            String name = dir.getFileName().toString();
            if (name.equals(dirListingName) || name.endsWith(manifestSuffix)) {
              // Verify it looks like a Lance dataset (has a data/ subdirectory)
              if (Files.isDirectory(dir.resolve("data"))) {
                datasetDirs.add(dir);
                return FileVisitResult.SKIP_SUBTREE;
              }
            }
            return FileVisitResult.CONTINUE;
          }
        });

    List<Path> allFiles = new ArrayList<>();
    for (Path dsDir : datasetDirs) {
      allFiles.addAll(findDataFiles(dsDir));
    }
    return allFiles;
  }

  // --------------- Protobuf parsing internals ---------------

  /**
   * Extracts compression schemes from a serialized ColumnMetadata protobuf.
   *
   * <p>Rather than navigating the exact protobuf field hierarchy (which is fragile across schema
   * versions), this method recursively scans all nested messages within the ColumnMetadata looking
   * for BufferCompression messages. A BufferCompression is identified by its structure: a small
   * message (≤12 bytes) containing only varint fields, with field 1 having value 0 (UNSPECIFIED), 1
   * (LZ4), or 2 (ZSTD).
   */
  static Set<CompressionScheme> extractCompressionSchemes(byte[] columnMetadata) {
    Set<CompressionScheme> schemes = EnumSet.noneOf(CompressionScheme.class);
    scanForBufferCompression(columnMetadata, schemes);
    return schemes;
  }

  /**
   * Recursively scans protobuf bytes for BufferCompression messages.
   *
   * <p>Descends into all length-delimited fields (which may be nested protobuf messages), and at
   * each level attempts to identify BufferCompression messages by their structure. Parsing errors
   * at any nesting level are caught and silently ignored, since length-delimited fields may also be
   * raw bytes or packed repeated fields rather than sub-messages.
   */
  private static void scanForBufferCompression(byte[] bytes, Set<CompressionScheme> schemes) {
    int[] pos = {0};
    try {
      while (pos[0] < bytes.length) {
        int tag = readVarint32(bytes, pos);
        int wireType = tag & 0x7;

        if (wireType == WIRE_LENGTH_DELIMITED) {
          byte[] inner = readLengthDelimited(bytes, pos);
          // Try to parse as BufferCompression first
          CompressionScheme scheme = tryParseBufferCompression(inner);
          if (scheme != null) {
            schemes.add(scheme);
          }
          // Always recurse into sub-messages to find deeper BufferCompression instances
          scanForBufferCompression(inner, schemes);
        } else if (wireType == WIRE_VARINT) {
          readVarint64(bytes, pos);
        } else if (wireType == WIRE_FIXED64) {
          pos[0] += 8;
        } else if (wireType == WIRE_FIXED32) {
          pos[0] += 4;
        } else {
          // Unknown wire type (e.g. deprecated groups) — stop parsing this message
          break;
        }
      }
    } catch (Exception e) {
      // Parsing error (truncated data, invalid varint, etc.) — return what we found so far
    }
  }

  /**
   * Attempts to parse a byte array as a BufferCompression protobuf message.
   *
   * <p>BufferCompression has: scheme (field 1, varint) and optional level (field 2, varint). If the
   * bytes match this structure and field 1 has value 0, 1, or 2, we treat it as a valid
   * BufferCompression.
   *
   * @return the compression scheme if parsing succeeds, null otherwise
   */
  private static CompressionScheme tryParseBufferCompression(byte[] bytes) {
    if (bytes.length == 0 || bytes.length > 12) {
      return null;
    }
    int[] pos = {0};
    CompressionScheme found = null;
    try {
      while (pos[0] < bytes.length) {
        int tag = readVarint32(bytes, pos);
        int fieldNum = tag >>> 3;
        int wireType = tag & 0x7;

        if (wireType != WIRE_VARINT) {
          return null; // BufferCompression only has varint fields
        }

        long value = readVarint64(bytes, pos);

        if (fieldNum == 1 && value >= 0 && value <= 2) {
          found = CompressionScheme.fromProtoValue((int) value);
        } else if (fieldNum == 2) {
          // level field — valid, continue
        } else {
          return null; // unexpected field
        }
      }
    } catch (Exception e) {
      return null;
    }
    return found;
  }

  // --------------- Low-level protobuf wire format helpers ---------------

  private static int readVarint32(byte[] data, int[] pos) {
    return (int) readVarint64(data, pos);
  }

  private static long readVarint64(byte[] data, int[] pos) {
    long result = 0;
    int shift = 0;
    while (pos[0] < data.length) {
      byte b = data[pos[0]++];
      result |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return result;
      }
      shift += 7;
      if (shift >= 64) {
        throw new IllegalStateException("Varint too long");
      }
    }
    throw new IllegalStateException("Truncated varint");
  }

  private static byte[] readLengthDelimited(byte[] data, int[] pos) {
    int len = readVarint32(data, pos);
    if (len < 0 || pos[0] + len > data.length) {
      throw new IllegalStateException("Invalid length-delimited field: len=" + len);
    }
    byte[] result = new byte[len];
    System.arraycopy(data, pos[0], result, 0, len);
    pos[0] += len;
    return result;
  }

  private static void skipField(byte[] data, int[] pos, int wireType) {
    switch (wireType) {
      case WIRE_VARINT:
        readVarint64(data, pos);
        break;
      case WIRE_FIXED64:
        pos[0] += 8;
        break;
      case WIRE_LENGTH_DELIMITED:
        int len = readVarint32(data, pos);
        pos[0] += len;
        break;
      case WIRE_FIXED32:
        pos[0] += 4;
        break;
      default:
        throw new IllegalStateException("Unknown wire type: " + wireType);
    }
  }
}
