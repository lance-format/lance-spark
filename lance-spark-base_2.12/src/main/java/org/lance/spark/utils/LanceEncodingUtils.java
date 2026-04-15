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

import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Utility methods and constants for per-column Lance encoding configuration via Spark
 * TBLPROPERTIES.
 *
 * <p>The TBLPROPERTIES key convention is {@code <column>.lance.<key>}. These map 1-to-1 to {@code
 * lance-encoding:<key>} Arrow field metadata keys, which the Lance Rust encoder reads at write
 * time.
 *
 * <p>Only connector-supported keys are handled (the five mapped below); dict/minichunk keys are
 * deferred. Unmatched column names and type-incompatible combinations are silently ignored —
 * semantic validation is left to the Lance Rust encoder.
 */
public final class LanceEncodingUtils {

  /** Domain segment used in TBLPROPERTIES keys: {@code <column>.lance.<key>}. */
  static final String LANCE_PROPERTY_DOMAIN = "lance";

  // TBLPROPERTIES key suffixes (after "<column>.")
  static final String COMPRESSION_SUFFIX = LANCE_PROPERTY_DOMAIN + ".compression";
  static final String COMPRESSION_LEVEL_SUFFIX = LANCE_PROPERTY_DOMAIN + ".compression-level";
  static final String STRUCTURAL_ENCODING_SUFFIX = LANCE_PROPERTY_DOMAIN + ".structural-encoding";
  static final String RLE_THRESHOLD_SUFFIX = LANCE_PROPERTY_DOMAIN + ".rle-threshold";
  static final String BSS_SUFFIX = LANCE_PROPERTY_DOMAIN + ".bss";

  // Arrow field metadata keys
  static final String LANCE_ENCODING_COMPRESSION = "lance-encoding:compression";
  static final String LANCE_ENCODING_COMPRESSION_LEVEL = "lance-encoding:compression-level";
  static final String LANCE_ENCODING_STRUCTURAL_ENCODING = "lance-encoding:structural-encoding";
  static final String LANCE_ENCODING_RLE_THRESHOLD = "lance-encoding:rle-threshold";
  static final String LANCE_ENCODING_BSS = "lance-encoding:bss";

  // Valid value sets
  private static final Set<String> VALID_COMPRESSION_SCHEMES =
      Set.of("zstd", "lz4", "fsst", "none");

  private static final Set<String> VALID_STRUCTURAL_ENCODINGS = Set.of("miniblock", "fullzip");

  private static final Set<String> VALID_BSS_MODES = Set.of("off", "on", "auto");

  private static final List<EncodingPropertyRule> SUPPORTED_ENCODING_PROPERTY_RULES =
      List.of(
          rule(
              COMPRESSION_SUFFIX,
              LANCE_ENCODING_COMPRESSION,
              LanceEncodingUtils::validateCompressionScheme),
          rule(
              COMPRESSION_LEVEL_SUFFIX,
              LANCE_ENCODING_COMPRESSION_LEVEL,
              LanceEncodingUtils::validateCompressionLevel),
          rule(
              STRUCTURAL_ENCODING_SUFFIX,
              LANCE_ENCODING_STRUCTURAL_ENCODING,
              LanceEncodingUtils::validateStructuralEncoding),
          rule(
              RLE_THRESHOLD_SUFFIX,
              LANCE_ENCODING_RLE_THRESHOLD,
              LanceEncodingUtils::validateRleThreshold),
          rule(BSS_SUFFIX, LANCE_ENCODING_BSS, LanceEncodingUtils::validateBssMode));

  private LanceEncodingUtils() {
    // Utility class
  }

  static String createPropertyKey(String columnName, String propertySuffix) {
    return columnName + "." + propertySuffix;
  }

  static List<EncodingPropertyRule> getSupportedEncodingPropertyRules() {
    return SUPPORTED_ENCODING_PROPERTY_RULES;
  }

  private static void validateCompressionScheme(String columnName, String value) {
    if (!VALID_COMPRESSION_SCHEMES.contains(value)) {
      throw new IllegalArgumentException(
          "Column '"
              + columnName
              + "': invalid compression scheme '"
              + value
              + "'. Valid values: "
              + VALID_COMPRESSION_SCHEMES);
    }
  }

  // codec-specific upper-bound enforcement is left to the Rust encoder
  private static void validateCompressionLevel(String columnName, String value) {
    int level;
    try {
      level = Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Column '"
              + columnName
              + "': invalid compression-level '"
              + value
              + "'. Must be a non-negative integer.");
    }
    if (level < 0) {
      throw new IllegalArgumentException(
          "Column '" + columnName + "': compression-level '" + value + "' must be non-negative.");
    }
  }

  private static void validateStructuralEncoding(String columnName, String value) {
    if (!VALID_STRUCTURAL_ENCODINGS.contains(value)) {
      throw new IllegalArgumentException(
          "Column '"
              + columnName
              + "': invalid structural-encoding '"
              + value
              + "'. Valid values: "
              + VALID_STRUCTURAL_ENCODINGS);
    }
  }

  private static void validateRleThreshold(String columnName, String value) {
    float threshold;
    try {
      threshold = Float.parseFloat(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Column '"
              + columnName
              + "': invalid rle-threshold '"
              + value
              + "'. Must be a float in (0.0, 1.0].");
    }
    if (threshold <= 0.0f || threshold > 1.0f) {
      throw new IllegalArgumentException(
          "Column '"
              + columnName
              + "': rle-threshold '"
              + value
              + "' is out of range. Must be in (0.0, 1.0].");
    }
  }

  private static void validateBssMode(String columnName, String value) {
    if (!VALID_BSS_MODES.contains(value)) {
      throw new IllegalArgumentException(
          "Column '"
              + columnName
              + "': invalid bss '"
              + value
              + "'. Valid values: "
              + VALID_BSS_MODES);
    }
  }

  private static EncodingPropertyRule rule(
      String propertySuffix, String arrowMetadataKey, BiConsumer<String, String> validator) {
    return new EncodingPropertyRule(propertySuffix, arrowMetadataKey, validator);
  }

  /** Rule descriptor for mapping a Spark TBLPROPERTY to an Arrow field metadata key. */
  static class EncodingPropertyRule {
    private final String propertySuffix;
    private final String arrowMetadataKey;
    private final BiConsumer<String, String> validator;

    private EncodingPropertyRule(
        String propertySuffix, String arrowMetadataKey, BiConsumer<String, String> validator) {
      this.propertySuffix = propertySuffix;
      this.arrowMetadataKey = arrowMetadataKey;
      this.validator = validator;
    }

    String getPropertySuffix() {
      return propertySuffix;
    }

    String getArrowMetadataKey() {
      return arrowMetadataKey;
    }

    String createPropertyKey(String columnName) {
      return LanceEncodingUtils.createPropertyKey(columnName, propertySuffix);
    }

    void validate(String columnName, String value) {
      validator.accept(columnName, value);
    }
  }
}
