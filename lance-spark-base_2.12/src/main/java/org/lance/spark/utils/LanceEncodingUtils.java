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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Utility methods and constants for per-column Lance encoding configuration via Spark
 * TBLPROPERTIES.
 *
 * <p>Two TBLPROPERTIES key formats are supported:
 *
 * <ol>
 *   <li><b>Legacy (top-level only):</b> {@code <column>.lance.<key>} — e.g. {@code
 *       payload.lance.compression = zstd}. The {@code <column>} segment is taken verbatim (no
 *       inner-dot splitting), so legacy keys cannot reach nested fields.
 *   <li><b>New (top-level or nested):</b> {@code lance.<key>.column.<segment1>.<segment2>...} —
 *       e.g. {@code lance.compression.column.events.payload = zstd}. Path segments navigate struct
 *       children by name; literal {@code element}, {@code key}, {@code value} navigate array
 *       elements, map keys, and map values respectively.
 * </ol>
 *
 * <p>Both formats accepted indefinitely. When two distinct properties target the same {@code (path,
 * rule)}, the new-format entry wins. When a single literal key is parseable as both (because the
 * user has a top-level column name that itself looks like a new-format key), the legacy
 * interpretation is reserved — keys ending in a legacy suffix such as {@code .lance.compression}
 * are never parsed as nested new-format paths. Field names containing dots are not reachable via
 * the new format in this revision.
 *
 * <p>Nested encoding metadata that <i>crosses an array or map boundary</i> (which has no
 * per-element {@link org.apache.spark.sql.types.StructField}) is smuggled on the nearest enclosing
 * {@code StructField}'s {@code metadata} under the {@link #LANCE_NESTED_PREFIX} prefix; the Scala
 * write path unpacks it onto the corresponding Arrow child {@code Field.metadata}. Metadata for
 * paths that traverse only struct children is written natively on the deepest child's metadata with
 * no smuggling required.
 */
public final class LanceEncodingUtils {

  /** Domain segment used in TBLPROPERTIES keys. */
  static final String LANCE_PROPERTY_DOMAIN = "lance";

  /** Separator that splits the new-format key into {@code lance.<encoding-key>} and dotted path. */
  static final String NEW_FORMAT_SEGMENT = ".column.";

  /**
   * Maximum number of dotted segments allowed in a new-format path. Bounds walker recursion cost on
   * pathological keys.
   */
  static final int MAX_PATH_DEPTH = 16;

  /**
   * Prefix for keys smuggled on a {@link org.apache.spark.sql.types.StructField}'s metadata to
   * carry nested encoding metadata. {@code LanceArrowUtils.toArrowField} (Scala) consumes these and
   * emits them on the corresponding Arrow child {@code Field.metadata}.
   *
   * <p><b>Wire-format invariant:</b> this prefix round-trips through {@code Metadata.json};
   * changing it is forwards-incompatible with datasets whose Spark schema was written under the
   * previous value.
   */
  public static final String LANCE_NESTED_PREFIX = "lance-nested.";

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

  /**
   * Rules sorted by suffix length descending so {@link #parseLegacy} matches the longest suffix
   * first. None of the current five suffixes is a true suffix of another, so removing the sort
   * would not change behavior today — kept as defense against future suffix ambiguity.
   */
  private static final List<EncodingPropertyRule> RULES_BY_SUFFIX_LENGTH_DESC = sortRulesDesc();

  private static List<EncodingPropertyRule> sortRulesDesc() {
    List<EncodingPropertyRule> sorted = new ArrayList<>(SUPPORTED_ENCODING_PROPERTY_RULES);
    sorted.sort(
        (a, b) -> Integer.compare(b.getPropertySuffix().length(), a.getPropertySuffix().length()));
    return List.copyOf(sorted);
  }

  private LanceEncodingUtils() {
    // Utility class
  }

  static String createPropertyKey(String columnName, String propertySuffix) {
    return columnName + "." + propertySuffix;
  }

  static List<EncodingPropertyRule> getSupportedEncodingPropertyRules() {
    return SUPPORTED_ENCODING_PROPERTY_RULES;
  }

  /**
   * Parse legacy-format keys ({@code <column>.lance.<key>}). The {@code <column>} prefix is taken
   * verbatim — NOT split on inner dots — so legacy keys can only address top-level fields.
   * Unrecognised keys are skipped silently. Validation of values is deferred to the walker so that
   * path-not-found errors can suppress validation noise.
   */
  static List<ParsedEncodingKey> parseLegacy(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return Collections.emptyList();
    }
    List<ParsedEncodingKey> out = new ArrayList<>();
    for (Map.Entry<String, String> e : properties.entrySet()) {
      String key = e.getKey();
      if (key == null) {
        continue;
      }
      for (EncodingPropertyRule rule : RULES_BY_SUFFIX_LENGTH_DESC) {
        String marker = "." + rule.getPropertySuffix();
        if (!key.endsWith(marker)) {
          continue;
        }
        String columnName = key.substring(0, key.length() - marker.length());
        if (columnName.isEmpty()) {
          break;
        }
        out.add(new ParsedEncodingKey(Collections.singletonList(columnName), rule, e.getValue()));
        break;
      }
    }
    return out;
  }

  /**
   * Parse new-format keys ({@code lance.<encoding-key>.column.<segment1>.<segment2>...}).
   *
   * <ul>
   *   <li>Keys not starting with {@code lance.}, keys that are valid legacy-format keys, or keys
   *       not containing {@code .column.} are skipped silently (other code paths may interpret
   *       them).
   *   <li>Keys whose shape matches but whose {@code <encoding-key>} portion is not recognised are
   *       skipped silently — users may have unrelated TBLPROPERTIES in this namespace.
   *   <li>Non-legacy keys whose shape matches and whose rule is recognised but whose path is empty,
   *       contains an empty segment, or exceeds {@link #MAX_PATH_DEPTH} segments throw {@link
   *       IllegalArgumentException}.
   * </ul>
   */
  static List<ParsedEncodingKey> parseNewFormat(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return Collections.emptyList();
    }
    String domainPrefix = LANCE_PROPERTY_DOMAIN + ".";
    List<ParsedEncodingKey> out = new ArrayList<>();
    for (Map.Entry<String, String> e : properties.entrySet()) {
      String key = e.getKey();
      if (key == null || !key.startsWith(domainPrefix) || isLegacyEncodingPropertyKey(key)) {
        continue;
      }
      int sepIdx = key.indexOf(NEW_FORMAT_SEGMENT);
      if (sepIdx < 0) {
        continue;
      }
      String rulePrefix = key.substring(0, sepIdx);
      String pathString = key.substring(sepIdx + NEW_FORMAT_SEGMENT.length());

      EncodingPropertyRule matchedRule = null;
      for (EncodingPropertyRule rule : SUPPORTED_ENCODING_PROPERTY_RULES) {
        if (rule.getPropertySuffix().equals(rulePrefix)) {
          matchedRule = rule;
          break;
        }
      }
      if (matchedRule == null) {
        continue;
      }

      if (pathString.isEmpty()) {
        throw new IllegalArgumentException(
            "Invalid TBLPROPERTIES key '"
                + sanitizeForMessage(key)
                + "': missing column path after '"
                + NEW_FORMAT_SEGMENT
                + "'. Expected shape: 'lance.<encoding>.column.<field>[.<nested>...]'"
                + ", e.g. 'lance.compression.column.payload'.");
      }
      String[] segments = pathString.split("\\.", -1);
      for (int i = 0; i < segments.length; i++) {
        if (segments[i].isEmpty()) {
          throw new IllegalArgumentException(
              "Invalid TBLPROPERTIES key '"
                  + sanitizeForMessage(key)
                  + "': empty path segment at index "
                  + i
                  + " (segments cannot be empty; check for leading, trailing, "
                  + "or consecutive dots).");
        }
      }
      if (segments.length > MAX_PATH_DEPTH) {
        throw new IllegalArgumentException(
            "Invalid TBLPROPERTIES key '"
                + sanitizeForMessage(key)
                + "': nested column path has "
                + segments.length
                + " segments, but at most "
                + MAX_PATH_DEPTH
                + " are allowed.");
      }
      out.add(new ParsedEncodingKey(Arrays.asList(segments), matchedRule, e.getValue()));
    }
    return out;
  }

  private static boolean isLegacyEncodingPropertyKey(String key) {
    for (EncodingPropertyRule rule : RULES_BY_SUFFIX_LENGTH_DESC) {
      String marker = "." + rule.getPropertySuffix();
      if (key.endsWith(marker) && key.length() > marker.length()) {
        return true;
      }
    }
    return false;
  }

  private static void validateCompressionScheme(String fullPath, String value) {
    if (!VALID_COMPRESSION_SCHEMES.contains(value)) {
      throw new IllegalArgumentException(
          "Column '"
              + sanitizeForMessage(fullPath)
              + "': invalid compression scheme '"
              + sanitizeForMessage(value)
              + "'. Valid values: "
              + VALID_COMPRESSION_SCHEMES);
    }
  }

  // codec-specific upper-bound enforcement is left to the Rust encoder
  private static void validateCompressionLevel(String fullPath, String value) {
    int level;
    try {
      level = Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Column '"
              + sanitizeForMessage(fullPath)
              + "': invalid compression-level '"
              + sanitizeForMessage(value)
              + "'. Must be a non-negative integer.");
    }
    if (level < 0) {
      throw new IllegalArgumentException(
          "Column '"
              + sanitizeForMessage(fullPath)
              + "': compression-level '"
              + sanitizeForMessage(value)
              + "' must be non-negative.");
    }
  }

  private static void validateStructuralEncoding(String fullPath, String value) {
    if (!VALID_STRUCTURAL_ENCODINGS.contains(value)) {
      throw new IllegalArgumentException(
          "Column '"
              + sanitizeForMessage(fullPath)
              + "': invalid structural-encoding '"
              + sanitizeForMessage(value)
              + "'. Valid values: "
              + VALID_STRUCTURAL_ENCODINGS);
    }
  }

  private static void validateRleThreshold(String fullPath, String value) {
    float threshold;
    try {
      threshold = Float.parseFloat(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Column '"
              + sanitizeForMessage(fullPath)
              + "': invalid rle-threshold '"
              + sanitizeForMessage(value)
              + "'. Must be a float in (0.0, 1.0].");
    }
    // Use a positive predicate so NaN (for which every comparison returns false) is rejected.
    if (!(threshold > 0.0f && threshold <= 1.0f)) {
      throw new IllegalArgumentException(
          "Column '"
              + sanitizeForMessage(fullPath)
              + "': rle-threshold '"
              + sanitizeForMessage(value)
              + "' is out of range. Must be in (0.0, 1.0].");
    }
  }

  private static void validateBssMode(String fullPath, String value) {
    if (!VALID_BSS_MODES.contains(value)) {
      throw new IllegalArgumentException(
          "Column '"
              + sanitizeForMessage(fullPath)
              + "': invalid bss '"
              + sanitizeForMessage(value)
              + "'. Valid values: "
              + VALID_BSS_MODES);
    }
  }

  /**
   * Replace control characters that some log appenders or terminals interpret as line/record
   * separators or render-direction overrides with their printable escapes, before interpolating
   * user-controlled strings into exception messages. Covers:
   *
   * <ul>
   *   <li>ASCII CR ({@code U+000D}) and LF ({@code U+000A}) — line separators.
   *   <li>NUL ({@code U+0000}) — truncates C-string consumers.
   *   <li>Unicode NEL ({@code U+0085}), Line Separator ({@code U+2028}), Paragraph Separator
   *       ({@code U+2029}) — line separators in many environments.
   *   <li>Bidi-override controls ({@code U+202A}–{@code U+202E}, {@code U+2066}–{@code U+2069}) —
   *       can reorder rendered text on terminals ("Trojan Source", CVE-2021-42574).
   * </ul>
   */
  private static String sanitizeForMessage(String s) {
    if (s == null) {
      return null;
    }
    StringBuilder sb = null;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      String escape = controlEscape(c);
      if (escape == null) {
        if (sb != null) {
          sb.append(c);
        }
        continue;
      }
      if (sb == null) {
        sb = new StringBuilder(s.length() + 8);
        sb.append(s, 0, i);
      }
      sb.append(escape);
    }
    return sb == null ? s : sb.toString();
  }

  private static String controlEscape(char c) {
    switch (c) {
      case '\r':
        return "\\r";
      case '\n':
        return "\\n";
      case '\u0000':
      case '\u0085':
      case '\u2028':
      case '\u2029':
      case '\u202A': // LRE
      case '\u202B': // RLE
      case '\u202C': // PDF
      case '\u202D': // LRO
      case '\u202E': // RLO
      case '\u2066': // LRI
      case '\u2067': // RLI
      case '\u2068': // FSI
      case '\u2069': // PDI
        return String.format("\\u%04X", (int) c);
      default:
        return null;
    }
  }

  private static EncodingPropertyRule rule(
      String propertySuffix, String arrowMetadataKey, BiConsumer<String, String> validator) {
    return new EncodingPropertyRule(propertySuffix, arrowMetadataKey, validator);
  }

  /** A parsed TBLPROPERTY key paired with the rule and value it carries. */
  static final class ParsedEncodingKey {
    private final List<String> pathSegments;
    private final EncodingPropertyRule rule;
    private final String value;

    ParsedEncodingKey(List<String> pathSegments, EncodingPropertyRule rule, String value) {
      this.pathSegments = List.copyOf(pathSegments);
      this.rule = rule;
      this.value = value;
    }

    List<String> getPathSegments() {
      return pathSegments;
    }

    EncodingPropertyRule getRule() {
      return rule;
    }

    String getValue() {
      return value;
    }

    /**
     * {@code (pathSegments, arrowMetadataKey)} tuple used as a HashSet element to suppress legacy
     * entries that a new-format key covers. Returning the segment list directly avoids the
     * collision risk a delimited-string encoding would have for user-supplied control characters.
     */
    List<Object> identityKey() {
      return List.of(pathSegments, rule.getArrowMetadataKey());
    }
  }

  /** Rule descriptor for mapping a Spark TBLPROPERTY to an Arrow field metadata key. */
  static final class EncodingPropertyRule {
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

    /**
     * Null-check first so {@link Float#parseFloat(String)} cannot leak an NPE through the
     * validator's {@link NumberFormatException} catch, then dispatch to the rule-specific
     * validator. The error label is derived from {@link #propertySuffix} (the part after {@code
     * lance.}).
     */
    void validate(String fullPath, String value) {
      if (value == null) {
        throw new IllegalArgumentException(
            "Column '"
                + sanitizeForMessage(fullPath)
                + "': "
                + propertySuffix.substring(LANCE_PROPERTY_DOMAIN.length() + 1)
                + " value must not be null.");
      }
      validator.accept(fullPath, value);
    }
  }
}
