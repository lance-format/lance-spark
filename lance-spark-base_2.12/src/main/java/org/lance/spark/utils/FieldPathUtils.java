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

import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Utilities for Lance field paths used by SQL index commands. */
public final class FieldPathUtils {

  private FieldPathUtils() {}

  public static String canonicalPath(List<String> parts) {
    if (parts == null || parts.isEmpty()) {
      throw new IllegalArgumentException("Field path must contain at least one part");
    }
    return parts.stream().map(FieldPathUtils::formatPathPart).collect(Collectors.joining("."));
  }

  public static String formatPathPart(String part) {
    if (part == null || part.isEmpty()) {
      throw new IllegalArgumentException("Field path part must not be empty");
    }
    if (needsQuoting(part)) {
      return "`" + part.replace("`", "``") + "`";
    }
    return part;
  }

  public static LanceField resolveLeafField(LanceSchema schema, String canonicalPath) {
    List<String> parts = parseCanonicalPath(canonicalPath);
    LanceField field = resolveField(schema, parts);
    if (!field.getChildren().isEmpty()) {
      throw new IllegalArgumentException(
          "Index column must be a leaf field, got: " + canonicalPath);
    }
    return field;
  }

  public static String pathByFieldId(LanceSchema schema, int fieldId) {
    for (LanceField field : schema.fields()) {
      String path = pathByFieldId(field, fieldId, new ArrayList<>());
      if (path != null) {
        return path;
      }
    }
    return null;
  }

  public static List<String> parseCanonicalPath(String path) {
    if (path == null || path.isEmpty()) {
      throw new IllegalArgumentException("Field path must not be empty");
    }

    List<String> parts = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean inQuotedPart = false;
    boolean quotedPart = false;
    boolean justClosedQuote = false;

    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (inQuotedPart) {
        if (c == '`') {
          if (i + 1 < path.length() && path.charAt(i + 1) == '`') {
            current.append('`');
            i++;
          } else {
            inQuotedPart = false;
            justClosedQuote = true;
          }
        } else {
          current.append(c);
        }
      } else if (c == '`') {
        if (current.length() != 0 || quotedPart || justClosedQuote) {
          throw new IllegalArgumentException("Malformed field path: " + path);
        }
        inQuotedPart = true;
        quotedPart = true;
      } else if (c == '.') {
        addPathPart(parts, current, quotedPart, path);
        quotedPart = false;
        justClosedQuote = false;
      } else {
        if (justClosedQuote) {
          throw new IllegalArgumentException("Malformed field path: " + path);
        }
        current.append(c);
      }
    }

    if (inQuotedPart) {
      throw new IllegalArgumentException("Malformed field path: " + path);
    }
    addPathPart(parts, current, quotedPart, path);
    return parts;
  }

  private static void addPathPart(
      List<String> parts, StringBuilder current, boolean quotedPart, String path) {
    if (current.length() == 0 && !quotedPart) {
      throw new IllegalArgumentException("Malformed field path: " + path);
    }
    parts.add(current.toString());
    current.setLength(0);
  }

  private static LanceField resolveField(LanceSchema schema, List<String> parts) {
    Objects.requireNonNull(schema, "schema must not be null");
    List<LanceField> fields = schema.fields();
    LanceField current = null;
    for (String part : parts) {
      current = null;
      for (LanceField field : fields) {
        if (field.getName().equals(part)) {
          current = field;
          break;
        }
      }
      if (current == null) {
        throw new IllegalArgumentException(
            "Cannot find field path in Lance schema: " + canonicalPath(parts));
      }
      fields = current.getChildren();
    }
    return current;
  }

  private static String pathByFieldId(LanceField field, int fieldId, List<String> ancestors) {
    ancestors.add(field.getName());
    try {
      if (field.getId() == fieldId) {
        return canonicalPath(ancestors);
      }
      for (LanceField child : field.getChildren()) {
        String path = pathByFieldId(child, fieldId, ancestors);
        if (path != null) {
          return path;
        }
      }
      return null;
    } finally {
      ancestors.remove(ancestors.size() - 1);
    }
  }

  private static boolean needsQuoting(String part) {
    for (int i = 0; i < part.length(); i++) {
      char c = part.charAt(i);
      if (!((c >= 'A' && c <= 'Z')
          || (c >= 'a' && c <= 'z')
          || (c >= '0' && c <= '9')
          || c == '_')) {
        return true;
      }
    }
    return false;
  }
}
