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

/** Utility methods for SQL parsing shared across Spark version modules. */
public final class ParserUtils {

  private ParserUtils() {}

  /**
   * Strips surrounding backticks from an ANTLR BACKQUOTED_IDENTIFIER token and unescapes doubled
   * backticks ({@code ``} → {@code `}). Returns the input unchanged if it is not backtick-quoted.
   *
   * @param text the raw token text from {@code ctx.getText()}, may be {@code null}
   * @return the cleaned identifier string, or {@code null} if the input is {@code null}
   */
  public static String cleanIdentifier(String text) {
    if (text == null) {
      return null;
    }
    if (text.length() >= 2 && text.startsWith("`") && text.endsWith("`")) {
      return text.substring(1, text.length() - 1).replace("``", "`");
    }
    return text;
  }

  /**
   * Backtick-quotes an identifier for use in a Spark multipart identifier string. Idempotent: if
   * the input is already surrounded by backticks it is returned unchanged. Throws when the input
   * has an unbalanced leading or trailing backtick.
   *
   * @param identifier a bare or already-quoted identifier
   * @return the backtick-quoted identifier
   * @throws IllegalArgumentException if the identifier has a backtick on only one side
   */
  public static String quoteIdentifier(String identifier) {
    boolean startsWithBacktick = identifier.startsWith("`");
    boolean endsWithBacktick = identifier.endsWith("`");
    if (identifier.length() >= 2 && startsWithBacktick && endsWithBacktick) {
      return identifier;
    }
    if (startsWithBacktick || endsWithBacktick) {
      throw new IllegalArgumentException(
          "Malformed identifier (unbalanced backticks): " + identifier);
    }
    return "`" + identifier + "`";
  }

  /**
   * Splits the raw body of a {@code REPLACE ... WHERE <predicate> AS <query>} command into its
   * predicate and query at the first <em>top-level</em> {@code AS} keyword — one that is not nested
   * inside parentheses, a string/backtick literal, or a comment. This lets the predicate itself
   * contain {@code AS} (e.g. {@code CAST(dt AS STRING)}) and lets the query contain column aliases.
   *
   * @param body the source text following {@code WHERE}, e.g. {@code "dt = '1' AS SELECT ..."}
   * @return a two-element array {@code [predicate, query]}, both trimmed
   * @throws IllegalArgumentException if no top-level {@code AS} separator is found
   */
  public static String[] splitReplaceBody(String body) {
    int depth = 0;
    int i = 0;
    int n = body.length();
    while (i < n) {
      char c = body.charAt(i);
      if (c == '\'' || c == '"' || c == '`') {
        i = skipQuoted(body, i, c);
        continue;
      }
      if (c == '-' && i + 1 < n && body.charAt(i + 1) == '-') {
        i = skipLineComment(body, i);
        continue;
      }
      if (c == '/' && i + 1 < n && body.charAt(i + 1) == '*') {
        i = skipBlockComment(body, i);
        continue;
      }
      if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
      } else if (depth == 0 && isAsKeywordAt(body, i)) {
        String predicate = body.substring(0, i).trim();
        String query = body.substring(i + 2).trim();
        if (predicate.isEmpty() || query.isEmpty()) {
          throw new IllegalArgumentException(
              "REPLACE ... WHERE requires a non-empty predicate and query around AS: " + body);
        }
        return new String[] {predicate, query};
      }
      i++;
    }
    throw new IllegalArgumentException(
        "REPLACE ... WHERE requires an AS separator between the predicate and query: " + body);
  }

  /** Returns the index just past the closing quote for the literal starting at {@code start}. */
  private static int skipQuoted(String s, int start, char quote) {
    int i = start + 1;
    int n = s.length();
    while (i < n) {
      char c = s.charAt(i);
      if (c == '\\' && quote != '`') {
        i += 2; // escaped char in a '...'/"..." literal
        continue;
      }
      if (c == quote) {
        // A doubled quote is an escaped quote, not a terminator.
        if (i + 1 < n && s.charAt(i + 1) == quote) {
          i += 2;
          continue;
        }
        return i + 1;
      }
      i++;
    }
    return n;
  }

  private static int skipLineComment(String s, int start) {
    int i = start + 2;
    int n = s.length();
    while (i < n && s.charAt(i) != '\n') {
      i++;
    }
    return i;
  }

  private static int skipBlockComment(String s, int start) {
    // Spark supports nested block comments, so track depth: an inner `*/` closes only the inner
    // comment, and an `AS` remains commented out until the outermost comment closes.
    int i = start + 2;
    int n = s.length();
    int depth = 1;
    while (i + 1 < n && depth > 0) {
      if (s.charAt(i) == '/' && s.charAt(i + 1) == '*') {
        depth++;
        i += 2;
      } else if (s.charAt(i) == '*' && s.charAt(i + 1) == '/') {
        depth--;
        i += 2;
      } else {
        i++;
      }
    }
    return depth == 0 ? i : n;
  }

  /**
   * Whether a standalone {@code AS} keyword (word-bounded, case-insensitive) begins at {@code i}.
   */
  private static boolean isAsKeywordAt(String s, int i) {
    int n = s.length();
    if (i + 2 > n) {
      return false;
    }
    char a = s.charAt(i);
    char b = s.charAt(i + 1);
    if (!((a == 'a' || a == 'A') && (b == 's' || b == 'S'))) {
      return false;
    }
    boolean leftBoundary = i == 0 || !isWordChar(s.charAt(i - 1));
    boolean rightBoundary = i + 2 == n || !isWordChar(s.charAt(i + 2));
    return leftBoundary && rightBoundary;
  }

  private static boolean isWordChar(char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }
}
