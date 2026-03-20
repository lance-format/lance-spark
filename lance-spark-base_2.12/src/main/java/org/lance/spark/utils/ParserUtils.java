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
}
