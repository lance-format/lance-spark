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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LanceEncodingUtilsTest {

  @Test
  public void testSupportedRulesContainExpectedSuffixesInOrder() {
    List<String> suffixes =
        LanceEncodingUtils.getSupportedEncodingPropertyRules().stream()
            .map(LanceEncodingUtils.EncodingPropertyRule::getPropertySuffix)
            .collect(Collectors.toList());

    assertEquals(
        List.of(
            LanceEncodingUtils.COMPRESSION_SUFFIX,
            LanceEncodingUtils.COMPRESSION_LEVEL_SUFFIX,
            LanceEncodingUtils.STRUCTURAL_ENCODING_SUFFIX,
            LanceEncodingUtils.RLE_THRESHOLD_SUFFIX,
            LanceEncodingUtils.BSS_SUFFIX),
        suffixes);
  }

  @Test
  public void testSupportedRulesContainExpectedArrowMetadataKeysInOrder() {
    List<String> metadataKeys =
        LanceEncodingUtils.getSupportedEncodingPropertyRules().stream()
            .map(LanceEncodingUtils.EncodingPropertyRule::getArrowMetadataKey)
            .collect(Collectors.toList());

    assertEquals(
        List.of(
            LanceEncodingUtils.LANCE_ENCODING_COMPRESSION,
            LanceEncodingUtils.LANCE_ENCODING_COMPRESSION_LEVEL,
            LanceEncodingUtils.LANCE_ENCODING_STRUCTURAL_ENCODING,
            LanceEncodingUtils.LANCE_ENCODING_RLE_THRESHOLD,
            LanceEncodingUtils.LANCE_ENCODING_BSS),
        metadataKeys);
  }

  @Test
  public void testCreatePropertyKey() {
    assertEquals(
        "payload.lance.compression",
        LanceEncodingUtils.createPropertyKey("payload", LanceEncodingUtils.COMPRESSION_SUFFIX));
  }

  @Test
  public void testGetSupportedEncodingPropertyRulesIsImmutable() {
    List<LanceEncodingUtils.EncodingPropertyRule> rules =
        LanceEncodingUtils.getSupportedEncodingPropertyRules();
    assertThrows(UnsupportedOperationException.class, () -> rules.add(rules.get(0)));
  }
}
