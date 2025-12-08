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
package org.lance.spark;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

public abstract class BaseTestSparkLanceDbRestNamespace extends SparkLanceNamespaceTestBase {
  private static final Logger log =
      LoggerFactory.getLogger(BaseTestSparkLanceDbRestNamespace.class);

  protected static String DATABASE;
  protected static String API_KEY;
  protected static String HOST_OVERRIDE;
  protected static String REGION;

  @BeforeAll
  public static void setUpClass() {

    DATABASE = System.getenv("LANCEDB_DB");
    API_KEY = System.getenv("LANCEDB_API_KEY");
    HOST_OVERRIDE = System.getenv("LANCEDB_HOST_OVERRIDE");
    REGION = System.getenv("LANCEDB_REGION");

    if (isNullOrEmpty(REGION)) {
      REGION = "us-east-1";
    }

    if (DATABASE != null && API_KEY != null) {
      log.info("Using configuration:");
      log.info("  Database: {}", DATABASE);
      log.info("  Region: {}", REGION);
      log.info("  Host Override: {}", isNullOrEmpty(HOST_OVERRIDE) ? "none" : HOST_OVERRIDE);
    }
  }

  @Override
  void setup() throws IOException {
    skipIfNotConfigured();
    super.setup();
  }

  @Override
  protected String getNsImpl() {
    return "rest";
  }

  @Override
  protected Map<String, String> getAdditionalNsConfigs() {
    Map<String, String> configs = new HashMap<>();

    configs.put("headers.x-api-key", API_KEY);
    configs.put("headers.x-lancedb-database", DATABASE);

    String uri;
    if (!isNullOrEmpty(HOST_OVERRIDE)) {
      uri = HOST_OVERRIDE;
    } else {
      String effectiveRegion = isNullOrEmpty(REGION) ? "us-east-1" : REGION;
      uri = String.format("https://%s.%s.api.lancedb.com", DATABASE, effectiveRegion);
    }
    configs.put("uri", uri);

    return configs;
  }

  /**
   * Skip test if environment variables are not set. Call this at the beginning of each test method.
   */
  protected void skipIfNotConfigured() {
    assumeTrue(
        !isNullOrEmpty(DATABASE) && !isNullOrEmpty(API_KEY),
        "Skipping test: LANCEDB_DB and LANCEDB_API_KEY environment variables must be set");
  }

  private static boolean isNullOrEmpty(String s) {
    return s == null || s.isEmpty();
  }

  @Test
  @Override
  public void testCreateAndDescribeTable() throws Exception {
    skipIfNotConfigured();
    super.testCreateAndDescribeTable();
  }

  @Test
  @Override
  public void testDropTable() throws Exception {
    skipIfNotConfigured();
    super.testDropTable();
  }

  @Test
  @Override
  public void testListTables() throws Exception {
    skipIfNotConfigured();
    super.testListTables();
  }

  @Test
  @Override
  public void testSparkSqlJoin() throws Exception {
    skipIfNotConfigured();
    super.testSparkSqlJoin();
  }

  @Test
  @Override
  public void testSparkSqlSelect() throws Exception {
    skipIfNotConfigured();
    super.testSparkSqlSelect();
  }

  @Test
  @Override
  public void testLoadSparkTable() throws Exception {
    skipIfNotConfigured();
    super.testLoadSparkTable();
  }
}
