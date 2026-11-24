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

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the path-based-access error enrichment in {@link BaseLanceNamespaceSparkCatalog}.
 *
 * <p>Storage credentials passed via {@code .option(...)} are dropped on path-based access (only the
 * table identifier is forwarded to the catalog), so the native open falls back to the default
 * credential chain and surfaces a confusing auth error far from the real cause. These tests cover
 * the message-based detection and enrichment; no cloud account is required.
 */
public class PathAccessErrorTest {

  private static final String URI = "abfss://fs@acct.dfs.core.windows.net/t.lance";

  /** The Azure MSI/IMDS failure users actually hit when a SAS token is dropped. */
  private static final String AZURE_MSI_ERROR =
      "LanceError(IO): Generic MicrosoftAzure error: Error performing token request: "
          + "Error performing GET http://169.254.169.254/metadata/identity/oauth2/token "
          + "after 3 retries";

  @Test
  public void detectsAzureMsiTokenFailure() {
    assertTrue(
        BaseLanceNamespaceSparkCatalog.looksLikeStorageAuthFailure(
            new RuntimeException(AZURE_MSI_ERROR)));
  }

  @Test
  public void detectsCommonAuthMarkers() {
    assertTrue(
        BaseLanceNamespaceSparkCatalog.looksLikeStorageAuthFailure(
            new RuntimeException("403 Forbidden")));
    assertTrue(
        BaseLanceNamespaceSparkCatalog.looksLikeStorageAuthFailure(
            new RuntimeException("<Code>SignatureDoesNotMatch</Code>")));
    assertTrue(
        BaseLanceNamespaceSparkCatalog.looksLikeStorageAuthFailure(
            new RuntimeException("The request is not authorized to perform this operation.")));
  }

  @Test
  public void detectsAuthMarkerInNestedCause() {
    RuntimeException wrapped =
        new RuntimeException("failed to open dataset", new RuntimeException(AZURE_MSI_ERROR));
    assertTrue(BaseLanceNamespaceSparkCatalog.looksLikeStorageAuthFailure(wrapped));
  }

  @Test
  public void ignoresNonAuthErrors() {
    assertFalse(
        BaseLanceNamespaceSparkCatalog.looksLikeStorageAuthFailure(
            new RuntimeException("Not a Lance dataset: missing _versions directory")));
    assertFalse(BaseLanceNamespaceSparkCatalog.looksLikeStorageAuthFailure(new RuntimeException()));
  }

  @Test
  public void enrichesAuthErrorWithActionableGuidance() {
    RuntimeException cause = new RuntimeException(AZURE_MSI_ERROR);
    RuntimeException enriched = BaseLanceNamespaceSparkCatalog.describePathAccessError(URI, cause);

    String message = enriched.getMessage();
    assertTrue(message.contains(URI), "message should name the dataset uri: " + message);
    assertTrue(message.contains(".option("), "message should call out .option(): " + message);
    assertTrue(
        message.contains("spark.sql.catalog"),
        "message should point at catalog config: " + message);
    // The original native error is preserved as the cause for debugging.
    assertSame(cause, enriched.getCause());
    assertTrue(
        message.contains(AZURE_MSI_ERROR),
        "message should include the underlying error: " + message);
  }

  @Test
  public void detectsNativeS3AccessDenied() {
    assertTrue(
        BaseLanceNamespaceSparkCatalog.looksLikeStorageAuthFailure(
            new IOException(nativeS3Error("403 Forbidden", "AccessDenied", "1.2ms"))));
  }

  /** Native messages carry ports and timings, which must not read as an HTTP 403. */
  @Test
  public void ignoresStatusLikeNumbersInPortsAndTimings() {
    assertFalse(
        BaseLanceNamespaceSparkCatalog.looksLikeStorageAuthFailure(
            new IOException(nativeS3Error("404 Not Found", "NoSuchKey", "403.583\u00b5s"))));
    assertFalse(
        BaseLanceNamespaceSparkCatalog.looksLikeStorageAuthFailure(
            new IOException(nativeS3Error("400 Bad Request", "InvalidRequest", "1.2ms"))));
  }

  /** The shape of a native S3 error from the object store, on port 54031. */
  private static String nativeS3Error(String status, String code, String elapsed) {
    return "LanceError(IO): Generic S3 error: Error performing list request: Error performing GET "
        + "http://127.0.0.1:54031/bucket?list-type=2&prefix=t.lance%2F_versions%2F in "
        + elapsed
        + " - Server returned non-2xx status code: "
        + status
        + ": <Error><Code>"
        + code
        + "</Code><Message>m</Message></Error>";
  }
}
