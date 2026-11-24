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

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LanceCoreClassloaderTest {

  /** Verifies the Kerberos provider's reflective SPI entry point in Spark's library classloader. */
  @Test
  public void testKerberosProviderSpiWithIsolatedClassloader() throws Exception {
    Set<String> classpath = collectClasspath();
    URL[] urls = new URL[classpath.size()];
    int index = 0;
    for (String entry : classpath) {
      urls[index++] = Path.of(entry).toUri().toURL();
    }
    try (URLClassLoader loader = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader())) {
      Class<?> provider =
          Class.forName("org.lance.spark.auth.KerberosContextProvider", true, loader);
      Class<?> spi = Class.forName("org.lance.namespace.DynamicContextProvider", false, loader);
      assertTrue(spi.isAssignableFrom(provider));
      InvocationTargetException error =
          assertThrows(
              InvocationTargetException.class,
              () -> provider.getConstructor(Map.class).newInstance(Map.of()));
      assertInstanceOf(IllegalArgumentException.class, error.getCause());
      assertTrue(error.getCause().getMessage().contains("'service_principal' is required"));

      String bundle = System.getProperty("lance.test.bundle");
      if (bundle != null) {
        for (String name :
            new String[] {
              "org.lance.spark.auth.KerberosContextProvider",
              "org.lance.spark.auth.KerberosAuthConfig",
              "org.lance.namespace.RestNamespace"
            }) {
          Class<?> bundledClass = Class.forName(name, false, loader);
          assertEquals(
              Path.of(bundle).toRealPath(),
              Path.of(bundledClass.getProtectionDomain().getCodeSource().getLocation().toURI())
                  .toRealPath());
        }
      }
    }
  }

  /**
   * Verifies that Lance JNI callbacks work when Spark loads the connector outside the system
   * classloader, as happens with {@code spark.jars.packages} and managed Spark libraries.
   */
  @Test
  public void testAsyncScanWithIsolatedClassloader() throws Exception {
    Set<String> isolatedClasspath = collectClasspath();
    Path testClasses =
        Path.of(
            ClassloaderIsolationBootstrap.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .toURI());
    Path nativeLibraryDirectory = testClasses.getParent().resolve("jni-tmp");
    Files.createDirectories(nativeLibraryDirectory);

    String javaExecutable = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    ProcessBuilder processBuilder =
        new ProcessBuilder(
            javaExecutable,
            "-XX:+IgnoreUnrecognizedVMOptions",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "-Dio.netty.tryReflectionSetAccessible=true",
            "-Djava.io.tmpdir=" + nativeLibraryDirectory,
            "-cp",
            testClasses.toString(),
            ClassloaderIsolationBootstrap.class.getName(),
            String.join(File.pathSeparator, isolatedClasspath));
    processBuilder.redirectErrorStream(true);

    Process process = processBuilder.start();
    boolean exited = process.waitFor(45, TimeUnit.SECONDS);
    if (!exited) {
      process.destroyForcibly();
    }
    assertTrue(exited, "Isolated Lance scan did not finish within 45 seconds");

    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertEquals(0, process.exitValue(), output);
    assertTrue(output.contains("SUCCESS"), output);
  }

  private static Set<String> collectClasspath() throws Exception {
    Set<String> entries = new LinkedHashSet<>();
    addClassloaderUrls(entries, Thread.currentThread().getContextClassLoader());
    addClassloaderUrls(entries, LanceCoreClassloaderTest.class.getClassLoader());

    String javaClasspath = System.getProperty("java.class.path");
    if (javaClasspath != null) {
      for (String entry : javaClasspath.split(File.pathSeparator)) {
        if (!entry.isEmpty()) {
          entries.add(entry);
        }
      }
    }
    assertTrue(!entries.isEmpty(), "Could not determine the test classpath");
    return entries;
  }

  private static void addClassloaderUrls(Set<String> entries, ClassLoader classloader)
      throws Exception {
    for (ClassLoader current = classloader; current != null; current = current.getParent()) {
      if (current instanceof URLClassLoader) {
        for (URL url : ((URLClassLoader) current).getURLs()) {
          if ("file".equals(url.getProtocol())) {
            URI uri = url.toURI();
            entries.add(Path.of(uri).toString());
          }
        }
      }
    }
  }
}
