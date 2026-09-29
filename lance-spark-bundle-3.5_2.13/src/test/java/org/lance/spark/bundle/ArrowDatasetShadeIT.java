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
package org.lance.spark.bundle;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the {@code arrow-dataset} shading in the Spark bundle uber jar.
 *
 * <p>The Arrow Dataset JNI native (~50-77 MB per platform) is packaged by the {@code
 * org.apache.arrow:arrow-dataset} artifact but never used at runtime -- native interop goes through
 * the Arrow C Data Interface ({@code arrow-c-data}). It must be stripped from the bundle to keep it
 * small.
 *
 * <p>However the arrow-dataset <em>Java API</em> classes must stay: {@code lance-core}'s {@code
 * org.lance.ipc.LanceScanner} (used on the Spark read path) implements {@code
 * org.apache.arrow.dataset.scanner.Scanner}, so dropping the whole artifact would make the JVM fail
 * to load {@code LanceScanner} with {@code NoClassDefFoundError}.
 *
 * <p>This test asserts both properties directly against the shaded jar produced by the build.
 */
class ArrowDatasetShadeIT {

  private static final String SCANNER_CLASS_ENTRY =
      "org/apache/arrow/dataset/scanner/Scanner.class";
  private static final String SCANNER_TYPE = "org.apache.arrow.dataset.scanner.Scanner";
  private static final String LANCE_SCANNER_TYPE = "org.lance.ipc.LanceScanner";

  private static File shadedJar() {
    String path = System.getProperty("bundle.shaded.jar");
    assertTrue(
        path != null && !path.isEmpty(),
        "system property bundle.shaded.jar must point at the shaded uber jar");
    File jar = new File(path);
    assertTrue(jar.isFile(), "shaded jar not found: " + jar.getAbsolutePath());
    return jar;
  }

  /** The heavy, unused native binaries must not be in the bundle. */
  @Test
  void stripsArrowDatasetNative() throws Exception {
    List<String> natives = new ArrayList<>();
    try (JarFile jf = new JarFile(shadedJar())) {
      Enumeration<JarEntry> entries = jf.entries();
      while (entries.hasMoreElements()) {
        String name = entries.nextElement().getName();
        // arrow 15 packages the native at the jar root (x86_64/, aarch_64/);
        // arrow 18+ under arrow_dataset_jni/. Match the JNI library by name in either layout.
        if (name.contains("arrow_dataset_jni")) {
          natives.add(name);
        }
      }
    }
    assertTrue(
        natives.isEmpty(),
        "arrow_dataset_jni native must be stripped from the bundle but found: " + natives);
  }

  /** The tiny Arrow Dataset Java API classes must stay so LanceScanner can link. */
  @Test
  void keepsArrowDatasetApiClasses() throws Exception {
    boolean hasScanner = false;
    try (JarFile jf = new JarFile(shadedJar())) {
      hasScanner = jf.getJarEntry(SCANNER_CLASS_ENTRY) != null;
    }
    assertTrue(
        hasScanner,
        SCANNER_CLASS_ENTRY
            + " must remain in the bundle (lance-core's LanceScanner implements it)");
  }

  /**
   * The decisive end-to-end check: loading LanceScanner from the bundle in isolation must succeed.
   * Loading a class forces the JVM to resolve its direct superinterfaces, so a missing
   * arrow-dataset Scanner would surface here as NoClassDefFoundError.
   */
  @Test
  void lanceScannerLinksAgainstBundledArrowDataset() throws Exception {
    URL jarUrl = shadedJar().toURI().toURL();
    // Platform parent only, so arrow/lance classes resolve solely from the shaded jar
    // (no leakage from the test/app classpath).
    try (URLClassLoader loader =
        new URLClassLoader(new URL[] {jarUrl}, ClassLoader.getPlatformClassLoader())) {
      Class<?> lanceScanner =
          assertDoesNotThrow(
              () -> Class.forName(LANCE_SCANNER_TYPE, false, loader),
              "LanceScanner failed to load from the bundle -- arrow-dataset Java API stripped?");

      boolean implementsScanner = false;
      for (Class<?> iface : lanceScanner.getInterfaces()) {
        if (SCANNER_TYPE.equals(iface.getName())) {
          implementsScanner = true;
          break;
        }
      }
      assertTrue(
          implementsScanner, LANCE_SCANNER_TYPE + " is expected to implement " + SCANNER_TYPE);
    }
  }

  /**
   * Documents why the whole artifact cannot simply be excluded: strip the arrow-dataset Java
   * classes from a copy of the bundle and LanceScanner no longer loads. This is the exact failure
   * the earlier "exclude org.apache.arrow:arrow-dataset" approach would have shipped.
   */
  @Test
  void removingArrowDatasetApiBreaksLanceScanner() throws Exception {
    File stripped = File.createTempFile("bundle-no-arrow-dataset", ".jar");
    stripped.deleteOnExit();
    try (JarFile in = new JarFile(shadedJar());
        java.util.jar.JarOutputStream out =
            new java.util.jar.JarOutputStream(new java.io.FileOutputStream(stripped))) {
      Enumeration<JarEntry> entries = in.entries();
      byte[] buf = new byte[8192];
      while (entries.hasMoreElements()) {
        JarEntry e = entries.nextElement();
        if (e.getName().startsWith("org/apache/arrow/dataset/")) {
          continue; // simulate excluding the arrow-dataset artifact entirely
        }
        out.putNextEntry(new JarEntry(e.getName()));
        if (!e.isDirectory()) {
          try (java.io.InputStream is = in.getInputStream(e)) {
            int n;
            while ((n = is.read(buf)) > 0) {
              out.write(buf, 0, n);
            }
          }
        }
        out.closeEntry();
      }
    }

    try (URLClassLoader loader =
        new URLClassLoader(
            new URL[] {stripped.toURI().toURL()}, ClassLoader.getPlatformClassLoader())) {
      boolean threw = false;
      try {
        Class.forName(LANCE_SCANNER_TYPE, false, loader);
      } catch (NoClassDefFoundError expected) {
        threw = true;
      }
      assertTrue(
          threw,
          "expected NoClassDefFoundError loading LanceScanner without arrow-dataset classes");
    }
  }
}
