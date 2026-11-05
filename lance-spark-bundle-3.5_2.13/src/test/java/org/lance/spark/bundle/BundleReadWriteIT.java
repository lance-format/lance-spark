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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Functional guard for the {@code arrow-dataset} shading: a real write-then-read of a Lance table
 * through the shaded bundle jar. Where {@link ArrowDatasetShadeIT} proves the right classes are in
 * the jar and that {@code LanceScanner} links, this proves the native read/write path actually
 * works when driven from the bundle -- i.e. keeping only the arrow-dataset Java API (native
 * stripped) is functionally sufficient.
 *
 * <p>The write/read runs in a child JVM with the shaded jar <em>prepended</em> to the classpath, so
 * every lance/arrow class and the bundled lance native library resolve from the bundle. Spark and
 * Scala are {@code provided} scope (intentionally not bundled) and come from the inherited
 * classpath, mirroring how a user consumes the uber jar. A child JVM (rather than an in-process
 * classloader) is used because the lance native library can only be loaded once per JVM.
 */
class BundleReadWriteIT {

  // Spark on JDK 17 needs these module opens; mirror the surefire/failsafe argLine in the root pom.
  private static final List<String> SPARK_JDK17_ARGS =
      List.of(
          "-XX:+IgnoreUnrecognizedVMOptions",
          "--add-opens=java.base/java.lang=ALL-UNNAMED",
          "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
          "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
          "--add-opens=java.base/java.io=ALL-UNNAMED",
          "--add-opens=java.base/java.net=ALL-UNNAMED",
          "--add-opens=java.base/java.nio=ALL-UNNAMED",
          "--add-opens=java.base/java.util=ALL-UNNAMED",
          "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
          "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
          "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
          "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
          "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
          "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
          "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
          "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
          "-Djdk.reflect.useDirectMethodHandle=false",
          "-Dio.netty.tryReflectionSetAccessible=true");

  private static File shadedJar() {
    String path = System.getProperty("bundle.shaded.jar");
    assertTrue(
        path != null && !path.isEmpty(),
        "system property bundle.shaded.jar must point at the shaded uber jar");
    File jar = new File(path);
    assertTrue(jar.isFile(), "shaded jar not found: " + jar.getAbsolutePath());
    return jar;
  }

  @Test
  void writesAndReadsThroughShadedBundle() throws Exception {
    File jar = shadedJar();
    // Bundle first, so lance/arrow classes + the lance native resolve from it rather than the
    // reactor jars; Spark/Scala (provided, unbundled) are supplied by the inherited classpath.
    String classpath =
        jar.getAbsolutePath() + File.pathSeparator + System.getProperty("java.class.path");

    Path workDir = Files.createTempDirectory("bundle-smoke");
    String javaBin =
        System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";

    List<String> cmd = new ArrayList<>();
    cmd.add(javaBin);
    cmd.addAll(SPARK_JDK17_ARGS);
    cmd.add("-cp");
    cmd.add(classpath);
    cmd.add("org.lance.spark.bundle.BundleReadWriteSmoke");
    cmd.add(workDir.toString());

    ProcessBuilder pb = new ProcessBuilder(cmd);
    pb.directory(workDir.toFile()); // keep derby.log / spark-warehouse out of the module dir
    pb.redirectErrorStream(true);
    Process process = pb.start();

    byte[] outBytes = process.getInputStream().readAllBytes();
    String output = new String(outBytes, StandardCharsets.UTF_8);
    boolean finished = process.waitFor(5, TimeUnit.MINUTES);
    if (!finished) {
      process.destroyForcibly();
      fail("child JVM read/write did not finish within 5 minutes; output:\n" + output);
    }

    int exit = process.exitValue();
    assertTrue(
        exit == 0 && output.contains("SMOKE_OK"),
        "read/write through the shaded bundle failed (exit=" + exit + "); output:\n" + output);
  }
}
