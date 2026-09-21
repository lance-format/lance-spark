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

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

/** Loads Lance outside the system classloader to simulate Spark's dynamically added JARs. */
public final class ClassloaderIsolationBootstrap {

  private ClassloaderIsolationBootstrap() {}

  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Usage: ClassloaderIsolationBootstrap <isolated-classpath>");
      System.exit(2);
    }

    try {
      String[] entries = args[0].split(File.pathSeparator);
      URL[] urls = new URL[entries.length];
      for (int i = 0; i < entries.length; i++) {
        urls[i] = new File(entries[i]).toURI().toURL();
      }

      try (URLClassLoader isolatedClassloader = new URLClassLoader(urls, null)) {
        Thread.currentThread().setContextClassLoader(isolatedClassloader);
        Class<?> helper =
            Class.forName("org.lance.spark.ClassloaderIsolationHelper", true, isolatedClassloader);
        Method run = helper.getMethod("run");
        run.invoke(null);
      }

      System.out.println("SUCCESS");
      System.exit(0);
    } catch (Throwable error) {
      error.printStackTrace(System.err);
      System.exit(1);
    }
  }
}
