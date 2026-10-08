/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.classloader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.accumulo.core.spi.common.ContextClassLoaderEnvironment;
import org.apache.accumulo.core.spi.common.ServiceEnvironment.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class URLContextClassLoaderFactoryTest {

  @Test
  public void test(@TempDir Path tempDir) throws Exception {
    Path allowedDir = tempDir.resolve("allowed");
    Path allowedJars = allowedDir.resolve("application_jars");
    Files.createDirectories(allowedJars);
    Path allowedJar = Files.createFile(allowedJars.resolve("allowed.jar"));
    Path outsideJar = Files.createFile(tempDir.resolve("outside.jar"));

    String allowedFilePattern = allowedDir.toUri().toURL().toExternalForm() + ".*";
    URLContextClassLoaderFactory factory =
        factoryForPattern(allowedFilePattern + "|http://path/b/.*");

    // Existing allowed files pass, including paths with literal dot segments that normalize
    // to an allowed file.
    URL expectedCanonicalUrl = allowedJar.toRealPath().toUri().toURL();
    assertEquals(expectedCanonicalUrl,
        factory.testContextAgainstPattern(allowedJar.toUri().toString()));
    assertEquals(expectedCanonicalUrl, factory.testContextAgainstPattern(
        allowedDir.toUri().toString() + "application_jars/../application_jars/allowed.jar"));
    assertThrows(IOException.class, () -> factory
        .testContextAgainstPattern(allowedJars.resolve("missing.jar").toUri().toString()));

    // Traversal that resolves outside the allowed directory is rejected, whether written
    // literally, percent-encoded, or reached through a symlink.
    assertThrows(IllegalArgumentException.class,
        () -> factory.testContextAgainstPattern(allowedDir.toUri() + "../outside.jar"));
    assertThrows(IllegalArgumentException.class,
        () -> factory.testContextAgainstPattern(allowedDir.toUri() + "%2e%2e/outside.jar"));
    Path symlink = Files.createSymbolicLink(allowedDir.resolve("escape.jar"), outsideJar);
    assertThrows(IllegalArgumentException.class,
        () -> factory.testContextAgainstPattern(symlink.toUri().toString()));

    // HTTP URLs retain literal dot-segment normalization, but encoded path delimiters, dots, and
    // percent escapes that could be double-decoded are rejected as ambiguous.
    assertNotNull(factory.testContextAgainstPattern("http:///path/b/application_jars/.*"));
    assertNotNull(factory.testContextAgainstPattern("http:/path/c/../b/application_jars/.*"));
    IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
        () -> factory.testContextAgainstPattern("http:///path/b/%2e%2e/c/application_jars/.*"));
    assertTrue(iae.getMessage().contains("encoded path traversal"));
    assertThrows(IllegalArgumentException.class,
        () -> factory.testContextAgainstPattern("http:///path/b/%2f../c/application_jars/.*"));
    assertThrows(IllegalArgumentException.class,
        () -> factory.testContextAgainstPattern("http:///path/b/%252e%252e/c/application_jars/.*"));

    iae = assertThrows(IllegalArgumentException.class,
        () -> factory.testContextAgainstPattern("http:///path/a/application_jars/.*"));
    assertTrue(iae.getMessage().contains("not allowed by pattern"));
  }

  private URLContextClassLoaderFactory factoryForPattern(String pattern) {
    URLContextClassLoaderFactory factory = new URLContextClassLoaderFactory();
    factory.init(new ContextClassLoaderEnvironment() {

      @Override
      public Configuration getConfiguration() {
        return Configuration
            .from(Map.of(URLContextClassLoaderFactory.URL_PATTERN_PROPERTY, pattern), false);
      }

    });
    return factory;
  }
}
