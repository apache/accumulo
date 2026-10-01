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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.accumulo.core.spi.common.ContextClassLoaderEnvironment;
import org.apache.accumulo.core.spi.common.ServiceEnvironment.Configuration;
import org.junit.jupiter.api.Test;

public class URLContextClassLoaderFactoryTest {

  @Test
  public void test() throws MalformedURLException, URISyntaxException {
    URLContextClassLoaderFactory factory = new URLContextClassLoaderFactory();
    factory.init(new ContextClassLoaderEnvironment() {

      @Override
      public Configuration getConfiguration() {
        return Configuration.from(Map.of(URLContextClassLoaderFactory.URL_PATTERN_PROPERTY,
            "file:/path/a/.*|file:///path/a/.*|http://path/b/.*"), false);
      }

    });

    assertNotNull(factory.testContextAgainstPattern("file:///path/a/application_jars/.*"));
    assertNotNull(factory.testContextAgainstPattern("file:/path/a/application_jars/.*"));
    assertNotNull(factory.testContextAgainstPattern("file:///path/b/../a/application_jars/.*"));
    assertNotNull(factory.testContextAgainstPattern("file:/path/b/../a/application_jars/.*"));
    IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
        () -> factory.testContextAgainstPattern("file:///path/b/application_jars/.*"));
    assertTrue(iae.getMessage().contains("not allowed by pattern"));
    iae = assertThrows(IllegalArgumentException.class,
        () -> factory.testContextAgainstPattern("file:/path/b/application_jars/.*"));
    assertTrue(iae.getMessage().contains("not allowed by pattern"));
    iae = assertThrows(IllegalArgumentException.class,
        () -> factory.testContextAgainstPattern("file:///path/a/../b/application_jars/.*"));
    assertTrue(iae.getMessage().contains("not allowed by pattern"));
    iae = assertThrows(IllegalArgumentException.class,
        () -> factory.testContextAgainstPattern("file:///path/a/../c/application_jars/.*"));
    assertTrue(iae.getMessage().contains("not allowed by pattern"));

    assertNotNull(factory.testContextAgainstPattern("http:///path/b/application_jars/.*"));
    assertNotNull(factory.testContextAgainstPattern("http:/path/b/application_jars/.*"));
    assertNotNull(factory.testContextAgainstPattern("http:///path/c/../b/application_jars/.*"));
    assertNotNull(factory.testContextAgainstPattern("http:/path/c/../b/application_jars/.*"));
    iae = assertThrows(IllegalArgumentException.class,
        () -> factory.testContextAgainstPattern("http:///path/a/application_jars/.*"));
    assertTrue(iae.getMessage().contains("not allowed by pattern"));
    iae = assertThrows(IllegalArgumentException.class,
        () -> factory.testContextAgainstPattern("http:/path/a/application_jars/.*"));
    assertTrue(iae.getMessage().contains("not allowed by pattern"));
    iae = assertThrows(IllegalArgumentException.class,
        () -> factory.testContextAgainstPattern("http:///path/b/../c/application_jars/.*"));
    assertTrue(iae.getMessage().contains("not allowed by pattern"));
    iae = assertThrows(IllegalArgumentException.class,
        () -> factory.testContextAgainstPattern("http:///path/b/../c/application_jars/.*"));
    assertTrue(iae.getMessage().contains("not allowed by pattern"));

  }
}
