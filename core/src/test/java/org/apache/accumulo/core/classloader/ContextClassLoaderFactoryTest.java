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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.URLClassLoader;
import java.util.Objects;

import org.apache.accumulo.core.WithTestNames;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class ContextClassLoaderFactoryTest extends WithTestNames {

  @TempDir
  private static File tempFolder;

  private String uri1;
  private String uri2;

  @BeforeEach
  public void setup() throws Exception {

    File folder1 = tempFolder.toPath().resolve(testName() + "_1").toFile();
    assertTrue(folder1.isDirectory() || folder1.mkdir(), "Failed to make a new sub-directory");
    FileUtils.copyURLToFile(
        Objects.requireNonNull(this.getClass().getResource("/accumulo.properties")),
        folder1.toPath().resolve("accumulo.properties").toFile());
    uri1 = folder1.toPath().resolve("accumulo.properties").toFile().toURI().toString();

    File folder2 = tempFolder.toPath().resolve(testName() + "_2").toFile();
    assertTrue(folder2.isDirectory() || folder2.mkdir(), "Failed to make a new sub-directory");
    FileUtils.copyURLToFile(
        Objects.requireNonNull(this.getClass().getResource("/accumulo2.properties")),
        folder2.toPath().resolve("accumulo2.properties").toFile());
    uri2 = folder2.toURI() + ".*";

  }

  @Test
  public void differentContexts() {

    ConfigurationCopy cc = new ConfigurationCopy();
    cc.set(Property.GENERAL_CONTEXT_CLASSLOADER_FACTORY.getKey(),
        URLContextClassLoaderFactory.class.getName());
    ClassLoaderUtil.resetContextFactoryForTests();
    ClassLoaderUtil.initContextFactory(cc);

    URLClassLoader cl1 = (URLClassLoader) ClassLoaderUtil.getContextFactory().getClassLoader(uri1);
    var urls1 = cl1.getURLs();
    assertEquals(1, urls1.length);
    assertEquals(uri1, urls1[0].toString());

    URLClassLoader cl2 = (URLClassLoader) ClassLoaderUtil.getContextFactory().getClassLoader(uri2);
    var urls2 = cl2.getURLs();
    assertEquals(1, urls2.length);
    assertEquals(uri2, urls2[0].toString());

  }

}
