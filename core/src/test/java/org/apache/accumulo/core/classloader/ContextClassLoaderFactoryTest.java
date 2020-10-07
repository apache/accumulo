/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.classloader;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class ContextClassLoaderFactoryTest {

  @Rule
  public TemporaryFolder tempFolder =
      new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  private File folder1;
  private File folder2;
  private String uri1;
  private String uri2;

  @Before
  public void setup() throws Exception {

    folder1 = tempFolder.newFolder();
    FileUtils.copyURLToFile(this.getClass().getResource("/accumulo.properties"),
        new File(folder1, "accumulo.properties"));
    uri1 = new File(folder1, "accumulo.properties").toURI().toString();

    folder2 = tempFolder.newFolder();
    FileUtils.copyURLToFile(this.getClass().getResource("/accumulo2.properties"),
        new File(folder2, "accumulo2.properties"));
    uri2 = folder2.toURI() + ".*";

  }

  @Test
  public void differentContexts() throws Exception {

    ConfigurationCopy cc = new ConfigurationCopy();
    cc.set(ContextClassLoaders.CONTEXT_CLASS_LOADER_FACTORY, URLClassLoaderFactory.class.getName());
    ContextClassLoaders.resetForTests();
    ContextClassLoaders.initialize(new Supplier<Map<String,String>>() {
      @Override
      public Map<String,String> get() {
        Map<String,String> props = new HashMap<>();
        cc.getProperties(props, key -> !key.isBlank());
        return props;
      }
    });

    URLClassLoader cl1 = (URLClassLoader) ContextClassLoaders.getClassLoader(uri1);
    var urls1 = cl1.getURLs();
    assertEquals(1, urls1.length);
    assertEquals(uri1, urls1[0].toString());

    URLClassLoader cl2 = (URLClassLoader) ContextClassLoaders.getClassLoader(uri2);
    var urls2 = cl2.getURLs();
    assertEquals(1, urls2.length);
    assertEquals(uri2, urls2[0].toString());

  }

}
