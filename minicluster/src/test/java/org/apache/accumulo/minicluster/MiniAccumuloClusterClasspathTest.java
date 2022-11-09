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
package org.apache.accumulo.minicluster;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class MiniAccumuloClusterClasspathTest extends WithTestNames {

  @SuppressWarnings("removal")
  private static final Property VFS_CONTEXT_CLASSPATH_PROPERTY =
      Property.VFS_CONTEXT_CLASSPATH_PROPERTY;

  @TempDir
  private static File tempDir;

  public static final String ROOT_PASSWORD = "superSecret";
  public static final String ROOT_USER = "root";

  public static File testDir;

  private static MiniAccumuloCluster accumulo;

  @BeforeAll
  public static void setupMiniCluster() throws Exception {
    File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests");
    assertTrue(baseDir.mkdirs() || baseDir.isDirectory());
    testDir = new File(baseDir, MiniAccumuloClusterTest.class.getName());
    FileUtils.deleteQuietly(testDir);
    assertTrue(testDir.mkdir());

    File jarFile = new File(tempDir, "iterator.jar");
    FileUtils.copyURLToFile(
        requireNonNull(MiniAccumuloClusterClasspathTest.class.getResource("/FooFilter.jar")),
        jarFile);

    MiniAccumuloConfig config = new MiniAccumuloConfig(testDir, ROOT_PASSWORD).setJDWPEnabled(true);
    config.setZooKeeperPort(0);
    HashMap<String,String> site = new HashMap<>();
    site.put(Property.TSERV_WORKQ_THREADS.getKey(), "2");
    site.put(VFS_CONTEXT_CLASSPATH_PROPERTY.getKey() + "cx1", jarFile.toURI().toString());
    config.setSiteConfig(site);
    accumulo = new MiniAccumuloCluster(config);
    accumulo.start();
  }

  @AfterAll
  public static void tearDownMiniCluster() throws Exception {
    accumulo.stop();
  }

  @Test
  @Timeout(60)
  public void testPerTableClasspath() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(accumulo.getClientProperties())
        .as(ROOT_USER, ROOT_PASSWORD).build()) {

      final String tableName = testName();

      var ntc = new NewTableConfiguration();
      ntc.setProperties(Map.of(Property.TABLE_CLASSLOADER_CONTEXT.getKey(), "cx1"));
      ntc.attachIterator(
          new IteratorSetting(100, "foocensor", "org.apache.accumulo.test.FooFilter"));

      client.tableOperations().create(tableName, ntc);

      try (BatchWriter bw = client.createBatchWriter(tableName, new BatchWriterConfig())) {

        Mutation m1 = new Mutation("foo");
        m1.put("cf1", "cq1", "v2");
        m1.put("cf1", "cq2", "v3");

        bw.addMutation(m1);

        Mutation m2 = new Mutation("bar");
        m2.put("cf1", "cq1", "v6");
        m2.put("cf1", "cq2", "v7");

        bw.addMutation(m2);

      }

      int count = 0;
      try (Scanner scanner = client.createScanner(tableName, new Authorizations())) {
        for (Entry<Key,Value> entry : scanner) {
          assertFalse(entry.getKey().getRowData().toString().toLowerCase().contains("foo"));
          count++;
        }
      }

      assertEquals(2, count);

      client.tableOperations().delete(tableName);
    }
  }

}
